package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"slices"
	"sync"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// streamEager is the byte-budget-aware streaming pipeline backing
// ReadIter / ReadManyIter. Three concurrent stages plus the
// caller's yield loop:
//
//  1. Producer goroutine: walks partitions in lex order and
//     pushes (partIdx, fileIdx) jobs into a download queue.
//
//  2. Downloader workers (MaxInflightRequests goroutines): pull
//     jobs, fetch parquet bodies, deposit into per-partition
//     slots. Cross-partition lookahead happens here — workers
//     are not partition-bound, so partition P+1's downloads can
//     run in parallel with partition P being yielded.
//
//  3. Decoder goroutine: walks partitions in order; for each,
//     waits until all files are downloaded, parses each parquet
//     footer to compute the partition's exact uncompressed total,
//     gates on (ReadAheadPartitions, ReadAheadBytes), decodes
//     into versionedRecord[T], and pushes a decodedBatch to the
//     yielder.
//
//  4. Yield loop (this goroutine): pulls decoded partitions in
//     order, runs sortAndDedup, and forwards records via yield.
//     Frees the partition's reserved bytes on completion so the
//     decoder can proceed.
//
// Compared with streamByPartition (which still backs
// ReadIterWhere): downloads are continuously in flight
// regardless of decode pace, the budget gate uses exact
// uncompressed sizes from parquet footers rather than partition
// counts, and a single oversized partition still flows (the cap
// can't bind below partition granularity without row-group-level
// streaming).
func (s *Reader[T]) streamEager(
	ctx context.Context, keys []core.KeyMeta,
	opts *core.QueryOpts, yield func(T, error) bool,
) {
	if len(keys) == 0 {
		return
	}

	parts := s.preparePartitions(keys)
	if len(parts) == 0 {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	concurrency := s.cfg.Target.EffectiveMaxInflightRequests()

	// Shared state: per-partition download progress + buffered
	// uncompressed byte total.
	state := &streamState{
		parts: parts,
	}
	state.cond = sync.NewCond(&state.mu)

	// One WaitGroup covers every helper goroutine so the deferred
	// cleanup below can cancel ctx and then wait for everything to
	// drain before returning — no orphaned goroutines, no leaked
	// state.
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	// Stage 1+2: producer feeds jobs; workers download into slots.
	jobsCh := make(chan downloadJob, concurrency)
	wg.Go(func() { s.runProducer(ctx, jobsCh, parts) })
	for range concurrency {
		wg.Go(func() {
			s.runDownloader(ctx, jobsCh, state, cancel)
		})
	}
	// Wake up any goroutine sleeping on state.cond when ctx
	// fires — Wait() doesn't observe context cancellation, so
	// without this broadcast a waiting decoder would stall after
	// the consumer breaks out of the iter loop.
	wg.Go(func() {
		<-ctx.Done()
		state.mu.Lock()
		state.cond.Broadcast()
		state.mu.Unlock()
	})

	// Stage 3: decoder. Channel cap = ReadAheadPartitions so the
	// pipeline buffers up to N decoded partitions ahead. ≥0
	// guarantees a usable buffer even at the default.
	readAheadParts := max(0, opts.ReadAheadPartitions)
	decodedCh := make(chan decodedBatch[T], readAheadParts)
	wg.Go(func() { s.runDecoder(ctx, state, opts, decodedCh) })

	// Stage 4: yield loop. Drains decodedCh, signals on each
	// completed partition so the decoder can release the
	// byte-budget reservation.
	for batch := range decodedCh {
		if batch.err != nil {
			yield(*new(T), batch.err)
			return
		}
		ok := s.emitPartition(batch.recs, opts.IncludeHistory, yield)
		state.releaseBytes(batch.uncompBytes)
		if !ok {
			return
		}
	}
}

// preparePartitions groups the LIST result by Hive partition,
// sorts partition keys lex, sorts files within each partition
// by S3 key (deterministic download order), and allocates the
// per-partition slots.
func (s *Reader[T]) preparePartitions(
	keys []core.KeyMeta,
) []*partState {
	byPartition := s.groupKeysByPartition(keys)
	if len(byPartition) == 0 {
		return nil
	}
	partitionKeys := make([]string, 0, len(byPartition))
	for k := range byPartition {
		partitionKeys = append(partitionKeys, k)
	}
	slices.Sort(partitionKeys)
	parts := make([]*partState, len(partitionKeys))
	for i, p := range partitionKeys {
		files := byPartition[p]
		sortKeyMetasByKey(files)
		parts[i] = &partState{
			files:  files,
			bodies: make([][]byte, len(files)),
			errs:   make([]error, len(files)),
		}
	}
	return parts
}

// runProducer walks partitions in order and pushes one download
// job per file. Closes jobsCh when done so workers can exit.
func (s *Reader[T]) runProducer(
	ctx context.Context, jobsCh chan<- downloadJob,
	parts []*partState,
) {
	defer close(jobsCh)
	for pi, p := range parts {
		for fi := range p.files {
			select {
			case jobsCh <- downloadJob{partIdx: pi, fileIdx: fi}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// runDownloader is one worker in the download pool. Each job
// fetches one parquet body and stores the result in the
// per-partition slot. NoSuchKey is treated as a soft skip
// (records the missing-data callback, leaves body nil); other
// errors trigger ctx cancel so siblings bail promptly.
func (s *Reader[T]) runDownloader(
	ctx context.Context, jobsCh <-chan downloadJob,
	state *streamState, cancel context.CancelFunc,
) {
	for job := range jobsCh {
		if ctx.Err() != nil {
			state.markComplete(job.partIdx, job.fileIdx, nil, ctx.Err())
			continue
		}
		key := state.parts[job.partIdx].files[job.fileIdx].Key
		body, err := s.cfg.Target.get(
			ctx, key, s.cfg.ConsistencyControl)
		if err != nil {
			if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
				if s.cfg.OnMissingData != nil {
					s.cfg.OnMissingData(key)
				}
				state.markComplete(job.partIdx, job.fileIdx, nil, nil)
				continue
			}
			state.markComplete(job.partIdx, job.fileIdx, nil,
				fmt.Errorf("s3parquet: get %s: %w", key, err))
			cancel()
			continue
		}
		state.markComplete(job.partIdx, job.fileIdx, body, nil)
	}
}

// runDecoder walks partitions in order; for each it waits on
// download completion, parses footers for the exact uncompressed
// total, gates on the byte budget, and decodes. Sends each
// completed partition's records (or first hard error) to
// decodedCh.
func (s *Reader[T]) runDecoder(
	ctx context.Context, state *streamState,
	opts *core.QueryOpts, decodedCh chan<- decodedBatch[T],
) {
	defer close(decodedCh)
	for pi := range state.parts {
		if !state.waitForPartition(ctx, pi) {
			return
		}
		ps := state.parts[pi]

		// Surface first hard download error.
		if err := ps.firstError(); err != nil {
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		// Sum exact uncompressed bytes from each file's footer.
		// Missing files (nil body) contribute zero.
		uncomp, err := footerUncompressedSum(ps)
		if err != nil {
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		// Gate on byte budget if configured. A single oversized
		// partition still flows once the buffer is empty —
		// otherwise the pipeline would deadlock.
		if !state.reserveBytes(ctx, uncomp, opts.ReadAheadBytes) {
			return
		}

		recs, err := s.decodePartition(ps)
		// Drop the compressed bodies as soon as decode finishes
		// so the GC can reclaim the bytes while the partition
		// sits in the decoded buffer.
		ps.bodies = nil
		ps.errs = nil
		if err != nil {
			state.releaseBytes(uncomp)
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		if !sendBatch(ctx, decodedCh, decodedBatch[T]{
			recs: recs, uncompBytes: uncomp,
		}) {
			state.releaseBytes(uncomp)
			return
		}
	}
}

// decodePartition parses every successfully-downloaded body in
// ps and returns the concatenated versionedRecords. Files that
// were missing on download (body == nil, err == nil) are
// skipped — OnMissingData was already invoked by the downloader.
func (s *Reader[T]) decodePartition(
	ps *partState,
) ([]versionedRecord[T], error) {
	var out []versionedRecord[T]
	for fi, body := range ps.bodies {
		if body == nil {
			continue
		}
		recs, err := decodeParquet[T](body)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: decode %s: %w", ps.files[fi].Key, err)
		}
		fileName := path.Base(ps.files[fi].Key)
		fallbackTime := ps.files[fi].InsertedAt
		for j, r := range recs {
			ia := fallbackTime
			if s.insertedAtFieldIndex != nil {
				colVal := reflect.ValueOf(&recs[j]).Elem().
					FieldByIndex(s.insertedAtFieldIndex).
					Interface().(time.Time)
				if !colVal.IsZero() {
					ia = colVal
				}
			}
			out = append(out, versionedRecord[T]{
				rec:        r,
				insertedAt: ia,
				fileName:   fileName,
			})
		}
	}
	return out, nil
}

// downloadJob is one (partition, file) tuple flowing through
// the producer → downloader queue.
type downloadJob struct {
	partIdx int
	fileIdx int
}

// partState holds per-partition download progress. files is
// fixed at preparePartitions time; bodies + errs + completed
// are mutated by downloaders under streamState.mu.
type partState struct {
	files     []core.KeyMeta
	bodies    [][]byte
	errs      []error
	completed int
}

// firstError returns the first non-nil download error in the
// partition, if any. Caller has already filtered NoSuchKey to
// nil + nil-body, so any error here is hard.
func (p *partState) firstError() error {
	for _, e := range p.errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// streamState carries the shared mutable state of the pipeline:
// per-partition download counters, the decoded-bytes reservation,
// and the cond var used by both downloaders (signaling completion)
// and the yield loop (signaling buffer release).
type streamState struct {
	mu            sync.Mutex
	cond          *sync.Cond
	parts         []*partState
	bufferedBytes int64
}

// markComplete is the downloader-side update: store the result
// into the partition's slot, increment completed, and wake the
// decoder if it's waiting on this partition.
func (s *streamState) markComplete(
	partIdx, fileIdx int, body []byte, err error,
) {
	s.mu.Lock()
	s.parts[partIdx].bodies[fileIdx] = body
	s.parts[partIdx].errs[fileIdx] = err
	s.parts[partIdx].completed++
	s.cond.Broadcast()
	s.mu.Unlock()
}

// waitForPartition blocks until every file in partition pi has
// been downloaded (success or error) or ctx is cancelled. Returns
// true on completion, false on cancellation.
func (s *streamState) waitForPartition(
	ctx context.Context, pi int,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.parts[pi].completed < len(s.parts[pi].files) {
		if ctx.Err() != nil {
			return false
		}
		s.cond.Wait()
	}
	return ctx.Err() == nil
}

// reserveBytes accounts uncomp bytes against the cap. Blocks
// while bufferedBytes + uncomp would exceed cap AND the buffer
// is non-empty; the empty-buffer escape lets a single oversized
// partition through (otherwise the pipeline would deadlock).
// Returns false if ctx is cancelled while waiting.
func (s *streamState) reserveBytes(
	ctx context.Context, uncomp, cap int64,
) bool {
	if cap <= 0 || uncomp <= 0 {
		return ctx.Err() == nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.bufferedBytes > 0 && s.bufferedBytes+uncomp > cap {
		if ctx.Err() != nil {
			return false
		}
		s.cond.Wait()
	}
	s.bufferedBytes += uncomp
	return true
}

// releaseBytes is called by the yield loop after a partition's
// records have been forwarded; frees the reservation so the
// decoder can pick the next partition.
func (s *streamState) releaseBytes(uncomp int64) {
	if uncomp <= 0 {
		return
	}
	s.mu.Lock()
	s.bufferedBytes -= uncomp
	s.cond.Broadcast()
	s.mu.Unlock()
}

// decodedBatch is one partition's decoded records (or a single
// hard error) flowing from the decoder to the yield loop.
// uncompBytes is what the decoder reserved; the yield loop
// returns it via releaseBytes after the records are forwarded.
type decodedBatch[T any] struct {
	recs        []versionedRecord[T]
	uncompBytes int64
	err         error
}

// sendBatch pushes a batch onto decodedCh, returning false on
// ctx cancellation so the caller can clean up the byte
// reservation it might have just made.
func sendBatch[T any](
	ctx context.Context, decodedCh chan<- decodedBatch[T],
	b decodedBatch[T],
) bool {
	select {
	case decodedCh <- b:
		return true
	case <-ctx.Done():
		return false
	}
}

// footerUncompressedSum opens each non-nil body via parquet-go's
// footer parser and sums per-row-group total_byte_size, which
// the parquet spec defines as the total uncompressed size of all
// column data in the row group. Metadata is parsed once per file
// (~10–100KB of footer bytes); the body is already in memory so
// this is essentially free.
func footerUncompressedSum(p *partState) (int64, error) {
	var total int64
	for fi, body := range p.bodies {
		if body == nil {
			continue
		}
		f, err := parquet.OpenFile(
			bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return 0, fmt.Errorf(
				"s3parquet: open %s: %w", p.files[fi].Key, err)
		}
		for _, rg := range f.Metadata().RowGroups {
			total += rg.TotalByteSize
		}
	}
	return total, nil
}
