package s3parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
	"github.com/ueisele/s3store/internal/core"
)

// streamEager is the byte-budget-aware streaming pipeline backing
// ReadIter. Three concurrent stages plus the caller's yield loop:
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
// Downloads are continuously in flight regardless of decode pace,
// the budget gate uses exact uncompressed sizes from parquet
// footers rather than partition counts, and a single oversized
// partition still flows (the cap can't bind below partition
// granularity without row-group-level streaming).
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
	// bodyCap bounds the in-memory compressed-body footprint:
	// downloaders block before fetching the next file once cap
	// slots are held; the decoder releases slots as it nils each
	// body. Floor at the largest partition's file count so a
	// single oversized partition still fits in the pool —
	// otherwise its last few files would block on the cap and
	// the decoder would block on those files, producing a
	// deadlock.
	bodyCap := concurrency
	for _, p := range parts {
		if n := len(p.files); n > bodyCap {
			bodyCap = n
		}
	}

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
			s.runDownloader(ctx, jobsCh, state, bodyCap, cancel)
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
	// pipeline buffers up to N decoded partitions ahead. Floor at
	// 1 so the default (zero value) gets one-partition lookahead —
	// decode of partition N+1 overlaps yield of partition N, the
	// minimum useful pipeline shape. To bound stacking when N>1,
	// use WithReadAheadBytes.
	readAheadParts := max(1, opts.ReadAheadPartitions)
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
// acquires one body-pool slot (back-pressuring the producer when
// the pool is full), fetches the parquet body, and stores it in
// the per-partition slot. The slot stays held until the decoder
// nils the body. NoSuchKey and hard errors release the slot
// immediately since no body is materialised.
func (s *Reader[T]) runDownloader(
	ctx context.Context, jobsCh <-chan downloadJob,
	state *streamState, bodyCap int,
	cancel context.CancelFunc,
) {
	for job := range jobsCh {
		if !state.acquireBodySlot(ctx, bodyCap) {
			state.markComplete(job.partIdx, job.fileIdx, nil, ctx.Err())
			continue
		}
		key := state.parts[job.partIdx].files[job.fileIdx].Key
		body, err := s.cfg.Target.get(
			ctx, key, s.cfg.ConsistencyControl)
		if err != nil {
			// No body materialised — return the slot.
			state.releaseBodySlots(1)
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
		// Slot stays held; decoder releases it when bodies are
		// nil'd in decodePartition.
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

		// Parse footers once: exact uncompressed total for the
		// byte budget AND total row count for pre-allocating the
		// decoded slice. Missing files (nil body) contribute zero.
		uncomp, totalRows, err := footerStats(ps)
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

		recs, err := s.decodePartition(state, ps, totalRows)
		// decodePartition nils each body + releases its body-pool
		// slot per-file; just clear the errs slice here.
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
// ps and returns the concatenated records. Files that were
// missing on download (body == nil, err == nil) are skipped —
// OnMissingData was already invoked by the downloader.
//
// Each body is nil'd and its body-pool slot released as soon as
// the file is decoded, so the compressed-byte footprint inside
// a single partition's decode shrinks to ~one body instead of
// holding every file's compressed bytes for the full loop.
//
// The output slice is pre-sized to totalRows (summed from
// row-group metadata in footerStats) so growth-doubling doesn't
// inflate the transient allocation peak.
func (s *Reader[T]) decodePartition(
	state *streamState, ps *partState, totalRows int64,
) ([]T, error) {
	out := make([]T, 0, totalRows)
	for fi, body := range ps.bodies {
		if body == nil {
			continue
		}
		recs, err := decodeParquet[T](body)
		// Free the body and return its slot regardless of decode
		// outcome — we're done with it either way.
		ps.bodies[fi] = nil
		state.releaseBodySlots(1)
		if err != nil {
			return nil, fmt.Errorf(
				"s3parquet: decode %s: %w", ps.files[fi].Key, err)
		}
		out = append(out, recs...)
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
// the in-memory compressed-body counter, and the cond var used
// to signal across stages (download completion, body slot
// release, decoded-byte release, ctx cancellation).
type streamState struct {
	mu                sync.Mutex
	cond              *sync.Cond
	parts             []*partState
	bufferedBytes     int64
	outstandingBodies int
}

// acquireBodySlot reserves one slot in the compressed-body pool
// against cap. Blocks while the pool is full and ctx is alive.
// Returns false if ctx is cancelled while waiting. cap == 0
// disables the cap (no back-pressure).
//
// The pool counts compressed parquet bodies that downloaders
// have stored into per-partition slots and the decoder has not
// yet cleared. It bounds the worst-case compressed-byte
// footprint of the pipeline to roughly cap × largest_compressed_size.
func (s *streamState) acquireBodySlot(
	ctx context.Context, cap int,
) bool {
	if cap <= 0 {
		return ctx.Err() == nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.outstandingBodies >= cap {
		if ctx.Err() != nil {
			return false
		}
		s.cond.Wait()
	}
	s.outstandingBodies++
	return true
}

// releaseBodySlots returns n slots to the pool and wakes any
// blocked downloader (or the watchdog if ctx fires concurrently).
func (s *streamState) releaseBodySlots(n int) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	s.outstandingBodies -= n
	s.cond.Broadcast()
	s.mu.Unlock()
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
	recs        []T
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

// footerStats opens each non-nil body via parquet-go's footer
// parser and returns the partition's totals: uncompressed bytes
// (per-row-group total_byte_size, which the parquet spec defines
// as the total uncompressed size of all column data in the row
// group) and total row count. Metadata is parsed once per file
// (~10–100KB of footer bytes); the body is already in memory so
// this is essentially free.
//
// The uncompressed total drives the byte-budget gate; the row
// count drives pre-sizing of the decoded slice so its growth
// doesn't double-allocate at decode time.
func footerStats(p *partState) (uncomp, totalRows int64, err error) {
	for fi, body := range p.bodies {
		if body == nil {
			continue
		}
		f, openErr := parquet.OpenFile(
			bytes.NewReader(body), int64(len(body)))
		if openErr != nil {
			return 0, 0, fmt.Errorf(
				"s3parquet: open %s: %w", p.files[fi].Key, openErr)
		}
		for _, rg := range f.Metadata().RowGroups {
			uncomp += rg.TotalByteSize
			totalRows += rg.NumRows
		}
	}
	return uncomp, totalRows, nil
}
