package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/parquet-go/parquet-go"
)

// ReadIter returns an iter.Seq2[T, error] yielding records one
// at a time, streaming partition-by-partition. Use when Read's
// O(records) memory is a problem.
//
// Dedup is per-partition (uniform across every read path now):
// correct only when the partition key strictly determines every
// component of EntityKeyOf so no entity ever spans partitions.
// For layouts that don't satisfy this invariant, pass WithHistory
// and dedup yourself.
//
// Partitions emit in lex order. Within a partition, record order
// depends on dedup configuration: when EntityKeyOf is set,
// records are in (entity, version) ascending order — last-wins
// on tied versions for default dedup, first-wins per
// (entity, version) group for WithHistory replica dedup. When
// EntityKeyOf is nil (no dedup configured), records emit in
// decode order: file lex order, then parquet row order within
// each file.
//
// Memory: O(one partition's records) by default. Tune with
// WithReadAheadPartitions (default 1; overlap decode of N+1 with
// yield of N) and/or WithReadAheadBytes (uncompressed-size cap).
//
// Breaking out of the for-range loop cancels in-flight downloads —
// no manual Close. Empty patterns slice yields nothing; a
// malformed pattern surfaces as the iter's first error.
func (s *Reader[T]) ReadIter(
	ctx context.Context, keyPatterns []string, opts ...ReadOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		scope := s.cfg.Target.metrics.methodScope(ctx, methodReadIter)
		var iterErr error
		defer scope.end(&iterErr)
		var o readOpts
		o.apply(opts...)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		keys, err := ResolvePatterns(
			ctx, s.cfg.Target, keyPatterns, methodReadIter)
		if err != nil {
			iterErr = fmt.Errorf("ReadIter: %w", err)
			yield(*new(T), iterErr)
			return
		}
		if len(keys) == 0 {
			return
		}

		s.downloadAndDecodeIter(ctx, keys, &o, scope,
			s.recordEmit(yield, &iterErr))
	}
}

// ReadRangeIter streams every record written in the [since, until)
// time window as an iter.Seq2[T, error]. Snapshot view of the ref
// stream over a wall-clock range — no offset cursor, no resume.
//
// Zero time.Time on either bound means unbounded: since=zero starts
// at the stream head, until=zero walks to the live tip
// (now - SettleWindow, captured at call entry so the upper bound
// stays stable under concurrent writes). Pair with non-zero values
// for time-windowed reads:
//
//	start := time.Date(2026, 4, 17, 0, 0, 0, 0, time.UTC)
//	end   := time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC)
//	for r, err := range store.ReadRangeIter(ctx, start, end) { ... }
//
// Same per-partition dedup as ReadIter (default latest-per-entity
// per partition; pass WithHistory to opt out). Memory bounded by
// WithReadAheadPartitions / WithReadAheadBytes.
//
// The ref-LIST runs upfront before the first record yields — usually
// sub-100ms but huge windows can take seconds; chunk via since/until.
// Breaking out of the loop cancels in-flight downloads. Errors are
// yielded as (zero, err) and terminate the iter.
//
// Does NOT expose per-batch offsets — consumer aborts cannot safely
// resume. Use PollRecords (Kafka-style cursor) when you need to
// checkpoint between batches.
func (s *Reader[T]) ReadRangeIter(
	ctx context.Context,
	since, until time.Time,
	opts ...ReadOption,
) iter.Seq2[T, error] {
	// Resolve time bounds outside the closure so the live-tip
	// snapshot freezes at call entry, not at first iteration —
	// see resolveRangeBounds.
	sinceOffset, untilOffset := s.resolveRangeBounds(since, until)

	return func(yield func(T, error) bool) {
		scope := s.cfg.Target.metrics.methodScope(ctx, methodReadRangeIter)
		var iterErr error
		defer scope.end(&iterErr)
		var o readOpts
		o.apply(opts...)

		keys, err := s.walkRangeKeys(ctx, sinceOffset, untilOffset)
		if err != nil {
			iterErr = err
			yield(*new(T), err)
			return
		}
		if len(keys) == 0 {
			return
		}

		s.downloadAndDecodeIter(ctx, keys, &o, scope,
			s.recordEmit(yield, &iterErr))
	}
}

// resolveRangeBounds converts a [since, until) wall-clock window
// into stream Offset bounds. Pure computation — no I/O. Captures
// the live-tip snapshot at call time when until is zero, freezing
// the upper bound so a busy writer can't keep extending the walk
// as time advances.
//
// Called outside the iter closure by ReadRangeIter /
// ReadPartitionRangeIter so the freeze happens at iter-construction
// time, not at iter-start time. Pair with walkRangeKeys, which
// performs the actual ref LIST.
func (s *Reader[T]) resolveRangeBounds(
	since, until time.Time,
) (sinceOffset, untilOffset Offset) {
	if !since.IsZero() {
		sinceOffset = s.OffsetAt(since)
	}
	if until.IsZero() {
		settleAt := time.Now().Add(
			-s.cfg.Target.SettleWindow())
		untilOffset = s.OffsetAt(settleAt)
	} else {
		untilOffset = s.OffsetAt(until)
	}
	return sinceOffset, untilOffset
}

// walkRangeKeys walks the ref stream between sinceOffset and
// untilOffset (resolved upfront by resolveRangeBounds) into a
// flat list of data-file KeyMetas. LIST-only — no parquet bodies
// fetched, so this phase is cheap. Uses the LIST page max as the
// per-Poll cap to minimize round trips. Slice growth here is
// bounded metadata, not decoded record memory.
//
// Used by ReadRangeIter / ReadPartitionRangeIter. Caller is
// responsible for setting iterErr and yielding the error to the
// consumer on a non-nil return.
func (s *Reader[T]) walkRangeKeys(
	ctx context.Context, sinceOffset, untilOffset Offset,
) ([]KeyMeta, error) {
	pollOpts := []PollOption{WithUntilOffset(untilOffset)}
	var keys []KeyMeta
	cur := sinceOffset
	for {
		entries, next, err := s.Poll(ctx, cur, s3ListMaxKeys, pollOpts...)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			keys = append(keys, KeyMeta{
				Key:        e.DataPath,
				InsertedAt: e.InsertedAt,
			})
		}
		cur = next
	}
	return keys, nil
}

// recordEmit returns the per-batch emit callback that flattens
// each partition's already-dedup'd records into the consumer's
// iter.Seq2[T, error] yield. Used by ReadIter / ReadRangeIter —
// paths that surface records one at a time.
//
// On a hard pipeline error: sets *iterErr, yields (zero T, err)
// once, returns (0, false) so the emit loop terminates and
// scope.end picks up iterErr for outcome classification.
//
// On success: delegates to emitPartition for the record-at-a-
// time yield + early-break handling.
func (s *Reader[T]) recordEmit(
	yield func(T, error) bool, iterErr *error,
) func(string, []T, error) (int64, bool) {
	return func(_ string, recs []T, err error) (int64, bool) {
		if err != nil {
			*iterErr = err
			yield(*new(T), err)
			return 0, false
		}
		return s.emitPartition(recs, yield)
	}
}

// downloadAndDecodeIter is the byte-budget-aware streaming pipeline backing
// ReadIter / ReadRangeIter / ReadPartitionIter / ReadPartitionRangeIter.
// Three concurrent stages plus the caller's emit loop:
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
//     records, sort+dedup's them in-place, and pushes a
//     decodedBatch to the emitter.
//
//  4. Emit loop (this goroutine): pulls decoded partitions in
//     order and forwards each to the per-method emit callback —
//     record-by-record yield (ReadIter / ReadRangeIter) or one
//     HivePartition[T] per partition (ReadPartitionIter /
//     ReadPartitionRangeIter). Frees the partition's reserved
//     bytes on completion so the decoder can proceed.
//
// emit is invoked once per decodedBatch; the callback owns the
// caller-side iter.Seq2 yield + iterErr bookkeeping. On a hard
// pipeline error, decoder sends decodedBatch{err: err} and the
// emit callback receives (partKey="", recs=nil, err=non-nil) —
// it should yield the error to the consumer, set iterErr, and
// return (0, false). On success, emit returns (records-yielded,
// keep-going); a false ok aborts the loop.
//
// Downloads are continuously in flight regardless of decode pace,
// the budget gate uses exact uncompressed sizes from parquet
// footers rather than partition counts, and a single oversized
// partition still flows (the cap can't bind below partition
// granularity without row-group-level streaming).
func (s *Reader[T]) downloadAndDecodeIter(
	ctx context.Context, keys []KeyMeta,
	opts *readOpts, scope *methodScope,
	emit func(partKey string, recs []T, err error) (int64, bool),
) {
	if len(keys) == 0 {
		return
	}

	parts := s.preparePartitions(keys)
	if len(parts) == 0 {
		return
	}

	// Records / bytes / files counters threaded through the
	// pipeline; flushed onto the caller's scope before return so
	// the deferred end sees totals. Each counter reflects work
	// that actually committed: filesDownloaded is incremented by
	// the downloader on every file with a definitive outcome
	// (body fetched OR NoSuchKey — both are "visited"), skipping
	// hard errors and ctx-cancel; bytesDownloaded sums received
	// body bytes; recordsYielded counts records the consumer
	// actually saw post-dedup. Partial-success error paths thus
	// surface real progress rather than the work plan.
	totalFiles := 0
	for _, p := range parts {
		totalFiles += len(p.files)
	}
	var recordsYielded int64
	var bytesDownloaded, filesDownloaded atomic.Int64
	defer func() {
		scope.addRecords(recordsYielded)
		scope.addBytes(bytesDownloaded.Load())
		scope.addFiles(filesDownloaded.Load())
	}()

	ctx, cancel := context.WithCancel(ctx)
	concurrency := s.cfg.Target.EffectiveMaxInflightRequests()
	// The iter pipeline spawns exactly `concurrency` worker
	// goroutines unconditionally (not via fanOut), so workers ==
	// concurrency by construction. Items = totalFiles. Recording
	// directly mirrors what fanOut would record for this shape.
	s.cfg.Target.metrics.recordFanout(ctx, totalFiles, concurrency)
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
	// uncompressed byte total + metrics handle for the wait-time
	// observations that acquireBodySlot / reserveBytes emit.
	state := &streamState{
		parts: parts,
		m:     s.cfg.Target.metrics,
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
			s.runDownloader(ctx, jobsCh, state, bodyCap, scope, cancel,
				&bytesDownloaded, &filesDownloaded)
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
	// pipeline buffers up to N decoded partitions ahead. The
	// pointer-typed option distinguishes "not supplied" (nil →
	// default 1, the minimum useful pipeline shape — decode of
	// partition N+1 overlaps yield of partition N) from "explicit
	// zero" (cap=0, unbuffered handoff). To bound stacking when
	// N>1, combine with WithReadAheadBytes.
	readAheadParts := 1
	if opts.readAheadPartitions != nil {
		readAheadParts = *opts.readAheadPartitions
	}
	decodedCh := make(chan decodedBatch[T], readAheadParts)
	wg.Go(func() { s.runDecoder(ctx, state, opts, decodedCh) })

	// Stage 4: emit loop. Drains decodedCh, hands each batch to
	// the per-method emit callback (record-by-record yield or
	// HivePartition[T] yield), signals on each completed partition
	// so the decoder can release the byte-budget reservation.
	for batch := range decodedCh {
		emitted, ok := emit(batch.partitionKey, batch.recs, batch.err)
		recordsYielded += emitted
		state.releaseBytes(batch.uncompBytes)
		if !ok {
			return
		}
	}
}

// emitPartition yields one partition's already-dedup'd records
// to the consumer. Sort+dedup runs upstream in decodePartition
// so the iter paths observe the same order / replica-collapse /
// latest-per-entity semantics as the materialised Read paths
// without paying for it on the yield-loop hot path. Returns the
// count of records actually yielded plus a continue flag — false
// when the consumer asked to stop (yield returned false), so the
// outer loop can break cleanly. Counted records flow into the
// s3store.read.records histogram on the caller's methodScope.
func (s *Reader[T]) emitPartition(
	recs []T,
	yield func(T, error) bool,
) (int64, bool) {
	var emitted int64
	for _, r := range recs {
		if !yield(r, nil) {
			return emitted, false
		}
		emitted++
	}
	return emitted, true
}

// sortKeyMetasByKey orders a partition's files by their S3 key
// for deterministic download order. Used by preparePartitions to
// pick a stable in-partition ordering before the pipeline fetches
// bodies; user-visible emission order is then decided by
// decodePartition's sortAndDedup on record content.
func sortKeyMetasByKey(files []KeyMeta) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Key < files[j].Key
	})
}

// preparePartitions groups the LIST result by Hive partition,
// sorts partition keys lex, sorts files within each partition
// by S3 key (deterministic download order), and allocates the
// per-partition slots.
func (s *Reader[T]) preparePartitions(
	keys []KeyMeta,
) []*partState {
	byPartition := s.groupKeysByPartition(keys)
	if len(byPartition) == 0 {
		return nil
	}
	partitionKeys := make([]string, 0, len(byPartition))
	for k := range byPartition {
		partitionKeys = append(partitionKeys, k)
	}
	// Public contract: partition emission is lex-ordered. Every
	// read path (Read / ReadIter / ReadPartitionIter /
	// ReadRangeIter / ReadPartitionRangeIter / PollRecords) flows
	// through this sort. Removing it surfaces Go's randomized
	// map iteration order to the consumer and breaks
	// byte-for-byte stable output across calls — see
	// "Deterministic emission order across read paths" in
	// CLAUDE.md.
	slices.Sort(partitionKeys)
	parts := make([]*partState, len(partitionKeys))
	for i, p := range partitionKeys {
		files := byPartition[p]
		sortKeyMetasByKey(files)
		parts[i] = &partState{
			partitionKey: p,
			files:        files,
			bodies:       make([][]byte, len(files)),
			errs:         make([]error, len(files)),
		}
	}
	return parts
}

// groupKeysByPartition splits a flat list of data-file KeyMetas
// into one slice per Hive partition (the path between dataPath
// and the filename). Within-partition emission order is decided
// downstream by decodePartition's sortAndDedup on record content
// — groupKeysByPartition itself does not impose any record
// ordering.
func (s *Reader[T]) groupKeysByPartition(
	keys []KeyMeta,
) map[string][]KeyMeta {
	out := make(map[string][]KeyMeta)
	for _, k := range keys {
		hk, ok := hiveKeyOfDataFile(k.Key, s.dataPath)
		if !ok {
			// Defensively skip keys that don't parse. List paths
			// already filtered to .parquet, so reaching here means
			// a layout corruption — drop, don't error.
			continue
		}
		out[hk] = append(out[hk], k)
	}
	return out
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
//
// scope drives the missing-data policy via its method: tolerant
// methods (ReadRangeIter) log + record the missing-data metric
// and mark the file as visited so the iter continues; strict
// methods (ReadIter) propagate the NoSuchKey as a wrapped error
// so the caller's retry resolves it. See
// methodTolerantOfMissingData.
//
// filesDownloaded counts files with a definitive outcome — body
// fetched OR tolerated NoSuchKey (the file was visited, just
// empty). It is NOT incremented on the acquire-slot cancellation
// path, hard transport errors, or strict NoSuchKey, so the metric
// reflects "files we genuinely visited," not "files we tried to
// visit." See downloadAndDecodeIter for how this is surfaced on
// the scope.
func (s *Reader[T]) runDownloader(
	ctx context.Context, jobsCh <-chan downloadJob,
	state *streamState, bodyCap int, scope *methodScope,
	cancel context.CancelFunc,
	bytesDownloaded, filesDownloaded *atomic.Int64,
) {
	for job := range jobsCh {
		if !state.acquireBodySlot(ctx, bodyCap) {
			state.markComplete(job.partIdx, job.fileIdx, nil, ctx.Err())
			continue
		}
		key := state.parts[job.partIdx].files[job.fileIdx].Key
		body, err := s.cfg.Target.get(ctx, key)
		if err != nil {
			// No body materialised — return the slot.
			state.releaseBodySlots(1)
			if _, ok := errors.AsType[*s3types.NoSuchKey](err); ok {
				if methodTolerantOfMissingData(scope.method) {
					slog.Warn("s3store: data file missing, skipping",
						"path", key, "method", string(scope.method))
					scope.recordMissingData()
					filesDownloaded.Add(1)
					state.markComplete(job.partIdx, job.fileIdx, nil, nil)
					continue
				}
				state.markComplete(job.partIdx, job.fileIdx, nil,
					fmt.Errorf("get %s: %w", key, err))
				cancel()
				continue
			}
			state.markComplete(job.partIdx, job.fileIdx, nil,
				fmt.Errorf("get %s: %w", key, err))
			cancel()
			continue
		}
		filesDownloaded.Add(1)
		// Slot stays held; decoder releases it when bodies are
		// nil'd in decodePartition.
		bytesDownloaded.Add(int64(len(body)))
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
	opts *readOpts, decodedCh chan<- decodedBatch[T],
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
		if !state.reserveBytes(ctx, uncomp, opts.readAheadBytes) {
			return
		}

		decodeStart := time.Now()
		recs, err := s.decodePartition(state, ps, totalRows,
			opts.includeHistory)
		state.m.recordIterDecodeDuration(ctx, time.Since(decodeStart))
		// decodePartition nils each body + releases its body-pool
		// slot per-file; just clear the errs slice here.
		ps.errs = nil
		if err != nil {
			state.releaseBytes(uncomp)
			sendBatch(ctx, decodedCh, decodedBatch[T]{err: err})
			return
		}

		if !sendBatch(ctx, decodedCh, decodedBatch[T]{
			partitionKey: ps.partitionKey,
			recs:         recs,
			uncompBytes:  uncomp,
		}) {
			state.releaseBytes(uncomp)
			return
		}
	}
}

// decodePartition parses every successfully-downloaded body in
// ps, sort+dedup's the concatenated records, and returns the
// final slice. Files that were missing on download (body == nil,
// err == nil) are skipped — the downloader already logged via
// slog.Warn and incremented the s3store.read.missing_data
// counter.
//
// Each body is nil'd and its body-pool slot released as soon as
// the file is decoded, so the compressed-byte footprint inside
// a single partition's decode shrinks to ~one body instead of
// holding every file's compressed bytes for the full loop.
//
// The pre-dedup slice is pre-sized to totalRows (summed from
// row-group metadata in footerStats) so growth-doubling doesn't
// inflate the transient allocation peak. sortAndDedup compacts
// in-place and returns out[:n] — same backing array, length
// truncated to the survivor count. includeHistory selects
// replica-only dedup over latest-per-entity (see sortAndDedup).
func (s *Reader[T]) decodePartition(
	state *streamState, ps *partState, totalRows int64,
	includeHistory bool,
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
				"decode %s: %w", ps.files[fi].Key, err)
		}
		out = append(out, recs...)
	}
	return s.sortAndDedup(out, includeHistory), nil
}

// downloadJob is one (partition, file) tuple flowing through
// the producer → downloader queue.
type downloadJob struct {
	partIdx int
	fileIdx int
}

// partState holds per-partition download progress. partitionKey
// and files are fixed at preparePartitions time; bodies + errs +
// completed are mutated by downloaders under streamState.mu.
// partitionKey is the Hive partition key ("period=X/customer=Y")
// that runDecoder forwards onto decodedBatch so the emit
// callback can surface it to partition-emitting public methods.
type partState struct {
	partitionKey string
	files        []KeyMeta
	bodies       [][]byte
	errs         []error
	completed    int
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
//
// m is the optional metrics handle. acquireBodySlot and
// reserveBytes report wait duration via metrics.recordIterBodySlotWait
// / recordIterByteBudgetWait when the call blocked and ended in
// success, so operators can see body-slot pool / byte-budget
// contention. Cancel-during-wait is not recorded (shutdown noise).
type streamState struct {
	mu                sync.Mutex
	cond              *sync.Cond
	parts             []*partState
	bufferedBytes     int64
	outstandingBodies int
	m                 *metrics
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
//
// Records to metrics.recordIterBodySlotWait only when the wait
// fired (cond.Wait at least once) AND the slot was eventually
// acquired — cancel-during-wait is intentionally not recorded
// (shutdown noise, near-zero duration would drown out the
// saturation signal).
//
// The wait duration spans all wait iterations, so a thrashing
// path records one cumulative observation rather than many
// fragments. The metric record fires after s.mu is unlocked
// (defer LIFO ordering) to keep the pipeline's hot-path lock
// free of OTel work.
func (s *streamState) acquireBodySlot(
	ctx context.Context, cap int,
) bool {
	if cap <= 0 {
		return ctx.Err() == nil
	}
	var waitDur time.Duration
	s.mu.Lock()
	defer func() {
		// Runs after the s.mu.Unlock below (LIFO defers) so the
		// metric record happens lock-free.
		if waitDur > 0 {
			s.m.recordIterBodySlotWait(ctx, waitDur)
		}
	}()
	defer s.mu.Unlock()

	var waitStart time.Time
	waited := false
	for s.outstandingBodies >= cap {
		if ctx.Err() != nil {
			// Cancel path: do NOT set waitDur — we only record
			// successful acquires.
			return false
		}
		if !waited {
			waitStart = time.Now()
			waited = true
		}
		s.cond.Wait()
	}
	if waited {
		waitDur = time.Since(waitStart)
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
//
// Records to metrics.recordIterByteBudgetWait only when the wait
// fired AND the reservation succeeded — same shape as
// acquireBodySlot, cancel path is not recorded.
func (s *streamState) reserveBytes(
	ctx context.Context, uncomp, cap int64,
) bool {
	if cap <= 0 || uncomp <= 0 {
		return ctx.Err() == nil
	}
	var waitDur time.Duration
	s.mu.Lock()
	defer func() {
		if waitDur > 0 {
			s.m.recordIterByteBudgetWait(ctx, waitDur)
		}
	}()
	defer s.mu.Unlock()

	var waitStart time.Time
	waited := false
	for s.bufferedBytes > 0 && s.bufferedBytes+uncomp > cap {
		if ctx.Err() != nil {
			// Cancel path: do NOT set waitDur — only successful
			// reservations are recorded.
			return false
		}
		if !waited {
			waitStart = time.Now()
			waited = true
		}
		s.cond.Wait()
	}
	if waited {
		waitDur = time.Since(waitStart)
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
// partitionKey is the Hive partition the records came from
// (carried so partition-emitting public methods can surface it).
// recs is already sort+dedup'd by decodePartition. uncompBytes
// is what the decoder reserved; the yield loop returns it via
// releaseBytes after the records are forwarded.
type decodedBatch[T any] struct {
	partitionKey string
	recs         []T
	uncompBytes  int64
	err          error
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
				"open %s: %w", p.files[fi].Key, openErr)
		}
		for _, rg := range f.Metadata().RowGroups {
			uncomp += rg.TotalByteSize
			totalRows += rg.NumRows
		}
	}
	return uncomp, totalRows, nil
}
