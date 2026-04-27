package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
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
// Dedup is per-partition (not global like Read): correct only
// when the partition key strictly determines every component of
// EntityKeyOf so no entity ever spans partitions. For layouts
// that don't satisfy this invariant, use Read or pass WithHistory
// and dedup yourself. Partitions emit in lex order; within a
// partition, records emit in lex/insertion order.
//
// Memory: O(one partition's records) by default. Tune with
// WithReadAheadPartitions (default 1; overlap decode of N+1 with
// yield of N) and/or WithReadAheadBytes (uncompressed-size cap).
//
// Breaking out of the for-range loop cancels in-flight downloads —
// no manual Close. Empty patterns slice yields nothing; a
// malformed pattern surfaces as the iter's first error.
func (s *Reader[T]) ReadIter(
	ctx context.Context, keyPatterns []string, opts ...QueryOption,
) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		scope := s.cfg.Target.metrics.methodScope(ctx, methodReadIter)
		var iterErr error
		defer scope.end(&iterErr)
		var o QueryOpts
		o.Apply(opts...)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		keys, err := ResolvePatterns(
			ctx, s.cfg.Target, keyPatterns, &o)
		if err != nil {
			iterErr = fmt.Errorf("s3store: ReadIter %w", err)
			yield(*new(T), iterErr)
			return
		}
		if len(keys) == 0 {
			return
		}

		s.downloadAndDecodeIter(ctx, keys, &o, scope, yield, &iterErr)
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
	opts ...QueryOption,
) iter.Seq2[T, error] {
	// Resolve time bounds outside the closure so the live-tip
	// snapshot freezes at call entry, not at first iteration:
	// without this, a busy writer could keep the walk running
	// indefinitely as the now-SettleWindow cutoff advances.
	var sinceOffset Offset
	if !since.IsZero() {
		sinceOffset = s.OffsetAt(since)
	}
	var untilOffset Offset
	if until.IsZero() {
		settleAt := time.Now().Add(
			-s.cfg.Target.EffectiveSettleWindow())
		untilOffset = s.OffsetAt(settleAt)
	} else {
		untilOffset = s.OffsetAt(until)
	}
	// Poll only honours WithUntilOffset; the snapshot-side opts
	// (WithHistory, WithReadAhead*, WithIdempotentRead) flow into
	// the snapshot-style decode pipeline below, not into Poll.
	pollOpts := []PollOption{WithUntilOffset(untilOffset)}

	return func(yield func(T, error) bool) {
		scope := s.cfg.Target.metrics.methodScope(ctx, methodReadRangeIter)
		var iterErr error
		defer scope.end(&iterErr)
		var o QueryOpts
		o.Apply(opts...)

		// Walk the ref stream into a flat KeyMeta slice. LIST-only
		// (no parquet bodies fetched), so this phase is cheap. Use
		// the LIST page max as the per-Poll cap to minimize round
		// trips — the slice growth here is bounded metadata, not
		// decoded record memory.
		var keys []KeyMeta
		cur := sinceOffset
		for {
			entries, next, err := s.Poll(ctx, cur, s3ListMaxKeys, pollOpts...)
			if err != nil {
				iterErr = err
				yield(*new(T), err)
				return
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
		if len(keys) == 0 {
			return
		}

		keys, err := applyIdempotentReadOpts(keys, s.dataPath, &o)
		if err != nil {
			iterErr = fmt.Errorf("s3store: %w", err)
			yield(*new(T), iterErr)
			return
		}
		if len(keys) == 0 {
			return
		}

		s.downloadAndDecodeIter(ctx, keys, &o, scope, yield, &iterErr)
	}
}

// downloadAndDecodeIter is the byte-budget-aware streaming pipeline backing
// ReadIter and ReadRangeIter. Three concurrent stages plus the
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
// Downloads are continuously in flight regardless of decode pace,
// the budget gate uses exact uncompressed sizes from parquet
// footers rather than partition counts, and a single oversized
// partition still flows (the cap can't bind below partition
// granularity without row-group-level streaming).
func (s *Reader[T]) downloadAndDecodeIter(
	ctx context.Context, keys []KeyMeta,
	opts *QueryOpts, scope *methodScope,
	yield func(T, error) bool, iterErr *error,
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
			s.runDownloader(ctx, jobsCh, state, bodyCap, cancel,
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
			*iterErr = batch.err
			yield(*new(T), batch.err)
			return
		}
		emitted, ok := s.emitPartition(
			batch.recs, opts.IncludeHistory, yield)
		recordsYielded += emitted
		state.releaseBytes(batch.uncompBytes)
		if !ok {
			return
		}
	}
}

// emitPartition yields one partition's records to the consumer
// through sortAndIterate, so the iter paths observe the same
// order / replica-collapse / latest-per-entity semantics as the
// materialised Read paths. Returns the count of records actually
// yielded plus a continue flag — false when the consumer asked
// to stop (yield returned false), so the outer loop can break
// cleanly. Counted records flow into the s3store.read.records
// histogram on the caller's methodScope.
func (s *Reader[T]) emitPartition(
	recs []T, includeHistory bool,
	yield func(T, error) bool,
) (int64, bool) {
	var emitted int64
	for r := range s.sortAndIterate(recs, includeHistory) {
		if !yield(r, nil) {
			return emitted, false
		}
		emitted++
	}
	return emitted, true
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

// groupKeysByPartition splits a flat list of data-file KeyMetas
// into one slice per Hive partition (the path between dataPath
// and the filename). Emission order is decided by the record-
// level sort in emitPartition — groupKeysByPartition itself no
// longer implies chronological-within-partition output.
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
// filesDownloaded counts files with a definitive outcome — body
// fetched OR NoSuchKey (the file was visited, just empty). It is
// NOT incremented on the acquire-slot cancellation path or on
// hard transport errors, so the metric reflects "files we
// genuinely visited," not "files we tried to visit." See
// downloadAndDecodeIter for how this is surfaced on the scope.
func (s *Reader[T]) runDownloader(
	ctx context.Context, jobsCh <-chan downloadJob,
	state *streamState, bodyCap int,
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
				if s.cfg.OnMissingData != nil {
					s.cfg.OnMissingData(key)
				}
				filesDownloaded.Add(1)
				state.markComplete(job.partIdx, job.fileIdx, nil, nil)
				continue
			}
			state.markComplete(job.partIdx, job.fileIdx, nil,
				fmt.Errorf("s3store: get %s: %w", key, err))
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
	opts *QueryOpts, decodedCh chan<- decodedBatch[T],
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

		decodeStart := time.Now()
		recs, err := s.decodePartition(state, ps, totalRows)
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
				"s3store: decode %s: %w", ps.files[fi].Key, err)
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
	files     []KeyMeta
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
				"s3store: open %s: %w", p.files[fi].Key, openErr)
		}
		for _, rg := range f.Metadata().RowGroups {
			uncomp += rg.TotalByteSize
			totalRows += rg.NumRows
		}
	}
	return uncomp, totalRows, nil
}
