# Using s3store on NetApp StorageGRID

s3store's correctness on AWS S3 and MinIO is automatic — both
backends are strongly consistent on LIST and GET out of the box,
and `ConsistencyControl` can stay at its zero value. **On NetApp
StorageGRID it is not automatic.** This document covers everything
the library needs from the StorageGRID side: what to configure,
how to apply it, and how to choose the consistency level for your
deployment topology.

The [Appendix](#appendix-why-this-configuration) at the bottom
explains *why* — what each consistency level means for every call
the library makes, where the docs are explicit, where they're
silent, and how s3store's correctness reasoning maps onto the
documented mechanism.

[sgcc]: https://docs.netapp.com/us-en/storagegrid-119/s3/consistency-controls.html
[sgcwl]: https://docs.netapp.com/us-en/storagegrid-119/s3/conflicting-client-requests.html
[sg-meta]: https://docs.netapp.com/us-en/storagegrid/admin/managing-object-metadata-storage.html
[sg-12]: https://docs.netapp.com/us-en/storagegrid/release-notes/whats-new.html
[sg-quorum-kb]: https://kb.netapp.com/hybrid/StorageGRID/Object_Mgmt/Configuring_StorageGRID_quorum_semantics_for_strong-global_consistency
[sg-ops]: https://docs.netapp.com/us-en/storagegrid-119/s3/operations-on-buckets.html

> **Scope: metadata visibility, not durability.** Every guarantee
> here is about **when a freshly-PUT object becomes findable** by
> a subsequent GET / HEAD / LIST. **None of it is about data
> durability.** Replica count, erasure coding, geographic
> placement, and write durability are all governed by **ILM
> rules** and are orthogonal to the `Consistency-Control` header.
> A `strong-global` PUT is not "more durable" than a
> `read-after-new-write` PUT — it just propagates the metadata
> more widely before returning success. The 11.9 docs cover [the
> consistency-vs-ILM interaction explicitly][sgcc].

## Required configuration

One thing is required:

- **`ConsistencyControl`** on the `S3Target` set to one of the
   stronger levels (`strong-global` or `strong-site` — see
   [Choosing the consistency level](#choosing-the-consistency-level)
   for which one). The level applies to every paired LIST / PUT /
   GET / HEAD the library issues, so writer and reader observe
   the same metadata view (NetApp's "same consistency for paired
   operations" rule).

> **No `s3:PutOverwriteObject` deny needed.** Earlier versions
> of the library required a bucket policy denying
> `s3:PutOverwriteObject` on the `data/` subtree to make
> idempotent retries route into a HEAD-then-LIST dedup path.
> Phase 4 of the commit-marker work replaced that mechanism
> with **per-attempt paths**: every retry under
> `WithIdempotencyToken` writes to a fresh path
> (`{token}-{tsMicros}-{shortID}`), so no PUT in the write path
> ever overwrites — sidestepping multi-site StorageGRID's
> eventual-consistency exposure on overwrites. The library's
> upfront LIST under `{partition}/{token}-` is the dedup gate
> instead. Operators who carried this policy from earlier
> versions can drop it; correctness no longer depends on it.

### `ConsistencyControl` on the S3 target

Set the level on the target so every S3 call routed through it
inherits the same value:

```go
target := s3store.NewS3Target(s3store.S3TargetConfig{
    Bucket:            "your-bucket",
    Prefix:            "your-prefix",
    S3Client:          s3Client,
    PartitionKeyParts: []string{"period", "customer"},

    // Multi-site grid with cross-cutting traffic — required.
    ConsistencyControl: s3store.ConsistencyStrongGlobal,

    // Single-site grid, or multi-site grid with co-located
    // reader/writer pairs per site — sufficient.
    // ConsistencyControl: s3store.ConsistencyStrongSite,
})
```

`s3store.Config.ConsistencyControl` forwards onto the target it
builds, so single-Config callers set the level once at the top of
the umbrella. Setting it on the *target* (not on `WriterConfig` /
`ReaderConfig` / `ProjectionDef`) means every S3 call routed through
that target uses one and the same value — NetApp's
"[same consistency for paired operations][sgcc]" rule is enforced
by construction. On AWS / MinIO the header is unknown to the
backend and ignored, so the zero value is correct there.

## Choosing the consistency level

`strong-site` only delivers read-after-write **when the reader
is in the same site as the writer**. `strong-global` (legacy
11.9 / upgraded 12.0) only succeeds **when every site is
available**. The choice is a deployment-topology decision —
two axes, not a tuning knob:

| Topology | Recommended | Correctness rationale | Availability rationale |
|---|---|---|---|
| **Single-site grid** | `strong-site` | One site, so `strong-site` ≡ `strong-global`. | Same fault tolerance — only that one site has to be up either way. |
| **Multi-site grid, every reader/writer pair co-located per site** (Site A's app talks only to Site A's bucket; Site B is DR) | `strong-site` *with caveat* | Safe in steady state. Breaks on **cross-site failover** — if a writer fails over to Site B mid-batch, the upfront-LIST dedup gate in Site B may not yet see the .commit Site A wrote, and an idempotent retry produces a duplicate per-attempt commit (collapsed by reader-side dedup with `EntityKeyOf` + `VersionOf`, but not silently absorbed without it). | **`strong-site` is more available than 11.9 `strong-global`** here. A failure of any non-local site doesn't block local writes; under `strong-global` (11.9) it would. |
| **Multi-site grid with cross-cutting traffic** (any reader/writer pair can land on different sites — global load balancer, readers in different regions than writers) | `strong-global` | Required for correctness. | **In 11.9, the cost is severe: any single site outage causes all writes to fail.** Plan for this — operators of 11.9 multi-site grids should understand strong-global as a *consistency-availability trade*, not a free upgrade over strong-site. |

## Operational notes

### The 11.9 availability cliff

Under legacy strong-global, write availability degrades to the
*minimum* of all sites — if any site is in maintenance or
partitioned, writes fail everywhere. Operationally:

- A site rolling-restart blocks *all* writers globally for the
  duration. Coordinate accordingly, or temporarily downgrade to
  `strong-site` (with the corresponding correctness regression
  for cross-site reads).
- Network partitions between sites cause global write
  unavailability, even for traffic local to a healthy site.
- 12.0 with Quorum semantics fixes this for new installs (3+
  site grids tolerate 1 site failure), but **upgraded grids
  retain 11.9 behavior** until you opt in via [the KB
  article's procedure][sg-quorum-kb].

If your operational reality includes scheduled maintenance
windows or cross-site network risk, factor that into the
choice. There's a real argument for running `strong-site` even
in a multi-site grid if the alternative is "all writes fail
during any site's maintenance" — but you have to accept that
cross-site reads may miss recent writes within the propagation
window. The library can't make this trade-off for you.

### Empirical latency profile

3-site benchmark, 1000-object LIST and 1 MiB PUTs:

- LIST: `read-after-new-write` ≈ 45 ms median, `strong-site` ≈
  `strong-global` ≈ 55 ms median. Both strong-* levels read
  local-site quorum, so latency profiles match.
- PUT (1 MiB, single-stream): `read-after-new-write` ≈
  `strong-site` ≈ 500 ms median (both stay local),
  `strong-global` ≈ 700 ms median. The ~200 ms gap is the
  slowest-site RTT — the price of "every site must respond".

The PUT penalty for `strong-global` is the actionable cost —
combined with the availability constraint above, it's why the
library defaults the field to empty (zero value) rather than
auto-selecting `strong-global`. Operators need to make the
topology + availability decision deliberately.

---

## Appendix: why this configuration

The rest of this document explains the reasoning that motivates
the `ConsistencyControl` requirement above and the per-attempt-paths
rationale that replaced the earlier `s3:PutOverwriteObject` deny
mechanism. **You don't need to read it to use s3store on
StorageGRID.** Use it when you're debugging a consistency-related
symptom, designing a topology where the simple recommendations
above don't obviously apply, or auditing the library's safety
claims against the StorageGRID docs.

### The five consistency levels

Per the StorageGRID 11.9 [docs][sgcc]:

- **`all`** — every storage node receives the metadata
  immediately, or the request fails. Strongest, slowest;
  intolerant of node outages.
- **`strong-global`** — read-after-write *and* list-after-write
  for every client request, across all sites.
- **`strong-site`** — same guarantee, scoped to a single site.
  Cross-site visibility lags.
- **`read-after-new-write`** *(StorageGRID's default)* — for
  **new** objects only: HEAD / GET see the freshly-PUT key (the
  server retries the lookup at increasing consistency, up to
  `strong-global`, on miss). **Overwrites, metadata updates,
  and deletes are eventually consistent** (overwrite propagation
  can take up to 15 days per the docs).
- **`available`** — eventual consistency for everything; HEAD /
  GET don't ladder up. Intended for log buckets or HEAD/GET on
  keys known to be missing.

### Operation × level matrix

The matrix lists S3 operation types — pure storage-layer
semantics, not s3store-specific use cases. **The library never
overwrites under Phase 4 of the commit-marker design** (every
attempt writes to a per-attempt path), so the "PUT (overwrite)"
column applies only when an external operator overwrites a key
out-of-band — not to anything the library does. The previously-
required `s3:PutOverwriteObject` deny is no longer needed (see
the note on per-attempt-paths in [Required configuration](#required-configuration)).

Cells describe the **within-level guarantee** the docs commit
to (paired PUT + GET at the same level). Specific write scope
(`W`) and read scope (`R`) per level are *not* docs-stated —
see [What's documented vs. inferred](#whats-documented-vs-inferred)
for what we do and don't know about the mechanism.

| Operation | `available` | `read-after-new-write` (default) | `strong-site` | `strong-global` (11.9 / upgraded) | `all` |
|---|---|---|---|---|---|
| **PUT (new key)** — first write to a key that doesn't yet exist | weak; reads at any level may not see for a while | within-level paired HEAD/GET reads find the PUT (via the ladder, which always reaches strong-global on miss); LIST has no ladder, so LIST visibility is **eventually consistent** | within-level paired reads find the PUT — read-after-write within a site, no cross-site claim | every site must be available for the PUT to succeed; paired reads find it across all sites | strongest |
| **PUT (overwrite)** — write replacing an existing key. Two parallel writes to the same key both succeed regardless of level — StorageGRID arbitrates with [latest-wins][sgcwl] | eventual | the docs explicitly call out overwrites: *"Overwrites of existing objects, metadata updates, and deletes are eventually consistent. Overwrites generally take seconds or minutes to propagate, but can take up to 15 days."* ([sgcc][sgcc]) | within-site overwrite consistency | every site has the new bytes after success | strongest |
| **GET** — fetch object body | may 404 a freshly-written object | safe for new keys via the ladder; the ladder is documented to *always* reach strong-global on miss before returning 404 ([sgcc][sgcc]), so **strong-global READ is the system's authoritative read**. For overwrites, eventual per the quote above | within-site read-after-write | finds any prior PUT (every site has the data after a strong-global PUT) | strongest |
| **HEAD** — existence / metadata check | same as GET | same as GET (ladder fires *on miss*) | same as GET | same as GET | same as GET |
| **LIST** — enumerate keys under a prefix | misses recent PUTs from other nodes | **eventually consistent** — load-bearing weak spot (see [Why LIST is the load-bearing operation](#why-list-is-the-load-bearing-operation)); the HEAD/GET ladder doesn't apply to LIST, and `read-after-new-write`'s definition is given in GET terms only | within-site list-after-write | within-site list-after-write (every site has the data after a strong-global PUT, so any site's read scope finds it) | strongest |

These S3 operations correspond to s3store call sites as
follows:

- **PUT (new)** — every PUT the library issues. Per-attempt-paths
  make data, ref, and commit-marker writes unique by construction
  (id = `{token}-{tsMicros}-{shortID}` under
  `WithIdempotencyToken`, or `{tsMicros}-{shortID}` for
  token-less writes); projection markers under
  `_projection/{Name}/{col}={value}/m.proj` are byte-identical
  empty objects, so even a recurring write to the same marker
  key is semantically a "new write" (not a content change). No
  PUT in the write path overwrites an existing key.
- **PUT (overwrite)** — only reachable through external operator
  action (e.g. manual S3 console PUT). The library does not
  produce overwrites itself.
- **GET** — parquet body fetch ([reader_read.go](reader_read.go));
  timing-config object fetch on `S3Target` construction
  ([target.go](target.go)).
- **HEAD** — post-data HEAD and post-marker HEAD on the write
  path (server-stamped LM capture + commit-marker timeliness
  verify, [writer_write.go](writer_write.go)); per-ref marker
  HEAD on the change-stream read path
  ([reader_poll.go](reader_poll.go)).
- **LIST** — partition LIST (snapshot reads); projection-marker
  LIST (`ProjectionReader.Lookup`); ref-stream LIST
  (`Poll` / `PollRecords` / `ReadRangeIter`); upfront-LIST dedup
  gate under `{partition}/{token}-` on the write path; all
  funnel through `listEach` ([target.go](target.go)).

### Why LIST is the load-bearing operation

The 11.9 [docs][sgcc] describe the `read-after-new-write` lookup
ladder explicitly in HEAD/GET terms only:

> "When a HEAD or GET operation uses the 'Read-after-new-write'
> consistency, StorageGRID performs the lookup in multiple
> steps... It first looks up the object using a low consistency.
> If that lookup fails, it repeats the lookup at the next
> consistency value until it reaches a consistency equivalent
> to the behavior for strong-global."

The ladder is the **mechanism** by which `read-after-new-write`
delivers its promise — and its existence is evidence that the
underlying metadata propagation is itself **eventual**. If a
freshly-PUT object were always visible at the low consistency
the ladder starts at, no retry-up would be needed. HEAD/GET get
to mask that eventual state; LIST does not. The docs describe
no equivalent ladder for LIST, and the `read-after-new-write`
definition itself uses GET language ("Any GET following a
successfully completed PUT will be able to read the newly
written data"). The conclusion is forced by the documented
mechanism: **LIST under `read-after-new-write` is eventually
consistent.** A LIST routed to a node that hasn't received the
new metadata yet will omit the freshly-PUT key, with no
automatic retry-up. `strong-site` and `strong-global` close
that gap by guaranteeing read-after-write "for all client
requests" — phrasing broad enough to include LIST.

s3store relies on LIST seeing recent PUTs in four places:

1. **Idempotent retry dedup (upfront-LIST gate)** — under
   `WithIdempotencyToken`, the writer LISTs under
   `{partition}/{token}-` to find any prior valid commit. A LIST
   that misses the prior commit produces a duplicate per-attempt
   triple (data + ref + commit marker). Reader-side dedup with
   `EntityKeyOf + VersionOf` collapses the duplicate records, but
   without it the duplicate is visible. Strong list-after-write
   keeps the gate reliable.
2. **`Reader.Read` / `ReadIter`** — partition LIST surfaces
   freshly-written data files for the read-after-write contract.
3. **`ProjectionReader.Lookup`** — LIST under `_projection/{col}={value}/`
   surfaces the marker emitted by the latest write.
4. **`Poll` / `PollRecords` / `ReadRangeIter`** — ref-stream LIST
   advances the consumer's cutoff. Without `strong-*`,
   `SettleWindow` would have to be sized as pure LIST-propagation
   slack; with it, `SettleWindow = CommitTimeout + MaxClockSkew`
   only has to cover the writer's in-flight commit-marker budget
   plus the reader↔server clock-skew bound.

HEAD and GET are *not* the bottleneck — they would be safe on
their own under default thanks to the documented ladder. We send
`strong-*` on them only because StorageGRID requires "[the same
consistency for both the PutObject and GetObject
operations][sgcc]", and the PUT side has to be `strong-*` to
keep its paired LIST consistent.

`all` would also satisfy the invariants but adds a strict-
quorum precondition the library doesn't need: the load-bearing
guarantee is list-after-write, not all-node ack at PUT time.
`read-after-new-write` (default) and `available` are too weak
— both leave LIST visibility unbounded.

### Asymmetric levels don't work (and one accidental exception)

A natural optimization question: can we save cost by mixing
levels — say, LIST at `strong-global` but PUTs at
`read-after-new-write`, or PUTs at `strong-site` but LISTs at
`strong-global`? Walking each setup through the per-site
mechanics:

| Setup (11.9) | What the PUT writes | Where the LIST reads | Verdict |
|---|---|---|---|
| `read-after-new-write` PUT + `strong-global` LIST | minimal local replicas; replication async | local-site quorum | **broken** — even within the writer's site, the PUT might be on too few local replicas to overlap with local quorum |
| `strong-site` PUT + `strong-global` LIST | local-site quorum (writer's site only) | local-site quorum (in any site) | **broken cross-site** — LIST in a non-writer site sees nothing until async replication catches up |
| `strong-global` PUT + `read-after-new-write` LIST | every site has local quorum | 1 replica, no ladder for LIST | **broken** — LIST can land on the 1-of-3 local replica that doesn't have the data |
| `strong-global` PUT + `strong-site` LIST | every site has local quorum | local-site quorum | *safe in 11.9* (every site has the data; local-quorum LIST in any site finds it). NetApp's pairing rule is `PutObject`/`GetObject` only — PUT/LIST asymmetry is technically out-of-scope. **Breaks under 12.0 Quorum** (strong-global PUT no longer guarantees every site has the data); benchmarks show no measurable LIST savings vs. strong-global LIST anyway. Don't ship it. |
| `strong-global` + `strong-global` | every site has local quorum | local-site quorum (works in any site) | safe |
| `strong-site` + `strong-site` (same site) | local-site quorum | local-site quorum | safe within a site only |

The "accidental exception" — `strong-global` PUT +
`strong-site` LIST — is a quirk of the 11.9 EACH_QUORUM
mechanism: because every site is *required* to ack a
strong-global PUT, every site has the data after success, so
any site's local-quorum LIST finds it.

NetApp's [pairing rule][sgcc] is silent on this combination.
The rule's exact wording is *"you must use the same
consistency for both the PutObject and GetObject
operations"* — `GetObject` specifically, not `ListObjects`.
PUT/LIST asymmetry isn't in scope of the rule. So the docs
neither bless nor forbid this configuration; we're operating
on the mechanism alone.

**Don't ship it anyway.** Three concrete reasons:

1. **It breaks on 12.0 with Quorum semantics.** A site that's
   temporarily unavailable will be skipped by the strong-global
   PUT (Quorum = majority of sites, not every site). A
   strong-site LIST in that recovered site won't see the write
   until async replication catches up. The "optimization"
   silently regresses across an upgrade — and the upgrade path
   is a one-way switch from EACH_QUORUM to Quorum that the
   operator may opt into without realizing s3store depended on
   the legacy semantics.
2. **Benchmarks show no measurable savings.** `strong-site`
   LIST and `strong-global` LIST come back at ~55 ms median
   in our 3-site grid — indistinguishable. The cost we're
   trying to chase is on the PUT side, and we're keeping
   strong-global there.
3. **The library couldn't ship it cleanly.**
   `ConsistencyControl` lives on the shared `S3Target` and
   applies uniformly to every routed call. Splitting per-op
   (a hypothetical `ListConsistencyControl` field) would add
   public API surface for a fragile, near-zero-benefit
   optimization.

More generally, asymmetric setups can't help s3store: both
halves of the library issue LISTs that need list-after-write.
The *write* side does the upfront-LIST dedup gate under
`{partition}/{token}-` for idempotent retries; the *read* side
does partition LIST, projection-marker LIST, and ref-stream LIST.
Whichever side you weaken to a non-strong level loses
list-after-write on its own LISTs — independently of any
PUT/GET pairing concern.

### Why per-attempt-paths replaced the `s3:PutOverwriteObject` deny

Earlier versions of the library required a bucket policy
denying `s3:PutOverwriteObject` on the `data/` subtree to make
idempotent retries route into a HEAD-then-LIST dedup branch.
**Phase 4 of the commit-marker design replaced that mechanism
with per-attempt paths.** The motivation is correctness on
multi-site StorageGRID — the deny had two structural problems
that per-attempt-paths sidestep entirely:

#### Concurrent writers always race

StorageGRID resolves overlapping writes to the same key on a
"latest-wins" basis ([sgcwl][sgcwl]):

> "Conflicting client requests, such as two clients writing to
> the same key, are resolved on a 'latest-wins' basis. The
> timing for the 'latest-wins' evaluation is based on when the
> StorageGRID system completes a given request, and not on when
> S3 clients begin an operation."

That means **two parallel writes to the same key both succeed,
on every consistency level**. At the moment each PUT is
authorized, neither has *completed* yet — so neither's existence
check sees an object, and the deny doesn't fire. Latest-wins
arbitrates the bytes after the fact. The deny was a
post-completion gate, not a synchronization primitive — and the
library has no use for a "first writer wins" synchronization
primitive on this path: idempotency tokens are per-(partition,
logical-write) unique, so two concurrent attempts of the same
token writing to the same key would race only across retries
of the same logical work.

#### Read-after-overwrite is eventually consistent on multi-site

Even when the deny fired and a sequential retry routed into the
old HEAD-then-LIST dedup branch, the retry's HEAD on the existing
data file relied on metadata propagation under
`strong-global` — and StorageGRID's docs explicitly call out
that **overwrites** of existing keys are eventually consistent
even at strong levels (*"Overwrites of existing objects,
metadata updates, and deletes are eventually consistent.
Overwrites generally take seconds or minutes to propagate, but
can take up to 15 days."* — [sgcc][sgcc]). A retry whose
auth-layer check missed the prior data file (because cross-site
metadata replication had not yet propagated) would route into
the fresh-write branch, write the body again, and produce a
duplicate.

#### Per-attempt-paths sidestep both

Phase 4's per-attempt id (`{token}-{tsMicros}-{shortID}`) makes
every PUT in the write path a **first write to a new key**. The
docs commit to read-after-new-write at every supported level (the
ladder always reaches `strong-global` on miss), so a post-PUT
HEAD against the data file always sees the bytes the writer just
wrote — no eventual-consistency exposure. The upfront-LIST dedup
gate runs at LIST level (where the strong-* levels deliver
list-after-write within a site / globally), reliably finding any
prior valid commit when one exists. Multi-site StorageGRID
correctness no longer depends on the auth-layer's existence
check seeing a recent overwrite.

The trade is **per-attempt orphans on failure**: every failed
attempt under `WithIdempotencyToken` leaves data + ref +
(possibly) marker on S3 as orphans, where earlier designs would
have produced a single deterministic-path orphan. Reader paths
filter them out via the commit-marker timeliness check; the
deferred operator-driven sweeper reclaims them.

### No per-call consistency overrides

Every S3 call routed through an `S3Target` uses that target's
`ConsistencyControl` — there is no per-method knob. If a future
caller needs a one-off operation at a different level (e.g. an
`available`-level maintenance scan), they can construct a
separate `S3Target` for that path. This enforces NetApp's "same
consistency for paired operations" rule by construction.

### What's documented vs. inferred

The 11.9 docs describe the *outcomes* each level guarantees but
do not document the underlying *mechanism* — and the gaps
matter because operationally the mechanism determines fault
tolerance, latency, and which configurations are safe. We pin
down what the docs commit to, then explicitly mark the rest as
gap.

#### Docs-stated facts

- **HEAD/GET ladder for `read-after-new-write`** ([sgcc][sgcc]):
  on a miss, the lookup retries at increasing levels and
  *"if the object does not exist, the object lookup will always
  reach a consistency equivalent to the behavior for
  strong-global"*. The ladder terminates at strong-global,
  treating it as definitive — implicitly asserting
  **strong-global READ is the system's authoritative read**.
- **PUT/GET pairing rule**: *"You must use the same consistency
  for both the PutObject and GetObject operations."*
  ([sgcc][sgcc])
- **Latest-wins for conflicting writes** ([sgcwl][sgcwl]):
  *"based on when the StorageGRID system completes a given
  request, and not on when S3 clients begin an operation."*
- **`strong-global` in 11.9 (and upgraded 12.0) requires every
  site to be available** for client writes and deletes
  ([sg-quorum-kb][sg-quorum-kb]).
- **12.0 introduces Quorum Strong-Global** with site-failure
  tolerance for 3+ site grids; new installs default to it,
  upgraded grids retain legacy behavior unless an operator opts
  in. ([sg-12][sg-12], [sg-quorum-kb][sg-quorum-kb])
- **LIST accepts the consistency header** ([sg-ops][sg-ops]).
- **Metadata is Cassandra-backed**, 3 replicas per site
  ([sg-meta][sg-meta]).

#### Not docs-stated — multiple models fit

- Exact write quorum (`W`) and read scope (`R`) for
  `available`, `read-after-new-write`, and `strong-site`.
- The mechanism by which strong-global READ achieves its
  definitive-read guarantee.
- Why the pairing rule exists — whether it's a quorum-overlap
  necessity, an internal optimization, or vendor caution.

We've cycled through several plausible models in this
document's history; none is confirmed:

- *`read-after-new-write` writes to local-quorum,
  `strong-site` writes to all local replicas* — making
  strong-site analogous to `all`-within-a-site.
- *`read-after-new-write` writes to one replica with the read
  ladder closing the gap*; this requires the ladder's top rung
  to query enough replicas to find any single-replica write,
  which (under naive Cassandra-style quorum) doesn't quite work
  unless the read scope is `ALL`.
- *Hash-based deterministic placement* (e.g., the primary
  replica derives from `hash(prefix) % N`); reads at any level
  know which primary to query, escalating to a quorum only on
  primary unavailability.
- *Some StorageGRID-specific mechanism* that doesn't map
  cleanly to standard Cassandra semantics.

The docs don't distinguish between these. **Empirical
benchmarks against a real 11.9 grid** can confirm two things:

- PUT: `read-after-new-write` ≈ `strong-site` ≈ 500 ms (both
  stay local); `strong-global` ≈ 700 ms (~one inter-site RTT
  added). The cross-site delta is consistent with EACH_QUORUM
  on `strong-global` writes.
- LIST: `read-after-new-write` ≈ 45 ms; `strong-site` ≈
  `strong-global` ≈ 55 ms — strong levels indistinguishable on
  the LIST side.

The benchmark distinguishes *strong-global* from the others
clearly, but cannot distinguish *`read-after-new-write`* from
*`strong-site`* on the write path — the two have
indistinguishable medians, consistent with several of the
candidate models.

#### Inferences specific to s3store's correctness reasoning

The library's only structural dependency on a docs-stated fact
is **list-after-write at the strong levels**: the upfront-LIST
dedup gate, projection-marker LIST, ref-stream LIST, and
partition LIST under `Reader.Read` all need it. Strong-* is
documented to provide list-after-write within its scope (within
a site for `strong-site`, across all sites for `strong-global`).
Per-attempt paths remove the previous design's dependency on
read-after-overwrite and on the auth-layer's existence check
for the conditional PUT — both of which had documented
eventual-consistency exposure on multi-site grids.

That leaves one inference for s3store-specific behaviour:

1. **LIST under `read-after-new-write` is eventually
   consistent.** *Evidence:* the HEAD/GET ladder is documented
   HEAD/GET-only; the `read-after-new-write` definition uses
   GET language ("Any GET following a successfully completed
   PUT…") with no LIST extension. The ladder's existence is
   evidence the underlying state is eventual; LIST has no
   equivalent, so it sees that state directly.

#### What we'd be wrong about, and how it'd fail

- *(1) wrong* → LIST under `read-after-new-write` is actually
  list-after-write. We'd be over-paying by sending `strong-*`
  for our LIST calls. **Failure mode: latency cost only.**

#### Bottom line

The actionable conclusion is reduced to what the docs *do*
commit to: **`strong-global` (11.9) delivers list-after-write
across all sites at the cost of every-site availability.**
That's the trade. Everything below `strong-global` (whether
the writes go to a local-quorum, a single primary, or
something else entirely) we cannot reason about precisely
from the docs alone.

The asymmetric mistake — assuming a weaker or mismatched setup
works when it doesn't — would fail *silently*: a LIST that
misses a recent PUT, a Lookup that returns no marker for a
record just written, an upfront-LIST dedup gate that produces a
duplicate per-attempt commit. We default to symmetric `strong-*`
to keep every load-bearing call inside an explicit doc guarantee.
