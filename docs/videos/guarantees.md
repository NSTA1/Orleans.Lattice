---
agent_spec: "docs/agents/concepts.yaml"
---

# The guarantees

Consistency is operation-specific: each operation carries its own caller-visible contract, and crash recovery depends on the WAL provider.

<!-- The video block is written by `npm run companions` in videos/; the documentation site replaces it with the player. -->
<!-- video:begin episode="guarantees" path="evaluate" order="2" length="3:20" cut="ed280209ec3e" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/guarantees.html),
> or [download it](../../docs-site/media/guarantees-ed280209ec3e.mp4)
> (MP4, 3:20, 10.1 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

### The guarantees

The guarantees: what each operation promises its caller, and what remains true when a silo stops.

When Lattice fits named its limits. Here, we make the caller-visible promises precise.

### Four kinds of promise

The consistency guide classifies the operations it lists. A method it does not list is not classified by that table.

Linearizable means a call takes effect at one point between its invocation and return.

Strong consistency means a result reflects a real point during the call, without missing, double-counted or misattributed entries as the tree changes.

An online snapshot is correct per key, but is not guaranteed to be one global instant.

### Bounded staleness

Eventual consistency allows bounded staleness, then converges within a configurable interval.

### Point operations

With the default cache TTL of zero, `GetAsync` is linearizable. A positive cache TTL makes it eventually consistent, with staleness bounded by the TTL plus one refresh.

`SetAsync` is linearizable. Its write-ahead log append is the commit boundary, before the in-memory projection changes.

### Batches

`GetManyAsync` is consistent per key by default, but it is not one snapshot across unrelated keys.

`SetManyAsync` is linearizable per key, not atomic as a batch. On failure, some keys may have committed; sibling writes may still be in flight after the exception, so re-read after they settle.

`SetManyAtomicAsync` is all or nothing, and atomic-visible across the tree. A concurrent reader sees every change, or none.

### Scans

`ScanKeysAsync` and `ScanEntriesAsync` stay ordered, without gaps or duplicates through topology changes.

Topology retries default to three; resilient scans separately reconnect up to eight times, with a smaller budget for stalled pages.

Exhaustion faults rather than returning an incomplete scan. A reconnect or resume captures a fresh saga-decision view, so an atomic saga crossing that boundary can appear pre-commit in earlier results and post-commit in later ones.

### Snapshots and recovery

An online snapshot is not a strict snapshot. A snapshot cursor freezes each shard's baseline during open and uses it on every page. The saga-decision gate keeps an atomic batch on one side of capture. For a multi-page cursor, its baselines are durable before the continuation returns, so it survives silo failover; eviction before the first read can expire it, and it must be reopened.

The WAL append is the commit point, but the provider decides whether it survives a restart. The default in-memory log does not; a durable provider is needed.

If the silo hosting an atomic saga crashes mid-flight, its keepalive resumes it on reactivation. A recorded commit is completed; an undecided saga is compensated once its caller's decision deadline has passed.

### Where next

Next on Evaluate: The evidence. The deep dive on how this works is Clocks and version vectors. The links are on this episode's page in the documentation.

<!-- transcript:end -->

## The code on screen

No code appears in this episode; it stays with the caller-visible contract.

## Where next

<!-- The list is written from the series plan (videos/series.json) by `npm run companions` in videos/; edit the plan, not this block. -->
<!-- where-next:begin -->

- **Next on Evaluate:** The evidence
- **The deep dive:** [Clocks and version vectors](clocks-and-version-vectors.md)
- **The pages it introduces:** [Consistency guarantees](../lattice/consistency.md)

<!-- where-next:end -->
