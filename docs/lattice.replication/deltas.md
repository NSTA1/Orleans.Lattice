# Typed CRDT delta records

The replication package ships a small set of typed delta records - one per replicable primitive - that form the wire contract between a producer cluster's commit-time change feed and a receiver cluster's apply pipeline. Each delta is the minimum information needed to merge the originating mutation into a remote replica without re-reading the primary.

Today the records are the **single wire contract** between the producer's commit-time accessor surface and the receiver's typed-delta apply pipeline. Every typed CRDT mode authors a public delta DTO into the single `WalRecord.Delta` byte slot at commit time, and the receiver-side apply pipeline dispatches on `WalRecord.Mode` to the matching primitive's instance `MergeDelta` operation. The canonical WAL encoder strips `WalRecord.Value` from every committed CRDT-mode `Set` that carries a `Delta`, so the stored and shipped record holds the delta only; prepared saga entries keep `Value`, which the receiver's pending-bucket apply needs. Change-feed consumers of a CRDT tree should therefore read `Delta`, or read current state through `ILattice`, rather than treating `Value` as a full-state snapshot. `LwwRegister` continues to use the opaque `Value` path and is unaffected.

## Why typed deltas

Cross-cluster LWW-on-bytes silently drops one side's update under concurrent active-active mutations: two clusters concurrently adding to a set both write a full post-merge byte string, the bytes are then merged by HLC and the loser's add disappears. CRDTs converge precisely because they exchange *operations* (or operation-shaped deltas), not post-merge snapshots. The records in this package preserve that property end-to-end.

Values that are not a recognised CRDT primitive - schemaless `byte[]` payloads on an `LwwRegister` tree - ship as the opaque `WalRecord.Value`, not as a typed delta: no shipped producer authors an `LwwRegisterDelta`. The record stays on the public surface as the typed last-writer-wins shape for custom transports and appliers, and carries the same "won't converge under concurrent updates" caveat.

## Records

Every record is a `readonly record struct` marked `[GenerateSerializer]`, with a stable Orleans alias defined in the core `Orleans.Lattice` assembly. Every record except `OrMapDelta<TKey, TValue>`, `OrMapDeltaEntry<TKey, TValue>`, and `MvRegisterEntry` is also marked `[Immutable]`; those three deliberately are not, because each carries a mutable payload (a CRDT value the receiver folds in place, or `byte[]` value bytes) that Orleans must copy on a same-silo grain call. All records are public - they appear on the `IChangeFeed` consumer surface and on transport payloads, so custom transports and applier implementations can name them directly.

| Type | Alias | Purpose |
|------|-------|---------|
| `LwwRegisterDelta` | `ol.lwd` | Last-writer-wins register: value bytes (or tombstone) + HLC + origin + expiry. Not authored by any shipped producer - last-writer-wins trees ship the opaque `WalRecord.Value`. |
| `OrSetDelta` | `ol.osa` | Observed-remove set: lists of added and removed `(element, dot)` pairs. |
| `OrSetDeltaDot` | `ol.osc` | A unique `(replicaId, counter)` dot attached to an element inside an `OrSetDelta`. |
| `OrSetDot` | `ol.osd` | A bare `(replicaId, counter)` dot with no element - the enable, disable, and tombstone dots inside `OrFlagDelta` and `RwFlagDelta`. |
| `PnCounterDelta` | `ol.pcd` | Positive-negative counter: per-replica cumulative increment and decrement components. |
| `VersionVectorDelta` | `ol.vvd` | Version-vector advance: per-replica HLC entries that have advanced. |
| `MvRegisterDelta` | `ol.mvg` | Multi-value register: dot-tagged `(replicaId, counter, value)` entries plus the producer's observed-dot context. |
| `MvRegisterEntry` | `ol.mvd` | A single dot-tagged `(replicaId, counter, value)` entry inside an `MvRegisterDelta`. |
| `OrMapDelta<TKey, TValue>` | `ol.omd` | Observed-remove map: typed adds (`OrMapDeltaEntry<TKey, TValue>`, alias `ol.omx`) plus removed-dot tombstones (`OrMapDeltaTombstone<TKey>`, alias `ol.omt`). Recurses through `ICrdt<TValue>.MergeFrom` to merge per-key values. |
| `RgaDelta` | `ol.rgd` | Replicated Growable Array (RGA) sequence: dot-explicit inserted nodes (`RgaDeltaNode`, alias `ol.rgi`) plus tombstoned dots. Carries the structural intent `(dot, parentDot, value)` per insert so the receiver converges on an identical ordered traversal, not a post-merge snapshot. |
| `RgaDeltaNode` | `ol.rgi` | A single inserted node inside an `RgaDelta`: the `(replicaId, counter)` dot, the parent dot it was linked under, and the value bytes. |
| `OrFlagDelta` | `ol.ofd` | Observed-remove (enable-wins) flag: the enable dots added (`OrSetDot`) plus the observed-remove (disable) dots. Carries the dot context so concurrent enable/disable converge enable-wins. |
| `RwFlagDelta` | `ol.rwd` | Remove-wins (disable-wins) flag: the enable dots added, the disable dots added, plus the disable dots an observed enable has tombstoned (all `OrSetDot`). Carries the dot context so concurrent enable/disable converge disable-wins. |
| `GCounterDelta` | `ol.gcd` | Grow-only counter: per-replica cumulative components that have advanced, merged by pointwise max. |
| `GSetDelta` | `ol.gsd` | Grow-only set: the element bytes added, merged by set union. |
| `RwSetDelta` | `ol.rsd` | Remove-wins observed-remove set: the add dots, the remove dots, and the remove dots an observed add has cancelled (tombstones), each an `OrSetDeltaDot` attached to its element. |
| `BoundedRegisterDelta` | `ol.mxd` | Monotonic bounded register, shared by `MaxRegister` and `MinRegister`: the candidate value bytes and their total-order key; the direction comes from the receiver's register, not the delta. |

The typed CRDT delta records each expose a static `Empty` property that returns a reusable, allocation-free no-op delta with non-null but empty backing collections - emit it instead of constructing fresh empty arrays / dictionaries. `LwwRegisterDelta.Tombstone(timestamp, originClusterId)` is the canonical factory for tombstone deltas.

## Apply rules

| Delta | Receiver merges by |
|-------|--------------------|
| `LwwRegisterDelta` | Last-writer-wins, matching the merge the receiver applies to an `LwwRegister` entry's opaque `Value`: the higher HLC wins, and an exact HLC tie falls to the replica-invariant tombstone, expiry, and value-byte fields before the observer-relative origin id. **Never** apply via `SetAsync` - that would stamp a fresh local HLC and lose the source causality. |
| `OrSetDelta` | Union `Adds` into the local element/dot map, then drop every `(element, dot)` pair in `Removes`. Order-independent, idempotent. |
| `PnCounterDelta` | Pointwise-max each `(replica, value)` against the local positive and negative components. Never subtract - values are cumulative counts, not deltas. |
| `VersionVectorDelta` | Pointwise-max each `(replica, clock)` against the local vector. Late or duplicate delivery is a no-op. |
| `MvRegisterDelta` | Union the incoming `Entries` into the local register, dropping any entry whose `(replicaId, counter)` dot is dominated by the **other** side's context, then pointwise-max the two contexts. The merge is order-independent and idempotent. |
| `OrMapDelta<TKey, TValue>` | For each entry in `Adds`, fold the inner `Value` into the local per-key value via `ICrdt<TValue>.MergeFrom`; for each `Tombstones` dot, drop the matching local dot. Order-independent and idempotent. Receivers must register the concrete `(TKey, TValue)` pair once at startup via `siloBuilder.AddOrMapShape<TKey, TValue>(treeName)`. |
| `RgaDelta` | Add each `Inserts` node as a live node keyed by its `(replicaId, counter)` dot (idempotent; a present node has its parent / value refreshed and its tombstone flag preserved), then mark each `Tombstones` dot tombstoned. Sibling order under a shared parent is the descending `(Counter, ReplicaId)` tie-break resolved at materialise time, so every replica that applies the same deltas yields an identical ordered traversal. Order-independent and idempotent; a tombstone observed before its insert records a tombstoned placeholder so the merge stays total. |
| `OrFlagDelta` | Union `Enables` into the local enable-dot set, then union `Disables` into the local tombstone set. The flag is enabled when at least one enable dot is not tombstoned, so a concurrent enable the disabler never observed survives (enable-wins). Order-independent and idempotent. |
| `RwFlagDelta` | Union `Enables`, `Disables`, and `Tombstones` into the matching local lists. The flag is enabled when at least one enable dot survives and every disable dot has been tombstoned, so a concurrent disable the enabler never observed suppresses the flag (remove-wins). Order-independent and idempotent. |
| `GCounterDelta` | Pointwise-max each per-replica cumulative component against the local counter. Late or duplicate delivery is a no-op. |
| `GSetDelta` | Union the added elements into the local set. Elements are never removed. |
| `RwSetDelta` | Union the add dots, remove dots, and tombstones into the local set; a concurrent add and remove of the same element converge remove-wins. Order-independent and idempotent. |
| `BoundedRegisterDelta` | Fold the candidate into the local register when it beats the current value under the register's direction (larger total-order key for `MaxRegister`, smaller for `MinRegister`); a losing candidate is ignored. |

## Sender-side delta combine (pre-ship coalescing)

When pre-ship coalescing is enabled (`LatticeReplicationOptions.PreShipCoalescingEnabled`, **default on**), the per-(tree, peer) shipper folds the typed deltas of a same-key run drained into one outbound batch into a single combined delta, re-encodes it onto the kept (highest-HLC) entry, and elides the earlier same-key entries. The combine for each primitive is a join over its own semilattice, so the combined delta's receiver-side apply effect is identical to applying the source deltas in sequence - for **every** receiver state `S`, `MergeDelta(S, combine(d1, d2)) == MergeDelta(MergeDelta(S, d1), d2)`:

| Delta | Sender combines by |
|-------|--------------------|
| `OrSetDelta` | Union the two deltas' `Adds`, union their `Removes` (both are grow-only dot sets, deduped by `(replicaId, counter, element)`). |
| `PnCounterDelta` | Pointwise-max the per-replica `Increments` and `Decrements` (cumulative components - never sum). |
| `VersionVectorDelta` | Pointwise-max the per-replica `Entries`. |
| `MvRegisterDelta` | Merge through the register's own dot-dominance rule (build a transient register from each delta and `MergeFrom`, then read the surviving entries + pointwise-max context back out). A naive entry concat would wrongly keep a superseded entry. |
| `RgaDelta` | Union the `Inserts` (deduped by dot) and union the `Tombstones` (both grow-only). |
| `OrFlagDelta` | Union the two deltas' `Enables` and union their `Disables` (both grow-only dot sets, deduped by `(replicaId, counter)`). |
| `RwFlagDelta` | Union the two deltas' `Enables`, `Disables`, and `Tombstones` (all grow-only dot sets, deduped by `(replicaId, counter)`). |
| `GCounterDelta` | Pointwise-max the per-replica `Increments` (cumulative components - never sum). |
| `GSetDelta` | Union the added elements. |
| `RwSetDelta` | Union the `Adds`, `Removes`, and `Tombstones` dot sets. |
| `BoundedRegisterDelta` | Keep the candidate that wins the register's direction, comparing the total-order key first and the value bytes second. |
| `OrMapDelta<TKey, TValue>` | Union the dot-tagged `Adds` (deduped by `(key, replicaId, counter)`) and the `Tombstones`, lattice-merging any same-dot value snapshots through the value CRDT's own `ICrdt<TValue>.MergeFrom`. First-seen dots insert a cloned value so source deltas are never mutated. Registered OR-Map trees coalesce like the closed shapes; an unregistered tree (no shape descriptor) or an opaque (null) entry still ships individually. |

Each combine is commutative, associative, and idempotent, so the shipper may fold an arbitrary same-key run in iteration (HLC-ascending) order and ship the result once. A CRDT entry carrying no typed delta (`WalRecord.Delta == null`, an opaque or legacy payload) is never combined; its whole key ships verbatim. Coalescing stays within a single origin and never crosses an atomic-batch boundary - range deletes, saga terminal marks, prepared atomic-batch entries, and zero-HLC entries are never candidates. The combined entry inherits the last contributing entry's HLC and causal metadata, and the on-wire entry shape is unchanged (fewer / merged entries of the existing format - no wire-version bump). See [`replication-drivers.md`](replication-drivers.md#pre-ship-coalescing) for the operator-facing description and [`observability.md`](observability.md#pre-ship-coalescing-coalesceentries_elided--coalescebytes_elided--coalescedeltas_merged) for the `coalesce.deltas_merged` metric.

## Equality caveats

Most of the records override `Equals` and `GetHashCode` with content equality: `LwwRegisterDelta`, `OrSetDelta`, `OrSetDeltaDot`, `MvRegisterDelta`, `MvRegisterEntry`, `RgaDelta`, `RgaDeltaNode`, `OrFlagDelta`, `RwFlagDelta`, `GSetDelta`, `RwSetDelta`, and `BoundedRegisterDelta` compare their byte arrays and collections element by element, so two structurally identical deltas built from independently allocated buffers are `Equals`-equal.

The remaining records keep the synthesized record equality, which delegates to `EqualityComparer<T>.Default` for each field, so their collection-typed fields compare by **reference**, not content:

- `PnCounterDelta.Increments` / `Decrements`, `VersionVectorDelta.Entries`, and `GCounterDelta.Increments` (dictionaries).
- `OrMapDelta<TKey, TValue>.Adds` / `Tombstones` (lists); `OrMapDeltaEntry<TKey, TValue>.Value` compares through `EqualityComparer<TValue>.Default`.

Consumers that need content equality for those records must compare collection contents explicitly.

## Origin and HLC propagation

`LwwRegisterDelta` carries `OriginClusterId` directly because a last-writer-wins value is attributed to its authoring cluster for cycle-break and dedupe; on an exact HLC tie the origin id is the last-ranked tie-break, after the replica-invariant tombstone, expiry, and value-byte fields. The other deltas encode origin implicitly through their per-replica indexed dots, components, or entries. Receivers do not need a separate origin field for those records - the per-replica row identifies the producer.

The receiver-side per-origin high-water-mark state is keyed `(treeId, originClusterId)` and filters at the `WalRecord` envelope layer through its snapshot-pinned causal floor; it does not inspect delta internals.
