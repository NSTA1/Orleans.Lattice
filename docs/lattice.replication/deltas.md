# Typed CRDT delta records

The replication package ships a small set of typed delta records - one per replicable primitive - that form the wire contract between a producer cluster's commit-time change feed and a receiver cluster's apply pipeline. Each delta is the minimum information needed to merge the originating mutation into a remote replica without re-reading the primary.

Today the records are the **single wire contract** between the producer's commit-time accessor surface and the receiver's typed-delta apply pipeline. Every typed CRDT mode (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`) authors a public delta DTO into the single `WalRecord.Delta` byte slot at commit time, and the receiver-side `ReplicationApplier` dispatches on `WalRecord.Mode` to the matching primitive's instance `MergeDelta` operation. `WalRecord.Value` is retained alongside `Delta` for change-feed back-compat (full-state snapshot view), but the typed-delta path is the only thing the receiver reads on CRDT modes. `LwwRegister` continues to use the opaque `Value` path and is unaffected.

## Why typed deltas

Cross-cluster LWW-on-bytes silently drops one side's update under concurrent active-active mutations: two clusters concurrently adding to a set both write a full post-merge byte string, the bytes are then merged by HLC and the loser's add disappears. CRDTs converge precisely because they exchange *operations* (or operation-shaped deltas), not post-merge snapshots. The records in this package preserve that property end-to-end.

For value types that are not a recognised CRDT primitive - schemaless `byte[]` payloads - the producer falls through to `LwwRegisterDelta`, which is structurally the same shape and simply documents the "won't converge under concurrent updates" caveat at the API surface.

## Records

Every record is a `readonly record struct`, marked `[GenerateSerializer]` and `[Immutable]`, with a stable Orleans alias declared on `TypeAliases` in the core `Orleans.Lattice` assembly. All records are public - they appear on the `IChangeFeed` consumer surface and on transport payloads, so custom transports and applier implementations can name them directly.

| Type | Alias | Purpose |
|------|-------|---------|
| `LwwRegisterDelta` | `ol.lwd` | Last-writer-wins register: value bytes (or tombstone) + HLC + origin + expiry. Also the opaque-bytes fallback. |
| `OrSetDelta` | `ol.osa` | Observed-remove set: lists of added and removed `(element, dot)` pairs. |
| `OrSetDeltaDot` | `ol.osc` | A unique `(replicaId, counter)` dot attached to an element inside an `OrSetDelta`. |
| `PnCounterDelta` | `ol.pcd` | Positive-negative counter: per-replica cumulative increment and decrement components. |
| `VersionVectorDelta` | `ol.vvd` | Version-vector advance: per-replica HLC entries that have advanced. |
| `MvRegisterDelta` | `ol.mvg` | Multi-value register: dot-tagged `(replicaId, counter, value)` entries plus the producer's observed-dot context. |
| `OrMapDelta<TKey, TValue>` | `ol.omd` | Observed-remove map: typed adds (`OrMapDeltaEntry<TKey, TValue>`, alias `ol.omx`) plus removed-dot tombstones (`OrMapDeltaTombstone<TKey>`, alias `ol.omt`). Recurses through `ICrdt<TValue>.MergeFrom` to merge per-key values. |

`OrSetDelta`, `PnCounterDelta`, `VersionVectorDelta`, and `MvRegisterDelta` each expose a static `Empty` property that returns a reusable, allocation-free no-op delta with non-null but empty backing collections - emit it instead of constructing fresh empty arrays / dictionaries. `LwwRegisterDelta.Tombstone(timestamp, originClusterId)` is the canonical factory for tombstone deltas.

## Apply rules

| Delta | Receiver merges by |
|-------|--------------------|
| `LwwRegisterDelta` | LWW with origin tiebreaker: install the incoming delta when `(delta.Timestamp, delta.OriginClusterId)` compares strictly greater than `(existing.Timestamp, existing.OriginClusterId)` lexicographically. **Never** apply via `SetAsync` - that would stamp a fresh local HLC and lose the source causality. |
| `OrSetDelta` | Union `Adds` into the local element/dot map, then drop every `(element, dot)` pair in `Removes`. Order-independent, idempotent. |
| `PnCounterDelta` | Pointwise-max each `(replica, value)` against the local positive and negative components. Never subtract - values are cumulative counts, not deltas. |
| `VersionVectorDelta` | Pointwise-max each `(replica, clock)` against the local vector. Late or duplicate delivery is a no-op. |
| `MvRegisterDelta` | Union the incoming `Entries` into the local register, dropping any entry whose `(replicaId, counter)` dot is dominated by the **other** side's context, then pointwise-max the two contexts. The merge is order-independent and idempotent. |
| `OrMapDelta<TKey, TValue>` | For each entry in `Adds`, fold the inner `Value` into the local per-key value via `ICrdt<TValue>.MergeFrom`; for each `Tombstones` dot, drop the matching local dot. Order-independent and idempotent. Receivers must register the concrete `(TKey, TValue)` pair once at startup via `siloBuilder.AddOrMapReplicationShape<TKey, TValue>(treeName)`. |

## Equality caveats

The deltas are `readonly record struct`s, so the synthesized `Equals` operator delegates to `EqualityComparer<T>.Default` for each field. That means:

- `byte[]` fields (`LwwRegisterDelta.Value`, `OrSetDeltaDot.Element`, `MvRegisterEntry.Value`, `OrMapDeltaEntry<TKey, TValue>.Value`) compare by **reference**, not content.
- Collection-typed fields (`OrSetDelta.Adds`/`Removes`, `PnCounterDelta.Increments`/`Decrements`, `VersionVectorDelta.Entries`, `MvRegisterDelta.Entries`/`Context`, `OrMapDelta<TKey, TValue>.Adds`/`Tombstones`) compare by **reference** as well.

Two structurally-identical deltas built from independently-allocated arrays / dictionaries are therefore not `Equals`-equal. Consumers that need content-equality (e.g. matching an inbound dot against the local set) must compare element bytes and collection contents explicitly.

## Origin and HLC propagation

`LwwRegisterDelta` carries `OriginClusterId` directly because the LWW register is the only delta whose convergence depends on the writer's identity (the lexicographic tiebreaker under equal HLCs). The other deltas encode origin implicitly through their per-replica indexed structure (`OrSetDeltaDot.ReplicaId`, the keys of `PnCounterDelta.Increments`/`Decrements`, the keys of `VersionVectorDelta.Entries`, `MvRegisterEntry.ReplicaId`, `OrMapDeltaEntry<TKey, TValue>.ReplicaId` / `OrMapDeltaTombstone<TKey>.ReplicaId`). Receivers do not need a separate origin field for those records - the per-replica row identifies the producer.

The receiver-side per-origin high-water-mark table is keyed `(treeId, originClusterId)` and dedupes at the `WalRecord` envelope layer; it does not inspect delta internals.
