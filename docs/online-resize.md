# Online Resize — Design

> **Status:** ✅ Phases 1–3 delivered. Phase 4 (chaos coverage + promotion to user-guide prose) outstanding.
> **Feature:** second half of F-019. `ILattice.ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` runs online — reads and writes continue throughout, with zero data loss, and a brief per-shard `Rejecting` window at the swap point that the stateless-worker `LatticeGrain` absorbs transparently via `StaleTreeRoutingException` retry.
> **Scope:** This doc defines the design, failure model, and the phased implementation plan. Phases 1–3 are shipped and referenced from `TreeResizeGrain`, `TreeSnapshotGrain`, and `ShardRootGrain`; Phase 4 remains design-only.

---

## 1 · Problem statement

`ResizeAsync` changes a tree's node fan-out (`MaxLeafKeys`, `MaxInternalChildren`). This is a whole-tree shape change: every leaf and every internal node has to be re-paginated at the new fan-out. Unlike resharding (which only mutates the `ShardMap` virtual-slot → physical-shard mapping and composes cheap per-slot splits), resize is fundamentally a full-tree rewrite.

The current implementation is **offline**: `TreeResizeGrain` drives a `TreeSnapshotGrain` in `SnapshotMode.Offline`, which marks every source shard as "deleted" (blocking reads + writes) while draining into a freshly-created destination tree, then atomically swaps the registry alias.

`SnapshotMode.Online` already exists but is **explicitly lossy** — the XML doc advertises "concurrent mutations may cause minor inconsistencies". It is unsuitable for resize, which must preserve every committed write.

### Goals

- Reads and writes remain available throughout the resize.
- Zero data loss: every write that returns success before the swap is visible after the swap; every delete that returns success before the swap is honoured after the swap.
- Crash-safe: a silo restart mid-resize resumes without data loss or duplicate work.
- Composable with existing primitives — the machinery introduced here should be reusable for any future "online copy between physical trees" operation (cluster rebalancing, cross-region migration, live upgrades).
- No API surface change: `ILattice.ResizeAsync` keeps its current signature. The `SnapshotMode.Offline` overload of `SnapshotAsync` stays available for explicit point-in-time snapshots.

### Non-goals

- Shrinking shard count (covered by a future `MergeAsync` / reverse-reshard path).
- Changing `ShardCount` inside `ResizeAsync` — use `ReshardAsync` first, then `ResizeAsync`.
- Changing `VirtualShardCount` — that is a one-time provisioning decision; no online path is planned.
- Online merging of tombstones / compaction — orthogonal.

---

## 2 · Existing machinery — what we can reuse

| Primitive | Role | Reusability for online resize |
|---|---|---|
| `TreeRegistryEntry` + `ILatticeRegistry.SetAliasAsync` | Atomic logical → physical tree ID redirection | ✅ The swap point. Unchanged. |
| `ITreeSnapshotGrain` | Per-shard drain; bulk-load raw LWW entries into destination | ✅ Reused. Will gain a third mode: `Online` upgraded to strictly consistent via shadow writes; or a new `SnapshotMode.OnlineConsistent` value (TBD — see §6). |
| LWW raw-entry bulk load (`TypeAliases.LwwEntry`) | Preserves per-entry HLC timestamps across the copy | ✅ The correctness linchpin — LWW commutativity is what makes shadow forwarding safe. |
| `ShardRootGrain` write path | Entry point for every mutation (point, bulk, atomic saga) | 🔶 Extended: new "draining" mode that shadow-forwards every accepted write to the corresponding shard on a destination physical tree. |
| `StaleShardRoutingException` + stateless-worker `LatticeGrain` retry | Transparent client-side re-resolution after a routing swap | ✅ Pattern extended: new `StaleTreeRoutingException` for post-swap rejection of the old physical tree. |
| `ITreeShardSplitGrain` shadow-write + swap + reject model | Identical structural pattern, but within one physical tree across shards | 🔶 Inspiration, not reused directly — resize crosses physical tree boundaries. |
| `HotShardMonitorGrain` interlock against `IsReshardCompleteAsync` | Prevents two coordinators mutating the same tree's `ShardMap` simultaneously | ✅ Extended: also interlock against `ITreeResizeGrain.IsCompleteAsync`. |

---

## 3 · Core insight — LWW commutativity

Every key in a Lattice tree carries a hybrid-logical-clock (HLC) timestamp. All writes flow through a last-writer-wins comparator. This gives us the critical property:

> **For any two writes `W₁(k, v₁, ts₁)` and `W₂(k, v₂, ts₂)` applied in any order to any subset of replicas, the final state on every replica that received both writes is `(k, v_max, ts_max)` where `ts_max = max(ts₁, ts₂)`.**

Consequences for shadow-write forwarding:

- A live write `W_live(k, v_live, ts_live)` that arrives during drain is applied locally to `T/S` **and** shadow-forwarded to `T'/S'`.
- The drain background task reads `(k, v_read, ts_read)` from `T/S` and writes it to `T'/S'`.
- In a race, either ordering produces the same outcome on `T'/S'` because `ts_live > ts_read` (the live write happened after the drain read the position).
- Deletions are tombstones with their own HLC; they dominate by the same rule.

This means the shadow-forward path does **not** require a distributed lock, a write-ahead journal, or two-phase commit. It only requires that every write committed on `T/S` during the drain window is *eventually* delivered to `T'/S'` before the swap. "Eventually" does not mean "before the local write returns" — but for correctness of client-visible behaviour, the latest possible delivery is "before the swap".

---

## 4 · Detailed design

### 4.1 · Identity of the destination tree

At resize start, `TreeResizeGrain` creates a destination physical tree ID `{treeId}/resized/{operationId}` (existing behaviour). The destination tree:

- Is registered in `ILatticeRegistry` with the **new** `MaxLeafKeys` / `MaxInternalChildren` via `TreeRegistryEntry`.
- Inherits the source tree's `ShardCount` and a copy of the source's `ShardMap`. This is the key constraint that lets shadow forwarding be a simple per-shard projection: slot `x` on `T` routes to the same physical shard index on `T'`.
- Starts empty. All shards activate fresh.

### 4.2 · New state on `ShardRootGrain`

A new nested block of state is added to `ShardRootState`, persisted alongside existing fields:

```csharp
[Id(N)] public ShadowForwardState? ShadowForward { get; set; }

internal sealed class ShadowForwardState
{
    [Id(0)] public string DestinationPhysicalTreeId { get; set; } = "";
    [Id(1)] public ShadowForwardPhase Phase { get; set; }
    [Id(2)] public string OperationId { get; set; } = "";
}

internal enum ShadowForwardPhase
{
    Draining  = 1, // background drain in progress; forward every mutation
    Drained   = 2, // drain complete; still forward (covers the pre-swap window)
    Rejecting = 3, // swap committed; reject new mutations with StaleTreeRoutingException
}
```

`null` = no resize in flight for this shard.

### 4.3 · Write path under shadow forwarding

Every mutation entry point on `ShardRootGrain` (`SetAsync`, `SetManyAsync`, `SetManyAtomicAsync`, `DeleteAsync`, `DeleteManyAsync`, `DeleteRangeAsync`, `BulkLoadAsync`) grows a small prologue:

```
if (ShadowForward is null)
    → unchanged local path
else if (ShadowForward.Phase == Rejecting)
    → throw StaleTreeRoutingException
else
    → run (local write on T/S)  ∥  (forward to T'/S' via inter-grain SetAsync etc.)
      await both. Surface failure if either fails.
```

**Concurrency.** Both writes run in parallel via `Task.WhenAll`. Total added latency ≈ `max(local, forward) - local` ≈ one additional grain hop. For a freshly-created destination tree on the same cluster, this is in the millisecond range.

**Atomicity.** The parallel write is **not** a two-phase commit. If local succeeds and forward fails, the client sees failure. On retry (idempotent via LWW), both succeed. Some writes may land on T only; they will be captured by the background drain reader, which emits them into T' with their original HLCs. LWW converges. **No durable shadow-queue is required.**

This is the pivotal simplification that falls out of LWW: **every write the client sees as successful is either applied synchronously to T' via the shadow path, or eventually applied to T' via the drain path, or both. Duplicates are absorbed by LWW.**

### 4.4 · Drain path

`TreeSnapshotGrain` in the new consistent-online mode drains each shard of `T` into `T'` using the existing raw-entry bulk-load primitive (which preserves HLCs). Per-shard drain is orchestrated by the source tree's `ShardRootGrain`, which already owns shard-level coordination.

Ordering per shard:

1. **Prepare.** `TreeResizeGrain` → `ShardRootGrain.BeginShadowForwardAsync(destinationTreeId, operationId)`. Shard root persists `ShadowForwardState { Phase = Draining }` before returning. Subsequent writes are mirrored.
2. **Drain.** `TreeSnapshotGrain` reads the shard's current entries and bulk-loads them into `T'/S`. The drain uses a point-in-time read — not a snapshot lock, just "whatever's there now". Writes that land during the drain are handled by the shadow-forward path in §4.3.
3. **Quiesce.** Shard root transitions `Phase = Drained`. No new background work; continues to mirror live writes.
4. Wait for every shard to reach `Drained`.

There is **no per-shard "wait for in-flight shadow forwards"** gate — see §4.3.

### 4.5 · Swap

Once every shard on `T` is `Drained`:

1. `TreeResizeGrain` updates the registry `TreeRegistryEntry` for the logical `treeId` with the new fan-out options.
2. `TreeResizeGrain` calls `registry.SetAliasAsync(treeId, T')`. This is the atomic cutover.
3. `TreeResizeGrain` instructs each shard of `T` to transition `Phase = Rejecting`. Subsequent writes against `T/S` throw `StaleTreeRoutingException`.
4. Stateless-worker `LatticeGrain` catches `StaleTreeRoutingException` (mirroring its existing `StaleShardRoutingException` handling), re-resolves via the registry, and retries against `T'`.

### 4.6 · Cleanup

Soft-delete `T` via `ITreeDeletionGrain`. The `SoftDeleteDuration` window (existing primitive) leaves `UndoResizeAsync` viable.

### 4.7 · `UndoResizeAsync`

Semantics carry over from the offline flow with one addition. If undo is invoked **during** the drain (i.e. before swap), the sequence is:

1. Instruct every shard root on `T` to transition `Phase = null` (clear shadow-forward).
2. Delete `T'` (no alias was ever set).
3. Clear resize state.

If undo is invoked **after** swap (current offline behaviour, already implemented), the existing flow applies: remove alias, recover `T`, delete `T'`, restore `TreeRegistryEntry`. For online, add: instruct every shard root on `T` to transition `Phase = null` (clearing the vestigial `Rejecting` flag) so `T` becomes writable again for undo.

---

## 5 · Failure modes

| Scenario | Behaviour |
|---|---|
| Shadow-forward transient failure (e.g. target shard root deactivating mid-call) | Local write succeeds; forward throws; client sees failure; retries idempotently via LWW. The dropped write is captured by the drain reader if it has not yet passed that position, or re-delivered by the retry. |
| Silo restart during drain | Reminder-anchored `TreeResizeGrain` + `TreeSnapshotGrain` resume. `ShadowForwardState` persisted on each shard root — writes continue to mirror. Drain picks up from its last committed bulk-load offset. |
| Silo restart after swap, before cleanup | `TreeResizeGrain` state shows `Phase = Cleanup`. Resumes soft-delete. |
| Swap fails partway (alias set but some shards never transition to `Rejecting`) | Writes continue to mirror (still in `Drained`). Coordinator retries the per-shard transition. Safe because alias swap already redirected clients; any late writes on `T` are still mirrored to `T'`. |
| `TreeSnapshotGrain` observes a shard that does not yet have `ShadowForwardState` (race: drain reader started before `BeginShadowForwardAsync` persisted) | `BeginShadowForwardAsync` completes **before** the drain reader starts on that shard — ordering invariant enforced by `TreeResizeGrain`. This is a coordinator invariant, not a runtime check. |
| Concurrent `ReshardAsync` or `HotShardMonitorGrain` split during resize | Suppressed. `HotShardMonitorGrain` gains `IsResizeCompleteAsync` to its existing interlock; `TreeReshardGrain.ReshardAsync` throws `InvalidOperationException` if a resize is in flight. |
| Destination tree already exists (from prior failed resize) | Deterministic destination ID includes `operationId` — fresh per attempt. Crash-resume uses the persisted `operationId` → same ID, idempotent. |
| Client writes to `T` after swap but before `Rejecting` transition lands on that shard | Write lands on `T` and is mirrored to `T'`. Client saw success. The eventual `Rejecting` transition drops subsequent writes. No data loss. |
| Client issues a scan that straddles the swap | Scan targets were resolved from the registry once at the fan-out entry. If the alias changes mid-scan, per-shard grain calls still hit `T` until they throw `StaleTreeRoutingException`; stateless-worker retry restarts the scan against `T'`. Same pattern as reshard. |

---

## 6 · API decision: new mode or upgrade existing?

Two options. Both preserve the public `ILattice.ResizeAsync` signature.

**Option A — Upgrade `SnapshotMode.Online` in-place.**

`SnapshotMode.Online` becomes strictly consistent. The current best-effort behaviour is lost.

- **Pro:** no new public enum value; no breaking code change.
- **Con:** changes published semantics of `Online` mode — any caller relying on the current best-effort behaviour breaks silently. This is a real concern because `SnapshotMode` is a public type.

**Option B — Add `SnapshotMode.OnlineConsistent`; leave `Online` as-is.**

- **Pro:** explicit, no silent behaviour change, lets callers pick their trade-off.
- **Con:** `Online` becomes a footgun people reach for expecting consistency.

**Recommendation: Option A, with a release-note flag.** The current best-effort `Online` mode is a misfeature — there is no legitimate use case for a snapshot that silently loses writes. The upgrade is a correctness improvement, and the higher per-write cost during drain is acceptable because the alternative is the existing offline lock. We retain the enum shape (no ABI break) and document the semantics tightening as a v-next note.

Confirmation point for implementation PR.

---

## 7 · New public / internal API surface

| Type | Kind | Notes |
|---|---|---|
| `StaleTreeRoutingException` | New public exception | Mirrors `StaleShardRoutingException`. Carries `LogicalTreeId`, `StaleTreeId`. Thrown by `ShardRootGrain` in `Rejecting` phase. |
| `ShadowForwardState` | New internal state record on `ShardRootState` | See §4.2. |
| `ShadowForwardPhase` | New internal enum | `Draining` / `Drained` / `Rejecting`. |
| `IShardRootGrain.BeginShadowForwardAsync(string destinationTreeId, string operationId)` | New internal method | Transitions `ShardRootState.ShadowForward` to `Draining`. Idempotent for same `operationId`. |
| `IShardRootGrain.MarkDrainedAsync(string operationId)` | New internal method | Draining → Drained. |
| `IShardRootGrain.EnterRejectingAsync(string operationId)` | New internal method | Drained → Rejecting. |
| `IShardRootGrain.ClearShadowForwardAsync(string operationId)` | New internal method | Used by undo. |
| `SnapshotMode.Online` | Existing public enum value | Semantics tightened to "strictly consistent via shadow forwarding". |
| `LatticeOptions.MaxConcurrentDrains` | New option, default 4 | Upper bound on the number of concurrent per-shard drains `TreeSnapshotGrain` dispatches. Mirrors `MaxConcurrentMigrations`. |

All three new `IShardRootGrain` methods are guarded by `InternalGrainGuardFilter` (not callable by external clients) and decorated with `[EditorBrowsable(Never)]`.

### No change to `ILattice`

`ResizeAsync` and `UndoResizeAsync` (if present on `ILattice` — currently they live on `ITreeResizeGrain`; may surface on `ILattice` as a follow-up) keep their signatures. The behavioural promise becomes "online with no availability loss".

---

## 8 · Phased implementation plan

Four PRs. Each stands alone, is independently mergeable, and ships with its own tests.

### Phase 1 — Shadow-forwarding primitive (biggest PR)

**Status:** ✅ Shipped.

**Scope.** The reusable building block. After this PR, nothing in the public API observes online resize — the primitive exists and is tested in isolation.

- New `StaleTreeRoutingException`.
- New `ShadowForwardState` + `ShadowForwardPhase` + migration of `ShardRootState` (add nullable field, no breaking serialization — new `[Id(n)]` slot).
- New `BeginShadowForwardAsync` / `MarkDrainedAsync` / `EnterRejectingAsync` / `ClearShadowForwardAsync` on `IShardRootGrain`. Guard + call-context-filter registrations.
- Modify every mutation path on `ShardRootGrain` to consult `ShadowForward`:
  - `SetAsync`, `SetManyAsync`, `SetManyAtomicAsync`
  - `DeleteAsync`, `DeleteManyAsync`, `DeleteRangeAsync`
  - `BulkLoadAsync`
- Each modified path runs `local ∥ forward` and throws if either fails.
- Stateless-worker `LatticeGrain` catches `StaleTreeRoutingException` on every call site and re-resolves via the registry.
- Tests:
  - Unit tests on `ShardRootGrain` for each mutation path × each phase × null/non-null shadow state × forward failure.
  - Integration test that wires two physical trees on the 4-shard fixture, places one in `Draining`, drives writes through the logical path, verifies both trees converge under LWW.

**Estimated size:** ~1,500 LOC + 40 tests.

### Phase 2 — Consistent online snapshot mode

**Status:** ✅ Shipped.

**Scope.** `SnapshotMode.Online` semantics tightened.

- Upgrade `TreeSnapshotGrain` to drive `BeginShadowForwardAsync` → drain → `MarkDrainedAsync` on each shard (bounded by `LatticeOptions.MaxConcurrentDrains`). Wait for all shards `Drained` before returning.
- Existing `SnapshotMode.Offline` path unchanged.
- Doc comment on `SnapshotMode.Online` rewritten.
- Tests:
  - Unit tests on `TreeSnapshotGrain` covering the new mode.
  - Integration test: drain + swap against a fresh destination, verify universe invariant.
  - Regression pass of existing snapshot tests.

**Estimated size:** ~600 LOC + 15 tests.

### Phase 3 — Wire into `ResizeAsync`

**Status:** ✅ Shipped. `TreeResizeGrain` now drives an online snapshot, adds a `ResizePhase.Reject` transition and dual-path undo (before-swap: drain + abort snapshot + clear shadow state; after-swap: existing recovery flow plus defensive `snapshot.AbortAsync`), and threads a `logicalTreeId` through `SnapshotWithOperationIdAsync` so `StaleTreeRoutingException` carries the caller's logical name rather than the internal physical ID. `HotShardMonitorGrain` polls `ITreeResizeGrain.IsCompleteAsync`; `TreeReshardGrain.ReshardAsync` rejects while a resize is in flight.

**Scope.** Flip `TreeResizeGrain` from `SnapshotMode.Offline` to `SnapshotMode.Online`. Wire `EnterRejectingAsync` post-swap. Wire `ClearShadowForwardAsync` into `UndoResizeAsync`.

- Add interlock suppression: `HotShardMonitorGrain` polls `ITreeResizeGrain.IsCompleteAsync`; `TreeReshardGrain.ReshardAsync` throws while a resize is in flight.
- Integration tests: resize under concurrent writes preserves every key, `Count` matches universe, undo during drain works, undo after swap works.

**Estimated size:** ~400 LOC + 10 tests.

### Phase 4 — Chaos coverage + docs

**Status:** ⬜ Not started.

**Scope.** Promote the design to user docs and verify under stress.

- Convert this design doc into a user-facing `docs/online-resize.md` guide (drop the "Status: Design" header, keep sections 1, 3, 4, 5, 7, and Tuning).
- Update `docs/api.md` if any public surface changed (probably just `LatticeOptions.MaxConcurrentDrains`).
- Update `roadmap.md`, `README.md` docs table.
- New `ChaosResizeIntegrationTests.Chaos_resize_under_concurrent_load_preserves_all_data` modelled on the reshard chaos test.

**Estimated size:** ~400 LOC + 1 chaos test.

---

## 9 · Testing strategy

### Unit (per-grain, fakes only)

- `ShardRootGrain` × every mutation path × `ShadowForward ∈ {null, Draining, Drained, Rejecting}` × `forward outcome ∈ {success, transient-fail, stale-routing}`.
- `TreeSnapshotGrain` in the new online mode — correct begin/drain/mark sequencing, concurrency cap, crash-resume.
- `TreeResizeGrain` — phase machine transitions, undo-during-drain, undo-after-swap.

### Integration (4-shard cluster fixture)

- Drain without resize: begin-shadow-forward + live writes + verify both trees converge.
- Full resize + single-threaded writes: all keys present, `Count` correct, options on `T'` match new fan-out.
- Resize with concurrent writes (bounded, deterministic): universe invariant holds post-resize.
- Undo during drain.
- Undo after swap.
- Interlock: `ReshardAsync` during resize throws; `HotShardMonitorGrain` suppresses its own pass.

### Chaos (20 s)

- 3 writers + 2 readers + 1 scanner + 1 counter racing against a resize that halves `MaxLeafKeys` and halves `MaxInternalChildren`. Universe pinned. Verifies zero envelope violations, final `Count == UniverseSize`, `KeysAsync` yields exactly the universe, resize reports complete, destination tree's options match the new fan-out.

---

## 10 · Risks and open questions

1. **Hot-path cost.** Every write during drain pays one extra grain hop. For a tree with a 100-ms end-to-end write latency, this is a measurable but bounded tax. Mitigation: drain fast (`MaxConcurrentDrains`); document the trade-off; advise callers to resize during off-peak windows even though it is online.
2. **Serialization version of `ShardRootState`.** Adding a new `[Id(n)]` slot is backward-compatible with Orleans serializer; existing activations without the field deserialize `ShadowForward = null`, which is the "no resize in flight" state. Verified pattern (already used for other nullable additions to grain state).
3. **Saga-path shadow forwarding.** `SetManyAtomicAsync` is a saga with compensation. Compensation paths must also shadow-forward. Needs explicit verification — the saga's rollback writes must also mirror. Phase 1 PR explicitly includes saga-path coverage in its test matrix.
4. **Option A vs B (see §6).** Behavioural tightening of `SnapshotMode.Online`. Needs product-level sign-off before Phase 2 lands.
5. **Drain reader sees a shard actively splitting mid-resize.** Mitigated by §5 interlock: splits are suppressed for the resize duration. A split in flight at resize-start must be awaited before `BeginShadowForwardAsync` is called (enforced by `TreeResizeGrain`).
6. **`BulkLoadAsync` with shadow forwarding.** Bulk-load during drain would forward each batch. Large bulk loads could inflate drain duration. Acceptable — the alternative is to reject bulk loads during resize, which is a worse UX. Keep it, measure, revisit if problematic.

---

## 11 · Out of scope for this design

- Online `MergeAsync` (shrink shards).
- Cross-cluster replication using the shadow-forward primitive (different problem shape — network partitions, not same-cluster grain calls).
- Incremental rebalancing triggered by storage pressure rather than explicit API call.

These all potentially reuse the Phase 1 primitive but are independent features.

---

## 12 · Sign-off checklist

Before Phase 1 PR is cut:

- [ ] Confirm Option A (upgrade `SnapshotMode.Online` in-place) vs Option B (new enum value). Default: Option A.
- [ ] Confirm phased PR plan (§8) matches the team's review-bandwidth preferences.
- [ ] Confirm `MaxConcurrentDrains` default (proposed: 4, matching `MaxConcurrentMigrations`).
- [ ] Confirm naming: `ShadowForward*` vs `DrainTarget*` vs `Mirror*`. This doc uses `ShadowForward`.
