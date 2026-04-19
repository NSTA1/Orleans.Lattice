# Atomic Multi-Key Writes

`ILattice.SetManyAtomicAsync(entries)` commits a batch of key-value pairs with
all-or-nothing semantics: either every entry in the batch is durably written,
or — on any failure — every already-committed entry in that batch is rolled
back to its pre-saga value. The feature is implemented as a saga coordinator
grain that wraps the existing per-key `SetAsync` path.

The non-atomic `SetManyAsync` remains available for throughput-oriented use
cases where partial application on failure is acceptable.

## Atomicity Guarantees

Given a batch `[(k₀, v₀), (k₁, v₁), …, (kₙ₋₁, vₙ₋₁)]`, a successful
`SetManyAtomicAsync` call guarantees:

1. **All-or-nothing commit.** On successful return every `kᵢ` holds `vᵢ` as
   its last-writer-wins (LWW) value, or — if the saga failed — every `kᵢ`
   holds the value it had before the saga started (its pre-saga value; for
   keys that did not exist before the saga, the key is tombstoned).
2. **Sequential per-key ordering.** Each key is written at most once by the
   saga, with a monotonically-increasing [Hybrid Logical Clock](state-primitives.md)
   timestamp. Compensation writes use a fresh HLC tick, so LWW merge resolves
   the rollback as the winner even if an external writer raced the saga.
3. **Crash recovery.** If the silo hosting the saga grain crashes mid-flight,
   a keepalive reminder resumes the saga on reactivation and drives it to a
   terminal state (either Completed or Compensated + Completed).
4. **Input validation.** Duplicate keys and null values fail fast with
   `ArgumentException` before any write is attempted. An empty batch returns
   immediately without contacting any leaf grain.
5. **Idempotent client retry.** A re-invocation of the same saga grain (same
   `treeId` + `operationId`) after successful completion returns success
   without re-executing. A re-invocation after a compensated-failure replays
   the original `InvalidOperationException` with the preserved failure
   message.

### What `SetManyAtomicAsync` does **not** guarantee

- **Reader isolation.** Readers concurrent with an in-flight saga may observe
  a partial view: some keys updated, some not. The window is bounded by the
  saga's execution time (ordering of ~ N × round-trip-to-leaf). **Once
  `SetManyAtomicAsync` returns without throwing, every key in the batch holds
  its target value and the partial-visibility window is closed.** Applications
  requiring strict isolation *during* a saga should layer optimistic
  concurrency on top using [`GetWithVersionAsync`](api.md) +
  [`SetIfVersionAsync`](api.md).
- **Ordering across distinct sagas.** Two concurrent `SetManyAtomicAsync`
  calls touching overlapping keys are resolved pairwise by LWW — the later
  HLC tick wins per key. There is no global transaction order.
- **Compensation durability in every failure mode.** If compensation itself
  fails persistently (every retry attempt throws), the saga is marked
  *poisoned* and the keepalive reminder continues firing; operators can
  inspect `AtomicWriteState.FailureMessage` via persistent state. This is
  rare in practice and limited to total-storage-outage scenarios.

## Usage

```csharp
var tree = grainFactory.GetGrain<ILattice>("orders");

// Byte-array surface
var batch = new List<KeyValuePair<string, byte[]>>
{
    new("order:42/status", Encoding.UTF8.GetBytes("shipped")),
    new("order:42/tracking", Encoding.UTF8.GetBytes("1Z999")),
    new("customer:alice/last-order", Encoding.UTF8.GetBytes("42")),
};
await tree.SetManyAtomicAsync(batch);

// Typed extension (System.Text.Json by default)
var typed = tree.AsTyped<Order>();
await typed.SetManyAtomicAsync(new Dictionary<string, Order>
{
    ["order:42"] = new Order(42, "shipped", "1Z999"),
    ["order:43"] = new Order(43, "pending", null),
});
```

## How It Works

### Saga Coordinator Grain

Each call to `SetManyAtomicAsync` routes through `LatticeGrain` to a freshly
created `AtomicWriteGrain` activation keyed by `{treeId}/{operationId}`,
where `operationId` is a GUID generated per call. The grain persists a single
`AtomicWriteState` POCO whose phase transitions drive the saga lifecycle:

```
NotStarted ──► Prepare ──► Execute ──► Completed     (success)
                              │
                              └──► Compensate ──► Completed   (failure)
```

### Phase 1 — Prepare

The coordinator registers a **keepalive reminder** (1-minute period) so that a
silo crash during any subsequent phase triggers reactivation and resumption
on the next reminder tick. It then issues `GetAsync(key)` for every key in
the batch and records each pre-saga value (including absence) in
`AtomicWriteState.PreValues`. Persisting the full pre-saga snapshot before
any write is the prerequisite for bounded-time compensation. The phase ends
with a `WriteStateAsync` that flips the persisted phase to `Execute`.

### Phase 2 — Execute

The coordinator walks the batch in order, calling `ILattice.SetAsync(key,
value)` for each entry. `NextIndex` is incremented and persisted after every
successful step, so crash-resume can pick up exactly where it left off
without replaying committed writes. Each step has a bounded retry budget
(`MaxRetriesPerStep = 1`); a persistent fault pivots the saga into
`Compensate` without re-throwing.

### Phase 3 — Compensate (failure path only)

The coordinator walks already-committed entries in **reverse** order. For
each previously-existing key it calls `SetAsync(key, preValue)`; for each
previously-absent key it calls `DeleteAsync(key)`. Every compensation write
receives a fresh HLC tick from the target leaf grain, so LWW merge resolves
the compensating write as the winner even if an external writer modified the
key during the saga's execution window.

On reminder-driven re-entry, the per-step retry counter is reset so a
transient fault that outlived the previous activation can be retried
freshly.

### Phase 4 — Complete

Whether the saga succeeds or rolls back, it ends by writing `Phase =
Completed`, unregistering the keepalive reminder, and calling
`DeactivateOnIdle`. Failed sagas preserve `FailureMessage` so a client that
re-invokes the same grain key receives the original failure via
`InvalidOperationException`.

## Crash-Recovery Timeline

| Crash point | State on reactivation | Recovery path |
|---|---|---|
| Before `Prepare` persists | `Phase = NotStarted` | Reminder tick unregisters itself and deactivates. Client's pending call returns a transport error; client retries with a fresh `operationId`. |
| During `Execute`, after *k* writes committed | `Phase = Execute`, `NextIndex = k` | Reminder tick calls `RunSagaAsync`; the saga resumes at entry *k* and drives to completion. |
| During `Compensate`, after *m* rollbacks | `Phase = Compensate`, `NextIndex = N − m` | Reminder tick resets `RetriesOnCurrentStep`, continues compensation, then completes. |
| After `Completed` persists | `Phase = Completed` | Reminder tick unregisters itself and deactivates. |

## Performance Notes

- A saga of size *N* issues approximately *2N* + 3 Orleans calls: *N*
  pre-saga reads, *N* writes, and 3 `WriteStateAsync` calls on the saga's own
  state. For large batches where atomicity is not required, prefer the
  parallel `SetManyAsync`.
- `AtomicWriteState` is stored under the Lattice storage provider
  (`"OrleansLattice"`). The saga grain deactivates on completion, so the
  storage row is typically read exactly once (on activation) and written
  four times (Prepare → Execute → … → Completed).
- Readers observing a saga in flight pay no additional cost; the
  coordinator does not block, lock, or serialise reads.

## Related

- [API Reference](api.md) — full `SetManyAtomicAsync` signature and typed
  extensions.
- [State Primitives](state-primitives.md) — HLC and LWW semantics the saga
  relies on for compensation correctness.
- [Chaos Tests](chaos-tests.md) — atomic-write workload exercised under
  concurrent splits and fault injection.
