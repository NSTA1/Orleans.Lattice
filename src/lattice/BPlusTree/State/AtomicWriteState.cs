namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A pre-saga snapshot of a single key's value, captured during the
/// <see cref="AtomicWritePhase.Prepare"/> phase so that compensation can
/// restore it via a fresh write (LWW resolves in favor of the newer HLC).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicPreValue)]
internal sealed class AtomicPreValue
{
    /// <summary>The key the snapshot belongs to.</summary>
    [Id(0)] public string Key { get; set; } = string.Empty;

    /// <summary>
    /// The value before the saga started, or <c>null</c> if the key was
    /// absent or tombstoned.
    /// </summary>
    [Id(1)] public byte[]? Value { get; set; }

    /// <summary>
    /// <c>true</c> if the key existed (i.e. had a live value) before the saga;
    /// <c>false</c> if it was absent or tombstoned.
    /// </summary>
    [Id(2)] public bool Existed { get; set; }

    /// <summary>
    /// The absolute UTC <c>DateTimeOffset.UtcTicks</c> at which the pre-saga
    /// entry was set to expire ( TTL), or <c>0</c> if the entry had no
    /// TTL. Defaulting to <c>0</c> keeps persisted pre-saga state from earlier
    /// versions wire-compatible (a missing <see cref="Id"/>-3 field decodes
    /// to <c>0</c>, matching a no-TTL entry).
    /// </summary>
    [Id(3)] public long ExpiresAtTicks { get; set; }

    /// <summary>
    /// Origin cluster id captured from the pre-saga entry's
    /// <see cref="Primitives.LwwValue{T}.OriginClusterId"/>, or <c>null</c>
    /// when the key was absent or authored locally. Restored through
    /// <see cref="LatticeOriginContext.With"/> during compensation so the
    /// rolled-back value re-lands with its original origin stamp.
    /// Wire-compatible: missing field on legacy persisted state decodes
    /// to <c>null</c>.
    /// </summary>
    [Id(4)] public string? OriginClusterId { get; set; }

    /// <summary>
    /// Vector-clock frontier captured from the pre-saga entry's
    /// <see cref="Primitives.LwwValue{T}.VectorClock"/>, or <c>null</c>
    /// when the key was absent or the entry carried no frontier.
    /// Restored through <see cref="LatticeVectorClockContext.With"/>
    /// during compensation so the rolled-back value re-lands with its
    /// original frontier. Wire-compatible: missing field on legacy
    /// persisted state decodes to <c>null</c>.
    /// </summary>
    [Id(5)] public Primitives.VersionVector? VectorClock { get; set; }
}

/// <summary>
/// Persistent state for <see cref="Grains.AtomicWriteGrain"/>.
/// Tracks the progress of an in-flight atomic multi-key write so that it
/// can be resumed (or compensated) after a silo restart.
/// Key format: <c>{treeId}/{operationId}</c>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AtomicWriteState)]
internal sealed class AtomicWriteState
{
    /// <summary>Current lifecycle phase of the saga.</summary>
    [Id(0)] public AtomicWritePhase Phase { get; set; } = AtomicWritePhase.NotStarted;

    /// <summary>
    /// Tree ID the saga writes into. Captured at <see cref="AtomicWritePhase.Prepare"/>
    /// so the grain remains deterministic after activation.
    /// </summary>
    [Id(1)] public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The entries to write, in the order they are applied. A duplicate-key
    /// check is performed before the saga starts so this list always has
    /// distinct keys.
    /// </summary>
    [Id(2)] public List<KeyValuePair<string, byte[]>> Entries { get; set; } = [];

    /// <summary>
    /// Pre-saga snapshots keyed by the same order as <see cref="Entries"/>.
    /// Populated during <see cref="AtomicWritePhase.Prepare"/>.
    /// </summary>
    [Id(3)] public List<AtomicPreValue> PreValues { get; set; } = [];

    /// <summary>
    /// Index of the next entry to commit during <see cref="AtomicWritePhase.Execute"/>,
    /// or the next entry to roll back during <see cref="AtomicWritePhase.Compensate"/>.
    /// </summary>
    [Id(4)] public int NextIndex { get; set; }

    /// <summary>
    /// Number of consecutive retries on the current step. Reset to zero on advance.
    /// </summary>
    [Id(5)] public int RetriesOnCurrentStep { get; set; }

    /// <summary>
    /// Message of the exception that forced the saga into
    /// <see cref="AtomicWritePhase.Compensate"/>. Preserved for logging and
    /// for the synchronous client path to re-throw a meaningful error.
    /// </summary>
    [Id(6)] public string? FailureMessage { get; set; }

    /// <summary>
    /// SHA-256 fingerprint of the sorted key set submitted when the saga
    /// was first started (caller-supplied idempotency key scenarios). When
    /// non-null, a re-entry to <see cref="Grains.AtomicWriteGrain.ExecuteAsync"/>
    /// whose entries produce a different fingerprint is rejected with
    /// <see cref="InvalidOperationException"/>. Null for legacy persisted
    /// state written before this field existed; absent fingerprint skips
    /// the check (matches prior behaviour).
    /// </summary>
    [Id(7)] public byte[]? KeyFingerprint { get; set; }

    /// <summary>
    /// Stable per-saga transaction id minted on the first
    /// <see cref="AtomicWritePhase.Prepare"/> and persisted across crash
    /// recovery so the saga's per-key writes (and its compensation
    /// rewrites) all carry the same
    /// <see cref="LatticeMutation.TransactionId"/>. <see cref="Guid.Empty"/>
    /// when unset (legacy persisted state from before this field
    /// existed); a fresh activation re-mints a value lazily during
    /// <see cref="Grains.AtomicWriteGrain.RunSagaAsync"/> so resumed
    /// sagas continue to share an id.
    /// </summary>
    [Id(8)] public Guid TransactionId { get; set; }

    /// <summary>
    /// Author-delta kind captured from
    /// <see cref="LatticeDeltaContext.Current"/> when the saga was first
    /// started, or <see langword="null"/> when the caller did not wrap
    /// the <c>SetManyAtomicAsync</c> call in a
    /// <see cref="LatticeDeltaContext.With"/> scope. Re-stamped onto
    /// Orleans <see cref="Runtime.RequestContext"/> on every per-key
    /// <c>SetAsync</c> / <c>DeleteAsync</c> the saga issues — including
    /// compensation rewrites — so every emitted
    /// <see cref="LatticeMutation"/> carries the same author-delta as
    /// the original batch. Wire-compatible: missing field on legacy
    /// persisted state decodes to <see langword="null"/>.
    /// </summary>
    [Id(9)] public string? DeltaKind { get; set; }

    /// <summary>
    /// Author-delta payload captured alongside <see cref="DeltaKind"/>.
    /// Opaque bytes (typically Orleans-serialized typed delta record);
    /// the lattice library never opens the payload. Wire-compatible:
    /// missing field on legacy persisted state decodes to
    /// <see langword="null"/>.
    /// </summary>
    [Id(10)] public byte[]? DeltaPayload { get; set; }

    /// <summary>
    /// Vector-clock frontier captured once from
    /// <see cref="LatticeVectorClockContext.Current"/> when the saga
    /// was first started, or <see langword="null"/> when the caller
    /// did not wrap the <c>SetManyAtomicAsync</c> call in a
    /// <see cref="LatticeVectorClockContext.With"/> scope. Re-stamped
    /// onto Orleans <see cref="Runtime.RequestContext"/> on every
    /// per-key <c>SetAsync</c> the saga issues during
    /// <see cref="AtomicWritePhase.Execute"/> so every emitted
    /// <see cref="LatticeMutation"/> in the batch carries the
    /// identical <see cref="LatticeMutation.VectorClock"/> — closing
    /// the per-key VC drift a remote receiver would otherwise see as
    /// a partial-set state where the writer's frontier said all N
    /// should be visible together. Compensation rewrites override the
    /// saga-wide stamp per-key with each
    /// <see cref="AtomicPreValue.VectorClock"/> via
    /// <see cref="LatticeVectorClockContext.With"/>; the
    /// saga-wide stamp is restored when each rollback's scope
    /// disposes. Wire-compatible: missing field on legacy persisted
    /// state decodes to <see langword="null"/>.
    /// </summary>
    [Id(11)] public Primitives.VersionVector? VectorClock { get; set; }

    /// <summary>
    /// <see langword="true"/> when this saga was started via
    /// <c>IReplicationApplyGrain.ApplyManyAtomicAsync</c> (cross-cluster
    /// atomic-batch apply). In apply mode the saga reads
    /// per-entry source metadata from <see cref="ApplyEntries"/> rather
    /// than treating every entry as a fresh local write: each per-key
    /// call is wrapped in nested
    /// <see cref="LatticeOriginContext.With(string?)"/> +
    /// <see cref="LatticeVectorClockContext.With(Primitives.VersionVector?)"/> +
    /// <see cref="LatticeHlcOverrideContext.With(Primitives.HybridLogicalClock?)"/>
    /// scopes drawn from <see cref="OriginClusterId"/> and the entry's
    /// <see cref="AtomicApplyEntry.VectorClock"/> /
    /// <see cref="AtomicApplyEntry.Timestamp"/>, so the leaf grain
    /// re-stamps the source-side metadata bit-identically. Wire-compatible:
    /// missing field on legacy persisted state decodes to
    /// <see langword="false"/> (local-saga semantics).
    /// </summary>
    [Id(12)] public bool IsApplyMode { get; set; }

    /// <summary>
    /// Per-entry source metadata for an apply-mode saga, populated when
    /// <see cref="IsApplyMode"/> is <see langword="true"/>. Indices are
    /// kept in lock-step with <see cref="Entries"/> so the
    /// pre-saga-capture and execute machinery in
    /// <see cref="Grains.AtomicWriteGrain"/> can reuse the existing
    /// <see cref="NextIndex"/> cursor. Empty for legacy local-saga state.
    /// </summary>
    [Id(13)] public List<AtomicApplyEntry> ApplyEntries { get; set; } = [];

    /// <summary>
    /// Saga-wide origin cluster id captured from the
    /// <see cref="IReplicationApplyGrain.ApplyManyAtomicAsync"/>
    /// call's <c>originClusterId</c> argument and re-stamped onto
    /// every per-key call the saga issues during
    /// <see cref="AtomicWritePhase.Execute"/> via
    /// <see cref="LatticeOriginContext.With(string?)"/> so the leaf
    /// grain stamps the authoring cluster's id verbatim onto the
    /// persisted <see cref="Primitives.LwwValue{T}.OriginClusterId"/>
    /// <see langword="null"/> for legacy local-saga state.
    /// </summary>
    [Id(14)] public string? OriginClusterId { get; set; }

    /// <summary>
    /// Total entry count of the enclosing atomic transaction, captured
    /// once on the first <see cref="AtomicWritePhase.Prepare"/> from
    /// <see cref="Entries"/>'s <c>Count</c> (or
    /// <see cref="ApplyEntries"/>'s <c>Count</c> in apply mode) and
    /// re-stamped — together with each per-key index — onto Orleans
    /// <see cref="Runtime.RequestContext"/> via
    /// <see cref="LatticeAtomicBatchContext.With"/> on every per-key
    /// call the saga issues during
    /// <see cref="AtomicWritePhase.Execute"/> and
    /// <see cref="AtomicWritePhase.Compensate"/> so every emitted
    /// <see cref="LatticeMutation"/> in the batch carries the
    /// identical <see cref="LatticeMutation.AtomicBatchSize"/>. The
    /// per-key index is computed deterministically from the saga's
    /// per-operation iteration order, so compensation rolls inherit
    /// the same <see cref="LatticeMutation.AtomicBatchIndex"/> as the
    /// original prepare for that key. Wire-compatible: missing field
    /// on legacy persisted state decodes to <c>0</c> (the
    /// "not-in-a-saga" sentinel the publish helpers stamp on
    /// single-key non-saga writes).
    /// </summary>
    [Id(15)] public int AtomicBatchSize { get; set; }

    /// <summary>
    /// Distinct physical-shard indices the saga's prepare phase routed
    /// per-key writes onto, captured during <see cref="AtomicWritePhase.Prepare"/>
    /// against the routing snapshot resolved up-front via
    /// <see cref="ILattice.GetRoutingAsync"/>. Drives the post-execute
    /// terminal broadcast loop in
    /// <see cref="Grains.AtomicWriteGrain"/>: one
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync(Guid, bool, CancellationToken)"/>
    /// call per index — never per key — produces the saga's per-shard
    /// linearization point. Persisted so a crash-resume re-broadcasts
    /// terminals to the same shard set; the leaf-side
    /// <c>_recentlyTerminal</c> dedup makes re-delivery idempotent.
    /// Wire-compatible: missing field on legacy persisted state decodes
    /// to an empty list, in which case the saga falls back to
    /// re-resolving the touched-shard set from the persisted
    /// <see cref="Entries"/> against a freshly fetched routing snapshot.
    /// </summary>
    [Id(16)] public List<int> TouchedShards { get; set; } = [];
}
