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
    [Id(5)] public Orleans.Lattice.VersionVector? VectorClock { get; set; }
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
    /// Author-delta payload captured from
    /// <see cref="LatticeDeltaContext.Current"/> when the saga was first
    /// started, or <see langword="null"/> when the caller did not wrap
    /// the <c>SetManyAtomicAsync</c> call in a
    /// <see cref="LatticeDeltaContext.With(byte[])"/> scope. Re-stamped
    /// onto Orleans <see cref="Runtime.RequestContext"/> on every
    /// per-key <c>SetAsync</c> / <c>DeleteAsync</c> the saga issues -
    /// including compensation rewrites - so every emitted
    /// <see cref="LatticeMutation"/> carries the same author-delta as
    /// the original batch. Opaque bytes (Orleans-serialized typed delta
    /// record); the lattice library never opens the payload.
    /// <para>
    /// The wire id <c>10</c> matches the slot previously named
    /// <c>DeltaPayload</c>; the rename is source-breaking but wire-
    /// compatible. The companion <c>DeltaKind</c> string (formerly id
    /// <c>9</c>) was retired in the same change because receivers now
    /// dispatch on <see cref="LatticeMergeMode"/>; that wire id is
    /// permanently reserved and must never be reused for a different
    /// field. Missing field on legacy persisted state decodes to
    /// <see langword="null"/>.
    /// </para>
    /// </summary>
    [Id(10)] public byte[]? Delta { get; set; }

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
    /// identical <see cref="LatticeMutation.VectorClock"/> - closing
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
    [Id(11)] public Orleans.Lattice.VersionVector? VectorClock { get; set; }

    /// <summary>
    /// Total entry count of the enclosing atomic transaction, captured
    /// once on the first <see cref="AtomicWritePhase.Prepare"/> from
    /// <see cref="Entries"/>'s <c>Count</c> and re-stamped - together
    /// with each per-key index - onto Orleans
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
    /// call per index - never per key - produces the saga's per-shard
    /// linearization point. Persisted so a crash-resume re-broadcasts
    /// terminals to the same shard set; the leaf-side
    /// <c>_recentlyTerminal</c> dedup makes re-delivery idempotent.
    /// Wire-compatible: missing field on legacy persisted state decodes
    /// to an empty list, in which case the saga falls back to
    /// re-resolving the touched-shard set from the persisted
    /// <see cref="Entries"/> against a freshly fetched routing snapshot.
    /// </summary>
    [Id(16)] public List<int> TouchedShards { get; set; } = [];

    /// <summary>
    /// Wall-clock tick (UTC) at which the saga first entered
    /// <see cref="AtomicWritePhase.Prepare"/>, captured once on the
    /// initial Prepare call and persisted across reminder-driven
    /// recovery so the end-to-end saga duration recorded by
    /// <c>orleans.lattice.atomic_write.duration</c> on
    /// <see cref="AtomicWritePhase.Completed"/> reflects the true
    /// wall-clock cost (including any time the saga was suspended
    /// across silo restarts), not just the time since the most recent
    /// activation. Wire-compatible: missing field on legacy persisted
    /// state decodes to <c>0</c>, in which case
    /// <see cref="Grains.AtomicWriteGrain"/> stamps the current tick
    /// on the next Prepare entry as a best-effort fallback.
    /// </summary>
    [Id(17)] public long SagaStartedAtTicks { get; set; }

    /// <summary>
    /// Guard predicate for a guarded atomic batch
    /// (<c>SetManyAtomicAsync&lt;T&gt;</c> with a predicate), evaluated once
    /// against each key's pre-saga snapshot during
    /// <see cref="AtomicWritePhase.Prepare"/>. <see langword="null"/> for an
    /// unguarded saga. Persisted so a reminder-driven Prepare replay re-applies
    /// the identical guard without the original caller context. When the guard
    /// rejects any key the saga transitions to
    /// <see cref="AtomicWritePhase.PreconditionFailed"/> and commits nothing.
    /// Wire-compatible: missing field on legacy persisted state decodes to
    /// <see langword="null"/> (an unguarded saga).
    /// </summary>
    [Id(18)] public LatticePredicateNode? Guard { get; set; }

    /// <summary>
    /// Coordinator key of the <see cref="Grains.LatticeCrossTreeTxGrain"/>
    /// driving this sub-saga when it participates in a cross-tree atomic write,
    /// or <see langword="null"/> for a standalone single-tree saga. When set,
    /// the saga runs in prepare-and-pause mode: after staging every write it
    /// registers the per-tree registry to delegate this saga's txid to the
    /// coordinator and parks in <see cref="AtomicWritePhase.Prepared"/> instead
    /// of recording the per-tree terminal decision, waiting for the
    /// coordinator's <c>FinalizeAsync</c> call. Persisted so a reminder-driven
    /// resume keeps the saga paused (the coordinator, not the keepalive
    /// reminder, drives the resume). Wire-compatible: missing field on legacy
    /// persisted state decodes to <see langword="null"/> (a standalone saga).
    /// </summary>
    [Id(19)] public string? ExternalAuthorityKey { get; set; }

    /// <summary>
    /// The full, canonical (ordinal-sorted) set of logical tree ids
    /// participating in the enclosing cross-tree atomic write, or
    /// <see langword="null"/> for a standalone single-tree saga. Captured
    /// from the coordinator at prepare time and persisted so the terminal
    /// broadcast - whether driven by <c>FinalizeAsync</c> or by a
    /// reminder-driven crash-recovery resume - can stamp
    /// <see cref="WalRecord.CrossTreeParticipants"/> onto every per-shard
    /// terminal record, feeding the receiver-side cross-tree visibility
    /// barrier. Wire-compatible: a missing field on legacy persisted state
    /// decodes to <see langword="null"/> (a standalone saga, no barrier).
    /// </summary>
    [Id(20)] public IReadOnlyList<string>? CrossTreeParticipants { get; set; }

    /// <summary>
    /// Optional per-entry author-delta carry aligned 1:1 with
    /// <see cref="Entries"/>: <c>EntryDeltas[i]</c> is the opaque,
    /// Orleans-serialised typed CRDT delta the producer staged for
    /// <c>Entries[i]</c>, or <see langword="null"/> for that slot when the
    /// entry was staged as a plain last-writer-wins value write (the common
    /// case, and the only case the public <c>Set</c> / <c>SetWhere</c> builder
    /// methods produce). The whole list is <see langword="null"/> when no
    /// entry in the batch carried a delta.
    /// <para>
    /// Distinct from the saga-wide <see cref="Delta"/> (<c>Id</c>-10), which
    /// stamps one identical delta onto <em>every</em> per-key emit. This slot
    /// lets a single saga carry a <em>different</em> typed delta per entry,
    /// which is what flag-CRDT membership rows need: each
    /// <c>(tag, key)</c> row author its own enable-dot delta even though they
    /// ride one cross-tree atomic write. The payload is opaque bytes (the
    /// library never opens it); the receiver dispatches the apply on the
    /// destination tree's <see cref="LatticeMergeMode"/>.
    /// </para>
    /// <para>
    /// Capture-once: the deltas are minted by the producer before the saga
    /// starts and persisted here on the first
    /// <see cref="Grains.AtomicWriteGrain.PrepareForCoordinatorAsync"/>; a
    /// reminder-driven replay reuses the persisted bytes verbatim and never
    /// re-mints. When a per-entry delta is present the saga stamps it onto the
    /// emitted <see cref="LatticeMutation.Delta"/> (overriding the saga-wide
    /// <see cref="Delta"/>) so the entry's row converges by replaying the
    /// author's intent rather than the post-merge bytes.
    /// </para>
    /// <para>
    /// Compensation needs no byte-inverse for a minted flag enable: the
    /// per-entry delta rides on a prepare-phase write (<c>IsPrepared</c>) that
    /// is invisible to readers until the saga's terminal mark surfaces it, so
    /// an aborting saga's <c>TxAbort</c> terminal drops the staged enable on
    /// every cluster exactly as it drops the staged value write - the enable
    /// dot never became visible and so requires no explicit disable.
    /// </para>
    /// <para>
    /// Wire-compatible: a missing field on legacy persisted state decodes to
    /// <see langword="null"/>, in which case every entry falls back to the
    /// saga-wide <see cref="Delta"/> behaviour exactly as before this field
    /// existed.
    /// </para>
    /// </summary>
    [Id(21)] public List<byte[]?>? EntryDeltas { get; set; }

    /// <summary>
    /// Optional per-entry delete (tombstone) channel aligned 1:1 with
    /// <see cref="Entries"/>: <c>EntryDeletes[i]</c> is <see langword="true"/>
    /// when <c>Entries[i]</c> is a retraction delete that rides the
    /// all-or-nothing batch as a prepared tombstone
    /// (<see cref="MutationKind.Delete"/>) rather than a prepared value write,
    /// or <see langword="false"/> for an upsert. The whole list is
    /// <see langword="null"/> when every entry is an upsert (the common case,
    /// and the only case the plain <c>SetManyAtomicAsync</c> entry points
    /// produce). A delete entry's value buffer is ignored - the leaf builds a
    /// tombstone <see cref="Primitives.LwwValue{T}"/> rather than reading the
    /// value - so a delete slot may carry an empty buffer.
    /// <para>
    /// Captured once on the first prepare and persisted so a reminder-driven
    /// replay reuses it verbatim; the saga stamps the delete set onto the
    /// ambient <see cref="LatticeAtomicBatchContext"/> for every batched
    /// <c>SetManyAsync</c> dispatch during
    /// <see cref="AtomicWritePhase.Execute"/>. Compensation needs no
    /// byte-inverse: a delete rides a prepared write invisible to readers until
    /// the saga's terminal mark surfaces it, so an aborting saga's
    /// <c>TxAbort</c> terminal drops the staged tombstone exactly as it drops a
    /// staged value write - the key's pre-saga value is never disturbed.
    /// </para>
    /// <para>
    /// Wire-compatible: a missing field on legacy persisted state decodes to
    /// <see langword="null"/>, in which case every entry is staged as a value
    /// upsert exactly as before this field existed.
    /// </para>
    /// </summary>
    [Id(22)] public List<bool>? EntryDeletes { get; set; }
}
