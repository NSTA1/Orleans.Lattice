using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Carries the metadata + entry stream produced by an
/// <see cref="ISnapshotProvider.ExportAsync"/> call. The stream
/// completes when every live key in the source tree has been emitted
/// or the supplied cancellation token fires.
/// <para>
/// Pair the entry stream with <see cref="CausalStableFrontier"/> on
/// the receiver: the bootstrap state machine pins the frontier as the
/// receiver-side local vector clock so the first incremental entry
/// arriving after the snapshot runs through a causal dependency check
/// from a non-zero starting frontier rather than from the empty map.
/// </para>
/// </summary>
public sealed class SnapshotStream
{
    /// <summary>The logical tree id this snapshot was produced from.</summary>
    public string TreeName { get; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> the snapshot was produced
    /// at. Entries with a strictly greater timestamp are excluded from
    /// <see cref="Entries"/>; the receiver resumes incremental
    /// replication from this HLC. A value of
    /// <see cref="HybridLogicalClock.Zero"/> means "include every live
    /// entry regardless of timestamp" - the common case for a fresh
    /// peer joining today.
    /// </summary>
    public HybridLogicalClock AsOfHlc { get; }

    /// <summary>
    /// The producer's causal-stable frontier at the moment the
    /// snapshot was produced — the pointwise minimum
    /// <see cref="VersionVector"/> across every consumer that has
    /// reported a vector through
    /// <see cref="ILatticeReplicationCursorRegistry.GetCausalStableAsync"/>.
    /// Receivers pin this on
    /// <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
    /// before draining <see cref="Entries"/>, so the causal dependency
    /// check in the apply path starts from a non-empty frontier and
    /// the first incremental entry is guaranteed to satisfy its
    /// declared dependencies. When no consumer has reported a
    /// VC-shaped cursor, the provider falls back to the producer's
    /// per-tree local vector clock — a strict superset of the meet
    /// that is safe as a snapshot cut-point because no entry can have
    /// a VC component above the producer's own local VC at capture
    /// time. Always non-null; the snapshot of an unreplicated tree
    /// returns the empty <see cref="VersionVector"/>.
    /// </summary>
    public VersionVector CausalStableFrontier { get; }

    /// <summary>
    /// Async stream of every live entry in the source tree whose
    /// <see cref="SnapshotEntry.Timestamp"/> is less than or equal to
    /// <see cref="AsOfHlc"/> (or every live entry, when
    /// <see cref="AsOfHlc"/> is <see cref="HybridLogicalClock.Zero"/>).
    /// Entries are emitted in lexicographic key order. Tombstoned and
    /// expired keys are not emitted in v1.
    /// </summary>
    public IAsyncEnumerable<SnapshotEntry> Entries { get; }

    /// <summary>
    /// Atomic-batch saga transaction ids that the producer-side
    /// snapshot quiesce path
    /// (<see cref="LatticeReplicationOptions.SnapshotSagaQuiesceTimeout"/>)
    /// could not drain to completion before the snapshot's tree
    /// scan began. Empty when no saga was in flight or every
    /// in-flight saga finished emitting before the timeout elapsed.
    /// <para>
    /// Each id in the list identifies a saga whose per-key
    /// <c>SetManyAtomicAsync</c> emissions <i>may</i> have been
    /// split across the snapshot / incremental boundary — some
    /// keys committed before the snapshot's <see cref="AsOfHlc"/>
    /// (visible in <see cref="Entries"/>), the remainder committed
    /// after (delivered on the post-snapshot incremental stream).
    /// The receiver's bootstrap state machine pins the list on
    /// <see cref="Grains.BootstrapCoordinatorState.SagaBlacklist"/>
    /// and registers it with the per-tree
    /// <see cref="Grains.IReplicationTxBufferGrain"/>; subsequent
    /// incremental entries carrying a blacklisted
    /// <see cref="ReplogEntry.TransactionId"/> bypass the staging
    /// buffer and are applied as point writes directly. Atomic
    /// visibility is degraded to causal+ for those specific
    /// timed-out sagas — operators should raise
    /// <see cref="LatticeReplicationOptions.SnapshotSagaQuiesceTimeout"/>
    /// (or reduce snapshot concurrency) if the blacklist is
    /// non-empty under steady-state load.
    /// </para>
    /// <para>
    /// Always non-null; the empty-list case is the steady-state
    /// happy path. Wire-compatibility: legacy receivers that decode
    /// a missing field simply observe an empty blacklist and apply
    /// every incremental entry through the unmodified
    /// staging-buffer path.
    /// </para>
    /// </summary>
    public IReadOnlyList<Guid> SagaBlacklist { get; }

    /// <summary>
    /// Constructs a new <see cref="SnapshotStream"/>. The constructor
    /// takes ownership of the supplied
    /// <paramref name="causalStableFrontier"/> reference; callers
    /// should not mutate it after the call returns.
    /// </summary>
    /// <param name="treeName">The logical tree id. Must be non-null and non-empty.</param>
    /// <param name="asOfHlc">The snapshot's as-of HLC.</param>
    /// <param name="causalStableFrontier">The producer's per-tree vector clock at snapshot time. Must be non-null.</param>
    /// <param name="entries">The entry stream. Must be non-null.</param>
    /// <param name="sagaBlacklist">
    /// Atomic-batch saga transaction ids that did not drain to
    /// completion within the producer-side quiesce window. May be
    /// <see langword="null"/> for backwards compatibility — the
    /// constructor coerces a null reference to
    /// <see cref="Array.Empty{T}"/>.
    /// </param>
    public SnapshotStream(
        string treeName,
        HybridLogicalClock asOfHlc,
        VersionVector causalStableFrontier,
        IAsyncEnumerable<SnapshotEntry> entries,
        IReadOnlyList<Guid>? sagaBlacklist = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentNullException.ThrowIfNull(causalStableFrontier);
        ArgumentNullException.ThrowIfNull(entries);

        TreeName = treeName;
        AsOfHlc = asOfHlc;
        CausalStableFrontier = causalStableFrontier;
        Entries = entries;
        SagaBlacklist = sagaBlacklist ?? Array.Empty<Guid>();
    }
}
