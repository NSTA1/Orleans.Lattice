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
    /// snapshot was produced - the pointwise minimum
    /// <see cref="VersionVector"/> across every consumer that has
    /// reported a vector through
    /// <see cref="IWalCursorRegistry.GetCausalStableAsync"/>.
    /// Receivers pin this on
    /// <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
    /// before draining <see cref="Entries"/>, so the causal dependency
    /// check in the apply path starts from a non-empty frontier and
    /// the first incremental entry is guaranteed to satisfy its
    /// declared dependencies. When no consumer has reported a
    /// VC-shaped cursor, the provider falls back to the producer's
    /// per-tree local vector clock - a strict superset of the meet
    /// that is safe as a snapshot cut-point because no entry can have
    /// a VC component above the producer's own local VC at capture
    /// time. Always non-null; the snapshot of an unreplicated tree
    /// returns the empty <see cref="VersionVector"/>.
    /// </summary>
    public VersionVector CausalStableFrontier { get; }

    /// <summary>
    /// Async stream of every key the source tree carries at
    /// <see cref="AsOfHlc"/>: every live committed entry whose
    /// <see cref="SnapshotEntry.Timestamp"/> is less than or equal to
    /// <see cref="AsOfHlc"/> (or every live entry, when
    /// <see cref="AsOfHlc"/> is <see cref="HybridLogicalClock.Zero"/>),
    /// plus the producer's saga-prepared per-key mutations that the
    /// producer's tx registry had not yet decided at the snapshot's
    /// linearization point. The committed-projection rows are emitted
    /// in lexicographic key order; saga-prepared rows
    /// (<see cref="SnapshotEntry.IsPrepared"/> =
    /// <see langword="true"/>) may be interleaved or trailing
    /// depending on the producer's enumeration strategy. Tombstoned
    /// and expired keys are not emitted on the committed-projection
    /// path; prepared deletes are emitted with
    /// <see cref="SnapshotEntry.IsTombstone"/> set.
    /// <para>
    /// Prepared entries carry the source saga's transaction id so the
    /// receiver can route them into the per-tx pending bucket via
    /// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedSetAsync"/>
    /// / <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>;
    /// the matching terminal record arrives subsequently via the
    /// post-snapshot incremental WAL stream and flips visibility
    /// atomically per saga through
    /// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyTxTerminalAsync"/>.
    /// Sagas the producer's tx registry already decided at snapshot
    /// time are folded into the committed-projection stream (Committed
    /// outcomes inline the post-saga value; Aborted outcomes drop the
    /// prepared mutation entirely), so no separate terminal-decision
    /// segment is required.
    /// </para>
    /// </summary>
    public IAsyncEnumerable<SnapshotEntry> Entries { get; }

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
    public SnapshotStream(
        string treeName,
        HybridLogicalClock asOfHlc,
        VersionVector causalStableFrontier,
        IAsyncEnumerable<SnapshotEntry> entries)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentNullException.ThrowIfNull(causalStableFrontier);
        ArgumentNullException.ThrowIfNull(entries);

        TreeName = treeName;
        AsOfHlc = asOfHlc;
        CausalStableFrontier = causalStableFrontier;
        Entries = entries;
    }
}
