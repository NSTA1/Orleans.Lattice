using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Carries the snapshot cut-point a sender cluster captures atomically
/// with the start of a remote snapshot stream. The cut-point lets the
/// receiver call
/// <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
/// before draining the entry stream, so the snapshot/incremental handoff
/// stays exactly-once even though the metadata RPC and the streaming
/// RPC are separate transport calls.
/// <para>
/// The metadata is captured once at the moment the sender begins
/// streaming and travels as a separate RPC ahead of the entry stream;
/// the corresponding <see cref="IRemoteSnapshotTransport.RequestSnapshotAsync"/>
/// call streams every entry whose
/// <see cref="SnapshotEntry.Timestamp"/> is less than or equal to
/// <see cref="AsOfHlc"/>. Splitting the cut-point onto its own RPC
/// avoids embedding cut-point markers inside the entry stream, which
/// would otherwise require the transport to be strictly ordered with
/// respect to the metadata frame.
/// </para>
/// <para>
/// Atomic-batch coordination is deliberately out of scope on this DTO.
/// Reconstructing receiver-side prepared-transaction state across a
/// cross-cluster bootstrap is tracked as a follow-on item; until it
/// lands, a producer running an in-flight multi-key transaction
/// concurrent with a cross-cluster bootstrap may deliver a split view
/// to the bootstrapping peer.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.RemoteSnapshotMetadata)]
[Immutable]
public readonly record struct RemoteSnapshotMetadata
{
    /// <summary>
    /// The logical tree id this snapshot was produced from. Matches
    /// the <c>treeName</c> passed to
    /// <see cref="IRemoteSnapshotTransport.GetMetadataAsync"/>.
    /// </summary>
    [Id(0)] public string TreeName { get; init; }

    /// <summary>
    /// The sender-cluster identifier the snapshot was captured on.
    /// Matches the <c>sourceClusterId</c> passed to
    /// <see cref="IRemoteSnapshotTransport.GetMetadataAsync"/>; the
    /// receiver uses it to key the per-origin high-water-mark
    /// <c>(treeName, sourceClusterId)</c> after the cut-point is pinned.
    /// </summary>
    [Id(1)] public string SourceClusterId { get; init; }

    /// <summary>
    /// The sender-side <see cref="HybridLogicalClock"/> at which the
    /// snapshot was captured. Entries delivered through
    /// <see cref="IRemoteSnapshotTransport.RequestSnapshotAsync"/>
    /// with a strictly greater
    /// <see cref="SnapshotEntry.Timestamp"/> are excluded; the
    /// receiver resumes incremental replication from this value after
    /// the snapshot drain completes. A value of
    /// <see cref="HybridLogicalClock.Zero"/> means "include every live
    /// entry regardless of timestamp" - the common case for a fresh
    /// peer with no incremental cursor yet.
    /// </summary>
    [Id(2)] public HybridLogicalClock AsOfHlc { get; init; }

    /// <summary>
    /// The sender's causal-stable frontier at the moment the snapshot
    /// was captured. Receivers pin this on
    /// <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
    /// before draining the entry stream so the causal dependency check
    /// on the first incremental entry runs from a non-empty frontier.
    /// Always non-null; the snapshot of an unreplicated tree carries
    /// the empty <see cref="Primitives.VersionVector"/>.
    /// </summary>
    [Id(3)] public VersionVector CausalStableFrontier { get; init; }
}
