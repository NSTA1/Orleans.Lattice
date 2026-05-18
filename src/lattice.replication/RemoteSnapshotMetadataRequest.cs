using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Wire-shaped request DTO carrying the routing arguments for the
/// cross-cluster snapshot transport RPCs. Used by both the
/// <c>GetMetadata</c> unary RPC and the server-streaming
/// <c>RequestSnapshot</c> RPC the gRPC binding in
/// <c>Orleans.Lattice.Replication.Grpc</c> exposes; the binding marshals
/// this DTO across the wire and the server-side handler delegates to
/// <see cref="LatticeRemoteSnapshotService"/>.
/// <para>
/// The DTO is independent of the gRPC binding so it can be reused by
/// any future transport (HTTP/JSON, blob-store fetch, custom RPC) that
/// chooses to share the same wire shape. Orleans serializer is the
/// canonical encoder; the alias is stable
/// (<see cref="ReplicationTypeAliases.RemoteSnapshotMetadataRequest"/>).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.RemoteSnapshotMetadataRequest)]
[Immutable]
public readonly record struct RemoteSnapshotMetadataRequest
{
    /// <summary>
    /// Logical tree id the receiver wants a snapshot from. Mirrors
    /// the <c>treeName</c> argument on
    /// <see cref="IRemoteSnapshotTransport.GetMetadataAsync"/> and
    /// <see cref="IRemoteSnapshotTransport.RequestSnapshotAsync"/>.
    /// </summary>
    [Id(0)] public string TreeName { get; init; }

    /// <summary>
    /// Stable identifier of the sender cluster the receiver expects
    /// the snapshot from. Mirrors the <c>sourceClusterId</c> argument
    /// on the corresponding transport methods; the receiver uses it
    /// to key the per-origin high-water-mark after the cut-point is
    /// pinned.
    /// </summary>
    [Id(1)] public string SourceClusterId { get; init; }

    /// <summary>
    /// Strict upper-bound <see cref="HybridLogicalClock"/> filter
    /// the receiver wishes to pin. <see cref="HybridLogicalClock.Zero"/>
    /// disables the filter (the common case for a fresh peer).
    /// Mirrors the <c>fromAsOfHlc</c> argument on the corresponding
    /// transport methods.
    /// </summary>
    [Id(2)] public HybridLogicalClock FromAsOfHlc { get; init; }
}