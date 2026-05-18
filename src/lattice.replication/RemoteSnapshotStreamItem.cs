namespace Orleans.Lattice.Replication;

/// <summary>
/// Wire-shaped message DTO carrying a single per-message payload on
/// the gRPC server-streaming <c>RequestSnapshot</c> RPC exposed by
/// <c>Orleans.Lattice.Replication.Grpc</c>. Each stream message
/// carries exactly one <see cref="SnapshotEntry"/>; the receiver-side
/// transport adapter yields it through the
/// <see cref="IRemoteSnapshotTransport.RequestSnapshotAsync"/>
/// async-enumerable.
/// <para>
/// The DTO wraps <see cref="SnapshotEntry"/> rather than carrying it
/// directly so the stream message shape can evolve (e.g. add a
/// future <c>EndOfStream</c> marker, a progress counter, or a
/// chunked-batch payload) without breaking the alias of the per-entry
/// shape. Aliased as
/// <see cref="ReplicationTypeAliases.RemoteSnapshotStreamItem"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.RemoteSnapshotStreamItem)]
[Immutable]
public readonly record struct RemoteSnapshotStreamItem
{
    /// <summary>
    /// The snapshot entry carried by this stream message. Never
    /// <see langword="default"/> on the canonical wire path; a
    /// hand-constructed message that leaves this slot defaulted
    /// decodes as a zero-valued <see cref="SnapshotEntry"/> with
    /// an empty key/value.
    /// </summary>
    [Id(0)] public SnapshotEntry Entry { get; init; }
}