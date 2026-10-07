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

    /// <summary>
    /// Optional end-of-stream source generation trailer. When this value is
    /// present the message is a trailer rather than a snapshot entry.
    /// </summary>
    [Id(1)] public SnapshotSourceGeneration? CloseGeneration { get; init; }

    /// <summary>
    /// The source's applied low watermarks and held writes for the tree (issue
    /// #4586 part 2b), carried on the trailer beside
    /// <see cref="CloseGeneration"/>. A receiver that predates the slot ignores
    /// it.
    /// </summary>
    [Id(2)] internal SnapshotSourceFrontier? SourceFrontier { get; init; }

    /// <summary>
    /// On the trailer, the sibling boundaries the export captured at its end
    /// (issue #4684): per tree the source replicates, other than the exported
    /// one. Present only on an export served under the cross-tree hold.
    /// </summary>
    [Id(3)] internal System.Collections.Immutable.ImmutableDictionary<string, CrossTreeSiblingBoundary>? SiblingBoundaries { get; init; }

    /// <summary>
    /// On the trailer, the exported tree's own boundary captured at the export's
    /// end (issue #4524): its physical write-ahead log and every partition's
    /// next sequence. A receiver retires the saga decision rows the export
    /// carried once its shipper has vouched acknowledged positions at or past
    /// every tail; absent (a source that predates it), the rows are retained.
    /// </summary>
    [Id(4)] internal CrossTreeSiblingBoundary? ExportBoundary { get; init; }

    /// <summary>Whether this item is the trailer rather than an entry.</summary>
    internal bool IsTrailer => CloseGeneration is not null || SiblingBoundaries is not null || ExportBoundary is not null;
}