namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the snapshot-capture trigger RPC: a source tree id plus the
/// destination tree id, snapshot mode, and optional destination sizing overrides.
/// The parameters are carried on the wire so the facade applies the same argument
/// validation and <c>InvalidArgument</c> / <c>FailedPrecondition</c> mapping a local
/// caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminSnapshotRequest)]
[Immutable]
public sealed record TreeAdminSnapshotRequest
{
    /// <summary>The source tree to snapshot.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The destination tree id to snapshot into.</summary>
    [Id(1)] public required string DestinationTreeId { get; init; }

    /// <summary>Whether to quiesce the source tree during the copy.</summary>
    [Id(2)] public TreeSnapshotMode Mode { get; init; }

    /// <summary>Optional leaf sizing override for the destination tree.</summary>
    [Id(3)] public int? MaxLeafKeys { get; init; }

    /// <summary>Optional internal-node sizing override for the destination tree.</summary>
    [Id(4)] public int? MaxInternalChildren { get; init; }
}
