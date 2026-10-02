namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying only a <see cref="Deep"/> flag, used by the
/// tree-administration control-API cluster-wide storage accounting RPC. It
/// addresses no single tree (the summary spans the whole cluster), so it carries no
/// tree id.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminStorageUsageRequest)]
[Immutable]
public sealed record TreeAdminStorageUsageRequest
{
    /// <summary>
    /// When <see langword="true"/>, force an expensive fresh leaf-walk that
    /// re-measures every shard; otherwise each tree uses its short-lived
    /// storage-usage cache refilled from shard-root byte totals and WAL partitions.
    /// </summary>
    [Id(0)] public bool Deep { get; init; }
}
