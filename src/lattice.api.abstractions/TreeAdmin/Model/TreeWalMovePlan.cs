namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A read-only preview of a WAL move operation, returned by the tree-admin WAL
/// move plan verb. Computes what the move would copy without quiescing the
/// partition or changing any placement, so an operator can review the work (and
/// confirm the target key resolves) before committing. The control-API mirror of
/// the core WAL move plan DTO.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalMovePlan)]
[Immutable]
public sealed record TreeWalMovePlan
{
    /// <summary>The tree whose partition would be moved.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The WAL partition that would be moved.</summary>
    [Id(1)] public int Partition { get; init; }

    /// <summary>The provider key currently backing the partition.</summary>
    [Id(2)] public string FromProviderKey { get; init; } = string.Empty;

    /// <summary>The requested target provider key.</summary>
    [Id(3)] public string ToProviderKey { get; init; } = string.Empty;

    /// <summary>
    /// The placement version the move must compare-and-swap against. A move
    /// computed from this plan aborts if the placement changes first.
    /// </summary>
    [Id(4)] public long PlacementVersion { get; init; }

    /// <summary>The lowest retained offset on the source, or <c>-1</c> when the source is empty.</summary>
    [Id(5)] public long SourceLowestOffset { get; init; }

    /// <summary>The highest offset on the source, or <c>-1</c> when the source is empty.</summary>
    [Id(6)] public long SourceHighestOffset { get; init; }

    /// <summary>The number of entries that would be copied to the target.</summary>
    [Id(7)] public long EntriesToCopy { get; init; }

    /// <summary>
    /// <see langword="true"/> when the target provider key resolves on the silo
    /// that produced this plan. A move whose target is unresolvable will fail
    /// closed; an operator should register the key on every silo first.
    /// </summary>
    [Id(8)] public bool TargetResolvableOnThisSilo { get; init; }

    /// <summary>
    /// <see langword="true"/> when the placement pin already maps the partition to
    /// <see cref="ToProviderKey"/>. Executing the move is then an idempotent
    /// no-copy repair.
    /// </summary>
    [Id(9)] public bool AlreadyAtTarget { get; init; }
}
