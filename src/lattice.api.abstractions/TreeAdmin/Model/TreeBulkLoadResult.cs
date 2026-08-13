namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The result of finalizing a resumable bulk-load session with
/// <see cref="ILatticeTreeAdmin.CommitBulkLoadAsync"/>. Commit is the caller's
/// explicit end-of-stream marker: it re-authorizes the whole-tree
/// <see cref="LatticeOperation.BulkLoad"/> grant and confirms the load is
/// complete. The grafted entries are already durable as each chunk was
/// acknowledged, so commit persists no further data.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeBulkLoadResult)]
[Immutable]
public sealed record TreeBulkLoadResult
{
    /// <summary>The tree the bulk-load populated.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The session operation id that was committed.</summary>
    [Id(1)] public required string OperationId { get; init; }

    /// <summary>
    /// The tree's live key count observed at commit time, sampled from the cheap
    /// per-shard projection. A convenience total for the caller to sanity-check
    /// its load against; not an authoritative deep count.
    /// </summary>
    [Id(2)] public required long TotalLiveKeys { get; init; }
}
