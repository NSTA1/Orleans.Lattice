namespace Orleans.Lattice.Api.State;

/// <summary>
/// Identity and high-level metadata for the cluster a state-API client is
/// connected to. Returned by <see cref="ILatticeStateQuery.GetClusterInfoAsync"/>
/// so a consumer (for example the explorer header) can show which cluster it is
/// looking at.
/// </summary>
/// <remarks>
/// The record is intentionally generic and additive: it surfaces the cluster
/// identity today and is designed to grow over time (region, silo count,
/// version, and similar metadata) by appending new <c>[Id(n)]</c> members, so a
/// newer server decodes cleanly under an older client.
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.ClusterInfo)]
[Immutable]
public sealed record ClusterInfo
{
    /// <summary>
    /// The Orleans cluster id the connected silo belongs to (the deployment's
    /// logical cluster identity). Empty when the host did not configure one.
    /// </summary>
    [Id(0)] public string ClusterId { get; init; } = string.Empty;

    /// <summary>
    /// The Orleans service id the connected silo belongs to (stable across
    /// rolling deployments of the same logical service). Empty when the host did
    /// not configure one.
    /// </summary>
    [Id(1)] public string ServiceId { get; init; } = string.Empty;
}
