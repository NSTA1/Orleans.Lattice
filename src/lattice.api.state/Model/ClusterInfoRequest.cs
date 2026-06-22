namespace Orleans.Lattice.Api.State;

/// <summary>
/// Request for <see cref="ILatticeStateQuery.GetClusterInfoAsync"/>. It carries
/// no fields today: cluster info is a single, cluster-wide record. The type
/// exists so the RPC has a stable request envelope that can grow additive
/// filter / projection options later without changing the method signature.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.ClusterInfoRequest)]
[Immutable]
public sealed record ClusterInfoRequest
{
}
