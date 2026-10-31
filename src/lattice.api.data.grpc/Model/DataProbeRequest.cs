namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the capability-probe RPC. A serializable mirror of the
/// <c>(treeId, key)</c> argument pair routed onto
/// <c>ILatticeDataApi.ProbeCapabilitiesAsync</c>. The response reuses the
/// facade's <see cref="LatticeDataCapabilities"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.DataProbeRequest)]
[Immutable]
public sealed record DataProbeRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key to probe, or <see langword="null"/> to probe the tree as a whole.</summary>
    [Id(1)] public string? Key { get; init; }
}
