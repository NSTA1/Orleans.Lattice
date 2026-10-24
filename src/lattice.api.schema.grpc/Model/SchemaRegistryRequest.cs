using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Appends immutable runtime schema definitions to an effective tree's catalog.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaRegistryRequest)]
[Immutable]
public sealed record SchemaRegistryRequest
{
    /// <summary>The tenant-local tree name.</summary>
    [Id(0)] public required string TreeId { get; init; }
    /// <summary>The descriptors and migration hops to register.</summary>
    [Id(1)] public required LatticeSchemaRuntimeRegistration Registration { get; init; }
}
