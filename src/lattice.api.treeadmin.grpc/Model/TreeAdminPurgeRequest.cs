namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the irreversible tree hard-purge lifecycle RPC: a tree id plus
/// the explicit confirmation flag the facade requires before it destroys a
/// soft-deleted tree's data. The flag is carried on the wire (rather than implied)
/// so an unconfirmed purge is rejected by the facade with the same
/// <c>InvalidArgument</c> mapping a local caller sees.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminPurgeRequest)]
[Immutable]
public sealed record TreeAdminPurgeRequest
{
    /// <summary>The tree to hard-purge.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>Must be <see langword="true"/> to acknowledge the irreversible purge.</summary>
    [Id(1)] public bool Confirm { get; init; }
}
