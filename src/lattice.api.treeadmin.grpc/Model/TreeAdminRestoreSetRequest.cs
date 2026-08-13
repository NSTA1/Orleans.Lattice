namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for the <c>RestoreTreeSet</c> RPC: the content-addressed id of the
/// backup set to restore as a single unit.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminRestoreSetRequest)]
[Immutable]
public sealed record TreeAdminRestoreSetRequest
{
    /// <summary>The content-addressed id of the backup set to restore.</summary>
    [Id(0)] public required string SetId { get; init; }
}
