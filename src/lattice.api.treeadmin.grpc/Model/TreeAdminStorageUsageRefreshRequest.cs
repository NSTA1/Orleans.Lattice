namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request of the accept-then-poll <c>StartStorageUsageRefresh</c> RPC: an
/// optional caller-chosen idempotency id for the tracked refresh. The refresh spans
/// the whole cluster, so it carries no tree id.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminStorageUsageRefreshRequest)]
[Immutable]
public sealed record TreeAdminStorageUsageRefreshRequest
{
    /// <summary>The optional idempotency id; <see langword="null"/> lets the server generate one.</summary>
    [Id(0)] public string? OperationId { get; init; }
}