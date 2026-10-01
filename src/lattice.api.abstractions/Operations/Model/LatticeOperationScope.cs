namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// What a long-running operation acts on: the tenant that owns it and the
/// effective (tenant-scoped) trees it targets. Status reads, listing and
/// cancellation are authorized against this scope.
/// </summary>
[GenerateSerializer]
[Alias(ApiOperationTypeAliases.LatticeOperationScope)]
[Immutable]
public sealed record LatticeOperationScope
{
    /// <summary>The owning tenant id.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The effective trees the operation targets.</summary>
    [Id(1)] public IReadOnlyList<string> TreeIds { get; init; } = [];
}
