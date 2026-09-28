namespace Orleans.Lattice.Apps;

/// <summary>
/// One entry of the tree ownership ledger (the reserved <c>sys-app-trees</c> tree), keyed by the
/// tree's tenant-composed id. Records which install owns the tree. A released claim is kept as a
/// tombstone value rather than deleted, so every ledger write stays a compare-and-set.
/// </summary>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppTreeClaim)]
internal sealed record AppTreeClaim
{
    /// <summary>The tenant of the owning install.</summary>
    [Id(0)] public required TenantId Tenant { get; init; }

    /// <summary>The slug of the owning install.</summary>
    [Id(1)] public required AppSlug Slug { get; init; }

    /// <summary>The publisher of the owning install, from the provenance its registry record carries.</summary>
    [Id(2)] public required string Publisher { get; init; }

    /// <summary>Whether the tree is the owner's structural tree or an adopted one.</summary>
    [Id(3)] public required AppTreeClaimKind Kind { get; init; }

    /// <summary>When the claim was taken.</summary>
    [Id(4)] public DateTimeOffset ClaimedAtUtc { get; init; }

    /// <summary>The registry revision of the install transition that took the claim.</summary>
    [Id(5)] public long InstallRevision { get; init; }

    /// <summary><c>true</c> once the claim has been released; the tree is then unowned.</summary>
    [Id(6)] public bool Released { get; init; }

    /// <summary>The owner identity the claim records.</summary>
    public AppTreeOwner Owner => new(Tenant, Slug, Publisher);
}
