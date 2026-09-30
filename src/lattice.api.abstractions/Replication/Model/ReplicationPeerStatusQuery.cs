namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The query for <see cref="ILatticeReplicationStatus.GetPeerStatusAsync"/>:
/// optional tree and peer-region filters, a page size, and an opaque
/// continuation from a previous page.
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationPeerStatusQuery)]
[Immutable]
public sealed record ReplicationPeerStatusQuery
{
    /// <summary>The page size used when <see cref="PageSize"/> is zero.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>The largest page size honoured; a larger <see cref="PageSize"/> is clamped to it.</summary>
    public const int MaxPageSize = 1000;

    /// <summary>A query for the first page of every link the caller may see, at the default page size.</summary>
    public static ReplicationPeerStatusQuery All { get; } = new();

    /// <summary>
    /// When set, only links of this tree are reported. A tenant-local name, scoped
    /// to the caller's tenant before use; an id the report itself returned (for
    /// example <c>t/{tenant}/a/{app}/{tree}</c>) selects the same tree.
    /// <see langword="null"/> or empty reports every tree the caller may see.
    /// </summary>
    [Id(0)] public string? TreeId { get; init; }

    /// <summary>
    /// When set, only links to or from this peer region (cluster id) are
    /// reported. <see langword="null"/> or empty reports every peer.
    /// </summary>
    [Id(1)] public string? PeerRegionId { get; init; }

    /// <summary>
    /// The maximum number of rows in the page. Zero selects
    /// <see cref="DefaultPageSize"/>; a value above <see cref="MaxPageSize"/> is
    /// clamped to it; a negative value is rejected.
    /// </summary>
    [Id(2)] public int PageSize { get; init; }

    /// <summary>
    /// The opaque <see cref="ReplicationPeerStatusPage.ContinuationToken"/> of the
    /// previous page, or <see langword="null"/> for the first page. It is only
    /// meaningful with the same filters it was issued under.
    /// </summary>
    [Id(3)] public string? ContinuationToken { get; init; }

    /// <summary>
    /// Resolves the page size this query asks for: <see cref="DefaultPageSize"/>
    /// for zero, otherwise <see cref="PageSize"/> clamped to <see cref="MaxPageSize"/>.
    /// </summary>
    /// <returns>The page size to apply, in <c>[1, <see cref="MaxPageSize"/>]</c>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="PageSize"/> is negative.</exception>
    public int ResolvePageSize()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(PageSize, nameof(PageSize));
        return PageSize == 0 ? DefaultPageSize : Math.Min(PageSize, MaxPageSize);
    }
}
