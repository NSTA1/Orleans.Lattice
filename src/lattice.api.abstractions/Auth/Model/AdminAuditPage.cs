namespace Orleans.Lattice.Api.Auth;

/// <summary>One page of admin audit records, newest first.</summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AdminAuditPage)]
[Immutable]
public sealed record AdminAuditPage
{
    /// <summary>The records on this page, newest first.</summary>
    [Id(0)] public IReadOnlyList<LatticeAdminAuditRecord> Records { get; init; } = Array.Empty<LatticeAdminAuditRecord>();

    /// <summary>
    /// The cursor for the next page, or <see langword="null"/> when this is the last page.
    /// Pass it as <see cref="AdminAuditQuery.PageToken"/> with the same filters.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
