using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// One page of the authorization rule catalog. Entries are the durable
/// <see cref="LatticeAuthorizationRule"/> policy model surfaced directly, so a
/// binding sees the same rule shape the store persists.
/// <see cref="NextPageToken"/> is the cursor to pass back in the next
/// <see cref="AuthPageRequest"/> to continue enumeration; it is
/// <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthRulePage)]
[Immutable]
public sealed record AuthRulePage
{
    /// <summary>
    /// The rules on this page, ordered by <c>(governed tree id, rule id)</c>.
    /// </summary>
    [Id(0)] public IReadOnlyList<LatticeAuthorizationRule> Entries { get; init; } = Array.Empty<LatticeAuthorizationRule>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }

    /// <summary>
    /// The tenant this page was narrowed to when the request asked for
    /// <see cref="AuthPageRequest.ActiveTenantOnly"/>, or <see langword="null"/>
    /// for a page of the whole catalogue. A caller that asked for a narrowed page
    /// and reads <see langword="null"/> here is talking to a server that predates
    /// the narrowing, and was answered the whole catalogue.
    /// </summary>
    [Id(2)] public string? Tenant { get; init; }

    /// <summary>
    /// The owning tenant of each tenant-tier rule on this page: the rules a
    /// tenant's own administrators authored over its trees, whose ids carry the
    /// reserved <c>tenant:{tenant}:</c> prefix. Empty when no entry on the page is a
    /// tenant-tier rule (and from a server that predates this member); otherwise
    /// index-aligned with <see cref="Entries"/>, holding the tenant id for a
    /// tenant-tier rule and <see langword="null"/> for an operator rule.
    /// </summary>
    /// <remarks>
    /// Operators can list and remove tenant-tier rules but cannot author them; the
    /// tenant id is the same one <c>LatticeTenantRuleIds.TryGetTenant</c> parses
    /// from the rule id.
    /// </remarks>
    [Id(3)] public IReadOnlyList<string?> TenantRuleTenants { get; init; } = Array.Empty<string?>();
}
