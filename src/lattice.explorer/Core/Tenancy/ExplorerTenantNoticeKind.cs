namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// What a tenant scope notice is telling the caller. Determines both the wording
/// and how it is announced: an outcome the caller can act on is assertive, an
/// applied change is polite.
/// </summary>
public enum ExplorerTenantNoticeKind
{
    /// <summary>The requested scope change was applied.</summary>
    Applied,

    /// <summary>
    /// The request was refused fail-closed because the caller is not a validated
    /// platform operator. Nothing changed.
    /// </summary>
    Refused,

    /// <summary>
    /// The requested tenant is not one the caller can reach, so it was not
    /// applied. Distinct from <see cref="Refused"/>: the caller may switch
    /// tenant, just not to that one.
    /// </summary>
    Unknown,

    /// <summary>
    /// A remembered tenant no longer resolves against the caller's current
    /// accessible tenants, so the scope fell back to a reachable one. The case
    /// that must never happen silently.
    /// </summary>
    RestoreAbandoned,
}
