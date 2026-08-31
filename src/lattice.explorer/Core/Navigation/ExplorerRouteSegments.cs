namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The reserved, canonical spellings the Explorer shell's own URLs are built
/// from: the home area slug, the catalog-kind slugs, and the query keys that
/// carry tenant scope.
/// </summary>
/// <remarks>
/// <para>
/// Only the shell's <em>own</em> vocabulary is declared here. An area contributed
/// by a plugin supplies its own slug (see
/// <see cref="ExplorerRoute.WithArea(string)"/>), and a downstream surface adds
/// its own query keys through <see cref="ExplorerRouteParameters"/>, so neither
/// has to edit this type. What is reserved is <see cref="Explore"/>: the shell's
/// built-in home area owns that slug, and a plugin must not claim it.
/// </para>
/// <para>
/// Every constant here is asserted lower case by the repository's route hygiene
/// test, so the epic-wide lower-case decision cannot drift through this file.
/// </para>
/// </remarks>
public static class ExplorerRouteSegments
{
    /// <summary>
    /// The area slug of the shell's built-in home surface, the catalog plus
    /// detail pairing that <c>/</c> resolves to. Reserved: a plugin area must
    /// choose a different slug.
    /// </summary>
    public const string Explore = "explore";

    /// <summary>The selection-kind slug for a tree selection.</summary>
    public const string Trees = "trees";

    /// <summary>The selection-kind slug for a view selection.</summary>
    public const string Views = "views";

    /// <summary>The selection-kind slug for a tag-index selection.</summary>
    public const string TagIndexes = "tag-indexes";

    /// <summary>
    /// The query key carrying the active tenant id, for example
    /// <c>?tenant=acme</c>. Absent means "whatever tenant scope the session
    /// resolves to"; present pins the view to one tenant so the link is
    /// reproducible for its recipient.
    /// </summary>
    public const string TenantQueryKey = "tenant";

    /// <summary>
    /// The query key carrying all-tenants visibility, for example
    /// <c>?all-tenants=true</c>. Only ever emitted when the flag is on, so the
    /// common URL stays short.
    /// </summary>
    public const string AllTenantsQueryKey = "all-tenants";

    /// <summary>
    /// The value <see cref="AllTenantsQueryKey"/> carries when the flag is on.
    /// Parsing also accepts <c>1</c> so a hand-written link works.
    /// </summary>
    public const string TrueValue = "true";
}
