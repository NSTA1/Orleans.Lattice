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
    /// choose a different slug. It owns the literal route <c>/explore</c>.
    /// </summary>
    public const string Explore = "explore";

    /// <summary>
    /// The literal first segment every contributed area's address is namespaced
    /// under, as in <c>/area/tenants</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Explorer is an embeddable library mounted under a caller-chosen prefix,
    /// and it shares that mount with framework and static assets
    /// (<c>_framework/**</c>, <c>_content/**</c>, published files). A route whose
    /// <em>first</em> segment is a parameter can shadow any of them, which is not
    /// a cosmetic problem: an asset request that reaches the shell renders the
    /// whole admin console at an asset URL and picks up a second
    /// <c>Content-Security-Policy</c> header, and browsers apply the intersection
    /// of duplicated policies.
    /// </para>
    /// <para>
    /// So every declared route except the bare <c>/</c> begins with a literal
    /// segment. The home area owns <see cref="Explore"/> because it is the
    /// shell's own surface; a contributed area cannot own a literal (its slug is
    /// only known at run time), so it is namespaced here instead. The cost is one
    /// segment in a plugin area's URL; the benefit is that no contributed slug,
    /// present or future, can collide with an asset path.
    /// </para>
    /// </remarks>
    public const string AreaPathPrefix = "area";

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
