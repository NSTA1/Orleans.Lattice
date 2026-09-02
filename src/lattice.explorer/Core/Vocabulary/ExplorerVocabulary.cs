namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// The settled words the Explorer's user interface uses. One concept has one
/// name, declared here, so two surfaces cannot drift apart by inventing their
/// own wording.
/// </summary>
/// <remarks>
/// <para>
/// The pair this exists for is tenant administration. The application had two
/// near-identically named areas for one concept, "Tenants" and "My Tenant", and
/// nothing said which was which. The settled naming is:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///     <see cref="TenantAdministrationArea"/> - the operator surface, over every
///     tenant in the cluster. "Tenants" alone was the ambiguous half of the pair
///     and is not used.
///     </description>
///   </item>
///   <item>
///     <description>
///     <see cref="MyTenantArea"/> - the self-service surface, over the one tenant
///     the signed-in identity belongs to.
///     </description>
///   </item>
/// </list>
/// <para>
/// Every name here is a compile-time constant, so using one costs nothing at
/// render time. Composed forms that embed a runtime value are methods, and each
/// says what it allocates.
/// </para>
/// </remarks>
public static class ExplorerVocabulary
{
    /// <summary>The area listing trees, views and tag indexes and their detail.</summary>
    public const string ExploreArea = "Explore";

    /// <summary>The area for backup catalogues and restores.</summary>
    public const string BackupsArea = "Backups";

    /// <summary>The area for identities, roles and grants.</summary>
    public const string AccessArea = "Access";

    /// <summary>
    /// The operator-facing area covering every tenant in the cluster. Preferred
    /// over the bare word "Tenants", which did not distinguish itself from
    /// <see cref="MyTenantArea"/>.
    /// </summary>
    public const string TenantAdministrationArea = "Tenant administration";

    /// <summary>
    /// The short form of <see cref="TenantAdministrationArea"/>, for a narrow
    /// rail or a breadcrumb where the full name will not fit. Still
    /// distinguishable from <see cref="MyTenantArea"/>.
    /// </summary>
    public const string TenantAdministrationAreaShort = "Tenant admin";

    /// <summary>
    /// The self-service area covering only the signed-in identity's own tenant.
    /// Sentence case, so it does not read as a proper noun beside
    /// <see cref="TenantAdministrationArea"/>.
    /// </summary>
    public const string MyTenantArea = "My tenant";

    /// <summary>The area for cluster telemetry.</summary>
    public const string TelemetryArea = "Telemetry";

    /// <summary>The catalog's tree kind, as the kind toggle labels it.</summary>
    public const string TreesLabel = "Trees";

    /// <summary>The catalog's view kind, as the kind toggle labels it.</summary>
    public const string ViewsLabel = "Views";

    /// <summary>
    /// The unabbreviated name for <see cref="ViewsLabel"/>, for a heading or an
    /// explanation with room for it.
    /// </summary>
    public const string ViewsLongLabel = "Materialised views";

    /// <summary>The catalog's tag-index kind, as the kind toggle labels it.</summary>
    public const string TagIndexesLabel = "Tag indexes";

    /// <summary>What the catalog list as a whole is called.</summary>
    public const string CatalogLabel = "Catalog";

    /// <summary>
    /// How the header names the tenant the Explorer is reading as. Replaces the
    /// bare "TENANT" prefix, which named no concept.
    /// </summary>
    public const string ActiveTenantLabel = "Active tenant";

    /// <summary>How a control offers the cross-tenant listing scope.</summary>
    public const string AllTenantsLabel = "All tenants";

    /// <summary>
    /// The label introducing what a caller can do about a refusal. Matches the
    /// help primitive's default so a remedy reads the same however it is
    /// rendered.
    /// </summary>
    public const string RemedyLabel = "What to do:";

    /// <summary>
    /// Who to ask for a grant the caller does not hold, as it appears inside a
    /// remedy sentence.
    /// </summary>
    /// <remarks>
    /// Declared once because it is one concept and must have one name. The
    /// access gates each carried this word as their own literal while the copy
    /// layer composed its own, so the console said "ask a platform
    /// administrator" in the rail and "ask an operator" in the panel, for the
    /// same grant in the same session. "Operator" is the register the rest of
    /// this vocabulary uses - the glossary, the subjects and the term ids all
    /// name the operator surface - so it is the one that survives.
    /// </remarks>
    public const string GrantAudience = "an operator";

    /// <summary>
    /// Who to ask for a grant within your own tenant, as it appears inside a
    /// remedy sentence.
    /// </summary>
    /// <remarks>
    /// Deliberately distinct from <see cref="GrantAudience"/>: a self-service
    /// tenant surface is administered by the tenant's own administrator, who is
    /// not the platform operator. Sending a tenant member to an operator for a
    /// grant their own administrator issues would be a wrong instruction, not a
    /// wording variation, so these two are separate terms rather than one.
    /// </remarks>
    public const string TenantGrantAudience = "your tenant's administrator";

    /// <summary>The action that clears a filter or a scope and shows everything again.</summary>
    public const string ClearScopeAction = "Show all tenants";

    /// <summary>The action that retries a failed read.</summary>
    public const string RetryAction = "Try again";

    /// <summary>The action that starts an interactive sign-in.</summary>
    public const string SignInAction = "Sign in";

    /// <summary>
    /// The heading a detail surface shows before anything has been selected.
    /// </summary>
    public const string NoSelectionHeadline = "Nothing selected";

    /// <summary>
    /// What a detail surface says before anything has been selected. Names all
    /// three catalog kinds, because the old wording named only two of them.
    /// </summary>
    public const string NoSelectionExplanation =
        "Choose a tree, view or tag index from the catalog to inspect it.";

    /// <summary>
    /// How the header renders the active tenant, as
    /// <c>Active tenant: {tenantId}</c>.
    /// </summary>
    /// <remarks>
    /// Allocates one string per call because the result embeds a runtime tenant
    /// id. It is a chrome-level affordance rendered once per shell, not per list
    /// item, so it is not on a hot path; a caller that renders it inside a loop
    /// should compose <see cref="ActiveTenantLabel"/> and the id in markup
    /// instead.
    /// </remarks>
    /// <param name="tenantId">The active tenant's id.</param>
    /// <returns>The composed label.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenantId"/> is null.</exception>
    public static string FormatActiveTenant(string tenantId)
    {
        ArgumentNullException.ThrowIfNull(tenantId);
        return ActiveTenantLabel + ": " + tenantId;
    }
}
