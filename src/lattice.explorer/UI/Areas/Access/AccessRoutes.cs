using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The Access area's addresses. They are the cluster-wide forms; a page at a
/// tenant-rooted address roots the links it draws at its own tenant
/// (<c>WithTenant</c>), so the listing a link leads to keeps the page's scope.
/// </summary>
internal static class AccessRoutes
{
    /// <summary>The area key.</summary>
    public const string AreaKey = "access";

    /// <summary>The rules segment.</summary>
    public const string RulesSegment = "rules";

    /// <summary>The groups segment.</summary>
    public const string GroupsSegment = "groups";

    /// <summary>The explain segment.</summary>
    public const string ExplainSegment = "explain";

    /// <summary>The members segment: a tenant's member set, at a tenant-rooted address only.</summary>
    public const string MembersSegment = "members";

    /// <summary>The route template of a tenant's member set, the one Access route with no cluster-wide form.</summary>
    public const string TenantMembersRoute = "/t/{tenant}/access/members";

    /// <summary>The query key naming the governed tree of a rule, which disambiguates a rule id reused across trees.</summary>
    public const string TreeQuery = "tree";

    /// <summary>The query key that opens a page's create form, used by the palette's create commands.</summary>
    public const string NewQuery = "new";

    /// <summary>The query key naming the subject on the explain page.</summary>
    public const string SubjectQuery = "subject";

    /// <summary>The query key naming the subject kind (<c>user</c> or <c>group</c>) on the explain page.</summary>
    public const string KindQuery = "kind";

    /// <summary>The query key naming the operation on the explain page.</summary>
    public const string OperationQuery = "operation";

    /// <summary>The query key choosing the explain page's view: <c>permissions</c> for effective permissions.</summary>
    public const string ViewQuery = "view";

    /// <summary>The <see cref="ViewQuery"/> value that shows effective permissions.</summary>
    public const string PermissionsView = "permissions";

    /// <summary>The area root.</summary>
    public static ExplorerAddress Root { get; } = ExplorerAddress.ForArea(AreaKey);

    /// <summary>The rule list.</summary>
    public static ExplorerAddress Rules { get; } = ExplorerAddress.ForArea(AreaKey, RulesSegment);

    /// <summary>The group list.</summary>
    public static ExplorerAddress Groups { get; } = ExplorerAddress.ForArea(AreaKey, GroupsSegment);

    /// <summary>The explain page.</summary>
    public static ExplorerAddress Explain { get; } = ExplorerAddress.ForArea(AreaKey, ExplainSegment);

    /// <summary>The page of the rule <paramref name="ruleId"/>, qualified by its governed tree when known.</summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="treeId">The rule's governed tree id, or <see langword="null"/>.</param>
    public static ExplorerAddress Rule(string ruleId, string? treeId = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        var address = ExplorerAddress.ForArea(AreaKey, RulesSegment, ruleId);
        return string.IsNullOrEmpty(treeId) ? address : address.WithQuery(TreeQuery, treeId);
    }

    /// <summary>The page of the group <paramref name="groupId"/>.</summary>
    /// <param name="groupId">The group id.</param>
    public static ExplorerAddress Group(string groupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return ExplorerAddress.ForArea(AreaKey, GroupsSegment, groupId);
    }

    /// <summary>The Apps area's roles page for the app <paramref name="slug"/>, where its rules are bound.</summary>
    /// <param name="slug">The app slug.</param>
    public static ExplorerAddress AppRoles(string slug)
    {
        ArgumentException.ThrowIfNullOrEmpty(slug);
        return ExplorerAddress.ForArea("apps", slug, "roles");
    }

    // ----- Delegated tenant access administration (epic #4154) -----
    //
    // The tenant-rooted forms of the area, served by the tenant pages when the
    // posture probe reports the feature enabled for a caller who administers the
    // tenant (or is a platform operator); otherwise the same addresses keep their
    // cluster-wide pages. Groups and rules are named by their tenant-local name
    // and id: the facade composes t/{tenant}/{name} and tenant:{tenant}:{id}.

    /// <summary>A tenant's Access root (<c>/t/{tenant}/access</c>).</summary>
    /// <param name="tenant">The tenant.</param>
    public static ExplorerAddress TenantRoot(string tenant) => Root.WithTenant(RequireTenant(tenant));

    /// <summary>A tenant's own groups (<c>/t/{tenant}/access/groups</c>).</summary>
    /// <param name="tenant">The tenant.</param>
    public static ExplorerAddress TenantGroups(string tenant) => Groups.WithTenant(RequireTenant(tenant));

    /// <summary>One of a tenant's own groups (<c>/t/{tenant}/access/groups/{name}</c>), by tenant-local name.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The group's tenant-local name.</param>
    public static ExplorerAddress TenantGroup(string tenant, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return ExplorerAddress.ForArea(AreaKey, GroupsSegment, name).WithTenant(RequireTenant(tenant));
    }

    /// <summary>A tenant's member set (<c>/t/{tenant}/access/members</c>).</summary>
    /// <param name="tenant">The tenant.</param>
    public static ExplorerAddress TenantMembers(string tenant) =>
        ExplorerAddress.ForArea(AreaKey, MembersSegment).WithTenant(RequireTenant(tenant));

    /// <summary>The rules governing a tenant (<c>/t/{tenant}/access/rules</c>).</summary>
    /// <param name="tenant">The tenant.</param>
    public static ExplorerAddress TenantRules(string tenant) => Rules.WithTenant(RequireTenant(tenant));

    /// <summary>One of a tenant's tenant-tier rules (<c>/t/{tenant}/access/rules/{localId}</c>), by local id.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="localId">The rule's tenant-local id.</param>
    public static ExplorerAddress TenantRule(string tenant, string localId)
    {
        ArgumentException.ThrowIfNullOrEmpty(localId);
        return ExplorerAddress.ForArea(AreaKey, RulesSegment, localId).WithTenant(RequireTenant(tenant));
    }

    /// <summary>The layer-aware explain page of a tenant (<c>/t/{tenant}/access/explain</c>).</summary>
    /// <param name="tenant">The tenant.</param>
    public static ExplorerAddress TenantExplain(string tenant) => Explain.WithTenant(RequireTenant(tenant));

    /// <summary>
    /// The sections of a tenant's delegated Access pages, in navigation order:
    /// Groups, Members, Rules and Explain.
    /// </summary>
    /// <param name="tenant">The tenant.</param>
    /// <returns>Each section's segment, label and address.</returns>
    public static IReadOnlyList<(string Segment, string Label, ExplorerAddress Address)> TenantSections(string tenant) =>
    [
        (GroupsSegment, "Groups", TenantGroups(tenant)),
        (MembersSegment, "Members", TenantMembers(tenant)),
        (RulesSegment, "Rules", TenantRules(tenant)),
        (ExplainSegment, "Explain", TenantExplain(tenant)),
    ];

    private static string RequireTenant(string tenant)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenant);
        return tenant;
    }
}
