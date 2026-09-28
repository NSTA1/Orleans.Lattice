using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// The Access area's addresses. The area is cluster-wide, so none of them is
/// ever rooted at a tenant; the navigator strips a tenant node if one arrives.
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
}
