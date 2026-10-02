using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The consent arithmetic behind the review: what an app needs, what a draft or
/// recorded consent covers, the activation failures a gap would cause (the same
/// checks the cluster applies), and the difference between two versions.
/// </summary>
internal static class AppConsentAnalysis
{
    /// <summary>The union of every role's requested operations: the ceiling the app needs.</summary>
    /// <param name="app">The described app.</param>
    public static LatticeOperation RequiredOperations(AppDescriptor app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.Roles.Aggregate(LatticeOperation.None, (mask, role) => mask | role.Operations);
    }

    /// <summary>
    /// The exception scopes the app needs: every role scope that names another app,
    /// and every adopted tree. Anything here lies outside <c>a/{slug}/</c>.
    /// </summary>
    /// <param name="app">The described app.</param>
    public static ImmutableArray<AppExceptionScope> RequiredScopes(AppDescriptor app)
    {
        ArgumentNullException.ThrowIfNull(app);
        var scopes = new List<AppExceptionScope>();
        foreach (var role in app.Roles)
        {
            foreach (var scope in role.Scopes)
            {
                if (IsOutsideNamespace(scope, app.Slug))
                {
                    Add(scopes, new AppExceptionScope { Kind = scope.Kind, App = scope.App, Tree = scope.Tree, KeyOrPrefix = scope.KeyOrPrefix });
                }
            }
        }

        foreach (var tree in app.Trees)
        {
            if (tree.AdoptedTreeId is { } adopted)
            {
                Add(scopes, new AppExceptionScope { AdoptedTreeId = adopted });
            }
        }

        return [.. scopes];
    }

    /// <summary>The bridge grants the app's UI requests; empty when it ships no UI.</summary>
    /// <param name="app">The described app.</param>
    public static ImmutableArray<AppUiBridgeGrantDescriptor> RequestedBridge(AppDescriptor app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.Ui is { } ui ? ui.Bridge : [];
    }

    /// <summary>Whether a role scope reaches outside the app's own <c>a/{slug}/</c> namespace.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="slug">The app's slug.</param>
    public static bool IsOutsideNamespace(AppRoleScope scope, string slug) =>
        scope.App is { } app && !string.Equals(app, slug, StringComparison.Ordinal);

    /// <summary>Whether <paramref name="granted"/> covers <paramref name="requested"/>: a grant with no tree covers every tree.</summary>
    /// <param name="granted">The consented grants.</param>
    /// <param name="requested">The requested grant.</param>
    public static bool Covers(IEnumerable<AppUiBridgeGrantDescriptor> granted, AppUiBridgeGrantDescriptor requested) =>
        granted.Any(grant =>
            string.Equals(grant.Operation, requested.Operation, StringComparison.Ordinal)
            && (grant.Tree is null || string.Equals(grant.Tree, requested.Tree, StringComparison.Ordinal)));

    /// <summary>
    /// Whether <paramref name="approved"/> covers <paramref name="required"/>, by the rule the
    /// cluster's role compiler applies: a whole-tree approval covers any extent of that tree, a
    /// prefix approval covers any key or narrower prefix that starts with it, and a key approval
    /// covers only that key.
    /// </summary>
    /// <param name="approved">The approved scopes.</param>
    /// <param name="required">The required scope.</param>
    public static bool Covers(IEnumerable<AppExceptionScope> approved, AppExceptionScope required) =>
        approved.Any(scope =>
            string.Equals(scope.AdoptedTreeId, required.AdoptedTreeId, StringComparison.Ordinal)
            && string.Equals(scope.App, required.App, StringComparison.Ordinal)
            && string.Equals(scope.Tree, required.Tree, StringComparison.Ordinal)
            && CoversExtent(scope, required));

    /// <summary>
    /// The failures an install or activation of <paramref name="app"/> under
    /// <paramref name="consent"/> would meet, in the order the cluster checks them.
    /// Empty means it would activate.
    /// </summary>
    /// <param name="app">The described app.</param>
    /// <param name="consent">The draft or recorded consent.</param>
    public static IReadOnlyList<AppActivationIssue> Preview(AppDescriptor app, AppConsentDraft consent)
    {
        ArgumentNullException.ThrowIfNull(app);
        ArgumentNullException.ThrowIfNull(consent);

        var issues = new List<AppActivationIssue>();
        foreach (var tree in app.Trees)
        {
            if (!string.IsNullOrWhiteSpace(tree.OwnershipConflict))
            {
                issues.Add(new(AppActivationIssueKind.TreeOwnershipConflict,
                    $"The tree {AppsPresentation.TreePath(app.Slug, tree.Name)} cannot be owned by this app, so install is refused."));
            }
        }

        foreach (var role in app.Roles)
        {
            var excess = role.Operations & ~consent.Operations;
            if (excess != LatticeOperation.None)
            {
                issues.Add(new(AppActivationIssueKind.CeilingExceeded,
                    $"The role {role.Name} asks to {AppsPresentation.OperationsText(excess)}, which the ceiling excludes: activation fails until it is approved."));
            }
        }

        foreach (var scope in RequiredScopes(app))
        {
            if (!Covers(consent.Scopes, scope))
            {
                issues.Add(new(AppActivationIssueKind.ScopeNotApproved,
                    $"{AppsPresentation.ScopeText(scope)} lies outside a/{app.Slug}/ and is not approved: activation fails until it is."));
            }
        }

        foreach (var grant in RequestedBridge(app))
        {
            if (!Covers(consent.BridgeGrants, grant))
            {
                issues.Add(new(AppActivationIssueKind.BridgeConsentRequired,
                    $"Its UI asks to {AppsPresentation.BridgeText(grant)}, which is not consented: activation fails until it is."));
            }
        }

        return issues;
    }

    /// <summary>
    /// Consent drift: the failures the installed version meets under its recorded
    /// consent. Empty means the consent still covers the installed manifest.
    /// </summary>
    /// <param name="installed">The installed version's description.</param>
    /// <param name="consent">The recorded consent.</param>
    public static IReadOnlyList<AppActivationIssue> Drift(AppDescriptor installed, AppConsentReport consent) =>
        Preview(installed, AppConsentDraft.FromReport(consent))
            .Where(issue => issue.Kind != AppActivationIssueKind.TreeOwnershipConflict)
            .ToArray();

    /// <summary>The difference between the installed version and the one under review.</summary>
    /// <param name="installed">The installed version's description.</param>
    /// <param name="next">The reviewed version's description.</param>
    /// <param name="consent">The installed version's recorded consent, or <see langword="null"/>.</param>
    public static AppUpgradeDiff Diff(AppDescriptor installed, AppDescriptor next, AppConsentReport? consent)
    {
        ArgumentNullException.ThrowIfNull(installed);
        ArgumentNullException.ThrowIfNull(next);

        var recorded = consent is null ? AppConsentDraft.Requested(installed) : AppConsentDraft.FromReport(consent);
        var oldTrees = installed.Trees.Select(tree => tree.Name).ToHashSet(StringComparer.Ordinal);
        var newTrees = next.Trees.Select(tree => tree.Name).ToHashSet(StringComparer.Ordinal);
        var oldRoles = installed.Roles.ToDictionary(role => role.Name, StringComparer.Ordinal);
        var newRoles = next.Roles.ToDictionary(role => role.Name, StringComparer.Ordinal);
        var oldBridge = RequestedBridge(installed);

        return new AppUpgradeDiff(next.Slug, installed.Version, next.Version)
        {
            TreesAdded = [.. newTrees.Where(name => !oldTrees.Contains(name)).Order(StringComparer.Ordinal)],
            TreesRemoved = [.. oldTrees.Where(name => !newTrees.Contains(name)).Order(StringComparer.Ordinal)],
            RolesAdded = [.. newRoles.Keys.Where(name => !oldRoles.ContainsKey(name)).Order(StringComparer.Ordinal)],
            RolesRemoved = [.. oldRoles.Keys.Where(name => !newRoles.ContainsKey(name)).Order(StringComparer.Ordinal)],
            RolesChanged = [.. newRoles.Values
                .Where(role => oldRoles.TryGetValue(role.Name, out var old) && !SameRole(old, role))
                .Select(role => role.Name)
                .Order(StringComparer.Ordinal)],
            CeilingAdded = RequiredOperations(next) & ~recorded.Operations,
            ScopesAdded = [.. RequiredScopes(next).Where(scope => !Covers(recorded.Scopes, scope))],
            BridgeAdded = [.. RequestedBridge(next).Where(grant => !Covers(recorded.BridgeGrants, grant))],
            BridgeRemoved = [.. oldBridge.Where(grant => !Covers(RequestedBridge(next), grant))],
        };
    }

    private static bool SameRole(AppRoleDescriptor a, AppRoleDescriptor b) =>
        a.Operations == b.Operations && a.Scopes.SequenceEqual(b.Scopes);

    // Mirrors AppRoleCompiler.IsCovered in Orleans.Lattice.Apps: the review previews the
    // cluster's decision, so it must be neither stricter nor looser than it.
    private static bool CoversExtent(AppExceptionScope approved, AppExceptionScope required) => approved.Kind switch
    {
        LatticeScopeKind.Tree => true,
        LatticeScopeKind.Prefix => required.Kind != LatticeScopeKind.Tree
            && approved.KeyOrPrefix is { } prefix
            && required.KeyOrPrefix is { } wanted
            && wanted.StartsWith(prefix, StringComparison.Ordinal),
        LatticeScopeKind.Key => required.Kind == LatticeScopeKind.Key
            && string.Equals(approved.KeyOrPrefix, required.KeyOrPrefix, StringComparison.Ordinal),
        _ => false,
    };

    private static void Add(List<AppExceptionScope> scopes, AppExceptionScope scope)
    {
        if (!scopes.Contains(scope))
        {
            scopes.Add(scope);
        }
    }
}
