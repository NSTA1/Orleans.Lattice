using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Everything the app bridge needs to authorize a request against one enabled install, derived once per
/// compiled install (and so once per registry record revision): the bridge grants both the operator consented
/// to and the installed manifest requests, the declared trees resolved server-side to their effective ids, and
/// the app-owned role grants over each of those trees.
/// </summary>
/// <remarks>
/// <para>
/// <b>Only app-owned grants.</b> A <see cref="TreeGrant"/> is exactly what an <c>app:{slug}</c> rule the role
/// compiler writes says: a role binding's membership group, the role's operations, and one of the role's
/// scopes. The grants are built from the install's compiled <see cref="AppRoleGate"/>s - the same definition of
/// "holds a role" the app workspace and the app MCP tools report - so a caller is only ever offered what the
/// bridge then allows. Nothing here reads the caller's other rules, which is what stops a caller's broad
/// operator rights flowing into the app's UI.
/// </para>
/// <para>
/// <b>The ceiling is re-checked.</b> A grant's operations are the role's operations intersected with the
/// install's consented ceiling and the operations a role may ever carry, and a grant over an adopted tree
/// exists only while an approved exception scope of the ceiling covers it. A stale projection therefore cannot
/// widen what a role confers.
/// </para>
/// </remarks>
internal sealed class AppBridgeInstallPlan
{
    private readonly Dictionary<string, TreePlan> _trees;

    private AppBridgeInstallPlan(AppUiBridgeRequest consented, AppUiBridgeRequest requested, Dictionary<string, TreePlan> trees)
    {
        Consented = consented;
        Requested = requested;
        _trees = trees;
    }

    /// <summary>The bridge grants the operator consented to; empty when none was recorded.</summary>
    public AppUiBridgeRequest Consented { get; }

    /// <summary>The bridge grants the installed manifest requests; empty when it requests none or is malformed.</summary>
    public AppUiBridgeRequest Requested { get; }

    /// <summary>Builds the plan for a compiled install.</summary>
    /// <param name="install">The compiled install.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="install"/> is null.</exception>
    public static AppBridgeInstallPlan Build(AppRoleGrantInstall install)
    {
        ArgumentNullException.ThrowIfNull(install);
        var record = install.Record;
        var manifest = install.Manifest;

        AppUiBridgeRequest requested;
        try
        {
            requested = AppUiBridgeRequest.FromManifest(manifest);
        }
        catch (ArgumentException)
        {
            requested = AppUiBridgeRequest.Empty;
        }

        var trees = new Dictionary<string, TreePlan>(StringComparer.Ordinal);
        foreach (var declaration in manifest.Trees ?? [])
        {
            if (declaration is null || !AppManifestValidator.IsName(declaration.Name) || trees.ContainsKey(declaration.Name))
            {
                continue;
            }

            var adopted = declaration.AdoptedTreeId is not null;
            if (adopted && !AppTreeIds.IsAdoptable(declaration.AdoptedTreeId))
            {
                continue;
            }

            var local = declaration.AdoptedTreeId ?? string.Concat(LatticeConstants.AppTreePrefix, record.Slug.Value, "/", declaration.Name);
            string effective;
            try
            {
                effective = LatticeTenantResolution.ComposeEffectiveTreeId(record.Tenant, local);
            }
            catch (LatticeTenantAccessDeniedException)
            {
                continue;
            }

            trees.Add(declaration.Name, new TreePlan(local, effective, BuildGrants(install, local, effective, adopted)));
        }

        return new AppBridgeInstallPlan(record.ConsentedBridge ?? AppUiBridgeRequest.Empty, requested, trees);
    }

    /// <summary>Resolves a declared, app-local tree name.</summary>
    /// <param name="logicalTree">The app-local tree name.</param>
    /// <param name="tree">The resolved tree when declared.</param>
    /// <returns>Whether the install declares the tree.</returns>
    public bool TryGetTree(string logicalTree, out TreePlan tree) => _trees.TryGetValue(logicalTree, out tree!);

    private static TreeGrant[] BuildGrants(AppRoleGrantInstall install, string local, string effective, bool adopted)
    {
        var ceiling = install.Record.Ceiling;
        List<TreeGrant>? grants = null;
        foreach (var role in install.Roles)
        {
            // The same compiled role the workspace and the MCP tool gate report as held: its operations are
            // already intersected with the ceiling and its groups are the install's bindings.
            if (!role.ConfersGrant)
            {
                continue;
            }

            foreach (var scope in role.Scopes)
            {
                if (!string.Equals(scope.TreeId, effective, StringComparison.Ordinal)
                    || (scope.Kind != LatticeScopeKind.Tree && scope.KeyOrPrefix is null)
                    || (adopted && !IsCoveredByException(new LatticeScope(scope.Kind, local, scope.KeyOrPrefix), ceiling)))
                {
                    continue;
                }

                foreach (var groupId in role.GroupIds)
                {
                    (grants ??= []).Add(new TreeGrant(groupId, role.Operations, scope.Kind, scope.KeyOrPrefix));
                }
            }
        }

        return grants is null ? [] : grants.ToArray();
    }

    /// <summary>
    /// Whether an approved exception scope of the ceiling covers a tenant-local scope over an adopted tree,
    /// with the role compiler's coverage rule: same tree; a tree exception covers any scope; a prefix exception
    /// covers a key or prefix scope that starts with it; a key exception covers only the identical key scope.
    /// </summary>
    private static bool IsCoveredByException(LatticeScope requested, AppCapabilityCeiling? ceiling)
    {
        if (ceiling is null || !AppTreeIds.IsGrantable(requested.TreeId))
        {
            return false;
        }

        foreach (var exception in ceiling.ApprovedExceptionScopes ?? [])
        {
            if (exception is null || !string.Equals(exception.TreeId, requested.TreeId, StringComparison.Ordinal))
            {
                continue;
            }

            var covered = exception.Kind switch
            {
                LatticeScopeKind.Tree => true,
                LatticeScopeKind.Prefix => requested.Kind != LatticeScopeKind.Tree
                    && exception.KeyOrPrefix is not null
                    && requested.KeyOrPrefix!.StartsWith(exception.KeyOrPrefix, StringComparison.Ordinal),
                LatticeScopeKind.Key => requested.Kind == LatticeScopeKind.Key
                    && string.Equals(requested.KeyOrPrefix, exception.KeyOrPrefix, StringComparison.Ordinal),
                _ => false,
            };
            if (covered)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>One declared tree: its tenant-local and effective ids and the app-owned grants over it.</summary>
    /// <param name="LocalTreeId">The tenant-local tree id: <c>a/{slug}/{tree}</c>, or the adopted tree id.</param>
    /// <param name="EffectiveTreeId">The tenant-composed tree id the data path addresses.</param>
    /// <param name="Grants">The app-owned grants over the tree; empty when no bound role reaches it.</param>
    internal readonly record struct TreePlan(string LocalTreeId, string EffectiveTreeId, TreeGrant[] Grants)
    {
        /// <summary>
        /// Whether a caller whose group closure is <paramref name="groups"/> holds an app-owned grant allowing
        /// <paramref name="operation"/> over the key <paramref name="key"/>, or over every key under the prefix
        /// <paramref name="prefix"/> when <paramref name="key"/> is null.
        /// </summary>
        /// <param name="groups">The caller's transitive group closure.</param>
        /// <param name="operation">The single concrete operation.</param>
        /// <param name="key">The concrete key of a point request, or null for a prefix request.</param>
        /// <param name="prefix">The concrete prefix of a prefix request; ignored for a point request.</param>
        /// <returns><c>true</c> when at least one grant allows the request.</returns>
        public bool Allows(IReadOnlyCollection<string>? groups, LatticeOperation operation, string? key, string prefix)
        {
            if (groups is null || groups.Count == 0 || operation == LatticeOperation.None)
            {
                return false;
            }

            foreach (var grant in Grants)
            {
                if ((grant.Operations & operation) == operation
                    && grant.Covers(key, prefix)
                    && AppRoleGate.IsMember(groups, grant.GroupId))
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>One app-owned grant: what one compiled <c>app:{slug}</c> rule allows over one tree.</summary>
    /// <param name="GroupId">The membership group the role is bound to.</param>
    /// <param name="Operations">The role's operations, intersected with the ceiling.</param>
    /// <param name="Kind">The scope kind.</param>
    /// <param name="KeyOrPrefix">The scope's key or prefix; null for a tree scope.</param>
    internal readonly record struct TreeGrant(string GroupId, LatticeOperation Operations, LatticeScopeKind Kind, string? KeyOrPrefix)
    {
        /// <summary>Whether the scope covers the key, or every key under the prefix when the key is null.</summary>
        /// <param name="key">The concrete key, or null for a prefix request.</param>
        /// <param name="prefix">The concrete prefix of a prefix request.</param>
        /// <returns><c>true</c> when covered.</returns>
        public bool Covers(string? key, string prefix) => Kind switch
        {
            LatticeScopeKind.Tree => true,
            LatticeScopeKind.Prefix => KeyOrPrefix is not null
                && (key ?? prefix).StartsWith(KeyOrPrefix, StringComparison.Ordinal),
            LatticeScopeKind.Key => key is not null && string.Equals(key, KeyOrPrefix, StringComparison.Ordinal),
            _ => false,
        };
    }
}
