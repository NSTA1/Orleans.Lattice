using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Well-known names for the reserved, dogfooded <c>ILattice</c> tree that backs
/// the authorization policy store, plus the guard that keeps that namespace from
/// being shadowed by an application tree. Like the sibling membership package,
/// the policy tree is an ordinary user-addressable tree (it does <b>not</b> use
/// the core <c>_lattice_</c> system-tree prefix) so the policy is fully
/// introspectable through the standard read / scan / change-feed surface. The
/// <see cref="ReservedTreePrefix"/> convention reserves the namespace from
/// collision with application trees.
/// </summary>
internal static class AuthConstants
{
    /// <summary>
    /// The shared prefix identifying every authorization-owned reserved tree. A
    /// governed tree id colliding with this prefix is rejected by
    /// <see cref="ThrowIfReservedTree"/> so an application tree can never shadow
    /// the policy store.
    /// </summary>
    internal const string ReservedTreePrefix = "sys-auth-";

    /// <summary>Tree holding authorization rules, keyed <c>{treeId}\u001f{ruleId}</c> so a tree's rules are a single prefix scan.</summary>
    internal const string PolicyTree = "sys-auth-policy";

    /// <summary>
    /// The optional append-only authorization audit tree that backs the durable
    /// decision trail. Off by default; written only when the durable audit trail
    /// is enabled. Mirrored publicly as <c>LatticeSystemTreeNames.AuthAudit</c>
    /// in the replication package, kept in sync by the auth test project's drift
    /// guard.
    /// </summary>
    internal const string AuditTree = "sys-auth-audit";

    /// <summary>Durable per-key history view name for <see cref="PolicyTree"/>.</summary>
    internal const string PolicyHistoryView = "sys-auth-policy-history";

    /// <summary>Field separator used inside composite rule keys.</summary>
    internal const char RuleKeySeparator = '\u001f';

    /// <summary>Enumerates the reserved backing tree names.</summary>
    internal static IReadOnlyList<string> AllTrees { get; } = new[] { PolicyTree };

    /// <summary>
    /// Whether <paramref name="treeId"/> names a tree in the tenant-registry
    /// system-data namespace (<c>sys-tenant-*</c>, per
    /// <see cref="LatticeConstants.TenantRegistryTreePrefix"/>). Such a tree holds
    /// the cross-tenant registry - every tenant's admin subjects, quotas,
    /// placement, and grants - so the enforcement gate governs it with
    /// control-plane read isolation rather than the data-plane default effect,
    /// keeping a broad data-plane Read grant (including a cluster-wide all-trees
    /// wildcard) from scanning it. Allocation-free: a single ordinal
    /// <see cref="string.StartsWith(string, StringComparison)"/> on the request's
    /// tree id, so it is safe to evaluate on the gate's synchronous fast path.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is in the tenant-registry namespace; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    internal static bool IsTenantRegistryTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId.StartsWith(LatticeConstants.TenantRegistryTreePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Rejects a tree id that collides with the reserved <c>sys-auth-*</c>
    /// namespace. Used on the policy-store write path so a rule can never be
    /// scoped at (and thereby steer authorization over) the internal policy tree,
    /// and exposed through <see cref="LatticeAuthReservedTrees"/> so an
    /// application can validate its own tree ids at registration time.
    /// </summary>
    /// <param name="treeId">The candidate tree id.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> starts with <see cref="ReservedTreePrefix"/>.</exception>
    internal static void ThrowIfReservedTree(string treeId, string paramName)
    {
        if (treeId.StartsWith(ReservedTreePrefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Tree ID '{treeId}' is reserved: names starting with '{ReservedTreePrefix}' " +
                "are reserved for the Orleans.Lattice.Auth policy store. Choose a tree ID that " +
                $"does not start with '{ReservedTreePrefix}'.",
                paramName);
        }
    }

    /// <summary>
    /// The policy-store <b>authoring</b> guard: throws unless <paramref name="rule"/>
    /// is a shape a caller is permitted to persist. A rule scoped at an ordinary
    /// (non-reserved) tree is always authorable. A rule scoped at the reserved
    /// <c>sys-auth-*</c> namespace is rejected fail-closed <b>except</b> for the
    /// single access-administration delegation shape - a <b>whole-tree</b>
    /// <see cref="LatticeOperation.Admin"/> rule on the policy tree
    /// (<see cref="PolicyTree"/>) - and only when
    /// <paramref name="delegationEnabled"/> is set. This is the one seam that
    /// decides whether a reserved-namespace rule may be written; the enforcement
    /// gate independently honours a matched allow on that namespace, so no gate
    /// change is needed for the delegation to take effect. The same seam also
    /// rejects an all-trees (<c>Tree:*</c>) data-plane rule when the all-trees tier
    /// is disabled (<paramref name="allTreesGrantsEnabled"/> is <c>false</c>), so
    /// an inert wildcard grant is refused at authoring time instead of persisted
    /// silently.
    /// </summary>
    /// <param name="rule">The candidate rule. Must not be <c>null</c>.</param>
    /// <param name="delegationEnabled">
    /// Whether access-administration delegation is enabled for this deployment
    /// (<c>LatticeAuthOptions.AccessAdministrationDelegationEnabled</c>).
    /// </param>
    /// <param name="allTreesGrantsEnabled">
    /// Whether the cluster-wide all-trees grant tier is enabled for this
    /// deployment (<c>LatticeAuthOptions.AllTreesGrantsEnabled</c>). When
    /// <c>false</c>, an all-trees (<c>Tree:*</c>) rule carrying any data-plane
    /// operation is rejected at authoring time rather than persisted inert, so the
    /// operator learns the tier must be enabled first.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="rule"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="rule"/> targets the reserved namespace and is not the
    /// permitted delegation shape, or is the delegation shape while
    /// <paramref name="delegationEnabled"/> is <c>false</c>; or is an all-trees
    /// data-plane grant while <paramref name="allTreesGrantsEnabled"/> is
    /// <c>false</c>.
    /// </exception>
    internal static void EnsureAuthorableRuleScope(
        LatticeAuthorizationRule rule,
        bool delegationEnabled,
        bool allTreesGrantsEnabled)
    {
        ArgumentNullException.ThrowIfNull(rule);
        var treeId = rule.Scope.TreeId;

        // All-trees (Tree:* sentinel) authoring guard: a wildcard rule carrying a
        // data-plane operation is inert unless the all-trees tier is enabled, so
        // reject it at authoring time rather than silently persisting a rule that
        // does nothing. A pure Telemetry wildcard rule is unaffected - telemetry is
        // a scopeless capability that resolves against the "*" bucket regardless of
        // the tier flag - so only rules that intersect the data-plane mask are
        // gated here.
        if (string.Equals(treeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal)
            && (rule.Operations & LatticeAuthOperations.All) != LatticeOperation.None
            && !allTreesGrantsEnabled)
        {
            throw new ArgumentException(
                $"Authoring an all-trees ('{LatticeScope.ClusterWideTreeId}'-scoped) data-plane rule requires the " +
                "all-trees grant tier to be enabled (LatticeAuthOptions.AllTreesGrantsEnabled). It is off by " +
                "default, and while off such a rule is inert (the decision engine never consults the all-trees " +
                "bucket for an ordinary tree). Enable the tier on the silo first, then author the rule.",
                nameof(rule));
        }

        // Ordinary application tree: always authorable, byte-for-byte unchanged.
        if (!treeId.StartsWith(ReservedTreePrefix, StringComparison.Ordinal))
        {
            return;
        }

        // The rule targets the reserved sys-auth-* namespace. Exactly one shape is
        // authorable there, and only when the operator has opted in: a whole-tree
        // Admin grant on the policy tree itself - the access-administration
        // delegation. Effect (Allow or Deny) is unconstrained.
        if (IsAccessAdministrationDelegationShape(rule))
        {
            if (!delegationEnabled)
            {
                throw new ArgumentException(
                    $"Authoring an access-administration delegation rule on the reserved policy tree '{PolicyTree}' " +
                    "requires access-administration delegation to be enabled " +
                    "(LatticeAuthOptions.AccessAdministrationDelegationEnabled). It is off by default; enable it on " +
                    "the silo to delegate access administration to a user or group.",
                    nameof(rule));
            }

            return;
        }

        // The policy tree, but not the permitted shape (a key/prefix scope, or an
        // operation set that is not exactly Admin): never authorable, even with
        // delegation enabled - fail closed.
        if (string.Equals(treeId, PolicyTree, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"Only a whole-tree '{nameof(LatticeOperation)}.{nameof(LatticeOperation.Admin)}' rule (the " +
                $"access-administration delegation shape) may be authored on the reserved policy tree '{PolicyTree}'. " +
                "A key or prefix scope, or any other operation set, is not authorable on the reserved namespace.",
                nameof(rule));
        }

        // Any other reserved sys-auth-* tree: unchanged rejection.
        ThrowIfReservedTree(treeId, nameof(rule));
    }

    /// <summary>
    /// Whether <paramref name="rule"/> is exactly the authorable
    /// access-administration delegation shape: a whole-tree rule on the policy tree
    /// whose operation set is exactly <see cref="LatticeOperation.Admin"/> (no
    /// other capability bits, and not a key/prefix scope).
    /// </summary>
    /// <param name="rule">The candidate rule. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the rule is the delegation shape; otherwise <c>false</c>.</returns>
    internal static bool IsAccessAdministrationDelegationShape(LatticeAuthorizationRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        return string.Equals(rule.Scope.TreeId, PolicyTree, StringComparison.Ordinal)
            && rule.Scope.Kind == LatticeScopeKind.Tree
            && rule.Operations == LatticeOperation.Admin;
    }
}
