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
    /// placement, and grants - so the enforcement gate governs it (through
    /// <see cref="IsControlPlaneRegistryTree"/>, which also covers the app
    /// registry) with
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
    /// Whether <paramref name="treeId"/> names a tree in either control-plane
    /// registry system-data namespace: the tenant registry (<c>sys-tenant-*</c>,
    /// per <see cref="LatticeConstants.TenantRegistryTreePrefix"/>) or the app
    /// registry (<c>sys-app-*</c>, per
    /// <see cref="LatticeConstants.AppRegistryTreePrefix"/>). The tenant registry
    /// holds every tenant's admin subjects, quotas, placement, and grants; the app
    /// registry holds every installed app's capability ceiling, consent record, and
    /// role-to-group bindings. The enforcement gate governs both with control-plane
    /// read isolation rather than the data-plane default effect, and the evaluator
    /// excludes both from the all-trees tier, so a broad data-plane Read grant
    /// (including a cluster-wide all-trees wildcard) cannot scan either. Allocation-
    /// free: at most two ordinal <see cref="string.StartsWith(string, StringComparison)"/>
    /// probes on the request's tree id, so it is safe to evaluate on the gate's
    /// synchronous fast path.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is in either registry namespace; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    internal static bool IsControlPlaneRegistryTree(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return treeId.StartsWith(LatticeConstants.TenantRegistryTreePrefix, StringComparison.Ordinal)
            || treeId.StartsWith(LatticeConstants.AppRegistryTreePrefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether <paramref name="treeId"/> names a control-plane system-data tree that a
    /// data-plane grant must never expose: either registry namespace
    /// (<see cref="IsControlPlaneRegistryTree"/>) or the membership store
    /// (<c>sys-membership-*</c>, owned by <c>Orleans.Lattice.Membership</c>) or schema
    /// backing stores (<c>sys-schema-*</c>, owned by <c>Orleans.Lattice.Schema</c>).
    /// These hold cross-tenant membership and source-value dead-letter previews.
    /// The enforcement gate governs these with control-plane read
    /// isolation rather than the data-plane default effect, and the evaluator excludes
    /// them from the all-trees tier, so neither <see cref="LatticeEffect.Allow"/> as
    /// the default nor a cluster-wide all-trees wildcard can read them; an explicit
    /// rule scoped at the tree, or a bootstrap administrator, still can. Allocation-
    /// free: at most four ordinal prefix probes on the request's tree id.
    /// </summary>
    /// <param name="treeId">The candidate tree id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is a control-plane system-data tree; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    internal static bool IsControlPlaneDataTree(string treeId) =>
        IsControlPlaneRegistryTree(treeId)
        || treeId.StartsWith(Membership.MembershipConstants.TreePrefix, StringComparison.Ordinal)
        || treeId.StartsWith("sys-schema-", StringComparison.Ordinal);

    /// <summary>
    /// Whether <paramref name="rule"/> is well formed: a non-empty rule id; a defined
    /// effect; a subject of a defined kind with a non-empty id; and a scope of a
    /// defined kind over a non-empty tree id whose key or prefix is absent for a
    /// whole-tree scope and non-empty otherwise. The public constructors check the
    /// ids and the key-or-prefix shape but not the enum values, and Orleans
    /// deserialization and <c>with</c> expressions skip them entirely, so a rule
    /// from a client, by replication or from a restore can carry an undefined kind.
    /// The evaluator would read an undefined subject kind as a group and an
    /// undefined scope kind as the whole tree, while tenant confinement tests only
    /// the defined kinds, so a malformed rule could grant more than any rule an
    /// author could write. Allocation-free.
    /// </summary>
    /// <param name="rule">The rule. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the rule is well formed.</returns>
    internal static bool IsWellFormedRule(LatticeAuthorizationRule rule) =>
        !string.IsNullOrEmpty(rule.RuleId)
        && Enum.IsDefined(rule.Effect)
        && rule.Subject is { } subject
        && Enum.IsDefined(subject.Kind)
        && !string.IsNullOrEmpty(subject.Id)
        && rule.Scope is { } scope
        && !string.IsNullOrEmpty(scope.TreeId)
        && scope.Kind switch
        {
            LatticeScopeKind.Tree => scope.KeyOrPrefix is null,
            LatticeScopeKind.Key or LatticeScopeKind.Prefix => !string.IsNullOrEmpty(scope.KeyOrPrefix),
            _ => false,
        };

    /// <summary>
    /// Refuses a rule that is not well formed (<see cref="IsWellFormedRule"/>).
    /// </summary>
    /// <param name="rule">The rule. Must not be <c>null</c>.</param>
    /// <param name="paramName">The caller's parameter name, for the thrown exception.</param>
    /// <exception cref="ArgumentException">The rule is not well formed.</exception>
    internal static void EnsureWellFormedRule(LatticeAuthorizationRule rule, string paramName)
    {
        if (!IsWellFormedRule(rule))
        {
            throw new ArgumentException(
                "An authorization rule must carry a rule id, a defined effect, a subject of a defined kind with an id, "
                + "and a scope of a defined kind over a tree id, with a key or prefix exactly when the scope is a key "
                + "or prefix scope.",
                paramName);
        }
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
