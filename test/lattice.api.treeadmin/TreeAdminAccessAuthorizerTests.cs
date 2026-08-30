namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminAccessAuthorizer"/>'s cluster-telemetry
/// authorization, proving it fails <b>closed</b> under the opt-in permissive
/// <c>DefaultEffect = Allow</c> default while honouring the grant the API
/// documents. The cluster-wide storage-accounting summary authorizes over the
/// cluster-wide sentinel <c>"*"</c> - the scope
/// <c>LatticeScope.ClusterWide()</c> authors - which the gate governs with
/// control-plane isolation because a request on the sentinel can only be a
/// scopeless capability request, never a data-plane one. So it is
/// denied-unless-explicitly-granted, independent of the data-plane default
/// effect, and cannot leak the elevated all-tree observability scope to any
/// caller, including an anonymous one (issue #1795).
/// </summary>
/// <remarks>
/// The gate double <see cref="DefaultEffectAllowGate"/> faithfully models the real
/// <c>PolicyAccessGate</c> under <c>DefaultEffect = Allow</c>: a request that
/// targets a reserved <c>sys-auth-*</c> tree or the cluster-wide sentinel routes
/// through control-plane isolation (denied unless an explicit grant matches the
/// caller), while every other (data-plane) scope inherits the permissive
/// <c>Allow</c> default. It is deliberately not a lenient strawman - the
/// control-plane / data-plane split is exactly the seam the fix relies on.
/// </remarks>
[TestFixture]
public sealed class TreeAdminAccessAuthorizerTests
{
    /// <summary>The cluster-wide sentinel scope a scopeless capability is granted over.</summary>
    private const string ClusterWideScope = "*";

    /// <summary>A named data-plane tree used to prove the per-tree verbs are unaffected.</summary>
    private const string DataPlaneTreeId = "orders";

    [Test]
    public async Task AuthorizeClusterTelemetryAsync_anonymous_caller_under_default_effect_allow_is_denied()
    {
        // Anonymous caller (no membership registered), permissive default effect,
        // and no telemetry rule authored. The sentinel is governed by control-plane
        // isolation, so an unmatched request is denied rather than inheriting Allow.
        var gate = new DefaultEffectAllowGate();
        var authorizer = new TreeAdminAccessAuthorizer(gate, membership: null);

        Assert.That(
            async () => await authorizer.AuthorizeClusterTelemetryAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task AuthorizeClusterTelemetryAsync_operator_granted_cluster_wide_is_allowed()
    {
        // Positive control (no over-denial), and the regression this fix exists for:
        // a grant authored the documented way - LatticeScope.ClusterWide(), which
        // targets the "*" sentinel - is the grant that authorizes the capability.
        var gate = new DefaultEffectAllowGate();
        gate.Grant("operator", ClusterWideScope, LatticeOperation.Telemetry);
        var authorizer = new TreeAdminAccessAuthorizer(
            gate, new FixedSubjectMembership(new LatticeSubject("operator")));

        Assert.That(
            async () => await authorizer.AuthorizeClusterTelemetryAsync(),
            Throws.Nothing);
    }

    [Test]
    public void AuthorizeClusterTelemetryAsync_targets_the_cluster_wide_sentinel()
    {
        // Pins the scope itself, so the treeadmin half can never drift from the
        // telemetry facade again: both must ask about the same tree id as
        // LatticeScope.ClusterWide() authors.
        Assert.That(TreeAdminAccessAuthorizer.ClusterWideScope, Is.EqualTo("*"));
    }

    [Test]
    public async Task AuthorizeTreeReadAsync_named_tree_under_default_effect_allow_is_allowed()
    {
        // Guard: the per-tree verbs authorize over a concrete tree id, which under
        // DefaultEffect = Allow is the intended permissive behaviour. The fix must
        // not disturb this path.
        var gate = new DefaultEffectAllowGate();
        var authorizer = new TreeAdminAccessAuthorizer(gate, membership: null);

        Assert.That(
            async () => await authorizer.AuthorizeTreeReadAsync(DataPlaneTreeId),
            Throws.Nothing);
    }

    /// <summary>
    /// A faithful model of the real <c>PolicyAccessGate</c> operating under
    /// <c>DefaultEffect = Allow</c>: a request against a reserved <c>sys-auth-*</c>
    /// tree is governed by control-plane isolation (denied unless an explicit grant
    /// matches the caller subject, tree, and operation); every other request
    /// inherits the permissive <c>Allow</c> default.
    /// </summary>
    private sealed class DefaultEffectAllowGate : ILatticeAccessGate
    {
        private const string ReservedTreePrefix = "sys-auth-";
        private const string ClusterWideSentinel = "*";

        private readonly HashSet<(string SubjectId, string TreeId, LatticeOperation Operation)> _grants = new();

        public void Grant(string subjectId, string treeId, LatticeOperation operation) =>
            _grants.Add((subjectId, treeId, operation));

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            // Control-plane isolation: the reserved namespace and the cluster-wide
            // capability sentinel are denied unless an explicit matched allow grant
            // exists, regardless of the data-plane default effect (fail-closed).
            if (request.TreeId.StartsWith(ReservedTreePrefix, StringComparison.Ordinal)
                || string.Equals(request.TreeId, ClusterWideSentinel, StringComparison.Ordinal))
            {
                var granted = _grants.Contains((request.Subject.SubjectId, request.TreeId, request.Operation));
                return new ValueTask<LatticeAccessDecision>(
                    granted
                        ? LatticeAccessDecision.Allow()
                        : LatticeAccessDecision.Deny("control-plane isolation: unmatched control-plane request"));
            }

            // Data-plane: inherit DefaultEffect = Allow.
            return new ValueTask<LatticeAccessDecision>(LatticeAccessDecision.Allow());
        }
    }

    /// <summary>
    /// A membership context that resolves every caller to a single fixed subject,
    /// so a positive-control test can present a genuine granted operator.
    /// </summary>
    private sealed class FixedSubjectMembership(LatticeSubject subject) : ILatticeMembershipContext
    {
        public ValueTask<LatticeSubject> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
            new(subject);

        public bool TryResolveCurrent(out LatticeSubject resolved)
        {
            resolved = subject;
            return true;
        }
    }
}
