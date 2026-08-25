namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TreeAdminAccessAuthorizer"/>'s cluster-telemetry
/// authorization, proving it fails <b>closed</b> under the opt-in permissive
/// <c>DefaultEffect = Allow</c> default. The cluster-wide storage-accounting
/// summary must authorize over the reserved auth policy tree (a control-plane
/// scope that is denied-unless-explicitly-granted, independent of the data-plane
/// default effect), not over the data-plane cluster-wide sentinel <c>"*"</c>
/// (which inherits <c>DefaultEffect = Allow</c> and would leak the elevated
/// all-tree observability scope to any caller, including an anonymous one).
/// </summary>
/// <remarks>
/// The gate double <see cref="DefaultEffectAllowGate"/> faithfully models the real
/// <c>PolicyAccessGate</c> under <c>DefaultEffect = Allow</c>: a request that
/// targets a reserved <c>sys-auth-*</c> tree routes through control-plane
/// isolation (denied unless an explicit grant matches the caller), while every
/// other (data-plane) scope inherits the permissive <c>Allow</c> default. It is
/// deliberately not a lenient strawman - the control-plane / data-plane split is
/// exactly the seam the fix relies on.
/// </remarks>
[TestFixture]
public sealed class TreeAdminAccessAuthorizerTests
{
    /// <summary>The reserved auth policy tree id (the <c>sys-auth-*</c> namespace).</summary>
    private const string PolicyTreeId = "sys-auth-policy";

    /// <summary>A named data-plane tree used to prove the per-tree verbs are unaffected.</summary>
    private const string DataPlaneTreeId = "orders";

    [Test]
    public async Task AuthorizeClusterTelemetryAsync_anonymous_caller_under_default_effect_allow_is_denied()
    {
        // Anonymous caller (no membership registered), permissive default effect,
        // and no telemetry rule authored. If cluster telemetry authorizes over the
        // data-plane "*" sentinel it inherits Allow and leaks; over the reserved
        // policy tree it is denied by control-plane isolation.
        var gate = new DefaultEffectAllowGate();
        var authorizer = new TreeAdminAccessAuthorizer(gate, membership: null);

        Assert.That(
            async () => await authorizer.AuthorizeClusterTelemetryAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task AuthorizeClusterTelemetryAsync_operator_granted_on_policy_tree_is_allowed()
    {
        // Positive control (no over-denial): a genuine operator that carries an
        // explicit Telemetry grant on the reserved policy tree is still allowed.
        var gate = new DefaultEffectAllowGate();
        gate.Grant("operator", PolicyTreeId, LatticeOperation.Telemetry);
        var authorizer = new TreeAdminAccessAuthorizer(
            gate, new FixedSubjectMembership(new LatticeSubject("operator")));

        Assert.That(
            async () => await authorizer.AuthorizeClusterTelemetryAsync(),
            Throws.Nothing);
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

        private readonly HashSet<(string SubjectId, string TreeId, LatticeOperation Operation)> _grants = new();

        public void Grant(string subjectId, string treeId, LatticeOperation operation) =>
            _grants.Add((subjectId, treeId, operation));

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            // Control-plane isolation: the reserved namespace is denied unless an
            // explicit matched allow grant exists, regardless of the data-plane
            // default effect (fail-closed).
            if (request.TreeId.StartsWith(ReservedTreePrefix, StringComparison.Ordinal))
            {
                var granted = _grants.Contains((request.Subject.SubjectId, request.TreeId, request.Operation));
                return new ValueTask<LatticeAccessDecision>(
                    granted
                        ? LatticeAccessDecision.Allow()
                        : LatticeAccessDecision.Deny("control-plane isolation: unmatched reserved request"));
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
