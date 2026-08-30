using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the fail-closed authorization seam: the coarse telemetry capability that
/// gates the surface, and the platform-operator validation that decides whether a
/// widening request may be honoured.
/// </summary>
[TestFixture]
public sealed class TelemetryAccessAuthorizerTests
{
    [Test]
    public async Task AuthorizeClusterTelemetryAsync_allows_a_caller_granted_telemetry()
    {
        var authorizer = new TelemetryAccessAuthorizer(StubAccessGate.TelemetryOnly());

        await authorizer.AuthorizeClusterTelemetryAsync();

        Assert.Pass();
    }

    [Test]
    public void AuthorizeClusterTelemetryAsync_denies_a_caller_without_the_capability()
    {
        var authorizer = new TelemetryAccessAuthorizer(new StubAccessGate());

        Assert.That(
            async () => await authorizer.AuthorizeClusterTelemetryAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public void AuthorizeClusterTelemetryAsync_is_not_conferred_by_admin()
    {
        var gate = new StubAccessGate().Allowing(LatticeOperation.Admin);
        var authorizer = new TelemetryAccessAuthorizer(gate);

        Assert.That(
            async () => await authorizer.AuthorizeClusterTelemetryAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "No other operation - not even Admin - confers the telemetry capability.");
    }

    [Test]
    public void AuthorizeClusterTelemetryAsync_refuses_a_key_filtered_allow()
    {
        var gate = new StubAccessGate().For(
            LatticeOperation.Telemetry, LatticeAccessDecision.Filtered(static _ => true));
        var authorizer = new TelemetryAccessAuthorizer(gate);

        Assert.That(
            async () => await authorizer.AuthorizeClusterTelemetryAsync(),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "Cluster telemetry is not attached to a key, so a narrowed allow does not authorize it.");
    }

    [Test]
    public async Task AuthorizeClusterTelemetryAsync_authorizes_over_the_cluster_wide_sentinel()
    {
        var gate = StubAccessGate.TelemetryOnly();

        await new TelemetryAccessAuthorizer(gate).AuthorizeClusterTelemetryAsync();

        Assert.That(gate.AuthorizedTrees, Is.EqualTo(new[] { LatticeScope.ClusterWideTreeId }),
            "The capability must be asked about on the same scope LatticeScope.ClusterWide() "
            + "authors, or a grant written the documented way is silently inert. The gate governs "
            + "the sentinel with control-plane isolation, so it still cannot fail open under a "
            + "permissive data-plane default effect.");
    }

    [Test]
    public async Task AuthorizeClusterTelemetryAsync_admits_when_no_gate_is_registered()
    {
        await new TelemetryAccessAuthorizer().AuthorizeClusterTelemetryAsync();

        Assert.Pass("An authorization-off cluster stays byte-for-byte unchanged.");
    }

    [Test]
    public async Task IsPlatformOperatorAsync_validates_admin_on_the_reserved_policy_tree()
    {
        var gate = StubAccessGate.PlatformOperator();

        var isOperator = await new TelemetryAccessAuthorizer(gate).IsPlatformOperatorAsync();

        Assert.Multiple(() =>
        {
            Assert.That(isOperator, Is.True);
            Assert.That(gate.AuthorizedTrees, Is.EqualTo(new[] { LatticeAuthReservedTrees.PolicyTreeId }));
        });
    }

    [Test]
    public async Task IsPlatformOperatorAsync_rejects_a_caller_with_only_telemetry()
    {
        var isOperator = await new TelemetryAccessAuthorizer(StubAccessGate.TelemetryOnly())
            .IsPlatformOperatorAsync();

        Assert.That(isOperator, Is.False,
            "Reading telemetry is not authority to read every tenant's telemetry.");
    }

    [Test]
    public async Task IsPlatformOperatorAsync_refuses_a_key_filtered_allow()
    {
        var gate = new StubAccessGate().For(
            LatticeOperation.Admin, LatticeAccessDecision.Filtered(static _ => true));

        var isOperator = await new TelemetryAccessAuthorizer(gate).IsPlatformOperatorAsync();

        Assert.That(isOperator, Is.False,
            "A whole-scope operator capability can never be narrowed to a key subset.");
    }

    [Test]
    public async Task IsPlatformOperatorAsync_fails_closed_when_no_gate_is_registered()
    {
        var isOperator = await new TelemetryAccessAuthorizer().IsPlatformOperatorAsync();

        Assert.That(isOperator, Is.False,
            "Widening beyond the caller's own tenant is honoured only after server-side validation, "
            + "which is impossible without the gate seam.");
    }

    [Test]
    public async Task IsPlatformOperatorAsync_admits_a_system_origin_turn()
    {
        using (LatticeSystemOrigin.Enter())
        {
            var isOperator = await new TelemetryAccessAuthorizer(new StubAccessGate())
                .IsPlatformOperatorAsync();

            Assert.That(isOperator, Is.True,
                "Trusted co-hosted infrastructure is not an external caller.");
        }
    }

    [Test]
    public async Task The_caller_subject_is_resolved_through_the_membership_seam()
    {
        var membership = new StubMembershipContext(LatticeSubject.Anonymous);

        await new TelemetryAccessAuthorizer(StubAccessGate.TelemetryOnly(), membership)
            .AuthorizeClusterTelemetryAsync();

        Assert.That(membership.AsyncResolutions, Is.Zero,
            "A warm synchronous resolution must not take the directory-reading path.");
    }

    [Test]
    public async Task An_uncached_subject_resolution_runs_under_a_system_origin_scope()
    {
        var membership = new StubMembershipContext(
            LatticeSubject.Anonymous, resolvesSynchronously: false);

        await new TelemetryAccessAuthorizer(StubAccessGate.TelemetryOnly(), membership)
            .AuthorizeClusterTelemetryAsync();

        Assert.Multiple(() =>
        {
            Assert.That(membership.AsyncResolutions, Is.EqualTo(1));
            Assert.That(membership.ResolvedUnderSystemOrigin, Is.True,
                "Resolution may read the dogfooded membership directory, which must not re-enter "
                + "the gate.");
        });
    }
}
