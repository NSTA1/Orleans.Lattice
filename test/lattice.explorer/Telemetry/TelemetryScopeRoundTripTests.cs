using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Round-trips <b>every</b> tenant-scope outcome the facade can pin through the
/// Explorer seam, and asserts each arrives in the domain model exactly as the
/// server decided it - requested visibility, effective visibility, pinned tenant,
/// and the degradation flag alike.
/// </summary>
/// <remarks>
/// <para>
/// The projection must not collapse the two single-tenant outcomes. They differ
/// only in the requested visibility they record, and that difference is what
/// <see cref="ExplorerTelemetryScope.WasDowngraded"/> is computed from - so
/// collapsing them would make an honoured operator view report itself as
/// degraded, or, worse, make a refused widening look honoured and let a panel
/// label one tenant's data as the whole cluster's.
/// </para>
/// <para>
/// No scope is constructed by the seam: each case seeds the fake client with the
/// scope a server-side resolver would have pinned, and the assertion is that the
/// value survives the projection unchanged.
/// </para>
/// </remarks>
[TestFixture]
public class TelemetryScopeRoundTripTests
{
    private FakeTelemetryQueryClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTelemetryQueryClient();

    /// <summary>
    /// Evaluates one query against a facade that pinned <paramref name="scope"/>,
    /// and returns the scope the domain model actually reports.
    /// </summary>
    private async Task<ExplorerTelemetryScope> RoundTripAsync(TelemetryTenantScope scope)
    {
        _client.Scope = scope;
        var result = await new TelemetryQueryService(_client)
            .QueryAsync(ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.That(result.IsSuccess, Is.True);
        return result.Value!.Scope;
    }

    [Test]
    public async Task Active_tenant_requested_arrives_pinned_and_not_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.ActiveTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo(SampleTelemetry.CallerTenant));
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public async Task All_tenants_requested_by_a_non_operator_arrives_pinned_and_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.AllTenants));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.AllTenants));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo(SampleTelemetry.CallerTenant));
            Assert.That(scope.WasDowngraded, Is.True);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public async Task All_tenants_requested_by_a_validated_operator_arrives_cross_tenant_and_not_downgraded()
    {
        var scope = await RoundTripAsync(TelemetryTenantScope.AcrossAllTenants());

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.AllTenants));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(ExplorerTelemetryVisibility.AllTenants));
            Assert.That(scope.TenantId, Is.Null);
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.True);
        });
    }

    [Test]
    public async Task Single_tenant_requested_by_a_non_operator_arrives_at_the_callers_own_tenant_and_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.SingleTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.SingleTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(
                scope.TenantId,
                Is.EqualTo(SampleTelemetry.CallerTenant),
                "the requested id is ignored in full for a non-operator");
            Assert.That(scope.WasDowngraded, Is.True);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public async Task Single_tenant_requested_by_a_validated_operator_with_a_usable_id_arrives_honoured()
    {
        var scope = await RoundTripAsync(TelemetryTenantScope.AtRequestedTenant(SampleTelemetry.OtherTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.SingleTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(ExplorerTelemetryVisibility.SingleTenant));
            Assert.That(scope.TenantId, Is.EqualTo(SampleTelemetry.OtherTenant));
            Assert.That(
                scope.WasDowngraded,
                Is.False,
                "an honoured operator view reporting itself downgraded is the exact bug the "
                + "pinned / at-requested split exists to prevent");
            Assert.That(
                scope.IsCrossTenant,
                Is.False,
                "one honoured tenant is not a cross-tenant evaluation, even when it is not the caller's own");
        });
    }

    [Test]
    public async Task Single_tenant_requested_by_a_validated_operator_with_an_unusable_id_arrives_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.SingleTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo(SampleTelemetry.CallerTenant));
            Assert.That(scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public async Task The_honoured_and_refused_single_tenant_outcomes_do_not_collapse()
    {
        var honoured = await RoundTripAsync(TelemetryTenantScope.AtRequestedTenant(SampleTelemetry.OtherTenant));
        var refused = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.SingleTenant));

        Assert.Multiple(() =>
        {
            Assert.That(honoured.WasDowngraded, Is.False);
            Assert.That(refused.WasDowngraded, Is.True);
            Assert.That(honoured.EffectiveVisibility, Is.Not.EqualTo(refused.EffectiveVisibility));
            Assert.That(honoured.TenantId, Is.Not.EqualTo(refused.TenantId));
        });
    }

    [Test]
    public async Task Every_scope_outcome_projects_to_a_distinct_domain_scope()
    {
        (TelemetryTenantScope Pinned, ExplorerTelemetryScope Expected)[] outcomes =
        [
            (
                TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.ActiveTenant),
                new ExplorerTelemetryScope(
                    ExplorerTelemetryVisibility.ActiveTenant,
                    ExplorerTelemetryVisibility.ActiveTenant,
                    SampleTelemetry.CallerTenant)),
            (
                TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.AllTenants),
                new ExplorerTelemetryScope(
                    ExplorerTelemetryVisibility.AllTenants,
                    ExplorerTelemetryVisibility.ActiveTenant,
                    SampleTelemetry.CallerTenant)),
            (
                TelemetryTenantScope.AcrossAllTenants(),
                new ExplorerTelemetryScope(
                    ExplorerTelemetryVisibility.AllTenants,
                    ExplorerTelemetryVisibility.AllTenants,
                    null)),
            (
                TelemetryTenantScope.PinnedTo(SampleTelemetry.CallerTenant, TelemetryTenantVisibility.SingleTenant),
                new ExplorerTelemetryScope(
                    ExplorerTelemetryVisibility.SingleTenant,
                    ExplorerTelemetryVisibility.ActiveTenant,
                    SampleTelemetry.CallerTenant)),
            (
                TelemetryTenantScope.AtRequestedTenant(SampleTelemetry.OtherTenant),
                new ExplorerTelemetryScope(
                    ExplorerTelemetryVisibility.SingleTenant,
                    ExplorerTelemetryVisibility.SingleTenant,
                    SampleTelemetry.OtherTenant)),
        ];

        var observed = new List<ExplorerTelemetryScope>(outcomes.Length);
        foreach (var (pinned, expected) in outcomes)
        {
            var scope = await RoundTripAsync(pinned);
            Assert.That(scope, Is.EqualTo(expected), $"the scope must arrive exactly as pinned: {expected}");
            observed.Add(scope);
        }

        Assert.That(
            observed.Distinct(),
            Has.Exactly(outcomes.Length).Items,
            "each of the five pinned outcomes must remain distinguishable in the domain model");
    }

    [Test]
    public void The_unevaluated_scope_is_the_fail_closed_one() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerTelemetryScope.None.EffectiveVisibility,
                Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(ExplorerTelemetryScope.None.TenantId, Is.Null);
            Assert.That(ExplorerTelemetryScope.None.WasDowngraded, Is.False);
            Assert.That(ExplorerTelemetryScope.None.IsCrossTenant, Is.False);
            Assert.That(
                default(ExplorerTelemetryScope),
                Is.EqualTo(ExplorerTelemetryScope.None),
                "an uninitialised scope must read as the narrowest one, not the widest");
        });
}
