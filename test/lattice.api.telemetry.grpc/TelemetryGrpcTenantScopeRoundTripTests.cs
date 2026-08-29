using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Telemetry.Grpc.Tests;

/// <summary>
/// Round-trips <b>every</b> tenant-scope outcome the facade can pin, through the
/// real wire encoding, and asserts each arrives at the client exactly as the server
/// decided it - effective visibility, pinned tenant, and
/// <see cref="TelemetryTenantScope.WasDowngraded"/> alike.
/// </summary>
/// <remarks>
/// <para>
/// The binding must not collapse <see cref="TelemetryTenantScope.PinnedTo"/> and
/// <see cref="TelemetryTenantScope.AtRequestedTenant"/>. They differ only in the
/// requested visibility they record, and that difference is exactly what
/// <c>WasDowngraded</c> is computed from - so collapsing them would make an
/// honoured operator view report itself as downgraded (or, worse, make a refused
/// widening look honoured). Both are real bugs the contract already guards against,
/// and this fixture is the transport-side half of that guard.
/// </para>
/// <para>
/// No scope is constructed by the binding: each case seeds the fake facade with the
/// scope a server-side resolver would have pinned, and the assertion is that the
/// value survives the wire unchanged.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TelemetryGrpcTenantScopeRoundTripTests
{
    private const string CallerTenant = "acme";
    private const string OtherTenant = "beta";

    private ServiceProvider _serializers = null!;

    [SetUp]
    public void SetUp() => _serializers = TelemetryGrpcTestSupport.Serializers();

    [TearDown]
    public void TearDown() => _serializers.Dispose();

    /// <summary>
    /// Drives one query through the loopback wire against a facade that pins
    /// <paramref name="scope"/>, and returns the scope the client actually observed.
    /// </summary>
    private async Task<TelemetryTenantScope> RoundTripAsync(TelemetryTenantScope scope)
    {
        var service = TelemetryGrpcTestSupport.Service(_serializers, new ScopedTelemetry(scope));
        var client = new LatticeTelemetryApiGrpcClient(
            new LoopbackCallInvoker(service, _serializers),
            TelemetryGrpcTestSupport.Methods(_serializers));

        var response = await client.QueryAsync(new TelemetryQueryRequest { QueryId = "lattice.ops.rate" });
        return response.Scope;
    }

    [Test]
    public async Task Active_tenant_requested_arrives_pinned_and_not_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.ActiveTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo(CallerTenant));
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.False);
        });
    }

    [Test]
    public async Task All_tenants_requested_by_a_non_operator_arrives_pinned_and_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.AllTenants));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo(CallerTenant));
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
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.AllTenants));
            Assert.That(scope.TenantId, Is.Null);
            Assert.That(scope.WasDowngraded, Is.False);
            Assert.That(scope.IsCrossTenant, Is.True);
        });
    }

    [Test]
    public async Task Single_tenant_requested_by_a_non_operator_arrives_at_the_callers_own_tenant_and_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.SingleTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(
                scope.TenantId,
                Is.EqualTo(CallerTenant),
                "The requested id is ignored in full for a non-operator.");
            Assert.That(scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public async Task Single_tenant_requested_by_a_validated_operator_with_a_usable_id_arrives_honoured()
    {
        var scope = await RoundTripAsync(TelemetryTenantScope.AtRequestedTenant(OtherTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.RequestedVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.SingleTenant));
            Assert.That(scope.TenantId, Is.EqualTo(OtherTenant));
            Assert.That(
                scope.WasDowngraded,
                Is.False,
                "An honoured operator view reporting itself downgraded is the exact bug the "
                + "PinnedTo / AtRequestedTenant split exists to prevent.");
            Assert.That(
                scope.IsCrossTenant,
                Is.False,
                "One honoured tenant is not a cross-tenant evaluation, even when it is not the caller's own.");
        });
    }

    [Test]
    public async Task Single_tenant_requested_by_a_validated_operator_with_an_unusable_id_arrives_downgraded()
    {
        var scope = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.SingleTenant));

        Assert.Multiple(() =>
        {
            Assert.That(scope.EffectiveVisibility, Is.EqualTo(TelemetryTenantVisibility.ActiveTenant));
            Assert.That(scope.TenantId, Is.EqualTo(CallerTenant));
            Assert.That(scope.WasDowngraded, Is.True);
        });
    }

    [Test]
    public async Task The_honoured_and_refused_single_tenant_outcomes_do_not_collapse_on_the_wire()
    {
        var honoured = await RoundTripAsync(TelemetryTenantScope.AtRequestedTenant(OtherTenant));
        var refused = await RoundTripAsync(
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.SingleTenant));

        Assert.Multiple(() =>
        {
            Assert.That(honoured.WasDowngraded, Is.False);
            Assert.That(refused.WasDowngraded, Is.True);
            Assert.That(honoured.EffectiveVisibility, Is.Not.EqualTo(refused.EffectiveVisibility));
            Assert.That(honoured.TenantId, Is.Not.EqualTo(refused.TenantId));
        });
    }

    [Test]
    public async Task Every_scope_outcome_survives_the_wire_byte_for_byte()
    {
        TelemetryTenantScope[] outcomes =
        [
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.ActiveTenant),
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.AllTenants),
            TelemetryTenantScope.AcrossAllTenants(),
            TelemetryTenantScope.PinnedTo(CallerTenant, TelemetryTenantVisibility.SingleTenant),
            TelemetryTenantScope.AtRequestedTenant(OtherTenant),
        ];

        foreach (var expected in outcomes)
        {
            Assert.That(
                await RoundTripAsync(expected),
                Is.EqualTo(expected),
                $"The scope must arrive exactly as pinned: {expected.EffectiveVisibility}/{expected.TenantId}.");
        }
    }

    /// <summary>
    /// A facade that reports one fixed, pre-pinned scope, standing in for whatever
    /// the server-side resolver decided. The binding must relay it untouched.
    /// </summary>
    private sealed class ScopedTelemetry(TelemetryTenantScope scope) : ILatticeTelemetry
    {
        public Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(TelemetryQueryCatalog.Empty);

        public Task<TelemetryQueryResponse> QueryAsync(
            TelemetryQueryRequest request,
            CancellationToken cancellationToken = default)
            => Task.FromResult(new TelemetryQueryResponse
            {
                QueryId = request.QueryId,
                Scope = scope,
                ResultKind = TelemetryResultKind.Empty,
                Series = [],
            });
    }
}
