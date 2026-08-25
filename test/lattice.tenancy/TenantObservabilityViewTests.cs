using static Orleans.Lattice.Tenancy.Tests.ObservabilityTestData;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantObservabilityView"/>: the fail-closed per-tenant
/// visibility seam (deliverable 3, security-critical). Exercises the full isolation
/// matrix - a tenant sees only its own series; a tenant never sees another tenant's
/// series; an operator without the explicit cluster-wide assertion still sees only
/// its active tenant; only an operator whose subject authorises against the auth
/// gate's platform-operator root of trust sees every tenant; a denied or anonymous
/// cluster-wide assertion falls back, fail-closed, to the active tenant. The ambient
/// active tenant is set directly and the gate is a hand-written double, so every
/// decision is exact and timing-independent.
/// </summary>
[TestFixture]
public sealed class TenantObservabilityViewTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");
    private static readonly LatticeSubject Operator = new("op-1");

    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static FakeTenantUsageIndex TwoTenants() => new FakeTenantUsageIndex()
        .With(Acme, View(Quotas(bytes: 1000), Usage(bytes: 100)))
        .With(Beta, View(Quotas(bytes: 2000), Usage(bytes: 200)));

    private static TenantObservabilityView Create(FakeTenantUsageIndex usage, ILatticeAccessGate gate, FakeTenantOverageBilling? billing = null) =>
        new(new TenantObservabilitySource(usage, billing ?? new FakeTenantOverageBilling()), gate);

    private static async Task<List<TenantObservabilitySnapshot>> ListAsync(TenantObservabilityView view, TenantObservabilityScope scope)
    {
        var results = new List<TenantObservabilitySnapshot>();
        await foreach (var snapshot in view.ListAsync(scope))
        {
            results.Add(snapshot);
        }

        return results;
    }

    // ---- GetActiveTenantAsync ------------------------------------------

    [Test]
    public async Task GetActiveTenantAsync_returns_the_callers_own_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var view = Create(TwoTenants(), AllowingGate());

        var snapshot = await view.GetActiveTenantAsync();

        Assert.That(snapshot, Is.Not.Null);
        Assert.That(snapshot!.Value.Tenant, Is.EqualTo(Acme));
    }

    [Test]
    public async Task GetActiveTenantAsync_with_no_active_tenant_is_null()
    {
        var view = Create(TwoTenants(), AllowingGate());

        Assert.That(await view.GetActiveTenantAsync(), Is.Null);
    }

    [Test]
    public async Task GetActiveTenantAsync_for_an_unregistered_active_tenant_is_null()
    {
        LatticeActiveTenantContext.Current = TenantId.Parse("ghost");
        var view = Create(TwoTenants(), AllowingGate());

        Assert.That(await view.GetActiveTenantAsync(), Is.Null);
    }

    // ---- ListAsync: tenant isolation -----------------------------------

    [Test]
    public async Task ListAsync_active_tenant_scope_yields_only_the_callers_own_series()
    {
        LatticeActiveTenantContext.Current = Acme;
        var gate = AllowingGate();
        var view = Create(TwoTenants(), gate);

        var results = await ListAsync(view, TenantObservabilityScope.ActiveTenant);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(s => s.Tenant), Is.EquivalentTo(new[] { Acme }));
            Assert.That(gate.CallCount, Is.Zero, "the default per-tenant path never consults the operator gate");
        });
    }

    [Test]
    public async Task ListAsync_active_tenant_scope_never_reveals_another_tenants_series()
    {
        LatticeActiveTenantContext.Current = Acme;
        var view = Create(TwoTenants(), AllowingGate());

        var results = await ListAsync(view, TenantObservabilityScope.ActiveTenant);

        Assert.That(results.Select(s => s.Tenant), Does.Not.Contain(Beta), "a tenant can never observe another tenant's series");
    }

    [Test]
    public async Task ListAsync_active_tenant_scope_with_no_active_tenant_is_empty()
    {
        var view = Create(TwoTenants(), AllowingGate());

        Assert.That(await ListAsync(view, TenantObservabilityScope.ActiveTenant), Is.Empty);
    }

    // ---- ListAsync: operator scope -------------------------------------

    [Test]
    public async Task ListAsync_cluster_wide_with_an_authorised_operator_yields_every_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var gate = AllowingGate();
        var view = Create(TwoTenants(), gate);

        var results = await ListAsync(view, TenantObservabilityScope.ClusterWide(Operator));

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(s => s.Tenant), Is.EquivalentTo(new[] { Acme, Beta }));
            Assert.That(gate.CallCount, Is.EqualTo(1), "the cluster-wide assertion is validated against the operator gate exactly once");
        });
    }

    [Test]
    public async Task ListAsync_operator_without_the_explicit_cluster_wide_scope_sees_only_the_active_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var gate = AllowingGate();
        var view = Create(TwoTenants(), gate);

        // The caller is an operator (the gate would allow), but passes the default
        // scope - so there is no ambient all-tenant view.
        var results = await ListAsync(view, TenantObservabilityScope.ActiveTenant);

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(s => s.Tenant), Is.EquivalentTo(new[] { Acme }));
            Assert.That(gate.CallCount, Is.Zero, "no explicit assertion means the operator gate is never consulted");
        });
    }

    [Test]
    public async Task ListAsync_cluster_wide_with_a_denied_subject_fails_closed_to_the_active_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var gate = DenyingGate();
        var view = Create(TwoTenants(), gate);

        var results = await ListAsync(view, TenantObservabilityScope.ClusterWide(Operator));

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(s => s.Tenant), Is.EquivalentTo(new[] { Acme }), "a non-operator falls back to its own tenant only");
            Assert.That(gate.CallCount, Is.EqualTo(1), "the assertion was evaluated and denied");
        });
    }

    [Test]
    public async Task ListAsync_cluster_wide_with_a_denied_subject_and_no_active_tenant_is_empty()
    {
        var view = Create(TwoTenants(), DenyingGate());

        var results = await ListAsync(view, TenantObservabilityScope.ClusterWide(Operator));

        Assert.That(results, Is.Empty, "fail-closed with no active tenant reveals nothing");
    }

    [Test]
    public async Task ListAsync_cluster_wide_with_an_anonymous_subject_never_consults_the_gate_and_sees_only_the_active_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var gate = AllowingGate();
        var view = Create(TwoTenants(), gate);

        var results = await ListAsync(view, TenantObservabilityScope.ClusterWide(LatticeSubject.Anonymous));

        Assert.Multiple(() =>
        {
            Assert.That(results.Select(s => s.Tenant), Is.EquivalentTo(new[] { Acme }));
            Assert.That(gate.CallCount, Is.Zero, "an anonymous subject short-circuits before the gate");
        });
    }

    [Test]
    public async Task ListAsync_cluster_wide_surfaces_the_metered_overage_signal_per_tenant()
    {
        LatticeActiveTenantContext.Current = Acme;
        var billing = new FakeTenantOverageBilling().With(Beta, Overage(bytes: 42));
        var view = Create(TwoTenants(), AllowingGate(), billing);

        var results = await ListAsync(view, TenantObservabilityScope.ClusterWide(Operator));

        var beta = results.Single(s => s.Tenant.Equals(Beta));
        Assert.That(beta.MeteredOverage, Is.EqualTo(Overage(bytes: 42)), "the operator view surfaces each tenant's metered overage");
    }
}
