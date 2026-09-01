using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Fail-closed and status-mapping edge tests for <see cref="LatticeTenantSelfService"/>:
/// a tenant outside the caller's authority is refused indistinguishably from an
/// absent one; the resolver's <c>default(TenantId)</c> "no tenant" sentinel is
/// never surfaced as a live null-id descriptor but refused; an uncached subject is
/// resolved under a gate-bypassing system-origin scope; and out-of-range persisted
/// tenant and region statuses map to their safe lifecycle defaults.
/// </summary>
public sealed partial class LatticeTenantSelfServiceTests
{
    [Test]
    public void GetTenantAsync_a_tenant_outside_the_callers_authority_is_indistinguishable_from_absent()
    {
        var registry = new FakeTenantRegistry();
        // The tenant exists, the caller is a real subject, but it does not administer
        // this tenant, so the inspect path must fall through to a fail-closed
        // not-found rather than confirming the tenant's existence.
        registry.Seed(SeededRecord("acme"));
        var service = Service(
            registry, TenantId.Default, allowedTenants: ["zzz"], subject: new LatticeSubject("op"));

        Assert.That(
            async () => await service.GetTenantAsync("acme"),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void ListAccessibleTenantsAsync_a_policy_sentinel_tenant_is_refused_never_surfaced_as_null()
    {
        var registry = new FakeTenantRegistry();
        var resolver = new FakeTenantContextResolver(TenantId.Default);
        var policyEngine = new SentinelPolicyEngine();
        var membership = new FixedMembershipContext(new LatticeSubject("op"));
        var service = new LatticeTenantSelfService(resolver, policyEngine, registry, membership);

        // A policy engine that surfaces the "no tenant" sentinel as an allowed tenant
        // must never produce a live null-id descriptor: the read fails closed.
        Assert.That(
            async () => await service.ListAccessibleTenantsAsync(),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public async Task ListAccessibleTenantsAsync_resolves_an_uncached_subject_under_system_origin()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));
        var resolver = new FakeTenantContextResolver(TenantId.Parse("acme"));
        var policyEngine = new FakeTenantPolicyEngine([]);
        var membership = new CacheMissMembershipContext(new LatticeSubject("op"));
        var service = new LatticeTenantSelfService(resolver, policyEngine, registry, membership);

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "acme" }));
            Assert.That(membership.ResolveCurrentCalled, Is.True);
            Assert.That(
                membership.ResolvedUnderSystemOrigin,
                Is.True,
                "the uncached subject resolution must run under a gate-bypassing system-origin scope.");
        });
    }

    [Test]
    public async Task GetCurrentTenantAsync_maps_an_out_of_range_status_to_the_safe_default()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme", (TenantStatus)99));
        var service = Service(registry, TenantId.Parse("acme"));

        var current = await service.GetCurrentTenantAsync();

        Assert.That(current.Status, Is.EqualTo(TenantLifecycleStatus.Active));
    }

    [Test]
    public async Task GetTenantAsync_maps_every_region_lifecycle_status()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord(
            "acme",
            statuses:
            [
                ("r-prov", TenantRegionStatus.Provisioning),
                ("r-back", TenantRegionStatus.Backfilling),
                ("r-drain", TenantRegionStatus.Draining),
                ("r-off", TenantRegionStatus.Offline),
                ("r-rem", TenantRegionStatus.Removed),
            ]));
        var service = Service(registry, TenantId.Parse("acme"));

        var report = await service.GetTenantAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(
                report.Regions.Single(r => r.RegionId == "r-prov").Status,
                Is.EqualTo(TenantRegionLifecycleStatus.Provisioning));
            Assert.That(
                report.Regions.Single(r => r.RegionId == "r-back").Status,
                Is.EqualTo(TenantRegionLifecycleStatus.Backfilling));
            Assert.That(
                report.Regions.Single(r => r.RegionId == "r-drain").Status,
                Is.EqualTo(TenantRegionLifecycleStatus.Draining));
            Assert.That(
                report.Regions.Single(r => r.RegionId == "r-off").Status,
                Is.EqualTo(TenantRegionLifecycleStatus.Offline));
            Assert.That(
                report.Regions.Single(r => r.RegionId == "r-rem").Status,
                Is.EqualTo(TenantRegionLifecycleStatus.Removed));
        });
    }

    /// <summary>
    /// A policy engine whose <see cref="ResolveAllowedTenants"/> returns the
    /// uninitialised <c>default(TenantId)</c> "no tenant" sentinel, modelling a
    /// broken or adversarial policy source; used to prove the facade refuses it
    /// rather than emitting a null-id descriptor.
    /// </summary>
    private sealed class SentinelPolicyEngine : ITenantPolicyEngine
    {
        public long CurrentEpoch => 0;

        public IReadOnlyList<TenantId> ResolveAllowedTenants(string subjectId)
        {
            ArgumentNullException.ThrowIfNull(subjectId);
            return new[] { default(TenantId) };
        }

        public TenantAccessDecision ValidateActiveTenant(string subjectId, TenantId activeTenant)
            => throw new NotSupportedException();

        public TenantAccessDecision ResolveCrossTenantGrant(
            TenantId sourceTenant, TenantId targetTenant, string scope, TenantGrantOperations operation)
            => throw new NotSupportedException();
    }
}
