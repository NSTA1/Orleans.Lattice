using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Tests that the self-awareness facade honours the resolver's fail-closed
/// denial sentinel instead of reporting it as a tenant.
/// </summary>
/// <remarks>
/// <para>
/// The tenancy resolver signals "this caller may not act as the tenant it
/// asserted" by resolving the uninitialised <c>default(TenantId)</c> - a
/// <c>null</c> <see cref="TenantId.Value"/>, deliberately distinct from the
/// reserved <see cref="TenantId.Default"/> whose value is <c>"default"</c>. The
/// data plane already turns that sentinel into a
/// <see cref="LatticeTenantAccessDeniedException"/>; this surface used to treat
/// it as an ordinary tenant, which produced two wrong answers that these tests
/// pin.
/// </para>
/// <para>
/// The failure was observable end to end: asserting an unregistered tenant made
/// the current-tenant projection answer with a live descriptor carrying a
/// <c>null</c> id and a status of <c>Active</c> - a reassuring answer to "which
/// tenant am I acting as" for an assertion that was in fact refused - while the
/// enumeration emitted a phantom entry with a <c>null</c> id, because the
/// sentinel's null value was added to the accessible set.
/// </para>
/// </remarks>
public sealed partial class LatticeTenantSelfServiceTests
{
    // ---- fail-closed denial sentinel -------------------------------------

    [Test]
    public void GetCurrentTenant_denied_assertion_fails_closed()
    {
        var registry = new FakeTenantRegistry();
        var service = Service(registry, default);

        Assert.That(
            async () => await service.GetCurrentTenantAsync(),
            Throws.TypeOf<LatticeTenantAccessDeniedException>(),
            "A refused assertion must be reported as a denial, never as a live tenant descriptor.");
    }

    [Test]
    public void ListAccessibleTenants_denied_assertion_fails_closed()
    {
        var registry = new FakeTenantRegistry();
        var service = Service(registry, default, allowedTenants: ["acme"]);

        Assert.That(
            async () => await service.ListAccessibleTenantsAsync(),
            Throws.TypeOf<LatticeTenantAccessDeniedException>(),
            "A refused assertion must not enumerate, and must never emit an entry with a null id.");
    }

    [Test]
    public async Task GetCurrentTenant_default_tenant_is_unaffected()
    {
        // The reserved default tenant is a real tenant, not the denial sentinel:
        // an unasserted caller on a tenancy-off cluster must be unchanged.
        var registry = new FakeTenantRegistry();
        var service = Service(registry, TenantId.Default);

        var descriptor = await service.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.TenantId, Is.EqualTo(TenantId.Default.Value));
            Assert.That(descriptor.IsDefault, Is.True);
            Assert.That(descriptor.Status, Is.EqualTo(TenantLifecycleStatus.Active));
        });
    }

    [Test]
    public async Task ListAccessibleTenants_default_tenant_is_unaffected()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));
        var service = Service(
            registry,
            TenantId.Default,
            allowedTenants: ["acme"],
            subject: new LatticeSubject("admin"));

        var tenants = await service.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "acme" }));
            Assert.That(tenants.Any(t => t.TenantId is null), Is.False, "No phantom null-id entry.");
        });
    }

    [Test]
    public async Task GetCurrentTenant_real_tenant_is_unaffected()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(SeededRecord("acme"));
        var service = Service(registry, TenantId.Parse("acme"));

        var descriptor = await service.GetCurrentTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.TenantId, Is.EqualTo("acme"));
            Assert.That(descriptor.IsDefault, Is.False);
        });
    }
}
