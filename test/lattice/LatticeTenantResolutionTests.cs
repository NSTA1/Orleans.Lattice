using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantResolution"/>, the internal helper that
/// composes the effective, tenant-scoped tree id at the <see cref="ILattice"/>
/// resolution boundary. They pin the acceptance bar for T2: the default tenant
/// returns the bare tree name unchanged (tenancy off is byte-for-byte identical),
/// a non-default tenant scopes an unqualified name, an already-qualified name is
/// never double-composed, and a denying resolver fails closed.
/// </summary>
[TestFixture]
public sealed class LatticeTenantResolutionTests
{
    [Test]
    public void ComposeEffectiveTreeId_default_tenant_returns_the_bare_name_unchanged()
    {
        const string name = "orders";

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Default, name);

        Assert.That(effective, Is.SameAs(name));
    }

    [Test]
    public void ComposeEffectiveTreeId_non_default_tenant_composes_a_scoped_id()
    {
        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), "orders");

        Assert.That(effective, Is.EqualTo("t/contoso/orders"));
    }

    [Test]
    public void ComposeEffectiveTreeId_no_tenant_value_throws_access_denied()
    {
        Assert.That(
            () => LatticeTenantResolution.ComposeEffectiveTreeId(default, "orders"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public void ComposeEffectiveTreeId_already_tenant_scoped_name_is_not_double_composed()
    {
        const string name = "t/contoso/orders";

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("fabrikam"), name);

        Assert.That(effective, Is.SameAs(name));
    }

    [Test]
    public void ComposeEffectiveTreeId_system_tree_name_is_returned_unchanged()
    {
        const string name = "_lattice_catalog";

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), name);

        Assert.That(effective, Is.SameAs(name));
    }

    [Test]
    public void ComposeEffectiveTreeId_system_data_tree_name_is_returned_unchanged()
    {
        const string name = "sys-auth-users";

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), name);

        Assert.That(effective, Is.SameAs(name));
    }

    [Test]
    public void ComposeEffectiveTreeId_reserved_name_under_default_tenant_is_returned_unchanged()
    {
        const string name = "t/contoso/orders";

        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Default, name);

        Assert.That(effective, Is.SameAs(name));
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_null_resolver_throws_argument_null()
    {
        Assert.That(
            () => LatticeTenantResolution.ResolveEffectiveTreeIdAsync(null!, "orders"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_null_name_throws_argument_null()
    {
        var resolver = new FakeTenantContextResolver(TenantId.Default);

        Assert.That(
            () => LatticeTenantResolution.ResolveEffectiveTreeIdAsync(resolver, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_empty_name_throws_argument()
    {
        var resolver = new FakeTenantContextResolver(TenantId.Default);

        Assert.That(
            () => LatticeTenantResolution.ResolveEffectiveTreeIdAsync(resolver, string.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_default_tenant_completes_synchronously_with_the_bare_name()
    {
        const string name = "orders";
        var resolver = new FakeTenantContextResolver(TenantId.Default);

        var pending = LatticeTenantResolution.ResolveEffectiveTreeIdAsync(resolver, name);

        Assert.That(pending.IsCompletedSuccessfully, Is.True);
        Assert.That(pending.Result, Is.SameAs(name));
        Assert.That(resolver.AsyncResolutionCount, Is.Zero);
    }

    [Test]
    public async Task ResolveEffectiveTreeIdAsync_non_default_tenant_composes_a_scoped_id()
    {
        var resolver = new FakeTenantContextResolver(TenantId.Parse("contoso"));

        var effective = await LatticeTenantResolution.ResolveEffectiveTreeIdAsync(resolver, "orders");

        Assert.That(effective, Is.EqualTo("t/contoso/orders"));
    }

    [Test]
    public async Task ResolveEffectiveTreeIdAsync_async_only_resolver_composes_via_the_async_path()
    {
        var resolver = new FakeTenantContextResolver(TenantId.Parse("contoso"), resolvesSynchronously: false);

        var effective = await LatticeTenantResolution.ResolveEffectiveTreeIdAsync(resolver, "orders");

        Assert.That(effective, Is.EqualTo("t/contoso/orders"));
        Assert.That(resolver.AsyncResolutionCount, Is.EqualTo(1));
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_sync_denying_resolver_fails_closed()
    {
        var resolver = new FakeTenantContextResolver(default);

        Assert.That(
            () => LatticeTenantResolution.ResolveEffectiveTreeIdAsync(resolver, "orders"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_async_denying_resolver_fails_closed()
    {
        var resolver = new FakeTenantContextResolver(default, resolvesSynchronously: false);

        Assert.That(
            async () => await LatticeTenantResolution.ResolveEffectiveTreeIdAsync(resolver, "orders"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }
}
