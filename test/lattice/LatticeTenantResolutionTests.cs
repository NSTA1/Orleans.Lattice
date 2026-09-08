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
    public void ComposeEffectiveTreeId_system_data_tree_name_is_refused_for_a_confined_tenant()
    {
        // Passing an already-qualified name through uncomposed is right for the
        // first-party add-ons that own the 'sys-' trees, but it was also the one
        // way a tenant could name a tree that resolves OUTSIDE its own namespace.
        // The resulting id stays global, so the tree is invisible to the per-tenant
        // tree-count and footprint accounting (which enumerates 't/{tenant}/'), it
        // is shared with every other tenant that picks the same name, and it can
        // collide with a first-party store. A confined tenant is refused.
        Assert.That(
            () => LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), "sys-auth-users"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public void ComposeEffectiveTreeId_system_data_tree_name_is_returned_unchanged_under_system_origin()
    {
        const string name = "sys-auth-users";

        // The add-on that owns the tree runs system-origin, so it keeps the
        // never-double-composed passthrough - the same reference, unchanged.
        using var scope = LatticeAccessGateContext.EnterSystemOrigin();
        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), name);

        Assert.That(effective, Is.SameAs(name));
    }

    [Test]
    public void ComposeEffectiveTreeId_system_data_tree_name_is_returned_unchanged_for_the_default_tenant()
    {
        const string name = "sys-auth-users";

        // The reserved default tenant is the "tenancy off / adopted" identity and
        // is not a confined caller, so a host without tenancy is unaffected.
        var effective = LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Default, name);

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
    // ---- The 't/' namespace has the same escape shape (security review F7) --
    //
    // IsReservedOrQualified treats anything starting 't/' as already-qualified and
    // returns it uncomposed, so it escaped the tenant's own namespace exactly as
    // 'sys-' did. A well-formed 't/{other}/x' is caught downstream by the gate
    // (the owner is another tenant, and no grant covers it), but a MALFORMED id
    // with no second segment - 't/x' - resolves to TreeOwnership.Platform, and the
    // gate admits platform-owned trees unconditionally. The result is a shared,
    // cross-tenant, accounting-invisible namespace reachable by any tenant whose
    // auth policy carries a broad or wildcard scope.

    [Test]
    public void A_confined_tenant_cannot_resolve_a_malformed_tenant_namespace_id()
    {
        // 't/x' has no second segment, so GetOwner cannot attribute it to a tenant
        // and reports it platform-owned - the shape that slips past the gate.
        Assert.That(
            () => LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), "t/x"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public void A_confined_tenant_may_still_address_another_tenants_well_formed_id()
    {
        // Deliberately NOT refused here. A well-formed foreign id must reach the
        // tenancy access gate, which denies the crossing by default but admits it
        // when the owning tenant has issued a matching cross-tenant grant. The
        // resolution layer cannot see grants, so it must not decide crossings -
        // refusing here would break cross-tenant grants outright.
        const string name = "t/acme/orders";

        Assert.That(
            LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), name),
            Is.SameAs(name));
    }

    [Test]
    public void A_tenant_may_still_pass_its_own_already_qualified_id()
    {
        // Load-bearing: LatticeTenantScopedTreeAdmin composes 't/{tenant}/{name}'
        // and re-passes the composed id into the inner facade, which re-resolves
        // it. Refusing this would break every tenant-scoped create.
        const string name = "t/contoso/orders";

        Assert.That(
            LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), name),
            Is.SameAs(name));
    }

    [Test]
    public void A_system_origin_caller_may_resolve_any_tenant_namespace_id()
    {
        // Infrastructure - metering, replication, backup - legitimately addresses
        // other tenants' trees, and runs system-origin to say so.
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();

        Assert.That(
            LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Parse("contoso"), "t/acme/orders"),
            Is.EqualTo("t/acme/orders"));
    }

    [Test]
    public void The_default_tenant_is_unaffected_by_the_tenant_namespace_guard()
    {
        // A host without tenancy resolves as the reserved default tenant and is not
        // a confined caller.
        const string name = "t/acme/orders";

        Assert.That(
            LatticeTenantResolution.ComposeEffectiveTreeId(TenantId.Default, name),
            Is.SameAs(name));
    }
}