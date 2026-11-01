namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the tenant capability scope on <see cref="LatticeScope"/>
/// (issue #5582): <see cref="LatticeScope.TenantCapability(TenantId)"/> builds a
/// whole-tree scope over the tenant's control-plane capability id and refuses the
/// reserved default tenant and the "no tenant" value;
/// <see cref="LatticeScope.TryGetTenantCapabilityTenant"/> recognises exactly that
/// shape and nothing else.
/// </summary>
[TestFixture]
public sealed class LatticeScopeTenantCapabilityTests
{
    private static readonly TenantId Acme = TenantId.Parse("acme");

    [Test]
    public void TenantCapability_is_a_whole_tree_scope_over_the_tenant_admin_capability_id()
    {
        var scope = LatticeScope.TenantCapability(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(scope.Kind, Is.EqualTo(LatticeScopeKind.Tree));
            Assert.That(scope.TreeId, Is.EqualTo("_lattice_tenant_admin_acme"));
            Assert.That(scope.TreeId, Is.EqualTo(LatticeTenantAdminScope.ForTenant(Acme).TreeScope));
            Assert.That(scope.KeyOrPrefix, Is.Null);
        });
    }

    [Test]
    public void TenantCapability_is_distinct_from_the_cluster_wide_and_tenant_wide_scopes()
    {
        var scope = LatticeScope.TenantCapability(Acme);

        Assert.Multiple(() =>
        {
            Assert.That(scope.TreeId, Is.Not.EqualTo(LatticeScope.ClusterWideTreeId));
            Assert.That(scope.TreeId, Is.Not.EqualTo(LatticeScope.TenantWide(Acme).TreeId));
            Assert.That(scope.IsTenantWide(), Is.False);
        });
    }

    [Test]
    public void TenantCapability_refuses_the_reserved_default_tenant() =>
        Assert.That(() => LatticeScope.TenantCapability(TenantId.Default), Throws.ArgumentException);

    [Test]
    public void TenantCapability_refuses_the_uninitialised_tenant() =>
        Assert.That(() => LatticeScope.TenantCapability(default), Throws.ArgumentException);

    [Test]
    public void TryGetTenantCapabilityTenant_round_trips_the_tenant()
    {
        var recognised = LatticeScope.TenantCapability(Acme).TryGetTenantCapabilityTenant(out var tenant);

        Assert.Multiple(() =>
        {
            Assert.That(recognised, Is.True);
            Assert.That(tenant, Is.EqualTo(Acme));
        });
    }

    [TestCase("*")]
    [TestCase("t/acme/*")]
    [TestCase("t/acme/orders")]
    [TestCase("sys-auth-policy")]
    [TestCase("_lattice_tenant_admin_")]
    [TestCase("_lattice_tenant_admin_default")]
    [TestCase("_lattice_tenant_admin_Not Valid")]
    [TestCase("_lattice_other_acme")]
    public void TryGetTenantCapabilityTenant_rejects_any_other_tree_scope(string treeId)
    {
        var recognised = LatticeScope.Tree(treeId).TryGetTenantCapabilityTenant(out var tenant);

        Assert.Multiple(() =>
        {
            Assert.That(recognised, Is.False);
            Assert.That(tenant, Is.EqualTo(default(TenantId)));
        });
    }

    [Test]
    public void TryGetTenantCapabilityTenant_rejects_a_key_or_prefix_scope_on_the_capability_id()
    {
        var treeId = LatticeScope.TenantCapability(Acme).TreeId;

        Assert.Multiple(() =>
        {
            Assert.That(LatticeScope.Key(treeId, "k").TryGetTenantCapabilityTenant(out _), Is.False);
            Assert.That(LatticeScope.Prefix(treeId, "p").TryGetTenantCapabilityTenant(out _), Is.False);
        });
    }
}
