namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantTrees"/>: the reserved <c>t/</c>
/// segment prefix, composing a segmented tree id from a tenant and an
/// unqualified name, the tenant-scoped prefix check, and parsing the owning
/// tenant back out (including round-trip compose/parse).
/// </summary>
[TestFixture]
public sealed class LatticeTenantTreesTests
{
    [Test]
    public void SegmentPrefix_is_the_reserved_tenant_namespace()
    {
        Assert.That(LatticeTenantTrees.SegmentPrefix, Is.EqualTo("t/"));
    }

    [Test]
    public void Compose_builds_the_segmented_tree_id()
    {
        var id = LatticeTenantTrees.Compose(TenantId.Parse("contoso"), "orders");

        Assert.That(id, Is.EqualTo("t/contoso/orders"));
    }

    [Test]
    public void Compose_preserves_slashes_in_the_local_name()
    {
        var id = LatticeTenantTrees.Compose(TenantId.Parse("contoso"), "orders/eu");

        Assert.That(id, Is.EqualTo("t/contoso/orders/eu"));
    }

    [Test]
    public void Compose_no_tenant_value_throws_argument()
    {
        Assert.That(
            () => LatticeTenantTrees.Compose(default, "orders"),
            Throws.ArgumentException);
    }

    [TestCase(null)]
    [TestCase("")]
    public void Compose_null_or_empty_name_throws_argument(string? name)
    {
        Assert.That(
            () => LatticeTenantTrees.Compose(TenantId.Parse("contoso"), name!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void IsTenantScoped_is_true_for_a_tenant_tree_id()
    {
        Assert.That(LatticeTenantTrees.IsTenantScoped("t/contoso/orders"), Is.True);
    }

    [TestCase("orders")]
    [TestCase("_lattice_trees")]
    [TestCase("sys-auth-users")]
    [TestCase("view-orders")]
    [TestCase("t")]
    public void IsTenantScoped_is_false_for_a_non_tenant_tree_id(string treeId)
    {
        Assert.That(LatticeTenantTrees.IsTenantScoped(treeId), Is.False);
    }

    [Test]
    public void IsTenantScoped_null_throws_argument_null()
    {
        Assert.That(() => LatticeTenantTrees.IsTenantScoped(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetTenant_extracts_the_owning_tenant()
    {
        var ok = LatticeTenantTrees.TryGetTenant("t/contoso/orders", out var tenant);

        Assert.That(ok, Is.True);
        Assert.That(tenant, Is.EqualTo(TenantId.Parse("contoso")));
    }

    [Test]
    public void TryGetTenant_ignores_slashes_after_the_tenant_segment()
    {
        var ok = LatticeTenantTrees.TryGetTenant("t/contoso/orders/eu", out var tenant);

        Assert.That(ok, Is.True);
        Assert.That(tenant.Value, Is.EqualTo("contoso"));
    }

    [TestCase("orders", TestName = "non_tenant")]
    [TestCase("t/contoso", TestName = "missing_name_segment")]
    [TestCase("t/contoso/", TestName = "empty_name_segment")]
    [TestCase("t//orders", TestName = "empty_tenant_segment")]
    [TestCase("t/-bad/orders", TestName = "invalid_tenant_grammar")]
    [TestCase("t/CONTOSO/orders", TestName = "uppercase_tenant")]
    public void TryGetTenant_returns_false_for_a_malformed_id(string treeId)
    {
        var ok = LatticeTenantTrees.TryGetTenant(treeId, out var tenant);

        Assert.That(ok, Is.False);
        Assert.That(tenant, Is.EqualTo(default(TenantId)));
    }

    [Test]
    public void TryGetTenant_null_throws_argument_null()
    {
        Assert.That(() => LatticeTenantTrees.TryGetTenant(null!, out _), Throws.ArgumentNullException);
    }

    [TestCase("contoso", "orders")]
    [TestCase("t0", "a")]
    [TestCase("default", "legacy")]
    public void Compose_then_TryGetTenant_round_trips_the_tenant(string tenantId, string name)
    {
        var tenant = TenantId.Parse(tenantId);

        var composed = LatticeTenantTrees.Compose(tenant, name);
        var ok = LatticeTenantTrees.TryGetTenant(composed, out var parsed);

        Assert.That(ok, Is.True);
        Assert.That(parsed, Is.EqualTo(tenant));
        Assert.That(LatticeTenantTrees.IsTenantScoped(composed), Is.True);
    }
}
