using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the tenant-wide scope on <see cref="LatticeScope"/>:
/// <see cref="LatticeScope.TenantWide(TenantId)"/> builds a whole-tree scope over
/// the sentinel id <c>t/{tenant}/*</c> and refuses the reserved <c>default</c>
/// tenant; <see cref="LatticeScope.IsTenantWide"/> and
/// <see cref="LatticeScope.TryGetTenantWideTenant"/> recognise exactly that shape
/// and nothing else, allocation-free on the negative path.
/// </summary>
[TestFixture]
public sealed class LatticeScopeTenantWideTests
{
    private static readonly TenantId Contoso = TenantId.Parse("contoso");

    [Test]
    public void TenantWide_is_a_whole_tree_scope_over_the_sentinel()
    {
        var scope = LatticeScope.TenantWide(Contoso);

        Assert.Multiple(() =>
        {
            Assert.That(scope.Kind, Is.EqualTo(LatticeScopeKind.Tree));
            Assert.That(scope.TreeId, Is.EqualTo("t/contoso/*"));
            Assert.That(scope.KeyOrPrefix, Is.Null);
        });
    }

    [Test]
    public void TenantWide_sentinel_is_the_tenant_tree_named_star()
    {
        // The data plane refuses to create a tenant tree whose id ends in "/*"
        // (LatticeGrainTenantWideSentinelGuardTests), so the sentinel can never
        // collide with a legal tree id.
        Assert.That(LatticeScope.TenantWide(Contoso).TreeId, Is.EqualTo(LatticeTenantTrees.Compose(Contoso, "*")));
    }

    [Test]
    public void TenantWide_is_owned_by_the_tenant_and_distinct_from_the_cluster_wide_sentinel()
    {
        var scope = LatticeScope.TenantWide(Contoso);

        Assert.That(LatticeTenantTrees.GetOwner(scope.TreeId).Tenant, Is.EqualTo(Contoso));
        Assert.That(scope.TreeId, Is.Not.EqualTo(LatticeScope.ClusterWideTreeId));
        Assert.That(LatticeScope.ClusterWide().IsTenantWide(), Is.False);
    }

    [Test]
    public void TenantWide_refuses_the_default_tenant()
    {
        Assert.That(
            () => LatticeScope.TenantWide(TenantId.Default),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void TenantWide_refuses_the_uninitialised_tenant()
    {
        Assert.That(
            () => LatticeScope.TenantWide(default),
            Throws.ArgumentException.With.Property(nameof(ArgumentException.ParamName)).EqualTo("tenant"));
    }

    [Test]
    public void TenantWide_scopes_are_equal_by_value()
    {
        Assert.That(LatticeScope.TenantWide(Contoso), Is.EqualTo(LatticeScope.Tree("t/contoso/*")));
    }

    [Test]
    public void IsTenantWide_and_TryGetTenantWideTenant_recognise_the_sentinel()
    {
        var scope = LatticeScope.TenantWide(TenantId.Parse("contoso-eu"));

        Assert.That(scope.IsTenantWide(), Is.True);
        Assert.That(scope.TryGetTenantWideTenant(out var tenant), Is.True);
        Assert.That(tenant, Is.EqualTo(TenantId.Parse("contoso-eu")));
    }

    [Test]
    public void A_tree_scope_over_the_sentinel_id_is_tenant_wide()
    {
        Assert.That(LatticeScope.Tree("t/contoso/*").IsTenantWide(), Is.True);
    }

    [TestCase("t/contoso/orders")]
    [TestCase("t/contoso/*/x")]
    [TestCase("t/contoso/orders/*")]
    [TestCase("t/contoso/**")]
    [TestCase("t/contoso*")]
    [TestCase("t//*")]
    [TestCase("t/*")]
    [TestCase("t/default/*")]
    [TestCase("t/Contoso/*")]
    [TestCase("t/-contoso/*")]
    [TestCase("x/contoso/*")]
    [TestCase("contoso/*")]
    [TestCase("*")]
    [TestCase("/*")]
    public void Other_tree_scopes_are_not_tenant_wide(string treeId)
    {
        var scope = LatticeScope.Tree(treeId);

        Assert.That(scope.IsTenantWide(), Is.False);
        Assert.That(scope.TryGetTenantWideTenant(out var tenant), Is.False);
        Assert.That(tenant, Is.EqualTo(default(TenantId)));
    }

    [Test]
    public void Key_and_prefix_scopes_over_the_sentinel_id_are_not_tenant_wide()
    {
        Assert.That(LatticeScope.Key("t/contoso/*", "k").IsTenantWide(), Is.False);
        Assert.That(LatticeScope.Prefix("t/contoso/*", "p").IsTenantWide(), Is.False);
    }

    [Test]
    public void IsTenantWide_allocates_nothing()
    {
        LatticeScope[] scopes =
        [
            LatticeScope.TenantWide(Contoso),
            LatticeScope.Tree("t/contoso/orders"),
            LatticeScope.ClusterWide(),
            LatticeScope.Key("orders", "k"),
        ];

        var growth = AllocationProbe.Growth(
            prepare: _ => scopes,
            measure: static (state, size) =>
            {
                long hits = 0;
                for (var i = 0; i < size; i++)
                {
                    foreach (var scope in state)
                    {
                        if (scope.IsTenantWide())
                        {
                            hits++;
                        }
                    }
                }

                AllocationProbe.ScalarSink += hits;
            },
            smallSize: 100,
            largeSize: 10_000);

        Assert.That(growth, Is.Zero);
    }
}
