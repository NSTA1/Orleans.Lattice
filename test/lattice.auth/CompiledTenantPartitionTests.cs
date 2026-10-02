using static Orleans.Lattice.Auth.Tests.TenantLayerTestRules;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Tests for the tenant-layer tree classification in
/// <see cref="TenantRuleConfinement"/> and for how <see cref="CompiledPolicy"/>
/// builds (or omits) the <see cref="CompiledTenantPartition"/>: tenant rules never
/// enter the operator partition, are inert while the layer is off, and a
/// non-conforming tenant rule that reached the policy tree around the store is
/// dropped rather than compiled.
/// </summary>
[TestFixture]
public sealed class CompiledTenantPartitionTests
{
    private static readonly LatticeSubjectSelector Alice = LatticeSubjectSelector.User("alice");

    [TestCase("t/contoso/orders", true)]
    [TestCase("t/contoso/x/y", true)]
    [TestCase("t/contoso/*", false)]
    [TestCase("t/contoso/a/app/tree", false)]
    [TestCase("t/contoso/a/", false)]
    [TestCase("t/contoso/sys-x", false)]
    [TestCase("t/contoso/_lattice_x", false)]
    [TestCase("t/default/orders", false)]
    [TestCase("t/Contoso/orders", false)]
    [TestCase("t/contoso/", false)]
    [TestCase("t//orders", false)]
    [TestCase("orders", false)]
    [TestCase("sys-auth-policy", false)]
    [TestCase("*", false)]
    public void TryGetTenantLayerTree_classifies_tenant_data_trees(string treeId, bool expected)
    {
        var actual = TenantRuleConfinement.TryGetTenantLayerTree(treeId, out var tenantSpan);
        var tenant = actual ? tenantSpan.ToString() : null;

        Assert.Multiple(() =>
        {
            Assert.That(actual, Is.EqualTo(expected));
            Assert.That(tenant, Is.EqualTo(expected ? "contoso" : null));
        });
    }

    [TestCase("t/contoso/a/app/tree", true)]
    [TestCase("t/contoso/orders", false)]
    [TestCase("t/default/a/app/tree", false)]
    [TestCase("a/app/tree", false)]
    public void TryGetAppOwnedTenantTree_classifies_app_owned_tenant_trees(string treeId, bool expected)
    {
        Assert.That(TenantRuleConfinement.TryGetAppOwnedTenantTree(treeId, out _), Is.EqualTo(expected));
    }

    [TestCase("t/contoso/*", true)]
    [TestCase("t/default/*", true)]
    [TestCase("t/x/y/*", true)]
    [TestCase("t/*", false)]
    [TestCase("t/contoso/orders", false)]
    [TestCase("*", false)]
    public void IsTenantWideSentinelShape_matches_the_sentinel_shape(string treeId, bool expected)
    {
        Assert.That(TenantRuleConfinement.IsTenantWideSentinelShape(treeId), Is.EqualTo(expected));
    }

    [Test]
    public void EnsureConfined_null_rule_throws()
    {
        Assert.That(() => TenantRuleConfinement.EnsureConfined(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Compile_with_the_layer_off_builds_no_partition_and_keeps_tenant_rules_out_of_the_operator_partition()
    {
        var rules = new[] { Tenant("tree", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow) };

        var policy = CompiledPolicy.Compile(rules);

        Assert.Multiple(() =>
        {
            Assert.That(policy.Tenant, Is.Null);
            Assert.That(policy.TreeCount, Is.Zero);
            Assert.That(policy.DistinctSubjectCount, Is.Zero);
        });
    }

    [Test]
    public void Compile_with_the_layer_on_indexes_tree_and_tenant_wide_rules_separately()
    {
        var rules = new[]
        {
            Tenant("tree", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("wide", LatticeSubjectSelector.Group(ContosoReaders), LatticeScope.TenantWide(Contoso), LatticeOperation.Read, LatticeEffect.Allow),
            Operator("ops", Alice, LatticeScope.Tree("orders"), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var policy = CompiledPolicy.Compile(rules, includeTenantLayer: true);

        Assert.Multiple(() =>
        {
            Assert.That(policy.TreeCount, Is.EqualTo(1), "only the operator rule is in the operator partition");
            Assert.That(policy.Tenant!.TreeCount, Is.EqualTo(1));
            Assert.That(policy.Tenant!.TenantWideCount, Is.EqualTo(1));
            Assert.That(policy.DistinctSubjectCount, Is.EqualTo(2));
            Assert.That(policy.Tenant!.TryGetBuckets(TenantTree, out var tree, out var wide), Is.True);
            Assert.That(tree, Is.Not.Null);
            Assert.That(wide, Is.Not.Null);
        });
    }

    [Test]
    public void Compile_drops_a_tenant_rule_that_escaped_the_store_confinement()
    {
        // Each would be refused by the store; one reaching the tree by restore or
        // replication must not govern anything.
        var rules = new LatticeAuthorizationRule[]
        {
            Tenant("other-tree", Alice, LatticeScope.Tree("t/fabrikam/orders"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("app-tree", Alice, LatticeScope.Tree("t/contoso/a/x/y"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("legacy", Alice, LatticeScope.Tree("orders"), LatticeOperation.Read, LatticeEffect.Allow),
            Tenant("telemetry", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Telemetry, LatticeEffect.Allow),
            Tenant("other-group", LatticeSubjectSelector.Group("t/fabrikam/g"), LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow),
            new("tenant:bad", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow),
        };

        var policy = CompiledPolicy.Compile(rules, includeTenantLayer: true);

        Assert.Multiple(() =>
        {
            Assert.That(policy.Tenant, Is.Null);
            Assert.That(policy.TreeCount, Is.Zero);
            Assert.That(policy.TenantLayerIncluded, Is.True);
        });
    }

    [Test]
    public void TryGetBuckets_reports_nothing_for_a_tenant_tree_without_tenant_rules()
    {
        var policy = CompiledPolicy.Compile(
            new[] { Tenant("tree", Alice, LatticeScope.Tree(TenantTree), LatticeOperation.Read, LatticeEffect.Allow) },
            includeTenantLayer: true);

        Assert.Multiple(() =>
        {
            Assert.That(policy.Tenant!.TryGetBuckets("t/contoso/other", out _, out _), Is.False);
            Assert.That(policy.Tenant!.TryGetBuckets("t/fabrikam/orders", out _, out _), Is.False);
        });
    }

    [Test]
    public void Build_null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => CompiledTenantPartition.Build(null!, new()), Throws.ArgumentNullException);
            Assert.That(() => CompiledTenantPartition.Build(Array.Empty<LatticeAuthorizationRule>(), null!), Throws.ArgumentNullException);
            Assert.That(CompiledTenantPartition.Build(Array.Empty<LatticeAuthorizationRule>(), new()), Is.Null);
        });
    }
}
