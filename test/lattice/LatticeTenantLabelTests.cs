using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantLabel"/>: the three-way tree-ownership
/// classification behind the derived <c>tenant</c> metric dimension, its
/// agreement with <see cref="LatticeTenantTrees.GetOwner"/>, the frozen
/// singletons that keep the measurement path allocation-free, and the
/// platform-sentinel measurement helper.
/// </summary>
[TestFixture]
public sealed class LatticeTenantLabelTests
{
    [Test]
    public void TagTenant_is_the_derived_tenant_dimension_key()
    {
        Assert.That(LatticeTenantLabel.TagTenant, Is.EqualTo("tenant"));
    }

    [Test]
    public void DefaultTenant_is_the_reserved_legacy_adoption_tenant()
    {
        Assert.That(LatticeTenantLabel.DefaultTenant, Is.EqualTo(TenantId.DefaultId));
    }

    [Test]
    public void PlatformTenant_is_distinct_from_the_default_tenant()
    {
        Assert.That(LatticeTenantLabel.PlatformTenant, Is.Not.EqualTo(LatticeTenantLabel.DefaultTenant));
    }

    [Test]
    public void PlatformTenant_cannot_collide_with_any_valid_tenant_id()
    {
        // The tenant-id grammar forbids a leading underscore, so the sentinel is
        // unreachable by any tenant a caller could ever register.
        Assert.That(TenantId.TryParse(LatticeTenantLabel.PlatformTenant, out _), Is.False);
    }

    [Test]
    public void ForTree_classifies_a_tenant_scoped_id_as_the_owning_tenant()
    {
        var tag = LatticeTenantLabel.ForTree("t/acme/orders");

        Assert.Multiple(() =>
        {
            Assert.That(tag.Key, Is.EqualTo(LatticeTenantLabel.TagTenant));
            Assert.That(tag.Value, Is.EqualTo("acme"));
        });
    }

    [Test]
    public void ForTree_classifies_a_bare_legacy_id_as_the_default_tenant()
    {
        Assert.That(LatticeTenantLabel.Resolve("orders"), Is.EqualTo(LatticeTenantLabel.DefaultTenant));
    }

    [Test]
    public void ForTree_classifies_a_system_internal_id_as_the_platform_sentinel()
    {
        Assert.That(
            LatticeTenantLabel.Resolve(LatticeConstants.SystemTreePrefix + "registry"),
            Is.EqualTo(LatticeTenantLabel.PlatformTenant));
    }

    [Test]
    public void ForTree_classifies_a_system_data_id_as_the_platform_sentinel()
    {
        Assert.That(
            LatticeTenantLabel.Resolve(LatticeConstants.SystemDataTreePrefix + "tenant-registry"),
            Is.EqualTo(LatticeTenantLabel.PlatformTenant));
    }

    [Test]
    public void ForTree_classifies_a_null_id_as_the_platform_sentinel()
    {
        Assert.That(LatticeTenantLabel.Resolve(null), Is.EqualTo(LatticeTenantLabel.PlatformTenant));
    }

    [TestCase("t/")]
    [TestCase("t//orders")]
    [TestCase("t/acme")]
    [TestCase("t/acme/")]
    [TestCase("t/NOTVALID/orders")]
    public void ForTree_classifies_a_malformed_reserved_namespace_id_as_the_platform_sentinel(string treeId)
    {
        // A malformed id in the reserved t/ namespace is not a bare legacy id, so
        // it must never be adopted by the default tenant and leak into a view.
        Assert.That(LatticeTenantLabel.Resolve(treeId), Is.EqualTo(LatticeTenantLabel.PlatformTenant));
    }

    [Test]
    public void ForTree_classifies_a_view_tree_as_its_owning_tenant()
    {
        // A materialised view of a tenant's tree is tenant-owned: the reserved
        // view- prefix sits inside the tenant segment, not in front of it.
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantLabel.Resolve("t/acme/view-orders-by-region"), Is.EqualTo("acme"));
            Assert.That(LatticeTenantLabel.Resolve("view-orders-by-region"), Is.EqualTo(LatticeTenantLabel.DefaultTenant));
        });
    }

    [TestCaseSource(nameof(OwnershipCases))]
    public void ForTree_agrees_with_the_tree_ownership_classifier(string treeId)
    {
        // LatticeTenantLabel re-implements the classification span-first so the
        // measurement path never allocates. This pins the two to one answer, so
        // the label can never drift from the ownership rule the rest of the
        // tenancy surface enforces.
        var owner = LatticeTenantTrees.GetOwner(treeId);
        var expected = owner.IsPlatformOwned
            ? LatticeTenantLabel.PlatformTenant
            : owner.Tenant.Value;

        Assert.That(LatticeTenantLabel.Resolve(treeId), Is.EqualTo(expected));
    }

    private static IEnumerable<string> OwnershipCases()
    {
        yield return "orders";
        yield return "view-orders";
        yield return "t/acme/orders";
        yield return "t/acme/view-orders";
        yield return "t/globex/audit";
        yield return "t/a/x";
        yield return "t/";
        yield return "t/acme";
        yield return "t/acme/";
        yield return "t//orders";
        yield return "t/NOTVALID/orders";
        yield return LatticeConstants.SystemTreePrefix + "registry";
        yield return LatticeConstants.SystemDataTreePrefix + "tenant-registry";
        yield return string.Empty;
    }

    [Test]
    public void ForTree_returns_the_frozen_default_singleton_for_every_bare_id()
    {
        // The tenancy-off path must not allocate: every bare id resolves to the
        // one pre-built tag instance rather than a fresh string per measurement.
        var first = LatticeTenantLabel.ForTree("orders");
        var second = LatticeTenantLabel.ForTree("invoices");

        Assert.Multiple(() =>
        {
            Assert.That(first.Value, Is.SameAs(LatticeTenantLabel.Default.Value));
            Assert.That(second.Value, Is.SameAs(LatticeTenantLabel.Default.Value));
        });
    }

    [Test]
    public void ForTree_returns_the_frozen_platform_singleton_for_every_system_id()
    {
        var first = LatticeTenantLabel.ForTree(LatticeConstants.SystemTreePrefix + "registry");
        var second = LatticeTenantLabel.ForTree(LatticeConstants.SystemDataTreePrefix + "tenant-usage");

        Assert.Multiple(() =>
        {
            Assert.That(first.Value, Is.SameAs(LatticeTenantLabel.Platform.Value));
            Assert.That(second.Value, Is.SameAs(LatticeTenantLabel.Platform.Value));
        });
    }

    [Test]
    public void ForTree_reuses_one_cached_tag_instance_per_tenant()
    {
        // The tenant id substring is materialised once per tenant for the process
        // lifetime; every later measurement is a span-keyed cache hit that
        // allocates nothing.
        var first = LatticeTenantLabel.ForTree("t/cachedtenant/orders");
        var second = LatticeTenantLabel.ForTree("t/cachedtenant/invoices");

        Assert.That(second.Value, Is.SameAs(first.Value));
    }

    [Test]
    public void ForTree_span_overload_agrees_with_the_string_overload()
    {
        const string treeId = "t/acme/orders";

        Assert.That(
            LatticeTenantLabel.ForTree(treeId.AsSpan()).Value,
            Is.EqualTo(LatticeTenantLabel.ForTree(treeId).Value));
    }

    [Test]
    public void ForTenant_returns_the_tenants_tag()
    {
        var tenant = TenantId.Parse("acme");

        Assert.That(LatticeTenantLabel.ForTenant(tenant).Value, Is.EqualTo("acme"));
    }

    [Test]
    public void ForTenant_returns_the_platform_sentinel_for_the_uninitialised_tenant()
    {
        Assert.That(LatticeTenantLabel.ForTenant(default).Value, Is.EqualTo(LatticeTenantLabel.PlatformTenant));
    }

    [Test]
    public void ForTenant_agrees_with_ForTree_for_the_same_tenant()
    {
        var viaTenant = LatticeTenantLabel.ForTenant(TenantId.Parse("globex"));
        var viaTree = LatticeTenantLabel.ForTree(LatticeTenantTrees.Compose(TenantId.Parse("globex"), "orders"));

        Assert.That(viaTenant.Value, Is.SameAs(viaTree.Value));
    }

    [Test]
    public void ForTenant_returns_the_default_tag_for_the_default_tenant()
    {
        Assert.That(LatticeTenantLabel.ForTenant(TenantId.Default).Value, Is.EqualTo(LatticeTenantLabel.DefaultTenant));
    }

    [Test]
    public void PlatformMeasurement_carries_the_platform_sentinel()
    {
        var measurement = LatticeTenantLabel.PlatformMeasurement(7L);

        Assert.Multiple(() =>
        {
            Assert.That(measurement.Value, Is.EqualTo(7L));
            Assert.That(measurement.Tags.Length, Is.EqualTo(1));
            Assert.That(measurement.Tags[0].Key, Is.EqualTo(LatticeTenantLabel.TagTenant));
            Assert.That(measurement.Tags[0].Value, Is.EqualTo(LatticeTenantLabel.PlatformTenant));
        });
    }

    [Test]
    public void PlatformMeasurement_preserves_a_double_value()
    {
        Measurement<double> measurement = LatticeTenantLabel.PlatformMeasurement(1.5d);

        Assert.That(measurement.Value, Is.EqualTo(1.5d));
    }

    [Test]
    public void Platform_and_Default_tags_use_the_shared_dimension_key()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeTenantLabel.Platform.Key, Is.EqualTo(LatticeTenantLabel.TagTenant));
            Assert.That(LatticeTenantLabel.Default.Key, Is.EqualTo(LatticeTenantLabel.TagTenant));
        });
    }
}
