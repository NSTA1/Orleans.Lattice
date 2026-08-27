using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="LatticeViewTrees"/>, the single place that composes,
/// classifies, and parses materialised-view tree ids. The behaviour that matters
/// is that classification survives tenant composition: scoping a view name into
/// <c>t/{tenant}/</c> moves the <c>view-</c> marker off the front of the id, and
/// every guard that tests the raw leading prefix would silently stop firing.
/// </summary>
[TestFixture]
public sealed class LatticeViewTreesTests
{
    // ----- Composition -----

    [Test]
    public void ComposeTreeId_prefixes_a_bare_view_name()
        => Assert.That(LatticeViewTrees.ComposeTreeId("orders"), Is.EqualTo("view-orders"));

    [Test]
    public void ComposeTreeId_does_not_lift_a_tenant_segment_out_of_the_name()
    {
        // A caller-supplied name can never carry a tenant segment, because
        // ViewNameValidator rejects '/' at every creation seam. A segment present
        // here can only have been added by tenant scoping, so it is lifted outside
        // the view prefix and the tree lands in that tenant's own namespace.
        Assert.That(
            LatticeViewTrees.ComposeTreeId("t/globex/orders"),
            Is.EqualTo("t/globex/view-orders"));
    }

    [Test]
    public void Two_tenants_using_one_name_reach_different_trees()
    {
        // The isolation property the tenant scoping exists for.
        var acme = LatticeViewTrees.ComposeTreeId("t/acme/orders");
        var globex = LatticeViewTrees.ComposeTreeId("t/globex/orders");

        Assert.Multiple(() =>
        {
            Assert.That(acme, Is.EqualTo("t/acme/view-orders"));
            Assert.That(globex, Is.EqualTo("t/globex/view-orders"));
            Assert.That(acme, Is.Not.EqualTo(globex));
            Assert.That(LatticeViewTrees.ViewNameFromTreeId(acme), Is.EqualTo("t/acme/orders"));
            Assert.That(LatticeViewTrees.ViewNameFromTreeId(globex), Is.EqualTo("t/globex/orders"));
        });
    }

    [Test]
    public void A_tenant_view_tree_is_owned_by_its_tenant()
    {
        // Placing the tenant segment outermost is what makes ownership,
        // enumeration filtering, and the tenant delete cascade work on a view tree
        // exactly as they do on any other tree.
        var owner = LatticeTenantTrees.GetOwner(LatticeViewTrees.ComposeTreeId("t/acme/orders"));

        Assert.Multiple(() =>
        {
            Assert.That(owner.IsTenantOwned, Is.True);
            Assert.That(owner.Tenant, Is.EqualTo(TenantId.Parse("acme")));
        });
    }

    [Test]
    public void ComposeTreeId_generation_zero_is_the_stable_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeViewTrees.ComposeTreeId("orders", 0), Is.EqualTo("view-orders"));
            Assert.That(LatticeViewTrees.ComposeTreeId("orders", -1), Is.EqualTo("view-orders"));
        });
    }

    [Test]
    public void ComposeTreeId_suffixes_a_higher_generation()
        => Assert.That(LatticeViewTrees.ComposeTreeId("orders", 2), Is.EqualTo("view-orders~g2"));

    [Test]
    public void ComposeTreeId_addresses_a_pre_existing_generation_through_the_legacy_separator()
        => Assert.That(
            LatticeViewTrees.ComposeTreeId("orders", 2, useLegacySeparator: true),
            Is.EqualTo("view-orders#g2"),
            "a view already past generation 0 must keep resolving its existing tree, "
            + "or adopting the storage-safe separator would orphan its data");

    [Test]
    public void The_generation_suffix_is_storage_safe()
    {
        // The composed id is an Orleans grain primary key and is carried into
        // ShardRootGrain's composite key, a persistent grain. Keyed storage
        // backends reject these characters there.
        var id = LatticeViewTrees.ComposeTreeId("orders", 9);

        Assert.That(id.IndexOfAny(['/', '\\', '#', '?']), Is.LessThan(0));
        Assert.That(id.Any(char.IsControl), Is.False);
    }

    [Test]
    public void A_generation_is_addressed_consistently_by_a_single_ceiling()
    {
        // Pins the invariant behind the self-healing migration: whether a
        // generation resolves through the legacy separator is decided purely by
        // the ceiling, so a generation written under one naming can never be read
        // back under the other. Pinning the ceiling after a generation had already
        // been allocated broke exactly this, sending reads to a tree that was
        // never written.
        const long ceiling = 1L;

        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeViewTrees.ComposeTreeId("orders", 1, useLegacySeparator: 1 <= ceiling),
                Is.EqualTo("view-orders#g1"),
                "a generation at or below the ceiling predates the change");
            Assert.That(
                LatticeViewTrees.ComposeTreeId("orders", 2, useLegacySeparator: 2 <= ceiling),
                Is.EqualTo("view-orders~g2"),
                "the next generation moves onto the storage-safe separator");
        });
    }

    [Test]
    public void Both_separators_resolve_to_the_same_view_name()
        => Assert.That(
            LatticeViewTrees.ViewNameFromTreeId("view-orders#g1"),
            Is.EqualTo(LatticeViewTrees.ViewNameFromTreeId("view-orders~g1")),
            "a legacy id must still resolve to its maintainer during the transition");

    [Test]
    public void ComposeTreeId_rejects_an_empty_name()
        => Assert.That(() => LatticeViewTrees.ComposeTreeId(""), Throws.ArgumentException);

    // ----- Classification survives composition -----

    [Test]
    public void IsViewTree_classifies_a_bare_view_id()
        => Assert.That(LatticeViewTrees.IsViewTree("view-orders"), Is.True);

    [Test]
    public void IsViewTree_classifies_a_tenant_composed_view_id()
        => Assert.That(
            LatticeViewTrees.IsViewTree("t/acme/view-orders"),
            Is.True,
            "this is the regression: a raw StartsWith(\"view-\") returns false here, "
            + "silently retiring every view guard for a tenant's own view");

    [Test]
    public void IsViewTree_classifies_a_generation_suffixed_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeViewTrees.IsViewTree("view-orders~g3"), Is.True);
            Assert.That(LatticeViewTrees.IsViewTree("t/acme/view-orders~g3"), Is.True);
        });
    }

    [TestCase("orders")]
    [TestCase("t/acme/orders")]
    [TestCase("sys-auth-policy")]
    [TestCase("_lattice_registry")]
    [TestCase("tag-bytag")]
    [TestCase("previewer")]
    public void IsViewTree_is_false_for_a_non_view_id(string treeId)
        => Assert.That(LatticeViewTrees.IsViewTree(treeId), Is.False);

    [Test]
    public void IsViewTree_does_not_match_an_embedded_prefix()
        => Assert.That(
            LatticeViewTrees.IsViewTree("t/acme/orders/view-x"),
            Is.False,
            "only the leading marker on the tenant-local name is the reserved prefix");

    [Test]
    public void IsViewTree_null_throws_argument_null()
        => Assert.That(() => LatticeViewTrees.IsViewTree(null!), Throws.ArgumentNullException);

    // ----- Parsing is the exact inverse of composition -----

    [TestCase("view-orders", "orders")]
    [TestCase("view-orders~g4", "orders")]
    [TestCase("view-t/globex/orders", "t/globex/orders")]
    [TestCase("t/acme/view-orders", "t/acme/orders")]
    [TestCase("t/acme/view-orders~g4", "t/acme/orders")]
    public void ViewNameFromTreeId_recovers_the_maintainer_key(string treeId, string expected)
        => Assert.That(LatticeViewTrees.ViewNameFromTreeId(treeId), Is.EqualTo(expected));

    [Test]
    public void ViewNameFromTreeId_round_trips_composition()
    {
        foreach (var name in new[] { "orders", "a_b-c", "t/globex/orders" })
        {
            Assert.That(
                LatticeViewTrees.ViewNameFromTreeId(LatticeViewTrees.ComposeTreeId(name)),
                Is.EqualTo(name));
        }
    }

    [Test]
    public void A_tenant_composed_id_resolves_to_its_own_maintainer_key()
    {
        // Nothing produces these ids yet, but classification and parsing already
        // handle them, which is what lets the tenancy change be a small step.
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeViewTrees.ViewNameFromTreeId("t/acme/view-orders"),
                Is.EqualTo("t/acme/orders"));
            Assert.That(
                LatticeViewTrees.ViewNameFromTreeId("t/globex/view-orders"),
                Is.EqualTo("t/globex/orders"));
        });
    }

    [TestCase("orders")]
    [TestCase("view-")]
    [TestCase("t/acme/view-")]
    public void ViewNameFromTreeId_is_empty_when_no_name_is_recoverable(string treeId)
        => Assert.That(LatticeViewTrees.ViewNameFromTreeId(treeId), Is.Empty);

    // ----- Tenancy off is byte-for-byte unchanged -----

    [Test]
    public void A_bare_name_composes_exactly_as_the_legacy_interpolation_did()
    {
        // With tenancy off no name is ever tenant-qualified, so every id this
        // helper produces is identical to the string the maintainer previously
        // interpolated. This is what makes the change inert on a single-tenant
        // cluster.
        foreach (var name in new[] { "orders", "audit", "a-b-c" })
        {
            Assert.Multiple(() =>
            {
                Assert.That(LatticeViewTrees.ComposeTreeId(name), Is.EqualTo($"view-{name}"));
                Assert.That(LatticeViewTrees.ComposeTreeId(name, 7), Is.EqualTo($"view-{name}~g7"));
            });
        }
    }
}
