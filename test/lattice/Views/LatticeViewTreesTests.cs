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
        // A view name is validated only for null/empty today, so a caller can
        // create one literally named "t/globex/orders". Composing tenant-first
        // would yield "t/globex/view-orders" and plant the tree in another
        // tenant's namespace, so tenant-aware composition must land together with
        // view-name validation that reserves the "t/" prefix - never before it.
        Assert.That(
            LatticeViewTrees.ComposeTreeId("t/globex/orders"),
            Is.EqualTo("view-t/globex/orders"));
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
        => Assert.That(LatticeViewTrees.ComposeTreeId("orders", 2), Is.EqualTo("view-orders#g2"));

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
            Assert.That(LatticeViewTrees.IsViewTree("view-orders#g3"), Is.True);
            Assert.That(LatticeViewTrees.IsViewTree("t/acme/view-orders#g3"), Is.True);
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
    [TestCase("view-orders#g4", "orders")]
    [TestCase("view-t/globex/orders", "t/globex/orders")]
    [TestCase("t/acme/view-orders", "t/acme/orders")]
    [TestCase("t/acme/view-orders#g4", "t/acme/orders")]
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
                Assert.That(LatticeViewTrees.ComposeTreeId(name, 7), Is.EqualTo($"view-{name}#g7"));
            });
        }
    }
}
