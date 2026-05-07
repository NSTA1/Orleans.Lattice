using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the routing semantics of <see cref="RoutingTableSnapshot.Route"/>
/// against the same contract as <c>InternalNodeState.Route</c>: the two
/// implementations must stay in lockstep, otherwise a cache hit and a
/// cross-grain fallback would route the same key to different children.
///
/// These are pure value-type tests - no grain framework, no async. They
/// exist as the safety-net for the cycle-14 routing-table cache: a
/// regression here would silently route reads to the wrong leaf on a
/// cache hit while the deeper integration tests (which traverse via the
/// cache) might still pass if their key distribution avoids the broken
/// boundary.
/// </summary>
[TestFixture]
public class RoutingTableSnapshotTests
{
    private static readonly GrainId Child0 = GrainId.Create("leaf", "child-0");
    private static readonly GrainId Child1 = GrainId.Create("leaf", "child-1");
    private static readonly GrainId Child2 = GrainId.Create("leaf", "child-2");
    private static readonly GrainId Child3 = GrainId.Create("leaf", "child-3");

    private static RoutingTableSnapshot CreateSnapshot(
        string?[] separators,
        GrainId[] children,
        bool childrenAreLeaves = true) => new()
        {
            SeparatorKeys = separators,
            ChildIds = children,
            ChildrenAreLeaves = childrenAreLeaves,
        };

    [Test]
    public void Route_returns_leftmost_child_when_key_below_first_separator()
    {
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1]);

        var (childId, _) = snapshot.Route("ant");

        Assert.That(childId, Is.EqualTo(Child0));
    }

    [Test]
    public void Route_returns_right_child_for_exact_separator_match()
    {
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1]);

        var (childId, _) = snapshot.Route("fox");

        Assert.That(childId, Is.EqualTo(Child1));
    }

    [Test]
    public void Route_returns_right_child_for_key_above_separator()
    {
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1]);

        var (childId, _) = snapshot.Route("zebra");

        Assert.That(childId, Is.EqualTo(Child1));
    }

    [Test]
    public void Route_with_multiple_separators_picks_correct_child()
    {
        var snapshot = CreateSnapshot(
            separators: [null, "fox", "monkey", "rabbit"],
            children: [Child0, Child1, Child2, Child3]);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Route("ant").ChildId, Is.EqualTo(Child0));
            Assert.That(snapshot.Route("fox").ChildId, Is.EqualTo(Child1));
            Assert.That(snapshot.Route("lion").ChildId, Is.EqualTo(Child1));
            Assert.That(snapshot.Route("monkey").ChildId, Is.EqualTo(Child2));
            Assert.That(snapshot.Route("penguin").ChildId, Is.EqualTo(Child2));
            Assert.That(snapshot.Route("rabbit").ChildId, Is.EqualTo(Child3));
            Assert.That(snapshot.Route("zebra").ChildId, Is.EqualTo(Child3));
        });
    }

    [Test]
    public void Route_returns_only_child_when_single_entry()
    {
        var snapshot = CreateSnapshot(
            separators: [null],
            children: [Child0]);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Route("").ChildId, Is.EqualTo(Child0));
            Assert.That(snapshot.Route("anything").ChildId, Is.EqualTo(Child0));
            Assert.That(snapshot.Route("\uFFFF").ChildId, Is.EqualTo(Child0));
        });
    }

    [Test]
    public void Route_propagates_children_are_leaves_flag_true()
    {
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1],
            childrenAreLeaves: true);

        var (_, childrenAreLeaves) = snapshot.Route("ant");

        Assert.That(childrenAreLeaves, Is.True);
    }

    [Test]
    public void Route_propagates_children_are_leaves_flag_false()
    {
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1],
            childrenAreLeaves: false);

        var (_, childrenAreLeaves) = snapshot.Route("zebra");

        Assert.That(childrenAreLeaves, Is.False);
    }

    [Test]
    public void Route_uses_ordinal_string_comparison_not_culture_sensitive()
    {
        // Under en-US linguistic comparison "Z" can sort after "a"; under
        // ordinal comparison Z (0x5A) sorts before a (0x61). Both
        // RoutingTableSnapshot.Route and the server-side
        // InternalNodeState.Route use ordinal comparison; this test pins
        // that contract on the snapshot side so a refactor to a culture-
        // sensitive comparer would be caught here.
        var snapshot = CreateSnapshot(
            separators: [null, "a"],
            children: [Child0, Child1]);

        // "Z" < "a" ordinally -> route to the leftmost catch-all.
        var (childId, _) = snapshot.Route("Z");

        Assert.That(childId, Is.EqualTo(Child0));
    }

    [Test]
    public void Route_with_empty_string_key_routes_to_leftmost()
    {
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1]);

        var (childId, _) = snapshot.Route("");

        Assert.That(childId, Is.EqualTo(Child0));
    }

    [Test]
    public void Route_at_separator_prefix_routes_to_left_child()
    {
        // "fo" < "fox" ordinally; a strict prefix of the separator routes
        // to the left child. Boundary case.
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1]);

        var (childId, _) = snapshot.Route("fo");

        Assert.That(childId, Is.EqualTo(Child0));
    }

    [Test]
    public void Route_at_separator_extension_routes_to_right_child()
    {
        // "foxa" > "fox" ordinally; a key that extends the separator routes
        // to the right child.
        var snapshot = CreateSnapshot(
            separators: [null, "fox"],
            children: [Child0, Child1]);

        var (childId, _) = snapshot.Route("foxa");

        Assert.That(childId, Is.EqualTo(Child1));
    }
}