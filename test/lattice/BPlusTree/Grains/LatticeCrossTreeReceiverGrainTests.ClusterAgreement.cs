using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Receiver-side coverage for the cross-tree cluster-id agreement check. The
/// barrier carries one global verdict across every tree in its wait set, which
/// is the same tree-boundary transitivity step the authoring coordinator makes,
/// so it rests on the same premise and is enforced at the wait-set freeze.
/// </summary>
public partial class LatticeCrossTreeReceiverGrainTests
{
    [Test]
    public void NotifyTerminalAsync_rejects_a_wait_set_whose_trees_resolve_different_cluster_ids()
    {
        var (grain, state) = CreateGrain(clusterIds: new Dictionary<string, string>
        {
            ["orders"] = "cluster-a",
            ["inventory"] = "cluster-b",
        });

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.NotifyTerminalAsync(Terminal("orders", true, ["orders", "inventory"])));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("cluster-a").And.Contain("cluster-b"));
            Assert.That(state.State.WaitSet, Is.Empty,
                "The check runs at the freeze, before the wait set or identity is recorded.");
            Assert.That(state.State.Arrived, Is.Empty);
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task NotifyTerminalAsync_admits_a_wait_set_whose_trees_agree()
    {
        var (grain, state) = CreateGrain(clusterIds: new Dictionary<string, string>
        {
            ["orders"] = "cluster-a",
            ["inventory"] = "cluster-a",
        });

        var decision = await grain.NotifyTerminalAsync(
            Terminal("orders", true, ["orders", "inventory"]));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Decided, Is.False, "the barrier still awaits the second tree");
            Assert.That(state.State.WaitSet, Is.EqualTo(new[] { "inventory", "orders" }));
        });
    }

    [Test]
    public async Task NotifyTerminalAsync_admits_a_single_cluster_host_where_every_tree_resolves_empty()
    {
        var (grain, state) = CreateGrain();

        var decision = await grain.NotifyTerminalAsync(
            Terminal("orders", true, ["orders", "inventory"]));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Decided, Is.False);
            Assert.That(state.State.WaitSet, Is.EqualTo(new[] { "inventory", "orders" }));
        });
    }

    [Test]
    public async Task NotifyTerminalAsync_does_not_re_check_cluster_agreement_after_the_freeze()
    {
        // Only the freeze checks. A later terminal on an already-frozen barrier
        // must be able to complete it: refusing there would strand a barrier
        // whose participants have already had their writes staged, and the
        // frozen-wait-set check below it already rejects a changed participant
        // set on its own reasoning.
        var (grain, state) = CreateGrain();
        await grain.NotifyTerminalAsync(Terminal("orders", true, ["orders", "inventory"]));

        var (drifted, _) = CreateGrain(
            existingState: state,
            clusterIds: new Dictionary<string, string>
            {
                ["orders"] = "cluster-a",
                ["inventory"] = "cluster-b",
            });

        var decision = await drifted.NotifyTerminalAsync(
            Terminal("inventory", true, ["orders", "inventory"]));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Decided, Is.True);
            Assert.That(decision.Committed, Is.True);
        });
    }

    [Test]
    public async Task NotifyTerminalAsync_admits_a_single_tree_wait_set_regardless_of_cluster_id()
    {
        // A one-tree wait set carries no verdict across any tree boundary, so
        // the premise is vacuously satisfied and there is nothing to compare.
        var (grain, state) = CreateGrain(clusterIds: new Dictionary<string, string>
        {
            ["orders"] = "cluster-a",
        });

        var decision = await grain.NotifyTerminalAsync(Terminal("orders", true, ["orders"]));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Decided, Is.True);
            Assert.That(state.State.WaitSet, Is.EqualTo(new[] { "orders" }));
        });
    }
}
