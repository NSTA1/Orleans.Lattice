using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the cross-tree cluster-id agreement check.
/// <para>
/// The cross-tree protocol repeatedly carries a guard verdict reached on one
/// participating tree across the tree boundary to another, and that step is
/// licensed only by the two trees resolving the same cluster id.
/// <c>LatticeReplicationOptions.ClusterId</c> is a per-tree option, so nothing
/// in the type system makes the agreement hold; the options validator asserts
/// it in prose and is structurally unable to check it, because a relation
/// between two trees' configurations is not observable from one options
/// instance. These tests pin the enforcement at the two admission points where
/// a participant set first exists.
/// </para>
/// </summary>
public partial class LatticeCrossTreeTxGrainTests
{
    [Test]
    public void CommitAsync_rejects_participants_that_resolve_different_cluster_ids()
    {
        var (grain, state, _, _) = CreateGrain(
            ["orders", "inventory"],
            clusterIds: new Dictionary<string, string>
            {
                ["orders"] = "cluster-a",
                ["inventory"] = "cluster-b",
            });

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.CommitAsync(Batches(("orders", "k1", "v1"), ("inventory", "k2", "v2"))));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("cluster-a").And.Contain("cluster-b"));
            Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted),
                "The check runs before anything is staged or persisted.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public async Task CommitAsync_admits_participants_that_agree_on_a_non_empty_cluster_id()
    {
        var (grain, state, _, _) = CreateGrain(
            ["orders", "inventory"],
            clusterIds: new Dictionary<string, string>
            {
                ["orders"] = "cluster-a",
                ["inventory"] = "cluster-a",
            });

        var outcome = await grain.CommitAsync(Batches(("orders", "k1", "v1"), ("inventory", "k2", "v2")));

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        });
    }

    [Test]
    public async Task CommitAsync_admits_a_single_cluster_host_where_every_tree_resolves_empty()
    {
        // The core default resolver returns string.Empty for every tree, so the
        // agreement check must read that as "one cluster identity", not as a
        // missing value. A host with no replication configured genuinely has one
        // cluster, and every argument the premise licenses is sound there.
        var (grain, state, _, _) = CreateGrain(["orders", "inventory"]);

        var outcome = await grain.CommitAsync(Batches(("orders", "k1", "v1"), ("inventory", "k2", "v2")));

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
        });
    }

    [Test]
    public async Task CommitAsync_does_not_re_check_cluster_agreement_on_a_resumed_saga()
    {
        // A saga past admission has staged writes parked in hidden buckets on
        // every participant. Re-checking there would let a configuration edit
        // made after prepare strand a saga that must still be driven to a
        // terminal decision, which is worse than the drift it would report.
        var (first, state, _, _) = CreateGrain(["orders", "inventory"]);
        var batches = Batches(("orders", "k1", "v1"), ("inventory", "k2", "v2"));
        state.State.OperationId = OperationId;
        state.State.Participants = [];
        state.State.Phase = CrossTreeTxPhase.Preparing;
        _ = first;

        var (grain, _, _, _) = CreateGrain(
            ["orders", "inventory"],
            existingState: state,
            clusterIds: new Dictionary<string, string>
            {
                ["orders"] = "cluster-a",
                ["inventory"] = "cluster-b",
            });

        // Reaches the coordinator loop rather than throwing the agreement fault.
        Assert.DoesNotThrowAsync(() => grain.CommitAsync(batches));
        await Task.CompletedTask;
    }

    [Test]
    public async Task CommitAsync_ignores_cluster_disagreement_on_trees_whose_slice_is_empty()
    {
        // BuildParticipants drops an empty per-tree slice, so such a tree is not
        // a participant and no verdict is ever carried to or from it. Checking
        // the caller's raw batch list instead of the participant set would fail
        // a write that is provably unaffected.
        var (grain, state, _, _) = CreateGrain(
            ["orders"],
            clusterIds: new Dictionary<string, string>
            {
                ["orders"] = "cluster-a",
                ["archive"] = "cluster-b",
            });

        var batches = Batches(("orders", "k1", "v1"));
        batches.Add(new LatticeTreeBatch("archive", []));

        var outcome = await grain.CommitAsync(batches);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
            Assert.That(state.State.Participants.Select(p => p.TreeId), Is.EqualTo(new[] { "orders" }));
        });
    }
}
