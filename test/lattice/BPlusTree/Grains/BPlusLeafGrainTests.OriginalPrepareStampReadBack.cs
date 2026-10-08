using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522 (PR2a): the leaf half of the coordinator's read-back of each key's
/// original prepare stamp. Every prepared key is reported, so the coordinator can
/// prove it accounted for every entry; only a marked last-writer-wins prepare
/// carries a stamp - an unmarked prepare's stamp is not the original P, and a
/// CRDT-delta prepare folds at the terminal stamp - so a reported stamp is always
/// one the leaf's own drain would apply the value at.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task The_read_back_reports_a_marked_prepare_at_its_bucketed_stamp()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        var prepared = await PendingSnapshotAsync(grain, tx, "k");

        var stamps = await grain.GetOriginalPrepareStampsAsync(tx);

        Assert.That(stamps, Is.Not.Null);
        Assert.That(stamps!, Has.Count.EqualTo(1));
        Assert.That(stamps!["k"], Is.EqualTo((HybridLogicalClock?)prepared.Timestamp));
    }

    [Test]
    public async Task The_read_back_reports_an_unmarked_prepare_without_a_stamp()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "marked", "saga", OwnRoute);
        await PrepareStampedAsync(grain, tx, "unmarked", "saga", route: $"{StampTreeId}/7");

        var stamps = await grain.GetOriginalPrepareStampsAsync(tx);

        Assert.That(stamps!.Keys, Is.EquivalentTo(new[] { "marked", "unmarked" }), "every prepared key is accounted for");
        Assert.That(stamps["marked"], Is.Not.Null);
        Assert.That(stamps["unmarked"], Is.Null,
            "an unmarked prepare's stamp may be this leaf's own clock, not the original P");
    }

    [Test]
    public async Task The_read_back_reports_a_crdt_delta_prepare_without_a_stamp()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = StampTreeId;
        state.State.ShardIndex = StampShardIndex;
        var grain = CreateGrain(state, mergeModeResolver: new FixedMergeModeResolver(LatticeMergeMode.OrSet));
        var tx = Guid.NewGuid();
        LatticeTransactionContext.Set(tx);
        try
        {
            using (LatticeDeltaContext.With(SingleAddOrSetDelta("apple")))
            using (LatticePreparedContext.BeginScope())
            {
                LatticeOriginalPrepareStampContext.StampPreparedRoute(OwnRoute);
                try
                {
                    await grain.SetAsync("k", Utf8("staged"));
                }
                finally
                {
                    RequestContext.Remove(LatticeEventConstants.PreparedRouteRequestContextKey);
                }
            }
        }
        finally
        {
            LatticeTransactionContext.Set(Guid.Empty);
        }

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1), "PRECONDITION: the delta prepare is bucketed");
        var stamps = await grain.GetOriginalPrepareStampsAsync(tx);
        Assert.That(stamps!.Keys, Is.EquivalentTo(new[] { "k" }), "the delta prepare is accounted for");
        Assert.That(stamps["k"], Is.Null, "a CRDT delta folds at the terminal stamp, so it has no stamp to carry");
    }

    [Test]
    public async Task The_read_back_is_null_for_a_saga_with_no_bucket_here()
    {
        var (grain, _) = CreateStampLeaf();
        await PrepareStampedAsync(grain, Guid.NewGuid(), "k", "saga", OwnRoute);

        Assert.That(await grain.GetOriginalPrepareStampsAsync(Guid.NewGuid()), Is.Null);
    }

    [Test]
    public async Task The_read_back_is_null_once_the_terminal_has_drained_the_bucket()
    {
        var (grain, _) = CreateStampLeaf();
        var tx = Guid.NewGuid();
        await PrepareStampedAsync(grain, tx, "k", "saga", OwnRoute);
        await grain.ApplyTxTerminalAsync(tx, committed: true);

        Assert.That(await grain.GetOriginalPrepareStampsAsync(tx), Is.Null);
    }
}
