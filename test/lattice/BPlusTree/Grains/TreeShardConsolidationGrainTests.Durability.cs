using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Durability-coherence guards for online shard consolidation - the
/// highest-severity invariant of the whole operation.
/// <para>
/// The WAL GC trims a prefix only up to the minimum durable checkpoint pin
/// across live materialiser consumers. A fold that released the donor's pins or
/// deleted its leaf state before the survivor had durably absorbed its data
/// would let the GC trim a prefix nothing else covers - and a leaf that later
/// needs to replay over a trimmed prefix is real data loss, not a slow start.
/// </para>
/// <para>
/// A fold that <em>never</em> released them is the opposite failure: a retired
/// donor's leaves keep one pin each at a frontier that can never advance, so the
/// tree's trim horizon is held back for good. The contract is therefore an
/// ordering, not an abstention. The donor's storage is released only through
/// <see cref="IShardRootGrain.RetireAsync"/>, only after every sweep - including
/// the authoritative ones over the frozen donor - has merged into the survivor
/// (whose merge path appends to its own write-ahead log before returning), only
/// after the donor's moved-away fence is permanent, and only when the live map
/// no longer routes any slot to the donor. The donor is never purged, deleted,
/// or force-deactivated through any other path.
/// </para>
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    private static async Task<Harness> RunCompleteFoldAsync()
    {
        var h = CreateGrain(leafEntries: [Entries("a", "b"), Entries("c")]);
        await h.Grain.StartAsync(0);
        await h.Grain.RunConsolidationPassAsync();

        Assert.That(h.State.State.Complete, Is.True, "Precondition: the fold must have landed.");
        return h;
    }

    [Test]
    public async Task A_fold_never_purges_the_donor_shard_state()
    {
        var h = await RunCompleteFoldAsync();

        await h.Donor.DidNotReceive().PurgeAsync();
        await h.Donor.DidNotReceive().MarkDeletedAsync();
    }

    [Test]
    public async Task A_fold_never_force_deactivates_the_donor()
    {
        // Deactivating the donor would drop its leaf activations and, with
        // them, the pins those leaves hold while active.
        var h = await RunCompleteFoldAsync();

        await h.Donor.DidNotReceive().ForceDeactivateAsync();
    }

    [Test]
    public async Task A_fold_never_rebuilds_or_disturbs_the_donor_projection()
    {
        // A projection rebuild resets the donor's durable checkpoint, which is
        // exactly the value its pin is derived from.
        var h = await RunCompleteFoldAsync();

        await h.Donor.DidNotReceive().RebuildShardProjectionAsync(Arg.Any<CancellationToken>());
        await h.Survivor.DidNotReceive().RebuildShardProjectionAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_fold_releases_the_donor_storage_only_through_RetireAsync()
    {
        // The complete set of donor-side mutations a fold performs: open the
        // shadow window, seal the leaves, freeze, record the permanent
        // retirement, and release the retired donor's storage through the one
        // verb that keeps its routing fence.
        var h = await RunCompleteFoldAsync();

        await h.Donor.Received().BeginSplitAsync(0, Arg.Any<int[]>(), VirtualShardCount);
        await h.Donor.Received().MarkLeavesMovedAwayAsync(Arg.Any<int[]>(), VirtualShardCount);
        await h.Donor.Received().EnterRejectPhaseAsync();
        await h.Donor.Received().CompleteSplitAsync();
        await h.Donor.Received(1).RetireAsync();

        await h.Donor.DidNotReceive().PurgeAsync();
        await h.Donor.DidNotReceive().MarkDeletedAsync();
        await h.Donor.DidNotReceive().ForceDeactivateAsync();
        await h.Donor.DidNotReceive().BulkLoadAsync(Arg.Any<string>(), Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [Test]
    public async Task A_fold_only_ever_adds_data_to_the_survivor()
    {
        // The survivor gains the donor's entries and lifts its own seal. It is
        // never asked to delete, purge, or reload, so its own durable
        // checkpoint only ever moves forward with data it has absorbed.
        var h = await RunCompleteFoldAsync();

        await h.Survivor.Received().MergeManyAsync(
            Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), isCrossShardMigration: true);
        await h.Survivor.Received().ReclaimSlotsAsync(Arg.Any<int[]>(), VirtualShardCount);

        await h.Survivor.DidNotReceive().PurgeAsync();
        await h.Survivor.DidNotReceive().MarkDeletedAsync();
        await h.Survivor.DidNotReceive().DeleteAsync(Arg.Any<string>());
        await h.Survivor.DidNotReceive().DeleteRangeAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>());
    }

    [Test]
    public async Task An_abandoned_fold_leaves_both_shards_durably_untouched()
    {
        var h = CreateGrain(
            existingState: InFlightState(ShardConsolidationPhase.Drain),
            leafEntries: [Entries("a")]);

        await h.Grain.CancelAsync();
        await h.Grain.RunConsolidationPassAsync();

        await h.Donor.DidNotReceive().PurgeAsync();
        await h.Donor.DidNotReceive().MarkDeletedAsync();
        await h.Donor.DidNotReceive().CompleteSplitAsync();
        await h.Survivor.DidNotReceive().PurgeAsync();
        Assert.That(h.PersistedMap!.GetPhysicalShardIndices(), Has.Count.EqualTo(2),
            "An abandoned fold must leave the tree's physical topology exactly as it was.");
    }

    [Test]
    public async Task The_donor_storage_is_released_only_after_every_merge_and_the_permanent_fence()
    {
        // The release is the last donor-side step: after the final sweep has
        // merged into the survivor, and after the moved-away fence that
        // redirects callers holding an older map has been made permanent.
        var h = await RunCompleteFoldAsync();

        var lastMerge = h.Log.Entries.LastIndexOf("survivor.MergeMany");
        var fence = h.Log.IndexOf("donor.CompleteSplit");
        var release = h.Log.IndexOf("donor.Retire");

        Assert.That(lastMerge, Is.GreaterThanOrEqualTo(0));
        Assert.That(fence, Is.GreaterThan(lastMerge));
        Assert.That(release, Is.GreaterThan(fence),
            "A donor's storage must never be released ahead of its data being absorbed and its fence being permanent.");
    }

    [TestCase("snapshot")]
    [TestCase("merge")]
    public async Task Finalise_waits_out_a_snapshot_or_merge_before_releasing_the_donor(string running)
    {
        // A snapshot or merge may still read the donor's leaves through the
        // shard list it recorded when it started.
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Complete));
        h.PersistedMap = new ShardMap { Slots = new int[VirtualShardCount] };
        var lattice = h.Factory.GetGrain<ILattice>(TreeId);
        if (running == "snapshot") lattice.IsSnapshotCompleteAsync().Returns(false);
        else lattice.IsMergeCompleteAsync().Returns(false);

        var finished = await h.Grain.FinaliseAsync();

        Assert.That(finished, Is.False);
        Assert.That(h.State.State.InProgress, Is.True);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Complete));
        await h.Donor.DidNotReceive().RetireAsync();

        lattice.IsSnapshotCompleteAsync().Returns(true);
        lattice.IsMergeCompleteAsync().Returns(true);

        Assert.That(await h.Grain.FinaliseAsync(), Is.True, "the fold completes once the maintenance has finished");
        await h.Donor.Received(1).RetireAsync();
    }

    [Test]
    public async Task Finalise_does_not_wait_out_a_resize()
    {
        // Waiting would strand the fold: once the resize completes the old
        // copy's shards reject stale-tree routing forever. The donor itself
        // refuses retirement while a resize forwards it, keeping its storage.
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Complete));
        h.PersistedMap = new ShardMap { Slots = new int[VirtualShardCount] };
        h.Factory.GetGrain<ILattice>(TreeId).IsResizeCompleteAsync().Returns(false);

        Assert.That(await h.Grain.FinaliseAsync(), Is.True);
        Assert.That(h.State.State.Complete, Is.True);
    }

    [Test]
    public async Task A_donor_the_live_map_still_routes_to_keeps_its_storage()
    {
        // Fail closed: the default two-shard map still routes the donor's odd
        // slots to it, so it may be the authoritative owner of live data.
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Complete));

        await h.Grain.FinaliseAsync();

        await h.Donor.DidNotReceive().RetireAsync();
        Assert.That(h.State.State.Complete, Is.True,
            "Keeping the storage must not hold the fold open; it completes as a routing-only retirement.");
    }

    [Test]
    public async Task A_donor_that_refuses_retirement_keeps_its_storage_and_the_fold_still_completes()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Complete));
        h.PersistedMap = new ShardMap { Slots = new int[VirtualShardCount] };
        h.Donor.RetireAsync().Returns(Task.FromException(new InvalidOperationException("resize forward")));

        await h.Grain.FinaliseAsync();

        Assert.That(h.State.State.Complete, Is.True);
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public void A_transient_retirement_failure_leaves_the_fold_in_Complete_for_the_next_pass()
    {
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Complete));
        h.PersistedMap = new ShardMap { Slots = new int[VirtualShardCount] };
        h.Donor.RetireAsync().Returns(Task.FromException(new TimeoutException()));

        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.FinaliseAsync());
        Assert.That(h.State.State.InProgress, Is.True);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.Complete));
    }

    [Test]
    public async Task The_survivor_absorbs_the_donor_before_the_donor_is_retired()
    {
        // The ordering that makes the durability claim hold: every entry has
        // reached the survivor before the donor's permanent retirement record
        // is written, so nothing is ever retired ahead of being absorbed.
        var h = await RunCompleteFoldAsync();

        var lastMerge = h.Log.Entries.LastIndexOf("survivor.MergeMany");
        var retire = h.Log.IndexOf("donor.CompleteSplit");

        Assert.That(lastMerge, Is.GreaterThanOrEqualTo(0));
        Assert.That(retire, Is.GreaterThan(lastMerge),
            "A donor must never be retired ahead of the survivor having absorbed its data.");
    }

    [Test]
    public async Task The_fold_drains_once_more_after_the_freeze_and_once_more_after_the_flip()
    {
        // Three sweeps: the bounded background drain, the authoritative sweep
        // over the frozen donor inside the swap, and a final sweep in
        // finalise that catches anything written during the freeze window.
        var h = await RunCompleteFoldAsync();

        var freezeIndex = h.Log.IndexOf("donor.EnterReject");
        var flipIndex = h.Log.IndexOf("registry.ReassignSlots");

        // IndexOf returns -1 for an absent marker, and every log position is
        // greater than -1. Without these two preconditions a fold that never
        // froze the donor or never flipped the slots would count EVERY merge as
        // "after" the missing marker, so both closing assertions would hold for
        // precisely the run they exist to reject.
        Assert.That(freezeIndex, Is.GreaterThanOrEqualTo(0),
            "precondition: the fold must actually have frozen the donor, or 'after the freeze' names no point in the log");
        Assert.That(flipIndex, Is.GreaterThan(freezeIndex),
            "precondition: the slot flip must follow the freeze, or 'after the flip' names no point in the log");

        var mergesAfterFreeze = 0;
        var mergesAfterFlip = 0;
        for (var i = 0; i < h.Log.Entries.Count; i++)
        {
            if (h.Log.Entries[i] != "survivor.MergeMany") continue;
            if (i > freezeIndex) mergesAfterFreeze++;
            if (i > flipIndex) mergesAfterFlip++;
        }

        Assert.That(mergesAfterFreeze, Is.GreaterThan(0),
            "The post-freeze sweep is what makes the survivor's copy authoritative.");
        Assert.That(mergesAfterFlip, Is.GreaterThan(0),
            "The post-flip sweep captures deletes that landed during the freeze window.");
    }
}
