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
/// across live materialiser consumers. A fold that released the donor's pins,
/// deleted its leaf state, or retired its shard record would therefore let the
/// GC trim a prefix the survivor may not yet have absorbed - and a leaf that
/// later needs to replay over a trimmed prefix is real data loss, not a slow
/// start.
/// </para>
/// <para>
/// Consolidation's answer is structural rather than best-effort: it retires the
/// donor from the <em>routing map</em> and nothing else. Its leaves, their
/// projection checkpoints and their pins all survive, so the trim horizon can
/// only stay where it is or move backwards-safe, and no prefix becomes
/// trimmable that was not trimmable before. These tests pin that structurally,
/// by asserting the fold never invokes any of the operations that could
/// release durability, across a complete end-to-end run.
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
    public async Task A_fold_retires_the_donor_by_routing_only()
    {
        // The complete set of donor-side mutations a fold performs: open the
        // shadow window, seal the leaves, freeze, and record the permanent
        // retirement. Nothing here touches storage lifetime or durability.
        var h = await RunCompleteFoldAsync();

        await h.Donor.Received().BeginSplitAsync(0, Arg.Any<int[]>(), VirtualShardCount);
        await h.Donor.Received().MarkLeavesMovedAwayAsync(Arg.Any<int[]>(), VirtualShardCount);
        await h.Donor.Received().EnterRejectPhaseAsync();
        await h.Donor.Received().CompleteSplitAsync();

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
        var flipIndex = h.Log.IndexOf("registry.SetShardMap");

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
