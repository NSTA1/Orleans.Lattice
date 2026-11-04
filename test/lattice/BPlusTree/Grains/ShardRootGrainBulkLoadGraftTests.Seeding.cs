using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5812, member B: how a bulk load seeds its leaves. Under the shard's open
/// switch the seed carries the shard root's routing generation, read after the
/// switch's marker opened (review C1), so the WAL shard admits it as the bulk-seed
/// class; a never-written shard opens no switch, stores no hint and seeds unstamped
/// (B2). A same-operation retry after an aborted switched load accepts the root that
/// replayed the aborted attempt's seeds and relocates it (review of the design).
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    private static WalRoutingStamp[] SeedRoutings(Harness h) =>
        h.Leaves.Values
            .SelectMany(l => l.ReceivedCalls())
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.MergeBulkSeedAsync))
            .Select(c => (WalRoutingStamp)c.GetArguments()[1]!)
            .ToArray();

    [Test]
    public async Task A_switched_load_seeds_under_the_shard_generation_read_after_its_marker_opened()
    {
        var rig = CreateHintRig();
        var seeded = rig.H.AddLeaf();
        var seededLeaf = rig.H.Leaf(seeded);
        rig.H.State.State.RootNodeId = seeded;
        rig.H.State.State.RootIsLeaf = true;
        seededLeaf.GetStatsAsync().Returns(Task.FromResult(new LeafStats()));
        seededLeaf.CopyRowsForBulkLoadAsync().Returns(CopiedForLoad([]));

        await rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));

        var routings = SeedRoutings(rig.H);
        Assert.Multiple(async () =>
        {
            Assert.That(routings, Has.Length.EqualTo(3), "precondition: three leaves seeded");
            Assert.That(routings, Has.All.Matches<WalRoutingStamp>(r =>
                    r.Initiator == WalRoutingInitiator.ShardRoot(0) && r.Generation > 0),
                "the seed carries the shard root's routing generation");
            Assert.That((await ShardMarkerAsync(rig)).RoutingGeneration, Is.GreaterThan(routings[0].Generation),
                "the switch's close re-claimed above it, refusing any later seed");
        });
    }

    [Test]
    public async Task A_never_written_load_opens_no_switch_and_seeds_unstamped()
    {
        var rig = CreateHintRig();

        await rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));

        var routings = SeedRoutings(rig.H);
        Assert.Multiple(async () =>
        {
            Assert.That(routings, Has.Length.EqualTo(3), "precondition: three leaves seeded");
            Assert.That(routings, Has.All.Matches<WalRoutingStamp>(r => r.ForcesUnstamped), "B2: the seed is unstamped and reported");
            var marker = await ShardMarkerAsync(rig);
            Assert.That(marker.Open, Is.Empty, "no switch token was opened");
            Assert.That(marker.BulkBirthHints, Is.Empty, "no birth hint was stored");
            await rig.Coordinator.DidNotReceive().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
            await rig.Wal.DidNotReceive().ClaimRoutingGenerationAsync(
                Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
            Assert.That(rig.H.State.State.LastCompletedBulkOperationId, Is.EqualTo("load"));
        });
    }

    [Test]
    public async Task A_same_operation_retry_after_an_aborted_switched_load_replaces_the_root_that_replayed_its_seeds()
    {
        var rig = CreateHintRig();
        var seeded = rig.H.AddLeaf();
        var seededLeaf = rig.H.Leaf(seeded);
        rig.H.State.State.RootNodeId = seeded;
        rig.H.State.State.RootIsLeaf = true;
        seededLeaf.GetStatsAsync().Returns(Task.FromResult(new LeafStats()));

        // The first attempt opens its switch and fails while seeding.
        var fail = true;
        rig.H.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var leaf = ResolveLeaf(rig.H.Leaves, call.ArgAt<Guid>(0));
            leaf.MergeBulkSeedAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<WalRoutingStamp>())
                .Returns(_ => fail ? Task.FromException(new TimeoutException("seed timed out")) : Task.CompletedTask);
            return leaf;
        });
        Assert.ThrowsAsync<TimeoutException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));
        Assert.That((await ShardMarkerAsync(rig)).Open, Contains.Key(ShardRootGrain.BulkHandOffToken("load")),
            "precondition: the aborted attempt's switch is still open");

        // Gated on that open switch, the seeded root replayed the aborted seeds.
        seededLeaf.GetStatsAsync().Returns(Task.FromResult(new LeafStats { LiveKeys = 2 }));
        var replayed = RawEntries(2);
        seededLeaf.CopyRowsForBulkLoadAsync().Returns(CopiedForLoad(replayed));
        fail = false;

        await rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));

        Assert.Multiple(() =>
        {
            Assert.That(rig.H.State.State.LastCompletedBulkOperationId, Is.EqualTo("load"));
            Assert.That(rig.H.State.State.RootNodeId, Is.Not.EqualTo(seeded), "the retry replaced the seeded root");
            Assert.That(seededLeaf.ReceivedCalls().Any(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.CopyRowsForBulkLoadAsync)),
                Is.True, "and relocated its rows into the loaded tree");
        });
    }

    [Test]
    public async Task A_retry_of_an_aborted_never_written_load_onto_a_root_that_replayed_its_seeds_is_refused_not_lost()
    {
        // Review S1 of #5830: a never-written load opens no switch, so nothing durable
        // names its seeds. If anything seeds a root before the retry, that root replays
        // the aborted seeds by key range, as in 10.2.0, and the retry is refused as
        // non-empty. Refused, never silent loss: the seeds are readable in that root.
        var rig = CreateHintRig();
        var fail = true;
        rig.H.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var leaf = ResolveLeaf(rig.H.Leaves, call.ArgAt<Guid>(0));
            leaf.MergeBulkSeedAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), Arg.Any<WalRoutingStamp>())
                .Returns(_ => fail ? Task.FromException(new TimeoutException("seed timed out")) : Task.CompletedTask);
            return leaf;
        });
        Assert.ThrowsAsync<TimeoutException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));
        Assert.Multiple(async () =>
        {
            Assert.That(SeedRoutings(rig.H), Has.All.Matches<WalRoutingStamp>(r => r.ForcesUnstamped),
                "precondition: the aborted attempt seeded unstamped, which every reader replays by key range");
            Assert.That((await ShardMarkerAsync(rig)).Open, Is.Empty, "precondition: it opened no switch");
            Assert.That(rig.H.State.State.RootNodeId, Is.Null, "precondition: it published nothing");
        });

        // A batch write seeds a root before the retry, and that root replays the seeds.
        var seeded = rig.H.AddLeaf();
        rig.H.State.State.RootNodeId = seeded;
        rig.H.State.State.RootIsLeaf = true;
        rig.H.Leaf(seeded).GetStatsAsync().Returns(Task.FromResult(new LeafStats { LiveKeys = 2 }));
        fail = false;

        var refused = Assert.ThrowsAsync<InvalidOperationException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));
        Assert.Multiple(async () =>
        {
            Assert.That(refused!.Message, Does.Contain("requires an empty shard"), "the retry is refused, loudly");
            Assert.That(rig.H.State.State.RootNodeId, Is.EqualTo(seeded), "the root holding the replayed seeds is kept");
            Assert.That(rig.H.State.State.LastCompletedBulkOperationId, Is.Null, "nothing reports the load complete");
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.NotRunning));
        });
    }

    [Test]
    public void A_load_onto_a_root_with_rows_and_no_open_switch_of_its_own_is_still_refused()
    {
        var rig = CreateHintRig();
        var seeded = rig.H.AddLeaf();
        rig.H.State.State.RootNodeId = seeded;
        rig.H.State.State.RootIsLeaf = true;
        rig.H.Leaf(seeded).GetStatsAsync().Returns(Task.FromResult(new LeafStats { LiveKeys = 1 }));

        Assert.ThrowsAsync<InvalidOperationException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));
    }
}
