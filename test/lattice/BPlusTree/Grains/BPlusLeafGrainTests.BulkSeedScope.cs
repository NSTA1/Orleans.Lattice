using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Review S2 of #5830: the bulk-seed scope is an <c>AsyncLocal</c>, so detached work a
/// seed's <c>MergeEntriesAsync</c> starts would inherit it and append through the seed's
/// fence. The one such task the leaf starts is the replay a barrier wait re-arms, whose
/// replayed terminals append; it runs outside the scope.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_replay_re_armed_by_a_bulk_seed_does_not_run_inside_the_seed_scope()
    {
        var seen = new List<bool>();
        using var leaf = new WedgedReplayLeaf(onHeadRead: () =>
        {
            lock (seen)
            {
                seen.Add(BPlusLeafGrain.InBulkSeedScopeForTesting);
            }
        });
        await leaf.ActivateAndParkAsync();

        // The activation's replay is cancelled under a waiting request, which disarms it,
        // so the seed's barrier wait re-arms one from inside the seed's scope.
        var waiting = leaf.Grain.GetAsync("k");
        Observe(waiting);
        leaf.Grain.CancelReplayBarrierForTest();
        Assert.That(() => waiting.GetAwaiter().GetResult(), Throws.InstanceOf<OperationCanceledException>(),
            "precondition: the cancelled replay failed its waiter and disarmed");
        var seed = leaf.Grain.MergeBulkSeedAsync(
            new Dictionary<string, LwwValue<byte[]>>
            {
                ["k"] = LwwValue<byte[]>.Create([1], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            },
            WalRoutingStamp.Unstamped);
        Observe(seed);

        var rearmed = SpinWait.SpinUntil(() => { lock (seen) { return seen.Count >= 2; } }, TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(rearmed, Is.True, "precondition: the seed re-armed a replay, which read the WAL head");
            Assert.That(BPlusLeafGrain.InBulkSeedScopeForTesting, Is.False, "the seed's scope does not leak to its caller");
            lock (seen)
            {
                Assert.That(seen, Has.Count.GreaterThanOrEqualTo(2));
                Assert.That(seen, Is.All.False, "the re-armed replay runs outside the seed's scope");
            }
        });
    }
}
