using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Review S2 of #5830: the bulk-seed scope is an <c>AsyncLocal</c>, so detached work a
/// seed's <c>MergeEntriesAsync</c> starts would inherit it and append through the seed's
/// fence. The one such task the leaf starts is the replay a barrier wait re-arms, whose
/// replayed terminals append; it runs outside the scope. Conversely (#6166), the seed's
/// own append reaches the writer with the seed's fence on every path.
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

    [Test]
    public async Task A_bulk_seed_whose_override_hold_raise_yields_still_appends_through_the_seed_fence()
    {
        // #6166: every merge needs an override hold, so a fresh seed leaf with a cursor
        // reporter always takes the hold raise's asynchronous path. That continuation
        // reached the fenced writer through its routed overload, which stamped with the
        // leaf's own, never-proven fence instead of the seed's: the seed was appended
        // unstamped and raised the shared unstamped watermark.
        var fences = new List<ILeafAppendFence>();
        var writer = Substitute.For<IFencedCommitLogWriter>();
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<ILeafAppendFence>(), Arg.Any<CancellationToken>())
            .Returns(call => RecordFence(call.ArgAt<ILeafAppendFence>(1), call.ArgAt<IReadOnlyList<WalRecord>>(0)));
        writer.AppendManyAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<ILeafAppendFence>(), Arg.Any<WalRoutingStamp>(), Arg.Any<CancellationToken>())
            .Returns(call => RecordFence(call.ArgAt<ILeafAppendFence>(1), call.ArgAt<IReadOnlyList<WalRecord>>(0)));
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.RaiseOverrideHoldsAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.Run(() => Task.Delay(10)));
        var grain = CreateGrain(
            new FakePersistentState<LeafNodeState>(),
            replicaId: Guid.NewGuid().ToString("N"),
            commitLog: writer,
            fenceGate: new OpenGate(),
            cursorReporter: reporter);

        await grain.MergeBulkSeedAsync(
            new Dictionary<string, LwwValue<byte[]>>
            {
                ["k"] = LwwValue<byte[]>.Create([1], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            },
            WalRoutingStamp.ForShardRoot(0, 3));

        await reporter.Received().RaiseOverrideHoldsAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<CancellationToken>());
        Assert.That(fences, Has.Count.EqualTo(1), "the seed is one batched append");
        Assert.That(fences[0], Is.InstanceOf<BulkSeedAppendFence>(),
            "the seed is stamped by its own fence, not the leaf activation's");

        Task<IReadOnlyList<long>> RecordFence(ILeafAppendFence fence, IReadOnlyList<WalRecord> entries)
        {
            lock (fences)
            {
                fences.Add(fence);
            }

            return Task.FromResult<IReadOnlyList<long>>(entries.Select(_ => 1L).ToArray());
        }
    }
}
