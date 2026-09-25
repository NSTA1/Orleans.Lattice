using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    [Test]
    public async Task Point_split_brackets_linking_before_the_first_persist_and_closes_on_fault()
    {
        var (grain, state, leaf, _) = CreateGrain();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () => gate.Task;
        state.ThrowOnWrite = new InvalidOperationException("injected split-link failure");
        leaf.SetAsync("k", Arg.Any<byte[]>()).Returns(new SplitResult
        {
            PromotedKey = "m", NewSiblingId = GrainId.Create("leaf", "sibling"),
        });
        var epoch = grain.RoutingEpoch;
        var write = grain.SetAsync("k", Bytes("v"));
        try
        {
            Assert.That(write.IsCompleted, Is.False);
            Assert.That(grain.RoutingEpoch, Is.EqualTo(epoch + 1));
            Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.False);
        }
        finally
        {
            gate.TrySetResult();
        }
        Assert.ThrowsAsync<InvalidOperationException>(async () => await write);
        Assert.That(grain.RoutingEpoch, Is.EqualTo(epoch + 2));
    }

    [Test]
    public async Task Point_write_retirement_retry_bumps_routing_epoch_without_bracketing_steady_writes()
    {
        var (grain, _, leaf, _) = CreateGrain();
        leaf.SetAsync("k", Arg.Any<byte[]>()).Returns(
            Task.FromException<SplitResult?>(new LeafRetiredException("root-leaf")),
            Task.FromResult<SplitResult?>(null));
        var epoch = grain.RoutingEpoch;
        await grain.SetAsync("k", Bytes("v"));
        Assert.That(grain.RoutingEpoch, Is.EqualTo(epoch + 2));
        await leaf.Received(2).SetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task Serial_read_warms_ownership_then_optimistic_absence_validates()
    {
        var (grain, _, leaf, _) = CreateGrain(warmStamp: false);
        leaf.GetWithVersionAsync("missing").Returns(Stamped(null));
        Assert.That((await grain.TryGetOptimisticAsync("missing")).IsValidated, Is.False);
        await leaf.DidNotReceive().GetWithVersionAsync("missing");

        await grain.GetAsync("missing");
        var read = await grain.TryGetOptimisticAsync("missing");

        Assert.That(read.IsValidated, Is.True);
        Assert.That(read.Value, Is.Null);
        await leaf.Received(2).GetWithVersionAsync("missing");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Old_wire_without_stamp_never_warms_or_validates(bool present)
    {
        var (grain, _, leaf, _) = CreateGrain(warmStamp: false);
        leaf.GetWithVersionAsync("k").Returns(new VersionedValue { Value = present ? Bytes("v") : null });
        for (var i = 0; i < 3; i++)
        {
            Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.False);
            await grain.GetAsync("k");
        }
        // The first failed warmup suppresses further probes, never validation.
        await leaf.Received(1).GetWithVersionAsync("k");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Default_or_zero_stamp_never_matches_even_if_cached(bool emptyEpoch)
    {
        var (grain, _, leaf, _) = CreateGrain();
        var epoch = emptyEpoch ? Guid.Empty : LeafEpoch;
        var generation = emptyEpoch ? 1 : 0;
        SeedRoutingStamp(grain, RootLeafId, epoch, generation);
        leaf.GetWithVersionAsync("k").Returns(new VersionedValue
        {
            LeafRoutingEpoch = epoch, LeafRoutingGeneration = generation,
        });
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Changed_leaf_generation_with_unchanged_root_epoch_records_serial_retry(bool absent)
    {
        const string tree = "optimistic-leaf-generation";
        var (listener, totals) = ListenForOptimisticReadOutcomes(tree);
        using (listener)
        {
            var (grain, _, leaf, _) = CreateGrain(shardKey: tree + "/0");
            var epoch = grain.RoutingEpoch;
            leaf.GetWithVersionAsync("k").Returns(Stamped(absent ? null : Bytes("new"), 2));
            Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.False);
            Assert.That(grain.RoutingEpoch, Is.EqualTo(epoch));
            Assert.That(totals.GetValueOrDefault("leaf_generation_changed"), Is.EqualTo(1));
            await grain.GetAsync("k");
            Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.True);
        }
    }

    [Test]
    public async Task Leaf_reactivation_cannot_match_a_previous_activation_stamp()
    {
        var (grain, _, leaf, _) = CreateGrain();
        leaf.GetWithVersionAsync("k").Returns(new VersionedValue
        {
            Value = Bytes("new"), LeafRoutingEpoch = Guid.NewGuid(), LeafRoutingGeneration = 1,
        });
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.False);
        await grain.GetAsync("k");
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Continuous_admitted_point_write_does_not_invalidate_a_read(bool ttl)
    {
        var (grain, _, leaf, _) = CreateGrain();
        var start = grain.RoutingEpoch;
        var call = CallContext(typeof(IShardRootGrain), nameof(IShardRootGrain.SetAsync), Task.CompletedTask);
        // The invocation drives the real Set overload, not only its classifier.
        var leafWrite = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.SetAsync("k", Arg.Any<byte[]>()).Returns(leafWrite.Task);
        leaf.SetAsync("k", Arg.Any<byte[]>(), Arg.Any<long>()).Returns(leafWrite.Task);
        call.Invoke().Returns(_ => ttl ? grain.SetAsync("k", Bytes("v"), 100) : grain.SetAsync("k", Bytes("v")));
        leaf.GetWithVersionAsync("k").Returns(Stamped(Bytes("v")));

        var write = ((IIncomingGrainCallFilter)grain).Invoke(call);
        Assert.That(write.IsCompleted, Is.False);
        Assert.That(grain.InterleavedPointWritesInFlight, Is.EqualTo(1));
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.True);
        leafWrite.SetResult(null);
        await write;
        Assert.That(grain.RoutingEpoch, Is.EqualTo(start));
    }

    [Test]
    public async Task Absent_then_written_during_read_can_linearize_before_the_write()
    {
        var (grain, _, leaf, _) = CreateGrain();
        var reply = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k").Returns(reply.Task);
        var read = grain.TryGetOptimisticAsync("k");
        await grain.SetAsync("k", Bytes("now-present"));
        reply.SetResult(Stamped(null)); // The leaf observed absence before Set.
        Assert.That((await read).IsValidated, Is.True);
        leaf.GetWithVersionAsync("k").Returns(Stamped(Bytes("now-present")));
        Assert.That((await grain.TryGetOptimisticAsync("k")).Value, Is.EqualTo(Bytes("now-present")));
    }

    [Test]
    public async Task Serial_warmup_overlapping_routing_change_does_not_publish_stamp()
    {
        var (grain, _, leaf, _) = CreateGrain(warmStamp: false);
        await grain.TryGetOptimisticAsync("k");
        var reply = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leaf.GetWithVersionAsync("k").Returns(reply.Task);
        var warmup = grain.GetAsync("k");
        grain.BeginRoutingMutation();
        grain.EndRoutingMutation();
        reply.SetResult(Stamped(Bytes("v")));
        await warmup;
        Assert.That((await grain.TryGetOptimisticAsync("k")).IsValidated, Is.False);
        await leaf.Received(1).GetWithVersionAsync("k");
    }
}
