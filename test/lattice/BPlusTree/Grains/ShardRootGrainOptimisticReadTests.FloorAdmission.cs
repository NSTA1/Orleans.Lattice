using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The bootstrap drop-floor admission gate (issue #4549), through the shard
/// root's real incoming call filter. Once a shard is armed with a floor epoch it
/// refuses a replicated write stamped with an older one, wherever that write
/// waits; the arm returns only after the writes it admitted under an older epoch
/// have finished; and a shard activated after the epoch was raised reads it from
/// the registry before admitting its first stamped write.
/// </summary>
public sealed partial class ShardRootGrainOptimisticReadTests
{
    private static readonly System.Reflection.MethodInfo SetManyMethod =
        typeof(IShardRootGrain).GetMethod(nameof(IShardRootGrain.SetManyAsync))!;

    private static Task InvokeStamped(IIncomingGrainCallFilter filter, IIncomingGrainCallContext call, long? epoch)
    {
        RequestContext.Clear();
        if (epoch is { } stamped)
        {
            ReplicationFloorAdmission.Stamp(stamped);
        }

        try
        {
            return filter.Invoke(call);
        }
        finally
        {
            RequestContext.Clear();
        }
    }

    private static void RegisterFloorEpoch(IGrainFactory factory, long epoch) =>
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(Arg.Any<string>())
            .Returns(new TreeRegistryEntry { ReplicationFloorEpoch = epoch });

    [Test]
    public async Task An_armed_shard_refuses_an_older_stamped_write_and_admits_current_and_unstamped_ones()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        await grain.ArmReplicationFloorEpochAsync(2);
        var invoked = new List<string>();
        IIncomingGrainCallContext Write(string label) =>
            QuiesceCallContext(nameof(IShardRootGrain.SetManyAsync), () => { invoked.Add(label); return Task.CompletedTask; }, SetManyMethod);

        var stale = InvokeStamped(filter, Write("stale"), 1);
        await InvokeStamped(filter, Write("current"), 2);
        await InvokeStamped(filter, Write("unstamped"), null);
        await InvokeStamped(filter,
            QuiesceCallContext(nameof(IShardRootGrain.GetLeafIdForKeyAsync), () => { invoked.Add("callback"); return Task.CompletedTask; }),
            1);

        var refusal = Assert.ThrowsAsync<ReplicationFloorAdmissionStaleException>(() => stale);
        Assert.Multiple(() =>
        {
            Assert.That(refusal!.AdmittedEpoch, Is.EqualTo(1));
            Assert.That(refusal.RequiredEpoch, Is.EqualTo(2));
            Assert.That(invoked, Is.EqualTo(new[] { "current", "unstamped", "callback" }),
                "the stale write never reaches a leaf; local writes and leaf callbacks are never gated");
        });
    }

    [Test]
    public async Task Arming_waits_for_an_interleaved_write_admitted_under_an_older_epoch()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var leafMerge = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var inFlight = InvokeStamped(filter, QuiesceCallContext(nameof(IShardRootGrain.SetManyAsync), () => leafMerge.Task, SetManyMethod), 0);
        var arm = grain.ArmReplicationFloorEpochAsync(1);
        await Task.Delay(50);
        var armedEarly = arm.IsCompleted;

        leafMerge.SetResult();
        await inFlight;
        await arm;

        Assert.That(armedEarly, Is.False, "the arm is the barrier: a write it let through must have finished its leaf merge");
    }

    [Test]
    public async Task A_point_write_that_waits_out_the_arming_turn_is_refused()
    {
        var (grain, _, _, _) = CreateGrain();
        var filter = (IIncomingGrainCallFilter)grain;
        var armBody = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var armMethod = typeof(IShardRootGrain).GetMethod(nameof(IShardRootGrain.ArmReplicationFloorEpochAsync))!;
        var pointWriteInvoked = false;

        var arm = filter.Invoke(QuiesceCallContext(
            nameof(IShardRootGrain.ArmReplicationFloorEpochAsync),
            async () =>
            {
                await armBody.Task;
                await grain.ArmReplicationFloorEpochAsync(1);
            },
            armMethod));
        var write = InvokeStamped(filter,
            QuiesceCallContext(nameof(IShardRootGrain.SetAsync), () => { pointWriteInvoked = true; return Task.CompletedTask; }),
            0);

        armBody.SetResult();
        await arm;

        Assert.ThrowsAsync<ReplicationFloorAdmissionStaleException>(() => write);
        Assert.That(pointWriteInvoked, Is.False);
    }

    [Test]
    public async Task A_shard_activated_after_the_epoch_was_raised_reads_it_from_the_registry()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (grain, _, _, _) = CreateGrain(factory: factory);
        RegisterFloorEpoch(factory, 3);
        var filter = (IIncomingGrainCallFilter)grain;
        var invoked = 0;
        IIncomingGrainCallContext Write() =>
            QuiesceCallContext(nameof(IShardRootGrain.SetManyAsync), () => { invoked++; return Task.CompletedTask; }, SetManyMethod);

        var stale = InvokeStamped(filter, Write(), 2);
        await InvokeStamped(filter, Write(), 3);

        Assert.ThrowsAsync<ReplicationFloorAdmissionStaleException>(() => stale,
            "a split or reshard target, or a reactivated shard, never armed directly");
        Assert.That(invoked, Is.EqualTo(1));
        Assert.That(grain.ReplicationFloorEpoch, Is.EqualTo(3));
    }

    [TestCase(nameof(IShardRootGrain.SetAsync), true)]
    [TestCase(nameof(IShardRootGrain.SetManyAsync), true)]
    [TestCase(nameof(IShardRootGrain.DeleteAsync), true)]
    [TestCase(nameof(IShardRootGrain.MergeManyAsync), true)]
    [TestCase(nameof(IShardRootGrain.ApplyCrdtDeltaAsync), true)]
    [TestCase(nameof(IShardRootGrain.ApplyCrdtDeltaManyAsync), true)]
    [TestCase(nameof(IShardRootGrain.GetLeafIdForKeyAsync), false)]
    [TestCase(nameof(IShardRootGrain.ArmReplicationFloorEpochAsync), false)]
    public void IsFloorGatedMethod_gates_exactly_the_write_entry_methods(string method, bool gated) =>
        Assert.That(ShardRootGrain.IsFloorGatedMethod(method), Is.EqualTo(gated));
}
