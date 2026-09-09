using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Resilience tests for the shard root's coalescing dirty-leaf flush loop.
/// <para>
/// Kept separate from <see cref="ShardRootGrainDirtyLeavesTests"/> deliberately:
/// that fixture wires no timer registry, and several of its assertions depend on
/// the flush timer failing to arm. These tests need the opposite, so they supply
/// a registry and drive the captured tick directly.
/// </para>
/// <para>
/// Issue 2419: the loop re-armed on every failure and retried for the life of the
/// activation. A shard root whose ETag no longer matched its stored row therefore
/// rewrote the same doomed state indefinitely, and the failures were the load that
/// produced the next failures.
/// </para>
/// </summary>
[TestFixture]
public class ShardRootGrainDirtyLeafFlushResilienceTests
{
    private const string ShardKey = "dirty-flush-tree/0";

    private sealed record Harness(
        ShardRootGrain Grain,
        FakePersistentState<ShardRootState> State,
        ITimerRegistry TimerRegistry,
        IGrainTimer Timer);

    private static Harness CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var timerRegistry = Substitute.For<ITimerRegistry>();
        var grainTimer = Substitute.For<IGrainTimer>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(grainTimer);
        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "dirty-flush-leaf-0");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.DeleteAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        // Leaf-access tracking off, so the ONLY timer this grain registers is the
        // dirty-leaf flush and the captured callback is unambiguous.
        var options = new LatticeOptions { LeafCachePreWarmCount = 0 };
        var optionsResolver = TestOptionsResolver.Create(baseOptions: options, factory: factory);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness(grain, state, timerRegistry, grainTimer);
    }

    private static Func<CancellationToken, Task> CapturedTimerCallback(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    /// <summary>
    /// Routes a Delete (which marks a leaf dirty), arms a persistent write fault,
    /// then fires one flush tick. <c>ThrowOnWrite</c> is one-shot, and the tick
    /// no-ops unless marks are pending, so both have to be re-established for
    /// every tick or consecutive failures silently merge into a single run.
    /// </summary>
    private static async Task FireFailingTickAsync(
        Harness harness,
        Func<CancellationToken, Task> tick,
        int sequence,
        Exception? fault = null)
    {
        await harness.Grain.DeleteAsync($"k-fail-{sequence}");
        harness.State.ThrowOnWrite = fault ?? new InconsistentStateException(
            "Version conflict (WriteState): ETag=5220.");
        await tick(CancellationToken.None);
    }

    [Test]
    public async Task The_dirty_flush_loop_suspends_itself_after_the_consecutive_failure_ceiling()
    {
        var harness = CreateGrain();
        await harness.Grain.DeleteAsync("k-prime");
        var tick = CapturedTimerCallback(harness.TimerRegistry);

        for (var i = 0; i < ShardRootGrain.MaxConsecutiveFlushFailures; i++)
        {
            await FireFailingTickAsync(harness, tick, i);
        }

        harness.Timer.Received(1).Dispose();
    }

    [Test]
    public async Task The_dirty_flush_loop_keeps_retrying_below_the_ceiling()
    {
        // Negative control: suspension must be caused by REACHING the ceiling,
        // not by any failure at all. A loop that gave up on the first failure
        // would pass the test above while being worse than the bug it replaces.
        var harness = CreateGrain();
        await harness.Grain.DeleteAsync("k-prime");
        var tick = CapturedTimerCallback(harness.TimerRegistry);

        for (var i = 0; i < ShardRootGrain.MaxConsecutiveFlushFailures - 1; i++)
        {
            await FireFailingTickAsync(harness, tick, i);
        }

        harness.Timer.DidNotReceive().Dispose();

        var writesBefore = harness.State.WriteCount;
        await tick(CancellationToken.None);
        Assert.That(harness.State.WriteCount, Is.GreaterThan(writesBefore),
            "the loop below the ceiling is still attempting to flush");
    }

    [Test]
    public async Task A_successful_dirty_flush_resets_the_consecutive_failure_count()
    {
        // Only an unbroken run should suspend, so an intermittent fault that
        // recovers in between must never accumulate toward the ceiling.
        var harness = CreateGrain();
        await harness.Grain.DeleteAsync("k-prime");
        var tick = CapturedTimerCallback(harness.TimerRegistry);
        var sequence = 0;

        for (var round = 0; round < 3; round++)
        {
            for (var i = 0; i < ShardRootGrain.MaxConsecutiveFlushFailures - 1; i++)
            {
                await FireFailingTickAsync(harness, tick, sequence++);
            }

            await harness.Grain.DeleteAsync($"k-ok-{round}");
            await tick(CancellationToken.None);
        }

        harness.Timer.DidNotReceive().Dispose();
    }

    [Test]
    public async Task A_suspended_dirty_flush_loop_is_not_re_armed_by_a_later_delete()
    {
        // This is the load-bearing one for the dirty-leaf loop specifically:
        // EnsureDirtyFlushTimerArmed() runs on EVERY mark, so disposing the
        // timer without latching would let the very next routed Delete restart
        // the loop and defeat the ceiling entirely.
        var harness = CreateGrain();
        await harness.Grain.DeleteAsync("k-prime");
        var tick = CapturedTimerCallback(harness.TimerRegistry);

        for (var i = 0; i < ShardRootGrain.MaxConsecutiveFlushFailures; i++)
        {
            await FireFailingTickAsync(harness, tick, i);
        }

        var registrationsAtSuspension = harness.TimerRegistry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

        await harness.Grain.DeleteAsync("k-after-suspension");
        await harness.Grain.DeleteAsync("k-after-suspension-2");

        var registrationsAfter = harness.TimerRegistry.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));

        Assert.That(registrationsAfter, Is.EqualTo(registrationsAtSuspension),
            "a suspended flush loop must stay suspended for the rest of the activation");
    }

    [Test]
    public async Task Suspension_does_not_discard_the_pending_dirty_marks()
    {
        // Suspension bounds wasted writes; it must not lose the compaction
        // signal. The marks stay in memory and stay discoverable through the
        // snapshot API the coordinator actually reads.
        var harness = CreateGrain();
        await harness.Grain.DeleteAsync("k-prime");
        var tick = CapturedTimerCallback(harness.TimerRegistry);

        for (var i = 0; i < ShardRootGrain.MaxConsecutiveFlushFailures; i++)
        {
            await FireFailingTickAsync(harness, tick, i);
        }

        var snapshot = await harness.Grain.GetDirtyLeavesSinceLastCompactionAsync();

        Assert.That(snapshot.DirtyLeaves, Is.Not.Empty,
            "a suspended flush loop must not drop the dirty-leaf signal");
    }

    [Test]
    public async Task A_transient_write_fault_also_counts_toward_the_ceiling()
    {
        // The dirty-leaf marks are a compaction signal with its own chain-walk
        // fallback, so unlike the leaf-access model there is no stale-ETag
        // special case here: every failure class is bounded the same way.
        var harness = CreateGrain();
        await harness.Grain.DeleteAsync("k-prime");
        var tick = CapturedTimerCallback(harness.TimerRegistry);

        for (var i = 0; i < ShardRootGrain.MaxConsecutiveFlushFailures; i++)
        {
            await FireFailingTickAsync(
                harness, tick, i, new InvalidOperationException("database is locked"));
        }

        harness.Timer.Received(1).Dispose();
    }
}
