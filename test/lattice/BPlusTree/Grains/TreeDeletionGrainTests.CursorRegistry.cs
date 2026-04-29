using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the leaf-cursor deregister wiring on
/// <see cref="TreeDeletionGrain"/>: the terminal lifecycle paths
/// (<see cref="TreeDeletionGrain.PurgeNowAsync"/> and
/// <see cref="TreeDeletionGrain.CompletePurgeAsync"/>) bulk-clear every
/// leaf-as-materialiser cursor for the tree so the per-shard WAL GC is
/// no longer pinned by stale cursors after the tree's data has been
/// removed.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private static (TreeDeletionGrain Grain,
                    FakePersistentState<TreeDeletionState> State,
                    ILeafCursorReporter Reporter,
                    IGrainFactory GrainFactory) CreateGrainWithReporter(
        ILeafCursorReporter? reporter = null,
        bool registerReporter = true,
        FakePersistentState<TreeDeletionState>? existingState = null)
    {
        reporter ??= Substitute.For<ILeafCursorReporter>();
        var sc = new ServiceCollection();
        if (registerReporter)
            sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("deletion", TreeId));
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var options = new LatticeOptions { SoftDeleteDuration = TimeSpan.FromHours(72) };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        for (int i = 0; i < ShardCount; i++)
        {
            var shardRoot = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Returns(shardRoot);
            shardRoot.MarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.PurgeAsync().Returns(Task.CompletedTask);
        }

        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId).Returns(compaction);
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
            }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);
        var state = existingState ?? new FakePersistentState<TreeDeletionState>();

        var grain = new TreeDeletionGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeDeletionGrain>(), state);
        return (grain, state, reporter, grainFactory);
    }

    [Test]
    public async Task PurgeNow_deregisters_leaf_cursors()
    {
        var (grain, state, reporter, _) = CreateGrainWithReporter();
        await grain.DeleteTreeAsync();

        await grain.PurgeNowAsync();

        await reporter.Received(1).UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        Assert.That(state.State.PurgeComplete, Is.True);
    }

    [Test]
    public async Task CompletePurge_deregisters_leaf_cursors()
    {
        var (grain, state, reporter, _) = CreateGrainWithReporter();
        await grain.DeleteTreeAsync();
        // Drive the timer-style purge path directly: BeginPurgeStateAsync
        // primes the in-progress flags, then CompletePurgeAsync runs the
        // tail (registry unregister + leaf-cursor deregister + reminder
        // teardown + purged-event publish).
        await grain.BeginPurgeStateAsync(startFromShard: 0);
        await grain.CompletePurgeAsync();

        await reporter.Received(1).UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        Assert.That(state.State.PurgeComplete, Is.True);
    }

    [Test]
    public async Task PurgeNow_is_no_op_when_reporter_not_registered()
    {
        // Hosts that have not added the replication package leave the
        // ILeafCursorReporter registration absent; the deregister
        // helper must silently no-op rather than fail the purge.
        var reporter = Substitute.For<ILeafCursorReporter>();
        var (grain, state, _, _) = CreateGrainWithReporter(
            reporter: reporter, registerReporter: false);
        await grain.DeleteTreeAsync();

        Assert.DoesNotThrowAsync(async () => await grain.PurgeNowAsync());

        await reporter.DidNotReceive().UnregisterTreeAsync(
            Arg.Any<string>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.PurgeComplete, Is.True);
    }

    [Test]
    public async Task PurgeNow_swallows_reporter_exceptions_and_completes_purge()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.UnregisterTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("registry hiccup")));
        var (grain, state, _, _) = CreateGrainWithReporter(reporter: reporter);
        await grain.DeleteTreeAsync();

        // Purge must complete even if the reporter throws: the tree's
        // data is gone and a residual cursor is harmless under the
        // in-memory registry.
        Assert.DoesNotThrowAsync(async () => await grain.PurgeNowAsync());
        Assert.That(state.State.PurgeComplete, Is.True);
    }
}
