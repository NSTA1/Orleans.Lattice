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

public class TreeResizeGrainIsCompleteTests
{
    private const string TreeId = "test-tree";
    private const int ShardCount = 2;

    private static TreeResizeGrain CreateGrainForIsComplete(
        FakePersistentState<TreeResizeState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("resize", TreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { ShardCount = ShardCount });
        var state = existingState ?? new FakePersistentState<TreeResizeState>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(Task.FromResult(TreeId));
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));

        var snapshot = Substitute.For<ITreeSnapshotGrain>();
        grainFactory.GetGrain<ITreeSnapshotGrain>(Arg.Any<string>()).Returns(snapshot);

        return new TreeResizeGrain(
            context, grainFactory, reminderRegistry, optionsMonitor,
            new LoggerFactory().CreateLogger<TreeResizeGrain>(), state);
    }

    [Test]
    public async Task IsCompleteAsync_returns_true_when_no_resize_initiated()
    {
        var grain = CreateGrainForIsComplete();
        var result = await grain.IsCompleteAsync();
        Assert.That(result, Is.True);
    }

    [Test]
    public async Task IsCompleteAsync_returns_false_when_resize_in_progress()
    {
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.InProgress = true;
        existingState.State.Phase = ResizePhase.Snapshot;
        var grain = CreateGrainForIsComplete(existingState);
        var result = await grain.IsCompleteAsync();
        Assert.That(result, Is.False);
    }

    [Test]
    public async Task IsCompleteAsync_returns_true_after_resize_completes()
    {
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.InProgress = false;
        existingState.State.Complete = true;
        var grain = CreateGrainForIsComplete(existingState);
        var result = await grain.IsCompleteAsync();
        Assert.That(result, Is.True);
    }
}
