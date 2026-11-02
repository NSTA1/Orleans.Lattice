using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5706: a leaf write checks its span, appends its WAL record, and only then
/// stores the row. A narrow or a division that reads the WAL heads as the new leaf's
/// read position while such a write is between its append and its store would leave
/// the record below every reader's position, unapplied, if the activation then died.
/// Both drain the writes admitted before them first.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly TimeSpan HeldWindow = TimeSpan.FromMilliseconds(500);

    [Test]
    public async Task NarrowHighKey_waits_for_a_write_admitted_before_it_and_hands_its_row_back()
    {
        var commitLog = new FakeCommitLogWriter();
        var grain = CreateGrain(commitLog: commitLog);
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("vb"));

        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        commitLog.HoldNextAppend = hold;
        var write = grain.SetAsync("z", Encoding.UTF8.GetBytes("vz"));
        await commitLog.AppendHeld.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var narrow = grain.NarrowHighKeyAsync("m");
        var first = await Task.WhenAny(narrow, Task.Delay(HeldWindow));
        var pendingWhileHeld = first != narrow;

        hold.SetResult();
        await write.WaitAsync(TimeSpan.FromSeconds(10));
        var stranded = await narrow.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(pendingWhileHeld, Is.True, "the narrow waits for the write whose record is already in the log");
            Assert.That(stranded.Select(r => r.Key), Is.EqualTo(new[] { "z" }),
                "the drained write's row is above the bound, so it is handed back for relocation");
        });
    }

    [Test]
    public async Task A_division_waits_for_a_write_admitted_before_it_reads_the_wal_heads()
    {
        var commitLog = new FakeCommitLogWriter();
        var (grain, _, siblingMock) = BuildDrainSplitFixture(commitLog, walHead: 42L);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        commitLog.HoldNextAppend = hold;
        var held = grain.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await commitLog.AppendHeld.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // a, b, d, e: over capacity, so this write divides the leaf.
        await grain.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        var dividing = grain.SetAsync("e", Encoding.UTF8.GetBytes("5"));
        var first = await Task.WhenAny(dividing, Task.Delay(HeldWindow));
        var pendingWhileHeld = first != dividing;
        var siblingBornWhileHeld = siblingMock.ReceivedCalls()
            .Any(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync));

        hold.SetResult();
        await held.WaitAsync(TimeSpan.FromSeconds(10));
        await dividing.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(pendingWhileHeld, Is.True, "the division waits for the write whose record is already in the log");
            Assert.That(siblingBornWhileHeld, Is.False, "no sibling is born from heads read above an unapplied record");
        });
        await siblingMock.Received().SetCheckpointOffsetHintsAsync(Arg.Any<long[]>());
    }

    [Test]
    public async Task A_resumed_division_gives_neither_leaf_a_read_position()
    {
        // The recovery cannot drain (a write waiting for the gate it holds would be one
        // of the writes it waits for), so heads read now could lie above a record not
        // yet applied. The sibling replays its range from the log's start instead.
        var (grain, state, siblingMock, _) = BuildSplitFixture(walHead: 42L);
        await grain.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain.SetAsync("m", Encoding.UTF8.GetBytes("2"));
        var checkpointBefore = state.State.ProjectionCheckpointOffset;

        state.State.SplitState = Orleans.Lattice.Primitives.SplitState.SplitInProgress;
        state.State.SplitKey = "m";
        var siblingId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        state.State.SplitSiblingId = siblingId;
        state.State.NextSibling = siblingId;

        var result = await grain.SetAsync("b", Encoding.UTF8.GetBytes("3"));

        Assert.That(result, Is.Not.Null, "precondition: the division was resumed");
        await siblingMock.DidNotReceive().SetCheckpointOffsetHintsAsync(Arg.Any<long[]>());
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(checkpointBefore),
            "the donor keeps its own read position");
    }

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, IBPlusLeafGrain SiblingMock)
        BuildDrainSplitFixture(FakeCommitLogWriter commitLog, long walHead)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "test-tree";

        var grainFactory = Substitute.For<IGrainFactory>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString()));
        var siblingMock = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)siblingMock).GrainContext.Returns(siblingContext);
        siblingMock.MergeEntriesAsync(Arg.Any<Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        siblingMock.SetCheckpointOffsetHintsAsync(Arg.Any<long[]>()).Returns(Task.CompletedTask);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(siblingMock);
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(siblingMock);

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(walHead));
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ICommitLogWriter)).Returns(commitLog);
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "test-leaf"));
        context.ActivationServices.Returns(services);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero, WalPartitions = 1 },
            maxLeafKeys: 3,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsResolver, TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, state, siblingMock);
    }
}
