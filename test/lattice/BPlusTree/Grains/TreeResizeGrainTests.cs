using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeResizeGrainTests
{
    private const string TreeId = "test-tree";
    private const int ShardCount = 2;

    private static (TreeResizeGrain grain,
                     FakePersistentState<TreeResizeState> state,
                     IReminderRegistry reminderRegistry,
                     IGrainFactory grainFactory,
                     IOptionsMonitor<LatticeOptions> optionsMonitor) CreateGrain(
        LatticeOptions? options = null,
        FakePersistentState<TreeResizeState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("resize", TreeId));
        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options ??= new LatticeOptions();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);
        var state = existingState ?? new FakePersistentState<TreeResizeState>();

        // Setup default registry grain mock.
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(TreeId).Returns(Task.FromResult(TreeId));
        // Default entry has the structural pin used by tests via resolver.
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
            }));
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory, options);

        var grain = new TreeResizeGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, optionsResolver,
            new LoggerFactory().CreateLogger<TreeResizeGrain>(), state);
        return (grain, state, reminderRegistry, grainFactory, optionsMonitor);
    }

    // --- ResizeAsync validation ---

    [Test]
    public void ResizeAsync_throws_on_invalid_maxLeafKeys()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => grain.ResizeAsync(1, 128));
    }

    [Test]
    public void ResizeAsync_throws_on_invalid_maxInternalChildren()
    {
        var (grain, _, _, _, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => grain.ResizeAsync(128, 2));
    }

    [Test]
    public async Task ResizeAsync_idempotent_with_same_parameters()
    {
        var (grain, state, _, _, _) = CreateGrain();

        // Simulate in-progress resize.
        state.State.InProgress = true;
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;

        // Same params — should not throw.
        await grain.ResizeAsync(256, 64);
    }

    [Test]
    public void ResizeAsync_throws_when_in_progress_with_different_parameters()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ResizeAsync(512, 64));
    }

    // --- InitiateResizeStateAsync ---

    [Test]
    public async Task InitiateResize_persists_intent_with_snapshot_phase()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.That(state.State.InProgress, Is.True);
        Assert.That(state.State.NewMaxLeafKeys, Is.EqualTo(256));
        Assert.That(state.State.NewMaxInternalChildren, Is.EqualTo(64));
        Assert.That(state.State.ShardCount, Is.EqualTo(ShardCount));
        Assert.That(state.State.OperationId, Is.Not.Null.And.Not.Empty);
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Snapshot));
        Assert.That(state.State.SnapshotTreeId, Is.Not.Null);
        Assert.That(state.State.OldPhysicalTreeId, Is.EqualTo(TreeId));
        // F-019c: the registry pin is now the source of structural truth,
        // so OldRegistryEntry is the seeded pin captured for UndoResizeAsync.
        Assert.That(state.State.OldRegistryEntry, Is.Not.Null);
        Assert.That(state.State.OldRegistryEntry!.ShardCount, Is.EqualTo(ShardCount));
    }

    [Test]
    public async Task InitiateResize_kicks_off_online_snapshot()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        await grain.InitiateResizeStateAsync(256, 64);

        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        await snapshot.Received(1).SnapshotWithOperationIdAsync(
            state.State.SnapshotTreeId!,
            SnapshotMode.Online,
            256, 64,
            state.State.OperationId!,
            Arg.Any<string>());
    }

    [Test]
    public async Task InitiateResize_captures_existing_registry_entry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var existingEntry = new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32 };
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(existingEntry));

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.That(state.State.OldRegistryEntry, Is.Not.Null);
        Assert.That(state.State.OldRegistryEntry!.MaxLeafKeys, Is.EqualTo(64));
        Assert.That(state.State.OldRegistryEntry.MaxInternalChildren, Is.EqualTo(32));
    }

    // --- WaitForSnapshotAsync ---

    [Test]
    public async Task WaitForSnapshot_advances_phase_to_swap()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";

        await grain.WaitForSnapshotAsync();

        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Swap));
        await grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId)
            .Received(1).RunSnapshotPassAsync();
    }

    // --- SwapAliasAsync ---

    [Test]
    public async Task SwapAlias_sets_alias_and_updates_registry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Swap;
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";

        await grain.SwapAliasAsync();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.MaxLeafKeys == 256 && e.MaxInternalChildren == 64));
        await registry.Received(1).SetAliasAsync(TreeId, $"{TreeId}/resized/op1");
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Reject));
    }

    // --- CleanupOldTreeAsync ---

    [Test]
    public async Task Cleanup_soft_deletes_old_physical_tree()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Cleanup;
        state.State.OldPhysicalTreeId = TreeId;

        await grain.CleanupOldTreeAsync();

        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .Received(1).DeleteTreeAsync();
    }

    // --- CompleteResizeAsync ---

    [Test]
    public async Task CompleteResize_resets_state()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        state.State.InProgress = true;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";
        state.State.OldPhysicalTreeId = TreeId;

        await grain.CompleteResizeAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task CompleteResize_unregisters_keepalive()
    {
        var (grain, _, reminderRegistry, _, _) = CreateGrain();
        var mockReminder = Substitute.For<IGrainReminder>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "resize-keepalive")
            .Returns(Task.FromResult(mockReminder));

        await grain.CompleteResizeAsync();

        await reminderRegistry.Received(1)
            .UnregisterReminder(Arg.Any<GrainId>(), mockReminder);
    }

    // --- Full pass ---

    [Test]
    public async Task Full_pass_snapshots_swaps_and_cleans_up()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;
        state.State.OperationId = "full-pass";
        state.State.ShardCount = ShardCount;
        state.State.SnapshotTreeId = $"{TreeId}/resized/full-pass";
        state.State.OldPhysicalTreeId = TreeId;

        // Snapshot phase
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Swap));

        // Swap phase → Reject
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Reject));

        // Reject phase → Cleanup
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Cleanup));

        // Cleanup phase → complete
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    // --- UndoResizeAsync ---

    [Test]
    public async Task UndoResize_recovers_old_tree_and_removes_alias()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        var oldEntry = new TreeRegistryEntry { MaxLeafKeys = 64, MaxInternalChildren = 32 };
        state.State.Complete = true;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/op1";
        state.State.OldRegistryEntry = oldEntry;

        await grain.UndoResizeAsync();

        // Recovered old tree.
        await grainFactory.GetGrain<ITreeDeletionGrain>(TreeId)
            .Received(1).RecoverAsync();

        // Removed alias.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.Received(1).RemoveAliasAsync(TreeId);

        // Deleted new tree.
        await grainFactory.GetGrain<ITreeDeletionGrain>($"{TreeId}/resized/op1")
            .Received(1).DeleteTreeAsync();

        // Restored old config.
        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.MaxLeafKeys == 64 && e.MaxInternalChildren == 32));

        Assert.That(state.State.Complete, Is.False);
        Assert.That(state.State.SnapshotTreeId, Is.Null);
        Assert.That(state.State.OldPhysicalTreeId, Is.Null);
        Assert.That(state.State.OldRegistryEntry, Is.Null);
    }

    [Test]
    public void UndoResize_throws_when_no_completed_resize()
    {
        var (grain, state, _, _, _) = CreateGrain();

        state.State.Complete = false;

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.UndoResizeAsync());
    }

    // --- Recovery ---

    [Test]
    public async Task Recovery_from_snapshot_phase_reruns_snapshot()
    {
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.InProgress = true;
        existingState.State.Phase = ResizePhase.Snapshot;
        existingState.State.OldPhysicalTreeId = TreeId;
        existingState.State.SnapshotTreeId = $"{TreeId}/resized/recovery";
        existingState.State.NewMaxLeafKeys = 256;
        existingState.State.NewMaxInternalChildren = 64;
        existingState.State.OperationId = "recovery";

        var (grain, state, reminderRegistry, grainFactory, _) =
            CreateGrain(existingState: existingState);
        SetupKeepalive(reminderRegistry);

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId)
            .Received(1).RunSnapshotPassAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Swap));
    }

    [Test]
    public async Task Recovery_from_swap_phase_reruns_alias_swap()
    {
        var existingState = new FakePersistentState<TreeResizeState>();
        existingState.State.InProgress = true;
        existingState.State.Phase = ResizePhase.Swap;
        existingState.State.NewMaxLeafKeys = 256;
        existingState.State.NewMaxInternalChildren = 64;
        existingState.State.SnapshotTreeId = $"{TreeId}/resized/recovery";
        existingState.State.OperationId = "recovery";

        var (grain, state, reminderRegistry, grainFactory, _) =
            CreateGrain(existingState: existingState);
        SetupKeepalive(reminderRegistry);

        await grain.ProcessNextPhaseAsync();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.Received(1).SetAliasAsync(TreeId, $"{TreeId}/resized/recovery");
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Reject));
    }

    // --- RunResizePassAsync ---

    [Test]
    public async Task RunResizePass_processes_all_phases()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;
        state.State.OperationId = "pass";
        state.State.ShardCount = ShardCount;
        state.State.SnapshotTreeId = $"{TreeId}/resized/pass";
        state.State.OldPhysicalTreeId = TreeId;

        await grain.RunResizePassAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    // --- Helpers ---

    private static void SetupKeepalive(IReminderRegistry reminderRegistry)
    {
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "resize-keepalive")
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
    }
}
