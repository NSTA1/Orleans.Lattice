using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Initiating a resize reads the current alias from the registry entry it
/// already captures for undo, rather than paying a separate
/// <c>ResolveAsync</c> turn for a projection of that entry.
/// </summary>
public partial class TreeResizeGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void UndoResize_bootstrap_fence_reuses_complete_owner_entry_but_reads_alias_owner(bool aliased)
    {
        var (grain, state, _, factory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Snapshot);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physical = aliased ? "copy" : TreeId;
        registry.GetEntryAsync(TreeId).Returns(new TreeRegistryEntry
        {
            MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = ShardCount,
            PhysicalTreeId = aliased ? physical : null,
        });
        registry.GetEntryAsync("copy").Returns(new TreeRegistryEntry
        {
            MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 3,
        });
        factory.GetGrain<IShardRootGrain>($"{physical}/0").IsBootstrapReadFencedAsync().Returns(false);
        factory.GetGrain<IShardRootGrain>($"{physical}/{(aliased ? 2 : 1)}").IsBootstrapReadFencedAsync().Returns(true);
        registry.ClearReceivedCalls();

        var refusal = Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());

        Assert.That(refusal!.Message, Does.Contain("read-fenced"));
        registry.Received(1).GetEntryAsync(TreeId);
        if (aliased) registry.Received(1).GetEntryAsync("copy");
        else Assert.That(registry.ReceivedCalls().Count(), Is.EqualTo(1));
        Assert.That(state.State.InProgress, Is.True);
    }

    [Test]
    public void UndoResize_bootstrap_fence_keeps_lazy_seeding_for_incomplete_pins()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Snapshot);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = new TreeRegistryEntry { ShardMap = new ShardMap { Slots = [0, 4] } };
        registry.GetEntryAsync(TreeId).Returns(entry);
        factory.GetGrain<IShardRootGrain>($"{TreeId}/0").IsBootstrapReadFencedAsync().Returns(false);
        factory.GetGrain<IShardRootGrain>($"{TreeId}/4").IsBootstrapReadFencedAsync().Returns(true);
        registry.ClearReceivedCalls();

        var refusal = Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());

        Assert.That(refusal!.Message, Does.Contain("read-fenced"));
        registry.Received(1).RegisterAsync(TreeId, entry);
        Assert.That(registry.ReceivedCalls().Count(c =>
            c.GetMethodInfo().Name == nameof(ILatticeRegistry.GetEntryAsync)), Is.GreaterThan(1));
        factory.GetGrain<IShardRootGrain>($"{TreeId}/4").Received(1).IsBootstrapReadFencedAsync();
    }

    [Test]
    public async Task InitiateResize_reads_options_and_undo_entry_in_one_registry_turn()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ClearReceivedCalls();

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.That(state.State.OldRegistryEntry!.ShardCount, Is.EqualTo(ShardCount));
        await registry.Received(1).GetEntryAsync(TreeId);
    }

    [Test]
    public async Task ResizeAsync_empty_probe_uses_one_entry_for_options_map_and_alias()
    {
        var (grain, state, _, factory, _) = CreateGrain();
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(new TreeRegistryEntry
        {
            MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = ShardCount,
            PhysicalTreeId = "copy", ShardMap = new ShardMap { Slots = [0, 4], Version = 2 },
        });
        factory.GetGrain<ITreeReshardGrain>(TreeId).IsIdleAsync().Returns(true);
        foreach (var index in new[] { 0, 4 })
            factory.GetGrain<IShardRootGrain>($"copy/{index}")
                .AnyBoundedAsync(Arg.Any<string?>()).Returns(new ShardAnyPage { Found = false });
        registry.ClearReceivedCalls();

        await grain.ResizeAsync(256, 64);

        Assert.That(state.State.Complete, Is.True);
        // The second read is the deliberately fresh mutation-time entry.
        await registry.Received(2).GetEntryAsync(TreeId);
        await registry.DidNotReceive().GetShardMapAsync(TreeId);
        await registry.DidNotReceive().ResolveAsync(TreeId);
        foreach (var index in new[] { 0, 4 })
            await factory.GetGrain<IShardRootGrain>($"copy/{index}").Received(1)
                .AnyBoundedAsync(Arg.Any<string?>());
    }

    [Test]
    public async Task InitiateResize_derives_the_old_physical_tree_from_the_captured_entry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = 128,
                MaxInternalChildren = 128,
                ShardCount = ShardCount,
                PhysicalTreeId = "aliased-tree",
            }));
        registry.ClearReceivedCalls();

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.OldPhysicalTreeId, Is.EqualTo("aliased-tree"));
            Assert.That(state.State.OldRegistryEntry!.PhysicalTreeId, Is.EqualTo("aliased-tree"));
            Assert.That(registry.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILatticeRegistry.ResolveAsync)), Is.Zero);
        });
    }
}
