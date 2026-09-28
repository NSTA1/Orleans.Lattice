using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for releasing the shadow-forward a standalone online
/// snapshot installs on its source shards. The snapshot used to complete with
/// every source shard still forwarding to the destination, so later source
/// writes kept landing in the (by then independent) destination tree, a second
/// online snapshot or resize of the source was refused, and deleting the
/// destination failed source writes. A coordinator-driven snapshot (online
/// resize) must keep the shadow-forward, because its coordinator takes it on
/// to the <c>Rejecting</c> phase and clears it itself.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private static void SeedCompletedOnlineCopy(
        FakePersistentState<TreeSnapshotState> state,
        bool releasesShadowForward,
        SnapshotMode mode = SnapshotMode.Online)
    {
        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = ShardCount;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = mode;
        state.State.OperationId = "op-release";
        state.State.ReleasesShadowForwardOnCompletion = releasesShadowForward;
    }

    [Test]
    public async Task CompleteSnapshot_releases_source_shadow_forward_for_a_standalone_online_snapshot()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardMocks(grainFactory, SourceTreeId);
        SeedCompletedOnlineCopy(state, releasesShadowForward: true);

        await grain.CompleteSnapshotAsync();

        for (var i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).ClearShadowForwardAsync("op-release");
        }
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Complete, Is.True);
            Assert.That(state.State.ReleasesShadowForwardOnCompletion, Is.False,
                "the release obligation is discharged by the completion flip");
        });
    }

    [Test]
    public async Task CompleteSnapshot_keeps_a_coordinator_owned_shadow_forward()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardMocks(grainFactory, SourceTreeId);
        SeedCompletedOnlineCopy(state, releasesShadowForward: false);

        await grain.CompleteSnapshotAsync();

        for (var i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .DidNotReceive().ClearShadowForwardAsync(Arg.Any<string>());
        }
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task CompleteSnapshot_releases_before_persisting_so_a_failed_persist_keeps_the_obligation()
    {
        // The release runs before the completion flip is persisted. A persist
        // that fails leaves the obligation in place for the retried completion,
        // and the release itself is idempotent per shard.
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardMocks(grainFactory, SourceTreeId);
        SeedCompletedOnlineCopy(state, releasesShadowForward: true);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.CompleteSnapshotAsync());

        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0")
            .Received(1).ClearShadowForwardAsync("op-release");
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.ReleasesShadowForwardOnCompletion, Is.True);
        });
    }

    [Test]
    public async Task RunSnapshotPass_releases_every_source_shard_it_began_forwarding_for_a_standalone_online_snapshot()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount,
            operationId: "op-e2e-release", releasesShadowForwardOnCompletion: true);
        await grain.RunSnapshotPassAsync();

        for (var i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}");
            await shard.Received(1).BeginShadowForwardAsync(DestTreeId, "op-e2e-release", SourceTreeId);
            await shard.Received(1).ClearShadowForwardAsync("op-e2e-release");
        }
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task Standalone_SnapshotAsync_owns_and_releases_its_shadow_forward()
    {
        var h = CreateGrainWithTimerRegistry();
        SetupShardMocks(h.Factory, SourceTreeId);

        await h.Grain.SnapshotAsync(DestTreeId, SnapshotMode.Online);

        Assert.That(h.State.State.ReleasesShadowForwardOnCompletion, Is.True);
        await h.Factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .Received(1).RegisterAsync(DestTreeId, Arg.Is<TreeRegistryEntry>(e => e.DerivedFrom == null));
    }

    [Test]
    public async Task Coordinator_driven_snapshot_leaves_its_shadow_forward_to_the_coordinator()
    {
        var h = CreateGrainWithTimerRegistry();
        SetupShardMocks(h.Factory, SourceTreeId);

        await h.Grain.SnapshotWithOperationIdAsync(DestTreeId, SnapshotMode.Online,
            maxLeafKeys: null, maxInternalChildren: null, operationId: "resize-op", logicalTreeId: "logical-source");

        await h.Factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .Received(1).RegisterAsync(DestTreeId, Arg.Is<TreeRegistryEntry>(e => e.DerivedFrom == "logical-source"));

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.ReleasesShadowForwardOnCompletion, Is.False);
            Assert.That(h.State.State.OperationId, Is.EqualTo("resize-op"));
        });
    }

    [Test]
    public async Task Abort_clears_the_release_obligation()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        SeedCompletedOnlineCopy(state, releasesShadowForward: true);

        await grain.AbortAsync("op-release");

        Assert.That(state.State.ReleasesShadowForwardOnCompletion, Is.False);
    }
}
