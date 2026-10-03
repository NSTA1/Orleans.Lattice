using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the order of the alias swap and the old copy's fence.
/// The swap used to move the alias and leave the old shards serving until the
/// next phase tick moved them to Rejecting. In between, a router whose cached
/// alias predated the swap read the old copy while writers with fresh routing
/// wrote to the resized one, which mirrors nothing back - so a committed batch
/// read back at its predecessor round for as long as the reject was delayed (a
/// silo restart stalled it for seconds).
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public async Task SwapAlias_fences_every_old_shard_before_moving_the_alias()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var order = new List<string>();
        for (var i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            var index = i;
            shard.EnterRejectingAsync("op1").Returns(_ =>
            {
                order.Add($"reject-{index}");
                return Task.CompletedTask;
            });
        }
        registry.SwapAliasAsync(TreeId, snapshotTreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>()).Returns(_ =>
        {
            order.Add("alias");
            return Task.FromResult<TreeRegistryEntry?>(null);
        });

        await grain.SwapAliasAsync();

        Assert.That(order, Is.EqualTo(new[] { "reject-0", "reject-1", "alias" }),
            "Every old shard must reject before the alias moves: a stale-routed reader must never be "
            + "able to read the old copy once the resized copy has taken a write.");
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Reject));
    }

    [Test]
    public async Task SwapAlias_does_not_move_the_alias_when_an_old_shard_cannot_be_fenced()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/1")
            .EnterRejectingAsync("op1").ThrowsAsync(new TimeoutException("silo leaving"));

        Assert.ThrowsAsync<TimeoutException>(() => grain.SwapAliasAsync());

        await registry.DidNotReceiveWithAnyArgs().SwapAliasAsync(default!, default!, default!, default, default);
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Received(1).ExitRejectingAsync("op1");
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Swap),
            "The swap retries on the next tick; the fence is idempotent.");
    }

    [Test]
    public async Task SwapAlias_lifts_the_fence_when_the_alias_cannot_move()
    {
        // An ownership guard can refuse the flip on every tick until an operator
        // intervenes; the old copy must not stay fenced - unavailable - meanwhile.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.SwapAliasAsync(TreeId, snapshotTreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>()).ThrowsAsync(new InvalidOperationException("refused"));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.SwapAliasAsync());

        for (var i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.Received(1).EnterRejectingAsync("op1");
            await shard.Received(1).ExitRejectingAsync("op1");
        }
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Swap));
    }

    [Test]
    public async Task SwapAlias_keeps_the_fence_when_a_failed_flip_reached_the_registry()
    {
        // The flip failed in transport but landed: the alias names the resized
        // copy, so lifting the fence would reopen the stale-read window.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.SwapAliasAsync(TreeId, snapshotTreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>()).ThrowsAsync(new TimeoutException("lost reply"));
        registry.ResolveAsync(TreeId).Returns(Task.FromResult(snapshotTreeId));

        Assert.ThrowsAsync<TimeoutException>(() => grain.SwapAliasAsync());

        for (var i = 0; i < ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            await shard.DidNotReceive().ExitRejectingAsync(Arg.Any<string>());
        }
    }
}
