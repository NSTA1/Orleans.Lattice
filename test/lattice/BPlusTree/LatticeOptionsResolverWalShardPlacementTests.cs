using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="LatticeOptionsResolver.ResolveWalShardPlacementAsync"/>,
/// the WAL shard activation's read of its placement and durable move fence
/// (issue #4525): a live fence fences the activation, a lapsed one is released
/// first and the provider is then resolved from the pin the release returned.
/// </summary>
[TestFixture]
public sealed class LatticeOptionsResolverWalShardPlacementTests
{
    private const string Tree = "tree-a";

    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry, IWalStorageProvider Baseline, IWalStorageProvider Secondary) Build(WalPlacementPin pin)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetWalPlacementAsync(Tree).Returns(Task.FromResult(pin));

        var baseline = Substitute.For<IWalStorageProvider>();
        var secondary = Substitute.For<IWalStorageProvider>();
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out Arg.Any<IWalStorageProvider>())
            .Returns(ci => { ci[1] = baseline; return true; });
        catalog.TryGet("secondary", out Arg.Any<IWalStorageProvider>())
            .Returns(ci => { ci[1] = secondary; return true; });
        return (new LatticeOptionsResolver(factory, monitor, logger: null, walProviderCatalog: catalog), registry, baseline, secondary);
    }

    private static WalMoveFence Fence(TimeSpan fromNow) => new()
    {
        MoveId = "move-a",
        SourceProviderKey = IWalStorageProviderCatalog.DefaultProviderKey,
        LeaseExpiresUtcTicks = DateTime.UtcNow.Add(fromNow).Ticks,
    };

    [Test]
    public async Task An_unfenced_partition_resolves_unfenced()
    {
        var (resolver, registry, baseline, _) = Build(WalPlacementPin.Create());

        var resolution = await resolver.ResolveWalShardPlacementAsync(Tree, 0);

        Assert.That(resolution.Provider, Is.SameAs(baseline));
        Assert.That(resolution.FenceExpiresUtcTicks, Is.Null);
        await registry.DidNotReceiveWithAnyArgs().ReleaseWalMoveFenceAsync(default!, default, default!, default);
    }

    [Test]
    public async Task A_live_fence_on_the_resolved_provider_fences_the_activation_without_releasing_it()
    {
        var fence = Fence(TimeSpan.FromMinutes(5));
        var (resolver, registry, _, _) = Build(WalPlacementPin.Create().WithFence(0, fence));

        var resolution = await resolver.ResolveWalShardPlacementAsync(Tree, 0);

        Assert.That(resolution.FenceExpiresUtcTicks, Is.EqualTo(fence.LeaseExpiresUtcTicks));
        await registry.DidNotReceiveWithAnyArgs().ReleaseWalMoveFenceAsync(default!, default, default!, default);
    }

    [Test]
    public async Task A_lapsed_fence_is_released_and_the_provider_is_resolved_from_the_pin_the_release_returned()
    {
        // The flip landed between the activation's read and its release: the
        // release returns the flipped pin, and the activation must bind the target.
        var (resolver, registry, _, secondary) = Build(WalPlacementPin.Create().WithFence(0, Fence(TimeSpan.FromMinutes(-1))));
        registry.ReleaseWalMoveFenceAsync(Tree, 0, "move-a", true)
            .Returns(Task.FromResult(WalPlacementPin.Create().WithPartition(0, "secondary", 1)));

        var resolution = await resolver.ResolveWalShardPlacementAsync(Tree, 0);

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Provider, Is.SameAs(secondary));
            Assert.That(resolution.PlacementVersion, Is.EqualTo(1));
            Assert.That(resolution.FenceExpiresUtcTicks, Is.Null);
        });
    }

    [Test]
    public async Task A_lapsed_fence_the_registry_keeps_leaves_the_activation_fenced_for_at_least_the_retry_floor()
    {
        var lapsed = WalPlacementPin.Create().WithFence(0, Fence(TimeSpan.FromMinutes(-1)));
        var (resolver, registry, _, _) = Build(lapsed);
        registry.ReleaseWalMoveFenceAsync(Tree, 0, "move-a", true).Returns(Task.FromResult(lapsed));
        var before = DateTime.UtcNow.Ticks;

        var resolution = await resolver.ResolveWalShardPlacementAsync(Tree, 0);

        Assert.That(resolution.FenceExpiresUtcTicks, Is.GreaterThanOrEqualTo(before + LatticeOptionsResolver.WalMoveFenceRetryFloor.Ticks));
    }

    [Test]
    public async Task A_fence_on_a_provider_the_placement_has_left_is_inert()
    {
        var pin = WalPlacementPin.Create().WithFence(0, Fence(TimeSpan.FromMinutes(5))).WithPartition(1, "secondary", 1)
            with { Overrides = new Dictionary<int, string> { [0] = "secondary" } };
        var (resolver, _, _, secondary) = Build(pin);

        var resolution = await resolver.ResolveWalShardPlacementAsync(Tree, 0);

        Assert.That(resolution.Provider, Is.SameAs(secondary));
        Assert.That(resolution.FenceExpiresUtcTicks, Is.Null);
    }
}
