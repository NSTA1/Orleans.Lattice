using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalShardGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Activation_reads_metadata_once_and_still_reads_fresh_placement(bool fenced)
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(TreeId).Returns(new TreeRegistryEntry { WalPartitions = 4, DerivedFrom = "logical" });
        var pin = WalPlacementPin.Create();
        if (fenced)
            pin = pin.WithFence(ShardIndex, new WalMoveFence
            {
                MoveId = "move",
                SourceProviderKey = IWalStorageProviderCatalog.DefaultProviderKey,
                LeaseExpiresUtcTicks = DateTime.UtcNow.AddMinutes(5).Ticks,
            });
        registry.GetWalPlacementAsync(TreeId).Returns(pin);
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var provider = new InMemoryWalStorageProvider();
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out Arg.Any<IWalStorageProvider>())
            .Returns(ci => { ci[1] = provider; return true; });
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal", $"{TreeId}/{ShardIndex}"));
        var grain = new WalShardGrain(context, options,
            new LatticeOptionsResolver(factory, options, walProviderCatalog: catalog),
            CreatePermissiveResolver(), CreatePermissiveClusterIdResolver(),
            CreateDefaultEncoder(), new FakePersistentState<WalShardFloorState>(), new TestWalClockFloorGate());

        await grain.OnActivateAsync(CancellationToken.None);

        await registry.Received(1).GetEntryAsync(TreeId);
        await registry.Received(1).GetWalPlacementAsync(TreeId);
        Received.InOrder(() =>
        {
            registry.GetEntryAsync(TreeId);
            registry.GetWalPlacementAsync(TreeId);
        });
        if (fenced)
            Assert.ThrowsAsync<LatticeWalQuiescingException>(() => grain.AppendAsync(MakeEntry("blocked"), CancellationToken.None));
        else
            Assert.That(await grain.AppendAsync(MakeEntry("accepted"), CancellationToken.None), Is.Zero);
    }
}
