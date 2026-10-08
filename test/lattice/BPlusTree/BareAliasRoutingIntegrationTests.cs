using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
[Category("Integration")]
public sealed class BareAliasRoutingIntegrationTests
{
    private TestCluster _cluster = null!;
    private IGrainFactory Grains => _cluster.Client;
    private ILatticeRegistry Registry => Grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    [OneTimeSetUp]
    public async Task SetUpCluster()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task StopCluster()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [TearDown]
    public void ClearContext() => RequestContext.Clear();

    [Test]
    public async Task SetAliasAsync_lost_publication_ack_preserves_a_real_destination_write_from_a_fresh_router()
    {
        var logical = $"alias-ack-{Guid.NewGuid():N}";
        var target = logical + "-target";
        await SeedAsync(logical, shards: 2, value: 1);
        await SeedAsync(target, shards: 3, value: 2);
        var warm = Router(logical);
        Assert.That(await warm.GetAsync("shared"), Is.EqualTo(new byte[] { 1 }));
        var backing = Grains.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId);
        var proxy = Substitute.For<ISystemLattice>();
        proxy.GetAsync(Arg.Any<string>()).Returns(call => backing.GetAsync(call.Arg<string>()));
        var loseAcknowledgement = true;
        proxy.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(async call =>
        {
            var key = call.Arg<string>();
            var bytes = call.Arg<byte[]>();
            await backing.SetAsync(key, bytes);
            if (loseAcknowledgement && key == logical
                && JsonSerializer.Deserialize<TreeRegistryEntry>(bytes)!.PhysicalTreeId == target)
            {
                loseAcknowledgement = false;
                var fresh = Router(logical);
                await fresh.SetAsync("accepted", [42]);
                throw new IOException("publication acknowledgement lost after a fresh router accepted a write");
            }
        });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId).Returns(proxy);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), null)
            .Returns(call => Grains.GetGrain<IShardRootGrain>(call.ArgAt<string>(0)));
        factory.GetGrain<ITreeDeletionGrain>(Arg.Any<string>(), null)
            .Returns(call => Grains.GetGrain<ITreeDeletionGrain>(call.ArgAt<string>(0)));
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var mutator = new LatticeRegistryGrain(factory, options);

        await mutator.SetAliasAsync(logical, target);

        Assert.That(await Registry.ResolveAsync(logical), Is.EqualTo(target));
        Assert.That(await warm.GetAsync("accepted"), Is.EqualTo(new byte[] { 42 }));
        Assert.That(await Grains.GetGrain<ILattice>(target).GetAsync("accepted"), Is.EqualTo(new byte[] { 42 }));
        RequestContext.Clear();
        var oldMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2);
        Assert.That(await Grains.GetGrain<IShardRootGrain>($"{logical}/{oldMap.Resolve("accepted")}").GetAsync("accepted"),
            Is.Null, "the accepted write was never rolled back to or copied onto the old tree");
    }

    [Test]
    public async Task SetAliasAsync_moves_every_warmed_router_and_never_writes_the_retained_copy()
    {
        var logical = $"bare-alias-{Guid.NewGuid():N}";
        var target = logical + "-target";
        await SeedAsync(logical, shards: 2, value: 1);
        await SeedAsync(target, shards: 3, value: 2);
        var first = Router(logical);
        var second = Router(logical);
        Assert.That((await first.GetRoutingAsync()).PhysicalTreeId, Is.EqualTo(logical));
        Assert.That((await second.GetRoutingAsync()).PhysicalTreeId, Is.EqualTo(logical));
        Assert.That(await first.GetAsync("shared"), Is.EqualTo(new byte[] { 1 }));
        Assert.That(await second.GetAsync("shared"), Is.EqualTo(new byte[] { 1 }));

        await Registry.SetAliasAsync(logical, target);

        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        Assert.That(await first.GetAsync("shared", budget.Token), Is.EqualTo(new byte[] { 2 }));
        await second.SetAsync("new", [3], budget.Token);
        await first.SetManyAsync([new KeyValuePair<string, byte[]>("batch", [4])], budget.Token);
        Assert.That(await second.DeleteAsync("shared", budget.Token), Is.True);
        var current = Grains.GetGrain<ILattice>(target);
        var firstRouting = await first.GetRoutingAsync();
        var secondRouting = await second.GetRoutingAsync();
        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.SameAs(second), "two independent, warmed stateless-worker caches");
            Assert.That(firstRouting.PhysicalTreeId, Is.EqualTo(target));
            Assert.That(secondRouting.PhysicalTreeId, Is.EqualTo(target));
        });
        Assert.That(await current.GetAsync("new"), Is.EqualTo(new byte[] { 3 }));
        Assert.That(await current.GetAsync("batch"), Is.EqualTo(new byte[] { 4 }));
        Assert.That(await current.GetAsync("shared"), Is.Null);

        RequestContext.Clear();
        var oldMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2);
        Assert.That(await Grains.GetGrain<IShardRootGrain>($"{logical}/{oldMap.Resolve("shared")}").GetAsync("shared"),
            Is.EqualTo(new byte[] { 1 }), "direct shard maintenance can still inspect the retained copy");
        Assert.That(await Grains.GetGrain<IShardRootGrain>($"{logical}/{oldMap.Resolve("new")}").GetAsync("new"), Is.Null);
        Assert.That(await Grains.GetGrain<IShardRootGrain>($"{logical}/{oldMap.Resolve("batch")}").GetAsync("batch"), Is.Null);
    }

    [Test]
    public async Task SetAliasAsync_repeated_moves_and_remove_restore_the_matching_map()
    {
        var logical = $"bare-roundtrip-{Guid.NewGuid():N}";
        var first = logical + "-first";
        var second = logical + "-second";
        await SeedAsync(logical, 2, 1);
        await SeedAsync(first, 3, 2);
        await SeedAsync(second, 4, 3);
        var router = Router(logical);
        Assert.That(await router.GetAsync("shared"), Is.EqualTo(new byte[] { 1 }));
        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        foreach (var (target, value) in new[] { (first, (byte)2), (second, (byte)3), (first, (byte)2), (first, (byte)2) })
        {
            await Registry.SetAliasAsync(logical, target);
            Assert.That(await router.GetAsync("shared", budget.Token), Is.EqualTo(new[] { value }));
        }
        Assert.That(await Grains.GetGrain<ILattice>(second).GetAsync("shared"), Is.EqualTo(new byte[] { 3 }),
            "a distinct physical id remains directly readable");

        await Registry.RemoveAliasAsync(logical);
        Assert.That(await router.GetAsync("shared", budget.Token), Is.EqualTo(new byte[] { 1 }));
        Assert.That((await router.GetRoutingAsync()).Map.Slots, Is.EqualTo(ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2).Slots));
        await router.SetAsync("restored", [5], budget.Token);
        Assert.That(await Grains.GetGrain<ILattice>(first).GetAsync("restored"), Is.Null);
    }

    [Test]
    public async Task Moving_one_of_two_aliases_does_not_erase_the_other_alias_redirect()
    {
        var physical = $"bare-shared-{Guid.NewGuid():N}";
        var first = physical + "-alias-a";
        var second = physical + "-alias-b";
        var destination = physical + "-destination";
        await SeedAsync(physical, 1, 1);
        await SeedAsync(destination, 1, 2);
        await Registry.SetAliasAsync(first, physical);
        await Registry.SetAliasAsync(second, physical);
        var a = Router(first);
        var b = Router(second);
        Assert.That(await a.GetAsync("shared"), Is.EqualTo(new byte[] { 1 }));
        Assert.That(await b.GetAsync("shared"), Is.EqualTo(new byte[] { 1 }));

        await Registry.SetAliasAsync(first, destination);
        await Registry.SetAliasAsync(second, destination);

        using var budget = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        Assert.That(await a.GetAsync("shared", budget.Token), Is.EqualTo(new byte[] { 2 }));
        Assert.That(await b.GetAsync("shared", budget.Token), Is.EqualTo(new byte[] { 2 }));
        Assert.That(await Grains.GetGrain<ILattice>(physical).GetAsync("shared"), Is.EqualTo(new byte[] { 1 }));
    }

    private LatticeGrain Router(string treeId)
    {
        // Explicit instances make the two warm caches deterministic, while every
        // registry and shard call still runs through the real Orleans cluster.
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        return new LatticeGrain(context, Grains, options, TestOptionsResolver.ForFactory(Grains),
            Substitute.For<IServiceProvider>(), NullLogger<LatticeGrain>.Instance);
    }

    private async Task SeedAsync(string treeId, int shards, byte value)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = shards });
        await Grains.GetGrain<ILattice>(treeId).SetAsync("shared", [value]);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
