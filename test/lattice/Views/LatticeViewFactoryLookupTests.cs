using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

[TestFixture]
public sealed class LatticeViewFactoryLookupTests
{
    [TestCase(null)]
    [TestCase(false)]
    [TestCase(true)]
    public async Task GetAsync_remote_registration_uses_one_scalar_lookup(bool? aggregation)
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.GetAggregationKindAsync("remote").Returns(aggregation);
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        grains.GetGrain<IViewMaintainerGrain>("remote").Returns(maintainer);
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns("view-remote#g1");
        var tree = Substitute.For<ILattice>();
        grains.GetGrain<ILattice>("view-remote#g1").Returns(tree);
        tree.CountAsync(Arg.Any<CancellationToken>()).Returns(8);
        tree.CountAsync(AggregationRowCodec.FirstNonReservedKey, null, Arg.Any<CancellationToken>()).Returns(2);
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        options.Get("remote").Returns(new LatticeViewOptions());
        var factory = new LatticeViewFactory(grains, new ViewCatalog(), [],
            Substitute.For<ILatticeReplicationContext>(), options, services,
            new RuntimeViewProjectionProviderCatalog([]),
            new PredicateRuntimeViewProjectionCodec(
                services.GetRequiredService<Serializer<LatticePredicateNode>>()),
            NullLogger<LatticeViewFactory>.Instance);

        var view = await factory.GetAsync("remote");

        await registry.Received(1).GetAggregationKindAsync("remote");
        await registry.DidNotReceive().ListAsync();
        if (aggregation is null)
        {
            Assert.That(view, Is.Null);
            grains.DidNotReceive().GetGrain<IViewMaintainerGrain>("remote");
        }
        else
        {
            Assert.That(view, Is.Not.Null);
            Assert.That(await view!.CountAsync(), Is.EqualTo(aggregation.Value ? 2 : 8));
        }
    }

    [Test]
    public async Task GetAsync_remote_registration_is_rechecked_after_deletion_and_propagates_failure()
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.GetAggregationKindAsync("remote").Returns(
            _ => (bool?)false,
            _ => null,
            _ => throw new InvalidOperationException("Registry unavailable"));
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        options.Get("remote").Returns(new LatticeViewOptions());
        var factory = new LatticeViewFactory(grains, new ViewCatalog(), [],
            Substitute.For<ILatticeReplicationContext>(),
            options, services,
            new RuntimeViewProjectionProviderCatalog([]),
            new PredicateRuntimeViewProjectionCodec(services.GetRequiredService<Serializer<LatticePredicateNode>>()),
            NullLogger<LatticeViewFactory>.Instance);

        Assert.That(await factory.GetAsync("remote"), Is.Not.Null);
        Assert.That(await factory.GetAsync("remote"), Is.Null);
        Assert.ThrowsAsync<InvalidOperationException>(async () => await factory.GetAsync("remote"));
        await registry.Received(3).GetAggregationKindAsync("remote");
        await registry.DidNotReceive().ListAsync();
    }
}
