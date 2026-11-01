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
    [Test]
    public async Task GetManyForSamplingAsync_keeps_local_startup_precedence_and_isolates_unavailable_remote_handles()
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        var batches = new List<string[]>();
        registry.GetAggregationKindsAsync(Arg.Any<string[]>()).Returns(call =>
        {
            batches.Add(call.Arg<string[]>());
            return new bool?[] { true, null };
        });
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        grains.GetGrain<IViewMaintainerGrain>(Arg.Any<string>()).Returns(maintainer);
        maintainer.GetActiveTreeIdAsync(Arg.Any<CancellationToken>()).Returns("view-active#g2");
        var tree = Substitute.For<ILattice>();
        grains.GetGrain<ILattice>("view-active#g2").Returns(tree);
        tree.CountAsync(Arg.Any<CancellationToken>()).Returns(8);
        tree.CountAsync(AggregationRowCodec.FirstNonReservedKey, null, Arg.Any<CancellationToken>()).Returns(2);
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
        var catalog = new ViewCatalog();
        var projection = Substitute.For<ILatticeViewProjection>();
        projection.ProjectionVersion.Returns("v1");
        catalog.Register(new ViewRegistration("local", "source", projection));
        var startup = new StartupViewRegistration("startup", "source", null,
            _ => Substitute.For<ILatticeAggregationProjection>());
        var factory = new LatticeViewFactory(grains, catalog, [startup],
            Substitute.For<ILatticeReplicationContext>(), options, services,
            new RuntimeViewProjectionProviderCatalog([]),
            new PredicateRuntimeViewProjectionCodec(services.GetRequiredService<Serializer<LatticePredicateNode>>()),
            NullLogger<LatticeViewFactory>.Instance);

        var handles = await factory.GetManyForSamplingAsync(["local", "startup", "remote", "missing"], CancellationToken.None);

        Assert.That(batches.Single(), Is.EqualTo(new[] { "remote", "missing" }));
        Assert.That(await handles[0]!.CountAsync(), Is.EqualTo(8));
        Assert.That(await handles[1]!.CountAsync(), Is.EqualTo(2));
        Assert.That(await handles[2]!.CountAsync(), Is.EqualTo(2));
        Assert.That(handles[3], Is.Null);
        grains.DidNotReceive().GetGrain<IViewMaintainerGrain>("missing");
        registry.GetAggregationKindsAsync(Arg.Any<string[]>()).Returns<Task<bool?[]>>(
            _ => throw new InvalidOperationException("registry unavailable"));
        var unavailable = await factory.GetManyForSamplingAsync(["local", "startup", "remote"], CancellationToken.None);
        Assert.That(unavailable.Take(2), Has.All.Not.Null);
        Assert.That(unavailable[2], Is.Null);
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await factory.GetManyForSamplingAsync(new string[33], CancellationToken.None));
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await factory.GetManyForSamplingAsync(["remote"], new CancellationToken(true)));
    }

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
