using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Tests;

public sealed partial class LatticeStateApiEdgeCaseTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task ListViewsAsync_default_factory_never_looks_up_hidden_off_page_or_stats_disabled_views(bool includeStats)
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(new[]
        {
            RuntimeView("a-hidden", "denied"),
            RuntimeView("b-visible", "allowed"),
            RuntimeView("c-off-page", "allowed"),
        });
        registry.GetAggregationKindsAsync(Arg.Any<string[]>()).Returns(new bool?[] { null });
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        using var services = SamplingServices(grains, source => source == "allowed");
        var query = CreateQuery(services: services, grainFactory: grains);

        var page = await query.ListViewsAsync(new CatalogRequest { PageSize = 1, IncludeViewStats = includeStats });

        Assert.That(page.Entries.Select(entry => entry.ViewName), Is.EqualTo(new[] { "b-visible" }));
        Assert.That(page.NextPageToken, Is.EqualTo("b-visible"));
        if (includeStats)
            await registry.Received(1).GetAggregationKindsAsync(
                Arg.Is<string[]>(names => names.SequenceEqual(new[] { "b-visible" })));
        else
            await registry.DidNotReceiveWithAnyArgs().GetAggregationKindsAsync(default!);
        grains.DidNotReceiveWithAnyArgs().GetGrain<IViewMaintainerGrain>(default!);
    }

    [Test]
    public async Task ListViewsAsync_batches_only_the_visible_page_and_settles_bounded_sampling_waves()
    {
        var registrations = Enumerable.Range(0, 35)
            .Select(i => RuntimeView($"sample-{i:D2}", "source")).ToArray();
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(registrations);
        var batches = new List<string[]>();
        registry.GetAggregationKindsAsync(Arg.Any<string[]>()).Returns(call =>
        {
            var names = call.Arg<string[]>();
            batches.Add(names);
            return names.Select(_ => (bool?)false).ToArray();
        });
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fullWave = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inFlight = 0;
        var peak = 0;
        foreach (var registration in registrations)
        {
            var name = registration.ViewName;
            var maintainer = Substitute.For<IViewMaintainerGrain>();
            grains.GetGrain<IViewMaintainerGrain>(name).Returns(maintainer);
            maintainer.GetStatusAsync(Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                var active = Interlocked.Increment(ref inFlight);
                peak = Math.Max(peak, active);
                if (active == 32) fullWave.TrySetResult();
                await release.Task;
                Interlocked.Decrement(ref inFlight);
                return new ViewMaintainerStatus(4, $"view-{name}#g2");
            });
            var tree = Substitute.For<ILattice>();
            grains.GetGrain<ILattice>($"view-{name}#g2").Returns(tree);
            tree.CountAsync(Arg.Any<CancellationToken>()).Returns(9);
        }
        using var services = SamplingServices(grains);
        var query = CreateQuery(services: services, grainFactory: grains);
        var read = query.ListViewsAsync(new CatalogRequest { PageSize = 33, IncludeViewStats = true });
        try
        {
            await fullWave.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(read.IsCompleted, Is.False);
            Assert.That(batches.Single(), Has.Length.EqualTo(32));
        }
        finally
        {
            release.TrySetResult();
        }
        var page = await read;
        Assert.Multiple(() =>
        {
            Assert.That(peak, Is.EqualTo(32));
            Assert.That(inFlight, Is.Zero);
            Assert.That(batches.SelectMany(names => names), Is.EqualTo(registrations.Take(33).Select(r => r.ViewName)));
            Assert.That(batches.Select(names => names.Length), Is.EqualTo(new[] { 32, 1 }));
            Assert.That(page.Entries.Select(e => e.ViewName), Is.EqualTo(registrations.Take(33).Select(r => r.ViewName)));
            Assert.That(page.Entries.All(e => e.Lag == 4 && e.EntryCount == 9), Is.True);
            Assert.That(page.NextPageToken, Is.EqualTo("sample-32"));
        });
        await registry.DidNotReceiveWithAnyArgs().GetAggregationKindAsync(default!);
        grains.DidNotReceive().GetGrain<IViewMaintainerGrain>("sample-33");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ListViewsAsync_fresh_lookup_removal_or_fault_leaves_statistics_unavailable(bool fail)
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(new[] { RuntimeView("removed", "source") });
        registry.GetAggregationKindsAsync(Arg.Any<string[]>()).Returns(_ =>
            fail ? throw new InvalidOperationException("registry down") : new bool?[] { null });
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        using var services = SamplingServices(grains);
        var query = CreateQuery(services: services, grainFactory: grains);

        var page = await query.ListViewsAsync(new CatalogRequest { IncludeViewStats = true });

        Assert.That(page.Entries.Single().Lag, Is.Null);
        Assert.That(page.Entries.Single().EntryCount, Is.Null);
        grains.DidNotReceive().GetGrain<IViewMaintainerGrain>("removed");
    }

    [Test]
    public async Task ListViewsAsync_cancellation_settles_launched_samples_and_does_not_lookup_the_next_wave()
    {
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(Enumerable.Range(0, 33).Select(i => RuntimeView($"v-{i:D2}", "source")).ToArray());
        registry.GetAggregationKindsAsync(Arg.Any<string[]>()).Returns(call =>
            call.Arg<string[]>().Select(_ => (bool?)false).ToArray());
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        using var cancellation = new CancellationTokenSource();
        var maintainer = Substitute.For<IViewMaintainerGrain>();
        grains.GetGrain<IViewMaintainerGrain>(Arg.Any<string>()).Returns(maintainer);
        maintainer.GetStatusAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            cancellation.Cancel();
            return Task.FromCanceled<ViewMaintainerStatus>(cancellation.Token);
        });
        using var services = SamplingServices(grains);
        var query = CreateQuery(services: services, grainFactory: grains);

        Assert.ThrowsAsync<TaskCanceledException>(async () => await query.ListViewsAsync(
            new CatalogRequest { PageSize = 33, IncludeViewStats = true }, cancellation.Token));
        await registry.Received(1).GetAggregationKindsAsync(Arg.Is<string[]>(names => names.Length == 32));
    }

    private static ServiceProvider SamplingServices(IGrainFactory grains, Func<string, bool>? allow = null)
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var options = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
        var factory = new LatticeViewFactory(grains, new ViewCatalog(), [],
            Substitute.For<ILatticeReplicationContext>(), options, services,
            new RuntimeViewProjectionProviderCatalog([]),
            new PredicateRuntimeViewProjectionCodec(services.GetRequiredService<Serializer<LatticePredicateNode>>()),
            NullLogger<LatticeViewFactory>.Instance);
        var registrations = new ServiceCollection().AddSingleton<ILatticeViewFactory>(factory)
            .AddSingleton<IReadOnlyList<StartupViewRegistration>>([]).AddSingleton<IDisposable>(services);
        if (allow is not null)
        {
            registrations.AddSingleton<ILatticeAccessGate>(new AllowMatchingGate(allow));
            registrations.AddSingleton<ILatticeMembershipContext>(new FixedMembership(NamedSubject));
        }
        return registrations.BuildServiceProvider();
    }
}
