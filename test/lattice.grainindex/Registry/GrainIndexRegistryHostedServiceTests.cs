using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Registry;

/// <summary>
/// Covers <see cref="GrainIndexRegistryHostedService"/>: the shim that attaches
/// reconciliation to host start-up, and the registration that puts it there.
/// </summary>
[TestFixture]
public sealed class GrainIndexRegistryHostedServiceTests
{
    private static (ServiceProvider Provider, FakeGrainIndexRegistryStore Store) Harness()
    {
        var builder = new StubSiloBuilder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));
        return (builder.BuildServiceProvider(), new FakeGrainIndexRegistryStore());
    }

    private static GrainIndexRegistryHostedService ServiceOver(
        ServiceProvider provider,
        FakeGrainIndexRegistryStore store) =>
        new(new GrainIndexRegistryReconciler(
            provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>(),
            store,
            new CapturingLogger<GrainIndexRegistryReconciler>()));

    [Test]
    public async Task Starting_reconciles_every_declared_index()
    {
        var (provider, store) = Harness();
        using (provider)
        {
            await ServiceOver(provider, store).StartAsync(CancellationToken.None);
        }

        Assert.That(store.Peek("users"), Is.Not.Null,
            "Attaching reconciliation to host start-up is the whole purpose of the shim.");
    }

    [Test]
    public async Task Starting_propagates_the_cancellation_token()
    {
        var (provider, store) = Harness();
        using var cts = new CancellationTokenSource();
        using (provider)
        {
            await ServiceOver(provider, store).StartAsync(cts.Token);
        }

        Assert.That(store.LastToken, Is.EqualTo(cts.Token));
    }

    [Test]
    public void Starting_surfaces_a_reconciliation_failure_so_the_host_fails_to_start()
    {
        var builder = new StubSiloBuilder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));
        using var provider = builder.BuildServiceProvider();

        var store = new FakeGrainIndexRegistryStore();
        var resolver = new FakeMergeModeResolver()
            .Replicating(GrainIndexTreeNames.ForIndex("users"), LatticeMergeMode.OrSet);
        var service = new GrainIndexRegistryHostedService(new GrainIndexRegistryReconciler(
            provider.GetRequiredService<IOptions<GrainIndexDeclarationOptions>>(),
            provider.GetRequiredService<IOptionsMonitor<GrainIndexOptions>>(),
            store,
            new CapturingLogger<GrainIndexRegistryReconciler>(),
            resolver));

        Assert.That(
            async () => await service.StartAsync(CancellationToken.None),
            Throws.TypeOf<GrainIndexReplicationNotAllowedException>(),
            "The shim must not swallow the rejection, or the silo would start with an index "
            + "configuration it was supposed to refuse.");
    }

    [Test]
    public async Task Stopping_is_a_no_op()
    {
        var (provider, store) = Harness();
        using (provider)
        {
            await ServiceOver(provider, store).StopAsync(CancellationToken.None);
        }

        Assert.That(store.WriteCount, Is.Zero,
            "The reconciler runs at start, not at stop; the service holds no resources to release.");
    }

    [Test]
    public void Declaring_an_index_registers_the_reconciliation_hosted_service()
    {
        var builder = new StubSiloBuilder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));

        Assert.That(
            builder.Services.Any(descriptor =>
                descriptor.ServiceType == typeof(IHostedService)
                && descriptor.ImplementationType == typeof(GrainIndexRegistryHostedService)),
            Is.True,
            "Without the registration the drift gate never runs and the whole feature is inert.");
    }

    [Test]
    public void Declaring_two_indexes_registers_the_hosted_service_only_once()
    {
        var builder = new StubSiloBuilder();
        builder
            .AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("users").Include(x => x.Age))
            .AddGrainIndex<ITestGuidKeyedGrain, TestGrainState>(
                static cfg => cfg.WithName("orders").Include(x => x.Country));

        Assert.That(
            builder.Services.Count(descriptor =>
                descriptor.ServiceType == typeof(IHostedService)
                && descriptor.ImplementationType == typeof(GrainIndexRegistryHostedService)),
            Is.EqualTo(1),
            "Reconciliation walks the whole declaration set, so registering it per index would "
            + "run the same pass once per declared index.");
    }

    [Test]
    public void Declaring_an_index_registers_the_registry_store_and_reconciler()
    {
        var builder = new StubSiloBuilder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));

        Assert.Multiple(() =>
        {
            Assert.That(
                builder.Services.Any(d => d.ServiceType == typeof(IGrainIndexRegistryStore)
                    && d.ImplementationType == typeof(GrainIndexRegistryStore)),
                Is.True);
            Assert.That(
                builder.Services.Any(d => d.ServiceType == typeof(GrainIndexRegistryReconciler)),
                Is.True);
            Assert.That(
                builder.Services.Any(d => d.ServiceType == typeof(OrleansGrainIndexSerializer<>)),
                Is.True,
                "The store needs an Orleans-backed serializer for whatever record type it stores.");
        });
    }

    [Test]
    public void Declaring_an_index_does_not_register_a_merge_mode_resolver_of_its_own()
    {
        var builder = new StubSiloBuilder();
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(
            static cfg => cfg.WithName("users").Include(x => x.Age));

        Assert.That(
            builder.Services.Any(d => d.ServiceType == typeof(ILatticeMergeModeResolver)),
            Is.False,
            "The replication guard audits whatever resolver the host already has; supplying one "
            + "would turn an audit into an override.");
    }
}
