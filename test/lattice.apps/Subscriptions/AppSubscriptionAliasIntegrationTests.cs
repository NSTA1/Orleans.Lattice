using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
[Category("Integration")]
public sealed class AppSubscriptionAliasIntegrationTests
{
    [Test]
    public async Task Subscription_keeps_receiving_mutations_after_resize()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<Configurator>();
        await using var cluster = builder.Build();
        await cluster.DeployAsync();
        try
        {
            var services = cluster.Silos.OfType<InProcessSiloHandle>().Single().SiloHost.Services;
            await services.GetRequiredService<AppSubscriptionRouter>().RefreshAsync();
            var handler = services.GetRequiredService<RecordingChangeFeedHandler>();
            const string treeId = "a/notes/docs";
            var tree = cluster.GrainFactory.GetGrain<ILattice>(treeId);
            await tree.SetAsync("before", [1]);
            Assert.That(handler.Deliveries.Select(d => d.Mutation.Key), Does.Contain("before"));

            var resize = cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
            await resize.ResizeAsync(64, 64);
            await resize.RunResizePassAsync();
            Assert.That(await cluster.GrainFactory.GetLatticeRegistry().ResolveAsync(treeId), Is.Not.EqualTo(treeId));
            handler.Deliveries.Clear();

            await tree.SetAsync("after", [2]);
            await tree.DeleteAsync("after");
            Assert.That(handler.Deliveries.Select(d => d.Mutation.Kind),
                Is.EqualTo(new[] { MutationKind.Set, MutationKind.Delete }));
            Assert.That(handler.Deliveries.Select(d => d.Mutation.TreeId), Is.All.EqualTo(treeId));
            Assert.That(handler.Deliveries.Select(d => d.Subscription.Name), Is.All.EqualTo("docs-feed"));
        }
        finally
        {
            await cluster.StopAllSilosAsync();
        }
    }

    private sealed class Configurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<RecordingChangeFeedHandler>();
            siloBuilder.Services.AddSingleton(services =>
            {
                var source = new FakeAppSource();
                source.Add(Manifest(Subscription("docs-feed", "docs")));
                var projection = new FakeAppRegistryProjection();
                projection.Publish(Record(Notes));
                var catalog = new AppSubscriptionHandlerCatalog(services,
                    [new(Notes, "docs-feed", provider => provider.GetRequiredService<RecordingChangeFeedHandler>())]);
                return new AppSubscriptionRouter(projection, source, catalog, AppRegistryTestData.CreateLedger(new InMemoryAppRegistryStore()), NullLogger<AppSubscriptionRouter>.Instance);
            });
            siloBuilder.Services.AddSingleton<IMutationObserver>(services => services.GetRequiredService<AppSubscriptionRouter>());
        }
    }
}
