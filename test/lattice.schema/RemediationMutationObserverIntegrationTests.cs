using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Schema.Tests;

[TestFixture]
[Category("Integration")]
public sealed class RemediationMutationObserverIntegrationTests
{
    [Test]
    public async Task Writes_after_remediation_publish_logical_identity()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<Configurator>();
        await using var cluster = builder.Build();
        await cluster.DeployAsync();
        try
        {
            const string treeId = "remediation-observer";
            var tree = cluster.GrainFactory.GetGrain<ILattice>(treeId);
            await tree.SetAsync("seed", "{}"u8.ToArray());
            var remediation = cluster.GrainFactory.GetGrain<ILatticeSchemaRemediationGrain>(treeId);
            var report = await remediation.StartAsync(LatticeValueTransform.Passthrough(),
                new LatticeSchemaPolicy([LatticeSchemaRule.MaxLength(4096)]));
            Assert.That(report.Succeeded, Is.True);
            Assert.That(await cluster.GrainFactory.GetLatticeRegistry().ResolveAsync(treeId), Is.Not.EqualTo(treeId));
            var observer = cluster.Silos.OfType<InProcessSiloHandle>().Single()
                .SiloHost.Services.GetRequiredService<Observer>();
            observer.Mutations.Clear();

            await tree.SetAsync("point", [2]);
            await tree.PnCounter("counter").IncrementAsync("local");
            await tree.SetManyAsync([new("many1", [3]), new("many2", [4])]);
            await tree.SetManyAtomicAsync([new("atomic1", [5]), new("atomic2", [6])]);
            await tree.DeleteAsync("point");
            await tree.DeleteRangeAsync("many", "manz");
            var apply = cluster.GrainFactory.GetGrain<IReplicationApplyGrain>(treeId);
            await apply.ApplySetAsync("remote", [7],
                new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks }, "peer", null, 0);

            var mutations = observer.Mutations.ToArray();
            Assert.That(mutations.Where(m => m.Kind == MutationKind.Set).Select(m => m.Key),
                Is.EquivalentTo(new[] { "point", "counter", "many1", "many2", "atomic1", "atomic2", "remote" }));
            Assert.That(mutations.Any(m => m.Kind == MutationKind.Delete && m.Key == "point"), Is.True);
            Assert.That(mutations.Any(m => m.Kind == MutationKind.DeleteRange), Is.True);
            Assert.That(mutations.Select(m => m.TreeId), Is.All.EqualTo(treeId));
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
            siloBuilder.AddLatticeSchemaEnforcement();
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<Observer>();
            siloBuilder.Services.AddSingleton<IMutationObserver>(services => services.GetRequiredService<Observer>());
        }
    }

    private sealed class Observer : IMutationObserver
    {
        public ConcurrentQueue<LatticeMutation> Mutations { get; } = new();

        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            Mutations.Enqueue(mutation);
            return Task.CompletedTask;
        }
    }
}
