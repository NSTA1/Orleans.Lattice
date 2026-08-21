using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Integration.Tests;

/// <summary>Real silo-start coverage for each rejected materialised-view topology.</summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class MaterialisedViewTopologyStartupIntegrationTests
{
    private static InvalidTopology _topology;

    [TestCase(InvalidTopology.DeriveLocallyReplicatesView, "two writers")]
    [TestCase(InvalidTopology.ShipViewDoesNotReplicateView, "never receive")]
    [TestCase(InvalidTopology.ShipViewReplicatesSourceWithoutProducer, "ShipViewProducerClusterId")]
    [TestCase(InvalidTopology.ShipViewSourceLessWithProducer, "Source-less-consumer topology")]
    public async Task Silo_start_rejects_invalid_topology(InvalidTopology topology, string expectedMessage)
    {
        _topology = topology;
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();

        try
        {
            var exception = Assert.CatchAsync(async () => await cluster.DeployAsync());
            Assert.That(exception!.ToString(), Does.Contain(expectedMessage));
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }

    public enum InvalidTopology
    {
        DeriveLocallyReplicatesView,
        ShipViewDoesNotReplicateView,
        ShipViewReplicatesSourceWithoutProducer,
        ShipViewSourceLessWithProducer,
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            const string viewName = "invalid-topology-view";
            const string sourceTreeId = "invalid-topology-source";
            const string viewTreeId = "view-invalid-topology-view";

            var replicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
            if (_topology != InvalidTopology.ShipViewDoesNotReplicateView)
            {
                replicatedTrees[viewTreeId] = LatticeMergeMode.LwwRegister;
            }

            if (_topology == InvalidTopology.ShipViewReplicatesSourceWithoutProducer)
            {
                replicatedTrees[sourceTreeId] = LatticeMergeMode.LwwRegister;
            }

            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(options =>
            {
                options.ClusterId = "site-a";
                options.ReplicatedTrees = replicatedTrees;
            });
            siloBuilder.AddLatticeViews(views => views.AddView(
                viewName,
                sourceTreeId,
                new PredicateLatticeViewProjection()));
            siloBuilder.ConfigureLatticeView(viewName, options =>
            {
                options.ReplicationMode = _topology == InvalidTopology.DeriveLocallyReplicatesView
                    ? LatticeViewReplicationMode.DeriveLocally
                    : LatticeViewReplicationMode.ShipView;
                if (_topology == InvalidTopology.ShipViewSourceLessWithProducer)
                {
                    options.ShipViewProducerClusterId = "site-a";
                }
            });
        }
    }
}
