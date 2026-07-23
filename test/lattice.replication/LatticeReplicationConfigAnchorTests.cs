using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the runtime replication-config anchor applied by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>
/// when <c>enableRuntimeConfig</c> is set, and the config-tree enrolment on
/// <see cref="LatticeSystemTreeNames"/>: the self-referential
/// <c>sys-replication-config</c> tree is enrolled under the fixed
/// <see cref="LatticeMergeMode.OrMap"/> mode, the OR-Map shape for the per-tree
/// config record is registered, host-declared trees are preserved, and the
/// anchor is idempotent.
/// </summary>
[TestFixture]
public class LatticeReplicationConfigAnchorTests
{
    private static ISiloBuilder BuilderWith(IServiceCollection services)
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    private static IServiceCollection RuntimeConfigServices(Action<LatticeReplicationOptions>? extra = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(
            o =>
            {
                o.ClusterId = "site-a";
                extra?.Invoke(o);
            },
            enableRuntimeConfig: true);
        return services;
    }

    [Test]
    public void AddLatticeReplication_returns_builder_for_fluent_chaining()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        var builder = BuilderWith(services);

        var result = builder.AddLatticeReplication(o => o.ClusterId = "site-a", enableRuntimeConfig: true);

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void EnableRuntimeConfig_enrols_config_tree_as_or_map()
    {
        var services = RuntimeConfigServices();

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.Multiple(() =>
        {
            Assert.That(trees, Is.Not.Null);
            Assert.That(trees![LatticeSystemTreeNames.ReplicationConfig], Is.EqualTo(LatticeMergeMode.OrMap));
        });
    }

    [Test]
    public void EnableRuntimeConfig_preserves_host_declared_trees()
    {
        var services = RuntimeConfigServices(o =>
        {
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                ["app-tree"] = LatticeMergeMode.PnCounter,
            };
        });

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.Multiple(() =>
        {
            Assert.That(trees!["app-tree"], Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(trees[LatticeSystemTreeNames.ReplicationConfig], Is.EqualTo(LatticeMergeMode.OrMap));
        });
    }

    [Test]
    public void EnableRuntimeConfig_forces_or_map_mode_over_host_declaration()
    {
        var services = RuntimeConfigServices(o =>
        {
            // Host mis-declares the config tree under a different mode; the
            // reserved enrolment must overwrite it with the fixed OrMap mode.
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                [LatticeSystemTreeNames.ReplicationConfig] = LatticeMergeMode.LwwRegister,
            };
        });

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.That(trees![LatticeSystemTreeNames.ReplicationConfig], Is.EqualTo(LatticeMergeMode.OrMap));
    }

    [Test]
    public async Task EnableRuntimeConfig_registers_the_config_tree_or_map_shape()
    {
        var services = RuntimeConfigServices();

        var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<CrdtShapeRegistry>();

        // Drain only the CRDT shape-startup hosted service (identified by name)
        // so the OR-Map descriptor is installed, without starting the
        // replication validators that assume a live cluster.
        foreach (var startup in provider.GetServices<IHostedService>()
                     .Where(h => h.GetType().Name == "CrdtShapeStartup"))
        {
            await startup.StartAsync(default);
        }

        var shape = registry.TryGet(LatticeSystemTreeNames.ReplicationConfig, LatticeMergeMode.OrMap);

        Assert.Multiple(() =>
        {
            Assert.That(shape, Is.Not.Null);
            Assert.That(shape!.Mode, Is.EqualTo(LatticeMergeMode.OrMap));
        });
    }

    [Test]
    public void EnableRuntimeConfig_is_idempotent()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        var builder = BuilderWith(services);

        builder.AddLatticeReplication(o => o.ClusterId = "site-a", enableRuntimeConfig: true);
        builder.AddLatticeReplication(o => o.ClusterId = "site-a", enableRuntimeConfig: true);

        var markerCount = services.Count(d =>
            d.ServiceType == typeof(LatticeReplicationServiceCollectionExtensions.ReplicationConfigAnchorMarker));

        // The second application of the anchor is a no-op: exactly one marker,
        // and the enrolment is still correct.
        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.Multiple(() =>
        {
            Assert.That(markerCount, Is.EqualTo(1));
            Assert.That(trees![LatticeSystemTreeNames.ReplicationConfig], Is.EqualTo(LatticeMergeMode.OrMap));
        });
    }

    [Test]
    public void DisabledRuntimeConfig_does_not_enrol_config_tree_or_install_anchor()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");

        var markerCount = services.Count(d =>
            d.ServiceType == typeof(LatticeReplicationServiceCollectionExtensions.ReplicationConfigAnchorMarker));

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.Multiple(() =>
        {
            Assert.That(markerCount, Is.EqualTo(0));
            Assert.That(
                trees is null || !trees.ContainsKey(LatticeSystemTreeNames.ReplicationConfig),
                Is.True,
                "sys-replication-config must not be enrolled when enableRuntimeConfig is false");
        });
    }

    [Test]
    public void BuildReplicationConfigEnrolmentMap_contains_only_the_config_tree_as_or_map()
    {
        var map = LatticeSystemTreeNames.BuildReplicationConfigEnrolmentMap();

        Assert.Multiple(() =>
        {
            Assert.That(map, Has.Count.EqualTo(1));
            Assert.That(map[LatticeSystemTreeNames.ReplicationConfig], Is.EqualTo(LatticeMergeMode.OrMap));
        });
    }
}
