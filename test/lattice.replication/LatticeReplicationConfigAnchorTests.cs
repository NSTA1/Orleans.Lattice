using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for
/// <see cref="LatticeReplicationServiceCollectionExtensions.ReplicateLatticeReplicationConfig"/>
/// and the config-tree enrolment on <see cref="LatticeSystemTreeNames"/>: the
/// self-referential <c>sys-replication-config</c> tree is enrolled under the
/// fixed <see cref="LatticeMergeMode.OrMap"/> mode, the OR-Map shape for the
/// per-tree config record is registered, host-declared trees are preserved, the
/// add-on is idempotent, and the guardrail rejects enrolment when the
/// replication add-on is not registered.
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

    private static IServiceCollection ReplicationServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o => o.ClusterId = "site-a");
        return services;
    }

    [Test]
    public void ReplicateLatticeReplicationConfig_throws_when_builder_is_null()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.ReplicateLatticeReplicationConfig(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReplicateLatticeReplicationConfig_returns_builder_for_fluent_chaining()
    {
        var services = ReplicationServices();
        var builder = BuilderWith(services);

        var result = builder.ReplicateLatticeReplicationConfig();

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void ReplicateLatticeReplicationConfig_throws_when_replication_not_registered()
    {
        var services = new ServiceCollection();
        var builder = BuilderWith(services);

        Assert.That(
            () => builder.ReplicateLatticeReplicationConfig(),
            Throws.InvalidOperationException
                .With.Message.Contains("AddLatticeReplication"));
    }

    [Test]
    public void ReplicateLatticeReplicationConfig_enrols_config_tree_as_or_map()
    {
        var services = ReplicationServices();
        BuilderWith(services).ReplicateLatticeReplicationConfig();

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
    public void ReplicateLatticeReplicationConfig_preserves_host_declared_trees()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o =>
        {
            o.ClusterId = "site-a";
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                ["app-tree"] = LatticeMergeMode.PnCounter,
            };
        });

        BuilderWith(services).ReplicateLatticeReplicationConfig();

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
    public void ReplicateLatticeReplicationConfig_forces_or_map_mode_over_host_declaration()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o =>
        {
            o.ClusterId = "site-a";
            // Host mis-declares the config tree under a different mode; the
            // reserved enrolment must overwrite it with the fixed OrMap mode.
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                [LatticeSystemTreeNames.ReplicationConfig] = LatticeMergeMode.LwwRegister,
            };
        });

        BuilderWith(services).ReplicateLatticeReplicationConfig();

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.That(trees![LatticeSystemTreeNames.ReplicationConfig], Is.EqualTo(LatticeMergeMode.OrMap));
    }

    [Test]
    public async Task ReplicateLatticeReplicationConfig_registers_the_config_tree_or_map_shape()
    {
        var services = ReplicationServices();
        BuilderWith(services).ReplicateLatticeReplicationConfig();

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
    public void ReplicateLatticeReplicationConfig_is_idempotent()
    {
        var services = ReplicationServices();
        var builder = BuilderWith(services);

        builder.ReplicateLatticeReplicationConfig();
        builder.ReplicateLatticeReplicationConfig();

        var markerCount = services.Count(d =>
            d.ServiceType == typeof(LatticeReplicationServiceCollectionExtensions.ReplicationConfigAnchorMarker));

        // The second call is a no-op: exactly one marker, and the enrolment is
        // still correct.
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
