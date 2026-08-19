using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication.Tests;

/// <summary>
/// Coverage for <see cref="RepoContextReplicationServiceCollectionExtensions.EnableRepoContextMultiCluster"/>:
/// the helper enrols every repository-context tree into
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> under the correct merge
/// mode, force-pins the vector-membership tree to the add-wins
/// <see cref="LatticeMergeMode.OrFlag"/> even over a host mis-declaration, defaults
/// every other tree to <see cref="LatticeMergeMode.LwwRegister"/> while respecting a
/// deliberate host override, forwards the runtime-config flag, and guards its
/// arguments. Assertions read back the <b>resolved</b> map through the options
/// pipeline (helper plus <c>PostConfigureAll</c>), never the pre-merge input.
/// </summary>
[TestFixture]
public class RepoContextReplicationServiceCollectionExtensionsTests
{
    private static ISiloBuilder BuilderWith(IServiceCollection services)
    {
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        return builder;
    }

    private static IServiceCollection BaseServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        return services;
    }

    private static IReadOnlyDictionary<string, LatticeMergeMode> ResolveTrees(IServiceCollection services)
    {
        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;
        Assert.That(trees, Is.Not.Null);
        return trees!;
    }

    [Test]
    public void EnableRepoContextMultiCluster_throws_when_builder_is_null()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.EnableRepoContextMultiCluster(_ => { }),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EnableRepoContextMultiCluster_throws_when_configure_is_null()
    {
        var builder = BuilderWith(BaseServices());

        Assert.That(
            () => builder.EnableRepoContextMultiCluster(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EnableRepoContextMultiCluster_returns_builder_for_fluent_chaining()
    {
        var builder = BuilderWith(BaseServices());

        var result = builder.EnableRepoContextMultiCluster(o => o.ClusterId = "cluster-a");

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void EnableRepoContextMultiCluster_registers_the_replication_engine()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o => o.ClusterId = "cluster-a");

        // AddLatticeReplication registers the receiver-side applier marker; its
        // presence is the guardrail the sibling ReplicateLatticeSystemTrees helper
        // checks for. The helper wires it itself, so the pipeline is complete.
        Assert.That(
            services.Any(d => d.ServiceType.Name == "ReplicationApplier"),
            Is.True);
    }

    [Test]
    public void EnableRepoContextMultiCluster_pins_vector_membership_to_or_flag()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o => o.ClusterId = "cluster-a");

        var trees = ResolveTrees(services);

        Assert.That(trees[RepoContextTrees.VectorMembership], Is.EqualTo(LatticeMergeMode.OrFlag));
    }

    [Test]
    public void EnableRepoContextMultiCluster_enrols_every_replicable_repo_context_tree()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o => o.ClusterId = "cluster-a");

        var trees = ResolveTrees(services);

        Assert.That(trees.Keys, Is.EquivalentTo(RepoContextTrees.All));
    }

    [Test]
    public void EnableRepoContextMultiCluster_defaults_non_membership_trees_to_lww()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o => o.ClusterId = "cluster-a");

        var trees = ResolveTrees(services);

        Assert.Multiple(() =>
        {
            foreach (var tree in RepoContextTrees.All)
            {
                if (tree == RepoContextTrees.VectorMembership)
                {
                    continue;
                }

                Assert.That(trees[tree], Is.EqualTo(LatticeMergeMode.LwwRegister), tree);
            }
        });
    }

    [Test]
    public void EnableRepoContextMultiCluster_respects_host_override_on_non_membership_tree()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o =>
        {
            o.ClusterId = "cluster-a";
            // A deployment with a single authoritative writer per key may pick a
            // different mode for a non-membership tree; the helper must not clobber it.
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                [RepoContextTrees.Structural] = LatticeMergeMode.OrSet,
            };
        });

        var trees = ResolveTrees(services);

        Assert.That(trees[RepoContextTrees.Structural], Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void EnableRepoContextMultiCluster_forces_membership_or_flag_over_host_lww_declaration()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o =>
        {
            o.ClusterId = "cluster-a";
            // Host mis-declares membership as LWW; the pin must overwrite it, or
            // active-active convergence would silently drop an embedding.
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                [RepoContextTrees.VectorMembership] = LatticeMergeMode.LwwRegister,
            };
        });

        var trees = ResolveTrees(services);

        Assert.That(trees[RepoContextTrees.VectorMembership], Is.EqualTo(LatticeMergeMode.OrFlag));
    }

    [Test]
    public void EnableRepoContextMultiCluster_preserves_unrelated_host_declared_tree()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o =>
        {
            o.ClusterId = "cluster-a";
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                ["app-tree"] = LatticeMergeMode.PnCounter,
            };
        });

        var trees = ResolveTrees(services);

        Assert.Multiple(() =>
        {
            Assert.That(trees["app-tree"], Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(trees[RepoContextTrees.VectorMembership], Is.EqualTo(LatticeMergeMode.OrFlag));
        });
    }

    [Test]
    public void EnableRepoContextMultiCluster_without_runtime_config_enrols_only_repo_context_trees()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(o => o.ClusterId = "cluster-a");

        var trees = ResolveTrees(services);

        Assert.That(trees, Has.Count.EqualTo(RepoContextTrees.All.Count));
    }

    [Test]
    public void EnableRepoContextMultiCluster_with_runtime_config_enrols_additional_control_tree()
    {
        var services = BaseServices();
        BuilderWith(services).EnableRepoContextMultiCluster(
            o => o.ClusterId = "cluster-a",
            enableRuntimeConfig: true);

        var trees = ResolveTrees(services);

        // The runtime-config overload additionally enrols the reserved
        // replication-configuration control-plane tree, so the resolved map is a
        // strict superset of the repository-context set (still with membership pinned).
        Assert.Multiple(() =>
        {
            Assert.That(trees.Keys, Is.SupersetOf(RepoContextTrees.All));
            Assert.That(trees, Has.Count.GreaterThan(RepoContextTrees.All.Count));
            Assert.That(trees[RepoContextTrees.VectorMembership], Is.EqualTo(LatticeMergeMode.OrFlag));
        });
    }
}
