using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for <see cref="LatticeReplicationServiceCollectionExtensions.ReplicateLatticeSystemTrees"/>
/// and the reserved-name map on <see cref="LatticeSystemTreeNames"/>: the reserved
/// membership/auth trees are enrolled into
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> under the correct merge
/// mode, the audit tree is opt-in, host-declared trees are preserved, and the
/// guardrail rejects enrolment when the replication add-on is not registered.
/// </summary>
[TestFixture]
public class LatticeSystemTreeReplicationExtensionsTests
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
    public void ReplicateLatticeSystemTrees_throws_when_builder_is_null()
    {
        ISiloBuilder builder = null!;

        Assert.That(
            () => builder.ReplicateLatticeSystemTrees(),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReplicateLatticeSystemTrees_returns_builder_for_fluent_chaining()
    {
        var services = ReplicationServices();
        var builder = BuilderWith(services);

        var result = builder.ReplicateLatticeSystemTrees();

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void ReplicateLatticeSystemTrees_throws_when_replication_not_registered()
    {
        var services = new ServiceCollection();
        var builder = BuilderWith(services);

        Assert.That(
            () => builder.ReplicateLatticeSystemTrees(),
            Throws.InvalidOperationException
                .With.Message.Contains("AddLatticeReplication"));
    }

    [Test]
    public void ReplicateLatticeSystemTrees_enrols_membership_and_policy_trees_lww()
    {
        var services = ReplicationServices();
        BuilderWith(services).ReplicateLatticeSystemTrees();

        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();
        var trees = options.Get("any-tree").ReplicatedTrees;

        Assert.That(trees, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(trees![LatticeSystemTreeNames.MembershipUsers], Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(trees[LatticeSystemTreeNames.MembershipGroups], Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(trees[LatticeSystemTreeNames.MembershipEdges], Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(trees[LatticeSystemTreeNames.AuthPolicy], Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public void ReplicateLatticeSystemTrees_does_not_enrol_audit_tree_by_default()
    {
        var services = ReplicationServices();
        BuilderWith(services).ReplicateLatticeSystemTrees();

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.That(trees!.ContainsKey(LatticeSystemTreeNames.AuthAudit), Is.False);
    }

    [Test]
    public void ReplicateLatticeSystemTrees_enrols_audit_tree_as_or_set_when_opted_in()
    {
        var services = ReplicationServices();
        BuilderWith(services).ReplicateLatticeSystemTrees(includeAudit: true);

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.That(trees![LatticeSystemTreeNames.AuthAudit], Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void ReplicateLatticeSystemTrees_preserves_host_declared_trees()
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

        BuilderWith(services).ReplicateLatticeSystemTrees();

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.Multiple(() =>
        {
            Assert.That(trees!["app-tree"], Is.EqualTo(LatticeMergeMode.PnCounter));
            Assert.That(trees[LatticeSystemTreeNames.AuthPolicy], Is.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public void ReplicateLatticeSystemTrees_forces_reserved_merge_mode_over_host_declaration()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IGrainFactory>());
        services.AddLogging();
        BuilderWith(services).AddLatticeReplication(o =>
        {
            o.ClusterId = "site-a";
            // Host mis-declares the policy tree under a CRDT mode; the reserved
            // enrolment must overwrite it with the correct LWW mode.
            o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                [LatticeSystemTreeNames.AuthPolicy] = LatticeMergeMode.OrSet,
            };
        });

        BuilderWith(services).ReplicateLatticeSystemTrees();

        var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()
            .Get("any-tree").ReplicatedTrees;

        Assert.That(trees![LatticeSystemTreeNames.AuthPolicy], Is.EqualTo(LatticeMergeMode.LwwRegister));
    }

    [Test]
    public void BuildEnrolmentMap_without_audit_contains_four_lww_trees()
    {
        var map = LatticeSystemTreeNames.BuildEnrolmentMap(includeAudit: false);

        Assert.Multiple(() =>
        {
            Assert.That(map, Has.Count.EqualTo(4));
            Assert.That(map.Values, Has.All.EqualTo(LatticeMergeMode.LwwRegister));
        });
    }

    [Test]
    public void BuildEnrolmentMap_with_audit_adds_or_set_audit_tree()
    {
        var map = LatticeSystemTreeNames.BuildEnrolmentMap(includeAudit: true);

        Assert.Multiple(() =>
        {
            Assert.That(map, Has.Count.EqualTo(5));
            Assert.That(map[LatticeSystemTreeNames.AuthAudit], Is.EqualTo(LatticeMergeMode.OrSet));
        });
    }
}
