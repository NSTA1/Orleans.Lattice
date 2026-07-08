using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="OptionsReplicatedTreeMembership"/>, the real
/// replicated-tree membership that projects
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> so the backup
/// package's shared-sink guard and the restore dispatcher can tell which trees
/// are replicated without the backup package depending on the replication
/// package.
/// </summary>
[TestFixture]
public class OptionsReplicatedTreeMembershipTests
{
    private static OptionsReplicatedTreeMembership Create(
        IReadOnlyDictionary<string, LatticeMergeMode>? replicatedTrees)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplicatedTrees = replicatedTrees,
        });
        return new OptionsReplicatedTreeMembership(options);
    }

    [Test]
    public void IsReplicated_configured_tree_returns_true()
    {
        var membership = Create(new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
        });

        Assert.That(membership.IsReplicated("orders"), Is.True);
    }

    [Test]
    public void IsReplicated_unconfigured_tree_returns_false()
    {
        var membership = Create(new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
        });

        Assert.That(membership.IsReplicated("inventory"), Is.False);
    }

    [Test]
    public void IsReplicated_null_map_returns_false()
    {
        var membership = Create(null);

        Assert.That(membership.IsReplicated("orders"), Is.False);
    }

    [Test]
    public void IsReplicated_null_argument_throws()
    {
        var membership = Create(null);

        Assert.That(() => membership.IsReplicated(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ReplicatedTrees_projects_every_configured_key()
    {
        var membership = Create(new Dictionary<string, LatticeMergeMode>
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
            ["inventory"] = LatticeMergeMode.OrSet,
        });

        Assert.That(membership.ReplicatedTrees, Is.EquivalentTo(new[] { "orders", "inventory" }));
    }

    [Test]
    public void ReplicatedTrees_empty_when_null_map()
    {
        var membership = Create(null);

        Assert.That(membership.ReplicatedTrees, Is.Empty);
    }

    [Test]
    public void ReplicatedTrees_empty_when_empty_map()
    {
        var membership = Create(new Dictionary<string, LatticeMergeMode>());

        Assert.That(membership.ReplicatedTrees, Is.Empty);
    }
}
