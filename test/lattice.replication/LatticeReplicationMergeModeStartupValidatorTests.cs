using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeReplicationMergeModeStartupValidator"/>,
/// the startup guard that fails fast when a flag-mode replicated tree has no
/// configured local replica id.
/// </summary>
[TestFixture]
public class LatticeReplicationMergeModeStartupValidatorTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static ILatticeReplicationContext Context(string localReplicaId)
    {
        var ctx = Substitute.For<ILatticeReplicationContext>();
        ctx.LocalReplicaId.Returns(localReplicaId);
        return ctx;
    }

    [Test]
    public void Start_throws_when_flag_tree_has_no_replica_id()
    {
        var validator = new LatticeReplicationMergeModeStartupValidator(
            Context(string.Empty),
            Monitor(new LatticeReplicationOptions
            {
                ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
                {
                    ["tag-orders"] = LatticeMergeMode.OrFlag,
                },
            }));

        Assert.That(() => validator.StartAsync(CancellationToken.None), Throws.InvalidOperationException);
    }

    [Test]
    public void Start_throws_for_rw_flag_mode_too()
    {
        var validator = new LatticeReplicationMergeModeStartupValidator(
            Context(string.Empty),
            Monitor(new LatticeReplicationOptions
            {
                ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
                {
                    ["tag-orders"] = LatticeMergeMode.RwFlag,
                },
            }));

        Assert.That(() => validator.StartAsync(CancellationToken.None), Throws.InvalidOperationException);
    }

    [Test]
    public void Start_succeeds_when_flag_tree_has_a_replica_id()
    {
        var validator = new LatticeReplicationMergeModeStartupValidator(
            Context("site-a"),
            Monitor(new LatticeReplicationOptions
            {
                ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
                {
                    ["tag-orders"] = LatticeMergeMode.OrFlag,
                },
            }));

        Assert.That(() => validator.StartAsync(CancellationToken.None), Throws.Nothing);
    }

    [Test]
    public void Start_succeeds_for_non_flag_trees_without_replica_id()
    {
        var validator = new LatticeReplicationMergeModeStartupValidator(
            Context(string.Empty),
            Monitor(new LatticeReplicationOptions
            {
                ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
                {
                    ["orders"] = LatticeMergeMode.LwwRegister,
                },
            }));

        Assert.That(() => validator.StartAsync(CancellationToken.None), Throws.Nothing);
    }

    [Test]
    public void Start_succeeds_when_no_replicated_trees_are_declared()
    {
        var validator = new LatticeReplicationMergeModeStartupValidator(
            Context(string.Empty),
            Monitor(new LatticeReplicationOptions { ReplicatedTrees = null }));

        Assert.That(() => validator.StartAsync(CancellationToken.None), Throws.Nothing);
    }

    [Test]
    public void Stop_is_a_no_op()
    {
        var validator = new LatticeReplicationMergeModeStartupValidator(
            Context("site-a"),
            Monitor(new LatticeReplicationOptions()));

        Assert.That(() => validator.StopAsync(CancellationToken.None), Throws.Nothing);
    }
}
