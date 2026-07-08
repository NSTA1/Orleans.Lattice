using NSubstitute;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for the fail-fast sink guard
/// (<see cref="LatticeBackupReplicatedSinkStartupValidator"/>): a replicated tree
/// backed by the default in-cluster sink fails fast at silo start with an
/// actionable message; a replicated tree backed by a shared external sink passes;
/// and the default no-op membership seam (a single-cluster deployment) passes even
/// with the in-cluster sink. The guard reads the replicated-tree set through the
/// backup-local <see cref="IReplicatedTreeMembership"/> seam, so it needs no
/// dependency on the replication package.
/// </summary>
[TestFixture]
public sealed class SinkGuardTests
{
    [Test]
    public void StartAsync_replicated_tree_with_in_cluster_sink_fails_fast()
    {
        var sink = new InClusterLatticeBackupSink(Substitute.For<IGrainFactory>());
        var membership = new FakeReplicatedTreeMembership("orders");
        var validator = new LatticeBackupReplicatedSinkStartupValidator(sink, membership);

        Assert.That(
            async () => await validator.StartAsync(CancellationToken.None),
            Throws.InvalidOperationException
                .With.Message.Contains("orders")
                .And.Message.Contains("shared external sink"));
    }

    [Test]
    public async Task StartAsync_replicated_tree_with_external_sink_passes()
    {
        var sink = Substitute.For<ILatticeBackupSink>();
        var membership = new FakeReplicatedTreeMembership("orders");
        var validator = new LatticeBackupReplicatedSinkStartupValidator(sink, membership);

        await validator.StartAsync(CancellationToken.None);

        Assert.Pass("A shared external sink is accepted for a replicated tree.");
    }

    [Test]
    public async Task StartAsync_single_cluster_no_op_seam_with_in_cluster_sink_passes()
    {
        var sink = new InClusterLatticeBackupSink(Substitute.For<IGrainFactory>());
        var membership = new NoReplicatedTreeMembership();
        var validator = new LatticeBackupReplicatedSinkStartupValidator(sink, membership);

        await validator.StartAsync(CancellationToken.None);

        Assert.Pass("A single-cluster deployment (nothing replicated) accepts the in-cluster sink.");
    }

    [Test]
    public void NoReplicatedTreeMembership_reports_nothing_replicated()
    {
        var membership = new NoReplicatedTreeMembership();

        Assert.Multiple(() =>
        {
            Assert.That(membership.ReplicatedTrees, Is.Empty);
            Assert.That(membership.IsReplicated("orders"), Is.False);
        });
    }

    /// <summary>A test-double membership seam that reports a fixed set of trees as replicated.</summary>
    private sealed class FakeReplicatedTreeMembership(params string[] trees) : IReplicatedTreeMembership
    {
        private readonly HashSet<string> _trees = new(trees, StringComparer.Ordinal);

        public IReadOnlyCollection<string> ReplicatedTrees => _trees;

        public bool IsReplicated(string treeId)
        {
            ArgumentNullException.ThrowIfNull(treeId);
            return _trees.Contains(treeId);
        }
    }
}
