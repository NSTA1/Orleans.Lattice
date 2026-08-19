using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication.Tests;

/// <summary>
/// Coverage for <see cref="ClusterRepoContextReplicaIdentity"/>: it authors
/// agent-memory CRDT writes under the replication cluster id so two clusters mint
/// distinct dots, and falls back to the stable local id when the cluster id is unset
/// so a partially-configured host still writes a non-empty replica id rather than
/// throwing on the write path.
/// </summary>
[TestFixture]
public sealed class ClusterRepoContextReplicaIdentityTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = clusterId });
        return monitor;
    }

    [Test]
    public void ReplicaId_is_the_cluster_id_when_set()
    {
        var identity = new ClusterRepoContextReplicaIdentity(Monitor("cluster-west"));

        Assert.That(identity.ReplicaId, Is.EqualTo("cluster-west"));
    }

    [Test]
    [TestCase("")]
    [TestCase("   ")]
    public void ReplicaId_falls_back_to_the_local_id_when_the_cluster_id_is_blank(string clusterId)
    {
        var identity = new ClusterRepoContextReplicaIdentity(Monitor(clusterId));

        Assert.That(identity.ReplicaId, Is.EqualTo(LocalRepoContextReplicaIdentity.LocalReplicaId));
    }
}
