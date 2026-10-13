using Grpc.Core;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Explorer's peer decommissioning facade through the real gRPC client stack.</summary>
[TestFixture]
public sealed class ShellReplicationPeerAdminTransportTests : ShellTransportAdapterContractTests<ILatticeReplicationPeerAdmin>
{
    private const string Rpc = "/orleans.lattice.api.replication/DecommissionPeer";

    internal override IEnumerable<ShellTransportCall<ILatticeReplicationPeerAdmin>> Calls() =>
    [
        new("DecommissionPeerAsync", Rpc, (f, ct) => f.DecommissionPeerAsync("west", ct)),
    ];

    [TestCase(false, 7)]
    [TestCase(true, 0)]
    public async Task DecommissionPeerAsync_forwards_the_peer_and_preserves_the_result(bool alreadyDecommissioned, int treeCount)
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeReplicationPeerAdmin>();
        circuit.Peer.Respond(Rpc, new ReplicationDecommissionPeerResponse
        {
            PeerClusterId = "west",
            TreeCount = treeCount,
            AlreadyDecommissioned = alreadyDecommissioned,
        });

        var result = await admin.DecommissionPeerAsync("west");

        Assert.Multiple(() =>
        {
            Assert.That(circuit.Peer.Requests, Has.Count.EqualTo(1));
            Assert.That(circuit.Peer.Requests[0].Path, Is.EqualTo(Rpc));
            Assert.That(circuit.Peer.Decode<ReplicationDecommissionPeerRequestMessage>(circuit.Peer.Requests[0]).PeerClusterId,
                Is.EqualTo("west"));
            Assert.That(result, Is.EqualTo(new ReplicationDecommissionPeerResult("west", treeCount, alreadyDecommissioned)));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void DecommissionPeerAsync_invalid_peer_is_rejected_before_any_rpc(string? peer)
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeReplicationPeerAdmin>();

        Assert.That(() => admin.DecommissionPeerAsync(peer!), Throws.InstanceOf<ArgumentException>());
        Assert.That(circuit.Peer.Requests, Is.Empty);
    }

    [Test]
    public void DecommissionPeerAsync_cancelled_call_does_not_reach_the_peer()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeReplicationPeerAdmin>();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.That(() => admin.DecommissionPeerAsync("west", cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(circuit.Peer.Requests, Is.Empty);
    }

    [Test]
    public void DecommissionPeerAsync_failed_precondition_is_not_reported_as_success()
    {
        using var circuit = new ShellTransportCircuit();
        var admin = circuit.Resolve<ILatticeReplicationPeerAdmin>();
        circuit.Peer.AnswerWith(StatusCode.FailedPrecondition, "peer is still required");

        Assert.That(() => admin.DecommissionPeerAsync("west"),
            Throws.InvalidOperationException.With.Message.EqualTo("peer is still required"));
    }
}
