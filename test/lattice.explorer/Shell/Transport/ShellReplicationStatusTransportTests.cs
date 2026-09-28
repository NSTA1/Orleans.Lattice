using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeReplicationStatus"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellReplicationStatusTransportTests : ShellTransportAdapterContractTests<ILatticeReplicationStatus>
{
    internal override IEnumerable<ShellTransportCall<ILatticeReplicationStatus>> Calls() =>
    [
        new("GetPeerStatusAsync", "/orleans.lattice.api.replication.status/GetPeerStatus", (f, ct) => f.GetPeerStatusAsync(ReplicationPeerStatusQuery.All, ct)),
    ];

    [Test]
    public async Task A_peer_status_page_arrives_intact()
    {
        using var circuit = new ShellTransportCircuit();
        var status = circuit.Resolve<ILatticeReplicationStatus>();
        circuit.Peer.Respond("/orleans.lattice.api.replication.status/GetPeerStatus", ReplicationPeerStatusPage.Empty("east"));
        circuit.Peer.AnswerWithSuccess();

        var page = await status.GetPeerStatusAsync(ReplicationPeerStatusQuery.All);

        Assert.Multiple(() =>
        {
            Assert.That(page.LocalRegionId, Is.EqualTo("east"));
            Assert.That(page.Peers, Is.Empty);
            Assert.That(page.ContinuationToken, Is.Null);
        });
    }

    [Test]
    public void A_null_query_is_rejected_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var status = circuit.Resolve<ILatticeReplicationStatus>();

        Assert.Multiple(() =>
        {
            Assert.That(() => status.GetPeerStatusAsync(null!), Throws.ArgumentNullException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
