using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeWalReclamation"/> transport adapter (#4195): the
/// read reaches its RPC with the tree it names, maps a denial, carries the circuit
/// credential and is one instance per circuit.
/// </summary>
[TestFixture]
public sealed class ShellWalReclamationTransportTests : ShellTransportAdapterContractTests<ILatticeWalReclamation>
{
    private const string Service = "/orleans.lattice.api.treeadmin/";

    private static readonly TreeWalReclamationReport Report = new()
    {
        TreeId = "orders",
        PinStoreReadable = true,
        PinCount = 1,
        FloorHolder = new TreeWalFloorHolder
        {
            ConsumerId = "consumer",
            LeafId = "bplusleaf/abc",
            PinOffset = 42,
            PersistedCheckpoint = -1,
            State = TreeWalFloorHolderState.NeverCheckpointed,
        },
    };

    internal override IEnumerable<ShellTransportCall<ILatticeWalReclamation>> Calls() =>
    [
        new("GetWalReclamationAsync", Service + "GetWalReclamation", (f, ct) => f.GetWalReclamationAsync("orders", ct)),
    ];

    internal override void ScriptSuccess(ShellTransportPeer peer) =>
        peer.Respond(Service + "GetWalReclamation", Report);

    [Test]
    public void The_read_validates_its_tree_without_a_call()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeWalReclamation>();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.GetWalReclamationAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }

    [Test]
    public async Task The_read_returns_the_clusters_report_with_its_wedge_verdict()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeWalReclamation>();
        ScriptSuccess(circuit.Peer);

        var report = await facade.GetWalReclamationAsync("orders");

        Assert.Multiple(() =>
        {
            Assert.That(report.FloorHolder!.LeafId, Is.EqualTo("bplusleaf/abc"));
            Assert.That(report.IsWedged, Is.True);
        });
    }
}
