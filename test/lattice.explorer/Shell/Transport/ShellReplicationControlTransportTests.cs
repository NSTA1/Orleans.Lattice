using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeReplicationControl"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellReplicationControlTransportTests : ShellTransportAdapterContractTests<ILatticeReplicationControl>
{
    private const string Service = "/orleans.lattice.api.replication/";

    internal override IEnumerable<ShellTransportCall<ILatticeReplicationControl>> Calls() =>
    [
        new("EnableReplicationAsync", Service + "EnableReplication", (f, ct) => f.EnableReplicationAsync("orders", LatticeMergeMode.LwwRegister, "west", ct)),
        new("DisableReplicationAsync", Service + "DisableReplication", (f, ct) => f.DisableReplicationAsync("orders", ct)),
        new("GetReplicationConfigAsync", Service + "GetReplicationConfig", (f, ct) => f.GetReplicationConfigAsync(ct)),
    ];

    [Test]
    public void A_mode_change_refusal_maps_to_invalid_operation()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeReplicationControl>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.FailedPrecondition, "disable first");

        Assert.That(
            () => control.EnableReplicationAsync("orders", LatticeMergeMode.OrSet),
            Throws.InvalidOperationException.With.Message.EqualTo("disable first"));
    }

    [Test]
    public void An_empty_tree_id_is_rejected_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeReplicationControl>();

        Assert.Multiple(() =>
        {
            Assert.That(() => control.EnableReplicationAsync(string.Empty, LatticeMergeMode.LwwRegister), Throws.ArgumentException);
            Assert.That(() => control.DisableReplicationAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
