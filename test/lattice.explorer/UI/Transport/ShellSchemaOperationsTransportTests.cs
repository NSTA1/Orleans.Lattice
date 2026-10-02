using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeSchemaOperations"/> transport adapter (#4123):
/// every accept-then-poll verb reaches its RPC, maps a denial, carries the
/// circuit credential and is one instance per circuit.
/// </summary>
[TestFixture]
public sealed class ShellSchemaOperationsTransportTests : ShellTransportAdapterContractTests<ILatticeSchemaOperations>
{
    private const string Service = "/orleans.lattice.api.schema/";

    private static readonly LatticeSchemaPolicy Policy = new([]);

    private static readonly LatticeOperationHandle Handle = new()
    {
        OperationId = "op-1",
        Kind = SchemaOperationKinds.Remediation,
        Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
        Created = true,
    };

    internal override IEnumerable<ShellTransportCall<ILatticeSchemaOperations>> Calls() =>
    [
        new("StartRemediationAsync", Service + "StartRemediation", (f, ct) => f.StartRemediationAsync("orders", LatticeValueTransform.DropMember("legacy"), Policy, "op-1", ct)),
        new("StartMigrationAsync", Service + "StartMigration", (f, ct) => f.StartMigrationAsync("orders", null, ct)),
        new("StartAdvanceAndMigrateAsync", Service + "StartAdvanceAndMigrate", (f, ct) => f.StartAdvanceAndMigrateAsync("orders", 2, null, ct)),
        new("GetOperationStatusAsync", Service + "GetSchemaOperationStatus", (f, ct) => f.GetOperationStatusAsync("op-1", ct)),
        new("ListOperationsAsync", Service + "ListSchemaOperations", (f, ct) => f.ListOperationsAsync(new LatticeOperationListRequest(), ct)),
        new("CancelOperationAsync", Service + "CancelSchemaOperation", (f, ct) => f.CancelOperationAsync("op-1", ct)),
    ];

    internal override void ScriptSuccess(ShellTransportPeer peer)
    {
        foreach (var start in new[] { "StartRemediation", "StartMigration", "StartAdvanceAndMigrate" })
        {
            peer.Respond(Service + start, Handle);
        }

        peer.Respond(Service + "GetSchemaOperationStatus", new SchemaOperationStatusResponse());
        peer.Respond(Service + "CancelSchemaOperation", new SchemaOperationStatusResponse());
        peer.Respond(Service + "ListSchemaOperations", new LatticeOperationPage());
    }

    [Test]
    public void The_verbs_validate_their_arguments_without_a_call()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeSchemaOperations>();

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.StartRemediationAsync(string.Empty, LatticeValueTransform.DropMember("x"), Policy), Throws.ArgumentException);
            Assert.That(() => facade.StartRemediationAsync("orders", LatticeValueTransform.DropMember("x"), null!), Throws.ArgumentNullException);
            Assert.That(() => facade.StartMigrationAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => facade.StartAdvanceAndMigrateAsync(string.Empty, 2), Throws.ArgumentException);
            Assert.That(() => facade.GetOperationStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => facade.ListOperationsAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => facade.CancelOperationAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }

    [Test]
    public async Task A_start_returns_the_clusters_handle_and_a_missing_status_reads_as_null()
    {
        using var circuit = new ShellTransportCircuit();
        var facade = circuit.Resolve<ILatticeSchemaOperations>();
        ScriptSuccess(circuit.Peer);

        var handle = await facade.StartRemediationAsync("orders", LatticeValueTransform.DropMember("legacy"), Policy, "op-1");
        var status = await facade.GetOperationStatusAsync("op-1");

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { "orders" }));
            Assert.That(status, Is.Null);
        });
    }
}
