using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeSchemaControl"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellSchemaControlTransportTests : ShellTransportAdapterContractTests<ILatticeSchemaControl>
{
    private const string Service = "/orleans.lattice.api.schema/";

    private static readonly LatticeSchemaPolicy Policy = new([]);

    internal override IEnumerable<ShellTransportCall<ILatticeSchemaControl>> Calls() =>
    [
        new("GetRegistryAsync", Service + "GetRegistry", (f, ct) => f.GetRegistryAsync("orders", ct)),
        new("RegisterSchemaAsync", Service + "RegisterSchema", (f, ct) => f.RegisterSchemaAsync("orders", new LatticeSchemaRuntimeRegistration(), ct)),
        new("SetPolicyAsync", Service + "SetPolicy", (f, ct) => f.SetPolicyAsync("orders", Policy, ct)),
        new("ClearPolicyAsync", Service + "ClearPolicy", (f, ct) => f.ClearPolicyAsync("orders", ct)),
        new("GetPolicyAsync", Service + "GetPolicy", (f, ct) => f.GetPolicyAsync("orders", ct)),
        new("ListDeadLettersAsync", Service + "StreamDeadLetters", async (f, ct) =>
        {
            await foreach (var _ in f.ListDeadLettersAsync("orders", ct))
            {
            }
        }),
        new("CountDeadLettersAsync", Service + "CountDeadLetters", (f, ct) => f.CountDeadLettersAsync("orders", ct)),
        new("SetVersionConfigAsync", Service + "SetVersionConfig", (f, ct) => f.SetVersionConfigAsync("orders", new LatticeSchemaVersionConfig(1, 1), ct)),
        new("GetVersionConfigAsync", Service + "GetVersionConfig", (f, ct) => f.GetVersionConfigAsync("orders", ct)),
        new("AdvanceTargetVersionAsync", Service + "AdvanceTargetVersion", (f, ct) => f.AdvanceTargetVersionAsync("orders", 2, ct)),
        new("ClearVersionConfigAsync", Service + "ClearVersionConfig", (f, ct) => f.ClearVersionConfigAsync("orders", ct)),
        new("GetRemediationStatusAsync", Service + "GetRemediationStatus", (f, ct) => f.GetRemediationStatusAsync("orders", ct)),
        new("ProbeCapabilitiesAsync", Service + "ProbeCapabilities", (f, ct) => f.ProbeCapabilitiesAsync("orders", ct)),
    ];

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeSchemaControl>();

        Assert.Multiple(() =>
        {
            Assert.That(() => control.GetRegistryAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.RegisterSchemaAsync(string.Empty, new LatticeSchemaRuntimeRegistration()), Throws.ArgumentException);
            Assert.That(() => control.RegisterSchemaAsync("orders", null!), Throws.ArgumentNullException);
            Assert.That(() => control.SetPolicyAsync("orders", null!), Throws.ArgumentNullException);
            Assert.That(() => control.SetPolicyAsync(string.Empty, Policy), Throws.ArgumentException);
            Assert.That(() => control.ClearPolicyAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.GetPolicyAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ListDeadLettersAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.CountDeadLettersAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.SetVersionConfigAsync(string.Empty, default), Throws.ArgumentException);
            Assert.That(() => control.GetVersionConfigAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.AdvanceTargetVersionAsync(string.Empty, 1), Throws.ArgumentException);
            Assert.That(() => control.ClearVersionConfigAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.GetRemediationStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ProbeCapabilitiesAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }

    [Test]
    public async Task Registry_round_trip_preserves_the_tree_descriptors_hops_and_host_transform_ids()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeSchemaControl>();
        var registration = new LatticeSchemaRuntimeRegistration
        {
            Descriptors = [new(7, 1, "Order v1"), new(7, 2, "Order v2")],
            Upcasters = [new() { SchemaId = 7, FromVersion = 1, ToVersion = 2, TransformId = "order-upgrade" }],
            SupportedTransformIds = ["order-upgrade"],
        };
        circuit.Peer.Respond(Service + "GetRegistry", registration);
        circuit.Peer.AnswerWithSuccess();

        await control.RegisterSchemaAsync("orders", registration);
        var read = await control.GetRegistryAsync("orders");

        var writeRequest = circuit.Peer.Decode<SchemaRegistryRequest>(circuit.Peer.Requests[0]);
        var readRequest = circuit.Peer.Decode<SchemaTreeRequest>(circuit.Peer.Requests[1]);
        Assert.Multiple(() =>
        {
            Assert.That(writeRequest.TreeId, Is.EqualTo("orders"));
            Assert.That(writeRequest.Registration.Descriptors, Is.EqualTo(registration.Descriptors));
            Assert.That(writeRequest.Registration.Upcasters, Is.EqualTo(registration.Upcasters));
            Assert.That(writeRequest.Registration.SupportedTransformIds, Is.EqualTo(new[] { "order-upgrade" }));
            Assert.That(readRequest.TreeId, Is.EqualTo("orders"));
            Assert.That(read.Descriptors, Is.EqualTo(registration.Descriptors));
            Assert.That(read.Upcasters, Is.EqualTo(registration.Upcasters));
            Assert.That(read.SupportedTransformIds, Is.EqualTo(new[] { "order-upgrade" }));
        });
    }
}
