using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>The Shell's <see cref="ILatticeSchemaControl"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellSchemaControlTransportTests : ShellTransportAdapterContractTests<ILatticeSchemaControl>
{
    private const string Service = "/orleans.lattice.api.schema/";

    private static readonly LatticeSchemaPolicy Policy = new([]);

    internal override IEnumerable<ShellTransportCall<ILatticeSchemaControl>> Calls() =>
    [
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
        new("AdvanceAndMigrateAsync", Service + "AdvanceAndMigrate", (f, ct) => f.AdvanceAndMigrateAsync("orders", 2, ct)),
        new("MigrateToTargetVersionAsync", Service + "MigrateToTargetVersion", (f, ct) => f.MigrateToTargetVersionAsync("orders", ct)),
        new("ClearVersionConfigAsync", Service + "ClearVersionConfig", (f, ct) => f.ClearVersionConfigAsync("orders", ct)),
        new("RemediateAsync", Service + "Remediate", (f, ct) => f.RemediateAsync("orders", LatticeValueTransform.DropMember("legacy"), Policy, ct)),
        new("GetRemediationStatusAsync", Service + "GetRemediationStatus", (f, ct) => f.GetRemediationStatusAsync("orders", ct)),
        new("ScanComplianceAsync", Service + "ScanCompliance", (f, ct) => f.ScanComplianceAsync("orders", ct)),
        new("ProbeCapabilitiesAsync", Service + "ProbeCapabilities", (f, ct) => f.ProbeCapabilitiesAsync("orders", ct)),
    ];

    [Test]
    public void A_failed_precondition_maps_to_invalid_operation()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeSchemaControl>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.FailedPrecondition, "the tree is unversioned");

        Assert.That(
            () => control.MigrateToTargetVersionAsync("orders"),
            Throws.InvalidOperationException.With.Message.EqualTo("the tree is unversioned"));
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeSchemaControl>();

        Assert.Multiple(() =>
        {
            Assert.That(() => control.SetPolicyAsync("orders", null!), Throws.ArgumentNullException);
            Assert.That(() => control.SetPolicyAsync(string.Empty, Policy), Throws.ArgumentException);
            Assert.That(() => control.ClearPolicyAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.GetPolicyAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ListDeadLettersAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.CountDeadLettersAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.SetVersionConfigAsync(string.Empty, default), Throws.ArgumentException);
            Assert.That(() => control.GetVersionConfigAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.AdvanceTargetVersionAsync(string.Empty, 1), Throws.ArgumentException);
            Assert.That(() => control.AdvanceAndMigrateAsync(string.Empty, 1), Throws.ArgumentException);
            Assert.That(() => control.MigrateToTargetVersionAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ClearVersionConfigAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.RemediateAsync("orders", default, null!), Throws.ArgumentNullException);
            Assert.That(() => control.GetRemediationStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ScanComplianceAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ProbeCapabilitiesAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
