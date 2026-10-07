using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeBackupControl"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellBackupControlTransportTests : ShellTransportAdapterContractTests<ILatticeBackupControl>
{
    private const string Service = "/orleans.lattice.api.backup/";

    private static readonly BackupScopeSelector Scope = BackupScopeSelector.WholeTree("orders");

    private static readonly LatticeRestoreResult Restore =
        new("b1", "orders", LatticeRestoreMode.InPlace, "op-1", ["b1"], 3);

    internal override IEnumerable<ShellTransportCall<ILatticeBackupControl>> Calls() =>
    [
        new("ScheduleBackupAsync", Service + "ScheduleBackup", (f, ct) => f.ScheduleBackupAsync(new LatticeBackupScheduleRequest(Scope, false, TimeSpan.FromHours(1)), ct)),
        new("CancelScheduleAsync", Service + "CancelSchedule", (f, ct) => f.CancelScheduleAsync(Scope, true, ct)),
        new("ListBackupsAsync", Service + "ListBackups", (f, ct) => f.ListBackupsAsync(new BackupCatalogRequest(), ct)),
        new("StreamBackupsAsync", Service + "StreamBackups", async (f, ct) =>
        {
            await foreach (var _ in f.StreamBackupsAsync(ct))
            {
            }
        }),
        new("DescribeBackupAsync", Service + "DescribeBackup", (f, ct) => f.DescribeBackupAsync("b1", ct)),
        new("DeleteBackupAsync", Service + "DeleteBackup", (f, ct) => f.DeleteBackupAsync("b1", ct)),
        new("RevertRestoreAsync", Service + "RevertRestore", (f, ct) => f.RevertRestoreAsync(Restore, ct)),
        new("ExportArtifactAsync", Service + "ExportArtifact", async (f, ct) =>
        {
            await foreach (var _ in f.ExportArtifactAsync("b1", "a1", ct))
            {
            }
        }),
        new("GetScopeStatusAsync", Service + "GetScopeStatus", (f, ct) => f.GetScopeStatusAsync(Scope, ct)),
        new("ProbeCapabilitiesAsync", Service + "ProbeCapabilities", (f, ct) => f.ProbeCapabilitiesAsync(Scope, ct)),
        new("IsHealthMonitoringAvailableAsync", Service + "IsHealthMonitoringAvailable", (f, ct) => f.IsHealthMonitoringAvailableAsync(ct)),
        new("GetBackupHealthAsync", Service + "GetBackupHealth", (f, ct) => f.GetBackupHealthAsync("b1", ct)),
        new("ConfigureBackupHealthAsync", Service + "ConfigureBackupHealth", (f, ct) => f.ConfigureBackupHealthAsync("b1", new BackupHealthConfig(true, TimeSpan.FromHours(1)), ct)),
    ];

    internal override void ScriptSuccess(ShellTransportPeer peer)
    {
    }

    [Test]
    public void The_verbs_the_binding_does_not_serve_fail_as_not_supported_without_a_call()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeBackupControl>();

        Assert.Multiple(() =>
        {
            Assert.That(() => control.GetInventoryAsync(), Throws.InstanceOf<NotSupportedException>());
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }

    [Test]
    public void A_stream_fault_maps_like_a_unary_fault()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeBackupControl>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.NotFound, "no such backup");

        Assert.That(
            async () =>
            {
                await foreach (var _ in control.ExportArtifactAsync("b1", "a1"))
                {
                }
            },
            Throws.InstanceOf<KeyNotFoundException>().With.Message.EqualTo("no such backup"));
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeBackupControl>();

        Assert.Multiple(() =>
        {
            Assert.That(() => control.ScheduleBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => control.CancelScheduleAsync(null!, false), Throws.ArgumentNullException);
            Assert.That(() => control.ListBackupsAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => control.DescribeBackupAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.DeleteBackupAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.RevertRestoreAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => control.ExportArtifactAsync("b1", string.Empty), Throws.ArgumentException);
            Assert.That(() => control.GetScopeStatusAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => control.ProbeCapabilitiesAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => control.GetBackupHealthAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ConfigureBackupHealthAsync("b1", null!), Throws.ArgumentNullException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
