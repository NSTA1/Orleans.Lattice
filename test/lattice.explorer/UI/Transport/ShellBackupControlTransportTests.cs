using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
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

    private static readonly LatticeBackupSetScheduleRequest SetRequest =
        LatticeBackupSetScheduleRequest.ForTrees("pics", [Scope], TimeSpan.FromHours(24), 7, null);

    private static readonly BackupSetScheduleStatus SetStatus = new(
        "pics", SetRequest, [Scope], true, null, false, DateTimeOffset.UnixEpoch, null, null, BackupScopeRunOutcome.None, null, 0, [], []);

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
        new("GetBackupHealthConfigAsync", Service + "GetBackupHealthConfig", (f, ct) => f.GetBackupHealthConfigAsync("b1", ct)),
        new("ScheduleBackupSetAsync", Service + "ScheduleBackupSet", (f, ct) => f.ScheduleBackupSetAsync(SetRequest, ct)),
        new("CancelBackupSetScheduleAsync", Service + "CancelBackupSetSchedule", (f, ct) => f.CancelBackupSetScheduleAsync("pics", ct)),
        new("RemoveBackupSetScheduleAsync", Service + "RemoveBackupSetSchedule", (f, ct) => f.RemoveBackupSetScheduleAsync("pics", true, ct)),
        new("GetBackupSetScheduleStatusAsync", Service + "GetBackupSetScheduleStatus", (f, ct) => f.GetBackupSetScheduleStatusAsync("pics", ct)),
        new("ListBackupSetSchedulesAsync", Service + "ListBackupSetSchedules", (f, ct) => f.ListBackupSetSchedulesAsync(ct)),
        new("RunBackupSetScheduleNowAsync", Service + "RunBackupSetScheduleNow", (f, ct) => f.RunBackupSetScheduleNowAsync("pics", ct)),
        new("GetBackupSetScheduleProgressAsync", Service + "GetBackupSetScheduleProgress", (f, ct) => f.GetBackupSetScheduleProgressAsync("pics", ct)),
    ];

    internal override void ScriptSuccess(ShellTransportPeer peer)
    {
        peer.Respond(Service + "ScheduleBackupSet", new BackupSetScheduleStatusResponse { Found = true, Status = SetStatus });
        peer.Respond(Service + "GetBackupSetScheduleProgress", new BackupSetScheduleProgressResponse { Found = true });
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
    public async Task The_backup_set_schedule_verbs_carry_their_answers_and_requests()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeBackupControl>();
        circuit.Peer.Respond(Service + "ScheduleBackupSet", new BackupSetScheduleStatusResponse { Found = true, Status = SetStatus });
        circuit.Peer.Respond(Service + "ListBackupSetSchedules", new BackupSetScheduleListResponse { Schedules = [SetStatus] });
        circuit.Peer.Respond(Service + "RemoveBackupSetSchedule", new BackupSetScheduleActionResponse { Result = true });
        circuit.Peer.AnswerWithSuccess();

        var scheduled = await control.ScheduleBackupSetAsync(SetRequest);
        var listed = await control.ListBackupSetSchedulesAsync();
        var removed = await control.RemoveBackupSetScheduleAsync("pics", deleteRetainedSets: true);

        Assert.Multiple(() =>
        {
            Assert.That(scheduled.Name, Is.EqualTo("pics"));
            var sent = circuit.Peer.Decode<BackupSetScheduleRequestMessage>(circuit.Peer.Requests[0]).Request!;
            Assert.That((sent.Name, sent.Scopes.Single().TreeId, sent.RetentionKeepLast), Is.EqualTo(("pics", "orders", (int?)7)));
            Assert.That(listed.Select(status => status.Name), Is.EqualTo(new[] { "pics" }));
            Assert.That(removed, Is.True);
            var remove = circuit.Peer.Decode<BackupSetScheduleRemoveRequestMessage>(circuit.Peer.Requests[2]);
            Assert.That((remove.Name, remove.DeleteRetainedSets), Is.EqualTo(("pics", true)));
        });
    }

    [Test]
    public void A_cluster_without_backup_set_schedules_reads_as_not_supported()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeBackupControl>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.Unimplemented, "no backup-set schedules");

        Assert.That(() => control.ListBackupSetSchedulesAsync(), Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void Running_an_absent_backup_set_schedule_reads_as_not_found()
    {
        using var circuit = new ShellTransportCircuit();
        var control = circuit.Resolve<ILatticeBackupControl>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.NotFound, "no such schedule");

        Assert.That(() => control.RunBackupSetScheduleNowAsync("pics"), Throws.InstanceOf<KeyNotFoundException>());
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
            Assert.That(() => control.GetBackupHealthConfigAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.ScheduleBackupSetAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => control.CancelBackupSetScheduleAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.RemoveBackupSetScheduleAsync(string.Empty, false), Throws.ArgumentException);
            Assert.That(() => control.GetBackupSetScheduleStatusAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.RunBackupSetScheduleNowAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => control.GetBackupSetScheduleProgressAsync(string.Empty), Throws.ArgumentException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
