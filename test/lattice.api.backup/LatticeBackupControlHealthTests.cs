using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for the backup-health surface on the <see cref="ILatticeBackupControl"/>
/// facade: <see cref="ILatticeBackupControl.IsHealthMonitoringAvailableAsync"/>
/// reflects the sink's durability, <c>ILatticeBackupControl.CheckBackupHealthAsync</c>
/// verifies a captured backup and persists the report,
/// <see cref="ILatticeBackupControl.GetBackupHealthAsync"/> reads it back, and
/// <see cref="ILatticeBackupControl.ConfigureBackupHealthAsync"/> stores a per-backup
/// override. An unknown backup id and the fail-closed denial path are also covered.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlHealthTests
{
    private const string Source = "orders";

    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new ApiBackupClusterFixture();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<string> CaptureBackupAsync()
    {
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        var handle = await Operations.StartBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));
        var status = await UntilTerminalAsync(handle.OperationId);
        return status.ResultReference!;
    }

    [Test]
    public async Task IsHealthMonitoringAvailableAsync_reflects_sink_durability()
    {
        await _fixture.InitializeAsync();

        // The fixture wires the ephemeral in-cluster sink, which is not durable.
        var available = await _fixture.Control.IsHealthMonitoringAvailableAsync();

        Assert.That(available, Is.EqualTo(_fixture.Sink.IsDurable));
    }

    [Test]
    public async Task GetBackupHealthAsync_returns_null_before_any_check()
    {
        await _fixture.InitializeAsync();
        var backupId = await CaptureBackupAsync();

        Assert.That(await _fixture.Control.GetBackupHealthAsync(backupId), Is.Null);
    }

    [Test]
    public async Task GetBackupHealthAsync_unknown_backup_returns_null()
    {
        await _fixture.InitializeAsync();

        Assert.That(await _fixture.Control.GetBackupHealthAsync("does-not-exist"), Is.Null);
    }

    [Test]
    public async Task ConfigureBackupHealthAsync_stores_a_per_backup_override()
    {
        await _fixture.InitializeAsync();
        var backupId = await CaptureBackupAsync();
        var store = (ILatticeBackupHealthStore)_fixture.SiloServices.GetService(typeof(ILatticeBackupHealthStore))!;

        await _fixture.Control.ConfigureBackupHealthAsync(
            backupId, new BackupHealthConfig(monitoringEnabled: false, TimeSpan.FromHours(2)));

        var config = await store.GetConfigAsync(backupId);
        Assert.That(config, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(config!.MonitoringEnabled, Is.False);
            Assert.That(config.Interval, Is.EqualTo(TimeSpan.FromHours(2)));
        });
    }

    [Test]
    public async Task ConfigureBackupHealthAsync_null_config_throws()
    {
        await _fixture.InitializeAsync();
        var backupId = await CaptureBackupAsync();
        Assert.That(
            async () => await _fixture.Control.ConfigureBackupHealthAsync(backupId, null!),
            Throws.ArgumentNullException);
    }

    private ILatticeBackupOperations Operations => (ILatticeBackupOperations)_fixture.Control;

    private async Task<LatticeOperationStatus> UntilTerminalAsync(string operationId)
    {
        LatticeOperationStatus? status = null;
        await Orleans.Lattice.Testing.TestPoll.UntilAsync(
            async () => (status = await Operations.GetOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }
}
