using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
/// override that <see cref="ILatticeBackupControl.GetBackupHealthConfigAsync"/> reads
/// back (or the deployment default when none was stored). An unknown backup id and
/// the fail-closed denial path are also covered.
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

    [Test]
    public async Task GetBackupHealthConfigAsync_reads_the_deployment_default_before_any_override()
    {
        await _fixture.InitializeAsync();
        var backupId = await CaptureBackupAsync();

        var config = await _fixture.Control.GetBackupHealthConfigAsync(backupId);

        Assert.That(config, Is.EqualTo(new BackupHealthConfig(true, LatticeBackupHealthOptions.DefaultSweepInterval)));
    }

    [Test]
    public async Task GetBackupHealthConfigAsync_reads_back_the_stored_override()
    {
        await _fixture.InitializeAsync();
        var backupId = await CaptureBackupAsync();
        var stored = new BackupHealthConfig(monitoringEnabled: false, TimeSpan.FromMinutes(90));
        await _fixture.Control.ConfigureBackupHealthAsync(backupId, stored);

        Assert.That(await _fixture.Control.GetBackupHealthConfigAsync(backupId), Is.EqualTo(stored));
    }

    [Test]
    public async Task GetBackupHealthConfigAsync_default_follows_the_cluster_monitor_options()
    {
        await _fixture.InitializeAsync();
        var backupId = await CaptureBackupAsync();
        var options = new LatticeBackupHealthOptions { Enabled = false, DefaultInterval = TimeSpan.FromSeconds(30) };
        var control = _fixture.CreateControlWith(
            _fixture.SiloServices.GetRequiredService<BackupAccessAuthorizer>(),
            services: new HealthOptionsServices(_fixture.SiloServices, options));

        var config = await control.GetBackupHealthConfigAsync(backupId);

        // Off when the sweep is off, and never shorter than the sweep's own minimum.
        Assert.That(config, Is.EqualTo(new BackupHealthConfig(false, LatticeBackupHealthOptions.MinimumInterval)));
    }

    [Test]
    public async Task GetBackupHealthConfigAsync_unknown_backup_returns_null()
    {
        await _fixture.InitializeAsync();

        Assert.That(await _fixture.Control.GetBackupHealthConfigAsync("does-not-exist"), Is.Null);
    }

    [Test]
    public async Task GetBackupHealthConfigAsync_denied_permission_fails_closed()
    {
        await _fixture.InitializeAsync();
        var backupId = await CaptureBackupAsync();
        await _fixture.Control.ConfigureBackupHealthAsync(backupId, new BackupHealthConfig(false, TimeSpan.FromHours(1)));
        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no read grant"), membership: null));

        Assert.That(
            async () => await denying.GetBackupHealthConfigAsync(backupId),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task GetBackupHealthConfigAsync_empty_id_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.GetBackupHealthConfigAsync(string.Empty),
            Throws.ArgumentException);
    }

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }

    /// <summary>The silo's services, with fixed backup-health monitor options.</summary>
    private sealed class HealthOptionsServices(IServiceProvider inner, LatticeBackupHealthOptions options) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            serviceType == typeof(IOptionsMonitor<LatticeBackupHealthOptions>)
                ? new FixedOptionsMonitor(options)
                : inner.GetService(serviceType);
    }

    private sealed class FixedOptionsMonitor(LatticeBackupHealthOptions value) : IOptionsMonitor<LatticeBackupHealthOptions>
    {
        public LatticeBackupHealthOptions CurrentValue => value;

        public LatticeBackupHealthOptions Get(string? name) => value;

        public IDisposable? OnChange(Action<LatticeBackupHealthOptions, string?> listener) => null;
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
