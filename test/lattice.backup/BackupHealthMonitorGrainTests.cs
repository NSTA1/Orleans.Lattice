using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="BackupHealthMonitorGrain"/>'s sweep logic, driven
/// directly through <see cref="ILatticeBackupHealthMonitorGrain.SweepAsync"/> so the
/// reminder cadence is not involved. Exercises the durable-sink gate, the
/// enabled-options gate, per-backup enrolment and cadence, and reminder
/// registration, using in-memory fakes for the catalog, health store, and health
/// service.
/// </summary>
[TestFixture]
public sealed class BackupHealthMonitorGrainTests
{
    private static BackupHealthMonitorGrain CreateGrain(
        FakeCatalog catalog,
        FakeHealthStore store,
        FakeHealthService service,
        bool durable = true,
        bool enabled = true,
        TimeSpan? interval = null,
        IReminderRegistry? reminders = null)
    {
        var sink = Substitute.For<ILatticeBackupSink>();
        sink.IsDurable.Returns(durable);

        var options = new LatticeBackupHealthOptions
        {
            Enabled = enabled,
            DefaultInterval = interval ?? TimeSpan.FromHours(6),
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupHealthOptions>>();
        monitor.CurrentValue.Returns(options);

        return new BackupHealthMonitorGrain(
            Substitute.For<IGrainContext>(),
            reminders ?? Substitute.For<IReminderRegistry>(),
            sink,
            catalog,
            service,
            store,
            monitor,
            NullLogger<BackupHealthMonitorGrain>.Instance,
            new FakePersistentState<BackupHealthMonitorState>());
    }

    [Test]
    public async Task SweepAsync_non_durable_sink_verifies_nothing()
    {
        var catalog = new FakeCatalog("a", "b");
        var service = new FakeHealthService();

        var verified = await CreateGrain(catalog, new FakeHealthStore(), service, durable: false).SweepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(verified, Is.Zero);
            Assert.That(service.VerifiedIds, Is.Empty);
        });
    }

    [Test]
    public async Task SweepAsync_disabled_options_verifies_nothing()
    {
        var catalog = new FakeCatalog("a");
        var service = new FakeHealthService();

        var verified = await CreateGrain(catalog, new FakeHealthStore(), service, enabled: false).SweepAsync();

        Assert.That(verified, Is.Zero);
        Assert.That(service.VerifiedIds, Is.Empty);
    }

    [Test]
    public async Task SweepAsync_durable_verifies_every_enrolled_backup_and_persists_reports()
    {
        var catalog = new FakeCatalog("a", "b");
        var store = new FakeHealthStore();
        var service = new FakeHealthService();

        var verified = await CreateGrain(catalog, store, service).SweepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(verified, Is.EqualTo(2));
            Assert.That(service.VerifiedIds, Is.EquivalentTo(new[] { "a", "b" }));
            Assert.That(store.Reports.Keys, Is.EquivalentTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public async Task SweepAsync_skips_a_backup_whose_config_disables_monitoring()
    {
        var catalog = new FakeCatalog("a", "b");
        var store = new FakeHealthStore();
        await store.SetConfigAsync("b", new BackupHealthConfig(monitoringEnabled: false, TimeSpan.FromHours(6)));
        var service = new FakeHealthService();

        var verified = await CreateGrain(catalog, store, service).SweepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(verified, Is.EqualTo(1));
            Assert.That(service.VerifiedIds, Is.EqualTo(new[] { "a" }));
        });
    }

    [Test]
    public async Task SweepAsync_skips_a_backup_verified_within_its_interval()
    {
        var catalog = new FakeCatalog("a");
        var store = new FakeHealthStore();
        await store.SetReportAsync(new BackupHealthReport(
            "a",
            BackupHealthStatus.Healthy,
            true,
            Array.Empty<string>(),
            Array.Empty<string>(),
            DateTimeOffset.UtcNow,
            "recent"));
        var service = new FakeHealthService();

        var verified = await CreateGrain(catalog, store, service, interval: TimeSpan.FromHours(6)).SweepAsync();

        Assert.That(verified, Is.Zero);
        Assert.That(service.VerifiedIds, Is.Empty);
    }

    [Test]
    public async Task EnsureStartedAsync_durable_and_enabled_registers_the_sweep_reminder()
    {
        var reminders = Substitute.For<IReminderRegistry>();
        var grain = CreateGrain(new FakeCatalog(), new FakeHealthStore(), new FakeHealthService(), reminders: reminders);

        await grain.EnsureStartedAsync();

        await reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            "backup-health-sweep",
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task EnsureStartedAsync_non_durable_registers_no_reminder()
    {
        var reminders = Substitute.For<IReminderRegistry>();
        var grain = CreateGrain(new FakeCatalog(), new FakeHealthStore(), new FakeHealthService(), durable: false, reminders: reminders);

        await grain.EnsureStartedAsync();

        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(),
            Arg.Any<string>(),
            Arg.Any<TimeSpan>(),
            Arg.Any<TimeSpan>());
    }

    private sealed class FakeHealthService : ILatticeBackupHealthService
    {
        public List<string> VerifiedIds { get; } = new();

        public Task<BackupHealthReport> VerifyAsync(string backupId, CancellationToken cancellationToken = default)
        {
            VerifiedIds.Add(backupId);
            return Task.FromResult(new BackupHealthReport(
                backupId,
                BackupHealthStatus.Healthy,
                true,
                Array.Empty<string>(),
                Array.Empty<string>(),
                DateTimeOffset.UtcNow,
                "healthy"));
        }
    }

    private sealed class FakeHealthStore : ILatticeBackupHealthStore
    {
        public Dictionary<string, BackupHealthReport> Reports { get; } = new(StringComparer.Ordinal);
        private readonly Dictionary<string, BackupHealthConfig> _configs = new(StringComparer.Ordinal);

        public Task SetReportAsync(BackupHealthReport report, CancellationToken cancellationToken = default)
        {
            Reports[report.BackupId] = report;
            return Task.CompletedTask;
        }

        public Task<BackupHealthReport?> GetReportAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(Reports.GetValueOrDefault(backupId));

        public async IAsyncEnumerable<BackupHealthReport> ListReportsAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var report in Reports.Values)
            {
                yield return report;
                await Task.Yield();
            }
        }

        public Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default)
        {
            var removed = Reports.Remove(backupId);
            removed |= _configs.Remove(backupId);
            return Task.FromResult(removed);
        }

        public Task SetConfigAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)
        {
            _configs[backupId] = config;
            return Task.CompletedTask;
        }

        public Task<BackupHealthConfig?> GetConfigAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_configs.GetValueOrDefault(backupId));
    }

    private sealed class FakeCatalog : ILatticeBackupCatalogStore
    {
        private readonly Dictionary<string, BackupManifest> _rows = new(StringComparer.Ordinal);

        public FakeCatalog(params string[] ids)
        {
            foreach (var id in ids)
            {
                _rows[id] = BackupManifestModelTests.Sample(id: id);
            }
        }

        public Task RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken = default)
        {
            _rows[manifest.Id] = manifest;
            return Task.CompletedTask;
        }

        public Task<BackupManifest?> GetAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.GetValueOrDefault(backupId));

        public Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.Remove(backupId));

        public async IAsyncEnumerable<BackupManifest> ListAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var manifest in _rows.Values.OrderBy(m => m.Id, StringComparer.Ordinal))
            {
                yield return manifest;
                await Task.Yield();
            }
        }
    }
}
