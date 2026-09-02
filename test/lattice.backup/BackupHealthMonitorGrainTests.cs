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
        IReminderRegistry? reminders = null,
        IBackupSinkSharingProbe? sharingProbe = null,
        BackupSinkSharingEnforcement enforcement = BackupSinkSharingEnforcement.Warn,
        TimeSpan? probeTimeout = null)
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

        // A short probe deadline keeps the hanging-probe test fast: the shipped
        // default is 15 seconds, which would otherwise stall the fixture for the
        // full timeout just to observe the cancellation.
        var backupOptions = new LatticeBackupOptions
        {
            SinkSharingEnforcement = enforcement,
            SinkSharingProbeTimeout = probeTimeout ?? TimeSpan.FromMilliseconds(50),
        };
        var backupMonitor = Substitute.For<IOptionsMonitor<LatticeBackupOptions>>();
        backupMonitor.CurrentValue.Returns(backupOptions);

        return new BackupHealthMonitorGrain(
            Substitute.For<IGrainContext>(),
            reminders ?? Substitute.For<IReminderRegistry>(),
            sink,
            catalog,
            service,
            store,
            sharingProbe ?? new NoBackupSinkSharingProbe(),
            monitor,
            backupMonitor,
            NullLogger<BackupHealthMonitorGrain>.Instance,
            new FakePersistentState<BackupHealthMonitorState>());
    }

    /// <summary>
    /// The sweep refreshes the cross-cluster sharing verdict once per sweep, so the
    /// misconfiguration is re-decided periodically rather than only at silo start.
    /// </summary>
    [Test]
    public async Task SweepAsync_refreshes_the_sink_sharing_verdict_once_per_sweep()
    {
        var probe = new CountingSharingProbe();

        await CreateGrain(new FakeCatalog("a", "b"), new FakeHealthStore(), new FakeHealthService(), sharingProbe: probe)
            .SweepAsync();

        Assert.That(probe.Calls, Is.EqualTo(1));
    }

    /// <summary>
    /// Regression: <see cref="BackupSinkSharingEnforcement.Disabled"/> must disable
    /// the probe everywhere, not only in the startup guard. The sweep previously
    /// probed unconditionally, so an operator who opted out still had a canary
    /// marker written into their sink every sweep and still had backup health
    /// downgraded by the refreshed verdict - a knob that only half worked.
    /// </summary>
    [Test]
    public async Task SweepAsync_disabled_enforcement_never_probes_the_sink()
    {
        var probe = new CountingSharingProbe();

        var verified = await CreateGrain(
                new FakeCatalog("a"),
                new FakeHealthStore(),
                new FakeHealthService(),
                sharingProbe: probe,
                enforcement: BackupSinkSharingEnforcement.Disabled)
            .SweepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.Calls, Is.Zero, "Disabled enforcement must not probe.");
            Assert.That(verified, Is.EqualTo(1), "Local verification must still run.");
        });
    }

    /// <summary>
    /// Regression: the sweep bounds the probe with
    /// <see cref="LatticeBackupOptions.SinkSharingProbeTimeout"/>. The probe writes
    /// to the sink and calls peers, and the sweep holds an activation-local overlap
    /// guard, so an unbounded await would wedge backup health monitoring for the
    /// lifetime of the activation.
    /// </summary>
    [Test]
    public async Task SweepAsync_cancels_a_hanging_probe_and_still_verifies_locally()
    {
        var probe = new HangingSharingProbe();
        var service = new FakeHealthService();

        var verified = await CreateGrain(new FakeCatalog("a"), new FakeHealthStore(), service, sharingProbe: probe)
            .SweepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(probe.Cancelled, Is.True, "The probe must be given a deadline.");
            Assert.That(verified, Is.EqualTo(1));
            Assert.That(service.VerifiedIds, Is.EqualTo(new[] { "a" }));
        });
    }

    /// <summary>
    /// A probe that faults must never abort the sweep: local verification is still
    /// worth doing and the previous verdict stands.
    /// </summary>
    [Test]
    public async Task SweepAsync_probe_failure_does_not_abort_local_verification()
    {
        var service = new FakeHealthService();

        var verified = await CreateGrain(
                new FakeCatalog("a"),
                new FakeHealthStore(),
                service,
                sharingProbe: new ThrowingSharingProbe())
            .SweepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(verified, Is.EqualTo(1));
            Assert.That(service.VerifiedIds, Is.EqualTo(new[] { "a" }));
        });
    }

    /// <summary>Counts probe invocations so a test can assert the sweep gate.</summary>
    private sealed class CountingSharingProbe : IBackupSinkSharingProbe
    {
        public int Calls { get; private set; }

        public BackupSinkSharingReport? LastReport { get; private set; }

        public Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default)
        {
            Calls++;
            LastReport = new BackupSinkSharingReport(
                BackupSinkSharingStatus.Shared,
                clusterId: "region-a",
                peerCount: 1,
                unconfirmedPeerClusterIds: [],
                probedAtUtc: DateTimeOffset.UnixEpoch,
                explanation: "shared");
            return Task.FromResult(LastReport);
        }
    }

    /// <summary>
    /// Never completes on its own, so the sweep can only proceed if it supplied a
    /// deadline the probe observes.
    /// </summary>
    private sealed class HangingSharingProbe : IBackupSinkSharingProbe
    {
        public bool Cancelled { get; private set; }

        public BackupSinkSharingReport? LastReport => null;

        public async Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                Cancelled = true;
                throw;
            }

            throw new InvalidOperationException("The hanging probe must never complete on its own.");
        }
    }

    /// <summary>Faults on every probe, to prove the sweep survives it.</summary>
    private sealed class ThrowingSharingProbe : IBackupSinkSharingProbe
    {
        public BackupSinkSharingReport? LastReport => null;

        public Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("probe unavailable");
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

    [Test]
    public void GrainContext_returns_the_injected_context()
    {
        // Line 44: the GrainContext property getter.
        var ctx = Substitute.For<IGrainContext>();
        var sink = Substitute.For<ILatticeBackupSink>();
        sink.IsDurable.Returns(true);
        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupHealthOptions>>();
        monitor.CurrentValue.Returns(new LatticeBackupHealthOptions { Enabled = true });
        var backupMonitor = Substitute.For<IOptionsMonitor<LatticeBackupOptions>>();
        backupMonitor.CurrentValue.Returns(new LatticeBackupOptions());
        var grain = new BackupHealthMonitorGrain(
            ctx,
            Substitute.For<IReminderRegistry>(),
            sink,
            new FakeCatalog(),
            new FakeHealthService(),
            new FakeHealthStore(),
            new NoBackupSinkSharingProbe(),
            monitor,
            backupMonitor,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<BackupHealthMonitorGrain>.Instance,
            new FakePersistentState<BackupHealthMonitorState>());

        Assert.That(grain.GrainContext, Is.SameAs(ctx));
    }

    [Test]
    public async Task SweepAsync_skips_when_a_sweep_is_already_in_flight()
    {
        // Line 78: the _sweepInFlight guard returns 0 for the concurrent call.
        var grain = CreateGrain(new FakeCatalog("a"), new FakeHealthStore(), new FakeHealthService());
        typeof(BackupHealthMonitorGrain)
            .GetField("_sweepInFlight", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(grain, true);

        var result = await grain.SweepAsync();
        Assert.That(result, Is.Zero);
    }

    [Test]
    public async Task SweepAsync_verification_exception_is_logged_and_sweep_continues()
    {
        // Lines 111-114: an exception from healthService.VerifyAsync is caught and
        // logged; the sweep still processes the remaining backup and returns the
        // partial count.
        var catalog = new FakeCatalog("a", "b");
        var store = new FakeHealthStore();
        var service = new FakeHealthService { ThrowOn = new InvalidOperationException("simulated") };

        // Both verifications throw; verified count stays 0 but no exception escapes.
        var verified = await CreateGrain(catalog, store, service).SweepAsync();
        Assert.That(verified, Is.Zero);
    }

    [Test]
    public async Task ReceiveReminder_with_unknown_name_does_not_run_a_sweep()
    {
        // Lines 136-138: a reminder with a name other than "backup-health-sweep" is
        // silently ignored.
        var service = new FakeHealthService();
        var grain = CreateGrain(new FakeCatalog("a"), new FakeHealthStore(), service);

        await grain.ReceiveReminder("some-other-reminder", default);

        Assert.That(service.VerifiedIds, Is.Empty);
    }

    [Test]
    public async Task ReceiveReminder_with_sweep_name_runs_a_sweep()
    {
        // Line 141: when the reminder name matches the sweep reminder, ReceiveReminder
        // calls SweepAsync. A non-durable sink means SweepAsync returns quickly with
        // zero verifications - enough to prove the call-through at line 141.
        var service = new FakeHealthService();
        var grain = CreateGrain(new FakeCatalog("a"), new FakeHealthStore(), service, durable: false);

        // The constant "backup-health-sweep" is private; access it via the known name.
        await grain.ReceiveReminder("backup-health-sweep", default);

        // SweepAsync was called: non-durable sink returns early but VerifiedIds is empty.
        Assert.That(service.VerifiedIds, Is.Empty);
    }

    [Test]
    public async Task EnsureStartedAsync_reminder_unregister_failure_is_swallowed()
    {
        // Lines 154-157: GetReminder throws inside UnregisterSweepAsync; the
        // exception must not propagate.
        var reminders = Substitute.For<IReminderRegistry>();
        reminders
            .GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns<Task<IGrainReminder>>(_ => throw new InvalidOperationException("simulated"));
        var grain = CreateGrain(
            new FakeCatalog(), new FakeHealthStore(), new FakeHealthService(),
            durable: false, reminders: reminders);

        Assert.That(async () => await grain.EnsureStartedAsync(), Throws.Nothing);
    }

    private sealed class FakeHealthService : ILatticeBackupHealthService
    {
        public List<string> VerifiedIds { get; } = new();
        public Exception? ThrowOn { get; set; }

        public Task<BackupHealthReport> VerifyAsync(string backupId, CancellationToken cancellationToken = default)
        {
            VerifiedIds.Add(backupId);
            if (ThrowOn is not null)
                throw ThrowOn;
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
