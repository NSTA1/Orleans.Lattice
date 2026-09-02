using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupHealthMonitorGrain"/>. Drives the periodic
/// backup-health sweep from a single reminder, mirroring the reminder-anchored
/// scheduling pattern <see cref="BackupSchedulerGrain"/> uses for scheduled
/// captures. On each firing it enumerates the catalog and re-verifies every backup
/// that is enrolled (per-backup <see cref="BackupHealthConfig.MonitoringEnabled"/>,
/// defaulting to enrolled) and due (its last report is older than its per-backup
/// interval), persisting each fresh <see cref="BackupHealthReport"/> through the
/// shared health store. A sweep never overlaps another: an activation-local guard
/// skips a firing while a sweep is still in flight.
/// <para>
/// The whole feature is gated on <see cref="ILatticeBackupSink.IsDurable"/>: against
/// a non-durable sink the grain registers no reminder and every sweep is a no-op.
/// </para>
/// </summary>
internal sealed class BackupHealthMonitorGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILatticeBackupSink sink,
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupHealthService healthService,
    ILatticeBackupHealthStore healthStore,
    IBackupSinkSharingProbe sharingProbe,
    IOptionsMonitor<LatticeBackupHealthOptions> optionsMonitor,
    IOptionsMonitor<LatticeBackupOptions> backupOptionsMonitor,
    ILogger<BackupHealthMonitorGrain> logger,
    [PersistentState("backup-health-monitor", LatticeOptions.StorageProviderName)]
    IPersistentState<BackupHealthMonitorState> state)
    : IGrainBase, IRemindable, ILatticeBackupHealthMonitorGrain
{
    private const string SweepReminderName = "backup-health-sweep";

    // Activation-local overlap guard. Not persisted: a crash that deactivates the
    // grain clears it, so a stale in-flight flag can never wedge the sweep.
    private bool _sweepInFlight;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private LatticeBackupHealthOptions Options => optionsMonitor.CurrentValue;

    /// <inheritdoc />
    public async Task EnsureStartedAsync()
    {
        var opts = Options;
        if (sink.IsDurable && opts.Enabled)
        {
            var period = ClampInterval(opts.DefaultInterval);
            await reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: context.GrainId,
                reminderName: SweepReminderName,
                dueTime: period,
                period: period);
        }
        else
        {
            await UnregisterSweepAsync();
        }
    }

    /// <inheritdoc />
    public async Task<int> SweepAsync()
    {
        if (!sink.IsDurable || !Options.Enabled)
        {
            // Inert against a non-durable sink or when monitoring is disabled.
            return 0;
        }

        if (_sweepInFlight)
        {
            return 0;
        }

        _sweepInFlight = true;
        try
        {
            await RefreshSinkSharingAsync();

            var now = DateTimeOffset.UtcNow;
            var defaultInterval = Options.DefaultInterval;
            var verified = 0;

            await foreach (var manifest in catalog.ListAsync())
            {
                var config = await healthStore.GetConfigAsync(manifest.Id);
                var enrolled = config?.MonitoringEnabled ?? true;
                if (!enrolled)
                {
                    continue;
                }

                var interval = config?.Interval ?? defaultInterval;
                var last = await healthStore.GetReportAsync(manifest.Id);
                if (last is not null && now - last.CheckedAtUtc < interval)
                {
                    // Verified recently enough for its cadence; skip this sweep.
                    continue;
                }

                try
                {
                    var report = await healthService.VerifyAsync(manifest.Id);
                    await healthStore.SetReportAsync(report);
                    verified++;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Backup health verification failed for backup {BackupId}.", manifest.Id);
                }
            }

            state.State.LastSweepUtc = DateTimeOffset.UtcNow;
            state.State.LastSweepVerifiedCount = verified;
            await state.WriteStateAsync();

            logger.LogInformation("Backup health sweep verified {Verified} backup(s).", verified);
            return verified;
        }
        finally
        {
            _sweepInFlight = false;
        }
    }

    /// <summary>
    /// Refreshes the cross-cluster sink-sharing verdict once per sweep, not once
    /// per backup: sharing is a slow-moving deployment fact and the per-backup
    /// verification reads only the cached result.
    /// <para>
    /// Two guards make this safe to run on the sweep path. It honours
    /// <see cref="LatticeBackupOptions.SinkSharingEnforcement"/>, so
    /// <see cref="BackupSinkSharingEnforcement.Disabled"/> genuinely disables the
    /// probe everywhere rather than only at silo start - otherwise an operator who
    /// opted out would still have a canary marker written into their sink on every
    /// sweep, and a stale verdict would still downgrade backup health. It also
    /// bounds the probe with <see cref="LatticeBackupOptions.SinkSharingProbeTimeout"/>,
    /// the same deadline the startup guard applies: the probe writes to the sink and
    /// calls peers over the control channel, and an unbounded await here would hold
    /// the overlap guard forever and wedge backup health monitoring for the lifetime
    /// of the activation.
    /// </para>
    /// <para>
    /// A probe that faults or times out must never abort the sweep - local
    /// verification is still worth doing - so the failure is logged and the previous
    /// verdict stands.
    /// </para>
    /// </summary>
    private async Task RefreshSinkSharingAsync()
    {
        var backupOptions = backupOptionsMonitor.CurrentValue;
        if (backupOptions.SinkSharingEnforcement == BackupSinkSharingEnforcement.Disabled)
        {
            return;
        }

        try
        {
            using var timeout = new CancellationTokenSource(backupOptions.SinkSharingProbeTimeout);
            await sharingProbe.ProbeAsync(timeout.Token);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "The cross-cluster backup sink sharing probe failed during the health sweep.");
        }
    }

    /// <summary>
    /// Handles the sweep reminder by running one sweep. Unknown reminder names are
    /// ignored.
    /// </summary>
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != SweepReminderName)
        {
            return;
        }

        await SweepAsync();
    }

    private async Task UnregisterSweepAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, SweepReminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to unregister the backup health sweep reminder.");
        }
    }

    private static TimeSpan ClampInterval(TimeSpan interval) =>
        interval < LatticeBackupHealthOptions.MinimumInterval
            ? LatticeBackupHealthOptions.MinimumInterval
            : interval;
}
