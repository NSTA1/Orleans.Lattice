using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupSchedulerGrain"/>. Drives on-demand and
/// reminder-scheduled full / incremental captures for one backup scope and
/// prunes the scope's backup chain per its retention policy, mirroring the
/// reminder-anchored scheduling pattern the core tag-index reconciliation
/// coordinator uses. A capture never overlaps another for the same scope: an
/// activation-local guard skips a request while a capture is in flight, and the
/// capture-driving methods are <see cref="AlwaysInterleaveAttribute"/> so a
/// concurrent request observes the guard instead of queueing behind the running
/// capture.
/// </summary>
internal sealed class BackupSchedulerGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILatticeBackupCaptureService captureService,
    ILatticeBackupIncrementalCaptureService incrementalCaptureService,
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupSink sink,
    IOptionsMonitor<LatticeBackupScheduleOptions> optionsMonitor,
    ILogger<BackupSchedulerGrain> logger,
    [PersistentState("backup-scheduler", LatticeOptions.StorageProviderName)]
    IPersistentState<BackupSchedulerState> state)
    : IGrainBase, IRemindable, ILatticeBackupSchedulerGrain
{
    private const string FullScheduleReminderName = "backup-schedule-full";
    private const string IncrementalScheduleReminderName = "backup-schedule-incremental";

    // Activation-local overlap guard. Not persisted: a crash that deactivates the
    // grain clears it, so a stale in-flight flag can never wedge the scope.
    private bool _captureInFlight;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string ScopeKey => context.GrainId.Key.ToString()!;

    private LatticeBackupScheduleOptions Options => optionsMonitor.Get(ScopeKey);

    /// <inheritdoc />
    public async Task<string?> TriggerFullAsync(BackupScopeSelector scope)
    {
        await PersistScopeAsync(scope);
        return await RunCaptureAsync(incremental: false, scope);
    }

    /// <inheritdoc />
    public async Task<string?> TriggerIncrementalAsync(BackupScopeSelector scope)
    {
        await PersistScopeAsync(scope);
        return await RunCaptureAsync(incremental: true, scope);
    }

    /// <inheritdoc />
    public async Task EnsureScheduleAsync(BackupScopeSelector scope)
    {
        await PersistScopeAsync(scope);
        var opts = Options;
        await ApplyScheduleAsync(
            FullScheduleReminderName, opts.FullBackupScheduleEnabled, opts.FullBackupInterval);
        await ApplyScheduleAsync(
            IncrementalScheduleReminderName, opts.IncrementalBackupScheduleEnabled, opts.IncrementalBackupInterval);
    }

    /// <inheritdoc />
    public async Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        var opts = Options;
        var manifests = await ListScopeAsync(scope);
        if (!opts.RetentionEnabled)
        {
            return new BackupRetentionReport(manifests.Count, Array.Empty<string>());
        }

        return await PruneCoreAsync(manifests, opts);
    }

    /// <inheritdoc />
    public async Task<string?> RunScheduledCycleAsync(bool incremental)
    {
        var scope = state.State.Scope;
        if (scope is null)
        {
            return null;
        }

        var backupId = await RunCaptureAsync(incremental, scope);
        if (backupId is not null && Options.RetentionEnabled)
        {
            var manifests = await ListScopeAsync(scope);
            await PruneCoreAsync(manifests, Options);
        }

        return backupId;
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() => Task.FromResult(!_captureInFlight);

    /// <inheritdoc />
    public async Task<bool> HasScheduleAsync(bool incremental)
    {
        var name = incremental ? IncrementalScheduleReminderName : FullScheduleReminderName;
        var reminder = await reminderRegistry.GetReminder(context.GrainId, name);
        return reminder is not null;
    }

    /// <summary>
    /// Handles a schedule reminder by running one scheduled cycle (capture then
    /// retention). Unknown reminder names and firings before the scope has been
    /// configured are ignored.
    /// </summary>
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName is not (FullScheduleReminderName or IncrementalScheduleReminderName))
        {
            return;
        }

        await RunScheduledCycleAsync(reminderName == IncrementalScheduleReminderName);
    }

    private async Task<string?> RunCaptureAsync(bool incremental, BackupScopeSelector scope)
    {
        if (_captureInFlight)
        {
            // A capture for this scope is already running; skip rather than start
            // an overlapping one.
            return null;
        }

        _captureInFlight = true;
        try
        {
            var name = BuildBackupName(scope, incremental);
            if (!incremental)
            {
                var full = await captureService
                    .CaptureAsync(new LatticeBackupCaptureRequest(name, scope));
                return full.BackupId;
            }

            var baseManifest = await FindLatestForScopeAsync(scope);
            if (baseManifest is null)
            {
                // No base to layer on yet: capture a full baseline instead.
                var baseline = await captureService
                    .CaptureAsync(new LatticeBackupCaptureRequest(name, scope));
                return baseline.BackupId;
            }

            var increment = await incrementalCaptureService
                .CaptureIncrementalAsync(new LatticeBackupIncrementalCaptureRequest(name, scope, baseManifest.Id));
            return increment.BackupId;
        }
        finally
        {
            _captureInFlight = false;
        }
    }

    private async Task<BackupManifest?> FindLatestForScopeAsync(BackupScopeSelector scope)
    {
        BackupManifest? latest = null;
        await foreach (var manifest in catalog.ListAsync())
        {
            if (!ScopeMatches(manifest.Scope, scope))
            {
                continue;
            }

            if (latest is null
                || manifest.CreatedAtUtc > latest.CreatedAtUtc
                || (manifest.CreatedAtUtc == latest.CreatedAtUtc
                    && string.CompareOrdinal(manifest.Id, latest.Id) > 0))
            {
                latest = manifest;
            }
        }

        return latest;
    }

    private async Task<List<BackupManifest>> ListScopeAsync(BackupScopeSelector scope)
    {
        var manifests = new List<BackupManifest>();
        await foreach (var manifest in catalog.ListAsync())
        {
            if (ScopeMatches(manifest.Scope, scope))
            {
                manifests.Add(manifest);
            }
        }

        return manifests;
    }

    private async Task<BackupRetentionReport> PruneCoreAsync(
        List<BackupManifest> manifests, LatticeBackupScheduleOptions opts)
    {
        var keep = ComputeKeepSet(manifests, opts, DateTimeOffset.UtcNow);
        ExpandBaseClosure(manifests, keep);

        // Artifacts referenced by any retained manifest must survive even if a
        // pruned manifest happens to reference the same artifact id.
        var retainedArtifacts = new HashSet<string>(StringComparer.Ordinal);
        foreach (var manifest in manifests)
        {
            if (!keep.Contains(manifest.Id))
            {
                continue;
            }

            foreach (var descriptor in manifest.ContentDescriptors)
            {
                retainedArtifacts.Add(descriptor.ArtifactId);
            }
        }

        var prunedIds = new List<string>();
        foreach (var manifest in manifests)
        {
            if (keep.Contains(manifest.Id))
            {
                continue;
            }

            foreach (var descriptor in manifest.ContentDescriptors)
            {
                if (!retainedArtifacts.Contains(descriptor.ArtifactId))
                {
                    await sink.DeleteArtifactAsync(descriptor.ArtifactId);
                }
            }

            await sink.DeleteManifestAsync(manifest.Id);
            await catalog.RemoveAsync(manifest.Id);
            prunedIds.Add(manifest.Id);
        }

        logger.LogInformation(
            "Retention pruned {Pruned} backup(s) and retained {Retained} for scope {Scope}.",
            prunedIds.Count, manifests.Count - prunedIds.Count, ScopeKey);

        return new BackupRetentionReport(manifests.Count - prunedIds.Count, prunedIds);
    }

    private static HashSet<string> ComputeKeepSet(
        IReadOnlyList<BackupManifest> manifests, LatticeBackupScheduleOptions opts, DateTimeOffset now)
    {
        var keep = new HashSet<string>(StringComparer.Ordinal);

        // Retention enabled but unbounded (neither knob set) retains everything -
        // a safe no-op that never prunes.
        if (opts.RetentionKeepLast is null && opts.RetentionMaxAge is null)
        {
            foreach (var manifest in manifests)
            {
                keep.Add(manifest.Id);
            }

            return keep;
        }

        if (opts.RetentionKeepLast is { } keepLast)
        {
            var recent = manifests
                .OrderByDescending(m => m.CreatedAtUtc)
                .ThenByDescending(m => m.Id, StringComparer.Ordinal)
                .Take(keepLast);
            foreach (var manifest in recent)
            {
                keep.Add(manifest.Id);
            }
        }

        if (opts.RetentionMaxAge is { } maxAge)
        {
            var cutoff = now - maxAge;
            foreach (var manifest in manifests.Where(m => m.CreatedAtUtc >= cutoff))
            {
                keep.Add(manifest.Id);
            }
        }

        return keep;
    }

    private static void ExpandBaseClosure(IReadOnlyList<BackupManifest> manifests, HashSet<string> keep)
    {
        var byId = new Dictionary<string, BackupManifest>(StringComparer.Ordinal);
        foreach (var manifest in manifests)
        {
            byId[manifest.Id] = manifest;
        }

        var changed = true;
        while (changed)
        {
            changed = false;
            foreach (var id in keep.ToList())
            {
                if (byId.TryGetValue(id, out var manifest)
                    && manifest.BaseBackupId is { } baseId
                    && byId.ContainsKey(baseId)
                    && keep.Add(baseId))
                {
                    changed = true;
                }
            }
        }
    }

    private async Task ApplyScheduleAsync(string reminderName, bool enabled, TimeSpan interval)
    {
        if (enabled)
        {
            var period = ClampInterval(interval);
            await reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: context.GrainId,
                reminderName: reminderName,
                dueTime: period,
                period: period);
        }
        else
        {
            await UnregisterReminderAsync(reminderName);
        }
    }

    private async Task UnregisterReminderAsync(string reminderName)
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, reminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Failed to unregister backup schedule reminder {Reminder} for scope {Scope}.",
                reminderName, ScopeKey);
        }
    }

    private async Task PersistScopeAsync(BackupScopeSelector scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (!ScopeMatches(state.State.Scope, scope))
        {
            state.State.Scope = scope;
            await state.WriteStateAsync();
        }
    }

    private static string BuildBackupName(BackupScopeSelector scope, bool incremental) =>
        $"{(incremental ? "incremental" : "full")}-{scope.TreeId}-{DateTimeOffset.UtcNow.UtcTicks}";

    private static bool ScopeMatches(BackupScopeSelector? a, BackupScopeSelector b)
    {
        if (a is null)
        {
            return false;
        }

        return a.Kind == b.Kind
            && string.Equals(a.TreeId, b.TreeId, StringComparison.Ordinal)
            && string.Equals(a.KeyOrPrefix, b.KeyOrPrefix, StringComparison.Ordinal);
    }

    private static TimeSpan ClampInterval(TimeSpan interval) =>
        interval < LatticeBackupScheduleOptions.MinimumInterval
            ? LatticeBackupScheduleOptions.MinimumInterval
            : interval;
}
