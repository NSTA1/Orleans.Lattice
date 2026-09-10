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
    BackupInventoryRegistry inventory,
    BackupAccessAuthorizer authorizer,
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
        ArgumentNullException.ThrowIfNull(scope);
        await AuthorizeScheduleAsync(scope);
        await PersistScopeAsync(scope);
        var opts = Options;
        await ApplyScheduleAsync(
            FullScheduleReminderName, opts.FullBackupScheduleEnabled, opts.FullBackupInterval);
        await ApplyScheduleAsync(
            IncrementalScheduleReminderName, opts.IncrementalBackupScheduleEnabled, opts.IncrementalBackupInterval);
    }

    /// <inheritdoc />
    public async Task ScheduleRecurringAsync(BackupScopeSelector scope, bool incremental, TimeSpan interval)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks);

        await AuthorizeScheduleAsync(scope);
        await PersistScopeAsync(scope);

        var period = ClampInterval(interval);
        if (incremental)
        {
            state.State.RuntimeIncrementalBackupInterval = period;
        }
        else
        {
            state.State.RuntimeFullBackupInterval = period;
        }

        await state.WriteStateAsync();

        var reminderName = incremental ? IncrementalScheduleReminderName : FullScheduleReminderName;
        await ApplyScheduleAsync(reminderName, enabled: true, period);
    }

    /// <inheritdoc />
    public async Task CancelScheduleAsync(bool incremental)
    {
        var reminderName = incremental ? IncrementalScheduleReminderName : FullScheduleReminderName;
        await UnregisterReminderAsync(reminderName);

        if (incremental)
        {
            state.State.RuntimeIncrementalBackupInterval = null;
        }
        else
        {
            state.State.RuntimeFullBackupInterval = null;
        }

        await state.WriteStateAsync();
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

        // A scheduled cycle firing while a capture for this scope is still in
        // flight is an overrun: the previous cycle has not drained before the next
        // was due. It is recorded distinctly from the generic skip the overlap
        // guard emits.
        if (_captureInFlight)
        {
            LatticeBackupMetrics.RecordSchedulerOverrun(ScopeKey);
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
    public async Task<BackupSchedulerRuntimeStatus> GetScopeRuntimeStatusAsync()
    {
        var fullRegistered = await HasScheduleAsync(incremental: false);
        var incrementalRegistered = await HasScheduleAsync(incremental: true);
        return new BackupSchedulerRuntimeStatus(
            fullRegistered,
            incrementalRegistered,
            state.State.LastFullRunUtc,
            state.State.LastFullSuccessUtc,
            state.State.LastIncrementalRunUtc,
            state.State.LastIncrementalSuccessUtc,
            state.State.LastRunOutcome,
            state.State.RuntimeFullBackupInterval,
            state.State.RuntimeIncrementalBackupInterval);
    }

    /// <inheritdoc />
    public async Task<bool> HasScheduleAsync(bool incremental)
    {
        var name = incremental ? IncrementalScheduleReminderName : FullScheduleReminderName;
        var reminder = await BackupReminderResilience.RunWithRetryAsync(
            () => reminderRegistry.GetReminder(context.GrainId, name),
            logger, nameof(reminderRegistry.GetReminder), name, ScopeKey);
        return reminder is not null;
    }

    /// <summary>
    /// Handles a schedule reminder by running one scheduled cycle (capture then
    /// retention). Unknown reminder names and firings before the scope has been
    /// configured are ignored.
    /// </summary>
    /// <remarks>
    /// The cycle runs inside an <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>
    /// scope. A reminder tick is authored by the Orleans runtime, not by a caller,
    /// so it carries no subject: without the marker every gated read a capture
    /// performs is refused on a host whose gate defaults to deny, and the scope
    /// simply stops being backed up with no denial surfaced anywhere (issue #2608).
    /// The marker is applied here rather than in <see cref="RunScheduledCycleAsync"/>
    /// because that method is on <see cref="ILatticeBackupSchedulerGrain"/> and a
    /// caller can invoke it directly; this handler is the narrowest seam that is
    /// genuinely infrastructure-authored. The trust decision for a scheduled cycle
    /// is taken where a caller asks for a schedule - see
    /// <see cref="AuthorizeScheduleAsync"/> - so a reminder can only exist because
    /// an authorized caller registered it.
    /// </remarks>
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName is not (FullScheduleReminderName or IncrementalScheduleReminderName))
        {
            return;
        }

        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        await RunScheduledCycleAsync(reminderName == IncrementalScheduleReminderName);
    }

    /// <summary>
    /// Authorizes a caller's request to create or update a schedule for
    /// <paramref name="scope"/>, throwing
    /// <see cref="LatticeAuthorizationDeniedException"/> when the
    /// <see cref="LatticeOperation.Backup"/> capability is not granted.
    /// </summary>
    /// <remarks>
    /// Registration is the seam that matters: a registered reminder later runs
    /// system-origin (see <see cref="ReceiveReminder"/>), so an unauthorized
    /// registration would otherwise buy a recurring gate-bypassed capture. The
    /// API facade already authorizes before reaching this grain; this check closes
    /// the in-process <see cref="ILatticeBackupScheduler"/> and direct-grain paths,
    /// which the facade does not cover. It is deliberately NOT applied inside
    /// <c>ApplyScheduleAsync</c>, which the grain's own housekeeping also reaches.
    /// </remarks>
    private ValueTask AuthorizeScheduleAsync(BackupScopeSelector scope) =>
        authorizer.AuthorizeBackupAsync(scope, CancellationToken.None);

    private async Task<string?> RunCaptureAsync(bool incremental, BackupScopeSelector scope)
    {
        if (_captureInFlight)
        {
            // A capture for this scope is already running; skip rather than start
            // an overlapping one.
            LatticeBackupMetrics.RecordSchedulerSkipped(ScopeKey);
            return null;
        }

        _captureInFlight = true;
        var startedAt = DateTimeOffset.UtcNow;
        if (incremental)
        {
            state.State.LastIncrementalRunUtc = startedAt;
        }
        else
        {
            state.State.LastFullRunUtc = startedAt;
        }

        try
        {
            var name = BuildBackupName(scope, incremental);
            string? resultId;
            if (!incremental)
            {
                var full = await captureService
                    .CaptureAsync(new LatticeBackupCaptureRequest(name, scope));
                resultId = full.BackupId;
            }
            else
            {
                var baseManifest = await FindLatestForScopeAsync(scope);
                if (baseManifest is null)
                {
                    // No base to layer on yet: capture a full baseline instead.
                    var baseline = await captureService
                        .CaptureAsync(new LatticeBackupCaptureRequest(name, scope));
                    resultId = baseline.BackupId;
                }
                else
                {
                    var increment = await incrementalCaptureService
                        .CaptureIncrementalAsync(new LatticeBackupIncrementalCaptureRequest(name, scope, baseManifest.Id));
                    resultId = increment.BackupId;
                }
            }

            var succeededAt = DateTimeOffset.UtcNow;
            if (incremental)
            {
                state.State.LastIncrementalSuccessUtc = succeededAt;
            }
            else
            {
                state.State.LastFullSuccessUtc = succeededAt;
            }

            state.State.LastRunOutcome = BackupScopeRunOutcome.Success;
            await state.WriteStateAsync();
            inventory.RecordScopeOutcome(ScopeKey, BackupScopeRunOutcome.Success, succeededAt);
            return resultId;
        }
        catch (Exception ex)
        {
            // A denial is recorded distinctly from a generic fault. Without this,
            // a gated host that refuses every scheduled capture presents as an
            // absence of successful backups - the failure mode is silence, and
            // silence is what nobody alerts on (issue #2608).
            var reason = LatticeBackupMetrics.MapReason(ex);
            var denied = ex is LatticeAuthorizationDeniedException;
            var outcome = denied ? BackupScopeRunOutcome.Denied : BackupScopeRunOutcome.Failure;

            if (denied)
            {
                logger.LogWarning(
                    ex,
                    "Backup scope {Scope}: the {Kind} capture cycle was DENIED by the access gate. "
                    + "The scope is not being backed up. This is a denial, not an empty backup set.",
                    ScopeKey,
                    incremental ? "incremental" : "full");
            }
            else
            {
                logger.LogWarning(
                    ex,
                    "Backup scope {Scope}: the {Kind} capture cycle faulted ({Reason}).",
                    ScopeKey,
                    incremental ? "incremental" : "full",
                    reason);
            }

            state.State.LastRunOutcome = outcome;
            await state.WriteStateAsync();
            inventory.RecordScopeOutcome(ScopeKey, outcome, DateTimeOffset.UtcNow);
            LatticeBackupMetrics.RecordSchedulerFailure(ScopeKey, reason);
            throw;
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
        var prunedManifests = new List<BackupManifest>();
        long reclaimedBytes = 0;
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
                    reclaimedBytes += descriptor.ByteLength;
                }
            }

            await sink.DeleteManifestAsync(manifest.Id);
            await catalog.RemoveAsync(manifest.Id);
            prunedIds.Add(manifest.Id);
            prunedManifests.Add(manifest);
        }

        foreach (var pruned in prunedManifests)
        {
            inventory.RecordPruned(pruned);
        }

        if (prunedIds.Count > 0)
        {
            LatticeBackupMetrics.RecordRetention(ScopeKey, reclaimedBytes, prunedIds.Count);
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
            // Guard the subtraction: a very large (but validator-permitted, since the
            // validator only rejects a non-positive window) RetentionMaxAge would push
            // the cutoff below DateTimeOffset.MinValue and throw. A "retain for
            // practically forever" window means keep every manifest, so saturate the
            // cutoff at MinValue rather than fault the retention pass.
            var cutoff = maxAge >= now - DateTimeOffset.MinValue
                ? DateTimeOffset.MinValue
                : now - maxAge;
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
            await BackupReminderResilience.RunWithRetryAsync(
                () => reminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: context.GrainId,
                    reminderName: reminderName,
                    dueTime: period,
                    period: period),
                logger, nameof(reminderRegistry.RegisterOrUpdateReminder), reminderName, ScopeKey);
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
            var reminder = await BackupReminderResilience.RunWithRetryAsync(
                () => reminderRegistry.GetReminder(context.GrainId, reminderName),
                logger, nameof(reminderRegistry.GetReminder), reminderName, ScopeKey);
            if (reminder is not null)
            {
                await BackupReminderResilience.RunWithRetryAsync(
                    () => reminderRegistry.UnregisterReminder(context.GrainId, reminder),
                    logger, nameof(reminderRegistry.UnregisterReminder), reminderName, ScopeKey);
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
