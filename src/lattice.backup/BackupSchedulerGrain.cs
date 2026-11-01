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
internal sealed partial class BackupSchedulerGrain(
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
    IPersistentState<BackupSchedulerState> state,
    BackupPublisher? publisher = null,
    IOptions<LatticeBackupOptions>? backupOptions = null)
    : IGrainBase, IRemindable, ILatticeBackupSchedulerGrain
{
    private const string FullScheduleReminderName = "backup-schedule-full";
    private const string IncrementalScheduleReminderName = "backup-schedule-incremental";

    // Activation-local overlap guard. Not persisted: a crash that deactivates the
    // grain clears it, so a stale in-flight flag can never wedge the scope.
    private bool _captureInFlight;

    // The reminder-driven cycle in flight, if any (issue #5473): a reminder tick
    // dispatches its cycle onto an interleaved one-shot grain timer and returns,
    // and a tick that arrives while one is still running is coalesced into it.
    // Activation-local, like the guards below.
    private bool _tickCycleInFlight;
    private bool _tickCycleIncremental;
    private IGrainTimer? _tickCycle;

    // A full tick that arrived while an incremental reminder cycle was running
    // (issue #5531). An incremental cycle does not subsume a full one, so the
    // full tick is not coalesced into it: it runs one full cycle when the
    // incremental cycle ends. Several such ticks collapse into one.
    private bool _pendingFullTick;

    // Activation-local retention guard (issue #5357): one retention pass at a time.
    // Not persisted, for the same reason as the capture guard.
    private bool _pruneInFlight;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string ScopeKey => context.GrainId.Key.ToString()!;

    private LatticeBackupScheduleOptions Options => optionsMonitor.Get(ScopeKey);

    /// <inheritdoc />
    public async Task<string?> TriggerFullAsync(BackupScopeSelector scope)
    {
        await AuthorizeScheduleAsync(scope);
        await PersistScopeAsync(scope);
        return await RunCaptureAsync(incremental: false, scope);
    }

    /// <inheritdoc />
    public async Task<string?> TriggerIncrementalAsync(BackupScopeSelector scope)
    {
        await AuthorizeScheduleAsync(scope);
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
        if (state.State.Scope is not { } scope)
        {
            return;
        }

        await AuthorizeScheduleAsync(scope);
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
        await AuthorizeScheduleAsync(scope);
        var opts = Options;
        if (!opts.RetentionEnabled)
        {
            var manifests = await ListScopeAsync(scope);
            return new BackupRetentionReport(manifests.Count, Array.Empty<string>());
        }

        return await PruneCoreAsync(scope, opts);
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
            await PruneCoreAsync(scope, Options);
        }

        return backupId;
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() => Task.FromResult(!_captureInFlight && !_tickCycleInFlight);

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
    /// Handles a schedule reminder by starting one scheduled cycle (capture then
    /// retention) and returning without waiting for it. Unknown reminder names
    /// and firings before the scope has been configured are ignored.
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
    /// <para>
    /// The cycle is not run inside the reminder call (issue #5473). A capture can
    /// outlast the reminder's response timeout, and <see cref="ReceiveReminder"/>
    /// is not interleaving, so awaiting it here held the activation for the whole
    /// capture: the reminder call timed out, and every tick that fired meanwhile
    /// queued behind it and then ran its own capture back to back, so a schedule
    /// shorter than its captures never caught up and kept the silo saturated.
    /// Instead the tick dispatches the cycle onto an interleaved, keep-alive
    /// one-shot grain timer and returns, and a tick that arrives while a
    /// reminder-driven cycle is still running is coalesced into it: it starts
    /// nothing and is recorded as an overrun. Coalescing is per kind (issue
    /// #5531): a full cycle subsumes an incremental one, so an incremental tick
    /// is coalesced into any running cycle, but a full tick arriving during an
    /// incremental cycle is deferred instead - recorded as an overrun, not a
    /// skip - and runs one full cycle as soon as the incremental one ends, so a
    /// scope with both schedules never loses its full backup. With no grain
    /// runtime (a unit-test harness, where <c>RegisterGrainTimer</c> throws) the
    /// cycle runs inline.
    /// </para>
    /// </remarks>
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName is not (FullScheduleReminderName or IncrementalScheduleReminderName))
        {
            return;
        }

        var incremental = reminderName == IncrementalScheduleReminderName;
        if (_tickCycleInFlight && !incremental && _tickCycleIncremental)
        {
            LatticeBackupMetrics.RecordSchedulerOverrun(ScopeKey);
            _pendingFullTick = true;
            logger.LogInformation(
                "Backup scope {Scope}: a full schedule tick arrived while an incremental scheduled cycle was "
                + "running; the full cycle runs when that cycle ends.",
                ScopeKey);
            return;
        }

        if (_tickCycleInFlight)
        {
            LatticeBackupMetrics.RecordSchedulerOverrun(ScopeKey);
            LatticeBackupMetrics.RecordSchedulerSkipped(ScopeKey);
            logger.LogInformation(
                "Backup scope {Scope}: a {Kind} schedule tick arrived while the previous scheduled cycle was "
                + "still running; it was coalesced into that cycle.",
                ScopeKey,
                incremental ? "incremental" : "full");
            return;
        }

        await StartTickCycleAsync(incremental);
    }

    private async Task StartTickCycleAsync(bool incremental)
    {
        _tickCycleInFlight = true;
        _tickCycleIncremental = incremental;
        try
        {
            // The callback cannot run before this assignment: it is queued on the
            // activation and the current turn does not yield before it.
            _tickCycle = this.RegisterGrainTimer(
                _ => RunTickCycleAsync(incremental),
                new GrainTimerCreationOptions(TimeSpan.Zero, Timeout.InfiniteTimeSpan) { Interleave = true, KeepAlive = true });
            return;
        }
        catch
        {
            _tickCycle = null;
        }

        await RunTickCycleAsync(incremental);
    }

    private async Task RunTickCycleAsync(bool incremental)
    {
        try
        {
            using var origin = LatticeAccessGateContext.EnterSystemOrigin();
            await RunScheduledCycleAsync(incremental);
        }
        catch (Exception ex)
        {
            // The capture path has already recorded its outcome; this surfaces a
            // fault anywhere in the cycle, retention included, which the reminder
            // call no longer carries back to the runtime.
            logger.LogWarning(ex, "Backup scope {Scope}: the scheduled cycle faulted.", ScopeKey);
        }
        finally
        {
            _tickCycleInFlight = false;
            Interlocked.Exchange(ref _tickCycle, null)?.Dispose();
        }

        if (_pendingFullTick)
        {
            _pendingFullTick = false;
            await StartTickCycleAsync(incremental: false);
        }
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
                || BackupRetentionPlanner.RankTime(manifest) > BackupRetentionPlanner.RankTime(latest)
                || (BackupRetentionPlanner.RankTime(manifest) == BackupRetentionPlanner.RankTime(latest)
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
        BackupScopeSelector scope, LatticeBackupScheduleOptions opts)
    {
        // Activation-local prune guard (issue #5357). An operator PruneAsync and a
        // scheduled cycle's prune ([AlwaysInterleave]) can otherwise run at once,
        // each from its own stale plan: one pass can restore a depended-on row after
        // the other has scanned for dependents and moved on to delete it. Like the
        // capture guard it is not persisted, so a crash cannot wedge the scope.
        if (_pruneInFlight)
        {
            logger.LogInformation(
                "Retention for scope {Scope} skipped: another retention pass is in flight.", ScopeKey);
            var listed = await ListScopeAsync(scope);
            return new BackupRetentionReport(listed.Count, Array.Empty<string>());
        }

        _pruneInFlight = true;

        // Every id published while the listing is read is recorded, so the plan
        // made from it never dooms a backup a capture has published since (#5418).
        _publishedDuringListing = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            // The listing is taken under the guard (issue #5412). Taken before it, a
            // pass could wait out another pass and then plan from a catalog that pass
            // had already pruned, and restore the row of a backup it had deleted.
            var manifests = await ListScopeAsync(scope);
            return await PruneUnderGuardAsync(manifests, opts);
        }
        finally
        {
            _publishedDuringListing = null;
            _pruneInFlight = false;
        }
    }

    private async Task<BackupRetentionReport> PruneUnderGuardAsync(
        List<BackupManifest> manifests, LatticeBackupScheduleOptions opts)
    {
        var keep = BackupRetentionPlanner.ComputeRetained(
            manifests, opts.RetentionKeepLast, opts.RetentionMaxAge, DateTimeOffset.UtcNow);

        // The doom decision and the reservation are one step, with no await between
        // the listing and here (issue #5418): an id held by a publish, published
        // during the listing, awaiting its set's stamp, or held by a delete is not
        // doomed, and every doomed id is held retiring before any row is removed,
        // so a publish of it waits until this pass has deleted or restored it.
        var held = new HashSet<string>(StringComparer.Ordinal);
        var doomed = new List<BackupManifest>();
        foreach (var manifest in BackupRetentionPlanner.Doomed(manifests, keep))
        {
            if (IsHeld(manifest.Id))
            {
                held.Add(manifest.Id);
            }
            else
            {
                doomed.Add(manifest);
            }
        }

        _publishedDuringListing = null;
        ReserveRetiring(doomed);
        try
        {
            return await PruneReservedAsync(manifests, keep, held, doomed);
        }
        finally
        {
            ReleaseRetiring(doomed);
        }
    }

    private async Task<BackupRetentionReport> PruneReservedAsync(
        List<BackupManifest> manifests, HashSet<string> keep, HashSet<string> held, List<BackupManifest> doomed)
    {
        if (doomed.Count == 0)
        {
            logger.LogInformation(
                "Retention pruned 0 backup(s) and retained {Retained} for scope {Scope}.",
                manifests.Count, ScopeKey);
            return new BackupRetentionReport(manifests.Count, Array.Empty<string>());
        }

        // Artifacts referenced by any retained manifest must survive even if a
        // pruned manifest happens to reference the same artifact id.
        var retainedArtifacts = BackupRetentionPlanner.RetainedArtifacts(manifests, keep.Union(held).ToHashSet(StringComparer.Ordinal));

        // The fence against an in-flight incremental capture (issue #5357). Each
        // doomed row is removed FIRST, so an increment that re-reads its base's row
        // after this point abandons itself; then one sink scan finds every
        // increment that wrote its manifest before that point, and the row of any
        // doomed manifest one outside this pass builds on - and of its own base
        // chain - is restored instead of deleted.
        foreach (var manifest in doomed)
        {
            await catalog.RemoveAsync(manifest.Id);
        }

        var sinkManifests = new List<BackupManifest>();
        await foreach (var manifest in sink.ListManifestsAsync())
        {
            sinkManifests.Add(manifest);
        }

        var blocked = BackupRetentionPlanner.DependedOn(doomed, sinkManifests);
        foreach (var manifest in doomed)
        {
            if (blocked.Contains(manifest.Id))
            {
                await catalog.RegisterAsync(manifest);
                ReleaseRetiring(manifest.Id);
            }
        }

        var prunedIds = new List<string>();
        var prunedManifests = new List<BackupManifest>();
        long reclaimedBytes = 0;
        var deleting = doomed.Where(m => !blocked.Contains(m.Id)).ToList();
        foreach (var manifest in BackupRetentionPlanner.TipFirst(deleting))
        {
            // Tip first, and each sink manifest before its artifacts, so an
            // interrupted pass leaves unreferenced artifacts rather than a manifest
            // without them, or an increment without its base.
            await sink.DeleteManifestAsync(manifest.Id);
            foreach (var descriptor in manifest.ContentDescriptors)
            {
                if (!retainedArtifacts.Contains(descriptor.ArtifactId))
                {
                    await sink.DeleteArtifactAsync(descriptor.ArtifactId);
                    reclaimedBytes += descriptor.ByteLength;
                }
            }

            prunedIds.Add(manifest.Id);
            prunedManifests.Add(manifest);
            ReleaseRetiring(manifest.Id);
        }

        foreach (var pruned in prunedManifests)
        {
            inventory.RecordPruned(pruned);
        }

        if (prunedIds.Count > 0)
        {
            LatticeBackupMetrics.RecordRetention(ScopeKey, reclaimedBytes, prunedIds.Count);
        }

        if (blocked.Count > 0)
        {
            logger.LogInformation(
                "Retention kept {Blocked} backup(s) outside the policy for scope {Scope}: an incremental backup builds on each.",
                blocked.Count, ScopeKey);
        }

        logger.LogInformation(
            "Retention pruned {Pruned} backup(s) and retained {Retained} for scope {Scope}.",
            prunedIds.Count, manifests.Count - prunedIds.Count, ScopeKey);

        return new BackupRetentionReport(manifests.Count - prunedIds.Count, prunedIds);
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

            // The scope now has a live schedule, so a cycle is expected of it.
            // Publish it to the inventory registry here, after the reminder is
            // actually registered, so the per-scope status gauge reports a
            // measured "scheduled, nothing has completed yet" (0) rather than no
            // series at all, which an operator cannot tell apart from a scope
            // nobody ever scheduled. Non-destructive: a scope with a recorded
            // outcome keeps it (issue #2645).
            inventory.EnsureScopeRegistered(ScopeKey);
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
        if (!string.Equals(BackupScopeKey.For(scope), ScopeKey, StringComparison.Ordinal))
        {
            throw new ArgumentException("The backup scope must match the scheduler grain key.", nameof(scope));
        }

        if (!ScopeMatches(state.State.Scope, scope))
        {
            var previous = state.State.Scope;
            state.State.Scope = scope;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                // Restore the scope storage still holds: the ScopeMatches guard
                // would otherwise skip the write on a retry, and a schedule
                // registered over an unpersisted scope captures nothing once a
                // reminder reaches a fresh activation.
                state.State.Scope = previous;
                throw;
            }
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
