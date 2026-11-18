using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupSetSchedulerGrain"/> (issue #6067). Mirrors
/// the per-scope <see cref="BackupSchedulerGrain"/>: a durable reminder per
/// schedule, a cycle dispatched onto an interleaved one-shot grain timer so a
/// long capture never holds the reminder call (cf. #5473), an activation-local
/// overlap guard that coalesces a tick arriving mid-run, and a run that executes
/// system-origin because a reminder carries no caller (cf. #2608).
/// </summary>
internal sealed partial class BackupSetSchedulerGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    IGrainFactory grainFactory,
    ILatticeBackupCaptureService captureService,
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupSink sink,
    IOptions<LatticeBackupOptions> backupOptions,
    ILogger<BackupSetSchedulerGrain> logger,
    [PersistentState("backup-set-scheduler", LatticeOptions.StorageProviderName)]
    IPersistentState<BackupSetSchedulerState> state,
    IBackupPublicationGate? publicationGate = null,
    ILatticeBackupHealthStore? healthStore = null)
    : IGrainBase, IRemindable, ILatticeBackupSetSchedulerGrain
{
    /// <summary>The reminder that drives the schedule.</summary>
    internal const string ReminderName = "backup-set-schedule";

    /// <summary>How many runs the status keeps.</summary>
    internal const int RecentRunLimit = 20;

    private const int FailureReasonLimit = 300;

    // Activation-local overlap guard; never persisted, so a crash cannot wedge
    // the schedule.
    private bool _cycleInFlight;
    private IGrainTimer? _cycleTimer;
    private IBackupPublicationGate? _gate;

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    private string ScheduleKey => context.GrainId.Key.ToString()!;

    private IBackupPublicationGate Gate =>
        _gate ??= publicationGate ?? new DirectBackupPublicationGate(new BackupPublisher(catalog, sink));

    /// <inheritdoc />
    public async Task<BackupSetScheduleStatus> ScheduleAsync(LatticeBackupSetScheduleRequest request, BackupSetScheduleTarget target)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(target);
        var period = ClampInterval(request.Interval);
        var s = state.State;
        var previous = (s.Config, s.Target, s.Scheduled, s.RegisteredAtUtc);
        var reminderExists = await HasReminderAsync();
        var keepClock = reminderExists
            && s.Scheduled
            && s.Config is { } existing
            && ClampInterval(existing.Interval) == period;

        s.Config = BackupSetScheduleConfig.From(request);
        s.Target = target;
        s.Scheduled = true;
        if (!keepClock)
        {
            s.RegisteredAtUtc = DateTimeOffset.UtcNow;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            (s.Config, s.Target, s.Scheduled, s.RegisteredAtUtc) = previous;
            throw;
        }

        if (!keepClock)
        {
            await BackupReminderResilience.RunWithRetryAsync(
                () => reminderRegistry.RegisterOrUpdateReminder(context.GrainId, ReminderName, period, period),
                logger, nameof(reminderRegistry.RegisterOrUpdateReminder), ReminderName, ScheduleKey);
        }

        await Registry.RegisterAsync(ScheduleKey, EffectiveName(request, target));
        return BuildStatus(reminderRegistered: true);
    }

    /// <inheritdoc />
    public async Task<bool> PauseAsync()
    {
        if (state.State.Config is null)
        {
            return false;
        }

        await UnregisterReminderAsync();
        if (state.State.Scheduled)
        {
            state.State.Scheduled = false;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.Scheduled = true;
                throw;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public async Task<bool> RemoveAsync(bool deleteRetainedSets)
    {
        if (state.State.Config is null)
        {
            return false;
        }

        await UnregisterReminderAsync();
        var sets = state.State.Sets.ToList();

        // Holds are lifted before anything is deleted, and the index entry is
        // dropped before the state, so an interrupted removal is finished by a
        // retry rather than leaving a hold nothing will ever lift.
        await ApplyHoldsAsync(Array.Empty<ScheduledBackupSet>());
        if (deleteRetainedSets)
        {
            await DeleteSetsAsync(sets, Array.Empty<ScheduledBackupSet>());
        }

        await Registry.UnregisterAsync(ScheduleKey);
        await state.ClearStateAsync();
        state.State = new BackupSetSchedulerState();
        return true;
    }

    /// <inheritdoc />
    public async Task<BackupSetScheduleStatus?> GetStatusAsync()
    {
        if (state.State.Config is null)
        {
            return null;
        }

        return BuildStatus(await HasReminderAsync());
    }

    /// <inheritdoc />
    public async Task<bool> RunNowAsync()
    {
        if (state.State.Config is null)
        {
            throw new KeyNotFoundException("The backup-set schedule does not exist.");
        }

        if (_cycleInFlight)
        {
            LatticeBackupMetrics.RecordSchedulerOverrun(ScheduleKey);
            return false;
        }

        await StartCycleAsync(BackupSetScheduleTrigger.Manual);
        return true;
    }

    /// <inheritdoc />
    public async Task<BackupSetScheduleRun?> RunCycleAsync()
    {
        if (state.State.Config is null || _cycleInFlight)
        {
            return null;
        }

        _cycleInFlight = true;
        try
        {
            using var origin = LatticeAccessGateContext.EnterSystemOrigin();
            return await RunGuardedCycleAsync(BackupSetScheduleTrigger.Manual);
        }
        finally
        {
            _cycleInFlight = false;
        }
    }

    /// <inheritdoc />
    public Task<ScheduledBackupSet?> GetSetAsync(string setId)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);
        return Task.FromResult(state.State.Sets.FirstOrDefault(
            set => string.Equals(set.SetId, setId, StringComparison.OrdinalIgnoreCase)));
    }

    /// <summary>
    /// Handles the schedule's reminder by starting one run and returning without
    /// waiting for it; a tick that arrives while a run is in flight is coalesced
    /// into it and recorded as an overrun.
    /// </summary>
    /// <param name="reminderName">The reminder that fired.</param>
    /// <param name="status">The tick status.</param>
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != ReminderName || state.State.Config is null)
        {
            return;
        }

        if (!state.State.Scheduled)
        {
            // A reminder that outlived a pause (its unregister failed) stops here.
            await UnregisterReminderAsync();
            return;
        }

        if (_cycleInFlight)
        {
            LatticeBackupMetrics.RecordSchedulerOverrun(ScheduleKey);
            LatticeBackupMetrics.RecordSchedulerSkipped(ScheduleKey);
            logger.LogInformation(
                "Backup-set schedule {Schedule}: a tick arrived while the previous run was still in flight; it was coalesced into that run.",
                ScheduleKey);
            return;
        }

        await StartCycleAsync(BackupSetScheduleTrigger.Schedule);
    }

    private IBackupSetScheduleRegistryGrain Registry =>
        grainFactory.GetGrain<IBackupSetScheduleRegistryGrain>(IBackupSetScheduleRegistryGrain.SingletonKey);

    private async Task StartCycleAsync(BackupSetScheduleTrigger trigger)
    {
        _cycleInFlight = true;
        try
        {
            // The callback cannot run before this assignment: it is queued on the
            // activation and the current turn does not yield before it.
            _cycleTimer = this.RegisterGrainTimer(
                _ => RunTimerCycleAsync(trigger),
                new GrainTimerCreationOptions(TimeSpan.Zero, Timeout.InfiniteTimeSpan) { Interleave = true, KeepAlive = true });
            return;
        }
        catch
        {
            _cycleTimer = null;
        }

        // No grain runtime (a unit-test harness): run inline.
        await RunTimerCycleAsync(trigger);
    }

    private async Task RunTimerCycleAsync(BackupSetScheduleTrigger trigger)
    {
        try
        {
            using var origin = LatticeAccessGateContext.EnterSystemOrigin();
            await RunGuardedCycleAsync(trigger);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Backup-set schedule {Schedule}: the run faulted.", ScheduleKey);
        }
        finally
        {
            _cycleInFlight = false;
            Interlocked.Exchange(ref _cycleTimer, null)?.Dispose();
        }
    }

    /// <summary>
    /// One run: resolve the members, capture the set (retrying a fence that could
    /// not be established), record the set and the run, then apply retention. The
    /// caller holds the overlap guard and the system-origin scope.
    /// </summary>
    private async Task<BackupSetScheduleRun> RunGuardedCycleAsync(BackupSetScheduleTrigger trigger)
    {
        var request = state.State.Config!.ToRequest();
        var target = state.State.Target!;
        var options = backupOptions.Value;
        var startedAt = DateTimeOffset.UtcNow;
        state.State.LastRunUtc = startedAt;
        var attempts = 0;
        var memberCount = 0;
        try
        {
            var scopes = await ResolveScopesAsync(request, target);
            memberCount = scopes.Count;
            if (scopes.Count == 0)
            {
                throw new InvalidOperationException("The backup-set schedule resolved no trees to capture.");
            }

            var setName = $"{request.Name}-{startedAt:yyyyMMdd'T'HHmmss'Z'}";
            LatticeBackupSetCaptureResult result;
            while (true)
            {
                attempts++;
                try
                {
                    result = await captureService.CaptureSetAsync(
                        new LatticeBackupSetCaptureRequest(setName, scopes, request.CrossTreeConsistent));
                    break;
                }
                catch (LatticeBackupCrossTreeFenceException ex) when (attempts <= options.SetScheduleFenceRetries)
                {
                    LatticeBackupMetrics.RecordCaptureRetry(LatticeBackupMetrics.ReasonCrossTreeFence);
                    logger.LogInformation(
                        ex,
                        "Backup-set schedule {Schedule}: attempt {Attempt} could not establish a cross-tree fence; retrying in {Delay}.",
                        ScheduleKey, attempts, options.SetScheduleFenceRetryDelay);
                    await Task.Delay(options.SetScheduleFenceRetryDelay);
                }
            }

            var set = ToScheduledSet(result, setName);
            RecordSet(set);
            var finishedAt = DateTimeOffset.UtcNow;
            state.State.LastSuccessUtc = finishedAt;
            state.State.LastOutcome = BackupScopeRunOutcome.Success;
            state.State.LastFailureReason = null;
            state.State.ConsecutiveFailures = 0;
            var run = new BackupSetScheduleRun(
                startedAt, finishedAt, trigger, BackupScopeRunOutcome.Success, set.SetId, memberCount, attempts, null);
            RecordRun(run);
            await state.WriteStateAsync();
            await Registry.SetRetainedSetIdsAsync(ScheduleKey, state.State.Sets.Select(s => s.SetId).ToList());
            await ApplyRetentionAsync(request);
            logger.LogInformation(
                "Backup-set schedule {Schedule}: captured set {SetId} over {MemberCount} tree(s) in {Attempts} attempt(s).",
                ScheduleKey, set.SetId, memberCount, attempts);
            return run;
        }
        catch (Exception ex)
        {
            var reason = LatticeBackupMetrics.MapReason(ex);
            var outcome = ex is LatticeAuthorizationDeniedException ? BackupScopeRunOutcome.Denied : BackupScopeRunOutcome.Failure;
            LatticeBackupMetrics.RecordSchedulerFailure(ScheduleKey, reason);
            logger.LogWarning(ex, "Backup-set schedule {Schedule}: the run failed ({Reason}).", ScheduleKey, reason);
            var message = Describe(ex);
            state.State.LastOutcome = outcome;
            state.State.LastFailureReason = message;
            state.State.ConsecutiveFailures++;
            var run = new BackupSetScheduleRun(
                startedAt, DateTimeOffset.UtcNow, trigger, outcome, null, memberCount, Math.Max(1, attempts), message);
            RecordRun(run);
            await state.WriteStateAsync();
            return run;
        }
    }

    private async Task<IReadOnlyList<BackupScopeSelector>> ResolveScopesAsync(
        LatticeBackupSetScheduleRequest request, BackupSetScheduleTarget target)
    {
        if (!request.IncludeNewTrees)
        {
            return target.FixedScopes;
        }

        var scopes = await BackupSetMembership.ResolveAllTreesAsync(
            grainFactory, target.TenantTreePrefix, target.ExcludedTreePrefixes);
        state.State.LastResolvedScopes = scopes.ToList();
        return scopes;
    }

    private static ScheduledBackupSet ToScheduledSet(LatticeBackupSetCaptureResult result, string setName)
    {
        var manifest = result.SetManifest;
        var members = result.Members
            .Select(member => new ScheduledBackupSetMember(member.Manifest.Scope, member.BackupId))
            .OrderBy(member => member.TreeId, StringComparer.Ordinal)
            .ToList();

        // A single-member set is captured unstamped and has no SetId of its own;
        // it is recorded under the id its one member would have been given, which
        // the restore path recognises (issue #6067).
        var setId = manifest.SetId ?? BackupSetIdentity.Compute(new[] { members[0].BackupId });
        return new ScheduledBackupSet(
            setId, setName, manifest.CreatedAtUtc, manifest.CreatedAtUtc, manifest.CrossTreeConsistent, members);
    }

    private void RecordSet(ScheduledBackupSet set)
    {
        var sets = state.State.Sets;
        var index = sets.FindIndex(existing => string.Equals(existing.SetId, set.SetId, StringComparison.OrdinalIgnoreCase));
        if (index >= 0)
        {
            // Identical content re-captured: the same content-addressed set, now
            // last captured by this run.
            sets[index] = sets[index] with { LastCapturedAtUtc = set.LastCapturedAtUtc };
        }
        else
        {
            sets.Add(set);
        }

        sets.Sort(static (a, b) =>
        {
            var byTime = b.LastCapturedAtUtc.CompareTo(a.LastCapturedAtUtc);
            return byTime != 0 ? byTime : string.CompareOrdinal(b.SetId, a.SetId);
        });
    }

    private void RecordRun(BackupSetScheduleRun run)
    {
        var runs = state.State.RecentRuns;
        runs.Insert(0, run);
        if (runs.Count > RecentRunLimit)
        {
            runs.RemoveRange(RecentRunLimit, runs.Count - RecentRunLimit);
        }
    }

    private BackupSetScheduleStatus BuildStatus(bool reminderRegistered)
    {
        var s = state.State;
        var request = s.Config!.ToRequest();
        var target = s.Target!;
        var memberScopes = request.IncludeNewTrees
            ? (IReadOnlyList<BackupScopeSelector>)s.LastResolvedScopes.ToList()
            : target.FixedScopes;
        var scheduled = s.Scheduled && reminderRegistered;
        return new BackupSetScheduleStatus(
            request.Name,
            request,
            memberScopes,
            scheduled,
            scheduled ? NextRun(s.RegisteredAtUtc, ClampInterval(request.Interval), DateTimeOffset.UtcNow) : null,
            _cycleInFlight,
            s.RegisteredAtUtc,
            s.LastRunUtc,
            s.LastSuccessUtc,
            s.LastOutcome,
            s.LastFailureReason,
            s.ConsecutiveFailures,
            s.RecentRuns.ToList(),
            s.Sets.ToList());
    }

    /// <summary>
    /// The next time a reminder registered at <paramref name="anchor"/> with
    /// period <paramref name="period"/> fires after <paramref name="now"/>: its
    /// first tick is one period after the anchor.
    /// </summary>
    internal static DateTimeOffset NextRun(DateTimeOffset anchor, TimeSpan period, DateTimeOffset now)
    {
        var first = anchor + period;
        if (now < first)
        {
            return first;
        }

        var elapsed = now - first;
        var periods = (elapsed.Ticks / period.Ticks) + 1;
        return first + TimeSpan.FromTicks(periods * period.Ticks);
    }

    private async Task<bool> HasReminderAsync()
    {
        var reminder = await BackupReminderResilience.RunWithRetryAsync(
            () => reminderRegistry.GetReminder(context.GrainId, ReminderName),
            logger, nameof(reminderRegistry.GetReminder), ReminderName, ScheduleKey);
        return reminder is not null;
    }

    private async Task UnregisterReminderAsync()
    {
        var reminder = await BackupReminderResilience.RunWithRetryAsync(
            () => reminderRegistry.GetReminder(context.GrainId, ReminderName),
            logger, nameof(reminderRegistry.GetReminder), ReminderName, ScheduleKey);
        if (reminder is not null)
        {
            await BackupReminderResilience.RunWithRetryAsync(
                () => reminderRegistry.UnregisterReminder(context.GrainId, reminder),
                logger, nameof(reminderRegistry.UnregisterReminder), ReminderName, ScheduleKey);
        }
    }

    private static string EffectiveName(LatticeBackupSetScheduleRequest request, BackupSetScheduleTarget target) =>
        target.TenantTreePrefix is { } prefix && !request.Name.StartsWith(prefix, StringComparison.Ordinal)
            ? prefix + request.Name
            : request.Name;

    private static TimeSpan ClampInterval(TimeSpan interval) =>
        interval < LatticeBackupScheduleOptions.MinimumInterval
            ? LatticeBackupScheduleOptions.MinimumInterval
            : interval;

    private static string Describe(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        var newline = message.IndexOfAny(['\r', '\n']);
        if (newline >= 0)
        {
            message = message[..newline];
        }

        message = message.Trim();
        return message.Length > FailureReasonLimit ? message[..FailureReasonLimit] : message;
    }
}
