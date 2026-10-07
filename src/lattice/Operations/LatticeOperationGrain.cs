using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Timers;
using System.Diagnostics;

namespace Orleans.Lattice.Operations;

/// <summary>
/// The default <see cref="ILatticeOperationGrain"/>: the durable record of one
/// coordinated operation. Every write goes through <see cref="PersistAsync"/>, so a
/// read always answers from the last persisted record.
/// </summary>
/// <remarks>
/// Resuming an interrupted operation is deliberately out of scope: the work runs
/// in the accepting silo's <see cref="LatticeOperationRunner"/>, and when that
/// silo is declared dead, or the runner stops heartbeating for longer than the
/// lease, the next read records the operation as failed rather than leaving it
/// running forever. The caller starts it again.
/// Index bookkeeping waits at most 250 ms per polling turn. An already outstanding
/// index request adds no wait, and a reconciliation tick shares one 250 ms remote
/// budget. One such tick ahead of a poll therefore adds at most 500 ms total remote
/// bookkeeping wait. Legacy-record activation additionally allows 250 ms to install
/// recovery before migration; reminder faults are logged and retried by the timer,
/// not propagated to polls. Begin allows five seconds to install recovery before
/// accepting work. Storage, scheduling and unrelated queued requests are not bounded
/// here. Listings can lag while the durable outbox is pending.
/// </remarks>
internal sealed class LatticeOperationGrain(
    IGrainContext context,
    [PersistentState("lattice-operation", LatticeOptions.StorageProviderName)]
    IPersistentState<LatticeOperationGrainState> state,
    IGrainFactory grainFactory,
    ILatticeOperationSiloLiveness liveness,
    IOptions<LatticeOperationOptions> options,
    ILogger<LatticeOperationGrain> logger,
    IReminderRegistry reminderRegistry) : IGrainBase, ILatticeOperationGrain, IRemindable
{
    internal static readonly TimeSpan IndexWaitBudget = TimeSpan.FromMilliseconds(250);
    internal static readonly TimeSpan ReminderRegistrationWaitBudget = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan ReconcileWaitBudget = TimeSpan.FromMilliseconds(250);
    private const string IndexReminderName = "operation-index";
    private static readonly TimeSpan ReconcilePeriod = TimeSpan.FromSeconds(5);
    private IGrainTimer? _timer;
    private Task? _indexTask;
    private LatticeOperationRecord? _indexRecord;
    private bool _indexRemoval;
    private bool _reminderRegistered;

    /// <summary>Timer registration seam for tests without an Orleans runtime.</summary>
    internal Func<Func<CancellationToken, Task>, IGrainTimer>? TimerFactory { get; set; }

    /// <inheritdoc />
    public async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        if (!state.State.IndexOutboxInitialized && state.State.Record is { } record)
        {
            // Upgrade records written before the durable outbox existed.
            try
            {
                await reminderRegistry.RegisterOrUpdateReminder(
                    context.GrainId, IndexReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1))
                    .WaitAsync(ReconcileWaitBudget);
                _reminderRegistered = true;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Operation {OperationId} legacy recovery reminder registration failed; the timer will retry",
                    record.OperationId);
            }
            state.State.PendingIndexRecord = record;
            state.State.IndexOutboxInitialized = true;
            await state.WriteStateAsync();
        }
        if (state.State.PendingIndexRecord is not null || state.State.Record is { IsTerminal: false })
        {
            StartTimer();
        }
    }

    /// <inheritdoc />
    public Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        _timer?.Dispose();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ReceiveReminder(string reminderName, TickStatus status)
    {
        _reminderRegistered = true;
        return ReconcileAsync();
    }

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    /// <summary>
    /// The clock used for heartbeats, leases and retention. Defaults to
    /// <see cref="TimeProvider.System"/>; unit tests substitute a controllable one.
    /// </summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    private LatticeOperationOptions Options => options.Value;

    /// <inheritdoc />
    public async Task<LatticeOperationBeginResult> BeginAsync(LatticeOperationBeginRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.Kind);

        var now = Clock.GetUtcNow();
        var existing = await LoadLiveAsync(now);
        if (existing is not null)
        {
            if (!string.Equals(existing.Kind, request.Kind, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Operation '{existing.OperationId}' already exists with kind '{existing.Kind}', "
                    + $"so it cannot be started again as '{request.Kind}'. Choose a different operation id.");
            }

            return new LatticeOperationBeginResult(false, existing);
        }

        var (tenantId, operationId) = LatticeOperationKey.Parse(context.GrainId.Key.ToString()!);
        var record = new LatticeOperationRecord
        {
            OperationId = operationId,
            Kind = request.Kind,
            TenantId = tenantId,
            TreeIds = Copy(request.TreeIds),
            State = LatticeOperationState.Queued,
            Phase = LatticeOperationPhaseNames.Queued,
            PhaseCount = request.Phases.Count > 0 ? request.Phases.Count : null,
            Phases = Copy(request.Phases),
            StartedAtUtc = now,
            RunnerSilo = request.RunnerSilo,
            Attributes = Copy(request.Attributes),
        };

        // Register recovery before accepting work: a crash after the state write
        // must not strand an unindexed operation that nobody will poll again.
        if (!_reminderRegistered)
        {
            await reminderRegistry.RegisterOrUpdateReminder(
                context.GrainId, IndexReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1))
                .WaitAsync(ReminderRegistrationWaitBudget);
            _reminderRegistered = true;
        }
        state.State.PendingIndexRecord = record;
        state.State.PendingIndexRemoval = false;
        state.State.IndexOutboxInitialized = true;
        await PersistAsync(record, now);
        StartTimer();
        await FlushIndexAsync();
        return new LatticeOperationBeginResult(true, record);
    }

    /// <inheritdoc />
    public async Task<bool> ReportAsync(LatticeOperationProgressReport report)
    {
        ArgumentException.ThrowIfNullOrEmpty(report.Phase);

        var now = Clock.GetUtcNow();
        var record = await LoadLiveAsync(now);
        if (record is null || record.IsTerminal)
        {
            return true;
        }

        var completed = Math.Max(0, report.CompletedUnits);

        // Progress never goes backwards within a phase: a late or replayed report
        // carrying fewer units than already recorded keeps the recorded count.
        if (string.Equals(record.Phase, report.Phase, StringComparison.Ordinal))
        {
            completed = Math.Max(completed, record.CompletedUnits);
        }

        var phaseIndex = IndexOf(record.Phases, report.Phase);
        record = record with
        {
            State = LatticeOperationState.Running,
            Phase = report.Phase,
            PhaseIndex = phaseIndex,
            CompletedUnits = completed,
            TotalUnits = report.TotalUnits is { } total ? Math.Max(total, completed) : null,
            UnitName = report.UnitName,
        };

        await PersistAsync(record, now);
        return record.CancelRequested;
    }

    /// <inheritdoc />
    public async Task<bool> HeartbeatAsync()
    {
        var now = Clock.GetUtcNow();
        var record = await LoadLiveAsync(now);
        if (record is null || record.IsTerminal)
        {
            return true;
        }

        await PersistAsync(record, now);
        return record.CancelRequested;
    }

    /// <inheritdoc />
    public async Task<LatticeOperationRecord?> CompleteAsync(LatticeOperationCompletion completion)
    {
        ArgumentNullException.ThrowIfNull(completion);
        if (completion.State is LatticeOperationState.Queued or LatticeOperationState.Running)
        {
            throw new ArgumentException("A completion must carry a terminal state.", nameof(completion));
        }

        var record = state.State.Record;
        if (record is null || record.IsTerminal)
        {
            return record;
        }

        return await FinishAsync(record, completion, Clock.GetUtcNow());
    }

    /// <inheritdoc />
    public Task<LatticeOperationRecord?> GetAsync() => LoadLiveAsync(Clock.GetUtcNow());

    /// <inheritdoc />
    public async Task<LatticeOperationRecord?> RequestCancelAsync()
    {
        var now = Clock.GetUtcNow();
        var record = await LoadLiveAsync(now);
        if (record is null || record.IsTerminal || record.CancelRequested)
        {
            return record;
        }

        record = record with { CancelRequested = true };
        state.State.Record = record;
        await state.WriteStateAsync();
        return record;
    }

    /// <summary>
    /// Returns the live record: <see langword="null"/> when absent or past its
    /// retention (which also clears it), and failed first when its runner is lost.
    /// </summary>
    private async Task<LatticeOperationRecord?> LoadLiveAsync(DateTimeOffset now, bool flushIndex = true)
    {
        var record = state.State.Record;
        if (record is null)
        {
            return null;
        }

        if (record.IsTerminal)
        {
            if (record.FinishedAtUtc is { } finished && finished + Options.Retention <= now)
            {
                state.State.Record = null;
                state.State.PendingIndexRecord = record;
                state.State.PendingIndexRemoval = true;
                await state.WriteStateAsync();
                if (flushIndex)
                {
                    await FlushIndexAsync();
                }
                if (state.State.PendingIndexRecord is not null)
                {
                    StartTimer();
                }
                return null;
            }

            return record;
        }

        if (DescribeLoss(record, now) is { } reason)
        {
            logger.LogWarning(
                "Coordinated operation {OperationId} ({Kind}) failed as lost: {Reason}",
                record.OperationId, record.Kind, reason);
            return await FinishAsync(record, LatticeOperationCompletion.Failed(reason), now, flushIndex);
        }

        return record;
    }

    private string? DescribeLoss(LatticeOperationRecord record, DateTimeOffset now)
    {
        if (record.RunnerSilo is { } silo && liveness.IsDead(silo))
        {
            return $"The silo {silo} running this operation was lost. Resuming an interrupted operation "
                + "is not supported, so it is reported as failed; start it again.";
        }

        if (now - state.State.LastHeartbeatUtc > Options.HeartbeatLease)
        {
            return $"The operation's runner stopped reporting for longer than {Options.HeartbeatLease}. "
                + "Resuming an interrupted operation is not supported, so it is reported as failed; start it again.";
        }

        return null;
    }

    private async Task<LatticeOperationRecord> FinishAsync(
        LatticeOperationRecord record,
        LatticeOperationCompletion completion,
        DateTimeOffset now,
        bool flushIndex = true)
    {
        var succeeded = completion.State == LatticeOperationState.Succeeded;
        record = record with
        {
            State = completion.State,
            FailureReason = completion.FailureReason,
            ResultReference = completion.ResultReference,
            Result = Copy(completion.Result),
            FinishedAtUtc = now,
            Phase = succeeded ? LatticeOperationPhaseNames.Completed : record.Phase,
            PhaseIndex = succeeded ? null : record.PhaseIndex,
        };

        state.State.Record = record;
        state.State.PendingIndexRecord = record;
        state.State.PendingIndexRemoval = false;
        await state.WriteStateAsync();
        if (flushIndex)
        {
            await FlushIndexAsync();
        }
        if (state.State.PendingIndexRecord is not null)
        {
            StartTimer();
        }
        return record;
    }

    private void StartTimer() =>
        _timer ??= TimerFactory is { } factory
            ? factory(_ => ReconcileAsync())
            : this.RegisterGrainTimer(
                _ => ReconcileAsync(),
                new GrainTimerCreationOptions(ReconcilePeriod, ReconcilePeriod) { Interleave = false, KeepAlive = true });

    private async Task ReconcileAsync()
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            if (!_reminderRegistered && (state.State.Record is not null || state.State.PendingIndexRecord is not null))
            {
                await reminderRegistry.RegisterOrUpdateReminder(
                    context.GrainId, IndexReminderName, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1))
                    .WaitAsync(RemainingWait(started));
                _reminderRegistered = true;
            }
            await LoadLiveAsync(Clock.GetUtcNow(), flushIndex: false);
            await FlushIndexAsync(RemainingWait(started));
            if (state.State.Record is { IsTerminal: true } && state.State.PendingIndexRecord is null)
            {
                _timer?.Dispose();
                _timer = null;
            }
            if (state.State.Record is null && state.State.PendingIndexRecord is null)
            {
                var reminder = await reminderRegistry.GetReminder(context.GrainId, IndexReminderName)
                    .WaitAsync(RemainingWait(started));
                if (reminder is not null)
                {
                    await reminderRegistry.UnregisterReminder(context.GrainId, reminder).WaitAsync(RemainingWait(started));
                }
                _reminderRegistered = false;
                _timer?.Dispose();
                _timer = null;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Operation index reconciliation for {OperationGrain} will retry on its next tick", context.GrainId);
        }
    }

    private static TimeSpan RemainingWait(long started)
    {
        var remaining = ReconcileWaitBudget - Stopwatch.GetElapsedTime(started);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    private async Task FlushIndexAsync(TimeSpan? waitBudget = null)
    {
        if (_indexTask is { IsCompleted: false })
        {
            return;
        }

        if (_indexTask is not null)
        {
            if (!await ObserveIndexAsync())
            {
                return;
            }
        }

        if (state.State.PendingIndexRecord is not { } pending)
        {
            return;
        }

        var budget = waitBudget ?? IndexWaitBudget;
        if (budget <= TimeSpan.Zero)
        {
            return;
        }
        _indexRecord = pending;
        _indexRemoval = state.State.PendingIndexRemoval;
        _indexTask = UpdateIndexAsync(pending, _indexRemoval);
        try
        {
            await _indexTask.WaitAsync(budget);
        }
        catch (TimeoutException) when (!_indexTask.IsCompleted)
        {
            logger.LogWarning("Operation {OperationId} index acknowledgement exceeded {Budget}; reconciliation remains pending",
                pending.OperationId, IndexWaitBudget);
            return;
        }
        catch (Exception)
        {
            // ObserveIndexAsync logs the underlying fault and retains the outbox.
        }
        await ObserveIndexAsync();
    }

    private async Task<bool> ObserveIndexAsync()
    {
        var task = _indexTask!;
        _indexTask = null;
        try
        {
            await task;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Operation {OperationId} index update failed; reconciliation remains pending", _indexRecord!.OperationId);
            return false;
        }

        if (ReferenceEquals(state.State.PendingIndexRecord, _indexRecord)
            && state.State.PendingIndexRemoval == _indexRemoval)
        {
            state.State.PendingIndexRecord = null;
            state.State.PendingIndexRemoval = false;
            try
            {
                if (_indexRemoval && state.State.Record is null)
                {
                    await state.ClearStateAsync();
                }
                else
                {
                    await state.WriteStateAsync();
                }
            }
            catch
            {
                state.State.PendingIndexRecord = _indexRecord;
                state.State.PendingIndexRemoval = _indexRemoval;
                throw;
            }
        }
        return true;
    }

    private Task UpdateIndexAsync(LatticeOperationRecord record, bool remove) =>
        Index(record.TenantId).ReconcileAsync(record, remove);

    private async Task PersistAsync(LatticeOperationRecord record, DateTimeOffset heartbeat)
    {
        state.State.Record = record;
        state.State.LastHeartbeatUtc = heartbeat;
        await state.WriteStateAsync();
    }

    private ILatticeOperationIndexGrain Index(string tenantId) =>
        grainFactory.GetGrain<ILatticeOperationIndexGrain>(LatticeOperationKey.ForIndex(tenantId));

    // The record is durable state, so it keeps its own read-only copy of every
    // collection a caller hands in: a same-silo call skips the [Immutable] copy,
    // and the record must not share an instance the sender could still change,
    // nor hand readers one they could write into.
    private static IReadOnlyList<string> Copy(IReadOnlyList<string> values) =>
        values.Count == 0 ? [] : [.. values];

    private static IReadOnlyDictionary<string, string> Copy(IReadOnlyDictionary<string, string> values) =>
        values.Count == 0
            ? LatticeOperationRecord.EmptyResult
            : new System.Collections.ObjectModel.ReadOnlyDictionary<string, string>(new Dictionary<string, string>(values, StringComparer.Ordinal));

    private static int? IndexOf(IReadOnlyList<string> phases, string phase)
    {
        for (var i = 0; i < phases.Count; i++)
        {
            if (string.Equals(phases[i], phase, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return null;
    }
}
