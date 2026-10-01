using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
/// </remarks>
internal sealed class LatticeOperationGrain(
    IGrainContext context,
    [PersistentState("lattice-operation", LatticeOptions.StorageProviderName)]
    IPersistentState<LatticeOperationGrainState> state,
    IGrainFactory grainFactory,
    ILatticeOperationSiloLiveness liveness,
    IOptions<LatticeOperationOptions> options,
    ILogger<LatticeOperationGrain> logger) : IGrainBase, ILatticeOperationGrain
{
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
            TreeIds = request.TreeIds,
            State = LatticeOperationState.Queued,
            Phase = LatticeOperationPhaseNames.Queued,
            PhaseCount = request.Phases.Count > 0 ? request.Phases.Count : null,
            Phases = request.Phases,
            StartedAtUtc = now,
            RunnerSilo = request.RunnerSilo,
            Attributes = request.Attributes,
        };

        await PersistAsync(record, now);
        await Index(tenantId).AddAsync(operationId, record.Kind, now);
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
    private async Task<LatticeOperationRecord?> LoadLiveAsync(DateTimeOffset now)
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
                await state.ClearStateAsync();
                await Index(record.TenantId).RemoveAsync(record.OperationId);
                return null;
            }

            return record;
        }

        if (DescribeLoss(record, now) is { } reason)
        {
            logger.LogWarning(
                "Coordinated operation {OperationId} ({Kind}) failed as lost: {Reason}",
                record.OperationId, record.Kind, reason);
            return await FinishAsync(record, LatticeOperationCompletion.Failed(reason), now);
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
        DateTimeOffset now)
    {
        var succeeded = completion.State == LatticeOperationState.Succeeded;
        record = record with
        {
            State = completion.State,
            FailureReason = completion.FailureReason,
            ResultReference = completion.ResultReference,
            Result = completion.Result,
            FinishedAtUtc = now,
            Phase = succeeded ? LatticeOperationPhaseNames.Completed : record.Phase,
            PhaseIndex = succeeded ? null : record.PhaseIndex,
        };

        state.State.Record = record;
        await state.WriteStateAsync();
        await Index(record.TenantId).MarkFinishedAsync(record.OperationId, now);
        return record;
    }

    private async Task PersistAsync(LatticeOperationRecord record, DateTimeOffset heartbeat)
    {
        state.State.Record = record;
        state.State.LastHeartbeatUtc = heartbeat;
        await state.WriteStateAsync();
    }

    private ILatticeOperationIndexGrain Index(string tenantId) =>
        grainFactory.GetGrain<ILatticeOperationIndexGrain>(LatticeOperationKey.ForIndex(tenantId));

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
