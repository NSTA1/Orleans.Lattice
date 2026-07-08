using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable participant model for a cross-cluster saga. See
/// <see cref="ICrossClusterSagaParticipantGrain"/> for the contract. One
/// activation per saga id (this grain's key). Resolves the local
/// <see cref="ISagaParticipant"/>(s) for the saga's target resource set and
/// drives them through:
/// <list type="number">
///   <item><description><b>Prepare.</b> Run each participant's resumable
///   prepare; if every one prepares, durably record
///   <see cref="SagaPhase.Prepared"/>, arm the cutover fence reminder, and vote
///   <see cref="SagaVote.Commit"/>. Any non-committing participant votes
///   <see cref="SagaVote.Abort"/> and every already-prepared participant is
///   compensated.</description></item>
///   <item><description><b>Commit / Abort.</b> Deliver the coordinator decision
///   to each participant, cancel the fence, and persist the terminal
///   phase.</description></item>
///   <item><description><b>Fence expiry.</b> If the decision never arrives
///   before the fence deadline, auto-compensate (roll back) - the
///   coordinator-loss safety net.</description></item>
/// </list>
/// The fence is durable because it is anchored on an Orleans reminder (grain
/// timers do not survive deactivation); retention cleanup reuses the
/// <see cref="TtlGrain{TSelf}"/> reminder lifecycle. Every RPC is idempotent.
/// </summary>
internal sealed class CrossClusterSagaParticipantGrain : TtlGrain<CrossClusterSagaParticipantGrain>, ICrossClusterSagaParticipantGrain
{
    private const string FenceReminderName = "saga-participant-fence";
    private const string RetentionReminderName = "saga-participant-retention";

    /// <summary>
    /// The bounded cutover fence window a prepared participant holds while
    /// waiting for the coordinator decision. Must exceed the coordinator's
    /// decide-and-deliver latency; past it the participant auto-compensates. A
    /// build-progress (prepare) deadline is a separate, longer coordinator-side
    /// concern.
    /// </summary>
    private static readonly TimeSpan FenceWindow = TimeSpan.FromMinutes(5);

    private readonly IReadOnlyList<ISagaParticipant> _participants;
    private readonly IOptionsMonitor<LatticeOptions> _optionsMonitor;
    private readonly IPersistentState<CrossClusterSagaParticipantState> _state;

    /// <summary>
    /// Creates the participant grain. Resolves every local
    /// <see cref="ISagaParticipant"/> so the grain can act over the whole
    /// resource set the cluster hosts for the saga.
    /// </summary>
    public CrossClusterSagaParticipantGrain(
        IGrainContext context,
        IEnumerable<ISagaParticipant> participants,
        IReminderRegistry reminderRegistry,
        IOptionsMonitor<LatticeOptions> optionsMonitor,
        ILogger<CrossClusterSagaParticipantGrain> logger,
        [PersistentState("saga-participant", LatticeOptions.StorageProviderName)]
        IPersistentState<CrossClusterSagaParticipantState> state)
        : base(context, reminderRegistry, logger)
    {
        ArgumentNullException.ThrowIfNull(participants);
        _participants = participants as IReadOnlyList<ISagaParticipant> ?? participants.ToArray();
        _optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    /// <summary>This participant's key (the saga id).</summary>
    private string SagaId => GrainContext.GrainId.Key.ToString()!;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => _optionsMonitor.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Cross-cluster saga participant {SagaId}: retention window expired; clearing state.",
            SagaId);
        await _state.ClearStateAsync();
    }

    /// <inheritdoc />
    protected override async Task OnOtherReminderAsync(string reminderName, TickStatus status)
    {
        if (reminderName != FenceReminderName) return;

        if (_state.State.Phase != SagaPhase.Prepared)
        {
            // The decision already arrived (terminal phase) - the fence is
            // obsolete. Cancel it idempotently.
            await UnregisterFenceAsync();
            return;
        }

        if (DateTime.UtcNow.Ticks < _state.State.FenceDeadlineTicks)
        {
            // Reminder tick before the deadline (Orleans minimum period is
            // coarse). Keep waiting for the coordinator decision.
            return;
        }

        // Coordinator-loss safety net: the decision never arrived before the
        // fence expired. Auto-compensate (roll back) so the prepared action
        // does not leak, and record the abort.
        Logger.LogWarning(
            "Cross-cluster saga participant {SagaId}: cutover fence expired without a coordinator " +
            "decision; auto-compensating.",
            SagaId);
        await CompensateParticipantsAsync();
        _state.State.Phase = SagaPhase.Aborted;
        _state.State.Vote = SagaVote.Abort;
        _state.State.Detail = "Cutover fence expired without a coordinator decision; auto-compensated.";
        _state.State.FenceDeadlineTicks = 0;
        await _state.WriteStateAsync();
        await UnregisterFenceAsync();
        await SlideTtlAsync();
    }

    /// <inheritdoc />
    public async Task<SagaControlResponse> PrepareAsync(SagaControlRequest request)
    {
        // Idempotent re-attach: a duplicate prepare returns the recorded
        // vote/phase without re-running the participants' prepare work.
        if (_state.State.Phase != SagaPhase.None)
        {
            return BuildResponse(_state.State.Vote);
        }

        _state.State.SagaId = SagaId;
        _state.State.TargetTree = request.TargetTree;
        _state.State.ManifestId = request.ManifestId;
        _state.State.CoordinatorClusterId = request.CoordinatorClusterId;

        // No local participant hosts anything for this saga: the safe default is
        // to vote abort (nothing prepared, nothing to commit).
        if (_participants.Count == 0)
        {
            _state.State.Phase = SagaPhase.Aborted;
            _state.State.Vote = SagaVote.Abort;
            _state.State.Detail = "No local saga participant is hosted on this cluster.";
            await _state.WriteStateAsync();
            await SlideTtlAsync();
            return BuildResponse(SagaVote.Abort);
        }

        // Run every local participant's resumable prepare. A participant that
        // cannot prepare self-compensates per the SPI contract; if any one
        // declines, we compensate the ones that did prepare and vote abort.
        var prepared = new List<ISagaParticipant>(_participants.Count);
        var allCommit = true;
        string? abortDetail = null;
        foreach (var participant in _participants)
        {
            var result = await participant.PrepareAsync(request);
            if (result.Vote == SagaVote.Commit)
            {
                prepared.Add(participant);
            }
            else
            {
                allCommit = false;
                abortDetail ??= result.Detail;
            }
        }

        if (allCommit)
        {
            _state.State.Phase = SagaPhase.Prepared;
            _state.State.Vote = SagaVote.Commit;
            _state.State.Detail = null;
            _state.State.FenceDeadlineTicks = DateTime.UtcNow.Ticks + FenceWindow.Ticks;
            await _state.WriteStateAsync();
            await RegisterFenceAsync();
            return BuildResponse(SagaVote.Commit);
        }

        // At least one participant declined: compensate the prepared subset so
        // no prepared state leaks, then vote abort.
        foreach (var participant in prepared)
        {
            await SafeAbortAsync(participant, request);
        }
        _state.State.Phase = SagaPhase.Aborted;
        _state.State.Vote = SagaVote.Abort;
        _state.State.Detail = abortDetail ?? "A local saga participant declined to prepare.";
        await _state.WriteStateAsync();
        await SlideTtlAsync();
        return BuildResponse(SagaVote.Abort);
    }

    /// <inheritdoc />
    public async Task<SagaControlResponse> CommitAsync(SagaControlRequest request)
    {
        switch (_state.State.Phase)
        {
            case SagaPhase.Prepared:
                foreach (var participant in _participants)
                {
                    await participant.CommitAsync(request);
                }
                _state.State.Phase = SagaPhase.Committed;
                _state.State.FenceDeadlineTicks = 0;
                _state.State.Detail = null;
                await _state.WriteStateAsync();
                await UnregisterFenceAsync();
                await SlideTtlAsync();
                break;
            case SagaPhase.Committed:
                // Idempotent duplicate commit.
                break;
            default:
                // Commit for a saga that was never prepared, or that already
                // aborted / compensated (for example after a fence expiry). The
                // decision is not applied; the durable phase is returned so the
                // coordinator observes the conflict.
                Logger.LogWarning(
                    "Cross-cluster saga participant {SagaId}: commit received in phase {Phase}; not applied.",
                    SagaId, _state.State.Phase);
                break;
        }

        return BuildStatusResponse();
    }

    /// <inheritdoc />
    public async Task<SagaControlResponse> AbortAsync(SagaControlRequest request)
    {
        switch (_state.State.Phase)
        {
            case SagaPhase.Prepared:
                await CompensateParticipantsAsync(request);
                _state.State.Phase = SagaPhase.Aborted;
                _state.State.Vote = SagaVote.Abort;
                _state.State.FenceDeadlineTicks = 0;
                _state.State.Detail = "Aborted by coordinator decision.";
                await _state.WriteStateAsync();
                await UnregisterFenceAsync();
                await SlideTtlAsync();
                break;
            case SagaPhase.None:
                // Abort for a saga that was never prepared: record aborted so a
                // later prepare cannot resurrect it, and so status is stable.
                _state.State.SagaId = SagaId;
                _state.State.Phase = SagaPhase.Aborted;
                _state.State.Vote = SagaVote.Abort;
                _state.State.Detail = "Aborted before prepare.";
                await _state.WriteStateAsync();
                await SlideTtlAsync();
                break;
            case SagaPhase.Aborted:
                // Idempotent duplicate abort.
                break;
            default:
                // Abort after commit: cannot un-commit. Return the durable phase
                // so the coordinator observes the conflict.
                Logger.LogWarning(
                    "Cross-cluster saga participant {SagaId}: abort received in phase {Phase}; not applied.",
                    SagaId, _state.State.Phase);
                break;
        }

        return BuildStatusResponse();
    }

    /// <inheritdoc />
    public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request) =>
        Task.FromResult(BuildStatusResponse());

    /// <summary>
    /// Compensates (rolls back) every local participant, swallowing per-participant
    /// abort faults so one failure does not strand the others. Used by both the
    /// coordinator-driven abort and the fence-expiry auto-compensation.
    /// </summary>
    private Task CompensateParticipantsAsync() => CompensateParticipantsAsync(RequestFromState());

    private async Task CompensateParticipantsAsync(SagaControlRequest request)
    {
        foreach (var participant in _participants)
        {
            await SafeAbortAsync(participant, request);
        }
    }

    private async Task SafeAbortAsync(ISagaParticipant participant, SagaControlRequest request)
    {
        try
        {
            await participant.AbortAsync(request);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cross-cluster saga participant {SagaId}: a local participant abort faulted (non-fatal).",
                SagaId);
        }
    }

    /// <summary>Rebuilds the control request from persisted identity fields.</summary>
    private SagaControlRequest RequestFromState() => new()
    {
        SagaId = SagaId,
        TargetTree = _state.State.TargetTree,
        ManifestId = _state.State.ManifestId,
        CoordinatorClusterId = _state.State.CoordinatorClusterId,
    };

    /// <summary>Builds a prepare-style response carrying the supplied vote.</summary>
    private SagaControlResponse BuildResponse(SagaVote vote) => new()
    {
        SagaId = SagaId,
        Phase = _state.State.Phase,
        Vote = vote,
        Detail = _state.State.Detail ?? string.Empty,
    };

    /// <summary>
    /// Builds a status-style response (vote slot not meaningful) carrying the
    /// durable phase.
    /// </summary>
    private SagaControlResponse BuildStatusResponse() => new()
    {
        SagaId = SagaId,
        Phase = _state.State.Phase,
        Vote = SagaVote.None,
        Detail = _state.State.Detail ?? string.Empty,
    };

    private async Task RegisterFenceAsync()
    {
        try
        {
            await ReminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: GrainContext.GrainId,
                reminderName: FenceReminderName,
                dueTime: FenceWindow,
                period: FenceWindow);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cross-cluster saga participant {SagaId}: failed to register fence reminder (non-fatal).",
                SagaId);
        }
    }

    private async Task UnregisterFenceAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, FenceReminderName);
            if (reminder is not null)
            {
                await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cross-cluster saga participant {SagaId}: failed to unregister fence reminder (non-fatal).",
                SagaId);
        }
    }
}
