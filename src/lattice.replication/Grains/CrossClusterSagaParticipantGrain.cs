using Microsoft.Extensions.DependencyInjection;
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
///   <item><description><b>Fence expiry.</b> If the decision has not arrived
///   by the fence deadline, ask the coordinator for the saga's durable decision
///   and apply it: commit on <c>Committed</c>, compensate on <c>Aborted</c>
///   (issue #4637). A participant that voted commit never compensates on its
///   timer alone, because the coordinator may already have committed the other
///   clusters. While the decision is pending or the coordinator cannot be
///   reached, the participant keeps its prepared state and fence, reports the
///   fence's age on <see cref="SagaParticipantFenceCensus.FenceHeldAge"/>, and
///   re-asks on every fence tick; an operator can resolve it through
///   <see cref="OperatorResolveAsync"/>. The request used is rebuilt from the
///   persisted state, set id included, so a backup-set decision reaches every
///   member tree.</description></item>
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

    /// <summary>
    /// Bound on one decision query to the coordinator. A query that outlasts it
    /// is treated as an unreachable coordinator: the fence stays up and the
    /// query is repeated on the next tick.
    /// </summary>
    private static readonly TimeSpan DecisionQueryTimeout = TimeSpan.FromSeconds(30);

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

        // The fence expired before the decision arrived. This participant voted
        // commit, so the coordinator may already have committed every other
        // cluster: compensating on the timer alone would leave this one on the
        // pre-restore tree while the others serve the restored copy (issue
        // #4637). Ask the coordinator for its durable decision instead.
        var (decision, fault) = await QueryCoordinatorDecisionAsync();
        switch (decision)
        {
            case CrossClusterSagaDecision.Committed:
                Logger.LogWarning(
                    "Cross-cluster saga participant {SagaId}: cutover fence expired; the coordinator reports the saga committed. Committing.",
                    SagaId);
                await ApplyCommitAsync(RequestFromState());
                return;

            case CrossClusterSagaDecision.Aborted:
                Logger.LogWarning(
                    "Cross-cluster saga participant {SagaId}: cutover fence expired; the coordinator reports the saga aborted. Compensating.",
                    SagaId);
                await ApplyAbortAsync(RequestFromState(), LatticeReplicationMetrics.SagaCauseVoteAbort,
                    "Aborted by coordinator decision, learned after the cutover fence expired.");
                return;

            case CrossClusterSagaDecision.InFlight:
                SagaParticipantFenceCensus.Hold(
                    SagaId, _state.State.FenceDeadlineTicks, SagaParticipantFenceCensus.ReasonDecisionPending);
                Logger.LogWarning(
                    "Cross-cluster saga participant {SagaId}: cutover fence expired while coordinator {Coordinator} is still deciding; keeping the fence up.",
                    SagaId, _state.State.CoordinatorClusterId);
                return;

            default:
                SagaParticipantFenceCensus.Hold(
                    SagaId, _state.State.FenceDeadlineTicks, SagaParticipantFenceCensus.ReasonCoordinatorUnreachable);
                Logger.LogError(fault,
                    "Cross-cluster saga participant {SagaId}: cutover fence expired and coordinator {Coordinator} cannot be reached; " +
                    "keeping the fence up rather than risk a mixed outcome. Restore the coordinator's reachability, or resolve the " +
                    "participant with ILatticeReplicationAdmin.ResolveCrossClusterSagaParticipantAsync once the coordinator is lost.",
                    SagaId, _state.State.CoordinatorClusterId);
                return;
        }
    }

    /// <summary>
    /// Asks the saga's coordinator cluster for its durable decision. Returns the
    /// decision, or <see langword="null"/> with the fault when the coordinator
    /// cannot be reached: no transport registered, the transport predates the
    /// verb, the call failed or timed out, or the coordinator refused the query.
    /// </summary>
    private async Task<(CrossClusterSagaDecision? Decision, Exception? Fault)> QueryCoordinatorDecisionAsync()
    {
        var channel = GrainContext.ActivationServices?.GetService<ISagaControlChannel>();
        if (channel is null || string.IsNullOrEmpty(_state.State.CoordinatorClusterId))
        {
            return (null, null);
        }

        try
        {
            using var timeout = new CancellationTokenSource(DecisionQueryTimeout);
            var response = await channel
                .GetDecisionAsync(_state.State.CoordinatorClusterId, RequestFromState(), timeout.Token)
                .WaitAsync(timeout.Token);
            return (response.Phase switch
            {
                SagaPhase.Committed => CrossClusterSagaDecision.Committed,
                SagaPhase.Aborted => CrossClusterSagaDecision.Aborted,
                _ => CrossClusterSagaDecision.InFlight,
            }, null);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }
    }

    /// <summary>
    /// <b>Operator override</b> for a participant whose coordinator is lost
    /// (issue #4637). Asks the coordinator first; if it answers with a decision,
    /// that decision is applied whatever was requested, and a request that
    /// contradicts it is refused. If the coordinator is still deciding, nothing
    /// changes and the call is refused, because the coordinator will decide. Only
    /// when the coordinator cannot be reached is <paramref name="commit"/>
    /// applied. Returns <see langword="true"/> when this call moved the
    /// participant to a terminal phase, <see langword="false"/> when it was
    /// already terminal.
    /// </summary>
    public async Task<bool> OperatorResolveAsync(bool commit)
    {
        if (_state.State.Phase != SagaPhase.Prepared)
        {
            if (_state.State.Phase == (commit ? SagaPhase.Committed : SagaPhase.Aborted))
                return false;
            throw new InvalidOperationException(
                $"Cross-cluster saga participant '{SagaId}' is {_state.State.Phase}; it cannot be resolved to {(commit ? "commit" : "abort")}.");
        }

        var (decision, _) = await QueryCoordinatorDecisionAsync();
        switch (decision)
        {
            case CrossClusterSagaDecision.Committed:
                await ApplyCommitAsync(RequestFromState());
                if (!commit)
                    throw new InvalidOperationException(
                        $"The coordinator of saga '{SagaId}' decided commit; the participant was committed and the requested abort was refused.");
                return true;

            case CrossClusterSagaDecision.Aborted:
                await ApplyAbortAsync(RequestFromState(), LatticeReplicationMetrics.SagaCauseVoteAbort,
                    "Aborted by coordinator decision, learned by an operator resolution.");
                if (commit)
                    throw new InvalidOperationException(
                        $"The coordinator of saga '{SagaId}' decided abort; the participant was compensated and the requested commit was refused.");
                return true;

            case CrossClusterSagaDecision.InFlight:
                throw new InvalidOperationException(
                    $"The coordinator of saga '{SagaId}' is reachable and still deciding; the participant was left prepared. Let the coordinator decide.");
        }

        if (commit)
        {
            await ApplyCommitAsync(RequestFromState());
        }
        else
        {
            await ApplyAbortAsync(RequestFromState(), LatticeReplicationMetrics.SagaCauseCoordinatorLoss,
                "Compensated by an operator after the coordinator was lost.");
        }

        return true;
    }

    /// <inheritdoc />
    public async Task<SagaControlResponse> PrepareAsync(SagaControlRequest request)
    {
        // Zero-prime both members of the compensation-cause taxonomy before the
        // idempotent re-attach below can return (issue #2918). Both arms are
        // reachable only from SagaPhase.Prepared - `vote-abort` from a
        // coordinator abort decision (delivered, or learned after the fence
        // expired) and `coordinator-loss` from an operator's resolution of a
        // participant whose coordinator was lost (issue #4637) - so the prepare
        // entry point is exactly the population that can arm either, and is the
        // same execution path rather than a constructor that proves only that
        // the type loaded.
        //
        // Before this, only the cause that had already fired existed as a series.
        // A cluster that has never lost a coordinator produced nothing for
        // `coordinator-loss`, which scrapes identically to a build where the
        // fence-expiry path was removed - so a flat absence could not be read as
        // "no coordinator was ever lost", the single reading the instrument
        // exists to support. Adding zero to a counter is the identity.
        //
        // Above the early return, not below it: a duplicate prepare returns at
        // the guard below, and a prime underneath it would be unreachable on
        // exactly the re-attach path whose absence it is meant to make readable.
        //
        // Written out rather than looped so the sibling priming-enrolment gate,
        // which reads literal `new KeyValuePair<string, object?>(...)` arguments
        // on a zero-valued Add, can see both arms.
        //
        // The issue filed this as unprimable because LatticeReplicationMetrics is
        // a static class with no silo-startup hook. The instrument is emitted
        // from a grain, and the emitting grain has an entry point.
        LatticeReplicationMetrics.SagaCompensations.Add(0,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagCause, LatticeReplicationMetrics.SagaCauseVoteAbort),
            LatticeTenantLabel.Platform);
        LatticeReplicationMetrics.SagaCompensations.Add(0,
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagCause, LatticeReplicationMetrics.SagaCauseCoordinatorLoss),
            LatticeTenantLabel.Platform);

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
        _state.State.SetId = request.SetId;

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
        // no prepared state leaks, then vote abort. SafeAbortAsync swallows
        // per-participant faults, so the compensations are side-effect isolated
        // from one another and mutually independent - see
        // CompensateParticipantsAsync for the full argument.
        await BoundedFanOut.ForEachAsync(prepared, BoundedFanOut.DefaultWidth,
            participant => SafeAbortAsync(participant, request));
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
                await ApplyCommitAsync(request);
                break;
            case SagaPhase.Committed:
                // Idempotent duplicate commit.
                break;
            default:
                // Commit for a saga that was never prepared, or that already
                // aborted. A participant that voted commit compensates only on
                // the coordinator's abort decision, so this is a conflict the
                // coordinator must not count as delivered: the decision is not
                // applied, and the durable phase is returned so it observes the
                // refusal (issue #4637).
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
                await ApplyAbortAsync(request, LatticeReplicationMetrics.SagaCauseVoteAbort, "Aborted by coordinator decision.");
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
    /// Commits every local participant and records the Committed phase. Every
    /// participant voted commit, so the decision is already made and delivery is
    /// unconditional: no participant's commit can change whether another's is
    /// issued, and each targets its own resource, so they are issued in bounded
    /// overlapped waves. Group atomicity holds - the wave settles completely
    /// before the terminal phase is persisted, so a fault in any participant
    /// surfaces before Committed is written.
    /// </summary>
    private async Task ApplyCommitAsync(SagaControlRequest request)
    {
        await BoundedFanOut.ForEachAsync(_participants, BoundedFanOut.DefaultWidth,
            participant => participant.CommitAsync(request));
        _state.State.Phase = SagaPhase.Committed;
        _state.State.FenceDeadlineTicks = 0;
        _state.State.Detail = null;
        await _state.WriteStateAsync();
        SagaParticipantFenceCensus.Release(SagaId);
        await UnregisterFenceAsync();
        await SlideTtlAsync();
    }

    /// <summary>
    /// Compensates every local participant and records the Aborted phase,
    /// counting the compensation under <paramref name="cause"/>.
    /// </summary>
    private async Task ApplyAbortAsync(SagaControlRequest request, string cause, string detail)
    {
        await CompensateParticipantsAsync(request);
        LatticeReplicationMetrics.SagaCompensations.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagCause, cause),
            LatticeTenantLabel.Platform);
        _state.State.Phase = SagaPhase.Aborted;
        _state.State.Vote = SagaVote.Abort;
        _state.State.FenceDeadlineTicks = 0;
        _state.State.Detail = detail;
        await _state.WriteStateAsync();
        SagaParticipantFenceCensus.Release(SagaId);
        await UnregisterFenceAsync();
        await SlideTtlAsync();
    }

    /// <summary>
    /// Compensates (rolls back) every local participant, swallowing per-participant
    /// abort faults so one failure does not strand the others. Used by the
    /// coordinator-driven abort, an abort learned after the fence expired, and an
    /// operator resolution.
    /// <para>
    /// Issued in bounded overlapped waves. The compensations are mutually
    /// independent (each participant rolls back only its own resource) and
    /// <see cref="SafeAbortAsync"/> already isolates their faults, which is the
    /// same "one failure must not strand the others" property the serial loop
    /// relied on - it is preserved exactly, because every wave settles through
    /// <c>Task.WhenAll</c> and each body has already swallowed its own fault.
    /// The whole group still completes before this method returns, so the
    /// caller's terminal-phase write remains group-atomic.
    /// </para>
    /// </summary>
    private Task CompensateParticipantsAsync(SagaControlRequest request) =>
        BoundedFanOut.ForEachAsync(_participants, BoundedFanOut.DefaultWidth,
            participant => SafeAbortAsync(participant, request));

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

    /// <summary>
    /// Rebuilds the control request from persisted identity fields, set id
    /// included (state written before #4637 carries none, so such a saga's
    /// rebuilt request takes the single-tree path).
    /// </summary>
    private SagaControlRequest RequestFromState() => new()
    {
        SagaId = SagaId,
        TargetTree = _state.State.TargetTree,
        ManifestId = _state.State.ManifestId,
        CoordinatorClusterId = _state.State.CoordinatorClusterId,
        SetId = _state.State.SetId,
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
