using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Coordinator for a cross-cluster saga. See
/// <see cref="ICrossClusterSagaCoordinatorGrain"/> for the contract. One
/// activation per saga id (this grain's key). Drives the saga over the
/// participant clusters' control-channel endpoints:
/// <list type="number">
///   <item><description><b>Prepare.</b> Dispatch
///   <see cref="ISagaControlChannel.PrepareAsync"/> to every participant
///   cluster and collect votes.</description></item>
///   <item><description><b>Decide.</b> When every participant votes
///   <see cref="SagaVote.Commit"/>, persist
///   <see cref="CrossClusterSagaPhase.Committed"/> - the single global decision
///   moment. Any non-committing vote persists
///   <see cref="CrossClusterSagaPhase.Aborted"/>.</description></item>
///   <item><description><b>Finalize.</b> Fan out
///   <see cref="ISagaControlChannel.CommitAsync"/> (on commit) or
///   <see cref="ISagaControlChannel.AbortAsync"/> (on abort / compensation) to
///   every prepared participant, then complete.</description></item>
/// </list>
/// Crash recovery is reminder-driven: a keepalive reminder resumes
/// <see cref="RunCoordinatorAsync"/> from the persisted phase, and every step
/// (prepare, decide, finalize) is idempotent. Retention cleanup reuses the
/// <see cref="TtlGrain{TSelf}"/> reminder lifecycle.
/// </summary>
internal sealed class CrossClusterSagaCoordinatorGrain(
    IGrainContext context,
    ISagaControlChannel controlChannel,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<CrossClusterSagaCoordinatorGrain> logger,
    [PersistentState("saga-coordinator", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossClusterSagaCoordinatorState> state)
    : TtlGrain<CrossClusterSagaCoordinatorGrain>(context, reminderRegistry, logger),
      ICrossClusterSagaCoordinatorGrain
{
    private const string KeepaliveReminderName = "saga-coordinator-keepalive";
    private const string RetentionReminderName = "saga-coordinator-retention";

    /// <summary>
    /// Coordinator-side prepare-progress (build) deadline: the wall-clock window
    /// a participant's long-running prepare is allowed to consume before the
    /// coordinator gives up and aborts. Distinct from - and deliberately far
    /// longer than - each participant's short cutover fence timer.
    /// </summary>
    private static readonly TimeSpan PrepareProgressDeadline = TimeSpan.FromHours(1);

    private readonly ISagaControlChannel _controlChannel =
        controlChannel ?? throw new ArgumentNullException(nameof(controlChannel));

    /// <summary>This coordinator's key (the saga id).</summary>
    private string SagaId => GrainContext.GrainId.Key.ToString()!;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Cross-cluster saga {SagaId}: retention window expired; clearing coordinator state.",
            SagaId);
        await state.ClearStateAsync();
    }

    /// <inheritdoc />
    protected override async Task OnOtherReminderAsync(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;

        switch (state.State.Phase)
        {
            case CrossClusterSagaPhase.Preparing:
            case CrossClusterSagaPhase.Committed:
            case CrossClusterSagaPhase.Aborted:
                try
                {
                    await RunCoordinatorAsync();
                }
                catch (Exception ex)
                {
                    Logger.LogWarning(ex,
                        "Cross-cluster saga {SagaId} failed on reminder-driven resume.",
                        SagaId);
                }
                break;
            case CrossClusterSagaPhase.Completed:
                // A crash between persisting Completed and arming retention
                // lands here with the keepalive still registered. Arm retention
                // idempotently, unregister the keepalive, and deactivate.
                await UnregisterKeepaliveAsync();
                await SlideTtlAsync();
                this.DeactivateOnIdle();
                break;
            case CrossClusterSagaPhase.NotStarted:
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
                break;
        }
    }

    /// <inheritdoc />
    public async Task<CrossClusterSagaOutcome> RunAsync(
        List<string> participantClusterIds,
        string targetTree,
        string manifestId,
        string coordinatorClusterId,
        string? setId = null)
    {
        ArgumentNullException.ThrowIfNull(participantClusterIds);
        ArgumentNullException.ThrowIfNull(targetTree);
        ArgumentNullException.ThrowIfNull(manifestId);
        ArgumentException.ThrowIfNullOrEmpty(coordinatorClusterId);

        // Idempotent re-attach to an already-decided coordinator: return the
        // memoized verdict without re-running.
        if (state.State.Phase == CrossClusterSagaPhase.Completed)
        {
            return state.State.Outcome ?? CrossClusterSagaOutcome.Committed;
        }

        if (state.State.Phase == CrossClusterSagaPhase.NotStarted)
        {
            var participants = BuildParticipants(participantClusterIds);

            // Empty participant set: vacuous commit. Nothing to prepare, decide,
            // or finalize.
            if (participants.Count == 0)
            {
                state.State.SagaId = SagaId;
                state.State.TargetTree = targetTree;
                state.State.ManifestId = manifestId;
                state.State.CoordinatorClusterId = coordinatorClusterId;
                state.State.SetId = setId;
                state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
                state.State.Phase = CrossClusterSagaPhase.Completed;
                state.State.Outcome = CrossClusterSagaOutcome.Committed;
                await state.WriteStateAsync();
                // This path never registers a keepalive, so the retention TTL
                // reminder is the only cleanup trigger.
                await SlideTtlAsync();
                return CrossClusterSagaOutcome.Committed;
            }

            state.State.SagaId = SagaId;
            state.State.TargetTree = targetTree;
            state.State.ManifestId = manifestId;
            state.State.CoordinatorClusterId = coordinatorClusterId;
            state.State.SetId = setId;
            state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
            state.State.Participants = participants;
            state.State.Fingerprint = ComputeFingerprint(participants, targetTree, manifestId, setId);
            state.State.Phase = CrossClusterSagaPhase.Preparing;
            await RegisterKeepaliveAsync();
            await state.WriteStateAsync();
        }
        else
        {
            // Re-submit of an in-flight coordinator: enforce participant-set /
            // target / manifest / set stability, mirroring the intra-cluster saga's
            // idempotency contract.
            var incoming = ComputeFingerprint(BuildParticipants(participantClusterIds), targetTree, manifestId, setId);
            if (state.State.Fingerprint is not { } persisted
                || !CryptographicOperations.FixedTimeEquals(persisted, incoming))
            {
                throw new InvalidOperationException(
                    $"Cross-cluster saga '{SagaId}' was previously submitted with a different participant " +
                    "set, target tree, manifest id, or set id; reuse of a saga id requires the exact same arguments.");
            }
        }

        await RunCoordinatorAsync();
        return state.State.Outcome ?? CrossClusterSagaOutcome.Committed;
    }

    /// <inheritdoc />
    public Task<CrossClusterSagaDecision> GetDecisionAsync()
    {
        var decision = state.State.Phase switch
        {
            CrossClusterSagaPhase.Committed => CrossClusterSagaDecision.Committed,
            CrossClusterSagaPhase.Aborted => CrossClusterSagaDecision.Aborted,
            CrossClusterSagaPhase.Completed =>
                state.State.Outcome == CrossClusterSagaOutcome.Committed
                    ? CrossClusterSagaDecision.Committed
                    : CrossClusterSagaDecision.Aborted,
            _ => CrossClusterSagaDecision.InFlight,
        };
        return Task.FromResult(decision);
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(state.State.Phase is CrossClusterSagaPhase.Completed or CrossClusterSagaPhase.NotStarted);

    /// <summary>
    /// Resumable phase machine. Each branch is idempotent so a coordinator crash
    /// between any two steps is recovered by a re-issued call (direct or
    /// reminder-driven). Advances Preparing -&gt; Committed/Aborted -&gt;
    /// Completed.
    /// </summary>
    private async Task RunCoordinatorAsync()
    {
        if (state.State.Phase == CrossClusterSagaPhase.Preparing)
        {
            await PreparePhaseAsync();
        }

        if (state.State.Phase is CrossClusterSagaPhase.Committed or CrossClusterSagaPhase.Aborted)
        {
            await FinalizePhaseAsync();
        }
    }

    /// <summary>
    /// Dispatches prepare to every participant cluster, collects votes, and
    /// records the single global decision (Committed iff every vote is
    /// <see cref="SagaVote.Commit"/>, else Aborted). Idempotent: re-dispatched
    /// prepare returns the participant's already-recorded vote.
    /// </summary>
    private async Task PreparePhaseAsync()
    {
        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await RunPreparePhaseAsync();
        }
        finally
        {
            LatticeReplicationMetrics.SagaPhaseDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds,
                new KeyValuePair<string, object?>(
                    LatticeReplicationMetrics.TagPhase, LatticeReplicationMetrics.SagaPhasePrepare));
        }
    }

    private async Task RunPreparePhaseAsync()
    {
        // Coordinator-side prepare-progress (build) deadline. A long-running
        // participant prepare is allowed up to this window; past it the
        // coordinator gives up and aborts. Distinct from the participant's short
        // cutover fence timer.
        if (state.State.StartedAtTicks > 0
            && DateTime.UtcNow.Ticks - state.State.StartedAtTicks > PrepareProgressDeadline.Ticks)
        {
            state.State.Phase = CrossClusterSagaPhase.Aborted;
            state.State.Outcome = CrossClusterSagaOutcome.Aborted;
            state.State.FailureMessage =
                $"Cross-cluster saga '{SagaId}' aborted: prepare-progress deadline exceeded.";
            await state.WriteStateAsync();
            return;
        }

        var participants = state.State.Participants;
        var request = BuildRequest();
        var voteTasks = new Task<SagaControlResponse>[participants.Count];
        for (var i = 0; i < participants.Count; i++)
        {
            voteTasks[i] = _controlChannel.PrepareAsync(participants[i].ClusterId, request);
        }

        SagaControlResponse[] responses;
        try
        {
            responses = await Task.WhenAll(voteTasks);
        }
        catch (Exception ex)
        {
            // A participant threw rather than returning a vote (transient
            // routing/transport error). Leave the coordinator in Preparing so
            // the keepalive reminder retries the whole prepare phase
            // idempotently.
            Logger.LogWarning(ex,
                "Cross-cluster saga {SagaId} prepare dispatch faulted; will retry.",
                SagaId);
            throw;
        }

        var allCommit = true;
        string? abortDetail = null;
        for (var i = 0; i < responses.Length; i++)
        {
            var vote = responses[i].Vote;
            participants[i].Vote = vote;
            if (vote != SagaVote.Commit)
            {
                allCommit = false;
                abortDetail ??= string.IsNullOrEmpty(responses[i].Detail)
                    ? $"participant '{participants[i].ClusterId}' voted {vote}."
                    : $"participant '{participants[i].ClusterId}': {responses[i].Detail}";
            }
        }

        if (allCommit)
        {
            state.State.Phase = CrossClusterSagaPhase.Committed;
            state.State.Outcome = CrossClusterSagaOutcome.Committed;
        }
        else
        {
            state.State.Phase = CrossClusterSagaPhase.Aborted;
            state.State.Outcome = CrossClusterSagaOutcome.Aborted;
            state.State.FailureMessage =
                $"Cross-cluster saga '{SagaId}' aborted: {abortDetail}";
        }

        await state.WriteStateAsync();
    }

    /// <summary>
    /// Fans out the terminal decision to every prepared participant (commit when
    /// the global decision is Committed, compensate otherwise), then marks the
    /// coordinator Completed and arms retention cleanup. Idempotent finalize
    /// tolerates a crash mid-fan-out; the per-participant durable model absorbs
    /// duplicate commit/abort deliveries.
    /// </summary>
    private async Task FinalizePhaseAsync()
    {
        var commit = state.State.Phase == CrossClusterSagaPhase.Committed;
        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            await RunFinalizePhaseAsync(commit);
        }
        finally
        {
            LatticeReplicationMetrics.SagaPhaseDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds,
                new KeyValuePair<string, object?>(
                    LatticeReplicationMetrics.TagPhase,
                    commit ? LatticeReplicationMetrics.SagaPhaseCommit : LatticeReplicationMetrics.SagaPhaseAbort));
        }
    }

    private async Task RunFinalizePhaseAsync(bool commit)
    {
        var participants = state.State.Participants;
        var request = BuildRequest();

        var finalizeTasks = new List<Task<SagaControlResponse>>(participants.Count);
        foreach (var p in participants)
        {
            // Only participants that prepared (voted Commit) hold state to
            // finalize. A participant that voted Abort already self-terminated
            // locally, so skipping it avoids a needless round-trip.
            if (p.Vote != SagaVote.Commit) continue;
            finalizeTasks.Add(commit
                ? _controlChannel.CommitAsync(p.ClusterId, request)
                : _controlChannel.AbortAsync(p.ClusterId, request));
        }

        await Task.WhenAll(finalizeTasks);

        state.State.Phase = CrossClusterSagaPhase.Completed;
        await state.WriteStateAsync();

        await UnregisterKeepaliveAsync();
        await SlideTtlAsync();
    }

    /// <summary>
    /// Builds the control request carried on every RPC for this saga from the
    /// persisted identity fields.
    /// </summary>
    private SagaControlRequest BuildRequest() => new()
    {
        SagaId = SagaId,
        TargetTree = state.State.TargetTree,
        ManifestId = state.State.ManifestId,
        CoordinatorClusterId = state.State.CoordinatorClusterId,
        SetId = state.State.SetId,
    };

    /// <summary>
    /// Canonicalises the caller's participant cluster ids into a de-duplicated,
    /// ordinal-sorted list of participant records, so the fingerprint is
    /// order-independent. Empty / null entries are rejected.
    /// </summary>
    private static List<CrossClusterSagaParticipantRef> BuildParticipants(List<string> clusterIds)
    {
        var seen = new HashSet<string>(clusterIds.Count, StringComparer.Ordinal);
        var result = new List<CrossClusterSagaParticipantRef>(clusterIds.Count);
        foreach (var clusterId in clusterIds)
        {
            if (string.IsNullOrEmpty(clusterId))
                throw new ArgumentException(
                    "Cross-cluster saga participant set contains a null or empty cluster id.",
                    nameof(clusterIds));
            if (!seen.Add(clusterId)) continue;
            result.Add(new CrossClusterSagaParticipantRef { ClusterId = clusterId });
        }

        result.Sort(static (a, b) => string.CompareOrdinal(a.ClusterId, b.ClusterId));
        return result;
    }

    /// <summary>
    /// Stable fingerprint over the (canonical) participant cluster set, target
    /// tree, manifest id, and optional set id. A re-submit with any of these
    /// changed is rejected.
    /// </summary>
    private static byte[] ComputeFingerprint(
        List<CrossClusterSagaParticipantRef> participants, string targetTree, string manifestId, string? setId)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lenPrefix = stackalloc byte[4];
        AppendLengthPrefixed(hash, targetTree, lenPrefix);
        AppendLengthPrefixed(hash, manifestId, lenPrefix);
        // Fold the set id into the fingerprint so a set restore and an otherwise
        // identical single-tree restore never collide on a reused saga id. A null
        // (single-tree) set id contributes the empty string, so an existing
        // single-tree caller's fingerprint is unchanged.
        AppendLengthPrefixed(hash, setId ?? string.Empty, lenPrefix);
        BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, participants.Count);
        hash.AppendData(lenPrefix);
        foreach (var p in participants)
        {
            AppendLengthPrefixed(hash, p.ClusterId, lenPrefix);
        }
        return hash.GetHashAndReset();
    }

    /// <summary>
    /// Appends a 4-byte little-endian length prefix followed by the UTF-8 bytes
    /// of <paramref name="value"/> to <paramref name="hash"/>, encoding through a
    /// stack buffer for short strings and renting from the array pool only for
    /// the rare long value.
    /// </summary>
    private static void AppendLengthPrefixed(IncrementalHash hash, string value, Span<byte> lenPrefix)
    {
        var maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = maxBytes > 512 ? System.Buffers.ArrayPool<byte>.Shared.Rent(maxBytes) : null;
        Span<byte> buffer = rented ?? stackalloc byte[512];
        var written = Encoding.UTF8.GetBytes(value, buffer);
        BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, written);
        hash.AppendData(lenPrefix);
        hash.AppendData(buffer[..written]);
        if (rented is not null)
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private async Task RegisterKeepaliveAsync()
    {
        try
        {
            await ReminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: GrainContext.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: TimeSpan.FromMinutes(1),
                period: TimeSpan.FromMinutes(1));
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cross-cluster saga {SagaId}: failed to register keepalive reminder (non-fatal).",
                SagaId);
        }
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(GrainContext.GrainId, KeepaliveReminderName);
            if (reminder is not null)
            {
                await ReminderRegistry.UnregisterReminder(GrainContext.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cross-cluster saga {SagaId}: failed to unregister keepalive reminder (non-fatal).",
                SagaId);
        }
    }
}
