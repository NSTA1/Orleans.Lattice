using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The first built-in <see cref="ISagaParticipant"/>: maps the backup restore
/// engine onto the cross-cluster saga so a restore into a replicated tree becomes
/// an all-or-nothing coordinated multi-cluster restore. One process-wide
/// singleton per cluster, driven by the local
/// <see cref="Grains.CrossClusterSagaParticipantGrain"/> through the SPI.
/// <list type="number">
///   <item><description><b>Prepare</b> probes admission (a cheap early refusal of
///   an infeasible target), then builds the shadow tree <b>without</b> fencing the
///   live tree - the shadow is the fixed past cut, so live traffic keeps running
///   during the (potentially long) build. The build is idempotent and resumable,
///   so a crash mid-build resumes rather than restarting. A transient capacity
///   exhaustion is retried within a bounded budget; a permanent failure garbage
///   collects any partial shadow and votes to abort.</description></item>
///   <item><description><b>Commit</b> engages the write fence for the target
///   tree(s) - the fence covers only the short cutover, never the build - performs
///   the single atomic alias swap, then unblocks local writes. Cross-cluster
///   shipping stays paused until the saga globally completes (the fence grain's
///   global gate), preserving cross-cluster atomic visibility.</description></item>
///   <item><description><b>Abort</b> reverts the alias to the pre-restore physical
///   tree when this participant had started the cutover (a saga that only
///   prepared never moved it), reliably garbage collects the shadow so no storage
///   leaks, then lifts
///   the fence. Idempotent and safe when called by the participant grain after it
///   learns an abort from the coordinator on fence expiry; that request carries
///   the persisted set id, so a backup-set restore takes the set path. Only a
///   participant state persisted before the set id was recorded rebuilds a
///   request without it, which takes the single-tree path, which knows nothing of
///   the set's members: it lifts the fence but leaves the member shadows in
///   place.</description></item>
/// </list>
/// <para>
/// The participant keeps a best-effort in-memory cache of the local build result
/// keyed by saga id, so commit reuses the built shadow and the dispatcher can read
/// back the local result. The cache is not durable: a reactivation that lost it
/// re-derives the shadow deterministically (commit rebuilds idempotently; abort
/// resolves the shadow id without rebuilding), so no durable participant state and
/// no new serialized types are required.
/// </para>
/// <para>
/// The backup engine dependencies are optional: a host that wires replication
/// without the backup package leaves them unresolved, in which case the
/// participant is inert and votes to abort any restore saga dispatched to it
/// (no backup means nothing to restore), rather than failing to construct.
/// </para>
/// <para>
/// <b>Authority.</b> The operator who starts a coordinated restore is authorized
/// once, on the coordinator cluster. A participant acting on a remote
/// coordinator's saga runs its restore steps with <b>peer authority</b> - a
/// system-origin scope entered only for a current, authenticated replication
/// peer, on trees this cluster replicates, replaying backups of trees this cluster
/// replicates (#5443). Every other caller, this cluster's own coordinator
/// included, runs as the ambient caller and is authorized as such. See
/// <c>HasPeerAuthority</c> for the trust argument.
/// </para>
/// </summary>
internal sealed class RestoreParticipant(
    ILatticeCoordinatedRestoreEngine? engine,
    ILatticeBackupRestoreService? restoreService,
    IRestoreCapacityProbe capacity,
    IGrainFactory grainFactory,
    ILogger<RestoreParticipant> logger,
    ILatticeBackupSetResolver? setResolver = null,
    IReplicatedTreeMembership? membership = null,
    Microsoft.Extensions.Options.IOptionsMonitor<LatticeReplicationOptions>? options = null,
    IReplicationTopology? topology = null) : ISagaParticipant
{
    /// <summary>
    /// Bounded retry budget for a transient capacity exhaustion mid-build. Past
    /// it the participant garbage collects any partial shadow and votes to abort
    /// (clean all-or-nothing), rather than hold the saga open indefinitely.
    /// </summary>
    private const int BuildAttemptBudget = 3;

    /// <summary>
    /// Cutover fence window in seconds. Zero selects the fence grain's own default
    /// bounded window, keeping a single source of truth for the deadline.
    /// </summary>
    private const int CutoverFenceWindowSeconds = 0;

    private readonly ConcurrentDictionary<string, LatticeRestoreResult> _built =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Saga ids whose cutover this participant has started in the current run of
    /// the saga, on this activation. An abort reverts the alias only for these: a
    /// saga that only prepared never moved the alias, and reverting it collides
    /// with the build's own open alias operation (#5443). The record belongs to one
    /// run: a new prepare starts a fresh run of the deterministic saga id and drops
    /// it, and an abort consumes it, so a later run never reverts an earlier run's
    /// cutover. It deliberately survives a completed commit, because an operator
    /// who resolves a participant to abort after its commit completed locally, but
    /// before its grain recorded the Committed phase, must still have the alias
    /// reverted. A reactivation that lost this set also lost the built cache, so the
    /// abort takes the no-revert garbage-collect path either way.
    /// </summary>
    private readonly ConcurrentDictionary<string, bool> _cutOver =
        new(StringComparer.Ordinal);

    /// <summary>Records a participant prepare vote on the saga vote counter, tagged by reason.</summary>
    private static void RecordVote(string reason) =>
        LatticeReplicationMetrics.SagaParticipantVotes.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.Platform);

    /// <summary>Records a participant commit on the saga commit counter, tagged by reason.</summary>
    private static void RecordCommit(string reason) =>
        LatticeReplicationMetrics.SagaParticipantCommits.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.Platform);

    /// <summary>Records a participant abort on the saga abort counter, tagged by reason.</summary>
    private static void RecordAbort(string reason) =>
        LatticeReplicationMetrics.SagaParticipantAborts.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.Platform);

    /// <summary>
    /// Best-effort in-memory cache of the group of shadows built for a <b>set</b>
    /// restore, keyed by saga id. Parallel to <see cref="_built"/> (which holds a
    /// single-tree restore's one shadow); a set saga caches every member tree's
    /// shadow here so commit flips and abort reverts all of them as one group. Not
    /// durable: a reactivation that lost it re-derives every member deterministically
    /// (commit rebuilds idempotently; abort resolves each shadow id without
    /// rebuilding), so no durable participant state and no new serialized types.
    /// </summary>
    private readonly ConcurrentDictionary<string, IReadOnlyList<LatticeRestoreResult>> _builtSets =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Attempts to read the local restore result the participant built for
    /// <paramref name="sagaId"/>. The dispatcher on the coordinator cluster reads
    /// it back after a committed coordinated restore.
    /// </summary>
    /// <param name="sagaId">The saga id. Must not be <c>null</c>.</param>
    /// <param name="result">The built result, when present.</param>
    /// <returns><c>true</c> when a result is cached; otherwise <c>false</c>.</returns>
    public bool TryGetLocalResult(string sagaId, out LatticeRestoreResult? result)
    {
        ArgumentNullException.ThrowIfNull(sagaId);
        return _built.TryGetValue(sagaId, out result);
    }

    /// <summary>
    /// Attempts to read the group of local restore results the participant built for
    /// a <b>set</b> restore under <paramref name="sagaId"/> (one per hosted member
    /// tree). The dispatcher on the coordinator cluster reads it back after a
    /// committed coordinated set restore.
    /// </summary>
    /// <param name="sagaId">The saga id. Must not be <c>null</c>.</param>
    /// <param name="results">The built per-member results, when present.</param>
    /// <returns><c>true</c> when a group result is cached; otherwise <c>false</c>.</returns>
    public bool TryGetLocalSetResult(string sagaId, out IReadOnlyList<LatticeRestoreResult>? results)
    {
        ArgumentNullException.ThrowIfNull(sagaId);
        return _builtSets.TryGetValue(sagaId, out results);
    }

    /// <summary>
    /// Whether this cluster hosts the <b>single-tree</b> target named by
    /// <paramref name="request"/>. The target arrives on the inbound cross-cluster
    /// saga control channel, which authorizes the <i>origin cluster</i> and not the
    /// tree, so it is peer-supplied and must be re-derived against local
    /// replication enrollment before anything is built, swapped, or reverted -
    /// otherwise a peer holding an accepted mesh credential picks the tree the
    /// shadow cutover overwrites, including a tree kept deliberately cluster-local
    /// or a reserved <c>sys-</c> authorization tree. This is the single-tree
    /// counterpart of <see cref="FilterHostedMembers"/>, which already applies the
    /// same seam per member on the set path.
    /// <para>
    /// A legitimate single-tree saga can only ever name a replicated tree:
    /// <c>RestoreSagaDispatcher</c> declines to promote a restore to a saga at all
    /// unless <see cref="IReplicatedTreeMembership.IsReplicated"/> holds for the
    /// resolved target, so the gate refuses nothing the coordinator can produce.
    /// When the membership seam is not wired (direct unit tests) the gate is a
    /// no-op, matching <see cref="FilterHostedMembers"/>.
    /// </para>
    /// </summary>
    private bool IsHostedTarget(SagaControlRequest request) =>
        membership is null || membership.IsReplicated(request.TargetTree);

    /// <summary>The refusal returned for a target tree this cluster does not replicate.</summary>
    private static SagaParticipantPrepareResult NotReplicated(string targetTree)
    {
        RecordVote(LatticeReplicationMetrics.SagaReasonNotReplicated);
        return new SagaParticipantPrepareResult(
            SagaVote.Abort,
            $"Target tree '{targetTree}' is not replicated on this cluster: " +
            "a coordinated restore may only target a tree this cluster is enrolled for.");
    }

    /// <summary>
    /// Whether this saga's restore steps run with <b>peer authority</b> - inside a
    /// system-origin scope - rather than as the ambient caller (#5443).
    /// <para>
    /// A participant acting on a remote coordinator's saga has no user identity of
    /// its own: the operator was authorized on the coordinator cluster, before the
    /// saga started, and the control call that reaches this cluster carries only the
    /// coordinator's cluster identity. Re-authorizing the engine's restore steps
    /// against the ambient subject therefore checks <c>anonymous</c>, and on a
    /// cluster that enforces authorization every replicated-tree restore was
    /// compensated. The participant instead acts on the authority it does hold - the
    /// authenticated replication peer's - and only where that authority already
    /// reaches.
    /// </para>
    /// <para>
    /// Every condition is re-derived locally; nothing the request body classifies is
    /// trusted. Peer authority holds only when all of these are true:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>The membership, options and topology seams are wired. A
    ///   missing seam is a refusal, never a pass.</description></item>
    ///   <item><description>The coordinator is a <b>remote</b> cluster. On the
    ///   inbound gRPC saga service <see cref="SagaControlRequest.CoordinatorClusterId"/>
    ///   is overwritten with the transport-stamped origin, which must equal the
    ///   cluster the presented credential is bound to, and is refused unless that
    ///   origin is an authorized peer. A request this cluster's own coordinator loops
    ///   back keeps the operator's ambient identity, which flows with the
    ///   call.</description></item>
    ///   <item><description>The coordinator is one of this cluster's
    ///   <b>current</b> replication peers.</description></item>
    ///   <item><description>The caller has already confined the targets to trees
    ///   this cluster replicates (<see cref="IsHostedTarget"/>,
    ///   <see cref="FilterHostedMembers"/>), and prepare confines every backup's
    ///   captured source trees the same way (<see cref="SourcesEnrolled"/>).</description></item>
    /// </list>
    /// <para>
    /// Within those bounds the restore grants the peer nothing its replication
    /// stream does not already hold: an enrolled peer can already write any value
    /// into a tree it replicates with this cluster, under system origin, and it
    /// already receives every write to the trees whose backups it may restore.
    /// </para>
    /// </summary>
    private bool HasPeerAuthority(SagaControlRequest request)
    {
        if (membership is null || options is null || topology is null)
        {
            return false;
        }

        var coordinator = request.CoordinatorClusterId;
        if (string.IsNullOrEmpty(coordinator)
            || string.Equals(coordinator, options.CurrentValue.ClusterId, StringComparison.Ordinal))
        {
            return false;
        }

        foreach (var peer in topology.CurrentPeers)
        {
            if (string.Equals(peer, coordinator, StringComparison.Ordinal))
            {
                return true;
            }
        }

        logger.LogWarning(
            "Restore participant: saga '{SagaId}' names coordinator '{Coordinator}', which is not a current " +
            "replication peer; its restore steps run as the ambient caller.",
            request.SagaId, coordinator);
        return false;
    }

    /// <summary>
    /// Enters the system-origin scope that carries peer authority for
    /// <paramref name="request"/>, or returns <c>null</c> (the ambient caller) when
    /// <see cref="HasPeerAuthority"/> does not hold. Cold path: one restore saga
    /// step per call, so the scope allocation is immaterial.
    /// </summary>
    private IDisposable? EnterPeerAuthority(SagaControlRequest request, out bool elevated)
    {
        elevated = HasPeerAuthority(request);
        return elevated ? LatticeAccessGateContext.EnterSystemOrigin() : null;
    }

    /// <summary>
    /// Whether every tree the backup chain in <paramref name="report"/> was captured
    /// from is a tree a peer-authority restore may read: the target itself, or a
    /// tree this cluster replicates. A report that names no source is unknown and
    /// refused. Without this check a peer could name any catalogued backup -
    /// including one of a tree kept deliberately cluster-local - and copy its
    /// contents into a replicated tree, because the engine's captured-source
    /// authorization is bypassed under system origin.
    /// </summary>
    private bool SourcesEnrolled(RestoreAdmissionReport report)
    {
        if (report.SourceTreeIds.Count == 0)
        {
            return false;
        }

        foreach (var source in report.SourceTreeIds)
        {
            if (!string.Equals(source, report.TargetTreeId, StringComparison.Ordinal)
                && !membership!.IsReplicated(source))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The single refusal a peer-coordinated prepare returns when the admission probe
    /// fails <b>or</b> the backup's captured sources are not replicated here. The
    /// probe runs under peer authority before the sources are known, so distinct
    /// answers would let a peer learn whether a backup id exists, or what it holds,
    /// for a tree it may not restore. The cause is logged locally and counted on the
    /// vote metric; the peer sees the same text either way.
    /// </summary>
    private static SagaParticipantPrepareResult PeerRefusal(string backupId, string targetTree, string reason)
    {
        RecordVote(reason);
        return new SagaParticipantPrepareResult(
            SagaVote.Abort,
            $"Backup '{backupId}' cannot be restored into tree '{targetTree}' by a peer-coordinated restore " +
            "on this cluster.");
    }

    /// <summary>
    /// Re-derives, at commit, that a backup a lost cache forces the participant to
    /// rebuild from still replays only replicated trees: a source tree can stop being
    /// replicated between prepare and commit. A probe failure is a refusal.
    /// </summary>
    private async Task<bool> SourcesStillEnrolledAsync(
        string sagaId, LatticeRestoreRequest restoreRequest, CancellationToken cancellationToken)
    {
        RestoreAdmissionReport report;
        try
        {
            report = await engine!.ProbeAdmissionAsync(restoreRequest, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Restore participant: commit re-probe failed for saga '{SagaId}' (backup '{BackupId}').",
                sagaId, restoreRequest.BackupId);
            return false;
        }

        return SourcesEnrolled(report);
    }

    /// <summary>Refuses a commit whose rebuild would replay a backup of a tree this cluster no longer replicates.</summary>
    private async Task RefuseCommitAsync(SagaControlRequest request)
    {
        logger.LogWarning(
            "Restore participant: refusing commit for saga '{SagaId}' - rebuilding its shadow would replay a backup " +
            "captured from a tree this cluster does not replicate.",
            request.SagaId);
        RecordCommit(LatticeReplicationMetrics.SagaReasonNotReplicated);
        await grainFactory.GetGrain<ISagaWriteFenceGrain>(request.SagaId).LiftAsync();
    }

    /// <inheritdoc />
    public async Task<SagaParticipantPrepareResult> PrepareAsync(
        SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        // A prepare starts a new run of this saga id (ids are deterministic), so no
        // cutover recorded by an earlier run may be reverted by this run's abort.
        _cutOver.TryRemove(request.SagaId, out _);

        if (engine is null)
        {
            RecordVote(LatticeReplicationMetrics.SagaReasonEngineUnavailable);
            return new SagaParticipantPrepareResult(
                SagaVote.Abort,
                "Restore engine unavailable: the backup package is not wired on this cluster.");
        }

        var setMembers = await ResolveSetMembersAsync(request, cancellationToken);
        if (setMembers is not null)
        {
            return await PrepareSetAsync(request, setMembers, cancellationToken);
        }

        if (!IsHostedTarget(request))
        {
            logger.LogWarning(
                "Restore participant: refusing saga '{SagaId}' - target tree '{TargetTree}' is not replicated here.",
                request.SagaId, request.TargetTree);
            return NotReplicated(request.TargetTree);
        }

        var restoreRequest = BuildRestoreRequest(request);
        using var authority = EnterPeerAuthority(request, out var peerAuthority);

        // Admission pre-flight: probe the manifest's self-describing size and
        // topology and hard-refuse an infeasible target BEFORE building anything.
        RestoreAdmissionReport report;
        try
        {
            report = await engine.ProbeAdmissionAsync(restoreRequest, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Restore participant: admission probe failed for saga '{SagaId}' (backup '{BackupId}').",
                request.SagaId, request.ManifestId);
            if (peerAuthority)
            {
                return PeerRefusal(request.ManifestId, request.TargetTree, LatticeReplicationMetrics.SagaReasonInfeasible);
            }

            RecordVote(LatticeReplicationMetrics.SagaReasonInfeasible);
            return new SagaParticipantPrepareResult(
                SagaVote.Abort, $"Admission probe failed: {ex.Message}");
        }

        if (peerAuthority && !SourcesEnrolled(report))
        {
            logger.LogWarning(
                "Restore participant: refusing saga '{SagaId}' - backup '{BackupId}' was captured from a tree " +
                "this cluster does not replicate.",
                request.SagaId, request.ManifestId);
            return PeerRefusal(request.ManifestId, request.TargetTree, LatticeReplicationMetrics.SagaReasonNotReplicated);
        }

        if (!await capacity.CanHostAsync(report, cancellationToken))
        {
            logger.LogWarning(
                "Restore participant: target infeasible for saga '{SagaId}': {Bytes} byte(s) over {Shards} shard(s).",
                request.SagaId, report.TotalByteLength, report.ShardCount);
            RecordVote(LatticeReplicationMetrics.SagaReasonInfeasible);
            return new SagaParticipantPrepareResult(
                SagaVote.Abort,
                $"Target infeasible: cannot host {report.TotalByteLength} byte(s) over {report.ShardCount} shard(s).");
        }

        // Unfenced, resumable shadow build. The live tree is NOT fenced here: the
        // shadow is materialized from the backup's fixed past cut while live
        // traffic keeps running. A transient failure is retried within a bounded
        // budget; a permanent one cleans up and votes to abort.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var result = await engine.BuildShadowAsync(restoreRequest, cancellationToken);
                _built[request.SagaId] = result;
                RecordVote(LatticeReplicationMetrics.SagaReasonCommit);
                return new SagaParticipantPrepareResult(SagaVote.Commit);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (LatticeRestoreValidationException ex)
            {
                // A pre-apply validation failed. A missing backup or base in the
                // chain is caught earlier - by the admission probe above, which
                // votes infeasible - so what reaches here is what the probe does
                // not check: a referenced artifact that is missing from the sink or
                // fails its integrity digest, or a backup / base removed after the
                // probe ran. It is permanent, not a capacity problem - do not retry.
                await SafeGarbageCollectAsync(restoreRequest, cancellationToken);
                _built.TryRemove(request.SagaId, out _);
                RecordVote(LatticeReplicationMetrics.SagaReasonPrecondition);
                return new SagaParticipantPrepareResult(
                    SagaVote.Abort, $"Precondition failed: {ex.Message}");
            }
            catch (Exception ex) when (attempt < BuildAttemptBudget)
            {
                logger.LogWarning(ex,
                    "Restore participant: shadow build attempt {Attempt}/{Budget} failed for saga '{SagaId}'; retrying.",
                    attempt, BuildAttemptBudget, request.SagaId);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Restore participant: shadow build exhausted its retry budget for saga '{SagaId}'; aborting.",
                    request.SagaId);
                await SafeGarbageCollectAsync(restoreRequest, cancellationToken);
                _built.TryRemove(request.SagaId, out _);
                RecordVote(LatticeReplicationMetrics.SagaReasonBuildFailed);
                return new SagaParticipantPrepareResult(
                    SagaVote.Abort, $"Shadow build failed: {ex.Message}");
            }
        }
    }

    /// <inheritdoc />
    public async Task CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        if (engine is null)
        {
            // Backup not wired: nothing was prepared, so there is nothing to commit.
            // Lift any fence defensively and return.
            await grainFactory.GetGrain<ISagaWriteFenceGrain>(request.SagaId).LiftAsync();
            return;
        }

        var setMembers = await ResolveSetMembersAsync(request, cancellationToken);
        if (setMembers is not null)
        {
            await CommitSetAsync(request, setMembers, cancellationToken);
            return;
        }

        // Re-derived independently of prepare: a replayed or forged commit must not
        // be able to reach the alias swap on the strength of never having prepared.
        if (!IsHostedTarget(request))
        {
            logger.LogWarning(
                "Restore participant: refusing commit for saga '{SagaId}' - target tree '{TargetTree}' is not replicated here.",
                request.SagaId, request.TargetTree);
            RecordCommit(LatticeReplicationMetrics.SagaReasonNotReplicated);
            await grainFactory.GetGrain<ISagaWriteFenceGrain>(request.SagaId).LiftAsync();
            return;
        }

        var restoreRequest = BuildRestoreRequest(request);
        using var authority = EnterPeerAuthority(request, out var peerAuthority);

        // Reuse the prepared shadow; if a reactivation lost the in-memory cache,
        // re-derive it - BuildShadowAsync is idempotent and resumable, so this
        // resolves back to the same fully built shadow without a full rebuild.
        if (!_built.TryGetValue(request.SagaId, out var result))
        {
            // The rebuild replays the backup again, so its sources are re-checked
            // against enrollment now rather than trusted from the earlier prepare.
            if (peerAuthority && !await SourcesStillEnrolledAsync(request.SagaId, restoreRequest, cancellationToken))
            {
                await RefuseCommitAsync(request);
                return;
            }

            result = await engine.BuildShadowAsync(restoreRequest, cancellationToken);
            _built[request.SagaId] = result;
        }

        // The write fence engages ONLY now, covering the short cutover. It is keyed
        // by saga id and fences the group of target trees this cluster hosts.
        var fence = grainFactory.GetGrain<ISagaWriteFenceGrain>(request.SagaId);
        await fence.EngageAsync(new SagaWriteFenceRequest
        {
            SagaId = request.SagaId,
            Trees = [request.TargetTree],
            CoordinatorClusterId = request.CoordinatorClusterId,
            FenceWindowSeconds = CutoverFenceWindowSeconds,
            // Issue #4593: the restored copy is closed for inbound replication
            // before the swap below makes it routable, and opens only when the
            // fence lifts, so a peer write that passed the cached receive gate
            // before the pause can never land on it.
            ReceiveClosedCopies = ShadowCopies([result]),
        });

        // Single atomic alias swap: a reader sees whole-old or whole-new, never
        // half. Recorded first, so an abort after a commit that faulted part-way
        // still reverts an alias that may have moved.
        _cutOver[request.SagaId] = true;
        await engine.CommitShadowAsync(result, cancellationToken);

        // Unblock local writes on this cluster now it has flipped. Cross-cluster
        // shipping stays paused until the saga globally completes (the fence
        // grain's global gate), so no early-flipping cluster re-advances the cut.
        await fence.UnblockWritesAsync();
        RecordCommit(LatticeReplicationMetrics.SagaReasonSingle);
    }

    /// <inheritdoc />
    public async Task AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        var fence = grainFactory.GetGrain<ISagaWriteFenceGrain>(request.SagaId);

        if (engine is null)
        {
            // Backup not wired: nothing was prepared. Lift any fence and return.
            await fence.LiftAsync();
            RecordAbort(LatticeReplicationMetrics.SagaReasonEngineUnavailable);
            return;
        }

        var setMembers = await ResolveSetMembersAsync(request, cancellationToken);
        if (setMembers is not null)
        {
            await AbortSetAsync(request, setMembers, fence, cancellationToken);
            return;
        }

        // Nothing was ever prepared for a tree this cluster does not replicate, so
        // there is nothing to compensate; reverting an alias on an unenrolled tree
        // would itself be the write the prepare gate exists to refuse. One case
        // does reach here with prepared state: a backup-set restore whose
        // participant state predates the persisted set id, so the abort it learns
        // on fence expiry is rebuilt with no set id and names the set id as its
        // target, and its member shadows are left in place.
        if (!IsHostedTarget(request))
        {
            RecordAbort(LatticeReplicationMetrics.SagaReasonNotReplicated);
            await fence.LiftAsync();
            return;
        }

        // Compensate: revert the alias to the pre-restore physical tree (a no-op
        // when this cluster never committed - the alias is already there), then
        // reliably garbage collect the shadow so no storage leaks, then lift the
        // fence. All three steps are idempotent, so this is safe when invoked by
        // the participant grain for an abort it learned from the coordinator on
        // fence expiry, and safe when the participant never prepared.
        var restoreRequest = BuildRestoreRequest(request);
        using var authority = EnterPeerAuthority(request, out _);

        if (_built.TryGetValue(request.SagaId, out var result))
        {
            // Revert only an alias this participant moved. A saga that only
            // prepared still holds the build's alias operation on the target, so a
            // revert would collide with it and fault (#5443); deleting the shadow
            // below releases that operation instead.
            if (_cutOver.TryRemove(request.SagaId, out _))
            {
                await SafeRevertAsync(result, cancellationToken);
            }

            await SafeDeleteShadowByIdAsync(result.ShadowPhysicalTreeId, cancellationToken);
            _built.TryRemove(request.SagaId, out _);
        }
        else
        {
            // Reactivation lost the cache: re-derive the shadow id deterministically
            // and garbage collect it without a rebuild. No commit can precede an
            // abort in this saga model, so the alias is still the pre-restore tree
            // and no revert is required.
            await SafeGarbageCollectAsync(restoreRequest, cancellationToken);
        }

        await fence.LiftAsync();
        RecordAbort(LatticeReplicationMetrics.SagaReasonSingle);
    }

    /// <inheritdoc />
    public Task<SagaPhase> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default) =>
        // The durable phase is owned by the participant grain; this SPI carries no
        // durable state of its own.
        Task.FromResult(SagaPhase.None);

    /// <summary>
    /// Resolves the set members this saga restores, or <c>null</c> when the request
    /// is an ordinary single-tree restore. A non-null, non-empty result switches the
    /// participant onto the group-atomic set path; a single-tree request (no
    /// <see cref="SagaControlRequest.SetId"/>, or no set resolver wired) returns
    /// <c>null</c> and takes the unchanged single-tree path with zero extra I/O.
    /// </summary>
    private async Task<IReadOnlyList<BackupSetMember>?> ResolveSetMembersAsync(
        SagaControlRequest request, CancellationToken cancellationToken)
    {
        if (request.SetId is not { Length: > 0 } setId || setResolver is null)
        {
            return null;
        }

        var members = await setResolver.ResolveMembersAsync(setId, cancellationToken);
        return members.Count > 0 ? members : null;
    }

    /// <summary>
    /// Filters the resolved set members to the subset of trees <b>this</b> cluster
    /// actually hosts, so a participant never tries to restore a tree it does not
    /// hold. A member tree is hosted here when this cluster replicates it
    /// (<see cref="IReplicatedTreeMembership.IsReplicated"/> is true exactly on a
    /// replicated tree's peer set), or when this cluster is the coordinator
    /// (initiating) cluster, which is where the set's local-only member trees
    /// physically live. When the membership or options seam is not wired (direct
    /// unit tests), the filter is a no-op and every member is treated as hosted.
    /// </summary>
    private IReadOnlyList<BackupSetMember> FilterHostedMembers(
        IReadOnlyList<BackupSetMember> members, SagaControlRequest request)
    {
        if (membership is null || options is null)
        {
            return members;
        }

        var self = options.CurrentValue.ClusterId;
        var isCoordinator = string.Equals(self, request.CoordinatorClusterId, StringComparison.Ordinal);

        var hosted = new List<BackupSetMember>(members.Count);
        foreach (var member in members)
        {
            if (membership.IsReplicated(member.TreeId) || isCoordinator)
            {
                hosted.Add(member);
            }
        }

        return hosted;
    }

    /// <summary>
    /// Group-atomic prepare for a set: admits and builds every member tree's shadow
    /// unfenced. If ANY member is infeasible or fails to build, every member built
    /// so far is garbage collected and the whole set votes abort, so a set never
    /// commits some trees while aborting others.
    /// </summary>
    private async Task<SagaParticipantPrepareResult> PrepareSetAsync(
        SagaControlRequest request,
        IReadOnlyList<BackupSetMember> members,
        CancellationToken cancellationToken)
    {
        // Restore only the members this cluster hosts. A participant that hosts none
        // of the set's trees prepares vacuously (nothing to build, an empty group).
        members = FilterHostedMembers(members, request);
        var requests = BuildSetRestoreRequests(members);
        var built = new List<LatticeRestoreResult>(requests.Count);
        using var authority = EnterPeerAuthority(request, out var peerAuthority);

        foreach (var restoreRequest in requests)
        {
            RestoreAdmissionReport report;
            try
            {
                report = await engine!.ProbeAdmissionAsync(restoreRequest, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Restore participant: set admission probe failed for saga '{SagaId}' (member tree '{TreeId}').",
                    request.SagaId, restoreRequest.TargetTreeId);
                await GarbageCollectAllAsync(built, requests, cancellationToken);
                if (peerAuthority)
                {
                    return PeerRefusal(
                        restoreRequest.BackupId, restoreRequest.TargetTreeId!, LatticeReplicationMetrics.SagaReasonInfeasible);
                }

                RecordVote(LatticeReplicationMetrics.SagaReasonInfeasible);
                return new SagaParticipantPrepareResult(
                    SagaVote.Abort, $"Set admission probe failed: {ex.Message}");
            }

            if (peerAuthority && !SourcesEnrolled(report))
            {
                logger.LogWarning(
                    "Restore participant: refusing set saga '{SagaId}' - member tree '{TreeId}' would replay a " +
                    "backup captured from a tree this cluster does not replicate.",
                    request.SagaId, restoreRequest.TargetTreeId);
                await GarbageCollectAllAsync(built, requests, cancellationToken);
                return PeerRefusal(
                    restoreRequest.BackupId, restoreRequest.TargetTreeId!, LatticeReplicationMetrics.SagaReasonNotReplicated);
            }

            if (!await capacity.CanHostAsync(report, cancellationToken))
            {
                logger.LogWarning(
                    "Restore participant: set member tree '{TreeId}' infeasible for saga '{SagaId}'.",
                    restoreRequest.TargetTreeId, request.SagaId);
                await GarbageCollectAllAsync(built, requests, cancellationToken);
                RecordVote(LatticeReplicationMetrics.SagaReasonInfeasible);
                return new SagaParticipantPrepareResult(
                    SagaVote.Abort,
                    $"Set member '{restoreRequest.TargetTreeId}' infeasible: cannot host " +
                    $"{report.TotalByteLength} byte(s) over {report.ShardCount} shard(s).");
            }

            try
            {
                built.Add(await engine!.BuildShadowAsync(restoreRequest, cancellationToken));
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Restore participant: set member '{TreeId}' shadow build failed for saga '{SagaId}'; aborting the whole set.",
                    restoreRequest.TargetTreeId, request.SagaId);
                await GarbageCollectAllAsync(built, requests, cancellationToken);
                RecordVote(LatticeReplicationMetrics.SagaReasonBuildFailed);
                return new SagaParticipantPrepareResult(
                    SagaVote.Abort, $"Set member '{restoreRequest.TargetTreeId}' build failed: {ex.Message}");
            }
        }

        _builtSets[request.SagaId] = built;
        RecordVote(LatticeReplicationMetrics.SagaReasonCommit);
        return new SagaParticipantPrepareResult(SagaVote.Commit);
    }

    /// <summary>
    /// Group-atomic commit for a set: engages ONE write fence over every member tree
    /// (the fence primitive already fans out over a tree list), then swaps every
    /// member's alias, then unblocks writes. Cross-cluster shipping stays globally
    /// gated for the whole group exactly as for a single tree.
    /// </summary>
    private async Task CommitSetAsync(
        SagaControlRequest request,
        IReadOnlyList<BackupSetMember> members,
        CancellationToken cancellationToken)
    {
        members = FilterHostedMembers(members, request);
        if (members.Count == 0)
        {
            // This cluster hosts none of the set's trees: nothing to flip here.
            return;
        }

        var requests = BuildSetRestoreRequests(members);
        using var authority = EnterPeerAuthority(request, out var peerAuthority);

        // Reuse the prepared group; a reactivation that lost the cache re-derives
        // every member idempotently (BuildShadowAsync is resumable).
        if (!_builtSets.TryGetValue(request.SagaId, out var built))
        {
            // The rebuild replays every member's backup again, so each member's
            // sources are re-checked against enrollment before any of it is rebuilt.
            if (peerAuthority)
            {
                foreach (var memberRequest in requests)
                {
                    if (!await SourcesStillEnrolledAsync(request.SagaId, memberRequest, cancellationToken))
                    {
                        await RefuseCommitAsync(request);
                        return;
                    }
                }
            }

            // Each build targets a DISTINCT member tree and produces that member's
            // own shadow, so no build can observe another's effect. RunAsync
            // returns the results in slot order, so `built` is still in the
            // resolved tree-id order BuildSetRestoreRequests produced - which is
            // what the commit wave and the abort compensations index against.
            var rebuilt = await BoundedFanOut.RunAsync(
                requests.Count,
                BoundedFanOut.DefaultWidth,
                slot => engine!.BuildShadowAsync(requests[slot], cancellationToken),
                cancellationToken);

            built = rebuilt;
            _builtSets[request.SagaId] = built;
        }

        // One fence over the whole group: every member tree is write-fenced and
        // shipping-paused together, so the set flips as one atomic unit.
        var fence = grainFactory.GetGrain<ISagaWriteFenceGrain>(request.SagaId);
        await fence.EngageAsync(new SagaWriteFenceRequest
        {
            SagaId = request.SagaId,
            Trees = members.Select(static m => m.TreeId).ToList(),
            CoordinatorClusterId = request.CoordinatorClusterId,
            FenceWindowSeconds = CutoverFenceWindowSeconds,
            ReceiveClosedCopies = ShadowCopies(built),
        });

        // The alias swaps run INSIDE the fence, so this loop's length is the
        // window during which every member tree refuses writes. Each swap targets
        // a distinct member and the group's atomicity is supplied by the fence
        // spanning all of them - not by the order they flip in - so overlapping
        // them shortens the write stall from N round trips to ceil(N / width)
        // without weakening the guarantee. A fault still surfaces after every
        // launched swap has settled, because ForEachAsync observes them all.
        _cutOver[request.SagaId] = true;
        await BoundedFanOut.ForEachAsync(
            built,
            BoundedFanOut.DefaultWidth,
            result => engine!.CommitShadowAsync(result, cancellationToken));

        await fence.UnblockWritesAsync();
        RecordCommit(LatticeReplicationMetrics.SagaReasonSet);
    }

    /// <summary>
    /// Group-atomic abort for a set: reverts and garbage collects EVERY member tree
    /// (never some), then lifts the shared fence. Idempotent and safe when the cache
    /// was lost. An abort the participant grain learns on fence expiry reaches
    /// this path through the persisted set id; only a participant state persisted
    /// before the set id was recorded rebuilds a request without it, which takes
    /// the single-tree path in <see cref="AbortAsync"/> instead.
    /// </summary>
    private async Task AbortSetAsync(
        SagaControlRequest request,
        IReadOnlyList<BackupSetMember> members,
        ISagaWriteFenceGrain fence,
        CancellationToken cancellationToken)
    {
        members = FilterHostedMembers(members, request);
        if (members.Count == 0)
        {
            // This cluster hosts none of the set's trees: lift any defensive fence
            // and return, so a hosted-nothing participant compensates vacuously.
            await fence.LiftAsync();
            return;
        }

        using var authority = EnterPeerAuthority(request, out _);

        if (_builtSets.TryGetValue(request.SagaId, out var built))
        {
            // Revert only aliases this participant moved: a group that only
            // prepared still holds each build's alias operation, which a revert
            // would collide with (#5443); the shadow delete releases it.
            var cutOver = _cutOver.TryRemove(request.SagaId, out _);

            // Per member the revert must precede that member's shadow delete, so
            // the two steps stay serial INSIDE the body; across members there is
            // no such constraint, because each pair touches only its own tree.
            // Both steps are the Safe* wrappers, which swallow per-member faults,
            // so "one failure must not strand the others" is a property of the
            // body rather than of the serialisation - overlapping preserves it.
            await BoundedFanOut.ForEachAsync(
                built,
                BoundedFanOut.DefaultWidth,
                async result =>
                {
                    if (cutOver)
                    {
                        await SafeRevertAsync(result, cancellationToken);
                    }

                    await SafeDeleteShadowByIdAsync(result.ShadowPhysicalTreeId, cancellationToken);
                });

            _builtSets.TryRemove(request.SagaId, out _);
        }
        else
        {
            // Reactivation lost the cache: re-derive each member's shadow id and GC
            // it without a rebuild. No commit can precede an abort in this saga
            // model, so each alias is still the pre-restore tree and no revert is
            // required. One GC per distinct member tree, so the wave is safe.
            await BoundedFanOut.ForEachAsync(
                BuildSetRestoreRequests(members),
                BoundedFanOut.DefaultWidth,
                restoreRequest => SafeGarbageCollectAsync(restoreRequest, cancellationToken));
        }

        await fence.LiftAsync();
        RecordAbort(LatticeReplicationMetrics.SagaReasonSet);
    }

    /// <summary>Garbage collects every already-built member and any not-yet-built shadow of a failed set prepare.</summary>
    private async Task GarbageCollectAllAsync(
        IReadOnlyList<LatticeRestoreResult> built,
        IReadOnlyList<LatticeRestoreRequest> requests,
        CancellationToken cancellationToken)
    {
        // Every delete addresses a distinct shadow tree and routes through the
        // fault-swallowing Safe* wrapper, so the cleanup is order-insensitive.
        await BoundedFanOut.ForEachAsync(
            built,
            BoundedFanOut.DefaultWidth,
            result => SafeDeleteShadowByIdAsync(result.ShadowPhysicalTreeId, cancellationToken));

        // Also resolve-and-GC any member whose build never completed, so a partial
        // set prepare leaks no shadow storage.
        var unbuilt = requests.Count - built.Count;
        if (unbuilt > 0)
        {
            var offset = built.Count;
            await BoundedFanOut.RunAsync(
                unbuilt,
                BoundedFanOut.DefaultWidth,
                slot => SafeGarbageCollectAsync(requests[offset + slot], cancellationToken),
                cancellationToken);
        }
    }

    /// <summary>
    /// The shadows a commit is about to make routable, each physical id mapped to
    /// the tree it restores, so the fence closes each with that tree's pause epoch.
    /// </summary>
    private static Dictionary<string, string> ShadowCopies(IReadOnlyList<LatticeRestoreResult> built)
    {
        var copies = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var result in built)
        {
            if (!string.IsNullOrEmpty(result.ShadowPhysicalTreeId))
            {
                copies[result.ShadowPhysicalTreeId] = result.TargetTreeId;
            }
        }

        return copies;
    }

    /// <summary>Builds the per-member engine restore requests for a set, in resolved (tree-id) order.</summary>
    private static List<LatticeRestoreRequest> BuildSetRestoreRequests(IReadOnlyList<BackupSetMember> members)
    {
        var requests = new List<LatticeRestoreRequest>(members.Count);
        foreach (var member in members)
        {
            requests.Add(new LatticeRestoreRequest(
                backupId: member.BackupId,
                targetTreeId: member.TreeId,
                scope: null,
                mode: LatticeRestoreMode.ShadowCutover));
        }

        return requests;
    }

    /// <summary>
    /// Builds the engine restore request from the saga control request. A
    /// coordinated restore always targets an explicit whole tree (no sub-scope)
    /// via shadow-cutover.
    /// </summary>
    private static LatticeRestoreRequest BuildRestoreRequest(SagaControlRequest request) =>
        new(
            backupId: request.ManifestId,
            targetTreeId: request.TargetTree,
            scope: null,
            mode: LatticeRestoreMode.ShadowCutover);

    private async Task SafeRevertAsync(LatticeRestoreResult result, CancellationToken cancellationToken)
    {
        if (restoreService is null)
        {
            return;
        }

        try
        {
            await restoreService.RevertRestoreAsync(result, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Restore participant: revert of tree '{TreeId}' faulted during abort (non-fatal).",
                result.TargetTreeId);
        }
    }

    private async Task SafeGarbageCollectAsync(LatticeRestoreRequest request, CancellationToken cancellationToken)
    {
        if (engine is null)
        {
            return;
        }

        string shadowTreeId;
        try
        {
            shadowTreeId = engine.ResolveShadowTreeId(request);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Restore participant: could not resolve shadow tree id for backup '{BackupId}' (non-fatal).",
                request.BackupId);
            return;
        }

        await SafeDeleteShadowByIdAsync(shadowTreeId, cancellationToken);
    }

    private async Task SafeDeleteShadowByIdAsync(string? shadowPhysicalTreeId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(shadowPhysicalTreeId) || engine is null)
        {
            return;
        }

        try
        {
            await engine.DeleteShadowAsync(shadowPhysicalTreeId, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Restore participant: shadow garbage collection of '{ShadowTreeId}' faulted (non-fatal).",
                shadowPhysicalTreeId);
        }
    }
}
