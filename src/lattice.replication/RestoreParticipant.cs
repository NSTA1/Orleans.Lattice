using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The first internal <see cref="ISagaParticipant"/>: maps the backup restore
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
///   tree, reliably garbage collects the shadow so no storage leaks, then lifts
///   the fence. Idempotent and safe when called by the participant grain's
///   fence-expiry auto-compensation after a coordinator loss.</description></item>
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
/// </summary>
internal sealed class RestoreParticipant(
    ILatticeCoordinatedRestoreEngine? engine,
    ILatticeBackupRestoreService? restoreService,
    IRestoreCapacityProbe capacity,
    IGrainFactory grainFactory,
    ILogger<RestoreParticipant> logger,
    ILatticeBackupSetResolver? setResolver = null) : ISagaParticipant
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

    /// <inheritdoc />
    public async Task<SagaParticipantPrepareResult> PrepareAsync(
        SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        if (engine is null)
        {
            return new SagaParticipantPrepareResult(
                SagaVote.Abort,
                "Restore engine unavailable: the backup package is not wired on this cluster.");
        }

        var setMembers = await ResolveSetMembersAsync(request, cancellationToken);
        if (setMembers is not null)
        {
            return await PrepareSetAsync(request, setMembers, cancellationToken);
        }

        var restoreRequest = BuildRestoreRequest(request);

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
            return new SagaParticipantPrepareResult(
                SagaVote.Abort, $"Admission probe failed: {ex.Message}");
        }

        if (!await capacity.CanHostAsync(report, cancellationToken))
        {
            logger.LogWarning(
                "Restore participant: target infeasible for saga '{SagaId}': {Bytes} byte(s) over {Shards} shard(s).",
                request.SagaId, report.TotalByteLength, report.ShardCount);
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
                return new SagaParticipantPrepareResult(SagaVote.Commit);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (LatticeRestoreValidationException ex)
            {
                // A precondition failed (missing backup / base in the chain): this
                // is permanent, not a capacity problem - do not retry.
                await SafeGarbageCollectAsync(restoreRequest, cancellationToken);
                _built.TryRemove(request.SagaId, out _);
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

        var restoreRequest = BuildRestoreRequest(request);

        // Reuse the prepared shadow; if a reactivation lost the in-memory cache,
        // re-derive it - BuildShadowAsync is idempotent and resumable, so this
        // resolves back to the same fully built shadow without a full rebuild.
        if (!_built.TryGetValue(request.SagaId, out var result))
        {
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
        });

        // Single atomic alias swap: a reader sees whole-old or whole-new, never
        // half.
        await engine.CommitShadowAsync(result, cancellationToken);

        // Unblock local writes on this cluster now it has flipped. Cross-cluster
        // shipping stays paused until the saga globally completes (the fence
        // grain's global gate), so no early-flipping cluster re-advances the cut.
        await fence.UnblockWritesAsync();
    }

    /// <inheritdoc />
    public async Task AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)
    {
        var fence = grainFactory.GetGrain<ISagaWriteFenceGrain>(request.SagaId);

        if (engine is null)
        {
            // Backup not wired: nothing was prepared. Lift any fence and return.
            await fence.LiftAsync();
            return;
        }

        var setMembers = await ResolveSetMembersAsync(request, cancellationToken);
        if (setMembers is not null)
        {
            await AbortSetAsync(request, setMembers, fence, cancellationToken);
            return;
        }

        // Compensate: revert the alias to the pre-restore physical tree (a no-op
        // when this cluster never committed - the alias is already there), then
        // reliably garbage collect the shadow so no storage leaks, then lift the
        // fence. All three steps are idempotent, so this is safe when invoked by
        // the participant grain's fence-expiry auto-compensation after a
        // coordinator loss, and safe when the participant never prepared.
        var restoreRequest = BuildRestoreRequest(request);

        if (_built.TryGetValue(request.SagaId, out var result))
        {
            await SafeRevertAsync(result, cancellationToken);
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
        var requests = BuildSetRestoreRequests(members);
        var built = new List<LatticeRestoreResult>(requests.Count);

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
                return new SagaParticipantPrepareResult(
                    SagaVote.Abort, $"Set admission probe failed: {ex.Message}");
            }

            if (!await capacity.CanHostAsync(report, cancellationToken))
            {
                logger.LogWarning(
                    "Restore participant: set member tree '{TreeId}' infeasible for saga '{SagaId}'.",
                    restoreRequest.TargetTreeId, request.SagaId);
                await GarbageCollectAllAsync(built, requests, cancellationToken);
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
                return new SagaParticipantPrepareResult(
                    SagaVote.Abort, $"Set member '{restoreRequest.TargetTreeId}' build failed: {ex.Message}");
            }
        }

        _builtSets[request.SagaId] = built;
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
        var requests = BuildSetRestoreRequests(members);

        // Reuse the prepared group; a reactivation that lost the cache re-derives
        // every member idempotently (BuildShadowAsync is resumable).
        if (!_builtSets.TryGetValue(request.SagaId, out var built))
        {
            var rebuilt = new List<LatticeRestoreResult>(requests.Count);
            foreach (var restoreRequest in requests)
            {
                rebuilt.Add(await engine!.BuildShadowAsync(restoreRequest, cancellationToken));
            }

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
        });

        foreach (var result in built)
        {
            await engine!.CommitShadowAsync(result, cancellationToken);
        }

        await fence.UnblockWritesAsync();
    }

    /// <summary>
    /// Group-atomic abort for a set: reverts and garbage collects EVERY member tree
    /// (never some), then lifts the shared fence. Idempotent and safe under the
    /// participant grain's fence-expiry auto-compensation and when the cache was lost.
    /// </summary>
    private async Task AbortSetAsync(
        SagaControlRequest request,
        IReadOnlyList<BackupSetMember> members,
        ISagaWriteFenceGrain fence,
        CancellationToken cancellationToken)
    {
        if (_builtSets.TryGetValue(request.SagaId, out var built))
        {
            foreach (var result in built)
            {
                await SafeRevertAsync(result, cancellationToken);
                await SafeDeleteShadowByIdAsync(result.ShadowPhysicalTreeId, cancellationToken);
            }

            _builtSets.TryRemove(request.SagaId, out _);
        }
        else
        {
            // Reactivation lost the cache: re-derive each member's shadow id and GC
            // it without a rebuild. No commit can precede an abort in this saga
            // model, so each alias is still the pre-restore tree and no revert is
            // required.
            foreach (var restoreRequest in BuildSetRestoreRequests(members))
            {
                await SafeGarbageCollectAsync(restoreRequest, cancellationToken);
            }
        }

        await fence.LiftAsync();
    }

    /// <summary>Garbage collects every already-built member and any not-yet-built shadow of a failed set prepare.</summary>
    private async Task GarbageCollectAllAsync(
        IReadOnlyList<LatticeRestoreResult> built,
        IReadOnlyList<LatticeRestoreRequest> requests,
        CancellationToken cancellationToken)
    {
        foreach (var result in built)
        {
            await SafeDeleteShadowByIdAsync(result.ShadowPhysicalTreeId, cancellationToken);
        }

        // Also resolve-and-GC any member whose build never completed, so a partial
        // set prepare leaks no shadow storage.
        for (var i = built.Count; i < requests.Count; i++)
        {
            await SafeGarbageCollectAsync(requests[i], cancellationToken);
        }
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
