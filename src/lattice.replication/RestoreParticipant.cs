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
    ILogger<RestoreParticipant> logger) : ISagaParticipant
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
