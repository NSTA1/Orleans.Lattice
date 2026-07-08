using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The real <see cref="IRestoreSagaDispatcher"/> for a replication-enabled host.
/// Promotes a restore whose <b>target tree is currently replicated</b> into an
/// all-or-nothing coordinated multi-cluster restore across the target's current
/// peer set, and declines (returns <c>null</c>, so the caller runs the plain
/// local shadow-cutover) otherwise. The decision is a function of the target tree
/// now, never of the backup's origin, which yields the four dispatch cases:
/// <list type="bullet">
///   <item><description>Replicated backup into a replicated target -> saga.</description></item>
///   <item><description>Unreplicated backup into a replicated target -> saga.</description></item>
///   <item><description>Replicated backup into an unreplicated target -> local, no saga.</description></item>
///   <item><description>Unreplicated backup into an unreplicated target -> local, no saga.</description></item>
/// </list>
/// <para>
/// Before starting the saga the dispatcher runs an admission pre-flight: it probes
/// the target's self-describing size against the coordinator cluster's capacity
/// and refuses an infeasible target up front, and it refuses to start unless every
/// current peer of the target is reachable (all-or-nothing; no partial restore).
/// It then runs the <see cref="ICrossClusterSagaCoordinatorGrain"/> over the
/// peer set plus this cluster, and on a committed outcome returns the local
/// cluster's restore result.
/// </para>
/// </summary>
internal sealed class RestoreSagaDispatcher(
    IReplicatedTreeMembership membership,
    IReplicationTopology topology,
    ILatticeCoordinatedRestoreEngine engine,
    IRestoreCapacityProbe capacity,
    ISagaControlChannel controlChannel,
    IGrainFactory grainFactory,
    RestoreParticipant localParticipant,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ILogger<RestoreSagaDispatcher> logger) : IRestoreSagaDispatcher
{
    /// <inheritdoc />
    public async Task<LatticeRestoreResult?> TryDispatchAsync(
        LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Fast local paths that avoid any manifest I/O: an explicitly targeted
        // tree that is not replicated, or a host with no replicated trees at all,
        // always takes the plain local restore.
        if (request.TargetTreeId is not null && !membership.IsReplicated(request.TargetTreeId))
        {
            return null;
        }

        if (membership.ReplicatedTrees.Count == 0)
        {
            return null;
        }

        // Resolve the effective target tree and self-describing size from the
        // manifest chain (needed when the request restores into the captured tree).
        var report = await engine.ProbeAdmissionAsync(request, cancellationToken);
        var targetTree = report.TargetTreeId;

        // The decision is keyed on the target tree's replication status now.
        if (!membership.IsReplicated(targetTree))
        {
            return null;
        }

        // Admission: refuse an infeasible target on the coordinator cluster before
        // fencing the fleet or building any shadow (the same posture as an offline
        // peer). Participant clusters re-check their own headroom in prepare.
        if (!await capacity.CanHostAsync(report, cancellationToken))
        {
            throw new LatticeRestoreValidationException(
                $"Coordinated restore of backup '{request.BackupId}' into replicated tree '{targetTree}' " +
                $"is infeasible on the coordinator cluster: {report.TotalByteLength} byte(s) over " +
                $"{report.ShardCount} shard(s).");
        }

        var self = options.CurrentValue.ClusterId;
        var peers = topology.CurrentPeers;

        // All-or-nothing: refuse to start unless every current peer is reachable.
        await EnsurePeersReachableAsync(peers, request, targetTree, self, cancellationToken);

        // Participant set: the target's current peers plus this (coordinator)
        // cluster, de-duplicated. The coordinator grain canonicalises the set.
        var participants = new List<string>(peers.Count + 1) { self };
        foreach (var peer in peers)
        {
            if (!string.Equals(peer, self, StringComparison.Ordinal))
            {
                participants.Add(peer);
            }
        }

        var sagaId = DeriveSagaId(request.BackupId, targetTree);
        logger.LogInformation(
            "Dispatching coordinated restore of backup '{BackupId}' into replicated tree '{TargetTree}' " +
            "as saga '{SagaId}' over {Count} cluster(s).",
            request.BackupId, targetTree, sagaId, participants.Count);

        var coordinator = grainFactory.GetGrain<ICrossClusterSagaCoordinatorGrain>(sagaId);
        var outcome = await coordinator.RunAsync(participants, targetTree, request.BackupId, self);

        if (outcome != CrossClusterSagaOutcome.Committed)
        {
            throw new LatticeRestoreValidationException(
                $"Coordinated restore of backup '{request.BackupId}' into replicated tree '{targetTree}' " +
                $"aborted: at least one cluster could not prepare. Every cluster was compensated back to " +
                $"its pre-restore state.");
        }

        // Return the local cluster's build/commit result so the public restore
        // surface is unchanged. If a reactivation dropped the in-memory cache,
        // synthesize an equivalent committed shadow-cutover summary.
        if (localParticipant.TryGetLocalResult(sagaId, out var local) && local is not null)
        {
            return local;
        }

        return new LatticeRestoreResult(
            backupId: request.BackupId,
            targetTreeId: targetTree,
            mode: LatticeRestoreMode.ShadowCutover,
            operationId: sagaId,
            manifestChain: report.ManifestChain,
            entriesApplied: 0);
    }

    /// <summary>
    /// Probes each peer over the saga control channel and throws if any is
    /// unreachable, so the coordinator refuses to start a partial restore.
    /// </summary>
    private async Task EnsurePeersReachableAsync(
        IReadOnlyCollection<string> peers,
        LatticeRestoreRequest request,
        string targetTree,
        string self,
        CancellationToken cancellationToken)
    {
        var probe = new SagaControlRequest
        {
            SagaId = DeriveSagaId(request.BackupId, targetTree),
            TargetTree = targetTree,
            ManifestId = request.BackupId,
            CoordinatorClusterId = self,
        };

        var unreachable = new List<string>();
        foreach (var peer in peers)
        {
            if (string.Equals(peer, self, StringComparison.Ordinal))
            {
                continue;
            }

            try
            {
                await controlChannel.GetStatusAsync(peer, probe, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex,
                    "Coordinated restore: peer '{Peer}' is unreachable; refusing to start a partial restore.",
                    peer);
                unreachable.Add(peer);
            }
        }

        if (unreachable.Count > 0)
        {
            throw new LatticeRestoreValidationException(
                $"Coordinated restore of backup '{request.BackupId}' into replicated tree '{targetTree}' " +
                $"refused: peer(s) [{string.Join(", ", unreachable)}] are unreachable. A coordinated restore " +
                $"is all-or-nothing across every current peer.");
        }
    }

    /// <summary>
    /// Deterministically derives the saga id from the backup id and target tree so
    /// a retried restore re-attaches to the same saga rather than starting a new
    /// one.
    /// </summary>
    private static string DeriveSagaId(string backupId, string targetTree)
    {
        var canonical = $"{backupId}\u001f{targetTree}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return $"restore-{Convert.ToHexString(hash.AsSpan(0, 16)).ToLowerInvariant()}";
    }
}
