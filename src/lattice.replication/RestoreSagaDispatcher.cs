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
    ILatticeCoordinatedRestoreEngine? engine,
    IRestoreCapacityProbe capacity,
    ISagaControlChannel controlChannel,
    IGrainFactory grainFactory,
    RestoreParticipant localParticipant,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ILogger<RestoreSagaDispatcher> logger,
    ILatticeBackupSetResolver? setResolver = null) : IRestoreSagaDispatcher
{
    /// <inheritdoc />
    public async Task<LatticeRestoreResult?> TryDispatchAsync(
        LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // No coordinated-restore engine wired means the backup package is absent on
        // this host: there is nothing to promote to a saga, so decline and let the
        // caller run its plain local restore. This mirrors the restore participant's
        // optional posture and lets a replication-only host build and start (the
        // dispatcher is registered even when backup is not wired).
        if (engine is null)
        {
            return null;
        }

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
        var sagaId = DeriveSagaId(request.BackupId, targetTree);

        // All-or-nothing: refuse to start unless every current peer is reachable.
        var probe = new SagaControlRequest
        {
            SagaId = sagaId,
            TargetTree = targetTree,
            ManifestId = request.BackupId,
            CoordinatorClusterId = self,
        };
        await EnsurePeersReachableAsync(
            peers, probe, self,
            $"restore of backup '{request.BackupId}' into replicated tree '{targetTree}'",
            cancellationToken);

        // Participant set: the target's current peers plus this (coordinator)
        // cluster, de-duplicated. The coordinator grain canonicalises the set.
        var participants = BuildParticipantSet(self, peers);

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

    /// <inheritdoc />
    public async Task<IReadOnlyList<LatticeRestoreResult>?> TryDispatchSetAsync(
        string setId, LatticeRestoreMode mode, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(setId);

        // No engine or no set read seam wired means the backup package is absent or
        // set restore is unsupported here: decline so the caller runs the plain
        // local multi-tree restore.
        if (engine is null || setResolver is null)
        {
            return null;
        }

        // Expand the set into its member trees. An empty result means the id is not
        // a set id (a single-tree backup id), so the caller handles it as such.
        var members = await setResolver.ResolveMembersAsync(setId, cancellationToken);
        if (members.Count == 0)
        {
            return null;
        }

        // Set-level dispatch decision, keyed on the members' replication status now:
        // if NO member tree is replicated the whole set takes the plain local
        // multi-tree restore (no saga); if ANY member tree is replicated the set
        // runs one saga so it stays cross-tree atomic, with local-only members
        // riding along as local participants in the same saga.
        var anyReplicated = false;
        foreach (var member in members)
        {
            if (membership.IsReplicated(member.TreeId))
            {
                anyReplicated = true;
                break;
            }
        }

        if (!anyReplicated)
        {
            return null;
        }

        var self = options.CurrentValue.ClusterId;
        var peers = topology.CurrentPeers;
        var sagaId = DeriveSetSagaId(setId);

        // All-or-nothing across the union of the replicated members' peer sets. The
        // topology exposes a single flat peer set, so the union is this cluster plus
        // the current peers; every participant restores only the subset of the set's
        // trees it hosts.
        var probe = new SagaControlRequest
        {
            SagaId = sagaId,
            TargetTree = setId,
            ManifestId = setId,
            CoordinatorClusterId = self,
            SetId = setId,
        };
        await EnsurePeersReachableAsync(
            peers, probe, self, $"restore of backup set '{setId}'", cancellationToken);

        var participants = BuildParticipantSet(self, peers);

        logger.LogInformation(
            "Dispatching coordinated restore of backup set '{SetId}' as saga '{SagaId}' over {Count} " +
            "cluster(s) ({MemberCount} member tree(s)).",
            setId, sagaId, participants.Count, members.Count);

        // One saga for the whole set: the set id threads through the coordinator so
        // every outgoing control request carries it and each participant flips the
        // set's hosted member trees as one group.
        var coordinator = grainFactory.GetGrain<ICrossClusterSagaCoordinatorGrain>(sagaId);
        var outcome = await coordinator.RunAsync(participants, setId, setId, self, setId);

        if (outcome != CrossClusterSagaOutcome.Committed)
        {
            throw new LatticeRestoreValidationException(
                $"Coordinated restore of backup set '{setId}' aborted: at least one cluster could not prepare. " +
                $"Every cluster was compensated back to its pre-restore state, so no member tree was left " +
                $"committed while another aborted.");
        }

        // Return the local cluster's per-member build/commit results so the public
        // set-restore surface reports what this cluster restored. If a reactivation
        // dropped the in-memory cache, synthesize an equivalent committed summary
        // for every member.
        if (localParticipant.TryGetLocalSetResult(sagaId, out var localResults) && localResults is not null)
        {
            return localResults;
        }

        var synthesized = new List<LatticeRestoreResult>(members.Count);
        foreach (var member in members)
        {
            synthesized.Add(new LatticeRestoreResult(
                backupId: member.BackupId,
                targetTreeId: member.TreeId,
                mode: LatticeRestoreMode.ShadowCutover,
                operationId: sagaId,
                manifestChain: Array.Empty<string>(),
                entriesApplied: 0));
        }

        return synthesized;
    }

    /// <summary>
    /// Probes each peer over the saga control channel and throws if any is
    /// unreachable, so the coordinator refuses to start a partial restore. The
    /// <paramref name="probe"/> carries the saga identity (and, for a set restore,
    /// the set id); <paramref name="subject"/> names the restore in the refusal
    /// message.
    /// </summary>
    private async Task EnsurePeersReachableAsync(
        IReadOnlyCollection<string> peers,
        SagaControlRequest probe,
        string self,
        string subject,
        CancellationToken cancellationToken)
    {
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
                $"Coordinated {subject} refused: peer(s) [{string.Join(", ", unreachable)}] are unreachable. " +
                $"A coordinated restore is all-or-nothing across every current peer.");
        }
    }

    /// <summary>
    /// Builds the participant cluster set for a saga: this (coordinator) cluster
    /// plus every current peer, de-duplicated. The coordinator grain canonicalises
    /// the set.
    /// </summary>
    private static List<string> BuildParticipantSet(string self, IReadOnlyCollection<string> peers)
    {
        var participants = new List<string>(peers.Count + 1) { self };
        foreach (var peer in peers)
        {
            if (!string.Equals(peer, self, StringComparison.Ordinal))
            {
                participants.Add(peer);
            }
        }

        return participants;
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

    /// <summary>
    /// Deterministically derives the saga id for a <b>set</b> restore from the set
    /// id, so a retried set restore re-attaches to the same saga. Distinct from the
    /// single-tree derivation so a set id and a single-tree backup id can never
    /// collide on a saga id.
    /// </summary>
    private static string DeriveSetSagaId(string setId)
    {
        var canonical = $"set\u001f{setId}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return $"restore-set-{Convert.ToHexString(hash.AsSpan(0, 16)).ToLowerInvariant()}";
    }
}
