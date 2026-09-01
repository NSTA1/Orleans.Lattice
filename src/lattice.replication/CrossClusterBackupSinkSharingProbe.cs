using System.Buffers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The real <see cref="IBackupSinkSharingProbe"/> for a replication-enabled host.
/// Proves - rather than assumes - that the backup sink this cluster captures into
/// is the same physical store every peer reads from, which is the precondition a
/// coordinated restore of a replicated tree silently depends on. Each cluster
/// resolves the manifest chain from its own configured sink, so a deployment that
/// points each region at an isolated sink captures backups that can never be
/// restored, and until this probe existed that only surfaced as an all-or-nothing
/// saga abort at restore time.
/// <para>
/// The check is a symmetric marker exchange that needs no new cross-cluster RPC:
/// every cluster writes a tiny marker naming itself into its own sink, then reads
/// each <b>peer's</b> marker id back out of that same sink. If the store is
/// shared, every peer's marker is visible locally; if each cluster has its own
/// store, only its own marker is. The verdict is deliberately three-valued rather
/// than a boolean, because an absent marker is only evidence of a non-shared sink
/// when the peer is otherwise known to be up: a peer that does not answer the saga
/// control channel may simply not have started, so it yields
/// <see cref="BackupSinkSharingStatus.Unverified"/> and never a false accusation.
/// </para>
/// <para>
/// Fail-closed by construction. Every marker id is derived from a <b>locally
/// configured</b> peer id and the body must attest to that same id, so nothing
/// read out of the sink can nominate itself as belonging to a peer; a malformed,
/// oversized, or mismatched body is treated as absent rather than as proof. The
/// probe is completely inert when it cannot apply - no replicated tree, no peers,
/// or no backup sink wired - so a single-cluster deployment performs no I/O and
/// gains no new failure mode. Every collaborator that this package does not
/// itself register is optional, so wiring the probe can never stop a host that
/// used to start: without a control channel no peer can be shown to be up and the
/// verdict stays <see cref="BackupSinkSharingStatus.Unverified"/>; without a sink
/// there is nothing to probe at all.
/// </para>
/// </summary>
internal sealed class CrossClusterBackupSinkSharingProbe(
    IReplicatedTreeMembership membership,
    IReplicationTopology topology,
    ISagaControlChannel? controlChannel,
    IOptionsMonitor<LatticeReplicationOptions> options,
    ILogger<CrossClusterBackupSinkSharingProbe> logger,
    ILatticeBackupSink? sink = null) : IBackupSinkSharingProbe
{
    private volatile BackupSinkSharingReport? _lastReport;

    /// <inheritdoc />
    public BackupSinkSharingReport? LastReport => _lastReport;

    /// <inheritdoc />
    public async Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken = default)
    {
        var self = options.CurrentValue.ClusterId;
        var peers = topology.CurrentPeers;
        var peerCount = CountPeers(peers, self);

        // Inert unless the probe can actually apply. Ordered cheapest-first: no
        // backup package wired, no usable local identity to attest to, nothing
        // replicated, or no peer to share a sink with. A single-cluster deployment
        // always lands here and never touches the sink or the network.
        if (sink is null
            || string.IsNullOrWhiteSpace(self)
            || membership.ReplicatedTrees.Count == 0
            || peerCount == 0)
        {
            return Publish(new BackupSinkSharingReport(
                BackupSinkSharingStatus.NotApplicable,
                clusterId: self ?? string.Empty,
                peerCount: 0,
                unconfirmedPeerClusterIds: [],
                probedAtUtc: DateTimeOffset.UtcNow,
                explanation: InapplicableReason(sink, self, membership.ReplicatedTrees.Count)));
        }

        var clusterId = self;
        var probedAt = DateTimeOffset.UtcNow;

        // Publish this cluster's own marker first so a peer probing concurrently
        // can see it. Rewriting the same id is idempotent, so exactly one small
        // marker per cluster ever exists in the sink - no unbounded litter.
        await sink.WriteArtifactAsync(
            BackupSinkCanary.ArtifactId(clusterId),
            SingleChunk(BackupSinkCanary.Encode(clusterId, probedAt)),
            cancellationToken).ConfigureAwait(false);

        var unconfirmed = new List<string>();
        var anyReachableAndMissing = false;
        foreach (var peer in peers)
        {
            if (string.Equals(peer, clusterId, StringComparison.Ordinal))
            {
                continue;
            }

            if (await PeerMarkerVisibleAsync(sink, peer, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            unconfirmed.Add(peer);

            // A missing marker only accuses the sink when the peer is demonstrably
            // up. An unreachable peer may simply not have started, so it downgrades
            // the verdict to Unverified instead of refuting the sink.
            if (await PeerReachableAsync(peer, clusterId, cancellationToken).ConfigureAwait(false))
            {
                anyReachableAndMissing = true;
            }
        }

        return Publish(BuildReport(clusterId, peerCount, unconfirmed, anyReachableAndMissing, probedAt));
    }

    /// <summary>
    /// Names which precondition kept the probe inert, so an operator who expected a
    /// verdict can see exactly why there is none rather than a bare "not applicable".
    /// </summary>
    private static string InapplicableReason(ILatticeBackupSink? backupSink, string? self, int replicatedTreeCount)
    {
        if (backupSink is null)
        {
            return "Cross-cluster backup sink sharing is not probed: no backup sink is wired into this host.";
        }

        if (string.IsNullOrWhiteSpace(self))
        {
            return "Cross-cluster backup sink sharing is not probed: this host has no configured replication cluster id to attest to.";
        }

        return replicatedTreeCount == 0
            ? "Cross-cluster backup sink sharing is not probed: no tree is replicated, so no backup needs to be readable from another cluster."
            : "Cross-cluster backup sink sharing is not probed: this deployment has no peer clusters.";
    }

    /// <summary>
    /// Reads <paramref name="peer"/>'s marker back out of <b>this</b> cluster's sink
    /// and reports whether it provably attests to that peer. A read fault is treated
    /// as "not visible" rather than propagated: the caller's reachability check
    /// decides whether that absence is an accusation or merely undecided.
    /// </summary>
    private async Task<bool> PeerMarkerVisibleAsync(
        ILatticeBackupSink backupSink,
        string peer,
        CancellationToken cancellationToken)
    {
        var artifactId = BackupSinkCanary.ArtifactId(peer);

        // Rented rather than allocated: the cap is the rejection threshold, not the
        // expected size (a marker is under a hundred bytes), so allocating it per
        // peer per probe would waste a 4 KB array to read ~80 bytes.
        var rented = ArrayPool<byte>.Shared.Rent(BackupSinkCanary.MaxBytes);
        try
        {
            var written = 0;
            await foreach (var chunk in backupSink.ReadArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false))
            {
                if (chunk.Length > BackupSinkCanary.MaxBytes - written)
                {
                    // Over the cap: not a marker this probe wrote, so it proves
                    // nothing. Stop reading rather than buffer an unbounded blob.
                    return false;
                }

                chunk.Span.CopyTo(rented.AsSpan(written));
                written += chunk.Length;
            }

            return BackupSinkCanary.Attests(rented.AsSpan(0, written), peer);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogDebug(
                ex,
                "Backup sink sharing probe: reading peer '{Peer}' marker '{ArtifactId}' failed; treating it as absent.",
                peer,
                artifactId);
            return false;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Tests whether <paramref name="peer"/> answers the saga control channel, the
    /// same liveness signal the coordinated-restore pre-flight uses. Any transport
    /// fault counts as unreachable, which is the conservative direction: it can only
    /// soften a verdict from refuted to unverified, never manufacture an accusation.
    /// <para>
    /// The control channel is optional because only the gRPC replication transport
    /// package registers one; a host on the no-op or a custom transport has no way
    /// to establish peer liveness at all, so every peer counts as unreachable and
    /// the verdict can only ever be <see cref="BackupSinkSharingStatus.Unverified"/>.
    /// Refusing to accuse without liveness evidence is the whole point of the
    /// three-valued verdict.
    /// </para>
    /// </summary>
    private async Task<bool> PeerReachableAsync(string peer, string self, CancellationToken cancellationToken)
    {
        if (controlChannel is null)
        {
            return false;
        }

        var probe = new SagaControlRequest
        {
            SagaId = string.Empty,
            TargetTree = string.Empty,
            ManifestId = string.Empty,
            CoordinatorClusterId = self,
        };

        try
        {
            await controlChannel.GetStatusAsync(peer, probe, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogDebug(
                ex,
                "Backup sink sharing probe: peer '{Peer}' is unreachable, so its absent sink marker is inconclusive.",
                peer);
            return false;
        }
    }

    /// <summary>
    /// Turns the per-peer observations into a verdict and the operator-facing
    /// sentence that names the remediation.
    /// </summary>
    private static BackupSinkSharingReport BuildReport(
        string clusterId,
        int peerCount,
        List<string> unconfirmed,
        bool anyReachableAndMissing,
        DateTimeOffset probedAt)
    {
        if (unconfirmed.Count == 0)
        {
            return new BackupSinkSharingReport(
                BackupSinkSharingStatus.Shared,
                clusterId,
                peerCount,
                unconfirmedPeerClusterIds: [],
                probedAt,
                $"The backup sink configured on cluster '{clusterId}' is shared with all {peerCount} peer cluster(s): "
                + "every peer's sink marker was read back from it, so a coordinated restore can resolve the same backup fleet-wide.");
        }

        var peers = string.Join(", ", unconfirmed);
        return anyReachableAndMissing
            ? new BackupSinkSharingReport(
                BackupSinkSharingStatus.NotShared,
                clusterId,
                peerCount,
                unconfirmed,
                probedAt,
                $"The backup sink configured on cluster '{clusterId}' is NOT shared with peer cluster(s) [{peers}]: "
                + "they are running and reachable, yet their sink markers are absent from this cluster's sink, so each cluster is "
                + "writing to an isolated store. Backups captured for a replicated tree cannot be restored, because a coordinated "
                + "restore requires every cluster to read the same backup. Point every cluster at one shared external backup sink.")
            : new BackupSinkSharingReport(
                BackupSinkSharingStatus.Unverified,
                clusterId,
                peerCount,
                unconfirmed,
                probedAt,
                $"Backup sink sharing between cluster '{clusterId}' and peer cluster(s) [{peers}] is unconfirmed: their sink markers "
                + "are absent and they did not answer the saga control channel, so they may not be running yet. The periodic backup "
                + "health sweep re-probes and resolves the verdict.");
    }

    /// <summary>Counts the peers excluding this cluster's own entry, if present.</summary>
    private static int CountPeers(IReadOnlyCollection<string> peers, string? self)
    {
        var count = 0;
        foreach (var peer in peers)
        {
            if (!string.Equals(peer, self, StringComparison.Ordinal))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Publishes the verdict for the cheap cached read the health path uses.</summary>
    private BackupSinkSharingReport Publish(BackupSinkSharingReport report)
    {
        _lastReport = report;
        return report;
    }

    /// <summary>Adapts a single marker payload to the sink's chunked write surface.</summary>
    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> SingleChunk(byte[] payload)
    {
        yield return payload;
        await Task.CompletedTask.ConfigureAwait(false);
    }
}
