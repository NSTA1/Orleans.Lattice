using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Startup guard that refuses to let a replicated tree be backed by a sink the
/// rest of the replication set cannot read. A coordinated restore is
/// all-or-nothing across every cluster and each cluster resolves the manifest
/// chain from its own configured sink, so a per-cluster sink silently produces
/// backups that can never be restored - a fault that used to surface only at
/// restore time, long after the operator started relying on those backups.
/// <para>
/// The guard applies two checks in order:
/// </para>
/// <list type="number">
/// <item>
/// <description>
/// The default in-cluster <see cref="InClusterLatticeBackupSink"/> is rejected
/// outright for any replicated tree. It dogfoods a per-cluster reserved tree, so
/// a backup written on one cluster is provably invisible to the others. This is
/// locally decidable, needs no peer, and always throws.
/// </description>
/// </item>
/// <item>
/// <description>
/// Any other (external) sink is put to an actual cross-cluster test through the
/// <see cref="IBackupSinkSharingProbe"/> seam rather than assumed to be shared.
/// "Is this external store the same store every peer reads" is a deployment
/// fact - two regions can hold identical-looking connection strings resolving to
/// different accounts - so it can only be settled by writing a marker into the
/// sink and reading every peer's marker back out of it. A positively refuted
/// sink is enforced according to
/// <see cref="LatticeBackupOptions.SinkSharingEnforcement"/>, which defaults to a
/// loud warning plus a backup-health annotation rather than a blocked start.
/// </description>
/// </item>
/// </list>
/// <para>
/// Both checks read the replicated-tree set through the backup-local
/// <see cref="IReplicatedTreeMembership"/> seam and reach the peers through the
/// backup-local <see cref="IBackupSinkSharingProbe"/> seam, so the guard carries
/// no dependency on the replication package. In a single-cluster deployment the
/// default no-op seams report nothing replicated and no peers, the guard performs
/// no I/O at all, and the in-cluster sink stays the accepted default.
/// </para>
/// </summary>
internal sealed class LatticeBackupReplicatedSinkStartupValidator(
    ILatticeBackupSink sink,
    IReplicatedTreeMembership membership,
    IBackupSinkSharingProbe sharingProbe,
    IOptionsMonitor<LatticeBackupOptions> options,
    ILogger<LatticeBackupReplicatedSinkStartupValidator> logger) : IHostedService
{
    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Nothing is replicated: every sink is trivially adequate and no peer could
        // care what this cluster captures. Never probe.
        if (membership.ReplicatedTrees.Count == 0)
        {
            return;
        }

        // Hard type check: the in-cluster sink is provably per-cluster, so it is
        // rejected without consulting any peer.
        if (sink is InClusterLatticeBackupSink)
        {
            foreach (var treeId in membership.ReplicatedTrees)
            {
                throw new InvalidOperationException(
                    $"Tree '{treeId}' participates in the cross-cluster replication set but the "
                    + $"backup sink resolved to the default in-cluster {nameof(InClusterLatticeBackupSink)}. "
                    + "A replicated tree must be backed by a shared external sink reachable by every "
                    + "cluster so a backup captured on one cluster is resolvable and extendable from the "
                    + $"others. Register a shared external {nameof(ILatticeBackupSink)} implementation "
                    + "(for example a durable off-cluster provider) before AddLatticeBackup, or remove "
                    + "this tree from the replicated set.");
            }
        }

        var enforcement = options.CurrentValue.SinkSharingEnforcement;
        if (enforcement == BackupSinkSharingEnforcement.Disabled)
        {
            return;
        }

        // An external sink is no longer assumed shared: put it to the test.
        var report = await ProbeAsync(cancellationToken).ConfigureAwait(false);
        switch (report.Status)
        {
            case BackupSinkSharingStatus.NotShared when enforcement == BackupSinkSharingEnforcement.FailFast:
                throw new InvalidOperationException(
                    $"{report.Explanation} Startup is blocked because "
                    + $"{nameof(LatticeBackupOptions)}.{nameof(LatticeBackupOptions.SinkSharingEnforcement)} "
                    + $"is {nameof(BackupSinkSharingEnforcement.FailFast)}. Point every cluster at the same "
                    + $"external {nameof(ILatticeBackupSink)} store, or remove these trees from the "
                    + "replicated set.");

            case BackupSinkSharingStatus.NotShared:
                logger.LogWarning(
                    "Backup sink is NOT shared across the replication set. {Explanation}",
                    report.Explanation);
                break;

            case BackupSinkSharingStatus.Unverified:
                logger.LogInformation(
                    "Backup sink sharing could not be confirmed yet. {Explanation}",
                    report.Explanation);
                break;

            default:
                break;
        }
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Runs the sharing probe under the configured timeout. A probe that faults or
    /// times out must never block a silo start on its own: an infrastructure hiccup
    /// while reading the sink is not evidence the sink is unshared, so a failure
    /// degrades to <see cref="BackupSinkSharingStatus.Unverified"/> and is
    /// re-decided by the periodic backup-health sweep.
    /// </summary>
    private async Task<BackupSinkSharingReport> ProbeAsync(CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(options.CurrentValue.SinkSharingProbeTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            return await sharingProbe.ProbeAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "The cross-cluster backup sink sharing probe failed; treating sharing as unverified.");
            return new BackupSinkSharingReport(
                BackupSinkSharingStatus.Unverified,
                clusterId: string.Empty,
                peerCount: 0,
                unconfirmedPeerClusterIds: [],
                probedAtUtc: DateTimeOffset.UtcNow,
                explanation: "The cross-cluster backup sink sharing probe did not complete, so sharing "
                    + "could be neither confirmed nor refuted. The periodic backup health sweep retries it.");
        }
    }
}
