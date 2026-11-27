using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Per-silo background reconciler for the admin audit trail. Each sweep resolves
/// this region's intents that have been pending longer than
/// <see cref="LatticeAuthOptions.AdminAuditReconcileLease"/> from their anchor's
/// evidence (<see cref="AdminAuditEvidence"/>), and, when
/// <see cref="LatticeAuthOptions.AdminAuditTrailTimeToLive"/> is set, removes final
/// records older than it.
/// </summary>
/// <remarks>
/// <para>
/// Every silo runs one. Sweeps on different silos may race on the same intent; the
/// create-only finalize (<see cref="AdminAuditStore.WriteFinalAsync"/>) admits exactly
/// one final record per intent, so a race is harmless.
/// </para>
/// <para>
/// Only intents authored by this region are resolved. An intent replicated from
/// another region describes a commit that region attempted, and only that region's
/// own history can say whether it landed; resolving it here would judge a remote call
/// by local evidence.
/// </para>
/// </remarks>
internal sealed class AdminAuditReconciler(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeAuthOptions> options,
    ILatticeReplicationContext replication,
    ILogger<AdminAuditReconciler> logger,
    TimeProvider? timeProvider = null) : BackgroundService
{
    /// <summary>The minimum spacing between two retention passes.</summary>
    internal static readonly TimeSpan RetentionSpacing = TimeSpan.FromHours(1);

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private DateTimeOffset _lastRetention = DateTimeOffset.MinValue;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(options.CurrentValue.AdminAuditReconcileInterval, _time, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (!options.CurrentValue.EnableAdminAuditTrail)
            {
                continue;
            }

            try
            {
                await SweepAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "The admin audit reconciler sweep failed; it will retry on the next interval.");
            }
        }
    }

    /// <summary>Runs one sweep: resolves stale intents, then applies retention when due.</summary>
    /// <param name="cancellationToken">Cancels the sweep.</param>
    /// <returns>The number of intents this sweep finalized.</returns>
    internal async Task<int> SweepAsync(CancellationToken cancellationToken)
    {
        var current = options.CurrentValue;
        var now = _time.GetUtcNow();
        var region = replication.LocalReplicaId ?? string.Empty;
        var tree = AdminAuditStore.Tree(grainFactory);

        // Nothing has ever been audited on this cluster: do not create the tree by reading it.
        if (!await grainFactory.GetLatticeRegistry().ExistsAsync(AuthConstants.AdminAuditTree).ConfigureAwait(false))
        {
            return 0;
        }

        var stale = new List<LatticeAdminAuditRecord>();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var entry in tree
                .ScanEntriesAsync(AdminAuditKeys.PendingPrefix, AdminAuditKeys.PendingEnd, cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (AdminAuditStore.Deserialize(entry.Value) is { } intent
                    && string.Equals(intent.OriginRegion, region, StringComparison.Ordinal)
                    && now - intent.TimestampUtc >= current.AdminAuditReconcileLease)
                {
                    stale.Add(intent);
                }
            }
        }

        var finalized = 0;
        foreach (var intent in stale)
        {
            try
            {
                // The caller already finalized but its intent delete was lost: remove
                // the leftover intent; the final record stands.
                using (LatticeAccessGateContext.EnterSystemOrigin())
                {
                    if (await tree.GetAsync(AdminAuditKeys.Final(intent), cancellationToken).ConfigureAwait(false) is not null)
                    {
                        await tree.DeleteAsync(AdminAuditKeys.Pending(intent.OperationId), cancellationToken).ConfigureAwait(false);
                        continue;
                    }
                }

                var completion = await AdminAuditEvidence
                    .ResolveAsync(grainFactory, intent, now - intent.TimestampUtc, current.AdminAuditMaxPendingAge, cancellationToken)
                    .ConfigureAwait(false);
                if (completion is not { } resolved)
                {
                    continue;
                }

                var final = intent with
                {
                    Outcome = resolved.Outcome,
                    ReasonCode = resolved.ReasonCode,
                    CompletedUtc = now,
                };
                if (await AdminAuditStore.WriteFinalAsync(grainFactory, final, cancellationToken).ConfigureAwait(false))
                {
                    finalized++;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                LatticeAuthMetrics.AdminAuditWriteFailures.Add(1, LatticeTenantLabel.Platform);
                logger.LogWarning(ex, "The admin audit reconciler could not resolve operation {OperationId}.", intent.OperationId);
            }
        }

        if (current.AdminAuditTrailTimeToLive is { } ttl && now - _lastRetention >= RetentionSpacing)
        {
            await ApplyRetentionAsync(tree, now - ttl, cancellationToken).ConfigureAwait(false);
            _lastRetention = now;
        }

        return finalized;
    }

    /// <summary>Removes every final record whose intent is older than <paramref name="cutoff"/>. Pending intents are never aged out.</summary>
    private static async Task ApplyRetentionAsync(ILattice tree, DateTimeOffset cutoff, CancellationToken cancellationToken)
    {
        var expired = new List<string>();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await foreach (var key in tree.ScanKeysAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (AdminAuditKeys.TryReadTicks(key, out var ticks) && ticks < cutoff.UtcTicks)
                {
                    expired.Add(key);
                }
            }

            foreach (var key in expired)
            {
                await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
