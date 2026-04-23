using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Per-silo hosted service that polls <see cref="IPartitionChaosGrain"/>
/// every two seconds. On a <c>true → false</c> transition it invokes
/// <see cref="PartCrdtStore.HealLocalShadowAsync"/> so every
/// partition-era shadow write is promoted into the shared CRDT state
/// and GC'd. Running on every silo is intentional: each silo heals
/// only its own shadow prefix (<c>shadow/{siloId}/…</c>), and the
/// resulting two promotions naturally merge in the shared tree via
/// LWW (operator register) and set union (G-Set labels).
/// </summary>
internal sealed class PartitionHealHostedService(
    IGrainFactory grainFactory,
    PartCrdtStore store,
    SiloIdentity silo,
    ILogger<PartitionHealHostedService> logger) : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        bool? previouslyPartitioned = null;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var current = await grainFactory
                    .GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey)
                    .IsPartitionedAsync()
                    .ConfigureAwait(false);

                if (previouslyPartitioned == true && !current)
                {
                    logger.LogInformation(
                        "Silo {Silo}: partition healed, draining local CRDT shadow...",
                        silo.Id);
                    var promoted = await store.HealLocalShadowAsync(stoppingToken).ConfigureAwait(false);
                    logger.LogInformation(
                        "Silo {Silo}: promoted {Count} shadow entries to shared CRDT state.",
                        silo.Id, promoted);
                }

                previouslyPartitioned = current;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Partition-heal poll failed; will retry");
            }

            try { await Task.Delay(PollInterval, stoppingToken).ConfigureAwait(false); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
        }
    }
}
