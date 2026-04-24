using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Replication.Grains;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// One-shot hosted service that activates every
/// <see cref="IReplicatorGrain"/> and <see cref="IReplogJanitorGrain"/>
/// exactly once per silo startup. Activation kicks the grain's
/// <c>OnActivateAsync</c>, which registers the persistent reminder —
/// after that the grain drives itself on its reminder schedule even
/// if no external caller ever invokes it again.
/// </summary>
/// <remarks>
/// Runs on <b>every</b> silo. Orleans' single-activation guarantee
/// means the cluster has exactly one active grain per
/// <c>(tree, peer)</c> pair regardless of how many silos call
/// <see cref="TickAsync"/>. A silo restart doesn't double-register
/// reminders because <c>RegisterOrUpdateReminder</c> is idempotent.
///
/// Bootstrap runs on a background task (not inline in
/// <see cref="StartAsync"/>) because <c>IHostedService.StartAsync</c>
/// executes before Orleans has finished the <c>Active</c> lifecycle
/// stage. Calling into <see cref="IGrainFactory"/> at that point
/// yields "AllActiveSilos empty" placement failures and
/// "Unable to create local activation" forwarding rejections. A
/// simple retry loop with backoff is sufficient: once membership is
/// up the activation succeeds on the next attempt.
/// </remarks>
internal sealed class ReplicationBootstrapHostedService(
    IGrainFactory grains,
    ReplicationTopology topology,
    ILogger<ReplicationBootstrapHostedService> logger) : IHostedService
{
    private readonly CancellationTokenSource _cts = new();
    private Task? _bootstrapTask;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!topology.IsEnabled)
        {
            logger.LogInformation("Replication is not enabled on cluster {Cluster} — skipping bootstrap", topology.LocalCluster);
            return Task.CompletedTask;
        }

        _bootstrapTask = Task.Run(() => BootstrapAsync(_cts.Token));
        return Task.CompletedTask;
    }

    private async Task BootstrapAsync(CancellationToken cancellationToken)
    {
        foreach (var tree in topology.ReplicatedTrees)
        {
            foreach (var peer in topology.Peers)
            {
                var key = $"{tree}|{peer.Name}";
                await ActivateWithRetryAsync(
                    $"Replicator tree={tree} peer={peer.Name}",
                    () => grains.GetGrain<IReplicatorGrain>(key).GetStatusAsync(),
                    cancellationToken).ConfigureAwait(false);
            }

            await ActivateWithRetryAsync(
                $"Replog janitor tree={tree}",
                () => grains.GetGrain<IReplogJanitorGrain>(tree).SweepAsync(),
                cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ActivateWithRetryAsync(string label, Func<Task> activate, CancellationToken cancellationToken)
    {
        // Exponential backoff: 1s, 2s, 4s, 8s, capped at 15s. Give up after ~2 minutes;
        // at that point something is genuinely wrong and the log warning is the signal.
        var delay = TimeSpan.FromSeconds(1);
        var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await activate().ConfigureAwait(false);
                logger.LogInformation("{Label} activated", label);
                return;
            }
            catch (Exception ex) when (DateTime.UtcNow < deadline)
            {
                logger.LogDebug(ex, "{Label} activation not yet ready; retrying in {Delay}", label, delay);
                try { await Task.Delay(delay, cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
                delay = TimeSpan.FromSeconds(Math.Min(15, delay.TotalSeconds * 2));
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "{Label} activation gave up after retry window", label);
                return;
            }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        if (_bootstrapTask is not null)
        {
            try { await _bootstrapTask.ConfigureAwait(false); }
            catch { /* swallow on shutdown */ }
        }
    }
}
