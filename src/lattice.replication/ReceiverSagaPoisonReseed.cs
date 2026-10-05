using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Starts or retries the full re-seed owed by a receiver-side poisoned saga.
/// </summary>
internal static class ReceiverSagaPoisonReseed
{
    /// <summary>
    /// Starts a bootstrap from <paramref name="originClusterId"/> when automatic
    /// re-seed is enabled for <paramref name="treeId"/>. The poison grain keeps
    /// the owed marker until this method observes an accepted kickoff.
    /// </summary>
    public static async Task TryStartOrMarkOwedAsync(
        IGrainFactory grainFactory,
        IOptionsMonitor<LatticeReplicationOptions> options,
        string treeId,
        string originClusterId,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);

        var poison = grainFactory.GetGrain<IReceiverSagaPoisonGrain>(treeId);
        if (!options.Get(treeId).AutoBootstrapOnFallOffLog)
        {
            await poison.SetReseedOwedAsync(originClusterId, owed: true).ConfigureAwait(false);
            logger.LogWarning(
                "Receiver saga poison on tree '{TreeId}' from origin {Origin} requires a full re-seed, but automatic bootstrap is disabled.",
                treeId,
                originClusterId);
            return;
        }

        try
        {
            var coordinator = grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(treeId);
            await coordinator.BootstrapAsync(originClusterId, cancellationToken).ConfigureAwait(false);
            await poison.SetReseedOwedAsync(originClusterId, owed: false).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await poison.SetReseedOwedAsync(originClusterId, owed: true).ConfigureAwait(false);
            logger.LogDebug(
                ex,
                "Receiver saga poison re-seed bootstrap from origin {Origin} for tree '{TreeId}' was not accepted; maintenance will retry.",
                originClusterId,
                treeId);
        }
    }
}
