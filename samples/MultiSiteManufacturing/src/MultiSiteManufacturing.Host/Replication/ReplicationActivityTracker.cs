using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Replication.Grains;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Façade over <see cref="IClusterReplicationStatsGrain"/> that is
/// safe to inject everywhere — even into singleton services on the
/// hot replication path — because all writes are fire-and-forget
/// and all reads are plain grain calls.
/// </summary>
/// <remarks>
/// Counters live in a single cluster-wide grain activation keyed by
/// <see cref="ReplicationTopology.LocalCluster"/>. Both silos in a
/// cluster route there, so any UI tab — regardless of which silo
/// served the Blazor circuit — sees aggregate cluster activity.
/// </remarks>
internal sealed class ReplicationActivityTracker(
    IGrainFactory grains,
    ReplicationTopology topology,
    ILogger<ReplicationActivityTracker> logger)
{
    private IClusterReplicationStatsGrain Grain =>
        grains.GetGrain<IClusterReplicationStatsGrain>(topology.LocalCluster);

    /// <summary>Records a successful outbound batch ship to <paramref name="peer"/>.</summary>
    public void RecordSent(string peer, int rows) =>
        _ = FireAndForgetAsync(Grain.RecordSentAsync(peer, rows), nameof(RecordSent));

    /// <summary>Records a failed outbound batch ship.</summary>
    public void RecordSendError() =>
        _ = FireAndForgetAsync(Grain.RecordSendErrorAsync(), nameof(RecordSendError));

    /// <summary>Records an applied inbound batch from <paramref name="peer"/>.</summary>
    public void RecordReceived(string peer, int rows) =>
        _ = FireAndForgetAsync(Grain.RecordReceivedAsync(peer, rows), nameof(RecordReceived));

    /// <summary>Returns the current cluster-wide snapshot.</summary>
    public Task<ReplicationActivitySnapshot> SnapshotAsync() => Grain.GetSnapshotAsync();

    private async Task FireAndForgetAsync(Task op, string name)
    {
        try
        {
            await op.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Replication stats {Op} swallowed (fire-and-forget).", name);
        }
    }
}

/// <summary>Immutable snapshot returned by <see cref="ReplicationActivityTracker.SnapshotAsync"/>.</summary>
[GenerateSerializer]
public sealed record ReplicationActivitySnapshot(
    [property: Id(0)] DateTime? LastSentUtc,
    [property: Id(1)] DateTime? LastReceivedUtc,
    [property: Id(2)] long BatchesSent,
    [property: Id(3)] long BatchesReceived,
    [property: Id(4)] long RowsSent,
    [property: Id(5)] long RowsReceived,
    [property: Id(6)] long SendErrors,
    [property: Id(7)] DateTime? LastSendErrorUtc,
    [property: Id(8)] string? LastSentPeer,
    [property: Id(9)] string? LastReceivedPeer);
