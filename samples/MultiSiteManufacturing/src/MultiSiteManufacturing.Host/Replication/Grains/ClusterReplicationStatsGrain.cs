using Orleans.Runtime;

namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>Persistent counters for <see cref="IClusterReplicationStatsGrain"/>.</summary>
[GenerateSerializer]
public sealed class ClusterReplicationStatsState
{
    /// <summary>UTC timestamp of the last successful outbound ship.</summary>
    [Id(0)] public DateTime? LastSentUtc { get; set; }

    /// <summary>UTC timestamp of the last applied inbound batch.</summary>
    [Id(1)] public DateTime? LastReceivedUtc { get; set; }

    /// <summary>Total outbound batches shipped since grain creation.</summary>
    [Id(2)] public long BatchesSent { get; set; }

    /// <summary>Total inbound batches applied since grain creation.</summary>
    [Id(3)] public long BatchesReceived { get; set; }

    /// <summary>Total rows shipped (sum of batch sizes).</summary>
    [Id(4)] public long RowsSent { get; set; }

    /// <summary>Total rows applied from inbound batches.</summary>
    [Id(5)] public long RowsReceived { get; set; }

    /// <summary>Outbound ship failures (transport + non-2xx).</summary>
    [Id(6)] public long SendErrors { get; set; }

    /// <summary>UTC timestamp of the last outbound send error.</summary>
    [Id(7)] public DateTime? LastSendErrorUtc { get; set; }

    /// <summary>Most recent peer to which we successfully shipped.</summary>
    [Id(8)] public string? LastSentPeer { get; set; }

    /// <summary>Most recent peer from which we applied an inbound batch.</summary>
    [Id(9)] public string? LastReceivedPeer { get; set; }
}

/// <inheritdoc cref="IClusterReplicationStatsGrain"/>
internal sealed class ClusterReplicationStatsGrain(
    [PersistentState("clusterReplicationStats", "msmfgGrainState")]
    IPersistentState<ClusterReplicationStatsState> state)
    : Grain, IClusterReplicationStatsGrain
{
    /// <inheritdoc />
    public async Task RecordSentAsync(string peer, int rows)
    {
        state.State.LastSentUtc = DateTime.UtcNow;
        state.State.BatchesSent++;
        state.State.RowsSent += rows;
        state.State.LastSentPeer = peer;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task RecordSendErrorAsync()
    {
        state.State.SendErrors++;
        state.State.LastSendErrorUtc = DateTime.UtcNow;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task RecordReceivedAsync(string peer, int rows)
    {
        state.State.LastReceivedUtc = DateTime.UtcNow;
        state.State.BatchesReceived++;
        state.State.RowsReceived += rows;
        state.State.LastReceivedPeer = peer;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public Task<ReplicationActivitySnapshot> GetSnapshotAsync() =>
        Task.FromResult(new ReplicationActivitySnapshot(
            LastSentUtc: state.State.LastSentUtc,
            LastReceivedUtc: state.State.LastReceivedUtc,
            BatchesSent: state.State.BatchesSent,
            BatchesReceived: state.State.BatchesReceived,
            RowsSent: state.State.RowsSent,
            RowsReceived: state.State.RowsReceived,
            SendErrors: state.State.SendErrors,
            LastSendErrorUtc: state.State.LastSendErrorUtc,
            LastSentPeer: state.State.LastSentPeer,
            LastReceivedPeer: state.State.LastReceivedPeer));
}
