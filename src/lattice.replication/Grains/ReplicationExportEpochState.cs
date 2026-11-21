namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persisted state of <see cref="IReplicationExportEpochGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationExportEpochState)]
internal sealed class ReplicationExportEpochState
{
    /// <summary>The last epoch a full export took; <c>0</c> before the first.</summary>
    [Id(0)]
    public long Epoch { get; set; }

    /// <summary>
    /// The cuts of the most recent exports, oldest first, at most
    /// <see cref="ReplicationExportEpochGrain.MaxRecordedCuts"/> (issue #5794).
    /// </summary>
    [Id(1)]
    public List<ReplicationExportCut> Cuts { get; set; } = [];
}
