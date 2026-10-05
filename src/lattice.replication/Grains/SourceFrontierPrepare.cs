namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// One saga in <see cref="SourceFrontierShipperState.Prepares"/>: the earliest
/// acknowledged prepare's stamp and the shards whose terminals the peer has
/// acknowledged.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SourceFrontierPrepare)]
internal sealed class SourceFrontierPrepare
{
    /// <summary>The lowest stamp of the saga's prepares the peer acknowledged.</summary>
    [Id(0)]
    public HybridLogicalClock MinPrepare { get; set; }

    /// <summary>The shard indices whose terminal the peer acknowledged.</summary>
    [Id(1)]
    public HashSet<int> AckedTerminalShards { get; set; } = new();
}
