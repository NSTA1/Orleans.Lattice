namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A cluster-split-gate heartbeat entry: one tree's most recently reported count
/// of autonomic shard splits currently in flight, together with the UTC instant
/// after which the report is considered stale and reclaimable.
/// <para>
/// Each enabled <see cref="Grains.HotShardMonitorGrain"/> refreshes its own
/// footprint every sampling pass with the authoritative in-flight count derived
/// from shard <c>IsSplitting</c> status. If a silo crashes and its monitor stops
/// reporting, the footprint lapses at <see cref="ExpiryUtc"/> and the gate drops
/// it on the next call, so an abandoned split cannot permanently consume cluster
/// budget or wedge splitting cluster-wide.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeSplitFootprint)]
[Immutable]
internal readonly record struct TreeSplitFootprint
{
    /// <summary>Creates a heartbeat footprint entry.</summary>
    /// <param name="treeId">The reporting tree's id.</param>
    /// <param name="inFlight">The tree's reported in-flight autonomic split count (observed drains plus slots granted this pass).</param>
    /// <param name="expiryUtc">UTC instant after which the report is stale and reclaimable by the gate.</param>
    public TreeSplitFootprint(string treeId, int inFlight, DateTime expiryUtc)
    {
        TreeId = treeId;
        InFlight = inFlight;
        ExpiryUtc = expiryUtc;
    }

    /// <summary>The reporting tree's id.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The tree's reported in-flight autonomic split count.</summary>
    [Id(1)] public int InFlight { get; init; }

    /// <summary>UTC instant after which this footprint is expired and dropped by the gate.</summary>
    [Id(2)] public DateTime ExpiryUtc { get; init; }
}
