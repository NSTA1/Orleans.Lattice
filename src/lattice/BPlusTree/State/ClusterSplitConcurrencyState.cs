namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persisted state for <see cref="Grains.ClusterSplitConcurrencyGrain"/>: the
/// per-tree heartbeat footprints reported by each enabled autonomic monitor.
/// <para>
/// Persistence exists so the cluster-wide in-flight count survives a
/// reactivation of the singleton gate without resetting to zero (which would
/// briefly let the aggregate ceiling be exceeded after a restart). Stale
/// footprints - trees whose monitors have stopped reporting - are dropped by
/// their expiry on the next call, so the list is self-cleaning and bounded by
/// the number of trees actively splitting.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ClusterSplitConcurrencyState)]
internal sealed class ClusterSplitConcurrencyState
{
    /// <summary>The most recent per-tree in-flight-split heartbeat footprints, live or awaiting expiry-based reclamation.</summary>
    [Id(0)] public List<TreeSplitFootprint> Footprints { get; set; } = [];
}
