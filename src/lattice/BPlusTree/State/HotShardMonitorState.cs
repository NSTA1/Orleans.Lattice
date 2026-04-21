using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Lightweight persisted state for <see cref="Grains.HotShardMonitorGrain"/>.
/// <para>
/// Tracks the first time this monitor activated for a tree so the
/// <see cref="LatticeOptions.AutoSplitMinTreeAge"/> grace period survives
/// silo restarts. Without persistence, the grace clock restarts
/// on every activation and in a cluster with frequent restarts the monitor
/// never accumulates enough uptime to trigger an autonomic split.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.HotShardMonitorState)]
internal sealed class HotShardMonitorState
{
    /// <summary>
    /// UTC timestamp of the monitor's first activation for this tree.
    /// Used to apply <see cref="LatticeOptions.AutoSplitMinTreeAge"/>
    /// consistently across silo restarts.
    /// </summary>
    [Id(0)] public DateTime? ActivationUtc { get; set; }
}
