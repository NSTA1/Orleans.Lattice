namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="CausalApplyBufferGrain"/>: the durable
/// per-tree causal-apply buffer (#4464). Holds, in FIFO park order, every
/// replication record that was acknowledged to its sender but parked because
/// its declared causal dependencies were not yet satisfied. Persisting the
/// buffer is what makes acknowledging a parked entry safe: a receiver restart
/// no longer loses it, and a drain is re-armed on reactivation. Bounded by
/// <see cref="LatticeReplicationOptions.CausalBufferMaxEntries"/> and
/// <see cref="LatticeReplicationOptions.CausalBufferMaxBytes"/>, exactly like
/// the in-memory buffer it mirrors.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.CausalApplyBufferState)]
internal sealed class CausalApplyBufferState
{
    /// <summary>The parked entries, oldest first.</summary>
    [Id(0)] public List<ParkedCausalEntry> Entries { get; set; } = new();
}
