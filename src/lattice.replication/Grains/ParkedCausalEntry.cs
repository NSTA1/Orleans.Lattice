using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// One entry parked in the durable per-tree causal-apply buffer
/// (<see cref="CausalApplyBufferState"/>): the replication record whose
/// declared causal dependencies were not yet satisfied when it arrived, and
/// the UTC ticks at which it was first parked (kept so the dependency-wait
/// histogram stays accurate across a reactivation).
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ParkedCausalEntry)]
[Immutable]
internal sealed class ParkedCausalEntry
{
    /// <summary>The parked replication record.</summary>
    [Id(0)] public WalRecord Entry { get; init; }

    /// <summary>UTC ticks at which the entry was first parked.</summary>
    [Id(1)] public long ParkedAtTicks { get; init; }
}
