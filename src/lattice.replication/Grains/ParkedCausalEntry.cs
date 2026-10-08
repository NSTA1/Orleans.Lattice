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

    /// <summary>
    /// The tree's receive-fence epoch, read uncached when the entry was parked
    /// (issue #4593). The drain stamps the entry's apply with it, so a restored
    /// copy refuses an entry parked before its restore paused receiving. Zero for
    /// an entry parked before the epoch was recorded.
    /// </summary>
    [Id(2)] public long AdmissionEpoch { get; init; }

    /// <summary>
    /// The source lineage the entry's sender stamped on the batch it arrived in
    /// (issue #4707). The drain checks the entry against the lineage the tree has
    /// drained by then. <see langword="null"/> for an unstamped entry, and for an
    /// entry parked before the stamp was recorded, which apply as before.
    /// </summary>
    [Id(3)] public ReplicationSourceLineageStamp? SourceLineage { get; init; }

    /// <summary>The authenticated direct sender that delivered the parked entry.</summary>
    [Id(4)] public string? AuthenticatedSenderClusterId { get; init; }
}
