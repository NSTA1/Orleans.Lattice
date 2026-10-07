namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The shipper's durable inputs to the applied low watermark it ships to its
/// peer (issue #4586 part 2b). See <see cref="ReplicationShipperState.Frontier"/>.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SourceFrontierShipperState)]
internal sealed class SourceFrontierShipperState
{
    /// <summary>Whether the shipper has seen any acknowledgement's receiver lineage yet.</summary>
    [Id(0)]
    public bool LineageObserved { get; set; }

    /// <summary>
    /// The receiver lineage of the tree the shipper last saw; <see langword="null"/>
    /// when the receiver tracks none (and before the first observation).
    /// </summary>
    [Id(1)]
    public Guid? Lineage { get; set; }

    /// <summary>
    /// <see langword="true"/> while a lineage change still owes the peer a re-seed
    /// the shipper has not started: re-seeds for lineage changes are paced per
    /// peer, so a rollout does not re-seed every tree at once.
    /// </summary>
    [Id(2)]
    public bool LineageReseedPending { get; set; }

    /// <summary><see langword="true"/> while the shipper holds a lineage re-seed slot from its peer's pacing.</summary>
    [Id(3)]
    public bool LineageReseedLeased { get; set; }

    /// <summary>
    /// Per saga, the shipped prepares the peer acknowledged whose terminals it has
    /// not all acknowledged yet. A prepared write is invisible on the peer until
    /// its saga's terminal lands, so the watermark never passes the earliest.
    /// Bounded by <see cref="ReplicationShipperGrain.MaxFrontierPrepares"/>.
    /// </summary>
    [Id(4)]
    public Dictionary<Guid, SourceFrontierPrepare> Prepares { get; set; } = new();

    /// <summary>
    /// The lowest stamp of a shipped prepare the shipper could not record because
    /// <see cref="Prepares"/> was full, or <see langword="null"/>. The watermark
    /// never passes it again.
    /// </summary>
    [Id(5)]
    public HybridLogicalClock? OverflowClamp { get; set; }

    /// <summary>
    /// The lowest stamp of a local record the cursor passed without delivering it
    /// (a batch that could not be encoded), or <see langword="null"/>. Cleared only by a
    /// re-seed from an export after <see cref="SkipClampEpoch"/>, which carries it.
    /// </summary>
    [Id(6)]
    public HybridLogicalClock? SkipClamp { get; set; }

    /// <summary>The tree's export epoch when <see cref="SkipClamp"/> was last lowered.</summary>
    [Id(7)]
    public long SkipClampEpoch { get; set; }

    /// <summary>
    /// Whether a modern peer acknowledged data before it first reported a
    /// lineage, with no earlier acknowledged cursor to protect. Its first
    /// reported lineage identifies those already-accepted contents rather than
    /// replacing a known lineage.
    /// </summary>
    [Id(8)]
    public bool ModernAcceptedBeforeFirstLineage { get; set; }
}
