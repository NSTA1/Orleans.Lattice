namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The counters sampled for one physical shard during a single hot-shard
/// monitor pass, in the shape <see cref="ShardSplitAdmissionCore"/> consumes.
/// </summary>
/// <remarks>
/// Occupancy is sampled in a second phase, only for shards that already cleared
/// every cheaper clause, so the first-phase sample carries
/// <see cref="EntriesNotSampled"/> and the occupancy clause is skipped. This
/// keeps a uniformly loaded tree - the bulk ingest shape - from paying for a
/// single occupancy probe.
/// </remarks>
internal readonly record struct ShardSplitSample
{
    /// <summary>
    /// Sentinel for <see cref="Entries"/> meaning "occupancy has not been
    /// sampled for this shard yet", which suppresses the occupancy clause
    /// rather than failing it.
    /// </summary>
    public const int EntriesNotSampled = -1;

    /// <summary>
    /// The shard's observed operations per second over its hotness window, as
    /// computed by <see cref="ShardSplitAdmissionCore.ComputeRate"/>.
    /// </summary>
    public double Rate { get; init; }

    /// <summary>
    /// The shard's live-entry count, or <see cref="EntriesNotSampled"/> when
    /// occupancy has not been probed for this shard.
    /// </summary>
    public int Entries { get; init; }

    /// <summary>
    /// How many virtual slots the shard owns in the current shard map, capped
    /// at two by the caller (the clause only asks whether there is more than
    /// one slot to subdivide).
    /// </summary>
    public int OwnedSlots { get; init; }

    /// <summary>
    /// Whether the shard is already the source of an unfinished split.
    /// </summary>
    public bool IsSplitting { get; init; }

    /// <summary>
    /// Whether the shard is inside its post-split cooldown window.
    /// </summary>
    public bool InCooldown { get; init; }
}
