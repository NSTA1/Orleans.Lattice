namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="ReplogShardGrain"/>. A single
/// append-only list of <see cref="ReplogShardEntry"/> records ordered
/// by their per-shard sequence number. Sequence numbers start at <c>0</c>
/// and are dense - there are no gaps in a successfully-persisted WAL.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplogShardState)]
internal sealed class ReplogShardState
{
    /// <summary>Entries in append order; index <c>i</c> always carries <see cref="ReplogShardEntry.Sequence"/> equal to <c>i</c>.</summary>
    [Id(0)] public List<ReplogShardEntry> Entries { get; set; } = new();
}
