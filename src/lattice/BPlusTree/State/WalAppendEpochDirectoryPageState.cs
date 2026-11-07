namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One page of a WAL partition's append-epoch directory (#5634): the leaves with an
/// epoch entry at the partition, so the entries can be enumerated, which
/// <c>IGrainStorage</c> cannot do. A leaf is listed before its entry row is first
/// written and delisted after its entry is retired, so every entry is listed.
/// </summary>
/// <remarks>
/// Stored under a reserved grain type keyed by partition, hash bucket and page
/// (<c>WalAppendFenceRows.DirectoryPageKey</c>). A page holds at most
/// <c>WalAppendEpochDirectory.PageCapacity</c> leaves, which keeps it well within
/// the provider's row limits in binary and in JSON (Azure Table: 64 KiB per
/// property, 1 MiB per entity). Binary persisted; every member is a public property
/// of a concrete type so the JSON grain-storage serializer round trips it too.
/// <see cref="Leaves"/> and <see cref="AddedUtcTicks"/> are parallel lists.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalAppendEpochDirectoryPageState)]
internal sealed class WalAppendEpochDirectoryPageState : ILatticeBinaryPersistedState
{
    /// <summary>The listed leaves.</summary>
    [Id(0)]
    public List<Guid> Leaves { get; set; } = new();

    /// <summary>When each leaf was listed, in UTC ticks; parallel to <see cref="Leaves"/>.</summary>
    [Id(1)]
    public List<long> AddedUtcTicks { get; set; } = new();
}
