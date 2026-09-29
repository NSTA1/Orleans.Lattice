namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// How long a physical tree's write-ahead-log retention still matters, as
/// reported by <see cref="ITreeDeletionGrain.GetPhysicalRetentionAsync"/>. The
/// WAL GC reads it before it touches a tree's leaves to heal a stuck retention
/// floor: a deleted physical tree receives no reads or writes, so activating its
/// leaves to replay is work for a tree nobody can reach, and a discarded one can
/// never be recovered, so nothing its pins protect will ever be read again.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.PhysicalTreeRetention)]
internal enum PhysicalTreeRetention
{
    /// <summary>The physical tree is live; its pins protect data that will be read.</summary>
    Live = 0,

    /// <summary>
    /// The physical tree is soft-deleted but still recoverable, so its pins must
    /// keep holding its WAL until it is purged or recovered.
    /// </summary>
    Deleted = 1,

    /// <summary>
    /// The physical tree was discarded - an undone resize's destination - and can
    /// never be recovered, so no pin held against it protects anything.
    /// </summary>
    Discarded = 2,
}
