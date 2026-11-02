namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A shard root's hand-off marker (epic #5605, member 2; design note section 2):
/// the open shard-level hand-offs (an adaptive split's or a shard fold's moved slots,
/// a tree range delete, a bulk switch) with their key ranges, the recent closes with
/// their fence heads, the shard's routing generation, and each in-flight bulk
/// operation's birth hint. A leaf reads it fresh, through
/// <c>IShardRootGrain.GetHandOffMarkerAsync</c>, before any live advance.
/// </summary>
/// <remarks>
/// <para>
/// Stored in its own row under the reserved grain type <c>_lattice_shard-handoff</c>
/// (<c>HandOffMarkerRows</c>), never inside <see cref="ShardRootState"/>, so a build
/// that predates it rewrites the shard root's row without dropping anything here.
/// The shard root is the row's only writer.
/// </para>
/// <para>
/// <b>Absence never means closed.</b> A row that does not exist, or whose
/// <see cref="Initialised"/> is <see langword="false"/>, holds every leaf of the
/// shard on the replay-based advance: a hand-off begun by an older build or while
/// the gate was closed opened no marker. The shard root sets the flag only in a
/// serial turn in which it has no hand-off in flight.
/// </para>
/// <para>
/// Binary persisted; every member is a public property of a concrete type, so the
/// JSON grain-storage serializer round trips it too (#5579).
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.ShardHandOffState)]
internal sealed class ShardHandOffState : ILatticeBinaryPersistedState, Orleans.Lattice.BPlusTree.Grains.IHandOffMarker
{
    /// <summary>Set once the shard root has had no hand-off in flight in a turn that wrote this row.</summary>
    [Id(0)]
    public bool Initialised { get; set; }

    /// <summary>The open hand-offs, by token.</summary>
    [Id(1)]
    public Dictionary<string, HandOffMarkerRange> Open { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The most recent closes, oldest first; at most <c>HandOffMarkerCore.MaxRetainedCloses</c>.</summary>
    [Id(2)]
    public List<HandOffClose> Closes { get; set; } = [];

    /// <summary>
    /// The per-partition maximum of the heads of every close evicted from
    /// <see cref="Closes"/>; an owner of any range replays to them, so eviction only
    /// ever over-holds.
    /// </summary>
    [Id(3)]
    public long[] CompactedHeads { get; set; } = [];

    /// <summary>The shard's routing generation: the one it currently stamps admissions with; 0 before its first claim.</summary>
    [Id(4)]
    public long RoutingGeneration { get; set; }

    /// <summary>Each in-flight bulk operation's birth hint (per-partition WAL heads), by operation id; member 3 writes it.</summary>
    [Id(5)]
    public Dictionary<string, long[]> BulkBirthHints { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Raised on every open and close, so a reader can tell that the marker changed.</summary>
    [Id(6)]
    public long Sequence { get; set; }

    /// <summary>
    /// The re-seed that last caught this marker up (#5716): the storage ETag of the
    /// purge tombstone the shard root lifted. Every activation lifting the same
    /// tombstone shares it, and a later purge writes a new one, so a duplicate
    /// activation's catch-up recognises a marker its sibling already caught up and
    /// seeded, and leaves it alone. <see langword="null"/> on a marker no re-seed
    /// has caught up.
    /// </summary>
    [Id(7)]
    public string? ReseedOf { get; set; }
}
