namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Well-known constants used across Lattice internals.
/// </summary>
internal static class LatticeConstants
{
    /// <summary>
    /// Prefix for system-internal tree IDs. Trees whose ID starts with this
    /// prefix are excluded from registry self-registration to avoid circular
    /// bootstrap (the registry tree itself uses this prefix).
    /// </summary>
    public const string SystemTreePrefix = "_lattice_";

    /// <summary>
    /// Reserved tree-name prefix used by the <c>Orleans.Lattice.Replication</c>
    /// package for its internal write-ahead-log (WAL) trees, named
    /// <c>_lattice_replog_{treeId}/{shardIndex}</c>. User-supplied tree IDs
    /// matching this prefix are rejected at
    /// <see cref="ILatticeRegistry.RegisterAsync"/> with
    /// <see cref="ArgumentException"/>, guaranteeing the replication package
    /// a collision-free namespace.
    /// <para>
    /// Subsumed by <see cref="SystemTreePrefix"/> (any <c>_lattice_replog_</c>
    /// name also starts with <c>_lattice_</c>, so it inherits the same
    /// registry / monitor / routing bypasses as other system trees). Exposed
    /// as a named constant so downstream replication code can reference the
    /// prefix by name rather than hardcoding the string literal.
    /// </para>
    /// </summary>
    public const string ReplogTreePrefix = "_lattice_replog_";

    /// <summary>
    /// The tree ID of the internal registry tree that stores tree metadata
    /// (existence and per-tree <see cref="LatticeOptions"/> overrides).
    /// Each key is a user tree ID; each value is the serialized
    /// <see cref="TreeRegistryEntry"/>.
    /// </summary>
    public const string RegistryTreeId = "_lattice_trees";

    /// <summary>
    /// Canonical default maximum number of keys per leaf node before a split
    /// is triggered. Seeded into the registry entry on first tree creation
    /// and thereafter mutable only through
    /// <see cref="ILattice.ResizeAsync"/>.
    /// </summary>
    public const int DefaultMaxLeafKeys = 128;

    /// <summary>
    /// Canonical default maximum number of children per internal node
    /// before a split is triggered. Seeded into the registry entry on first
    /// tree creation and thereafter mutable only through
    /// <see cref="ILattice.ResizeAsync"/>.
    /// </summary>
    public const int DefaultMaxInternalChildren = 128;

    /// <summary>
    /// Canonical default number of independent physical shards a tree is
    /// divided into. Seeded into the registry entry on first tree creation
    /// and thereafter mutable only through
    /// <see cref="ILattice.ReshardAsync"/>.
    /// </summary>
    public const int DefaultShardCount = 64;

    /// <summary>
    /// Size of the virtual shard space used for key routing. Keys are hashed
    /// into one of <see cref="DefaultVirtualShardCount"/> virtual slots, and a
    /// per-tree <c>ShardMap</c> collapses those virtual slots onto the physical
    /// shards pinned in the tree registry. This indirection enables adaptive
    /// shard splitting without rehashing existing keys.
    /// <para>
    /// This value is a hard-coded constant because changing it would invalidate
    /// every persisted <c>ShardMap</c> (slots are referenced by integer index).
    /// The virtual shard space must be greater than or equal to the pinned
    /// physical shard count and an integer multiple of it for the default
    /// identity map to preserve <c>hash % shardCount</c> routing; both
    /// invariants are enforced by <c>ShardMap.CreateDefault</c> at use time.
    /// </para>
    /// </summary>
    public const int DefaultVirtualShardCount = 4096;
}
