
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal grain that manages the tree registry - a Lattice tree
/// (<see cref="LatticeConstants.RegistryTreeId"/>) whose keys are user tree IDs
/// and whose values are serialized <see cref="State.TreeRegistryEntry"/> records.
/// <para>
/// Provides tree existence checks, per-tree configuration overrides, and
/// enumeration of all known tree IDs. The registry tree itself uses the
/// <see cref="LatticeConstants.SystemTreePrefix"/> and is excluded from
/// self-registration.
/// </para>
/// Key format: singleton - use <see cref="LatticeConstants.RegistryTreeId"/> as the grain key.
/// </summary>
[Alias(TypeAliases.ILatticeRegistry)]
internal interface ILatticeRegistry : IGrainWithStringKey
{
    /// <summary>
    /// Registers a tree in the registry. If the tree already exists, this is a no-op
    /// (existing config is preserved). Must be called before the first data write to
    /// the tree succeeds.
    /// </summary>
    /// <param name="treeId">The tree ID to register.</param>
    /// <param name="entry">
    /// Optional configuration overrides. Pass <c>null</c> to register with default options.
    /// </param>
    Task RegisterAsync(string treeId, State.TreeRegistryEntry? entry = null);

    /// <summary>
    /// Updates the registry entry for a tree, replacing any previous
    /// configuration overrides. If the tree is not yet registered, it is
    /// registered with the given entry (upsert semantics).
    /// </summary>
    Task UpdateAsync(string treeId, State.TreeRegistryEntry entry);

    /// <summary>
    /// Removes a tree from the registry. Idempotent - no-op if the tree is not registered.
    /// </summary>
    Task UnregisterAsync(string treeId);

    /// <summary>Returns <c>true</c> if the tree is registered.</summary>
    Task<bool> ExistsAsync(string treeId);

    /// <summary>
    /// Returns the <see cref="State.TreeRegistryEntry"/> for the given tree,
    /// or <c>null</c> if not registered.
    /// </summary>
    Task<State.TreeRegistryEntry?> GetEntryAsync(string treeId);

    /// <summary>
    /// Returns all registered tree IDs in sorted order.
    /// </summary>
    Task<IReadOnlyList<string>> GetAllTreeIdsAsync();

    /// <summary>
    /// Returns the registered tree IDs that begin with <paramref name="prefix"/>,
    /// in sorted order. A <c>null</c> or empty prefix returns every id and is
    /// exactly equivalent to <see cref="GetAllTreeIdsAsync()"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The registry is itself an ordinally-sorted Lattice tree, so a prefix
    /// occupies one contiguous key range. Supplying a prefix therefore turns the
    /// enumeration into a bounded range scan (the registry stops touching pages
    /// outside the range) and keeps the whole catalog off the wire, instead of
    /// transferring every id for the caller to discard most of client-side.
    /// </para>
    /// <para>
    /// The prefix is a <b>performance hint, never an authorization boundary</b>.
    /// It narrows which ids are read; it grants nothing. Every caller-facing
    /// visibility and authorization check stays exactly where it was, so a
    /// hand-crafted prefix can only ever return a subset of what the caller could
    /// already have enumerated.
    /// </para>
    /// </remarks>
    /// <param name="prefix">The tree-id prefix to scope the enumeration to, or <c>null</c> for all ids.</param>
    Task<IReadOnlyList<string>> GetAllTreeIdsAsync(string? prefix);

    /// <summary>
    /// Sets a tree alias so that the logical <paramref name="treeId"/> maps to
    /// <paramref name="physicalTreeId"/>. All subsequent reads and writes routed
    /// through <see cref="ILattice"/> will target the physical tree instead.
    /// <para>
    /// Only a single level of indirection is allowed - <paramref name="physicalTreeId"/>
    /// must not itself be aliased. Throws <see cref="InvalidOperationException"/> if
    /// this constraint would be violated.
    /// </para>
    /// </summary>
    Task SetAliasAsync(string treeId, string physicalTreeId);

    /// <summary>
    /// Removes the alias for <paramref name="treeId"/>, restoring it to use
    /// itself as the physical tree ID. No-op if no alias is set.
    /// </summary>
    Task RemoveAliasAsync(string treeId);

    /// <summary>
    /// Resolves the physical tree ID for the given logical <paramref name="treeId"/>.
    /// Returns <paramref name="treeId"/> itself if no alias is set.
    /// </summary>
    Task<string> ResolveAsync(string treeId);

    /// <summary>
    /// Returns the persisted <see cref="ShardMap"/> for <paramref name="treeId"/>,
    /// or <c>null</c> if the tree uses the default identity map. Callers should
    /// fall back to <see cref="ShardMap.CreateDefault"/> when this returns <c>null</c>.
    /// </summary>
    Task<ShardMap?> GetShardMapAsync(string treeId);

    /// <summary>
    /// Persists a custom <see cref="ShardMap"/> for <paramref name="treeId"/>.
    /// Used by adaptive shard splits to retarget virtual slots to new physical
    /// shards. Upserts the registry entry if the tree is not yet registered.
    /// </summary>
    Task SetShardMapAsync(string treeId, ShardMap map);

    /// <summary>
    /// Atomically allocates a fresh physical shard index for an adaptive split
    ///. Returns <c>max(currentMaxFromMap, persisted) + 1</c> and
    /// persists the new high-water mark so concurrent split coordinators each
    /// receive a unique target shard index. The registry grain's non-reentrant
    /// scheduling guarantees the read-modify-write is atomic across callers.
    /// </summary>
    /// <param name="treeId">The tree whose shard space is being expanded.</param>
    /// <param name="currentMaxFromMap">
    /// The maximum physical shard index in the caller's view of the current
    /// <see cref="ShardMap"/>. Used as the floor when no allocation has yet
    /// been recorded.
    /// </param>
    Task<int> AllocateNextShardIndexAsync(string treeId, int currentMaxFromMap);

    /// <summary>
    /// Sets or clears the per-tree <see cref="State.TreeRegistryEntry.PublishEvents"/>
    /// override for <paramref name="treeId"/>. Pass <c>true</c>/<c>false</c> to
    /// pin the setting for this tree, or <c>null</c> to remove the override and
    /// fall back to the silo-wide <see cref="LatticeOptions.PublishEvents"/>.
    /// Upserts the registry entry if the tree is not yet registered.
    /// </summary>
    Task SetPublishEventsAsync(string treeId, bool? enabled);

    /// <summary>
    /// Sets or clears the per-tree durable-history retention override for
    /// <paramref name="treeId"/> in one atomic upsert: the
    /// <see cref="State.TreeRegistryEntry.HistoryRetentionMode"/> applied to LWW
    /// value bytes and the age-bound
    /// <see cref="State.TreeRegistryEntry.HistoryRetentionWindowTicks"/>. A
    /// <see langword="null"/> argument clears that half of the override
    /// independently (mode falls back to
    /// <see cref="HistoryRetentionMode.MetadataOnly"/>; window falls back to no age
    /// bound). The <paramref name="window"/> must be strictly positive when
    /// supplied. Upserts the registry entry if the tree is not yet registered;
    /// propagation to other activations is best-effort.
    /// </summary>
    /// <param name="treeId">The tree whose history retention is being configured.</param>
    /// <param name="mode">The retention mode to pin, or <see langword="null"/> to clear it.</param>
    /// <param name="window">The age bound to pin, or <see langword="null"/> to clear it.</param>
    Task SetHistoryRetentionAsync(string treeId, HistoryRetentionMode? mode, TimeSpan? window);

    /// <summary>
    /// Sets or clears the per-tree
    /// <see cref="State.TreeRegistryEntry.MaintainProjectionDigest"/>
    /// override for <paramref name="treeId"/>. Pass <c>true</c>/<c>false</c>
    /// to pin the setting for this tree, or <c>null</c> to remove the
    /// override and fall back to the silo-wide
    /// <see cref="LatticeOptions.MaintainProjectionDigest"/>.
    /// Upserts the registry entry if the tree is not yet registered.
    /// <para>
    /// Note: the
    /// <see cref="State.TreeRegistryEntry.ProjectionDigestPermanentlyDisabled"/>
    /// latch supersedes this override. Once mutations have landed while
    /// digest maintenance was disabled, the latch forces the effective
    /// resolved value to <c>false</c> regardless of what this method
    /// pins, because the persisted aggregate has gaps that cannot be
    /// reconstructed without rewriting every entry.
    /// </para>
    /// </summary>
    Task SetMaintainProjectionDigestAsync(string treeId, bool? enabled);

    /// <summary>
    /// Sets or clears the per-tree runtime
    /// <see cref="State.TreeRegistryEntry.MaxCacheValueBytes"/> override for
    /// <paramref name="treeId"/>. Pass a positive byte count to pin the
    /// read-through-cache payload cap for this tree, or <c>null</c> to remove
    /// the override and fall back to the silo-wide
    /// <see cref="LatticeOptions.MaxCacheValueBytes"/>. The value, when
    /// supplied, must be greater than or equal to 1 (mirroring the silo-wide
    /// option's validation); a value below 1 throws
    /// <see cref="ArgumentOutOfRangeException"/>. Upserts the registry entry if
    /// the tree is not yet registered. Propagation to other activations is
    /// best-effort: each <see cref="Grains.LeafCacheGrain"/> re-resolves the cap
    /// on its next cache refresh.
    /// </summary>
    /// <param name="treeId">The tree whose cache-value cap override is being set.</param>
    /// <param name="maxCacheValueBytes">
    /// The per-tree payload-byte cap to pin (must be &gt;= 1), or <c>null</c> to
    /// clear the override.
    /// </param>
    Task SetMaxCacheValueBytesAsync(string treeId, long? maxCacheValueBytes);

    /// <summary>
    /// Stamps the
    /// <see cref="State.TreeRegistryEntry.ProjectionDigestPermanentlyDisabled"/>
    /// latch to <c>true</c> for <paramref name="treeId"/>. Idempotent
    /// once stamped; the latch is one-way and never cleared. Called by
    /// the leaf trimmed mutation path the first time a write lands
    /// while the resolved
    /// <see cref="LatticeOptions.MaintainProjectionDigest"/> is
    /// <c>false</c>. Upserts the registry entry if the tree is not yet
    /// registered.
    /// </summary>
    Task LatchProjectionDigestPermanentlyDisabledAsync(string treeId);

    /// <summary>
    /// Returns the durable WAL placement pin for <paramref name="treeId"/>.
    /// Never returns <see langword="null"/>: a tree with no persisted placement
    /// (a fresh registry row, or a row persisted before the placement slot was
    /// introduced) resolves to <see cref="State.WalPlacementPin.Create"/> - the
    /// default pin in which every partition uses
    /// <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>.
    /// </summary>
    Task<State.WalPlacementPin> GetWalPlacementAsync(string treeId);

    /// <summary>
    /// Atomically re-points a single WAL partition to a new provider key using
    /// compare-and-swap on the placement <see cref="State.WalPlacementPin.Version"/>.
    /// The registry grain is non-reentrant and singleton-keyed, so the
    /// read-validate-write sequence is atomic across concurrent callers.
    /// <para>
    /// Throws <see cref="InvalidOperationException"/> when the current pin's
    /// version does not equal <paramref name="expectedVersion"/> - the caller
    /// observed a stale placement and must re-read and retry. On success the
    /// returned pin carries the bumped version (<paramref name="expectedVersion"/>
    /// + 1). Routing a partition back to
    /// <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/> removes its
    /// override, so a reversal restores the exact prior shape via the same call.
    /// Upserts the registry entry if the tree is not yet registered.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree whose WAL placement is being changed.</param>
    /// <param name="expectedVersion">The placement version the caller last observed.</param>
    /// <param name="partition">The WAL partition to re-point.</param>
    /// <param name="providerKey">The catalog key the partition should resolve to.</param>
    Task<State.WalPlacementPin> UpdateWalPlacementAsync(string treeId, long expectedVersion, int partition, string providerKey);

    /// <summary>
    /// Atomically re-points several WAL partitions to new provider keys under a
    /// single compare-and-swap on the placement
    /// <see cref="State.WalPlacementPin.Version"/>. The batch analogue of
    /// <see cref="UpdateWalPlacementAsync(string, long, int, string)"/>: every
    /// reassignment in <paramref name="moves"/> is applied together and the
    /// version bumps exactly once (<paramref name="expectedVersion"/> + 1), so a
    /// multi-partition move flips atomically with no intermediate placement
    /// observable. The registry grain is non-reentrant and singleton-keyed, so
    /// the read-validate-write sequence is atomic across concurrent callers.
    /// <para>
    /// Throws <see cref="InvalidOperationException"/> when the current pin's
    /// version does not equal <paramref name="expectedVersion"/> - the caller
    /// observed a stale placement and must re-read and retry, leaving the whole
    /// batch un-applied. Throws <see cref="ArgumentException"/> when
    /// <paramref name="moves"/> is empty. Routing a partition back to
    /// <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/> removes its
    /// override. Upserts the registry entry if the tree is not yet registered.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree whose WAL placement is being changed.</param>
    /// <param name="expectedVersion">The placement version the caller last observed.</param>
    /// <param name="moves">The partition-to-key reassignments to apply together.</param>
    Task<State.WalPlacementPin> UpdateWalPlacementAsync(string treeId, long expectedVersion, IReadOnlyCollection<(int Partition, string ProviderKey)> moves);
}
