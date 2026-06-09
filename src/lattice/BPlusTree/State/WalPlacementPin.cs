using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Durable, per-tree record of which <see cref="IWalStorageProviderCatalog"/>
/// key backs each WAL partition. Stored in <see cref="TreeRegistryEntry"/> and
/// resolved to a live <see cref="IWalStorageProvider"/> at WAL shard activation
/// time, replacing host-supplied placement delegates with an audit-able,
/// version-stamped pin that changes only through the managed
/// <see cref="ILatticeAdmin"/> move surface.
/// <para>
/// A partition with no explicit entry in <see cref="Overrides"/> resolves to
/// <see cref="DefaultProviderKey"/>. The default pin (see <see cref="Create"/>)
/// uses <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/> with no
/// overrides, which preserves pre-placement behaviour exactly: every partition
/// resolves to the silo's baseline provider (honouring any per-tree
/// <see cref="LatticeOptions.WalStorageProvider"/> resolver).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalPlacementPin)]
internal sealed record WalPlacementPin
{
    /// <summary>
    /// Monotonic version bumped on every placement change. The WAL shard
    /// records the version it activated against; the move coordinator carries
    /// it through the quiesce lease so a shard whose pin changed underneath it
    /// fails closed rather than accepting appends against a stale placement.
    /// The default pin has version <c>0</c>.
    /// </summary>
    [Id(0)] public long Version { get; init; }

    /// <summary>
    /// The catalog key used for any partition without an explicit
    /// <see cref="Overrides"/> entry. Defaults to
    /// <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>.
    /// </summary>
    [Id(1)] public string DefaultProviderKey { get; init; } = IWalStorageProviderCatalog.DefaultProviderKey;

    /// <summary>
    /// Sparse per-partition overrides mapping a partition index to a catalog
    /// key. Partitions absent from this map resolve to
    /// <see cref="DefaultProviderKey"/>. <see langword="null"/> or empty means
    /// every partition uses the default key.
    /// </summary>
    [Id(2)] public Dictionary<int, string>? Overrides { get; init; }

    /// <summary>
    /// The default placement pin: every partition resolves to the catalog's
    /// baseline key, version <c>0</c>. Equivalent to the absence of a pin and
    /// used to seed registry rows and as the fallback for rows persisted before
    /// the placement slot was introduced.
    /// </summary>
    public static WalPlacementPin Create() => new();

    /// <summary>
    /// Resolves the catalog key backing <paramref name="partition"/>.
    /// </summary>
    /// <param name="partition">The WAL partition index.</param>
    /// <returns>The override key for the partition, or <see cref="DefaultProviderKey"/> when none is pinned.</returns>
    public string ResolveKey(int partition)
    {
        if (Overrides is { } overrides && overrides.TryGetValue(partition, out var key))
        {
            return key;
        }
        return DefaultProviderKey;
    }

    /// <summary>
    /// Produces a copy of this pin with <paramref name="partition"/> routed to
    /// <paramref name="providerKey"/> and <see cref="Version"/> set to
    /// <paramref name="newVersion"/>. Routing a partition back to
    /// <see cref="DefaultProviderKey"/> removes its override entry so the pin
    /// stays minimal (and a reversal restores the exact prior shape).
    /// </summary>
    /// <param name="partition">The partition to re-point.</param>
    /// <param name="providerKey">The catalog key the partition should resolve to.</param>
    /// <param name="newVersion">The version to stamp on the returned pin.</param>
    public WalPlacementPin WithPartition(int partition, string providerKey, long newVersion)
    {
        ArgumentNullException.ThrowIfNull(providerKey);
        var overrides = Overrides is null
            ? new Dictionary<int, string>()
            : new Dictionary<int, string>(Overrides);

        if (string.Equals(providerKey, DefaultProviderKey, StringComparison.Ordinal))
        {
            overrides.Remove(partition);
        }
        else
        {
            overrides[partition] = providerKey;
        }

        return this with
        {
            Version = newVersion,
            Overrides = overrides.Count == 0 ? null : overrides,
        };
    }
}
