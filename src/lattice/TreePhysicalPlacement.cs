namespace Orleans.Lattice;

/// <summary>
/// The physical placement resolved for a tree at the moment it is first
/// registered: the <see cref="IWalStorageProviderCatalog"/> key its write-ahead-log
/// partitions should be pinned to, and an optional silo placement filter. Produced
/// by <see cref="ITreePlacementResolver"/> and consumed by the tree registry to
/// seed the tree's durable, thereafter-immutable WAL placement pin.
/// </summary>
/// <remarks>
/// This is a transient resolver result derived on demand; it never crosses a grain
/// boundary and so carries no Orleans serialization attributes, mirroring
/// <see cref="TreeOwnership"/>. The baseline value, <see cref="Default"/>, names the
/// catalog's default provider key and no placement filter, which reproduces
/// pre-placement behaviour exactly: a tree seeded with it keeps a <c>null</c> WAL
/// placement pin and every partition resolves to the silo's baseline provider.
/// </remarks>
public readonly record struct TreePhysicalPlacement
{
    /// <summary>
    /// The <see cref="IWalStorageProviderCatalog"/> key every WAL partition of the
    /// tree should resolve to. <see cref="Default"/> uses
    /// <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>, the silo's
    /// baseline provider.
    /// </summary>
    public string WalProviderKey { get; init; }

    /// <summary>
    /// The silo placement filter name the tree's grains should be bound to, or
    /// <see langword="null"/> for the cluster-wide default placement.
    /// <para>
    /// Advisory in v1: the registration path records the WAL provider binding only
    /// and does not yet act on the placement filter - honouring a per-tree silo
    /// placement director is a separate follow-up. The value is surfaced here so
    /// the seam stays stable for that later work rather than needing a signature
    /// change.
    /// </para>
    /// </summary>
    public string? PlacementFilter { get; init; }

    /// <summary>
    /// The baseline placement: the catalog's default WAL provider key and no
    /// placement filter. Byte-for-byte equivalent to the pre-placement default, so
    /// a tree seeded with this value keeps a <c>null</c> WAL placement pin and
    /// behaves exactly as it did before per-tenant placement existed.
    /// </summary>
    public static TreePhysicalPlacement Default =>
        new() { WalProviderKey = IWalStorageProviderCatalog.DefaultProviderKey };
}
