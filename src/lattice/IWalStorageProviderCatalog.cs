namespace Orleans.Lattice;

/// <summary>
/// A silo-wide, named directory of <see cref="IWalStorageProvider"/> backends.
/// Each tree's per-partition WAL placement (see
/// <see cref="Orleans.Lattice.BPlusTree.State.TreeRegistryEntry.WalPlacement"/>)
/// stores a stable, serializable provider <i>key</i> for every partition; the
/// catalog turns that key back into a live provider instance at WAL shard
/// activation time.
/// <para>
/// This is the safe, audit-able replacement for resolving WAL backends through
/// an opaque host delegate: keys are comparable strings recorded in durable
/// per-tree registry state and changed only through the managed
/// <see cref="ILatticeAdmin"/> move surface, so an accidental change to the
/// dependency-injection wiring cannot silently re-point a partition's log at a
/// different storage account.
/// </para>
/// <para>
/// Every silo in a cluster must register the same set of keys; a key that
/// resolves on one silo but not another causes WAL shards routed to that key to
/// fail closed (rather than silently re-route) on the silo missing it - see
/// <see cref="LatticeWalProviderMissingException"/>.
/// </para>
/// </summary>
public interface IWalStorageProviderCatalog
{
    /// <summary>
    /// The reserved key for the silo's baseline <see cref="IWalStorageProvider"/>
    /// registration - the provider every tree resolves to before any explicit
    /// per-partition placement override is applied. Honours the per-tree
    /// <see cref="LatticeOptions.WalStorageProvider"/> resolver when one is
    /// configured, exactly preserving pre-placement behaviour. This key is
    /// always present and cannot be registered through
    /// <see cref="LatticeServiceCollectionExtensions.AddLatticeWalStorageProvider"/>.
    /// </summary>
    const string DefaultProviderKey = "default";

    /// <summary>
    /// Resolves the provider registered under <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The provider key. <see cref="DefaultProviderKey"/> resolves the baseline provider.</param>
    /// <param name="provider">The resolved provider when the method returns <see langword="true"/>; otherwise <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if a provider is registered under <paramref name="key"/>; otherwise <see langword="false"/>.</returns>
    bool TryGet(string key, out IWalStorageProvider provider);

    /// <summary>
    /// The set of provider keys registered on this silo, always including
    /// <see cref="DefaultProviderKey"/>. Used by the administrative audit
    /// surface to validate that a tree's pinned placement references only keys
    /// the silo can resolve.
    /// </summary>
    IReadOnlyCollection<string> Keys { get; }
}
