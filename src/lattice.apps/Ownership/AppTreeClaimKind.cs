namespace Orleans.Lattice.Apps;

/// <summary>How an install came to own a tree recorded in the tree ownership ledger.</summary>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppTreeClaimKind)]
internal enum AppTreeClaimKind
{
    /// <summary>No claim kind; never written by the ledger.</summary>
    None = 0,

    /// <summary>
    /// The app's own structural <c>a/{slug}/{tree}</c> tree. Held through uninstall and the whole
    /// soft-delete window; free only once the tree is purged and its owner is no longer installed.
    /// </summary>
    Structural = 1,

    /// <summary>
    /// A pre-app tree the manifest adopts. Released on uninstall, or on an upgrade that stops
    /// adopting it, after which the tree reverts to an ordinary unowned tree.
    /// </summary>
    Adopted = 2,
}
