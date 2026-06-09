namespace Orleans.Lattice;

/// <summary>
/// Dependency-injection marker recording that a named
/// <see cref="IWalStorageProvider"/> was registered through
/// <see cref="LatticeServiceCollectionExtensions.AddLatticeWalStorageProvider"/>.
/// One instance is registered per key; <see cref="WalStorageProviderCatalog"/>
/// injects the full set to learn which keys it can resolve. The provider
/// factory itself is registered as a keyed singleton under the same
/// <see cref="Key"/>.
/// </summary>
/// <param name="Key">The catalog key the provider was registered under.</param>
internal sealed record WalStorageProviderRegistration(string Key);
