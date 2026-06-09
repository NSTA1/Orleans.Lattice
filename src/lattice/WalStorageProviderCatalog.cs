using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="IWalStorageProviderCatalog"/> implementation. Resolves
/// the <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/> to the
/// silo's baseline <see cref="IWalStorageProvider"/> registration and every
/// other key to the keyed singleton registered through
/// <see cref="LatticeServiceCollectionExtensions.AddLatticeWalStorageProvider"/>.
/// <para>
/// Resolution is lazy: a provider factory supplied at registration time is not
/// invoked until the first <see cref="TryGet"/> for its key, so registering a
/// catalog entry for a backend a given silo never routes to costs nothing.
/// </para>
/// </summary>
internal sealed class WalStorageProviderCatalog : IWalStorageProviderCatalog
{
    private readonly IServiceProvider _services;
    private readonly string[] _keys;

    /// <summary>
    /// Initialises the catalog from the silo's named provider registrations.
    /// </summary>
    /// <param name="services">The root service provider used to resolve the baseline and keyed providers.</param>
    /// <param name="registrations">One marker per key registered through <see cref="LatticeServiceCollectionExtensions.AddLatticeWalStorageProvider"/>.</param>
    public WalStorageProviderCatalog(
        IServiceProvider services,
        IEnumerable<WalStorageProviderRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(registrations);
        _services = services;
        var keys = new HashSet<string>(StringComparer.Ordinal) { IWalStorageProviderCatalog.DefaultProviderKey };
        foreach (var registration in registrations)
        {
            keys.Add(registration.Key);
        }
        _keys = [.. keys];
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Keys => _keys;

    /// <inheritdoc />
    public bool TryGet(string key, out IWalStorageProvider provider)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (string.Equals(key, IWalStorageProviderCatalog.DefaultProviderKey, StringComparison.Ordinal))
        {
            // The baseline registration is always present whenever the WAL
            // pipeline is wired (AddLattice self-registers the in-memory
            // baseline), so a required-service resolution is correct here.
            provider = _services.GetRequiredService<IWalStorageProvider>();
            return true;
        }

        var keyed = _services.GetKeyedService<IWalStorageProvider>(key);
        provider = keyed!;
        return keyed is not null;
    }
}
