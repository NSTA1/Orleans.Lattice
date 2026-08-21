namespace Orleans.Lattice.Views;

/// <summary>Immutable lookup of host-configured runtime-view projection providers.</summary>
internal sealed class RuntimeViewProjectionProviderCatalog
{
    private readonly IReadOnlyDictionary<string, RuntimeViewProjectionProviderRegistration> _providers;

    public RuntimeViewProjectionProviderCatalog(
        IReadOnlyList<RuntimeViewProjectionProviderRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);
        _providers = registrations.ToDictionary(r => r.ProviderKey, StringComparer.Ordinal);
    }

    public RuntimeViewProjectionProviderRegistration? TryGet(string providerKey) =>
        _providers.TryGetValue(providerKey, out var registration) ? registration : null;
}
