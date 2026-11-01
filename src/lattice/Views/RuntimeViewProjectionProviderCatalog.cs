namespace Orleans.Lattice.Views;

/// <summary>Immutable lookup of host-configured runtime-view projection providers.</summary>
internal sealed class RuntimeViewProjectionProviderCatalog
{
    private readonly IReadOnlyDictionary<string, RuntimeViewProjectionProviderRegistration> _providers;
    private readonly RuntimeViewProjectionProviderRegistration[] _hostProviders;

    public RuntimeViewProjectionProviderCatalog(
        IReadOnlyList<RuntimeViewProjectionProviderRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(registrations);
        _providers = registrations.ToDictionary(r => r.ProviderKey, StringComparer.Ordinal);

        // The built-in predicate provider persists filter views created through the
        // library; its payload is a private binary codec, never something an operator
        // supplies, so it is not offered for runtime creation.
        _hostProviders = registrations
            .Where(r => !string.Equals(r.ProviderKey, PredicateRuntimeViewProjectionCodec.ProviderKey, StringComparison.Ordinal))
            .OrderBy(r => r.ProviderKey, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>The host-registered providers an operator may create a view with, ordered by key.</summary>
    public IReadOnlyList<RuntimeViewProjectionProviderRegistration> HostProviders => _hostProviders;

    public RuntimeViewProjectionProviderRegistration? TryGet(string providerKey) =>
        _providers.TryGetValue(providerKey, out var registration) ? registration : null;
}
