namespace Orleans.Lattice.Views;

/// <summary>
/// Durable persisted state of the <see cref="IViewRegistryGrain"/>: the set of
/// runtime-created materialised views, keyed by view name, that must be
/// re-registered and re-activated after a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewRegistryState)]
internal sealed class ViewRegistryState
{
    /// <summary>
    /// The durable runtime-view registrations, keyed by
    /// <see cref="RuntimeViewRegistration.ViewName"/>.
    /// </summary>
    [Id(0)]
    public Dictionary<string, RuntimeViewRegistration> Registrations { get; set; } =
        new(StringComparer.Ordinal);
}
