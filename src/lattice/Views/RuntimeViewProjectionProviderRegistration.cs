namespace Orleans.Lattice.Views;

/// <summary>A keyed startup registration for reconstructing one runtime-view definition.</summary>
internal sealed record RuntimeViewProjectionProviderRegistration(
    string ProviderKey,
    Func<IServiceProvider, LatticeRuntimeViewProjectionContext, LatticeViewDefinition> Factory);
