using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for enabling the generic atomic-action (saga / TCC)
/// coordinator on an Orleans silo.
/// </summary>
public static class LatticeAtomicActionServiceCollectionExtensions
{
    /// <summary>
    /// Adds the atomic-action coordinator to the silo and registers the custom
    /// handlers declared in <paramref name="configure"/>. Registers the handler
    /// catalog (which is also the allow-list: only handlers registered here can be
    /// invoked by a saga step). The coordinator's built-in tree-write step
    /// delegates to the tree's verified atomic-write machinery, so the host must
    /// register a lattice with
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice"/> as well.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional custom-handler declarations.</param>
    /// <returns>The silo builder, for chaining.</returns>
    /// <exception cref="System.ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    public static ISiloBuilder AddLatticeAtomicAction(
        this ISiloBuilder builder,
        Action<AtomicActionRegistrationBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var registrationBuilder = new AtomicActionRegistrationBuilder();
        configure?.Invoke(registrationBuilder);

        builder.Services.TryAddSingleton<IAtomicActionCatalog>(
            _ => new AtomicActionCatalog(registrationBuilder.Handlers));

        return builder;
    }
}
