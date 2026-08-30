using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.DesignSystem;

/// <summary>
/// Registers the Explorer design system's services.
/// </summary>
public static class LatticeDesignSystemServiceCollectionExtensions
{
    /// <summary>
    /// Registers the viewport seam that
    /// <see cref="Components.LatticeAdaptiveRoot"/> drives and every adaptive
    /// primitive reads through its cascaded breakpoint.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The viewport is registered <em>scoped</em>, so each Blazor circuit gets
    /// its own: two browser windows served by the same process must never share
    /// a breakpoint. Registration is idempotent, so a head and a plugin may both
    /// call it.
    /// </para>
    /// <para>
    /// The design system also ships stylesheets, which are static web assets
    /// rather than services. Reference them from the host document:
    /// <c>_content/Orleans.Lattice.Explorer.DesignSystem/lattice-tokens.css</c>,
    /// <c>lattice-breakpoints.css</c>, and <c>lattice-primitives.css</c>.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection to add to.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    public static IServiceCollection AddLatticeExplorerDesignSystem(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<ILatticeViewport, LatticeViewport>();

        return services;
    }
}
