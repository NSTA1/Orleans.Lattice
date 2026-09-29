using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI;

/// <summary>The type-ahead pickers' registrations (S3, issue #3949).</summary>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the circuit's shared suggestion sources. Scoped, so each circuit
    /// has its own and nothing one circuit remembers reaches another; every source
    /// is built on first use, so a head that serves none of the facades still
    /// validates.
    /// </summary>
    /// <param name="services">The service collection.</param>
    static partial void AddSuggestions(IServiceCollection services)
    {
        services.TryAddScoped<ExplorerSuggestions>();
    }
}
