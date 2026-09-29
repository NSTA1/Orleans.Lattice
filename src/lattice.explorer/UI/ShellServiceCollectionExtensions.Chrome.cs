using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Completion;

namespace Orleans.Lattice.Explorer.UI;

/// <summary>The navigation chrome's registrations (S1, issue #3815).</summary>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the navigation chrome: the area directory, the address navigator
    /// and its tenancy reading, the completion fan-out, the chrome's JavaScript
    /// interop, and the appearance state. Per-circuit state is scoped.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddChrome(IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton(new ExplorerChromeOptions());

        services.TryAddScoped<ExplorerAreaDirectory>();
        services.TryAddScoped<ExplorerTenancy>();
        services.TryAddScoped<ExplorerNavigator>();
        services.TryAddScoped<AddressCompletionFanOut>();
        services.TryAddScoped<TenantCompletionSource>();
        services.TryAddScoped<ExplorerTenantSwitch>();

        services.TryAddScoped<ShellChromeInterop>();
        services.TryAddScoped<IShellAppearanceApplier, JsShellAppearanceApplier>();
        services.TryAddScoped<ShellAppearance>();
    }
}
