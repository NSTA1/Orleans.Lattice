using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using NSubstitute;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Transport;

/// <summary>
/// The services a Blazor head provides per circuit that the Shell's chrome reads - the
/// navigation manager and the JavaScript runtime - so a transport test container that
/// registers the whole Shell validates on build without a renderer.
/// </summary>
internal static class ShellTransportHeadServices
{
    /// <summary>Registers a scoped <see cref="TestNavigationManager"/> and <see cref="IJSRuntime"/> substitute.</summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The same collection, for chaining.</returns>
    public static IServiceCollection AddShellTransportTestHead(this IServiceCollection services)
    {
        services.AddScoped<NavigationManager>(_ => new TestNavigationManager());
        services.AddScoped(_ => Substitute.For<IJSRuntime>());
        return services;
    }
}
