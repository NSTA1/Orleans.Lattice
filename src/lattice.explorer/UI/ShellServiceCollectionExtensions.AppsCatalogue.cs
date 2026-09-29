using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI;

/// <summary>The Apps area's catalogue, consent and lifecycle registrations (A1, issue #3818).</summary>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Apps area - the single <c>apps</c> stop on the directory spine,
    /// its completions and palette commands - and the per-circuit state behind its
    /// catalogue, consent review and lifecycle pages. Every service is scoped, so each
    /// circuit probes its own caller's rights and keeps its own install flows.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddAppsCatalogue(IServiceCollection services)
    {
        services.TryAddScoped<AppsFacades>();
        services.TryAddScoped<AppsAccess>();
        services.TryAddScoped<AppsLifecycleIntents>();
        services.TryAddScoped<AppInstallFlowStore>();
        services.TryAddScoped<AppsCompletionSource>();
        services.AddExplorerArea<AppsArea>();
    }
}
