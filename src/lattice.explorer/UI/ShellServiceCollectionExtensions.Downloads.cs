using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Downloads;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the download seam (issue #5366): the process-wide ticket store the
    /// download endpoint redeems against, and each circuit's own
    /// <see cref="ExplorerDownloads"/>. The store holds tickets, which carry values
    /// only; nothing scoped is ever captured by it.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddDownloads(IServiceCollection services)
    {
        services.TryAddSingleton<ExplorerDownloadTicketStore>();
        services.TryAddScoped(provider => new ExplorerDownloads(
            provider.GetRequiredService<ExplorerDownloadTicketStore>(),
            provider.GetService<IExplorerSession>(),
            provider.GetService<IExplorerAuthSession>(),
            provider.GetService<ShellAssertedTenant>()));
    }
}
