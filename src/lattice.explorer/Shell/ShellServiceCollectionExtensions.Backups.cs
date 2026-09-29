using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Shell.Areas.Backups;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Backups area (#3826): the area itself, its probes, address
    /// completions, staged operations and download interop, all per circuit.
    /// </summary>
    /// <param name="services">The service collection.</param>
    static partial void AddBackups(IServiceCollection services)
    {
        services.TryAddScoped<BackupsAccess>();
        services.TryAddScoped<BackupsCompletionSource>();
        services.TryAddScoped<BackupAppTrees>();
        services.TryAddScoped<BackupOperations>();
        services.TryAddScoped<BackupActions>();
        services.TryAddScoped<BackupsInterop>();
        services.AddExplorerArea<BackupsArea>();
    }
}
