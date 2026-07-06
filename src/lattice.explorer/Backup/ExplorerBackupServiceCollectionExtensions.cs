using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// Registration helpers for the explorer's Backups area: the backup control
/// client, the catalog reader, and the capability service, plus the navigation
/// capability store they publish into.
/// </summary>
public static class ExplorerBackupServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Backups feature. Also calls
    /// <see cref="ExplorerNavigationServiceCollectionExtensions.AddExplorerNavigation"/>
    /// so the shell's capability store exists. Call after
    /// <c>AddExplorerConfiguration</c> and <c>AddExplorerAuth</c>, whose session
    /// and sign-in the backup client reads.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    public static IServiceCollection AddExplorerBackup(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerNavigation();
        // GrpcBackupControlClient owns its own Orleans serializer provider; it must
        // not be handed the application root provider (which has no AddSerializer),
        // or every backup gRPC call fails resolving its per-message serializers and
        // the Backups area silently greys out. Its single constructor keeps that
        // guarantee, so a plain type registration is safe here.
        services.TryAddSingleton<IBackupControlClient, GrpcBackupControlClient>();
        services.TryAddSingleton<IBackupCatalogReader, BackupCatalogReader>();
        services.TryAddSingleton<IBackupCapabilityService, BackupCapabilityService>();
        return services;
    }
}
