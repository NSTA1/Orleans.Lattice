using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// Registration helpers for the explorer's Backups area: the backup control
/// client, the catalog reader, and the plugin access gate, plus the keyed
/// plugin access store the gate publishes into.
/// </summary>
public static class ExplorerBackupServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Backups feature. Also calls
    /// <see cref="ExplorerPluginServiceCollectionExtensions.AddExplorerPluginHost"/>
    /// so the keyed access store the gate publishes into exists. Call after
    /// <c>AddExplorerConfiguration</c> and <c>AddExplorerAuth</c>, whose session
    /// and sign-in the backup client reads.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    public static IServiceCollection AddExplorerBackup(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerPluginHost();
        // Scoped per Blazor circuit: the backup control client reads the calling
        // scope's session and sign-in, so it must not be shared across circuits.
        // GrpcBackupControlClient owns its own Orleans serializer provider; it must
        // not be handed the application root provider (which has no AddSerializer),
        // or every backup gRPC call fails resolving its per-message serializers and
        // the Backups area silently greys out. Its single constructor keeps that
        // guarantee, so a plain type registration is safe here.
        services.TryAddScoped<IBackupControlClient, GrpcBackupControlClient>();
        services.TryAddScoped<IBackupCatalogReader, BackupCatalogReader>();
        services.TryAddScoped<IBackupCapabilityService, BackupCapabilityService>();
        return services;
    }
}
