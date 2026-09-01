using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// Registration helpers for the explorer's Backups plugin: the backup control
/// client, the catalogue reader, the controlled domain model, and the plugin
/// access gate, plus the keyed plugin access store the gate publishes into.
/// </summary>
public static class ExplorerBackupServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Backups feature. Also calls
    /// <see cref="ExplorerPluginServiceCollectionExtensions.AddExplorerPluginHost"/>
    /// so the keyed access store the gate publishes into exists, and
    /// <see cref="ExplorerSessionServiceCollectionExtensions.AddExplorerSession"/>
    /// so the shell-state contract the panel remembers its open surface on
    /// exists. Call after <c>AddExplorerConfiguration</c> and
    /// <c>AddExplorerAuth</c>, whose session and sign-in the backup client reads.
    /// </summary>
    /// <remarks>
    /// The panel declares <see cref="BackupsPluginKeys.SurfacePreference"/> on
    /// the resolved catalog when it mounts rather than here, which is how the
    /// shell's own rail declares its key too: the catalog is a singleton and
    /// registration is idempotent by reference, so a key arrives exactly once
    /// however many circuits mount the area, and a head cannot compose the
    /// plugin into a container whose catalog was built before the key existed.
    /// </remarks>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    public static IServiceCollection AddExplorerBackup(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddExplorerPluginHost();

        // Idempotent (every registration inside it is a TryAdd), so a head that
        // already composed the session stack is not disturbed - and a head that
        // did not cannot end up with a panel whose declared preference key and
        // addressable surface have nowhere to live.
        services.AddExplorerSession();

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

        // The controlled domain model the plugin declares. Registered here rather
        // than by the head, so the one contract the host may resolve for Backups
        // ships with the package that defines it.
        services.TryAddScoped<IBackupsDomain, BackupsDomain>();
        return services;
    }

    /// <summary>
    /// Registers the Backups area plugin, so the shell enumerates it from the
    /// container and renders its panel. Call <see cref="AddExplorerBackup"/> as
    /// well: that registers the control client, the domain model, and the access
    /// gate this plugin resolves. A head that calls neither ships no Backups
    /// area at all, which is the whole of the opt-out.
    /// <para>
    /// The head is also responsible for registering the two host-side plugin
    /// adapters (<c>AddExplorerPluginAdapters</c>), which live on the shell's
    /// side of the seam and are shared by every plugin.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerBackupsPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.AddExplorerPlugin<BackupsAreaPlugin>();
    }
}

