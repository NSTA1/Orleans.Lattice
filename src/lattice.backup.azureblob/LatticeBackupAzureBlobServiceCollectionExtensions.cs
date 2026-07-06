using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.AzureBlob;

/// <summary>
/// DI extensions for registering the Azure Blob Storage
/// <see cref="ILatticeBackupSink"/> against an Orleans silo.
/// </summary>
public static class LatticeBackupAzureBlobServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="AzureBlobLatticeBackupSink"/> as the silo's
    /// <see cref="ILatticeBackupSink"/>, replacing the in-cluster default that
    /// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup"/>
    /// installs. Because the sink is resolved purely through the
    /// <see cref="ILatticeBackupSink"/> seam, the core capture engine stays
    /// unaware of Azure specifics. The registration is idempotent: calling this
    /// more than once keeps the last configuration, and it may be called before
    /// or after <c>AddLatticeBackup</c> since it replaces the sink registration
    /// outright.
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="configure">
    /// Callback that populates <see cref="LatticeBackupAzureBlobOptions"/>. Invoked
    /// at options-resolution time; the container client is built once from the
    /// populated authentication mode when the sink is first resolved.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static ISiloBuilder AddLatticeBackupAzureBlob(
        this ISiloBuilder builder,
        Action<LatticeBackupAzureBlobOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(configure);
        builder.Services.AddOptions<LatticeBackupAzureBlobOptions>();

        // Replace, not TryAdd: this call is meant to displace the in-cluster
        // default sink whether AddLatticeBackup ran before or after it.
        builder.Services.Replace(ServiceDescriptor.Singleton<ILatticeBackupSink>(static sp =>
        {
            var options = sp.GetRequiredService<IOptions<LatticeBackupAzureBlobOptions>>().Value;
            return new AzureBlobLatticeBackupSink(
                options.BuildContainerClient(),
                sp.GetRequiredService<Serializer<BackupManifest>>());
        }));

        return builder;
    }
}
