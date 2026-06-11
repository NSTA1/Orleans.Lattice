using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// DI extensions for registering the Azure Table Storage
/// <see cref="IWalStorageProvider"/> against an Orleans silo.
/// </summary>
public static class LatticeAzureTableServiceCollectionExtensions
{
    /// <summary>
    /// The Zstandard compression level the Azure Table WAL provider's
    /// default <see cref="ZstdLatticeCompressor"/> fallback is
    /// constructed with (<c>3</c>, the canonical "fast" preset that
    /// matches the replication framing-tail default). Hosts that want a
    /// different WAL compression level pre-register their own
    /// <see cref="ZstdLatticeCompressor"/> instance via
    /// <see cref="LatticeCompressionServiceCollectionExtensions.AddLatticeCompressor(IServiceCollection, ILatticeCompressor)"/>
    /// before calling <see cref="AddAzureTableWalStorage"/>;
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAddEnumerable(IServiceCollection, ServiceDescriptor)"/>
    /// preserves the pre-registered instance.
    /// </summary>
    public const int DefaultCompressionLevel = 3;

    /// <summary>
    /// Registers <see cref="AzureTableWalStorageProvider"/> as the
    /// silo's <see cref="IWalStorageProvider"/>, layering on top of the
    /// core <see cref="LatticeServiceCollectionExtensions.AddWalStorage"/>
    /// seam. The host-supplied factory is registered via
    /// <c>Services.Replace</c> under the hood, so this call displaces
    /// the in-memory baseline that
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// installs - regardless of whether <c>AddLattice</c> is invoked
    /// before or after <c>AddAzureTableWalStorage</c>. Last
    /// <c>AddWalStorage</c>-with-factory call wins; calling
    /// <c>AddAzureTableWalStorage</c> twice keeps the second
    /// configuration.
    /// <para>
    /// A <see cref="ZstdLatticeCompressor"/> is registered as an
    /// <see cref="ILatticeCompressor"/> via
    /// <see cref="ServiceCollectionDescriptorExtensions.TryAddEnumerable(IServiceCollection, ServiceDescriptor)"/>
    /// so opting in to per-row payload compression
    /// (<see cref="AzureTableWalStorageOptions.Compression"/> =
    /// <see cref="LatticeCompression.Zstd"/>) requires no extra wiring.
    /// <c>TryAddEnumerable</c> deduplicates by
    /// <c>(ServiceType, ImplementationType)</c>, so this fallback
    /// co-exists order-insensitively with the equivalent registrations
    /// in the replication and gRPC extensions and produces exactly one
    /// registered instance. The first fallback factory to run wins the
    /// compression level; a host that needs a WAL level different from
    /// the replication framing default pre-registers its own
    /// <see cref="ZstdLatticeCompressor"/> instance before calling any
    /// integration extension. See <c>docs/lattice/compression.md</c>.
    /// </para>
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="configure">Callback that populates
    /// <see cref="AzureTableWalStorageOptions"/>. Invoked at options
    /// resolution time; the populated authentication mode is read once
    /// at first use of the provider.</param>
    public static ISiloBuilder AddAzureTableWalStorage(
        this ISiloBuilder builder,
        Action<AzureTableWalStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(configure);
        builder.Services.AddOptions<AzureTableWalStorageOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeCompressor, ZstdLatticeCompressor>(
                _ => new ZstdLatticeCompressor(DefaultCompressionLevel)));
        builder.AddWalStorage(static sp => new AzureTableWalStorageProvider(
            sp.GetRequiredService<IOptions<AzureTableWalStorageOptions>>(),
            sp.GetRequiredService<Serializer<WalRecord>>(),
            sp.GetService<IWalSaturationSignal>(),
            sp.GetServices<ILatticeCompressor>()));

        return builder;
    }
}
