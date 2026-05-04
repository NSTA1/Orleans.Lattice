using Microsoft.Extensions.DependencyInjection;
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
    /// Registers <see cref="AzureTableWalStorageProvider"/> as the
    /// silo's <see cref="IWalStorageProvider"/>, layering on top of the
    /// core <see cref="LatticeServiceCollectionExtensions.AddWalStorage"/>
    /// seam. Idempotent: the underlying registration uses
    /// <c>TryAddSingleton</c>, so a previously-registered provider is
    /// preserved.
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
        builder.AddWalStorage(static sp => new AzureTableWalStorageProvider(
            sp.GetRequiredService<IOptions<AzureTableWalStorageOptions>>(),
            sp.GetRequiredService<Serializer<LatticeMutation>>()));

        return builder;
    }
}
