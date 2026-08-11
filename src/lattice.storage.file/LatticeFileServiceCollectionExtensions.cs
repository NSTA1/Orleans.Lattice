using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Storage.File;

/// <summary>
/// DI extensions for registering the local disk-backed
/// <see cref="IWalStorageProvider"/> against an Orleans silo.
/// </summary>
public static class LatticeFileServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="FileWalStorageProvider"/> as the silo's
    /// <see cref="IWalStorageProvider"/>, layering on top of the core
    /// <see cref="LatticeServiceCollectionExtensions.AddWalStorage"/>
    /// seam. The host-supplied factory is registered via
    /// <c>Services.Replace</c> under the hood, so this call displaces the
    /// in-memory baseline that
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice"/>
    /// installs - regardless of whether <c>AddLattice</c> is invoked
    /// before or after <c>AddFileWalStorage</c>. Last
    /// <c>AddWalStorage</c>-with-factory call wins; calling
    /// <c>AddFileWalStorage</c> twice keeps the second configuration.
    /// <para>
    /// The same durable-WAL GC wiring the Azure Table provider installs
    /// (WAL cursor registry + leaf reporter + WAL GC) is registered here
    /// so opting into a durable local WAL never silently pairs with a
    /// process-local, restart-wiped cursor registry (issue #919). All
    /// three are idempotent (<c>TryAddSingleton</c>): a host that already
    /// supplied its own keeps it.
    /// </para>
    /// </summary>
    /// <param name="builder">The Orleans silo builder.</param>
    /// <param name="configure">Callback that populates
    /// <see cref="FileWalStorageOptions"/>. Invoked at options-resolution
    /// time; the populated root directory is read once at provider
    /// construction.</param>
    public static ISiloBuilder AddFileWalStorage(
        this ISiloBuilder builder,
        Action<FileWalStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(configure);
        builder.Services.AddOptions<FileWalStorageOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<FileWalStorageOptions>, FileWalStorageOptionsValidator>());
        builder.AddWalStorage(static sp => new FileWalStorageProvider(
            sp.GetRequiredService<IOptions<FileWalStorageOptions>>(),
            sp.GetRequiredService<Serializer<WalRecord>>()));

        builder.AddWalCursorRegistry();
        builder.AddLatticeWalGc();

        return builder;
    }
}
