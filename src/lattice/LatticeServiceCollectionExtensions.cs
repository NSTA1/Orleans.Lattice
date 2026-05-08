using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for configuring Lattice on an Orleans silo.
/// </summary>
public static class LatticeServiceCollectionExtensions
{
    /// <summary>
    /// Adds Lattice to the silo and registers the grain storage provider
    /// that Lattice grains require. The <paramref name="configureStorage"/>
    /// delegate receives the <see cref="ISiloBuilder"/> and the provider
    /// name that must be used when registering storage.
    /// <para>
    /// Also registers the core write-ahead-log adapters
    /// (<see cref="ICommitLogReader"/>, <see cref="ICommitLogWriter"/>)
    /// and the in-memory <see cref="IWalStorageProvider"/> default so a
    /// single-cluster host gets durable commit-log infrastructure with
    /// no extra wiring. Hosts that need a different WAL backing store
    /// call <see cref="AddWalStorage"/> with a custom factory before
    /// (or after) <c>AddLattice</c>; the registration is idempotent.
    /// </para>
    /// <para>Example:</para>
    /// <code>
    /// silo.AddLattice((silo, name) =&gt; silo.AddMemoryGrainStorage(name));
    /// </code>
    /// </summary>
    public static ISiloBuilder AddLattice(
        this ISiloBuilder builder,
        Action<ISiloBuilder, string> configureStorage)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureStorage);

        configureStorage(builder, LatticeOptions.StorageProviderName);
        builder.Services.AddSingleton<IValidateOptions<LatticeOptions>, LatticeOptionsValidator>();
        builder.Services.AddSingleton<LatticeOptionsResolver>();
        builder.Services.AddSingleton<MutationObserverDispatcher>();
        builder.Services.AddSingleton<ILatticeFallOffLogDetector, LatticeFallOffLogDetector>();

        // Core WAL durability seams: in-memory provider as the singleton
        // default (hosts replace via AddWalStorage), commit-log writer
        // and reader, and a null-returning mode resolver. The replication
        // package replaces the resolver via services.Replace(...) so per-
        // tree mode resolution kicks in only when replication is added.
        builder.AddWalStorage();
        builder.Services.TryAddSingleton<ICommitLogWriter, WalCommitLogWriter>();
        builder.Services.TryAddSingleton<ICommitLogReader, WalCommitLogReader>();
        builder.Services.TryAddSingleton<ILatticeMergeModeResolver, DefaultLatticeMergeModeResolver>();
        return builder;
    }

    /// <summary>
    /// Configures global <see cref="LatticeOptions"/> that apply to all trees
    /// unless a per-tree override is registered.
    /// </summary>
    public static ISiloBuilder ConfigureLattice(
        this ISiloBuilder builder,
        Action<LatticeOptions> configure)
    {
        builder.Services.ConfigureAll(configure);
        return builder;
    }

    /// <summary>
    /// Configures <see cref="LatticeOptions"/> for a specific tree identified
    /// by <paramref name="treeName"/>. These settings override the global defaults
    /// for that tree only.
    /// </summary>
    public static ISiloBuilder ConfigureLattice(
        this ISiloBuilder builder,
        string treeName,
        Action<LatticeOptions> configure)
    {
        builder.Services.Configure(treeName, configure);
        return builder;
    }

    /// <summary>
    /// Registers an <see cref="IWalStorageProvider"/> on the silo. When
    /// <paramref name="factory"/> is supplied it is invoked once at
    /// resolution time; otherwise the in-memory default
    /// (<see cref="InMemoryWalStorageProvider"/>) is registered. This is
    /// the core-side seam consumed by single-cluster deployments under
    /// the WAL-as-sole-commit-point flip; the replication package builds
    /// on top of this registration via
    /// <see cref="LatticeOptions.WalStorageProvider"/> for per-tree
    /// configurability. Idempotent: a previously-registered provider
    /// (whether from a host-supplied factory or from a downstream
    /// <c>AddLattice*</c> call) is preserved.
    /// </summary>
    public static ISiloBuilder AddWalStorage(
        this ISiloBuilder builder,
        Func<IServiceProvider, IWalStorageProvider>? factory = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (factory is null)
        {
            builder.Services.TryAddSingleton<IWalStorageProvider, InMemoryWalStorageProvider>();
        }
        else
        {
            builder.Services.TryAddSingleton<IWalStorageProvider>(factory);
        }
        return builder;
    }
}
