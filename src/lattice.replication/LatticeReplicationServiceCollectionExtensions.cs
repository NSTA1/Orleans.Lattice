using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication.Adapters;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Extension methods for configuring <c>Orleans.Lattice.Replication</c> on an
/// Orleans silo.
/// </summary>
public static class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Adds <c>Orleans.Lattice.Replication</c> to the silo. Registers the
    /// no-op <see cref="IReplicationTransport"/> as the default and binds the
    /// supplied <paramref name="configure"/> delegate to the unnamed
    /// <see cref="LatticeReplicationOptions"/> instance. Replace the transport
    /// registration after this call (e.g. with an HTTP or gRPC implementation)
    /// to enable real cross-cluster shipping.
    /// </summary>
    public static ISiloBuilder AddLatticeReplication(
        this ISiloBuilder builder,
        Action<LatticeReplicationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        // ConfigureAll so the cluster-wide setup is the baseline for every
        // named (per-tree) options instance; per-tree overrides registered via
        // ConfigureLatticeReplication(treeName, ...) layer on top. The
        // commit-time observer resolves options via Get(treeId), so the
        // baseline must be visible to every named lookup - not only the
        // default instance.
        builder.Services.ConfigureAll(configure);
        builder.Services.TryAddSingleton<IReplicationTransport, NoOpReplicationTransport>();
        builder.Services.TryAddSingleton<IReplogSink, ShardedReplogSink>();
        builder.Services.TryAddSingleton<IChangeFeed, ChangeFeed>();

        builder.Services.TryAddSingleton<ISnapshotProvider, LatticeSnapshotProvider>();
        builder.Services.TryAddSingleton<ILatticeBootstrapCoordinator, LatticeBootstrapCoordinator>();
        builder.Services.TryAddSingleton<ILatticeWalIntrospection, LatticeWalIntrospection>();
        builder.Services.TryAddSingleton<ILatticeFallOffLogDetector, LatticeFallOffLogDetector>();
        builder.Services.TryAddSingleton<ILatticeReplicationAdmin, LatticeReplicationAdmin>();

        // Register the canonical applier as a concrete singleton so the
        // dead-letter tracking decorator and the inspection seam can
        // both share the exact same activation. The IReplicationApplier
        // public seam resolves to the decorator, which folds in the
        // retry-tracking + DLQ-routing behaviour around the canonical
        // implementation.
        builder.Services.TryAddSingleton<ReplicationApplier>();
        builder.Services.TryAddSingleton<IReplicationApplier>(sp =>
            new DeadLetterTrackingReplicationApplier(
                sp.GetRequiredService<ReplicationApplier>(),
                sp.GetRequiredService<IGrainFactory>(),
                sp.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()));
        builder.Services.TryAddSingleton<ILatticeReplicationDeadLetters>(sp =>
            new LatticeReplicationDeadLetters(
                sp.GetRequiredService<IGrainFactory>(),
                sp.GetRequiredService<ReplicationApplier>()));

        builder.Services.TryAddSingleton<IReplicationModeResolver, ReplicationModeResolver>();
        builder.Services.TryAddSingleton<IWalStorageProvider, InMemoryWalStorageProvider>();
        builder.Services.TryAddSingleton<IReplicationBatchEncoder, OrleansBinaryReplicationBatchEncoder>();
        builder.Services.TryAddSingleton<ILatticeReplicationCursorRegistry, InMemoryReplicationCursorRegistry>();
        builder.Services.TryAddSingleton<Orleans.Lattice.BPlusTree.Grains.ILeafCursorReporter, ReplicationLeafCursorReporter>();
        builder.Services.TryAddSingleton<ILatticeReplicationGc, LatticeReplicationGc>();
        builder.Services.TryAddSingleton<ReplicationPeerStats>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IMutationObserver, ReplicationMutationObserver>());
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeReplicationOptions>, LatticeReplicationOptionsValidator>());

        // Commit-log adapter seams (dormant). The core library resolves
        // these as nullable services; registering them here lets a future
        // foreground caller drive WAL append / read without taking a hard
        // reference on this package.
        builder.Services.TryAddSingleton<Orleans.Lattice.BPlusTree.Grains.ICommitLogWriter, ReplicationCommitLogWriter>();
        builder.Services.TryAddSingleton<Orleans.Lattice.BPlusTree.Grains.ICommitLogReader, ReplicationCommitLogReader>();
        builder.Services.TryAddSingleton<Orleans.Lattice.BPlusTree.Grains.ILeafSnapshotProvider, ReplicationLeafSnapshotProvider>();

        // Production replication drivers: host-startup
        // activation of one shipper per (tree, peer) and one
        // maintenance grain per tree. Registered via
        // TryAddEnumerable so a host that pre-registers its own
        // hosted service doesn't lose this one and vice versa.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, ReplicationDriverActivationService>());

        return builder;
    }

    /// <summary>
    /// Configures global <see cref="LatticeReplicationOptions"/> that apply to
    /// all replicated trees unless a per-tree override is registered.
    /// </summary>
    public static ISiloBuilder ConfigureLatticeReplication(
        this ISiloBuilder builder,
        Action<LatticeReplicationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.ConfigureAll(configure);
        return builder;
    }

    /// <summary>
    /// Configures <see cref="LatticeReplicationOptions"/> for a specific tree
    /// identified by <paramref name="treeName"/>. These settings override the
    /// global defaults for that tree only.
    /// </summary>
    public static ISiloBuilder ConfigureLatticeReplication(
        this ISiloBuilder builder,
        string treeName,
        Action<LatticeReplicationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(treeName, configure);
        return builder;
    }
}
