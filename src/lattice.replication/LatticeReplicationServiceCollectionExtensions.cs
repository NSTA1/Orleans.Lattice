using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Adapters;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Extension methods for configuring <c>Orleans.Lattice.Replication</c> on an
/// Orleans silo.
/// </summary>
public static partial class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Adds <c>Orleans.Lattice.Replication</c> to the silo. Registers the
    /// no-op <see cref="IReplicationTransport"/> as the default and binds the
    /// supplied <paramref name="configure"/> delegate to the unnamed
    /// <see cref="LatticeReplicationOptions"/> instance. Replace the transport
    /// registration after this call (e.g. with an HTTP or gRPC implementation)
    /// to enable real cross-cluster shipping.
    /// <para>
    /// Must be called <i>after</i>
    /// <see cref="LatticeServiceCollectionExtensions.AddLattice"/>: the core
    /// registration is the source of truth for the WAL grain, the in-memory
    /// WAL storage provider, the foreground commit-log adapters, and the
    /// default null-returning <see cref="ILatticeMergeModeResolver"/>. This
    /// call replaces the resolver with a per-tree
    /// <see cref="ConfiguredLatticeMergeModeResolver"/>, registers the
    /// replication-only sinks/transports/applier, and mirrors the WAL-related
    /// fields of <see cref="LatticeReplicationOptions"/> onto
    /// <see cref="LatticeOptions"/> so the core WAL grain observes the same
    /// per-tree values when both options instances are configured.
    /// </para>
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

        // Mirror WAL-related fields from LatticeReplicationOptions onto
        // LatticeOptions so the core WAL grain (which reads LatticeOptions)
        // observes the same per-tree values when the host configures the
        // replication options. Registered as IPostConfigureOptions so the
        // mirror runs after every Configure(...) action on LatticeOptions,
        // including the host's own per-tree overrides.
        builder.Services.AddSingleton<IPostConfigureOptions<LatticeOptions>, MirrorReplicationOptionsToLatticeOptions>();

        builder.Services.TryAddSingleton<IReplicationTransport, NoOpReplicationTransport>();
        builder.Services.TryAddSingleton<IReplogSink, ShardedReplogSink>();
        builder.Services.TryAddSingleton<IChangeFeed, ChangeFeed>();

        builder.Services.TryAddSingleton<ISnapshotProvider, LatticeSnapshotProvider>();
        // Receiver-side bootstrap source. The bootstrap state machine
        // drains from this seam; the seam is split from
        // ISnapshotProvider so a single silo can be both a sender
        // (its ISnapshotProvider streams the local tree out to peer
        // receivers via LatticeRemoteSnapshotService) and a receiver
        // (its IBootstrapSnapshotSource drains from an upstream peer
        // via the cross-cluster RemoteSnapshotProvider) at the same
        // time. The factory below makes active-active the zero-
        // ceremony default: when any IRemoteSnapshotTransport is
        // registered alongside AddLatticeReplication (the gRPC
        // binding, an in-process loopback, a custom HTTP binding,
        // etc.), the bootstrap seam resolves to the cross-cluster
        // adapter; when no transport is registered (the single-
        // cluster recovery path) it resolves to a local wrapper that
        // forwards to the silo's ISnapshotProvider. Hosts that want
        // to force the local-only path even with a transport present
        // can pre-register their own IBootstrapSnapshotSource before
        // AddLatticeReplication and the TryAdd below is a no-op.
        builder.Services.TryAddSingleton<IBootstrapSnapshotSource>(sp =>
            sp.GetService<IRemoteSnapshotTransport>() is null
                ? new LocalBootstrapSnapshotSource(sp.GetRequiredService<ISnapshotProvider>())
                : ActivatorUtilities.CreateInstance<RemoteSnapshotProvider>(sp));
        // Sender-side handler for IRemoteSnapshotTransport. Registered as
        // a singleton so the concrete cross-cluster bindings (gRPC,
        // in-process loopback, custom HTTP) can resolve it directly and
        // delegate their inbound metadata/stream RPCs to the local
        // ISnapshotProvider without duplicating the contract-level
        // argument validation or the cut-point semantics.
        builder.Services.TryAddSingleton<LatticeRemoteSnapshotService>();
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

        // The core AddLattice registers DefaultLatticeMergeModeResolver (returns
        // null for every tree). Swap that out for the per-tree resolver so
        // configured trees get their declared LatticeMergeMode at commit time,
        // while preserving any user-supplied custom resolver registered before
        // this call. The protocol is: remove the Default registration if (and
        // only if) it is the active one, then TryAdd Configured. A user who
        // registered their own ILatticeMergeModeResolver before AddLatticeReplication
        // is left untouched and the TryAdd is a no-op.
        for (var i = builder.Services.Count - 1; i >= 0; i--)
        {
            var d = builder.Services[i];
            if (d.ServiceType == typeof(ILatticeMergeModeResolver)
                && d.ImplementationType == typeof(DefaultLatticeMergeModeResolver))
            {
                builder.Services.RemoveAt(i);
            }
        }
        builder.Services.TryAddSingleton<ILatticeMergeModeResolver, ConfiguredLatticeMergeModeResolver>();

        // Same swap protocol for the per-tree origin-cluster-id resolver:
        // remove the core's DefaultLatticeOriginClusterIdResolver (returns
        // string.Empty) when present, then TryAdd the configured resolver
        // that reads LatticeReplicationOptions.ClusterId. A user-supplied
        // resolver registered before this call is left untouched (TryAdd
        // is a no-op).
        for (var i = builder.Services.Count - 1; i >= 0; i--)
        {
            var d = builder.Services[i];
            if (d.ServiceType == typeof(ILatticeOriginClusterIdResolver)
                && d.ImplementationType == typeof(DefaultLatticeOriginClusterIdResolver))
            {
                builder.Services.RemoveAt(i);
            }
        }
        builder.Services.TryAddSingleton<ILatticeOriginClusterIdResolver, ConfiguredLatticeOriginClusterIdResolver>();

        builder.Services.TryAddSingleton<IReplicationBatchEncoder, OrleansBinaryReplicationBatchEncoder>();
        // The cursor registry, leaf-cursor reporter, and WAL GC are core
        // seams (formerly in this package, promoted in v3.5.0). Calling
        // AddWalCursorRegistry registers the in-memory default plus the
        // leaf-as-materialiser reporter; AddLatticeWalGc registers the
        // GC. Both extension calls are idempotent - a host that already
        // wired them up directly is unaffected.
        builder.AddWalCursorRegistry();
        builder.AddLatticeWalGc();
        builder.Services.TryAddSingleton<ReplicationPeerStats>();
        // Default runtime-observable peer topology: projects
        // LatticeReplicationOptions.ReplicationPeers via
        // IOptionsMonitor.OnChange so hosts can add or remove peers at
        // runtime without restarting the silo. Hosts that source their
        // topology from a service registry or other dynamic surface
        // can replace the registration by pre-registering their own
        // IReplicationTopology singleton before AddLatticeReplication.
        builder.Services.TryAddSingleton<IReplicationTopology, OptionsReplicationTopology>();
        // Producer-side per-(silo, tree) local vector clock cache.
        // Read by ReplicationMutationObserver to stamp every emit's
        // VectorClock when the caller does not supply one via
        // LatticeVectorClockContext; advanced post-WAL-append (local
        // diagonal) by ShardedReplogSink and post-TryAdvanceAsync
        // (foreign entries) by ReplicationApplier.
        builder.Services.TryAddSingleton<LocalVectorClockCache>();

        // Producer-side seeder used by operator tooling after an
        // intra-cluster snapshot/restore to walk the restored values'
        // VC slots and re-seed the per-tree LocalVectorClock (durable
        // pin via IReplicationHighWaterMarkGrain.PinSnapshotAsync +
        // in-memory prime via LocalVectorClockCache.AdvanceForeign).
        // IShardCountProvider is the testability seam wrapping the
        // core LatticeOptionsResolver shard-count component.
        builder.Services.TryAddSingleton<IShardCountProvider, DefaultShardCountProvider>();
        builder.Services.TryAddSingleton<IReplicationLocalVcSeeder, LatticeReplicationLocalVcSeeder>();

        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IMutationObserver, ReplicationMutationObserver>());
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeReplicationOptions>, LatticeReplicationOptionsValidator>());

        // Security: secret-source surface and the startup hostile-config
        // scan. The default secret source reads from the
        // LATTICE_REPLICATION_* environment-variable surface; hosts
        // replace it via AddLatticeReplicationSecrets<TSource>() or via
        // AddLatticeReplicationSecretsFromConfiguration. The scan
        // refuses to start the silo when secrets are sourced from a
        // file under the application directory (typically a checked-in
        // appsettings.json).
        builder.Services.TryAddSingleton<ILatticeReplicationSecretSource, EnvironmentVariableSecretSource>();
        builder.Services.TryAddSingleton<TimeProvider>(_ => TimeProvider.System);
        builder.Services.TryAddSingleton<IReplicationSecretProvider, CachingReplicationSecretProvider>();
        builder.Services.AddOptions<LatticeReplicationSecurityOptions>();
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, LatticeReplicationConfigurationSafetyValidator>());

        // Replication-only commit-log seam: streaming snapshot drain for
        // the SnapshotThenWal recovery path. The core ICommitLogReader /
        // ICommitLogWriter / IWalStorageProvider are already wired by
        // AddLattice, so they are not re-registered here.
        builder.Services.TryAddSingleton<ILeafSnapshotProvider, LeafSnapshotProvider>();

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

    /// <summary>
    /// <see cref="IPostConfigureOptions{TOptions}"/> that mirrors WAL-related
    /// fields from <see cref="LatticeReplicationOptions"/> onto
    /// <see cref="LatticeOptions"/> for the same tree id, so a host that
    /// configures the replication options surface gets the matching values
    /// reflected on the core options surface read by the WAL grain. Runs
    /// after every <c>Configure</c> action on <see cref="LatticeOptions"/>,
    /// preserving any explicit per-tree override the host applied directly.
    /// </summary>
    /// <remarks>
    /// The mirror is deliberately one-way (replication -> core). Hosts that
    /// configure <see cref="LatticeOptions"/> directly take precedence: the
    /// post-configure step only writes when the replication side carries a
    /// non-default value for the corresponding field, so a direct override
    /// is preserved as long as the replication side is left at its default.
    /// </remarks>
    private sealed class MirrorReplicationOptionsToLatticeOptions(
        IOptionsMonitor<LatticeReplicationOptions> replicationOptions) : IPostConfigureOptions<LatticeOptions>
    {
        public void PostConfigure(string? name, LatticeOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            var rep = replicationOptions.Get(name ?? Options.DefaultName);

            if (rep.ReplogPartitions != LatticeReplicationOptions.DefaultReplogPartitions
                && options.WalPartitions == LatticeOptions.DefaultWalPartitions)
            {
                options.WalPartitions = rep.ReplogPartitions;
            }

            if (rep.WalMaxBatchEntries != LatticeReplicationOptions.DefaultWalMaxBatchEntries
                && options.WalMaxBatchEntries == LatticeOptions.DefaultWalMaxBatchEntries)
            {
                options.WalMaxBatchEntries = rep.WalMaxBatchEntries;
            }

            if (rep.WalMaxBatchBytes != LatticeReplicationOptions.DefaultWalMaxBatchBytes
                && options.WalMaxBatchBytes == LatticeOptions.DefaultWalMaxBatchBytes)
            {
                options.WalMaxBatchBytes = rep.WalMaxBatchBytes;
            }

            if (rep.WalMaxPendingBatches != LatticeReplicationOptions.DefaultWalMaxPendingBatches
                && options.WalMaxPendingBatches == LatticeOptions.DefaultWalMaxPendingBatches)
            {
                options.WalMaxPendingBatches = rep.WalMaxPendingBatches;
            }

            if (rep.WalStorageProvider is not null && options.WalStorageProvider is null)
            {
                options.WalStorageProvider = rep.WalStorageProvider;
            }

            if (rep.WalRetention is not null && options.WalRetention is null)
            {
                options.WalRetention = rep.WalRetention;
            }
        }
    }
}
