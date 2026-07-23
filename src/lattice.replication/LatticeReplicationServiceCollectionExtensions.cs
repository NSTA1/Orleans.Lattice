using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
    /// <para>
    /// By default this call configures replication from the static
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map only. Set
    /// <paramref name="enableRuntimeConfig"/> to opt into runtime per-tree
    /// enable/disable that converges across peers: it enrols the
    /// <c>sys-replication-config</c> tree and swaps in the dynamic,
    /// snapshot-backed membership and merge-mode seams. It is off by default so a
    /// static-only deployment does not pay for, or ship, the runtime control
    /// plane, mirroring how <see cref="ReplicateLatticeSystemTrees(ISiloBuilder, bool)"/>
    /// gates the membership and auth system trees.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder. Must not be <see langword="null"/>.</param>
    /// <param name="configure">Delegate that populates <see cref="LatticeReplicationOptions"/>. Must not be <see langword="null"/>.</param>
    /// <param name="enableRuntimeConfig">
    /// When <see langword="true"/>, enrols the reserved <c>sys-replication-config</c>
    /// tree and installs the dynamic, snapshot-backed replication-config control
    /// plane (the <see cref="ILatticeReplicationConfigAuthority"/> the
    /// <c>Orleans.Lattice.Api.Replication</c> facade drives). Defaults to
    /// <see langword="false"/>, leaving replication configured purely from the
    /// static <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    public static ISiloBuilder AddLatticeReplication(
        this ISiloBuilder builder,
        Action<LatticeReplicationOptions> configure,
        bool enableRuntimeConfig = false)
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
        // Anti-entropy digest-probe transport: default no-op so the
        // detection scheduler records a non-comparable outcome rather
        // than a spurious mismatch when no real probe transport (e.g.
        // the gRPC binding) is wired in. The gRPC package replaces this
        // with a real implementation via Replace().
        builder.Services.TryAddSingleton<IReplicationDigestProbeTransport, NoOpReplicationDigestProbeTransport>();
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
        // Register the options-backed resolver both as the concrete type (so the
        // dynamic snapshot resolver installed when enableRuntimeConfig is set
        // can inject it as its static seed/fallback) and as the active
        // ILatticeMergeModeResolver. A user who registered their own resolver
        // before this call is left untouched (both TryAdds are no-ops).
        builder.Services.TryAddSingleton<ConfiguredLatticeMergeModeResolver>();
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

        // Same swap protocol for the replication-configuration seam: remove the
        // core's DefaultLatticeReplicationContext (reports replication disabled)
        // when present, then TryAdd the configured context that exposes the
        // ClusterId as the local replica id and delegates per-tree merge-mode
        // resolution to the resolver above. A host-supplied context registered
        // before this call is left untouched (TryAdd is a no-op).
        for (var i = builder.Services.Count - 1; i >= 0; i--)
        {
            var d = builder.Services[i];
            if (d.ServiceType == typeof(ILatticeReplicationContext)
                && d.ImplementationType == typeof(DefaultLatticeReplicationContext))
            {
                builder.Services.RemoveAt(i);
            }
        }
        builder.Services.TryAddSingleton<ILatticeReplicationContext, ConfiguredLatticeReplicationContext>();

        // The reusable runtime precondition validator. Shared by the boot-time
        // startup guard below (over statically declared trees) and the later
        // runtime enable path, so a flag-mode tree declared or enabled without a
        // local replica id is rejected cleanly rather than faulting on first
        // write.
        builder.Services.TryAddSingleton<ILatticeReplicationPreconditionValidator, LatticeReplicationPreconditionValidator>();

        // Fail fast at silo start when a flag-mode tree is declared without a
        // configured replica id, rather than faulting on the first flag-CRDT
        // membership write.
        builder.Services.AddSingleton<IHostedService, LatticeReplicationMergeModeStartupValidator>();

        // Fail fast at silo start when a materialised view's replication mode is
        // inconsistent with the replicated-trees configuration (DeriveLocally +
        // view tree replicated = two writers; ShipView + view tree not replicated
        // = consumers never receive it). Only meaningful when replication is
        // configured, so it lives here rather than in AddLatticeViews; it no-ops
        // when no startup views are declared.
        builder.Services.AddSingleton<IHostedService, LatticeViewReplicationStartupValidator>();

        builder.Services.TryAddSingleton<IReplicationBatchEncoder, OrleansBinaryReplicationBatchEncoder>();

        // Framing-tail compressor registry. Each algorithm-specific
        // implementation is registered as an ILatticeCompressor so the
        // canonical encoder's IEnumerable<ILatticeCompressor>
        // constructor can build its dispatch dictionary once. Zstd is
        // registered unconditionally because dict-less
        // LatticeCompression.Zstd is now the default framing algorithm,
        // so this registration backs the out-of-the-box behaviour (not
        // just an opt-in); the underlying ZstdSharp.Port library is pure
        // managed code so the registration cost when the algorithm is
        // never used is one allocation at startup. The default
        // compressor uses LatticeReplicationOptions.DefaultFramingCompressionLevel
        // (3, the canonical "fast" preset). Hosts that want a non-default
        // level pre-register their own ILatticeCompressor singleton
        // (constructed with their preferred level) before calling
        // AddLatticeReplication, and TryAddEnumerable preserves that
        // entry. Hosts that want a custom algorithm cast a byte in
        // [0x80, 0xFF] into LatticeCompression as the
        // ILatticeCompressor.Algorithm tag and call
        // AddLatticeCompressor on the service collection - the encoder
        // keys its dispatch on the raw byte so no core enum churn is
        // required. See docs/lattice/compression.md.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeCompressor, ZstdLatticeCompressor>(
                _ => new ZstdLatticeCompressor(LatticeReplicationOptions.DefaultFramingCompressionLevel)));

        // Default shared-dictionary provider: an empty operator-supplied
        // provider that resolves no dictionary ids. This keeps the
        // dictionary-aware compressor's DI dependency satisfiable on
        // every silo (so the optional AddLatticeZstdDictionaryCompressor
        // registration always activates), while a default build never
        // resolves a dictionary - opting into shared-dictionary
        // compression requires a host to register its own provider (e.g.
        // via AddLatticeCompressionDictionaries) before this call, and
        // TryAddSingleton preserves that host-supplied registration.
        builder.Services.TryAddSingleton<ILatticeCompressionDictionaryProvider>(
            _ => OperatorSuppliedCompressionDictionaryProvider.Empty);

        // Default receiver-side flow-control policy: WAL-saturation-driven
        // back-pressure (on by default). The policy reads the core
        // IWalSaturationSignal and asks the sender to shrink batches and
        // pause when the receiver's local WAL is Throttled or Saturated,
        // and degrades to ReceiverFlowControlHint.None when no signal is
        // registered. Registered via TryAddSingleton, so a host that wants
        // the old blind-push behaviour pre-registers
        // NoOpReceiverFlowControlPolicy before this call, and a host that
        // wants to tune the mapping calls AddWalSaturationReceiverFlowControl(...).
        builder.Services.AddOptions<WalSaturationReceiverFlowControlOptions>();
        builder.Services.TryAddSingleton<IReceiverFlowControlPolicy>(sp =>
            new WalSaturationReceiverFlowControlPolicy(
                sp.GetService<IWalSaturationSignal>(),
                sp.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<WalSaturationReceiverFlowControlOptions>>()));
        // The cursor registry, leaf-cursor reporter, and WAL GC are core
        // seams (formerly in this package, promoted in v3.5.0). Calling
        // AddWalCursorRegistry registers the in-memory default plus the
        // leaf-as-materialiser reporter; AddLatticeWalGc registers the
        // GC. Both extension calls are idempotent - a host that already
        // wired them up directly is unaffected.
        builder.AddWalCursorRegistry();
        builder.AddLatticeWalGc();
        builder.Services.TryAddSingleton<ReplicationPeerStats>();
        // Receiver-side applied-content index backing the content-hash
        // payload-elision manifest exchange. Populated by the applier
        // when it applies a point-Set on a content-hash-dedup-enabled
        // tree, and queried by the gRPC receiver service when it answers
        // an inbound manifest exchange. A best-effort, in-process,
        // bounded cache - never serialized, never durable - so a cold
        // or evicted index simply reports the entry as missing and the
        // sender ships it (always safe). Maintaining it is off-path-free
        // when ContentHashDedupEnabled is false.
        builder.Services.TryAddSingleton<ReceiverAppliedContentIndex>();
        // Per-peer wire-version negotiation telemetry singleton. Backs
        // the wire_version.negotiated / wire_version.downgrade_active
        // observable gauges and is injected into the shipper grain so
        // it can record the negotiated version each pump tick. Wired
        // unconditionally - the gauges report only for peers the
        // shipper actually negotiates with, so an inactive replication
        // host emits nothing.
        builder.Services.TryAddSingleton<WireVersionNegotiationState>();
        // Per-peer shared-dictionary negotiation telemetry singleton.
        // Records the per-(tree, peer) negotiation outcome each pump tick
        // and exposes a Snapshot() for diagnostics. Wired unconditionally -
        // it records only for peers the shipper actually negotiates a
        // dictionary with, so an inactive replication host stores nothing.
        builder.Services.TryAddSingleton<SharedDictionaryNegotiationState>();
        // Default runtime-observable peer topology: projects
        // LatticeReplicationOptions.ReplicationPeers via
        // IOptionsMonitor.OnChange so hosts can add or remove peers at
        // runtime without restarting the silo. Hosts that source their
        // topology from a service registry or other dynamic surface
        // can replace the registration by pre-registering their own
        // IReplicationTopology singleton before AddLatticeReplication.
        builder.Services.TryAddSingleton<IReplicationTopology, OptionsReplicationTopology>();

        // Producer-side seeder used by operator tooling after an
        // intra-cluster snapshot/restore to walk the restored values'
        // VC slots and re-seed the per-tree LocalVectorClock (durable
        // pin via IReplicationHighWaterMarkGrain.PinSnapshotAsync).
        // IShardCountProvider is the testability seam wrapping the
        // core LatticeOptionsResolver shard-count component.
        builder.Services.TryAddSingleton<IShardCountProvider, DefaultShardCountProvider>();
        builder.Services.TryAddSingleton<IReplicationLocalVcSeeder, LatticeReplicationLocalVcSeeder>();

        // Durable write-fence / shipping-pause primitive (issue #1173) seams.
        // ISagaCompletionSource gates the cross-cluster shipping resume on
        // observed global saga completion; IReplicationReceiveGate lets the
        // inbound apply path consult the per-tree receive fence with a short
        // cache. Both are TryAddSingleton so a host (or test) can substitute an
        // alternative - notably a fake completion source that simulates a
        // laggard participant.
        builder.Services.TryAddSingleton<ISagaCompletionSource, CoordinatorSagaCompletionSource>();
        builder.Services.TryAddSingleton<IReplicationReceiveGate, ReplicationReceiveGate>();

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
        // hosted service doesn't lose this one and vice versa. The
        // factory injects the replicated-tree membership union and,
        // when the host opted into runtime replication config, the
        // snapshot maintainer so runtime-enabled trees are enrolled
        // live; GetService keeps the maintainer optional for a
        // static-only host.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, ReplicationDriverActivationService>(sp => new ReplicationDriverActivationService(
                sp.GetRequiredService<IGrainFactory>(),
                sp.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
                sp.GetRequiredService<IReplicationTopology>(),
                sp.GetRequiredService<ILogger<ReplicationDriverActivationService>>(),
                sp.GetRequiredService<ReplicationPeerStats>(),
                sp.GetRequiredService<Orleans.Lattice.Backup.IReplicatedTreeMembership>(),
                sp.GetService<CompiledReplicationConfigSnapshotMaintainer>())));

        // Durable cross-cluster saga participant model. Registered here,
        // before the gRPC binding's TryAddSingleton default runs, so this
        // durable handler - which routes every inbound saga RPC to the
        // per-saga participant grain - is the effective
        // ILatticeSagaControlHandler and the transport-only
        // NoParticipantSagaControlHandler is never used on a silo that
        // wires replication. TryAdd still lets a host substitute its own
        // handler ahead of this call.
        builder.Services.TryAddSingleton<ILatticeSagaControlHandler, LatticeSagaControlHandler>();

        // Coordinated multi-cluster restore (issue #1175). The restore participant
        // maps the backup restore engine onto the saga as the first internal
        // ISagaParticipant; the dispatcher promotes a restore whose target tree is
        // replicated into an all-or-nothing coordinated saga. The participant is
        // registered as a concrete singleton and forwarded into the ISagaParticipant
        // collection so the dispatcher and the participant grain share one instance
        // (and one in-memory built-shadow cache). The capacity probe defaults to
        // permissive; a host that enforces a storage budget substitutes it.
        builder.Services.TryAddSingleton<IRestoreCapacityProbe, UnboundedRestoreCapacityProbe>();
        builder.Services.TryAddSingleton(static sp => new RestoreParticipant(
            sp.GetServices<Orleans.Lattice.Backup.ILatticeCoordinatedRestoreEngine>().FirstOrDefault(),
            sp.GetServices<Orleans.Lattice.Backup.ILatticeBackupRestoreService>().FirstOrDefault(),
            sp.GetRequiredService<IRestoreCapacityProbe>(),
            sp.GetRequiredService<IGrainFactory>(),
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RestoreParticipant>>(),
            sp.GetServices<Orleans.Lattice.Backup.ILatticeBackupSetResolver>().FirstOrDefault(),
            sp.GetRequiredService<Orleans.Lattice.Backup.IReplicatedTreeMembership>(),
            sp.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()));
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ISagaParticipant, RestoreParticipant>(
                static sp => sp.GetRequiredService<RestoreParticipant>()));

        // Replace the backup package's no-op defaults with the real
        // replication-backed implementations. AddSingleton (not TryAdd) so the real
        // implementation wins regardless of whether AddLatticeBackup ran before or
        // after this call; the backup no-op remains the fallback for a backup-only
        // host. Wiring the real IReplicatedTreeMembership makes the shared-sink
        // guard fire under replication; wiring the real dispatcher makes the public
        // restore entry point promote a replicated-tree restore to a saga.
        builder.Services.AddSingleton<Orleans.Lattice.Backup.IReplicatedTreeMembership, OptionsReplicatedTreeMembership>();
        builder.Services.AddSingleton<Orleans.Lattice.Backup.IRestoreSagaDispatcher>(static sp => new RestoreSagaDispatcher(
            sp.GetRequiredService<Orleans.Lattice.Backup.IReplicatedTreeMembership>(),
            sp.GetRequiredService<IReplicationTopology>(),
            sp.GetServices<Orleans.Lattice.Backup.ILatticeCoordinatedRestoreEngine>().FirstOrDefault(),
            sp.GetRequiredService<IRestoreCapacityProbe>(),
            sp.GetRequiredService<ISagaControlChannel>(),
            sp.GetRequiredService<IGrainFactory>(),
            sp.GetRequiredService<RestoreParticipant>(),
            sp.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>(),
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RestoreSagaDispatcher>>(),
            sp.GetServices<Orleans.Lattice.Backup.ILatticeBackupSetResolver>().FirstOrDefault()));

        // Runtime replication-configuration control plane (opt-in). Applied last
        // so the engine registrations above are the seed/fallback the dynamic,
        // snapshot-backed seams layer on top. Off by default: a static-only
        // deployment never enrols or ships the sys-replication-config tree.
        if (enableRuntimeConfig)
        {
            ApplyReplicationConfigAnchor(builder);
        }

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
    /// Opts into the self-distributing, auto-activating shared compression
    /// dictionary with a single switch. Composes the four primitives that were
    /// previously separate host wiring: it registers the auto-training
    /// shared-dictionary provider (<see cref="AutoTrainingCompressionDictionaryProvider"/>)
    /// with training enabled and the dictionary-aware Zstandard compressor;
    /// exposes the provider as the commit-time training sampler so every
    /// replicated value feeds the reservoir; registers a turn-safe training
    /// pump (<see cref="AutoSharedDictionaryTrainingService"/>) so dictionaries
    /// are trained off the hot path with no host code; and turns on the per-tree
    /// <see cref="LatticeReplicationOptions.AutoSharedDictionaryEnabled"/> switch
    /// so the ship path converges onto a peer's advertised dictionary (pulling
    /// the bytes it does not yet hold), negotiates it fingerprint-safely, and
    /// compresses wire traffic with it. Two clusters that both call this
    /// converge on a usable shared dictionary with no out-of-band asset
    /// provisioning; the default build (this method not called) is byte-for-byte
    /// unchanged and ships no new RPC traffic.
    /// <para>
    /// Must be called after <see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions}, bool)"/>.
    /// Because it is an explicit opt-in, it installs the auto-training provider
    /// as the active <see cref="ILatticeCompressionDictionaryProvider"/>,
    /// overriding the framework-default operator-supplied provider; a host that
    /// wants a different provider simply does not call this switch.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configureTraining">
    /// Optional delegate to tune the
    /// <see cref="CompressionDictionaryTrainingOptions"/> (reservoir size,
    /// training cadence, retained versions, ...). <see cref="CompressionDictionaryTrainingOptions.Enabled"/>
    /// is forced on regardless of the delegate.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    public static ISiloBuilder AddLatticeAutoSharedDictionary(
        this ISiloBuilder builder,
        Action<CompressionDictionaryTrainingOptions>? configureTraining = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Register the auto-training provider (training forced on) as the
        // singleton ILatticeCompressionDictionaryProvider, and the
        // dictionary-aware Zstandard compressor that resolves its bytes.
        builder.Services.AddLatticeAutoTrainingCompressionDictionary(options =>
        {
            configureTraining?.Invoke(options);
            options.Enabled = true;
        });
        builder.Services.AddLatticeZstdDictionaryCompressor();

        // This is an explicit opt-in, so it wins over the framework-default
        // operator-supplied provider that AddLatticeReplication registers. A
        // host that wants a different provider simply does not call this
        // switch; calling it installs the auto-trainer as the active provider.
        builder.Services.Replace(ServiceDescriptor.Singleton<ILatticeCompressionDictionaryProvider>(
            sp => sp.GetRequiredService<AutoTrainingCompressionDictionaryProvider>()));

        // Drive the trainer with no host code: the commit-time observer
        // already samples through the injected provider (it implements the
        // sampler seam), and this hosted service pumps TryTrain on a
        // turn-safe cadence.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, AutoSharedDictionaryTrainingService>());

        // Turn on the per-tree ship-path switch for every replicated tree.
        // ConfigureAll layers under any later per-tree override a host applies.
        builder.Services.ConfigureAll<LatticeReplicationOptions>(
            options => options.AutoSharedDictionaryEnabled = true);

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
