using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

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
    /// and the in-memory <see cref="IWalStorageProvider"/> baseline so a
    /// single-cluster host gets a working commit-log pipeline with no
    /// extra wiring. Hosts that need a durable WAL backing store call
    /// <see cref="AddWalStorage"/> with a custom factory - or one of the
    /// package-level overloads that wraps it (for example
    /// <c>AddAzureTableWalStorage</c>) - either before or after
    /// <c>AddLattice</c>. The host-supplied factory wins regardless of
    /// order: <see cref="AddWalStorage"/> with a factory uses
    /// <c>Services.Replace</c> so it displaces the in-memory baseline
    /// rather than being silently dropped by the <c>TryAdd</c> path that
    /// installs the baseline. See <see cref="AddWalStorage"/> for the
    /// full registration-order contract.
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

        // Register the GrainId TypeConverter before any grain storage is
        // configured below so Newtonsoft.Json (the default serialiser
        // backing AzureTableGrainStorage) can round-trip the
        // GrainId-keyed dictionaries the lattice persists (e.g.
        // InternalNodeState.ChildDigests). Without this, the first
        // grain reactivation against a non-empty Azure Tables grain-
        // state table throws JsonSerializationException. Idempotent.
        Internal.GrainIdTypeConverterRegistration.EnsureRegistered();

        configureStorage(builder, LatticeOptions.StorageProviderName);
        builder.Services.AddSingleton<IValidateOptions<LatticeOptions>, LatticeOptionsValidator>();
        builder.Services.AddSingleton<IValidateOptions<LatticeTagIndexReconciliationOptions>, LatticeTagIndexReconciliationOptionsValidator>();
        builder.Services.AddSingleton<LatticeOptionsResolver>();
        builder.Services.AddSingleton<MutationObserverDispatcher>();
        builder.Services.AddSingleton<ILatticeFallOffLogDetector, LatticeFallOffLogDetector>();

        // Storage-usage observable-gauge sink. Constructing the singleton
        // registers the byte-accurate storage gauges on the shared meter
        // (idempotent, process-wide); the per-tree aggregator pushes the
        // latest report here so a meter scrape never fans out to grains.
        builder.Services.AddSingleton<LatticeStorageUsageMetrics>();

        // Per-tree admission-control observable-gauge sink. Constructing the
        // singleton registers the admission gauges (live_keys, estimated_bytes,
        // over_advisory, utilization) on the shared meter (idempotent,
        // process-wide); the per-tree aggregator pushes the latest admission
        // sample here so a meter scrape never fans out to grains.
        builder.Services.AddSingleton<LatticeAdmissionMetrics>();

        // Per-silo background poller that drives every registered tree's
        // storage-usage aggregator on a cadence so the gauges populate
        // without any caller invoking ILattice.GetStorageUsageAsync. Each
        // aggregator publishes to its own host silo's sink, so a tree's
        // series appears on exactly one silo and a cross-silo sum counts it
        // once; the sink's staleness horizon clears a migrated tree from the
        // old silo. Safe to run on every silo (redundant polls hit the
        // aggregator's cache).
        builder.Services.AddSingleton<IHostedService, LatticeStorageUsagePoller>();

        // Core WAL durability seams: in-memory provider as the singleton
        // default (hosts replace via AddWalStorage), commit-log writer
        // and reader, and a null-returning mode resolver. The replication
        // package replaces the resolver via services.Replace(...) so per-
        // tree mode resolution kicks in only when replication is added.
        builder.AddWalStorage();
        // Per-tree pinned WAL placement: the catalog turns the durable
        // per-partition provider keys (recorded in TreeRegistryEntry.WalPlacement)
        // back into live IWalStorageProvider instances at WAL shard activation.
        // The "default" key always resolves to the IWalStorageProvider baseline
        // registered just above; named keys are added through
        // AddLatticeWalStorageProvider. TryAdd so a host or downstream package
        // can supply an alternate catalog implementation.
        builder.Services.TryAddSingleton<IWalStorageProviderCatalog, WalStorageProviderCatalog>();
        builder.Services.TryAddSingleton<ICommitLogWriter, WalCommitLogWriter>();
        builder.Services.TryAddSingleton<ICommitLogReader, WalCommitLogReader>();
        // Per-silo writer drain: wire WalCommitLogWriter.DrainAsync into
        // the host's StopAsync stage so every parked admission caller is
        // released within bounded time of SIGTERM. Each silo process gets
        // its own hosted-service instance bound to its own writer singleton;
        // peer silos in the cluster drain independently with no cross-silo
        // coordination. The hosted service safely no-ops on hosts where
        // ICommitLogWriter is replaced with a non-WalCommitLogWriter
        // implementation (test doubles, future alternates).
        builder.Services.AddSingleton<IHostedService, BPlusTree.Grains.WalCommitLogWriterDrainer>();
        // Per-silo WAL saturation back-pressure surface: the signal
        // singleton backs the public IWalSaturationSignal (polling +
        // await-able gate), the observer dispatcher fans out per-
        // transition notifications to every registered
        // IWalSaturationObserver, and the sampler hosted-service
        // recomputes the per-tree classification on a fixed cadence
        // (LatticeOptions.WalSaturationSampleInterval, default 200 ms).
        // All three are zero-cost when no caller queries the signal
        // and no observer is registered - the sampler still runs but
        // its per-tick work is a small dictionary enumeration that
        // never reaches a grain.
        builder.Services.TryAddSingleton<BPlusTree.Grains.WalSaturationSignal>();
        builder.Services.TryAddSingleton<IWalSaturationSignal>(sp => sp.GetRequiredService<BPlusTree.Grains.WalSaturationSignal>());
        builder.Services.TryAddSingleton<BPlusTree.Grains.WalSaturationObserverDispatcher>();
        // Always-on in-memory consumer-cursor registry. The WAL is integral to
        // every Lattice deployment, so the registry that the saturation sampler
        // reads to compute materialiser drain lag must never be silently absent:
        // a missing registry would turn the drain-lag back-pressure input off
        // without any signal. We register the process-local
        // InMemoryWalCursorRegistry here as a guaranteed fallback so the sampler
        // can take a hard dependency on IWalCursorRegistry. Hosts that opt into
        // a materialiser/replication stack call AddWalCursorRegistry, which
        // layers in the leaf cursor reporter (and may replace this default with
        // a host-supplied factory); core-only hosts still get a live registry
        // that reports a null frontier (no reporters) - correctly yielding zero
        // drain lag rather than a disabled signal. TryAdd so a downstream
        // package or host registration still wins.
        builder.Services.TryAddSingleton<IWalCursorRegistry, InMemoryWalCursorRegistry>();
        // Always-on leaf cursor reporting. Registering the registry alone is not
        // enough: the registry is the container, and the leaf cursor reporter is
        // the producer that publishes each leaf's applied checkpoint frontier
        // into it. Without a reporter the registry stays empty, GetMinCursorAsync
        // returns null, and the sampler's drain-lag input reads zero forever -
        // the same silent-off failure mode one layer down. We register the
        // lightweight InMemoryLeafCursorReporter here so every leaf reports its
        // in-memory cursor by default and drain-lag back-pressure is live for
        // every write workload. It does only the cheap in-memory work; the
        // durable cross-restart pin store (real write amplification) stays
        // opt-in, layered in by AddWalCursorRegistry, which Replaces this
        // default with the durable-pin-aware LeafCursorReporter. TryAdd so an
        // opt-in registration still wins.
        builder.Services.TryAddSingleton<BPlusTree.Grains.ILeafCursorReporter, BPlusTree.Grains.InMemoryLeafCursorReporter>();
        builder.Services.AddSingleton<IHostedService, BPlusTree.Grains.WalSaturationSampler>();
        // WAL byte-budget encoder: the canonical Orleans-binary
        // implementation produces the exact serialised bytes for each
        // captured WalRecord, so WalShardGrain pays one encode per
        // append and hands the same bytes straight to the configured
        // IWalStorageProvider via AppendEncodedBatchAsync. Singleton-
        // scoped so the underlying codec stays hot.
        builder.Services.TryAddSingleton<IWalRecordEncoder, OrleansBinaryWalRecordEncoder>();
        builder.Services.TryAddSingleton<ILatticeMergeModeResolver, DefaultLatticeMergeModeResolver>();
        // CRDT shape registry: closed-shape modes (OrSet / PnCounter /
        // VersionVector / MvRegister) are pre-populated on construction
        // so no host registration is required for them. Generic OrMap
        // descriptors are installed per tree via AddOrMapShape.
        builder.Services.TryAddSingleton<CrdtShapeRegistry>();
        // Single-cluster default for the per-tree origin-cluster-id resolver.
        // Returns string.Empty for every tree so the WAL writer stamps an
        // empty OriginClusterId on locally-authored records. The replication
        // package replaces this with ConfiguredLatticeOriginClusterIdResolver
        // (reads LatticeReplicationOptions.ClusterId) via services.Replace(...).
        builder.Services.TryAddSingleton<ILatticeOriginClusterIdResolver, DefaultLatticeOriginClusterIdResolver>();
        // Single-cluster default for the replication-configuration seam. Reports
        // "replication disabled", an empty local replica id, and a null merge
        // mode for every tree, so core features (e.g. the tag index) use their
        // single-writer path. The replication package replaces this with
        // ConfiguredLatticeReplicationContext (backed by the replication
        // options) via the same remove-Default-then-TryAdd swap it uses for the
        // merge-mode and origin-cluster-id resolvers.
        builder.Services.TryAddSingleton<ILatticeReplicationContext, DefaultLatticeReplicationContext>();
        // Tag-index factory. Captures the replication-configuration seam so an
        // injected tag index derives its membership mode and replica id from
        // server config instead of per-call parameters; the TagIndex extension
        // methods remain the single-cluster (last-writer-wins) convenience path.
        builder.Services.TryAddSingleton<ILatticeTagIndexFactory, DefaultLatticeTagIndexFactory>();

        // In-library shutdown log demotion. During the host deactivation
        // window the Orleans runtime emits a Warning per in-flight grain
        // call from two transport tear-down categories ("Orleans.Messaging"
        // and the placement service) - the silo is refusing application
        // messages and cannot create local activations because the
        // placement directory is being torn down. That is expected
        // shutdown back-pressure, not a fault, but at steady-state log
        // verbosity it floods the silo log on every clean stop. The filter
        // below demotes ONLY those two categories' Warning-level records to
        // suppressed, and ONLY while IHostApplicationLifetime.ApplicationStopping
        // is signalled; on a healthy host the categories keep their Warning
        // floor untouched, and Error/Critical always survive even during
        // shutdown. The mechanism is a dynamic MEL LoggerFilterRule rather
        // than an IGrainCallFilter because the offending records originate
        // in the Orleans runtime's own logging, not in the grain-call
        // pipeline, so a call filter could never intercept them. The filter
        // instance resolves the lifetime lazily at first log-time (never at
        // logging-options build time) so it cannot perturb host startup.
        builder.Services.TryAddSingleton<LatticeShutdownLogFilter>();
        builder.Services.AddOptions<LoggerFilterOptions>()
            .Configure<LatticeShutdownLogFilter>(static (options, filter) =>
            {
                foreach (var category in LatticeShutdownLogFilter.DemotedCategories)
                {
                    // logLevel:Warning sets the per-category floor so the
                    // categories behave as on a healthy host (Warning+
                    // visible); the filter predicate additionally suppresses
                    // the Warning level once the host is stopping.
                    options.Rules.Add(new LoggerFilterRule(
                        providerName: null,
                        categoryName: category,
                        logLevel: LogLevel.Warning,
                        filter: (provider, cat, level) => filter.ShouldEmit(cat, level)));
                }
            });
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
    /// Configures global <see cref="LatticeTagIndexReconciliationOptions"/> that
    /// apply to every tag index unless a per-index override is registered.
    /// </summary>
    public static ISiloBuilder ConfigureLatticeTagIndexReconciliation(
        this ISiloBuilder builder,
        Action<LatticeTagIndexReconciliationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.ConfigureAll(configure);
        return builder;
    }

    /// <summary>
    /// Configures <see cref="LatticeTagIndexReconciliationOptions"/> for a
    /// specific tag index identified by <paramref name="indexName"/>. These
    /// settings override the global defaults for that index only.
    /// </summary>
    public static ISiloBuilder ConfigureLatticeTagIndexReconciliation(
        this ISiloBuilder builder,
        string indexName,
        Action<LatticeTagIndexReconciliationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(configure);
        builder.Services.Configure(indexName, configure);
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
    /// configurability.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Registration semantics differ by overload, and the difference is
    /// load-bearing because <see cref="AddLattice"/> self-registers the
    /// in-memory baseline via the no-factory overload as part of its
    /// own setup:
    /// </para>
    /// <list type="bullet">
    /// <item><description>
    /// <b>No-factory baseline</b> (<c>AddWalStorage()</c>): registered via
    /// <c>TryAddSingleton</c>. First registration wins, so a host (or a
    /// downstream package) that has already registered a real provider
    /// keeps it - the baseline never displaces an explicit choice.
    /// </description></item>
    /// <item><description>
    /// <b>Host-supplied factory</b> (<c>AddWalStorage(factory)</c>):
    /// registered via <c>Services.Replace(...)</c>. Supplying a factory
    /// is unambiguous host intent to override; the call therefore wins
    /// regardless of whether <see cref="AddLattice"/> (or any other
    /// downstream extension that wraps <c>AddWalStorage</c>) has already
    /// installed the in-memory baseline.
    /// </description></item>
    /// </list>
    /// <para>
    /// Net effect: the host's choice of WAL provider is order-independent
    /// with respect to <see cref="AddLattice"/>. <c>AddWalStorage(factory)</c>
    /// and the package-level overloads that build on it
    /// (e.g. <c>AddAzureTableWalStorage</c>) may be called before or
    /// after <see cref="AddLattice"/> and the durable provider survives.
    /// Calling <c>AddWalStorage(factory)</c> multiple times follows
    /// last-call-wins.
    /// </para>
    /// </remarks>
    public static ISiloBuilder AddWalStorage(
        this ISiloBuilder builder,
        Func<IServiceProvider, IWalStorageProvider>? factory = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (factory is null)
        {
            // Baseline registration: first-wins so AddLattice's self-call
            // never stomps a real provider that a host registered earlier
            // in the builder.
            builder.Services.TryAddSingleton<IWalStorageProvider, InMemoryWalStorageProvider>();
        }
        else
        {
            // Explicit host intent: replace whatever is there (the in-memory
            // baseline installed by AddLattice, a sibling package's earlier
            // registration, or nothing) with the supplied factory. Using
            // Replace rather than TryAdd makes the host's choice
            // order-independent with respect to AddLattice; using TryAdd
            // here would silently drop the host's factory when AddLattice
            // had already installed the baseline, which is the bug this
            // overload exists to prevent.
            builder.Services.Replace(ServiceDescriptor.Singleton<IWalStorageProvider>(factory));
        }
        return builder;
    }

    /// <summary>
    /// Registers a named <see cref="IWalStorageProvider"/> in the silo's
    /// <see cref="IWalStorageProviderCatalog"/> under <paramref name="key"/>, so
    /// a tree's WAL partitions can be pinned to it (and moved to it through the
    /// <see cref="ILatticeAdmin"/> move surface) by referencing the key. This is
    /// the safe, multi-account fan-out seam: register one provider per storage
    /// backend (for example one Azure Table account each) under distinct keys,
    /// then place a hot tree's partitions across them to exceed a single
    /// account's throughput ceiling.
    /// <para>
    /// <b>Cluster contract.</b> Every silo in the cluster must register an
    /// identical set of keys. A partition pinned to a key that a given silo did
    /// not register fails closed on that silo (see
    /// <see cref="LatticeWalProviderMissingException"/>) rather than silently
    /// re-routing to the baseline provider.
    /// </para>
    /// <para>
    /// The reserved key <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>
    /// names the baseline provider registered through <see cref="AddWalStorage"/>
    /// and cannot be registered here. Registering the same key twice follows
    /// last-call-wins.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="key">The catalog key. Must not be <see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>.</param>
    /// <param name="factory">Factory producing the provider for this key.</param>
    public static ISiloBuilder AddLatticeWalStorageProvider(
        this ISiloBuilder builder,
        string key,
        Func<IServiceProvider, IWalStorageProvider> factory)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(factory);
        if (string.Equals(key, IWalStorageProviderCatalog.DefaultProviderKey, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"'{IWalStorageProviderCatalog.DefaultProviderKey}' is reserved for the baseline provider registered through AddWalStorage; choose a different key.",
                nameof(key));
        }

        // Keyed singleton resolved lazily by the catalog on first use; last-call
        // -wins via Replace so re-registering a key swaps the factory.
        builder.Services.Replace(ServiceDescriptor.KeyedSingleton<IWalStorageProvider>(key, (sp, _) => factory(sp)));
        // Marker enumerated by the catalog to learn its key set. One per key;
        // de-duplicated by the catalog's HashSet so a re-registration is benign.
        builder.Services.AddSingleton(new WalStorageProviderRegistration(key));
        return builder;
    }

    /// <summary>
    /// Registers the WAL consumer-cursor registry on the silo. Single-cluster
    /// deployments need the registry so the leaf-as-materialiser cursor
    /// can pin the per-shard WAL GC against the leaf's durably-applied
    /// frontier. The default <see cref="InMemoryWalCursorRegistry"/> is
    /// process-local and is wiped on restart; the durability backstop for that
    /// is the cluster-wide <c>IWalMaterialiserPinGrain</c> (persisted through
    /// the configured grain storage), which the WAL GC consults so it never
    /// trims past a leaf's durable checkpoint for a leaf that has not yet
    /// re-activated and re-reported after a restart. Forward consumers (for
    /// example the replication shipper) still re-report their own
    /// durably-persisted cursors on restart, and the fall-off-log seam handles
    /// any genuine retention gap. The default <see cref="InMemoryWalCursorRegistry"/>
    /// is already registered by <see cref="AddLattice"/> as an always-on
    /// fallback, so calling this without a <paramref name="factory"/> only adds
    /// the leaf reporter and tailing subscriber. A host-supplied
    /// <paramref name="factory"/> takes precedence over that core default
    /// (registered via <c>Replace</c>); a repeated default call is a no-op.
    /// </summary>
    public static ISiloBuilder AddWalCursorRegistry(
        this ISiloBuilder builder,
        Func<IServiceProvider, IWalCursorRegistry>? factory = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (factory is null)
        {
            builder.Services.TryAddSingleton<IWalCursorRegistry, InMemoryWalCursorRegistry>();
        }
        else
        {
            // Replace (not TryAdd) so a host-supplied factory wins even though
            // AddLattice has already registered the in-memory default - TryAdd
            // would silently drop the host's registry.
            builder.Services.Replace(ServiceDescriptor.Singleton<IWalCursorRegistry>(factory));
        }

        // Opting into the cursor registry implies opting into the durable
        // leaf-as-materialiser cursor reporting: upgrade the always-on
        // lightweight InMemoryLeafCursorReporter (registered by AddLattice)
        // to the durable-pin-aware LeafCursorReporter so BPlusLeafGrain
        // .CursorRegistry not only reports its applied frontier in-memory but
        // also mirrors it into the sharded cluster-wide IWalMaterialiserPinGrain
        // store, pinning the WAL GC trim floor across restarts. We drop only
        // the core in-memory default and then TryAdd the durable reporter: a
        // host that registered its own custom ILeafCursorReporter (in either
        // order) keeps it - matching the extensibility contract that
        // AddWalCursorRegistry never clobbers a host-supplied reporter - and a
        // host that never opts in keeps the lightweight in-memory default with
        // its drain-lag signal still live.
        var coreDefaultReporter = builder.Services.FirstOrDefault(d =>
            d.ServiceType == typeof(ILeafCursorReporter) &&
            d.ImplementationType == typeof(InMemoryLeafCursorReporter));
        if (coreDefaultReporter is not null)
        {
            builder.Services.Remove(coreDefaultReporter);
        }

        builder.Services.TryAddSingleton<ILeafCursorReporter, LeafCursorReporter>();

        // Reusable per-shard WAL tailing loop shared by every log consumer
        // (materialised views, the replication producer, future change-feed /
        // audit sinks). It depends on both the commit-log reader (registered by
        // AddLattice) and the cursor registry registered just above, so it is
        // wired here - where the registry is guaranteed present - rather than in
        // AddLattice, which a core-only host may call without a cursor registry.
        builder.Services.TryAddSingleton<Orleans.Lattice.Wal.IWalSubscriber, Orleans.Lattice.Wal.WalLogSubscriber>();
        return builder;
    }

    /// <summary>
    /// Registers the WAL garbage collector on the silo. The GC consumes the
    /// <see cref="IWalCursorRegistry"/> and the <see cref="IWalStorageProvider"/>
    /// registered above, plus <see cref="LatticeOptions.WalRetention"/> as an
    /// optional wall-clock hard ceiling, and trims per-shard WAL partitions
    /// to the safe trim point. Hosts decide when to run a GC pass: a hosted
    /// background service, an Orleans reminder, or an admin-triggered call.
    /// Idempotent: a host-supplied registration via <paramref name="factory"/>
    /// takes precedence and a second call is a no-op.
    /// </summary>
    public static ISiloBuilder AddLatticeWalGc(
        this ISiloBuilder builder,
        Func<IServiceProvider, ILatticeWalGc>? factory = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (factory is null)
        {
            builder.Services.TryAddSingleton<ILatticeWalGc, LatticeWalGc>();
        }
        else
        {
            builder.Services.TryAddSingleton<ILatticeWalGc>(factory);
        }

        // Per-silo background scheduler that drives the WAL GC for every
        // registered tree on the LatticeOptions.WalGcInterval cadence so
        // a durable-WAL host gets bounded retention out of the box,
        // independent of the replication package. Default-off
        // (WalGcInterval == TimeSpan.Zero): the scheduler short-circuits
        // at start until a host opts in. TryAddEnumerable keeps a single
        // registration even though both AddLatticeWalGc and the
        // replication package call this method.
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, LatticeWalGcScheduler>());
        return builder;
    }

    /// <summary>
    /// Configures the shipped <see cref="BoundedExponentialRetryPolicy"/>
    /// as the global <see cref="LatticeOptions.RetryPolicy"/> for every
    /// tree on the silo. The policy is constructed once from the
    /// populated <see cref="BoundedExponentialRetryPolicyOptions"/>
    /// instance and assigned to <see cref="LatticeOptions.RetryPolicy"/>
    /// via <c>ConfigureAll</c>. Hosts that want a per-tree policy
    /// (different budgets for different trees) skip this convenience
    /// and use <c>ConfigureLattice("tree", o =&gt; o.RetryPolicy = ...)</c>
    /// directly.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The retry policy only takes effect when the caller has also
    /// entered a <see cref="LatticeIdempotencyContext"/> scope so the
    /// retried mutation re-stamps the same
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Timestamp"/> and collapses through the
    /// existing dedup paths. Authoring cluster identity is stamped
    /// independently by the silo via
    /// <see cref="LatticeOriginContext"/> /
    /// <see cref="ILatticeOriginClusterIdResolver"/> and is not part
    /// of the idempotency key. Mutating calls without an ambient
    /// idempotency key bypass the policy entirely - retry without an
    /// idempotency key would double-count a
    /// <see cref="PnCounterAccessor"/> increment and produce N
    /// distinct WAL appends per logical operation, which is exactly
    /// the negative-control behaviour the feature is designed to
    /// prevent.
    /// </para>
    /// </remarks>
    public static ISiloBuilder AddLatticeRetryPolicy(
        this ISiloBuilder builder,
        Action<BoundedExponentialRetryPolicyOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new BoundedExponentialRetryPolicyOptions();
        configure?.Invoke(options);
        var policy = new BoundedExponentialRetryPolicy(options);
        builder.Services.ConfigureAll<LatticeOptions>(o => o.RetryPolicy = policy);
        return builder;
    }

    /// <summary>
    /// Registers a typed OR-Map shape for the tree identified by
    /// <paramref name="treeName"/>. Required whenever a tree is configured
    /// for <see cref="LatticeMergeMode.OrMap"/>: the producer-side accessor
    /// and the receiver-side applier both look the descriptor up at runtime
    /// so they can deserialise the generic
    /// <see cref="OrMap{TKey, TValue}"/> state and the matching
    /// <see cref="OrMapDelta{TKey, TValue}"/> wire payload, and fold the
    /// delta in via
    /// <see cref="OrMap{TKey, TValue}.MergeDelta(OrMapDelta{TKey, TValue})"/>
    /// through a single type-erased seam. The closed-shape CRDT modes
    /// (<see cref="LatticeMergeMode.OrSet"/>, <see cref="LatticeMergeMode.PnCounter"/>,
    /// <see cref="LatticeMergeMode.VersionVector"/>, <see cref="LatticeMergeMode.MvRegister"/>)
    /// do not require host registration because their descriptors are
    /// unambiguous; the <see cref="CrdtShapeRegistry"/> pre-populates the
    /// per-mode global defaults on construction. Registering a different
    /// <c>(TKey, TValue)</c> pair for the same tree is a configuration
    /// error and throws at registration time.
    /// </summary>
    public static ISiloBuilder AddOrMapShape<TKey, TValue>(
        this ISiloBuilder builder,
        string treeName)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        builder.Services.TryAddSingleton<CrdtShapeRegistry>();
        builder.Services.AddSingleton<IConfigureOptions<CrdtShapeRegistryStartupMarker>>(
            new ConfigureCrdtShape(treeName, CrdtShape.ForOrMap<TKey, TValue>()));
        // Eager registration via a hosted-startup hook so the descriptor is
        // installed before the first producer emission or WAL apply runs.
        builder.Services.AddSingleton<IHostedService, CrdtShapeStartup>();
        return builder;
    }

    /// <summary>Internal hosted-service marker; registered once per silo.</summary>
    internal sealed class CrdtShapeRegistryStartupMarker { }

    /// <summary>Internal <see cref="IConfigureOptions{TOptions}"/> carrying one shape registration.</summary>
    internal sealed class ConfigureCrdtShape(string treeId, CrdtShape shape)
        : IConfigureOptions<CrdtShapeRegistryStartupMarker>
    {
        /// <summary>The tree id this shape is bound to.</summary>
        public string TreeId { get; } = treeId;

        /// <summary>The shape descriptor.</summary>
        public CrdtShape Shape { get; } = shape;

        /// <inheritdoc />
        public void Configure(CrdtShapeRegistryStartupMarker options) { }
    }

    /// <summary>
    /// Hosted service that drains every registered
    /// <see cref="ConfigureCrdtShape"/> into the singleton
    /// <see cref="CrdtShapeRegistry"/> at silo start, before any
    /// producer-side accessor or receiver-side applier accepts an entry.
    /// </summary>
    internal sealed class CrdtShapeStartup(
        CrdtShapeRegistry registry,
        IEnumerable<IConfigureOptions<CrdtShapeRegistryStartupMarker>> registrations) : IHostedService
    {
        /// <inheritdoc />
        public Task StartAsync(CancellationToken cancellationToken)
        {
            foreach (var entry in registrations)
            {
                if (entry is ConfigureCrdtShape c)
                {
                    registry.Register(c.TreeId, c.Shape);
                }
            }
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    /// <summary>
    /// In-library logging seam that demotes a narrowly-scoped set of Orleans
    /// transport tear-down warnings while the host is shutting down, then
    /// restores their normal Warning visibility once the host is healthy
    /// again. Registered by <see cref="AddLattice"/> as a dynamic
    /// <see cref="LoggerFilterRule"/> per managed category.
    /// <para>
    /// <b>Why a logger filter and not an <c>IGrainCallFilter</c>.</b> The
    /// warnings this seam targets ("the silo is blocking application
    /// messages" from <c>Orleans.Messaging</c> and "Unable to create local
    /// activation" from the placement service) originate inside the Orleans
    /// runtime's own logging, not inside the grain-call pipeline, so an
    /// <c>IGrainCallFilter</c> can never intercept them. A Microsoft.Extensions.Logging
    /// <see cref="LoggerFilterRule"/> gated on
    /// <see cref="IHostApplicationLifetime.ApplicationStopping"/> is the
    /// mechanism that actually sees those records.
    /// </para>
    /// <para>
    /// <b>Conservative scoping.</b> The filter only ever affects the two
    /// categories in <see cref="DemotedCategories"/> (matched by prefix), it
    /// only suppresses the <see cref="LogLevel.Warning"/> level, and it only
    /// suppresses while the host is stopping. On a healthy host the
    /// categories keep their Warning floor (the registration sets the rule's
    /// floor to <see cref="LogLevel.Warning"/>) so the same warnings stay
    /// visible; <see cref="LogLevel.Error"/> and above always survive, even
    /// during shutdown, so a genuine transport fault is never hidden.
    /// </para>
    /// <para>
    /// The lifetime is resolved lazily on first log-time (never at
    /// logging-options build time, which runs before
    /// <see cref="IHostApplicationLifetime"/> is usable) and cached, mirroring
    /// the lazy-resolve pattern the lattice write grains use for their own
    /// shutdown fast-fail guard.
    /// </para>
    /// </summary>
    internal sealed class LatticeShutdownLogFilter(IServiceProvider services)
    {
        /// <summary>
        /// The Orleans logger categories whose Warning-level tear-down
        /// chatter is suppressed while the host is shutting down. Matched by
        /// prefix so nested sub-categories are covered. Kept in sync with the
        /// benchmark-side static log filter that performs the same demotion
        /// unconditionally for the throughput harness.
        /// </summary>
        public static readonly string[] DemotedCategories =
        [
            "Orleans.Messaging",
            "Orleans.Runtime.Placement.PlacementService",
        ];

        private IHostApplicationLifetime? _lifetime;
        private bool _lifetimeResolved;

        private bool IsApplicationStopping
        {
            get
            {
                if (!_lifetimeResolved)
                {
                    _lifetimeResolved = true;
                    _lifetime = services.GetService<IHostApplicationLifetime>();
                }
                return _lifetime is not null && _lifetime.ApplicationStopping.IsCancellationRequested;
            }
        }

        /// <summary>
        /// Logger-filter predicate: returns whether a record for
        /// <paramref name="category"/> at <paramref name="level"/> should be
        /// emitted given the host's current lifetime state.
        /// </summary>
        /// <param name="category">The log record's category name.</param>
        /// <param name="level">The log record's level.</param>
        /// <returns><see langword="true"/> to emit the record; <see langword="false"/> to suppress it.</returns>
        public bool ShouldEmit(string? category, LogLevel level)
            => ShouldEmit(category, level, IsApplicationStopping);

        /// <summary>
        /// Pure demotion policy. For a managed category (see
        /// <see cref="IsDemotedCategory"/>) the floor is
        /// <see cref="LogLevel.Warning"/> (Trace/Debug/Information are
        /// suppressed, matching the healthy-host Warning floor), the
        /// Warning level is suppressed only while
        /// <paramref name="applicationStopping"/> is <see langword="true"/>,
        /// and Error/Critical always survive. Non-managed categories are
        /// always emitted (the rule is only ever installed for managed
        /// categories, so this is a defensive default).
        /// </summary>
        /// <param name="category">The log record's category name.</param>
        /// <param name="level">The log record's level.</param>
        /// <param name="applicationStopping">Whether the host has begun shutting down.</param>
        /// <returns><see langword="true"/> to emit the record; <see langword="false"/> to suppress it.</returns>
        public static bool ShouldEmit(string? category, LogLevel level, bool applicationStopping)
        {
            if (!IsDemotedCategory(category)) return true;
            if (level < LogLevel.Warning) return false;
            if (level == LogLevel.Warning && applicationStopping) return false;
            return true;
        }

        /// <summary>
        /// Returns whether <paramref name="category"/> is one of the managed
        /// Orleans transport tear-down categories (prefix match against
        /// <see cref="DemotedCategories"/>).
        /// </summary>
        /// <param name="category">The log record's category name.</param>
        /// <returns><see langword="true"/> when the category is managed by this filter.</returns>
        public static bool IsDemotedCategory(string? category)
        {
            if (string.IsNullOrEmpty(category)) return false;
            foreach (var demoted in DemotedCategories)
            {
                if (category.StartsWith(demoted, StringComparison.Ordinal)) return true;
            }
            return false;
        }
    }
}
