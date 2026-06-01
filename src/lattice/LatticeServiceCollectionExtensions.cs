using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
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
        builder.Services.AddSingleton<LatticeOptionsResolver>();
        builder.Services.AddSingleton<MutationObserverDispatcher>();
        builder.Services.AddSingleton<ILatticeFallOffLogDetector, LatticeFallOffLogDetector>();

        // Storage-usage observable-gauge sink. Constructing the singleton
        // registers the byte-accurate storage gauges on the shared meter
        // (idempotent, process-wide); the per-tree aggregator pushes the
        // latest report here so a meter scrape never fans out to grains.
        builder.Services.AddSingleton<LatticeStorageUsageMetrics>();

        // Core WAL durability seams: in-memory provider as the singleton
        // default (hosts replace via AddWalStorage), commit-log writer
        // and reader, and a null-returning mode resolver. The replication
        // package replaces the resolver via services.Replace(...) so per-
        // tree mode resolution kicks in only when replication is added.
        builder.AddWalStorage();
        builder.Services.TryAddSingleton<ICommitLogWriter, WalCommitLogWriter>();
        builder.Services.TryAddSingleton<ICommitLogReader, WalCommitLogReader>();
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
    /// Registers the WAL consumer-cursor registry on the silo. Single-cluster
    /// deployments need the registry so the leaf-as-materialiser cursor
    /// can pin the per-shard WAL GC against the leaf's durably-applied
    /// frontier. The default <see cref="InMemoryWalCursorRegistry"/> is process-local;
    /// after a silo restart every consumer must re-report its cursor before
    /// the GC predicate can trim past it (the fall-off-log seam handles
    /// that recovery). Idempotent: a host-supplied registration via
    /// <paramref name="factory"/> takes precedence and a second call is a
    /// no-op.
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
            builder.Services.TryAddSingleton<IWalCursorRegistry>(factory);
        }

        // Opting into the cursor registry implies opting into
        // leaf-as-materialiser cursor reporting: register the leaf-facing
        // reporter so BPlusLeafGrain.CursorRegistry pulls a non-null
        // ILeafCursorReporter from DI and starts pinning the WAL GC
        // against its applied frontier. Hosts that do not call
        // AddWalCursorRegistry leave both registrations absent and the
        // leaf grain no-ops on the report path.
        builder.Services.TryAddSingleton<ILeafCursorReporter, LeafCursorReporter>();
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
    /// <see cref="LwwValue{T}.Timestamp"/> and collapses through the
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
}
