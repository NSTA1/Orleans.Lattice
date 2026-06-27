using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IMutationObserver"/> registered by the replication package.
/// Observes every locally-originating mutation at commit time and, when
/// the tree is declared for replication and the mutation passes the
/// per-key filters, nudges the registered <see cref="IReplogSink"/> so
/// the background log-tailing shipper for that tree pumps immediately.
/// The durable change-feed record is written separately by the
/// foreground leaf commit-log writer in the core assembly; this observer
/// no longer builds a <c>WalRecord</c> or captures any vector clock.
/// </summary>
/// <remarks>
/// <para>
/// The observer fires on the grain's scheduler. The sink call is
/// awaited inline, so any latency in <see cref="IReplogSink.WriteAsync"/>
/// is added to the caller's write latency. The default no-op sink is
/// O(1).
/// </para>
/// <para>
/// <see cref="LatticeMutation.OriginClusterId"/> is preserved verbatim
/// when the mutation already carries an origin (i.e. it is a replay of a
/// remote write). When the mutation is local-origin (<c>OriginClusterId</c>
/// is <c>null</c>), the observer stamps the configured local
/// <see cref="LatticeReplicationOptions.ClusterId"/>; the registered
/// <c>LatticeReplicationOptionsValidator</c> guarantees that value is
/// non-empty before any observer call can reach this code path.
/// </para>
/// <para>
/// Trees opt in to replication explicitly via
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/>; the observer
/// resolves the declared <see cref="LatticeMergeMode"/> through the
/// registered <see cref="ILatticeMergeModeResolver"/> and short-circuits
/// before the sink is touched when the resolver returns <c>null</c>. The
/// per-key filters (<see cref="LatticeReplicationOptions.KeyFilter"/>, 
/// <see cref="LatticeReplicationOptions.KeyPrefixes"/>) layer on top of
/// the mode resolution. Per-tree configured option instances are resolved
/// via <see cref="IOptionsMonitor{TOptions}.Get(string)"/> using the
/// mutation's tree id, so a host can override filters per tree using
/// <see cref="LatticeReplicationServiceCollectionExtensions.ConfigureLatticeReplication(ISiloBuilder, string, Action{LatticeReplicationOptions})"/>.
/// </para>
/// <para>
/// To keep the commit-time hot path tight, the observer compiles each
/// resolved <see cref="LatticeReplicationOptions"/> instance into an
/// immutable <see cref="CompiledFilter"/> snapshot (snapshotted prefix
/// array, key predicate, cluster id) on first use of a tree id and caches
/// it in a <see cref="ConcurrentDictionary{TKey, TValue}"/>. The
/// per-mutation path is then a resolver lookup, a dictionary read, a bool,
/// and at most one delegate / linear prefix check. The cache is invalidated
/// via <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
/// so a host that reconfigures filters at runtime sees the new values
/// on the next mutation.
/// </para>
/// </remarks>
internal sealed class ReplicationMutationObserver : IMutationObserver, IDisposable
{
    private readonly IReplogSink _sink;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options;
    private readonly ILatticeMergeModeResolver _modeResolver;
    private readonly ILatticeCompressionDictionarySampler? _dictionarySampler;
    private readonly ConcurrentDictionary<string, CompiledFilter> _filters = new(StringComparer.Ordinal);
    private readonly Func<string, CompiledFilter> _factory;
    private readonly IDisposable? _changeSubscription;

    public ReplicationMutationObserver(
        IReplogSink sink,
        IOptionsMonitor<LatticeReplicationOptions> options,
        ILatticeMergeModeResolver modeResolver,
        ILatticeCompressionDictionaryProvider? dictionaryProvider = null)
    {
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(modeResolver);
        _sink = sink;
        _options = options;
        _modeResolver = modeResolver;
        // The injected shared-dictionary provider doubles as the training
        // sampler when it trains at runtime (the auto-trainer). The default
        // operator-supplied provider does not implement the sampler, so the
        // capture path samples nothing unless the host opted into the
        // auto-distributing dictionary.
        _dictionarySampler = dictionaryProvider as ILatticeCompressionDictionarySampler;
        _factory = treeId => CompiledFilter.From(_options.Get(treeId));

        // Any options change invalidates every cached filter. ConfigureAll
        // registrations fan out to every named instance, so coarse-grained
        // invalidation is the correct semantics: the next mutation per tree
        // re-resolves and re-compiles.
        _changeSubscription = options.OnChange((_, _) => _filters.Clear());
    }

    /// <inheritdoc />
    public async Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        // Tombstone-reap envelopes (`MutationKind.Tombstone`) are local
        // structural cleanup records emitted by
        // `BPlusLeafGrain.CompactTombstonesAsync`. They are filtered
        // out at the producer-side `ReplicationShipperGrain.ShouldShip`
        // and `ChangeFeed.Subscribe` boundaries, and have no defined
        // observer dispatch path (the leaf write path does not invoke
        // `IMutationObserver` for compaction). The defence-in-depth
        // short-circuit here keeps a future emit path that routes a
        // tombstone-reap through the observer from faulting the switch
        // below, which only enumerates `Set` / `Delete` / `DeleteRange`.
        if (mutation.Kind == MutationKind.Tombstone)
        {
            return;
        }

        var op = mutation.Kind switch
        {
            MutationKind.Set => MutationKind.Set,
            MutationKind.Delete => MutationKind.Delete,
            MutationKind.DeleteRange => MutationKind.DeleteRange,
            _ => throw new InvalidOperationException(
                $"Unknown mutation kind: {mutation.Kind}"),
        };

        // Maintenance-category mutations (resize / rebalance / compaction /
        // internal structural rewrite) are stamped with
        // <see cref="MutationCategory.Maintenance"/> by the producer site
        // via <c>LatticeMaintenanceContext</c>. They are structural rewrites
        // of state already authored by user writes, not semantic causal
        // events; replicating them would (a) inflate every peer's vector
        // clock with edges the writer never authored, (b) pollute the
        // dependency graph with non-user-authored edges, and (c) generate
        // wire traffic for events that converged peers will run
        // independently against their own copy of the data. Skip the WAL
        // append entirely - no entry is constructed, no filter runs, no
        // resolver is consulted. The classification is independent of
        // <see cref="LatticeMutation.OriginClusterId"/>: a remote-origin
        // maintenance emit is still Maintenance and still skipped.
        if (mutation.Category == MutationCategory.Maintenance)
        {
            return;
        }

        // Mode resolution is the gate: an undeclared tree never replicates.
        var mode = _modeResolver.Resolve(mutation.TreeId);
        if (mode is null)
        {
            return;
        }

        var filter = _filters.GetOrAdd(mutation.TreeId, _factory);

        var key = mutation.Key ?? string.Empty;
        if (!filter.AcceptsKey(key))
        {
            return;
        }

        // Auto-trained shared-dictionary sampling (opt-in): feed the
        // committed value bytes into the training reservoir so the
        // auto-trainer builds a dictionary representative of the very
        // traffic that will be shipped, with no host wiring. Gated by the
        // per-tree AutoSharedDictionaryEnabled switch snapshotted on the
        // compiled filter, so the default-off build samples nothing. Only
        // Set carries a value worth sampling; Delete / DeleteRange do not.
        if (filter.SampleForTraining
            && _dictionarySampler is { } sampler
            && op == MutationKind.Set
            && mutation.Value is { Length: > 0 } valueBytes)
        {
            sampler.Observe(valueBytes);
        }

        // The tree is declared, the key passed the filters, and the
        // mutation is not maintenance: nudge the registered sink so the
        // background log-tailing shipper for this tree pumps immediately.
        // The durable change-feed record was already written to the leaf
        // WAL by the foreground commit-log writer; nothing is built here.
        await _sink.WriteAsync(mutation.TreeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose() => _changeSubscription?.Dispose();

    /// <summary>
    /// Immutable, per-tree-id snapshot of the producer-side per-key
    /// filter. Built once per tree id on first use and cached on the
    /// observer; invalidated whenever
    /// <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
    /// fires. Tree-level opt-in is decided separately by the
    /// <see cref="ILatticeMergeModeResolver"/>.
    /// </summary>
    private sealed class CompiledFilter
    {
        private readonly string[]? _prefixes;
        private readonly Func<string, bool>? _keyFilter;

        private CompiledFilter(
            string clusterId,
            string[]? prefixes,
            Func<string, bool>? keyFilter,
            bool sampleForTraining)
        {
            ClusterId = clusterId;
            _prefixes = prefixes;
            _keyFilter = keyFilter;
            SampleForTraining = sampleForTraining;
        }

        /// <summary>Snapshot of <see cref="LatticeReplicationOptions.ClusterId"/>.</summary>
        public string ClusterId { get; }

        /// <summary>
        /// Snapshot of <see cref="LatticeReplicationOptions.AutoSharedDictionaryEnabled"/>:
        /// whether committed values for this tree are sampled into the
        /// auto-trained shared-dictionary reservoir.
        /// </summary>
        public bool SampleForTraining { get; }

        /// <summary>
        /// Returns <c>true</c> when the supplied key passes the
        /// per-key filters (predicate + prefix allowlist).
        /// </summary>
        public bool AcceptsKey(string key)
        {
            if (_keyFilter is { } predicate && !predicate(key))
            {
                return false;
            }

            var prefixes = _prefixes;
            if (prefixes is null)
            {
                return true;
            }

            for (var i = 0; i < prefixes.Length; i++)
            {
                if (key.StartsWith(prefixes[i], StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        public static CompiledFilter From(LatticeReplicationOptions options)
        {
            // Snapshot the prefix collection into a non-null array only
            // when at least one usable (non-null) prefix is configured;
            // null means "no prefix restriction" so the hot path can
            // short-circuit on a reference check.
            string[]? prefixes = null;
            if (options.KeyPrefixes is { Count: > 0 } source)
            {
                var buffer = new List<string>(source.Count);
                foreach (var prefix in source)
                {
                    if (prefix is not null)
                    {
                        buffer.Add(prefix);
                    }
                }

                if (buffer.Count > 0)
                {
                    prefixes = buffer.ToArray();
                }
            }

            return new CompiledFilter(
                options.ClusterId,
                prefixes,
                options.KeyFilter,
                options.AutoSharedDictionaryEnabled);
        }
    }
}

