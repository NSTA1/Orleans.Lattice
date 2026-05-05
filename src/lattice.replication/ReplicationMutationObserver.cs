using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IMutationObserver"/> registered by the replication package.
/// Captures every locally-originating mutation at commit time, builds a
/// fully-formed <see cref="ReplogEntry"/> (op + key + value + HLC + origin
/// + TTL + declared <see cref="ReplicationMode"/>), and forwards it to the
/// registered <see cref="IReplogSink"/> before the originating grain's
/// write returns. Replaces the host-level outgoing-call filter used by the
/// legacy <c>MultiSiteManufacturing</c> sample - capture is now atomic
/// with the write.
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
/// resolves the declared <see cref="ReplicationMode"/> through the
/// registered <see cref="IReplicationModeResolver"/> and short-circuits
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
    private readonly IReplicationModeResolver _modeResolver;
    private readonly LocalVectorClockCache _localVectorClockCache;
    private readonly ConcurrentDictionary<string, CompiledFilter> _filters = new(StringComparer.Ordinal);
    private readonly Func<string, CompiledFilter> _factory;
    private readonly IDisposable? _changeSubscription;

    public ReplicationMutationObserver(
        IReplogSink sink,
        IOptionsMonitor<LatticeReplicationOptions> options,
        IReplicationModeResolver modeResolver,
        LocalVectorClockCache localVectorClockCache)
    {
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(modeResolver);
        ArgumentNullException.ThrowIfNull(localVectorClockCache);
        _sink = sink;
        _options = options;
        _modeResolver = modeResolver;
        _localVectorClockCache = localVectorClockCache;
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
        var op = mutation.Kind switch
        {
            MutationKind.Set => ReplogOp.Set,
            MutationKind.Delete => ReplogOp.Delete,
            MutationKind.DeleteRange => ReplogOp.DeleteRange,
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

        // The mutation already carries an origin when it is a replay of a
        // remote write; otherwise stamp the validated local cluster id.
        // LatticeReplicationOptionsValidator guarantees ClusterId is non-empty
        // before any observer call can reach this point.
        var origin = mutation.OriginClusterId ?? filter.ClusterId;

        // Vector-clock capture priority:
        //   1. mutation.VectorClock when supplied via
        //      LatticeVectorClockContext.With(...) — preserves the
        //      caller-supplied frontier verbatim. This is the path
        //      structural rewrites (shard-split shadow-forward, saga
        //      compensate, atomic multi-key writes) take so the
        //      shadow-forwarded entry inherits the originating commit's
        //      VC rather than capturing a fresh one against the
        //      destination shard's local view.
        //   2. LocalVectorClockCache snapshot when the mutation does
        //      not carry an explicit VC. Multi-shard user writes
        //      (range delete, multi-leaf saga) emit from multiple
        //      grains in close succession; reading the cache on each
        //      emit yields a silo-wide consistent view of the local
        //      vector clock so every per-emit VC agrees on cross-shard
        //      origins. The cache cold-starts from
        //      IReplicationHighWaterMarkGrain.GetVectorAsync once per
        //      tree per silo lifetime.
        // Defensive snapshot (case 1): VersionVector is a mutable
        // reference type whose Entries dictionary is publicly mutable,
        // so retaining the supplied mutation.VectorClock reference
        // would leave the captured entry exposed to any caller that
        // advances the frontier after this observer returns. The
        // cache's GetSnapshotAsync already returns a defensive copy
        // (no further clone needed for case 2).
        VersionVector? capturedFrontier = mutation.VectorClock?.Clone();
        if (capturedFrontier is null)
        {
            capturedFrontier = await _localVectorClockCache
                .GetSnapshotAsync(mutation.TreeId, cancellationToken)
                .ConfigureAwait(false);
        }

        var entry = new ReplogEntry
        {
            TreeId = mutation.TreeId,
            Op = op,
            Key = key,
            EndExclusiveKey = mutation.EndExclusiveKey,
            Value = mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = origin,
            Mode = mode.Value,
            // Causal-plus frontier: stamp the captured snapshot (either
            // the caller-supplied clone or the cache's defensive copy)
            // so the entry is detached from any post-emit mutation of
            // the producer's frontier source. Both slots share the
            // single instance — the dependency summary slot is
            // reserved for a future Bloom-filter shape but starts
            // identical to the absolute frontier so a receiver
            // consulting either slot sees the same value.
            VectorClock = capturedFrontier,
            DependencySummary = capturedFrontier,
            // Pre-merge typed delta passthrough: the producer-side
            // accessors (OR-Set / PN-Counter / version-vector) and any
            // caller that opted in via LatticeDeltaContext stamped these
            // slots on the originating LatticeMutation. Forwarding them
            // verbatim lets receivers replay the author's intent rather
            // than the post-merge state. Both fields decode as null on
            // legacy peers and on plain Set/Delete writes that did not
            // author a delta.
            DeltaKind = mutation.DeltaKind,
            DeltaPayload = mutation.DeltaPayload,
        };

        await _sink.WriteAsync(entry, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose() => _changeSubscription?.Dispose();

    /// <summary>
    /// Immutable, per-tree-id snapshot of the producer-side per-key
    /// filter. Built once per tree id on first use and cached on the
    /// observer; invalidated whenever
    /// <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
    /// fires. Tree-level opt-in is decided separately by the
    /// <see cref="IReplicationModeResolver"/>.
    /// </summary>
    private sealed class CompiledFilter
    {
        private readonly string[]? _prefixes;
        private readonly Func<string, bool>? _keyFilter;

        private CompiledFilter(
            string clusterId,
            string[]? prefixes,
            Func<string, bool>? keyFilter)
        {
            ClusterId = clusterId;
            _prefixes = prefixes;
            _keyFilter = keyFilter;
        }

        /// <summary>Snapshot of <see cref="LatticeReplicationOptions.ClusterId"/>.</summary>
        public string ClusterId { get; }

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
                options.KeyFilter);
        }
    }
}

