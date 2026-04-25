using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="IMutationObserver"/> registered by the replication package.
/// Captures every locally-originating mutation at commit time, builds a
/// fully-formed <see cref="ReplogEntry"/> (op + key + value + HLC + origin
/// + TTL), and forwards it to the registered <see cref="IReplogSink"/>
/// before the originating grain''s write returns. Replaces the host-level
/// outgoing-call filter used by the legacy <c>MultiSiteManufacturing</c>
/// sample - capture is now atomic with the write.
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
/// Producer-side filters (<see cref="LatticeReplicationOptions.ReplicatedTrees"/>, 
/// <see cref="LatticeReplicationOptions.KeyFilter"/>, 
/// <see cref="LatticeReplicationOptions.KeyPrefixes"/>) are evaluated
/// before the sink is invoked so non-replicated mutations never reach
/// the WAL. Per-tree configured instances are resolved via
/// <see cref="IOptionsMonitor{TOptions}.Get(string)"/> using the
/// mutation's tree id, so a host can override filters per tree using
/// <see cref="LatticeReplicationServiceCollectionExtensions.ConfigureLatticeReplication(ISiloBuilder, string, Action{LatticeReplicationOptions})"/>.
/// </para>
/// <para>
/// To keep the commit-time hot path tight, the observer compiles each
/// resolved <see cref="LatticeReplicationOptions"/> instance into an
/// immutable <see cref="CompiledFilter"/> snapshot (tree-allowlist
/// outcome, snapshotted prefix array, key predicate, cluster id) on
/// first use of a tree id and caches it in a
/// <see cref="ConcurrentDictionary{TKey, TValue}"/>. The per-mutation
/// path is then a single dictionary read, a bool, and at most one
/// delegate / linear prefix check. The cache is invalidated via
/// <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
/// so a host that reconfigures filters at runtime sees the new values
/// on the next mutation.
/// </para>
/// </remarks>
internal sealed class ReplicationMutationObserver : IMutationObserver, IDisposable
{
    private readonly IReplogSink _sink;
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options;
    private readonly ConcurrentDictionary<string, CompiledFilter> _filters = new(StringComparer.Ordinal);
    private readonly Func<string, CompiledFilter> _factory;
    private readonly IDisposable? _changeSubscription;

    public ReplicationMutationObserver(
        IReplogSink sink,
        IOptionsMonitor<LatticeReplicationOptions> options)
    {
        _sink = sink;
        _options = options;
        _factory = treeId => CompiledFilter.From(treeId, _options.Get(treeId));

        // Any options change invalidates every cached filter. ConfigureAll
        // registrations fan out to every named instance, so coarse-grained
        // invalidation is the correct semantics: the next mutation per tree
        // re-resolves and re-compiles.
        _changeSubscription = options.OnChange((_, _) => _filters.Clear());
    }

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        var op = mutation.Kind switch
        {
            MutationKind.Set => ReplogOp.Set,
            MutationKind.Delete => ReplogOp.Delete,
            MutationKind.DeleteRange => ReplogOp.DeleteRange,
            _ => throw new InvalidOperationException(
                $"Unknown mutation kind: {mutation.Kind}"),
        };

        var filter = _filters.GetOrAdd(mutation.TreeId, _factory);

        // Tree allowlist outcome was decided once at compile time for this
        // tree id; the per-mutation path is a single bool check.
        if (!filter.IncludeTree)
        {
            return Task.CompletedTask;
        }

        var key = mutation.Key ?? string.Empty;
        if (!filter.AcceptsKey(key))
        {
            return Task.CompletedTask;
        }

        // The mutation already carries an origin when it is a replay of a
        // remote write; otherwise stamp the validated local cluster id.
        // LatticeReplicationOptionsValidator guarantees ClusterId is non-empty
        // before any observer call can reach this point.
        var origin = mutation.OriginClusterId ?? filter.ClusterId;

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
        };

        return _sink.WriteAsync(entry, cancellationToken);
    }

    /// <inheritdoc />
    public void Dispose() => _changeSubscription?.Dispose();

    /// <summary>
    /// Immutable, per-tree-id snapshot of the producer-side replication
    /// filter. Built once per tree id on first use and cached on the
    /// observer; invalidated whenever
    /// <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
    /// fires.
    /// </summary>
    private sealed class CompiledFilter
    {
        private readonly string[]? _prefixes;
        private readonly Func<string, bool>? _keyFilter;

        private CompiledFilter(
            bool includeTree,
            string clusterId,
            string[]? prefixes,
            Func<string, bool>? keyFilter)
        {
            IncludeTree = includeTree;
            ClusterId = clusterId;
            _prefixes = prefixes;
            _keyFilter = keyFilter;
        }

        /// <summary>
        /// <c>true</c> when the tree id this filter was built for passes
        /// the configured tree allowlist. Decided once at compile time.
        /// </summary>
        public bool IncludeTree { get; }

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

        public static CompiledFilter From(string treeId, LatticeReplicationOptions options)
        {
            // Tree allowlist: null means "all trees"; empty means "no
            // trees"; non-empty restricts to the listed trees. The
            // outcome for this specific tree id is captured here so the
            // hot path collapses to a single bool read.
            var trees = options.ReplicatedTrees;
            var includeTree = trees is null || trees.Contains(treeId);

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
                includeTree,
                options.ClusterId,
                prefixes,
                options.KeyFilter);
        }
    }
}

