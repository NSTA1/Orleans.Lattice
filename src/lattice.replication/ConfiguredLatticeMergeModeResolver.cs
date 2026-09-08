using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeMergeModeResolver"/> implementation backed by
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/>. Per-tree-id
/// resolution outcomes are cached and invalidated on
/// <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
/// so the commit-time hot path is a single dictionary read.
/// </summary>
internal sealed class ConfiguredLatticeMergeModeResolver : ILatticeMergeModeResolver, IDisposable
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options;
    private readonly ConcurrentDictionary<string, LatticeMergeMode?> _cache = new(StringComparer.Ordinal);
    private readonly Func<string, LatticeMergeMode?> _factory;
    private readonly IDisposable? _changeSubscription;

    /// <summary>
    /// The maximum number of per-tree entries retained. The cache key is a tree id
    /// the caller supplies, and an entry is added for every tree id resolved -
    /// including unreplicated ids, which cache a null sentinel - so an unbounded
    /// map grew silo memory without limit as distinct tree ids accumulated
    /// (CWE-770). The bound is safe because a miss simply re-reads the options, so
    /// refusing an insert costs a dictionary lookup and never changes the answer.
    /// </summary>
    internal const int MaxCachedTrees = 4096;

    /// <summary>
    /// The number of per-tree entries currently cached. Exposed for unit testing
    /// that the cache honours <see cref="MaxCachedTrees"/>.
    /// </summary>
    internal int CachedTreeCount => _cache.Count;

    public ConfiguredLatticeMergeModeResolver(IOptionsMonitor<LatticeReplicationOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _factory = treeId =>
        {
            var trees = _options.Get(treeId).ReplicatedTrees;
            if (trees is null)
            {
                return null;
            }

            return trees.TryGetValue(treeId, out var mode) ? mode : null;
        };
        _changeSubscription = options.OnChange((_, _) => _cache.Clear());
    }

    /// <inheritdoc />
    public LatticeMergeMode? Resolve(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // The hot path stays a single dictionary read; only a miss consults the
        // cap, and only a brand-new key is refused there.
        if (_cache.TryGetValue(treeId, out var cached))
        {
            return cached;
        }

        var resolved = _factory(treeId);
        if (_cache.Count < MaxCachedTrees)
        {
            _cache.TryAdd(treeId, resolved);
        }

        return resolved;
    }

    /// <inheritdoc />
    public void Dispose() => _changeSubscription?.Dispose();
}
