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
        return _cache.GetOrAdd(treeId, _factory);
    }

    /// <inheritdoc />
    public void Dispose() => _changeSubscription?.Dispose();
}
