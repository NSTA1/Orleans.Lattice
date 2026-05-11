using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Replication-package <see cref="ILatticeOriginClusterIdResolver"/>
/// implementation backed by <see cref="LatticeReplicationOptions.ClusterId"/>.
/// Per-tree-id resolution outcomes are cached and invalidated on
/// <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
/// so the commit-time hot path is a single dictionary read. The configured
/// cluster id is the local cluster's stable identifier; the WAL writer
/// stamps it onto every locally-authored record so multi-site receivers
/// can attribute origin and break replication cycles.
/// </summary>
internal sealed class ConfiguredLatticeOriginClusterIdResolver : ILatticeOriginClusterIdResolver, IDisposable
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _options;
    private readonly ConcurrentDictionary<string, string> _cache = new(StringComparer.Ordinal);
    private readonly Func<string, string> _factory;
    private readonly IDisposable? _changeSubscription;

    public ConfiguredLatticeOriginClusterIdResolver(IOptionsMonitor<LatticeReplicationOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _factory = treeId => _options.Get(treeId).ClusterId ?? string.Empty;
        _changeSubscription = options.OnChange((_, _) => _cache.Clear());
    }

    /// <inheritdoc />
    public string Resolve(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _cache.GetOrAdd(treeId, _factory);
    }

    /// <inheritdoc />
    public void Dispose() => _changeSubscription?.Dispose();
}
