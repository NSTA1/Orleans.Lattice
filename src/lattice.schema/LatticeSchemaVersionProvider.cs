using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaVersionProvider"/>. Caches one
/// <see cref="LatticeSchemaVersionConfig"/> (or a <c>null</c> sentinel for an
/// unversioned tree) per governed tree so per-write / per-read resolution is a
/// dictionary lookup. Also an <see cref="IMutationObserver"/>: a write to the
/// reserved <c>sys-schema-version</c> tree evicts the affected tree's cache entry on
/// the silo that committed it. Every other silo drops its cache within
/// <see cref="LatticeSchemaVersioningOptions.PropagationInterval"/> of the change,
/// through <see cref="SchemaConfigEpochSynchronizer{TCache}"/>, so a config change is
/// picked up cluster-wide without a restart.
/// Shares cache admission and invalidation rules with the policy provider.
/// </summary>
internal sealed class LatticeSchemaVersionProvider : ILatticeSchemaVersionProvider, IMutationObserver, ISchemaConfigCache
{
    private readonly ILatticeSchemaVersionStore _store;
    private readonly ConcurrentDictionary<string, LatticeSchemaVersionConfig?> _cache = new(StringComparer.Ordinal);
    private readonly SchemaTreeCacheGenerations _generations = new();

    /// <summary>
    /// The number of per-tree entries currently cached. Exposed for unit testing
    /// that the cache honours <see cref="SchemaTreeCacheExtensions.MaxCachedTrees"/>.
    /// </summary>
    internal int CachedTreeCount => _cache.Count;

    /// <summary>Initializes a new <see cref="LatticeSchemaVersionProvider"/>.</summary>
    /// <param name="store">The durable version-config store to load misses from.</param>
    /// <param name="options">The versioning options carrying the global strict flag.</param>
    /// <exception cref="ArgumentNullException"><paramref name="store"/> or <paramref name="options"/> is <c>null</c>.</exception>
    public LatticeSchemaVersionProvider(
        ILatticeSchemaVersionStore store,
        IOptions<LatticeSchemaVersioningOptions> options)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(options);
        _store = store;
        StrictIngestEnabled = options.Value.StrictIngest;
    }

    /// <inheritdoc />
    public bool StrictIngestEnabled { get; }

    /// <inheritdoc />
    public ValueTask<LatticeSchemaVersionConfig?> GetConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // The reserved trees are never versioned - they hold versioning state
        // itself - so short-circuit without a store round-trip.
        if (treeId.StartsWith(SchemaConstants.ReservedTreePrefix, StringComparison.Ordinal))
        {
            return new ValueTask<LatticeSchemaVersionConfig?>((LatticeSchemaVersionConfig?)null);
        }

        if (_cache.TryGetValue(treeId, out var cached))
        {
            return new ValueTask<LatticeSchemaVersionConfig?>(cached);
        }

        return new ValueTask<LatticeSchemaVersionConfig?>(LoadAndCacheAsync(treeId, cancellationToken));
    }

    private async Task<LatticeSchemaVersionConfig?> LoadAndCacheAsync(string treeId, CancellationToken cancellationToken)
    {
        // Captured before the read: an invalidation after this point overtakes the
        // load, which must then not install its possibly pre-change answer.
        var generation = _generations.Capture(treeId);
        var config = await _store.GetConfigAsync(treeId, cancellationToken).ConfigureAwait(false);

        _cache.CacheIfCurrent(_generations, treeId, generation, config);

        return config;
    }

    /// <inheritdoc />
    public void Invalidate(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        _cache.Invalidate(_generations, treeId);
    }

    /// <inheritdoc />
    public void InvalidateAll() => _cache.InvalidateAll(_generations);

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        _cache.InvalidateForMutation(_generations, mutation, SchemaConstants.VersionConfigTree);
        return Task.CompletedTask;
    }
}
