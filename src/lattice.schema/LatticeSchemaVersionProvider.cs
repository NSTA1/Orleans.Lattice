using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaVersionProvider"/>. Caches one
/// <see cref="LatticeSchemaVersionConfig"/> (or a <c>null</c> sentinel for an
/// unversioned tree) per governed tree so per-write / per-read resolution is a
/// dictionary lookup. Also an <see cref="IMutationObserver"/>: a write to the
/// reserved <c>sys-schema-version</c> tree evicts the affected tree's cache entry,
/// so a config change is picked up on the next write / read without a restart.
/// Mirrors <c>LatticeSchemaPolicyProvider</c>.
/// </summary>
internal sealed class LatticeSchemaVersionProvider : ILatticeSchemaVersionProvider, IMutationObserver
{
    private readonly ILatticeSchemaVersionStore _store;
    private readonly ConcurrentDictionary<string, LatticeSchemaVersionConfig?> _cache = new(StringComparer.Ordinal);

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

        return new ValueTask<LatticeSchemaVersionConfig?>(LoadAsync(treeId, cancellationToken));
    }

    private async Task<LatticeSchemaVersionConfig?> LoadAsync(string treeId, CancellationToken cancellationToken)
    {
        var config = await _store.GetConfigAsync(treeId, cancellationToken).ConfigureAwait(false);

        // Last write wins on a concurrent miss; the load is idempotent.
        _cache[treeId] = config;
        return config;
    }

    /// <inheritdoc />
    public void Invalidate(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        _cache.TryRemove(treeId, out _);
    }

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        // Only version-tree writes are relevant; every other tree fast-paths out so
        // the observer stays cheap on the general write path. The config is keyed by
        // governed tree id, so the mutation key is the tree to evict.
        if (string.Equals(mutation.TreeId, SchemaConstants.VersionConfigTree, StringComparison.Ordinal)
            && !string.IsNullOrEmpty(mutation.Key))
        {
            _cache.TryRemove(mutation.Key, out _);
        }

        return Task.CompletedTask;
    }
}
