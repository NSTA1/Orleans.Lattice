using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaPolicyProvider"/>. Caches one
/// <see cref="CompiledSchemaPolicy"/> per governed tree (and a <c>null</c> sentinel
/// for a tree known to have no policy) so per-write resolution is a dictionary
/// lookup. Also an <see cref="IMutationObserver"/>: a write to the reserved
/// <c>sys-schema-policy</c> tree evicts the affected tree's cache entry on the silo
/// that committed it. Every other silo drops its cache within
/// <see cref="LatticeSchemaEnforcementOptions.PropagationInterval"/> of the change,
/// through <see cref="SchemaConfigEpochSynchronizer{TCache}"/>, so a policy change is
/// picked up cluster-wide without a restart.
/// </summary>
internal sealed class LatticeSchemaPolicyProvider : ILatticeSchemaPolicyProvider, IMutationObserver, ISchemaConfigCache
{
    private readonly ILatticeSchemaPolicyStore _store;
    private readonly ConcurrentDictionary<string, CompiledSchemaPolicy?> _cache = new(StringComparer.Ordinal);
    private readonly SchemaTreeCacheGenerations _generations = new();

    /// <summary>
    /// The number of per-tree entries currently cached. Exposed for unit testing
    /// that the cache honours <see cref="SchemaTreeCacheExtensions.MaxCachedTrees"/>.
    /// </summary>
    internal int CachedTreeCount => _cache.Count;

    /// <summary>Initializes a new <see cref="LatticeSchemaPolicyProvider"/>.</summary>
    /// <param name="store">The durable policy store to load misses from.</param>
    /// <param name="options">The enforcement options carrying the global strict flag.</param>
    /// <exception cref="ArgumentNullException"><paramref name="store"/> or <paramref name="options"/> is <c>null</c>.</exception>
    public LatticeSchemaPolicyProvider(
        ILatticeSchemaPolicyStore store,
        IOptions<LatticeSchemaEnforcementOptions> options)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(options);
        _store = store;
        StrictIngestEnabled = options.Value.StrictIngest;
    }

    /// <inheritdoc />
    public bool StrictIngestEnabled { get; }

    /// <inheritdoc />
    public ValueTask<CompiledSchemaPolicy?> GetCompiledPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // The reserved trees are never governed - they hold enforcement state
        // itself - so short-circuit without a store round-trip and without
        // caching a sentinel that a policy mutation would have to evict.
        if (treeId.StartsWith(SchemaConstants.ReservedTreePrefix, StringComparison.Ordinal))
        {
            return new ValueTask<CompiledSchemaPolicy?>((CompiledSchemaPolicy?)null);
        }

        if (_cache.TryGetValue(treeId, out var cached))
        {
            return new ValueTask<CompiledSchemaPolicy?>(cached);
        }

        return new ValueTask<CompiledSchemaPolicy?>(LoadAndCacheAsync(treeId, cancellationToken));
    }

    private async Task<CompiledSchemaPolicy?> LoadAndCacheAsync(string treeId, CancellationToken cancellationToken)
    {
        // Captured before the read: an invalidation after this point overtakes the
        // load, which must then not install its possibly pre-change answer.
        var generation = _generations.Capture(treeId);
        var policy = await _store.GetPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
        var compiled = policy is null ? null : CompiledSchemaPolicy.Compile(policy);

        _cache.CacheIfCurrent(_generations, treeId, generation, compiled);

        return compiled;
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
        _cache.InvalidateForMutation(_generations, mutation, SchemaConstants.PolicyTree);
        return Task.CompletedTask;
    }
}
