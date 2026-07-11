using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The default <see cref="ILatticeSchemaPolicyProvider"/>. Caches one
/// <see cref="CompiledSchemaPolicy"/> per governed tree (and a <c>null</c> sentinel
/// for a tree known to have no policy) so per-write resolution is a dictionary
/// lookup. Also an <see cref="IMutationObserver"/>: a write to the reserved
/// <c>sys-schema-policy</c> tree evicts the affected tree's cache entry, so a
/// policy change is picked up on the next write without a restart.
/// </summary>
internal sealed class LatticeSchemaPolicyProvider : ILatticeSchemaPolicyProvider, IMutationObserver
{
    private readonly ILatticeSchemaPolicyStore _store;
    private readonly ConcurrentDictionary<string, CompiledSchemaPolicy?> _cache = new(StringComparer.Ordinal);

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

        return new ValueTask<CompiledSchemaPolicy?>(LoadAsync(treeId, cancellationToken));
    }

    private async Task<CompiledSchemaPolicy?> LoadAsync(string treeId, CancellationToken cancellationToken)
    {
        var policy = await _store.GetPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
        var compiled = policy is null ? null : CompiledSchemaPolicy.Compile(policy);

        // Last write wins on a concurrent miss; compiling twice is harmless.
        _cache[treeId] = compiled;
        return compiled;
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
        // Only policy-tree writes are relevant; every other tree fast-paths out so
        // the observer stays cheap on the general write path. The policy is keyed
        // by governed tree id, so the mutation key is the tree to evict.
        if (string.Equals(mutation.TreeId, SchemaConstants.PolicyTree, StringComparison.Ordinal)
            && !string.IsNullOrEmpty(mutation.Key))
        {
            _cache.TryRemove(mutation.Key, out _);
        }

        return Task.CompletedTask;
    }
}
