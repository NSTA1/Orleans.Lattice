namespace Orleans.Lattice.Schema;

/// <summary>
/// Resolves and caches the compiled schema policy for a tree on the enforcement
/// hot path. A tree with no policy resolves to <c>null</c> from a cached lookup so
/// the interceptor short-circuits with no per-write work; a governed tree resolves
/// to a <see cref="CompiledSchemaPolicy"/> whose regexes were compiled once at
/// cache-load time.
/// </summary>
internal interface ILatticeSchemaPolicyProvider
{
    /// <summary>
    /// Whether strict-mode ingest is globally enabled. The interceptor mirrors
    /// this into <c>ILatticeWriteInterceptor.InterceptsSystemOrigin</c>, so when
    /// it is <c>false</c> system-origin writes are never inspected.
    /// </summary>
    bool StrictIngestEnabled { get; }

    /// <summary>
    /// Resolves the compiled policy governing <paramref name="treeId"/>, or
    /// <c>null</c> when the tree is not governed. Cached: the first call for a tree
    /// loads and compiles the policy; subsequent calls are served from memory
    /// until the policy tree mutates.
    /// </summary>
    /// <param name="treeId">The tree id being written.</param>
    /// <param name="cancellationToken">Cancels a cache-miss load.</param>
    /// <returns>The compiled policy, or <c>null</c> when the tree is not governed.</returns>
    ValueTask<CompiledSchemaPolicy?> GetCompiledPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Evicts any cached policy for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The governed tree id to evict.</param>
    void Invalidate(string treeId);
}
