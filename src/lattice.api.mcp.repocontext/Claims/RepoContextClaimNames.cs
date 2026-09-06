namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Derives the cluster-wide lock name a repository-context record is claimed
/// under. The claim surface owns no lock of its own: it addresses the core
/// <see cref="ILatticeLockGrain"/> by a name derived deterministically from the
/// record key, so two callers naming the same record contend on the same lock
/// activation and no other Lattice lock user can collide with a claim.
/// </summary>
internal static class RepoContextClaimNames
{
    /// <summary>
    /// The namespace prefix every repository-context claim lock carries, keeping
    /// the claim locks disjoint from every other named lock in the cluster.
    /// </summary>
    internal const string LockNamespace = "repocontext/claim/";

    /// <summary>
    /// The lock name guarding <paramref name="recordKey"/>. The mapping is total,
    /// injective, and stable across activations, so it can be recomputed rather
    /// than stored.
    /// </summary>
    /// <param name="recordKey">The full repository-context key being claimed. Must not be <see langword="null"/>.</param>
    /// <returns>The lock name to resolve <see cref="ILatticeLockGrain"/> by.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="recordKey"/> is null.</exception>
    internal static string LockName(string recordKey)
    {
        ArgumentNullException.ThrowIfNull(recordKey);
        return string.Concat(LockNamespace, recordKey);
    }
}
