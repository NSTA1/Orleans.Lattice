namespace Orleans.Lattice.Schema;

/// <summary>
/// The durable, runtime-mutable store of per-tree <see cref="LatticeSchemaPolicy"/>
/// entries. Policies are persisted into the reserved <c>sys-schema-policy</c>
/// <c>ILattice</c> tree keyed by governed tree id, so a tree's policy is a single
/// point read, and every mutation runs through the standard write path. This
/// interface is the policy storage surface; resolving and caching policies for the
/// enforcement hot path is the job of <see cref="ILatticeSchemaPolicyProvider"/>.
/// </summary>
public interface ILatticeSchemaPolicyStore
{
    /// <summary>
    /// Creates or replaces the policy governing <paramref name="treeId"/>. The
    /// policy is validated (every regex rule is compiled with
    /// <c>RegexOptions.NonBacktracking</c>) before it is persisted, so an
    /// uncompilable pattern is rejected here rather than on a later write.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="policy">The policy to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or targets the reserved <c>sys-schema-*</c> namespace, or a rule is invalid / carries an uncompilable regex.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the policy governing <paramref name="treeId"/>, or <c>null</c> when
    /// the tree has no policy.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the policy governing <paramref name="treeId"/>. Returns <c>true</c>
    /// when a policy was removed, <c>false</c> when none existed.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every governed tree id and its policy.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<KeyValuePair<string, LatticeSchemaPolicy>> ListPoliciesAsync(CancellationToken cancellationToken = default);
}
