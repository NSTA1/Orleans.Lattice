namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-management control plane: the set / clear / inspect verbs for a
/// tree's enforcement policy and its strict-mode dead-letter queue. These verbs
/// are the <see cref="LatticeOperation.SchemaAdmin"/>-gated surface (distinct from
/// data-plane <see cref="LatticeOperation.Admin"/>): changing a policy or replaying
/// dead letters authorizes on <see cref="LatticeOperation.SchemaAdmin"/>, while the
/// read verbs stay on ordinary read authority.
/// </summary>
public interface ILatticeSchemaAdmin
{
    /// <summary>
    /// Sets or replaces the enforcement policy for <paramref name="treeId"/>,
    /// enforced immediately on subsequent writes. Rejects an uncompilable regex at
    /// this point. (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="policy">The policy to apply. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or a rule is invalid / carries an uncompilable regex.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="policy"/> is <c>null</c>.</exception>
    Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clears the enforcement policy for <paramref name="treeId"/>. Returns
    /// <c>true</c> when a policy was removed. (<see cref="LatticeOperation.SchemaAdmin"/>.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the enforcement policy for <paramref name="treeId"/>, or <c>null</c>
    /// when none exists. (Read authority.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the strict-mode dead-letter entries retained for
    /// <paramref name="treeId"/>. (Read authority.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Counts the strict-mode dead-letter entries retained for
    /// <paramref name="treeId"/>. (Read authority.)
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the count.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default);
}
