namespace Orleans.Lattice.Auth;

/// <summary>
/// The durable, runtime-mutable store of <see cref="LatticeAuthorizationRule"/>s.
/// Rules are persisted into the reserved <c>sys-auth-policy</c> <c>ILattice</c>
/// tree keyed by their governed tree id so a tree's rules can be retrieved with a
/// single prefix scan, and every mutation runs through the standard write path so
/// it is durably captured by the per-key history view (enabled by default when
/// the authorization package is registered). This interface is the policy
/// storage surface only; evaluating rules into a decision is the responsibility
/// of a later feature.
/// </summary>
public interface ILatticeAuthorizationPolicyStore
{
    /// <summary>
    /// Creates or replaces a rule. The rule is stored under its governed tree id
    /// (<see cref="LatticeScope.TreeId"/>) and <see cref="LatticeAuthorizationRule.RuleId"/>.
    /// </summary>
    /// <param name="rule">The rule to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="rule"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">The rule's scope targets the reserved <c>sys-auth-*</c> namespace.</exception>
    Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a single rule by its governed tree id and rule id, or <c>null</c>
    /// when no such rule exists.
    /// </summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="ruleId"/> is <c>null</c> or empty.</exception>
    Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a rule by its governed tree id and rule id. Returns <c>true</c>
    /// when a rule was removed, <c>false</c> when none existed.
    /// </summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="ruleId"/> is <c>null</c> or empty.</exception>
    Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates every rule governing <paramref name="treeId"/>, in rule-id
    /// order, as a single prefix scan of the policy store.
    /// </summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    IAsyncEnumerable<LatticeAuthorizationRule> ListRulesForTreeAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every rule in the store, across every governed tree.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<LatticeAuthorizationRule> ListRulesAsync(CancellationToken cancellationToken = default);
}
