using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The policy-administration and policy-introspection surface the Access area
/// drives over the auth-admin control plane: listing, authoring, editing, and
/// deleting authorization rules; and the two facade-driven introspection views -
/// Explain (a subject / operation / scope verdict plus its matched rules) and
/// EffectivePermissions (every rule in effect for a subject). The introspection
/// views are computed <b>entirely</b> by the facade, so the UI renders their
/// verdicts verbatim and never re-implements decision logic. Every read folds a
/// denial / transport failure into a non-success view, and every mutation into an
/// <see cref="AccessOperationResult"/>.
/// </summary>
public interface IPolicyAdminService
{
    /// <summary>Lists one page of every rule in the store, ordered by <c>(governed tree id, rule id)</c>.</summary>
    /// <param name="pageSize">The page size, or <c>0</c> for the facade default.</param>
    /// <param name="pageToken">The continuation cursor, or <see langword="null"/> to start from the beginning.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessListView<LatticeAuthorizationRule>> ListRulesAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default);

    /// <summary>Lists one page of the rules governing a single tree, ordered by rule id.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="pageSize">The page size, or <c>0</c> for the facade default.</param>
    /// <param name="pageToken">The continuation cursor, or <see langword="null"/> to start from the beginning.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessListView<LatticeAuthorizationRule>> ListRulesForTreeAsync(string treeId, int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default);

    /// <summary>Reads a single rule, or <see langword="null"/> when it does not exist or the read is denied / fails.</summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces an authorization rule.</summary>
    /// <param name="rule">The rule to persist. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default);

    /// <summary>Deletes an authorization rule.</summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> DeleteRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Explains whether <paramref name="subjectId"/> may perform
    /// <paramref name="operation"/> over <paramref name="scope"/>, returning the
    /// facade's verdict and the matched rules. The verdict is authoritative - the
    /// same access gate the data plane consults.
    /// </summary>
    /// <param name="subjectId">The subject to explain the decision for. Must not be <see langword="null"/> or empty.</param>
    /// <param name="operation">The operation to evaluate.</param>
    /// <param name="scope">The keyspace scope to evaluate. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ExplainView> ExplainAsync(string subjectId, LatticeOperation operation, LatticeScope scope, CancellationToken cancellationToken = default);

    /// <summary>Returns the authorization rules currently in effect for <paramref name="subjectId"/>.</summary>
    /// <param name="subjectId">The subject to resolve permissions for. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<EffectivePermissionsView> EffectivePermissionsAsync(string subjectId, CancellationToken cancellationToken = default);
}
