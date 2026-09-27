namespace Orleans.Lattice.Auth;

/// <summary>
/// The reserved rule-id namespace for authorization rules owned by an installed
/// app. Every rule the app role-to-rule compiler emits carries an id starting with
/// <see cref="Prefix"/>, which encodes app ownership in the id itself with no
/// change to the <see cref="LatticeAuthorizationRule"/> shape. Operators must
/// author their own rules with ids outside this prefix; a direct operator write or
/// delete of an app-owned rule id is rejected by the policy store.
/// </summary>
public static class LatticeAppRuleIds
{
    /// <summary>
    /// The prefix every app-owned authorization rule id starts with. Rules emitted
    /// by the app role-to-rule compiler use it so ownership is recoverable from the
    /// id alone; operator-authored rules must use ids outside it, and direct
    /// operator writes or deletes of an app-owned id are rejected by the policy
    /// store.
    /// </summary>
    public const string Prefix = "app:";

    /// <summary>
    /// Returns <c>true</c> when <paramref name="ruleId"/> is in the app-owned
    /// rule-id namespace, that is when it starts with <see cref="Prefix"/> under an
    /// ordinal comparison. Allocation-free.
    /// </summary>
    /// <param name="ruleId">The candidate rule id. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> if the id is app-owned; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="ruleId"/> is <c>null</c>.</exception>
    public static bool IsAppOwned(string ruleId)
    {
        ArgumentNullException.ThrowIfNull(ruleId);
        return ruleId.StartsWith(Prefix, StringComparison.Ordinal);
    }
}
