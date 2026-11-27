namespace Orleans.Lattice.Auth;

/// <summary>
/// The admin audit anchors of the authorization policy store: the rule record whose
/// write is the commit point of a rule upsert or removal (#3638).
/// </summary>
internal static class PolicyAuditAnchors
{
    /// <summary>The rule record for (<paramref name="treeId"/>, <paramref name="ruleId"/>).</summary>
    /// <param name="treeId">The tree the rule governs.</param>
    /// <param name="ruleId">The rule id.</param>
    internal static LatticeAdminAuditAnchor Rule(string treeId, string ruleId) =>
        new(AuthConstants.PolicyTree, LatticeAuthorizationPolicyStore.RuleKey(treeId, ruleId));

    /// <summary>The audit target id of a rule: <c>{treeId}/{ruleId}</c>.</summary>
    /// <param name="treeId">The tree the rule governs.</param>
    /// <param name="ruleId">The rule id.</param>
    internal static string RuleTarget(string treeId, string ruleId) => string.Concat(treeId, "/", ruleId);
}
