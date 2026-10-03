using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The production <see cref="ITenantGroupRuleCascade"/> over the registered
/// <see cref="ILatticeAuthorizationPolicyStore"/>: scans the policy tree for the
/// tenant's own rules (the <see cref="LatticeTenantRuleIds.For"/> prefix) whose
/// subject is the removed group and deletes each under system origin, which the
/// store's tenant-tier write guard requires.
/// </summary>
/// <param name="store">The authorization policy store. Must not be <c>null</c>.</param>
internal sealed class PolicyStoreTenantGroupRuleCascade(ILatticeAuthorizationPolicyStore store) : ITenantGroupRuleCascade
{
    private readonly ILatticeAuthorizationPolicyStore _store = store ?? throw new ArgumentNullException(nameof(store));

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> RemoveRulesNamingGroupAsync(
        TenantId tenant, string groupId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(groupId);

        // For() validates the tenant (refusing the uninitialised and default values);
        // the owned prefix is everything before the placeholder local id.
        var ownedPrefix = LatticeTenantRuleIds.For(tenant, "x")[..^1];

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var matches = new List<(string TreeId, string RuleId)>();
            await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
            {
                if (rule.RuleId.StartsWith(ownedPrefix, StringComparison.Ordinal)
                    && rule.Subject.Kind == LatticeSubjectSelectorKind.Group
                    && string.Equals(rule.Subject.Id, groupId, StringComparison.Ordinal))
                {
                    matches.Add((rule.Scope.TreeId, rule.RuleId));
                }
            }

            var removed = new List<string>(matches.Count);
            foreach (var (treeId, ruleId) in matches)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await _store.RemoveRuleAsync(treeId, ruleId, cancellationToken).ConfigureAwait(false))
                {
                    removed.Add(ruleId);
                }
            }

            removed.Sort(OrdinalStringOrder.Comparison);
            return removed;
        }
    }
}
