namespace Orleans.Lattice.Auth;

/// <summary>
/// The tenant-tier rule purge behind
/// <see cref="ITenantPolicyRuleStore.PurgeTenantRulesAsync"/>, written over the
/// public <see cref="ILatticeAuthorizationPolicyStore"/> primitives so it runs the
/// store's own list and delete paths and can be exercised against an in-memory
/// store.
/// </summary>
internal static class TenantRulePurge
{
    /// <summary>
    /// Removes every rule whose id starts with <c>tenant:{tenant}:</c> from
    /// <paramref name="store"/>, wherever it is scoped. Collects the matches first and
    /// deletes them afterwards, so the scan never walks a range it is mutating.
    /// Idempotent: a second call finds nothing and removes nothing; an interrupted
    /// call is finished by the next one.
    /// </summary>
    /// <param name="store">The policy store to purge. Must not be <c>null</c>.</param>
    /// <param name="tenant">The tenant whose rules to remove. Must be an initialised tenant other than <see cref="TenantId.Default"/>.</param>
    /// <param name="cancellationToken">Cancels the purge.</param>
    /// <returns>The number of rules this call removed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="store"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenant"/> is uninitialised or the default tenant.</exception>
    public static async Task<int> PurgeAsync(
        ILatticeAuthorizationPolicyStore store,
        TenantId tenant,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(store);

        // For() validates the tenant (refusing the uninitialised and default values)
        // and yields "tenant:{tenant}:x"; the owned-id prefix is everything before x.
        var ownedPrefix = LatticeTenantRuleIds.For(tenant, "x")[..^1];

        // The purge is infrastructure acting for the tenant deletion pipeline, so it
        // runs under system origin: the store's tenant-tier write guard admits it.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var owned = new List<(string TreeId, string RuleId)>();
            await foreach (var rule in store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
            {
                if (rule.RuleId.StartsWith(ownedPrefix, StringComparison.Ordinal))
                {
                    owned.Add((rule.Scope.TreeId, rule.RuleId));
                }
            }

            var removed = 0;
            foreach (var (treeId, ruleId) in owned)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await store.RemoveRuleAsync(treeId, ruleId, cancellationToken).ConfigureAwait(false))
                {
                    removed++;
                }
            }

            return removed;
        }
    }
}
