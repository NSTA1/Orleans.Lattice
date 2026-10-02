using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>Tenant-tier rule authoring and listing.</summary>
internal sealed partial class LatticeTenantPolicyAdmin
{
    /// <summary>The separator between a rule's tree id and rule id in a listing page token, as in the store key.</summary>
    private const char CatalogKeySeparator = '\u001f';

    /// <summary><see cref="CatalogKeySeparator"/> as a string, for composing a page token.</summary>
    private const string CatalogKeySeparatorText = "\u001f";

    /// <inheritdoc />
    public async Task<TenantRuleView> PutRuleAsync(
        string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentNullException.ThrowIfNull(rule);

        var record = await AuthorizeAsync(tenant, nameof(PutRuleAsync), answersWhileDisabled: false, cancellationToken)
            .ConfigureAwait(false);
        var scope = TenantPolicyScope.For(tenant);

        // The friendly, typed confinement verdict comes first; the store's own guards
        // re-check the composed rule as the backstop.
        var stored = scope.ToStoredRule(rule);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var census = await CensusAsync(scope, stored.RuleId, cancellationToken).ConfigureAwait(false);

            // Replacing a rule the tenant already has adds nothing, so only a new id is
            // admitted against the cap.
            if (census.Matches.Count == 0)
            {
                TenantAccessCaps.AdmitAddition(
                    tenant,
                    stored.Scope.TreeId,
                    TenantAccessCaps.TenantRulesDimension,
                    census.TenantRuleCount,
                    record.Quotas.EffectiveMaxTenantRules);
            }

            await _store.PutRuleAsync(stored, cancellationToken).ConfigureAwait(false);

            // A local id is unique within the tenant, but the store keys a rule by its
            // tree too: a put that moves the rule to another tree removes the copy it
            // replaced. Written after the put, so an interruption leaves the old rule in
            // force rather than none.
            foreach (var match in census.Matches)
            {
                if (!string.Equals(match.Scope.TreeId, stored.Scope.TreeId, StringComparison.Ordinal))
                {
                    await _store.RemoveRuleAsync(match.Scope.TreeId, match.RuleId, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        return scope.ToView(stored, TenantRuleVisibility.Tenant);
    }

    /// <inheritdoc />
    public async Task<TenantRuleView?> GetRuleAsync(
        string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);

        await AuthorizeAsync(tenant, nameof(GetRuleAsync), answersWhileDisabled: false, cancellationToken)
            .ConfigureAwait(false);
        var scope = TenantPolicyScope.For(tenant);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var census = await CensusAsync(scope, scope.ComposeRuleId(ruleId), cancellationToken).ConfigureAwait(false);
            return census.Matches.Count == 0 ? null : scope.ToView(census.Matches[0], TenantRuleVisibility.Tenant);
        }
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(
        string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);

        await AuthorizeAsync(tenant, nameof(RemoveRuleAsync), answersWhileDisabled: false, cancellationToken)
            .ConfigureAwait(false);
        var scope = TenantPolicyScope.For(tenant);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // Only a tenant:{T}: id is ever addressed, so a platform rule can never be
            // removed through this surface.
            var census = await CensusAsync(scope, scope.ComposeRuleId(ruleId), cancellationToken).ConfigureAwait(false);
            var removed = false;
            foreach (var match in census.Matches)
            {
                removed |= await _store.RemoveRuleAsync(match.Scope.TreeId, match.RuleId, cancellationToken).ConfigureAwait(false);
            }

            return removed;
        }
    }

    /// <inheritdoc />
    public async Task<TenantRulePage> ListRulesAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentNullException.ThrowIfNull(page);

        await AuthorizeAsync(tenant, nameof(ListRulesAsync), answersWhileDisabled: false, cancellationToken)
            .ConfigureAwait(false);
        var scope = TenantPolicyScope.For(tenant);
        var size = page.EffectivePageSize;
        var token = page.PageToken;
        var entries = new List<TenantRuleView>(Math.Min(size, 32));
        LatticeAuthorizationRule? last = null;
        string? next = null;

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            // The store scans in (tree id, rule id) order, which is the stable listing
            // order and the page token's ordering.
            await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
            {
                var visibility = scope.Classify(rule);
                if (visibility is not (TenantRuleVisibility.Tenant or TenantRuleVisibility.PlatformTree))
                {
                    continue;
                }

                if (token is not null && CompareToCatalogKey(rule, token) <= 0)
                {
                    continue;
                }

                if (entries.Count == size)
                {
                    next = CatalogKey(last!);
                    break;
                }

                entries.Add(scope.ToView(rule, visibility));
                last = rule;
            }
        }

        return new TenantRulePage { Entries = entries, NextPageToken = next };
    }

    /// <summary>
    /// Scans the policy store once for the tenant: counts its tenant-tier rules and
    /// collects every stored copy of <paramref name="ruleId"/>. Runs inside the
    /// caller's system-origin scope.
    /// </summary>
    private async Task<RuleCensus> CensusAsync(TenantPolicyScope scope, string ruleId, CancellationToken cancellationToken)
    {
        var count = 0L;
        List<LatticeAuthorizationRule>? matches = null;
        await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!scope.OwnsRuleId(rule.RuleId))
            {
                continue;
            }

            count++;
            if (string.Equals(rule.RuleId, ruleId, StringComparison.Ordinal))
            {
                (matches ??= []).Add(rule);
            }
        }

        return new RuleCensus(count, matches ?? (IReadOnlyList<LatticeAuthorizationRule>)[]);
    }

    /// <summary>Counts the tenant's tenant-tier rules. Runs inside the caller's system-origin scope.</summary>
    private async Task<long> CountTenantRulesAsync(TenantPolicyScope scope, CancellationToken cancellationToken)
    {
        var count = 0L;
        await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
        {
            if (scope.OwnsRuleId(rule.RuleId))
            {
                count++;
            }
        }

        return count;
    }

    private static string CatalogKey(LatticeAuthorizationRule rule) =>
        string.Concat(rule.Scope.TreeId, CatalogKeySeparatorText, rule.RuleId);

    /// <summary>
    /// Compares a rule's catalog key, <c>{treeId}\u001f{ruleId}</c>, with
    /// <paramref name="token"/> ordinally without composing the key.
    /// </summary>
    private static int CompareToCatalogKey(LatticeAuthorizationRule rule, string token)
    {
        var treeId = rule.Scope.TreeId.AsSpan();
        var tokenSpan = token.AsSpan();
        var shared = Math.Min(treeId.Length, tokenSpan.Length);
        var byTree = treeId[..shared].SequenceCompareTo(tokenSpan[..shared]);
        if (byTree != 0)
        {
            return byTree;
        }

        // The token is a prefix of the tree id, so the longer catalog key sorts after it.
        if (tokenSpan.Length <= treeId.Length)
        {
            return 1;
        }

        var bySeparator = CatalogKeySeparator.CompareTo(tokenSpan[treeId.Length]);
        return bySeparator != 0
            ? bySeparator
            : rule.RuleId.AsSpan().SequenceCompareTo(tokenSpan[(treeId.Length + 1)..]);
    }

    /// <summary>One store scan's view of a tenant: its rule count and the stored copies of one rule id.</summary>
    private readonly record struct RuleCensus(long TenantRuleCount, IReadOnlyList<LatticeAuthorizationRule> Matches);
}
