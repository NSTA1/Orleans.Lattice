using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>The tenant access posture probe.</summary>
internal sealed partial class LatticeTenantPolicyAdmin
{
    /// <inheritdoc />
    public async Task<TenantAccessPosture> GetPostureAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);

        // The one call that answers while the feature is off: it is how a caller
        // tells "off" from "denied". It is still authorized first.
        var record = await AuthorizeAsync(tenant, nameof(GetPostureAsync), answersWhileDisabled: true, cancellationToken)
            .ConfigureAwait(false);
        var enabled = _isEnabled();
        var caller = await ResolveCallerAsync(cancellationToken).ConfigureAwait(false);
        var isOperator = LatticeSystemOrigin.IsActive
            || await IsPlatformOperatorAsync(caller, cancellationToken).ConfigureAwait(false);

        // Group-held admin authority counts only while the feature is on; off, the
        // exact-id admin rule is the whole answer, as for active-tenant validation.
        var isTenantAdmin = !caller.IsAnonymous
            && (enabled
                ? record.IsAdmin(caller.SubjectId, caller.GroupIds)
                : record.HasAdminSubject(caller.SubjectId));

        var quotas = record.Quotas;
        long? groups = null;
        long? edges = null;
        long rules;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            if (_membershipUsage is not null)
            {
                groups = await _membershipUsage.CountGroupsAsync(tenant, cancellationToken).ConfigureAwait(false);
                edges = await _membershipUsage.CountEdgesAsync(tenant, cancellationToken).ConfigureAwait(false);
            }

            rules = await CountTenantRulesAsync(TenantPolicyScope.For(tenant), cancellationToken).ConfigureAwait(false);
        }

        return new TenantAccessPosture
        {
            TenantId = tenant.Value,
            Enabled = enabled,
            CallerIsTenantAdmin = isTenantAdmin,
            CallerIsPlatformOperator = isOperator,
            Groups = CapUsage(groups, quotas.EffectiveMaxGroups),
            MembershipEdges = CapUsage(edges, quotas.EffectiveMaxMembershipEdges),
            MemberSubjects = CapUsage(record.MemberSubjectCount, quotas.EffectiveMaxMemberSubjects),
            TenantRules = CapUsage(rules, quotas.EffectiveMaxTenantRules),
        };
    }

    /// <summary>
    /// One access cap against its usage. The caps have no burst allowance, so the
    /// burst-adjusted ceiling is the cap itself; overage is what usage exceeds it by
    /// (possible when an operator lowers a cap below current usage).
    /// </summary>
    /// <param name="usage">The current usage, or <see langword="null"/> when unmeasured.</param>
    /// <param name="cap">The effective cap. Always bounded.</param>
    /// <returns>The dimension reading.</returns>
    internal static TenantQuotaDimensionUsage CapUsage(long? usage, long cap) =>
        new()
        {
            Usage = usage,
            Limit = cap,
            BurstLimit = cap,
            Overage = usage is { } used && used > cap ? used - cap : 0,
        };

    /// <summary>
    /// The platform-operator test the tenant-tier authorizer applies: a whole-scope
    /// <see cref="LatticeOperation.Admin"/> allow on the reserved policy tree. A
    /// key-filtered allow never counts.
    /// </summary>
    private async ValueTask<bool> IsPlatformOperatorAsync(LatticeSubject subject, CancellationToken cancellationToken)
    {
        if (subject.IsAnonymous)
        {
            return false;
        }

        var request = new LatticeAccessRequest(LatticeAuthReservedTrees.PolicyTreeId, LatticeOperation.Admin, subject);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);
        return decision.Allowed && decision.KeyFilter is null;
    }
}
