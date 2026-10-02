using ModelContextProtocol;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Maps the typed failures the delegated tenant access facades raise onto the
/// existing MCP error shape - an <see cref="McpException"/>, marked as a client
/// error when the call itself was the mistake - so an agent reads an actionable
/// message instead of an exception type name.
/// </summary>
/// <remarks>
/// <para>
/// Every mapped message is composed here from fixed text and server-chosen values
/// (a confinement rule, a quota dimension and limit); none echoes caller-supplied
/// text except the facade's own argument-validation message, which is a client
/// error and so is sanitized and length-capped at the credential-stamping seam
/// before it reaches the caller or the log.
/// </para>
/// <para>
/// An authorization denial (<see cref="LatticeAuthorizationDeniedException"/>) is
/// deliberately <b>not</b> mapped: it keeps its existing path through the shared
/// fault translator and is surfaced as a denial, never downgraded to a client
/// error (see <c>.github/instructions/security.instructions.md</c>). Any other
/// fault is likewise left to that translator.
/// </para>
/// </remarks>
internal static class TenantAccessToolFaults
{
    /// <summary>
    /// The message a tenant access tool returns while delegated tenant access
    /// administration is off on the cluster.
    /// </summary>
    internal const string DisabledMessage =
        "Delegated tenant access administration is not enabled on this cluster. A platform operator can "
        + "enable it with LatticeTenancyOptions.DelegatedAccessAdministrationEnabled; "
        + "lattice_tenant_access_posture reports whether it is enabled.";

    /// <summary>The message a tenant access tool returns for the reserved default tenant.</summary>
    internal const string ReservedTenantMessage =
        "The reserved default tenant has no delegated access administration: it has no tenant groups, "
        + "members or tenant rules, and its access stays operator-administered through the cluster auth tools.";

    /// <summary>The message a tenant access tool returns when a change would orphan the tenant.</summary>
    internal const string LastAdminMessage =
        "The change was refused because it would leave the tenant with no admin subject. Add another admin "
        + "subject to the tenant first.";

    /// <summary>
    /// Maps <paramref name="fault"/> onto the MCP error shape when it is one of the
    /// facades' typed failures.
    /// </summary>
    /// <param name="fault">The exception a facade call raised. Must not be <see langword="null"/>.</param>
    /// <param name="mapped">The mapped exception when this method returns <see langword="true"/>.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="fault"/> was mapped;
    /// <see langword="false"/> when it should propagate unchanged.
    /// </returns>
    public static bool TryTranslate(Exception fault, out McpException mapped)
    {
        ArgumentNullException.ThrowIfNull(fault);
        switch (fault)
        {
            case TenantAccessAdministrationDisabledException:
                mapped = new McpException(DisabledMessage, fault);
                return true;

            // Before the ArgumentException arm: a confinement refusal is an
            // ArgumentException, but it names a security rule and gets its own text.
            case TenantAccessConfinementException confinement:
                mapped = McpToolClientErrors.RejectedContent(DescribeConfinement(confinement.Rule));
                return true;

            case ReservedTenantOperationException:
                mapped = McpToolClientErrors.InvalidArgument(ReservedTenantMessage);
                return true;

            case TenantLastAdminSubjectException:
                mapped = McpToolClientErrors.InvalidArgument(LastAdminMessage);
                return true;

            case LatticeQuotaExceededException quota:
                mapped = new McpException(DescribeQuota(quota), fault);
                return true;

            // The facades validate their arguments (an invalid tenant id or group
            // name, a member or group that does not exist) with ArgumentException.
            // That is the caller's mistake; its message is sanitized at the seam.
            case ArgumentException argument:
                mapped = McpToolClientErrors.InvalidArgument(argument.Message);
                return true;

            default:
                mapped = null!;
                return false;
        }
    }

    /// <summary>Describes a confinement refusal from fixed text only.</summary>
    /// <param name="rule">The confinement rule the request broke.</param>
    /// <returns>The caller-facing message.</returns>
    internal static string DescribeConfinement(TenantAccessConfinementRule rule) => rule switch
    {
        TenantAccessConfinementRule.GroupNesting =>
            "The request was refused by tenant access confinement (GroupNesting): a tenant group may contain "
            + "users, cluster groups and the same tenant's groups only, and may never be nested in a cluster group "
            + "or in another tenant's group.",
        TenantAccessConfinementRule.ForeignTenantGroup =>
            "The request was refused by tenant access confinement (ForeignTenantGroup): it names another "
            + "tenant's group. Name the tenant's own groups by their tenant-local name with kind TenantGroup.",
        TenantAccessConfinementRule.RuleTree =>
            "The request was refused by tenant access confinement (RuleTree): a tenant rule may target the "
            + "tenant's own trees only.",
        TenantAccessConfinementRule.RuleOperations =>
            "The request was refused by tenant access confinement (RuleOperations): a tenant rule may not cover "
            + "operations a tenant admin cannot delegate.",
        TenantAccessConfinementRule.ReservedRuleId =>
            "The request was refused by tenant access confinement (ReservedRuleId): the rule id is reserved.",
        _ => "The request was refused by tenant access confinement.",
    };

    private static string DescribeQuota(LatticeQuotaExceededException quota)
        => string.IsNullOrEmpty(quota.Dimension)
            ? "The tenant is at one of its delegated access caps. Remove entries, or ask a platform operator to "
              + "raise the cap with lattice_tenant_set_quotas."
            : $"The tenant is at its {quota.Dimension} cap of {quota.Limit}. Remove entries, or ask a platform "
              + "operator to raise the cap with lattice_tenant_set_quotas.";
}
