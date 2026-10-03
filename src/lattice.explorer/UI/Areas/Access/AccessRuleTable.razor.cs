using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// A table of authorization rules, each linking to its page, with app-owned
/// rules attributed to (and linked to the roles of) their owning app, tenant-tier
/// rules attributed to (and linked to the rules of) their owning tenant, and the
/// rule that decided an explanation marked.
/// </summary>
public partial class AccessRuleTable
{
    /// <summary>The rules, in the order to show them.</summary>
    [Parameter]
    public IReadOnlyList<LatticeAuthorizationRule> Rules { get; set; } = [];

    /// <summary>The table's caption.</summary>
    [Parameter]
    public string Caption { get; set; } = "Rules";

    /// <summary>Whether the caption is for assistive technology only.</summary>
    [Parameter]
    public bool CaptionHidden { get; set; }

    /// <summary>What the table says when there is no rule.</summary>
    [Parameter]
    public string EmptyText { get; set; } = "No rules.";

    /// <summary>
    /// The tenant the page's address is rooted at, or <see langword="null"/> on a
    /// cluster-wide page; a rule's link keeps it.
    /// </summary>
    [Parameter]
    public string? Tenant { get; set; }

    /// <summary>
    /// The owning tenant of each tenant-tier rule among <see cref="Rules"/>, as the
    /// cluster's listing reported it (<c>AuthRulePage.TenantRuleTenants</c>); a rule
    /// it does not name is not tenant-tier. <see langword="null"/> names none.
    /// </summary>
    [Parameter]
    public IReadOnlyDictionary<LatticeAuthorizationRule, string>? TenantRules { get; set; }

    /// <summary>
    /// The id of the rule that decided an explanation (<c>AuthExplanation.DecidingRuleId</c>),
    /// whose row is marked, or <see langword="null"/> when no rule is marked.
    /// </summary>
    [Parameter]
    public string? DecidingRuleId { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    // The rule record itself: value equality over (tree, id, ...) with no per-row allocation.
    private static object RowKey(LatticeAuthorizationRule rule) => rule;

    private static string CompactSummary(LatticeAuthorizationRule rule) => string.Concat(
        AccessRuleFormat.EffectLabel(rule.Effect), " ",
        AccessRuleFormat.SubjectLabel(rule.Subject), " - ",
        AccessRuleFormat.ScopeLabel(rule.Scope), " - ",
        AccessRuleFormat.OperationsLabel(rule.Operations));

    private string RuleHref(LatticeAuthorizationRule rule) =>
        Navigator.Canonicalize(AccessRoutes.Rule(rule.RuleId, rule.Scope.TreeId).WithTenant(Tenant)).ToHref();

    private string AppRolesHref(string slug) => Navigator.Canonicalize(AccessRoutes.AppRoles(slug)).ToHref();

    /// <summary>The link to <paramref name="tenant"/>'s own rules, or <see langword="null"/> when this Explorer has no tenant-rooted addresses.</summary>
    private string? TenantRulesHref(string tenant) =>
        TenantId.TryParse(tenant, out _) && Navigator.Canonicalize(AccessRoutes.TenantRules(tenant)) is { Tenant: not null } address
            ? address.ToHref()
            : null;

    private string? TenantOf(LatticeAuthorizationRule rule) =>
        TenantRules is { } tenants && tenants.TryGetValue(rule, out var tenant) ? tenant : null;

    private bool IsDeciding(LatticeAuthorizationRule rule) => string.Equals(rule.RuleId, DecidingRuleId, StringComparison.Ordinal);
}
