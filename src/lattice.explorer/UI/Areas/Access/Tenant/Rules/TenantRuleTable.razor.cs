using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// A table of one layer of a tenant's rules: the tenant's own rules, each linking
/// to its page, or the read-only platform rules on the tenant's trees, which link
/// nowhere because nothing about them can be changed here.
/// </summary>
public partial class TenantRuleTable
{
    /// <summary>The rules, in the order to show them.</summary>
    [Parameter]
    public IReadOnlyList<TenantRuleView> Rules { get; set; } = [];

    /// <summary>The tenant the rules govern; a tenant rule's link is rooted at it.</summary>
    [Parameter]
    [EditorRequired]
    public string Tenant { get; set; } = string.Empty;

    /// <summary>Whether the rules are the tenant's own, editable on their pages; otherwise they are read-only.</summary>
    [Parameter]
    public bool Editable { get; set; }

    /// <summary>The table's caption.</summary>
    [Parameter]
    public string Caption { get; set; } = "Rules";

    /// <summary>Whether the caption is for assistive technology only.</summary>
    [Parameter]
    public bool CaptionHidden { get; set; }

    /// <summary>What the table says when there is no rule.</summary>
    [Parameter]
    public string EmptyText { get; set; } = "No rules.";

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    // The view record itself: value equality over its fields, with no per-row allocation.
    private static object RowKey(TenantRuleView rule) => rule;

    private static string CompactSummary(TenantRuleView rule) => string.Concat(
        AccessRuleFormat.EffectLabel(rule.Effect), " ",
        TenantRuleFormat.SubjectLabel(rule.SubjectKind, rule.SubjectId), " - ",
        TenantRuleFormat.ScopeLabel(rule), " - ",
        AccessRuleFormat.OperationsLabel(rule.Operations));

    private string RuleHref(TenantRuleView rule) =>
        Navigator.Canonicalize(AccessRoutes.TenantRule(Tenant, rule.RuleId)).ToHref();
}
