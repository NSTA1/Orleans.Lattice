using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// A table of authorization rules, each linking to its page, with app-owned
/// rules attributed to (and linked to the roles of) their owning app.
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
        Navigator.Canonicalize(AccessRoutes.Rule(rule.RuleId, rule.Scope.TreeId)).ToHref();

    private string AppRolesHref(string slug) => Navigator.Canonicalize(AccessRoutes.AppRoles(slug)).ToHref();
}
