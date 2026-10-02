using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Explain;

/// <summary>
/// Draws a tenant explanation's two layers: the Platform layer above the Tenant
/// layer, the rules that matched in each, the deciding layer and rule marked,
/// and the layer that lost dimmed. It renders the server's answer as it arrives
/// and never re-decides it.
/// </summary>
public partial class TenantExplainLayers
{
    private static readonly TenantRuleLayer[] Layers = [TenantRuleLayer.Platform, TenantRuleLayer.Tenant];

    private TenantExplanation? _split;
    private IReadOnlyList<TenantRuleView> _platform = [];
    private IReadOnlyList<TenantRuleView> _tenant = [];

    /// <summary>The tenant the explanation is for.</summary>
    [Parameter]
    [EditorRequired]
    public string Tenant { get; set; } = string.Empty;

    /// <summary>The explanation to draw.</summary>
    [Parameter]
    [EditorRequired]
    public TenantExplanation Explanation { get; set; } = default!;

    private string DecidingAttribute => Explanation.DecidingLayer is { } layer ? LayerAttribute(layer) : "default";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        ArgumentNullException.ThrowIfNull(Explanation);
        if (ReferenceEquals(_split, Explanation))
        {
            return;
        }

        var platform = new List<TenantRuleView>();
        var tenant = new List<TenantRuleView>();
        foreach (var rule in Explanation.MatchedRules)
        {
            (rule.Layer == TenantRuleLayer.Platform ? platform : tenant).Add(rule);
        }

        // The deciding rule is always drawn, even when the matched list leaves it out.
        if (Explanation.DecidingRule is { SubjectWithheld: false } deciding && Explanation.DecidingLayer is { } layer)
        {
            var list = layer == TenantRuleLayer.Platform ? platform : tenant;
            if (!list.Exists(rule => string.Equals(rule.RuleId, deciding.RuleId, StringComparison.Ordinal)))
            {
                list.Insert(0, deciding);
            }
        }

        _platform = platform;
        _tenant = tenant;
        _split = Explanation;
    }

    private static string LayerAttribute(TenantRuleLayer layer) => layer == TenantRuleLayer.Platform ? "platform" : "tenant";

    private IReadOnlyList<TenantRuleView> RulesIn(TenantRuleLayer layer) => layer == TenantRuleLayer.Platform ? _platform : _tenant;

    private bool IsDecidingRule(TenantRuleView rule) =>
        Explanation.DecidingRule is { } deciding && string.Equals(rule.RuleId, deciding.RuleId, StringComparison.Ordinal);

    private string Note(TenantRuleLayer layer) => layer == TenantRuleLayer.Platform
        ? "Read-only. Evaluated first, and final when a rule matches."
        : $"Tenant {Tenant}'s own rules. Consulted only when no platform rule matches.";

    private string NoneText(TenantRuleLayer layer) => layer switch
    {
        TenantRuleLayer.Platform => "No platform rule matched.",
        _ when Explanation.DecidingLayer == TenantRuleLayer.Platform => "Not consulted: a platform rule decided first.",
        _ => "No tenant rule matched.",
    };
}
