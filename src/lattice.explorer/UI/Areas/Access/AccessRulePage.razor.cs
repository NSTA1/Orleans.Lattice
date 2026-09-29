using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// One rule (<c>/access/rules/{id}</c>, with <c>?tree=</c> naming its governed
/// tree). Without a tree the page finds the id across the rule store; an id used
/// in several trees is offered as a choice, and an id used in none is not found.
/// </summary>
public partial class AccessRulePage
{
    private const int SearchPages = 20;

    private ExplorerAddress? _loaded;
    private LatticeAuthorizationRule? _rule;
    private List<LatticeAuthorizationRule>? _candidates;
    private AccessAppRule? _owner;
    private AccessFailure? _failure;
    private AccessModelDescriptor? _model;
    private bool _editorOpen;
    private bool _confirmOpen;

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private string? RuleId => Address.Path.Count > 1 ? Address.Path[1] : null;

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Equals(_loaded, Address))
        {
            return;
        }

        _loaded = Address;
        _model ??= await Catalog.GetAccessModelAsync(CancellationToken.None).ConfigureAwait(true);
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _rule = null;
        _candidates = null;
        _owner = null;
        var ruleId = RuleId;
        if (string.IsNullOrEmpty(ruleId))
        {
            Navigation.NotFound();
            return;
        }

        try
        {
            var tree = Address.GetQuery(AccessRoutes.TreeQuery);
            if (!string.IsNullOrEmpty(tree))
            {
                var rule = await Catalog.Admin.GetRuleAsync(tree, ruleId).ConfigureAwait(true);
                Show(rule);
                return;
            }

            var matches = new List<LatticeAuthorizationRule>();
            var request = new AuthPageRequest { PageSize = AuthPageRequest.MaxPageSize };
            for (var page = 0; page < SearchPages; page++)
            {
                var result = await Catalog.Admin.ListRulesAsync(request).ConfigureAwait(true);
                matches.AddRange(result.Entries.Where(rule => string.Equals(rule.RuleId, ruleId, StringComparison.Ordinal)));
                if (result.NextPageToken is null)
                {
                    break;
                }

                request = request with { PageToken = result.NextPageToken };
            }

            if (matches.Count > 1)
            {
                _candidates = matches;
                return;
            }

            Show(matches.Count == 1 ? matches[0] : null);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            if (failure.Kind == AccessFailureKind.NotFound)
            {
                Navigation.NotFound();
                return;
            }

            _failure = failure;
        }
    }

    private void Show(LatticeAuthorizationRule? rule)
    {
        if (rule is null)
        {
            Navigation.NotFound();
            return;
        }

        _rule = rule;
        _owner = AccessAppRule.TryParse(rule.RuleId, out var owner) ? owner : null;
    }

    private void OnSaved(LatticeAuthorizationRule rule)
    {
        _editorOpen = false;
        _rule = rule;
        Toasts.Show($"Rule {rule.RuleId} saved.", LtToastTone.Success);
    }

    private async Task DeleteAsync()
    {
        var rule = _rule;
        if (rule is null || _owner is not null)
        {
            return;
        }

        try
        {
            await Catalog.Admin.RemoveRuleAsync(rule.Scope.TreeId, rule.RuleId).ConfigureAwait(true);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
            return;
        }

        Catalog.Invalidate();
        Toasts.Show($"Rule {rule.RuleId} deleted.", LtToastTone.Success);
        Navigator.NavigateTo(Navigator.Canonicalize(AccessRoutes.Rules));
    }

    private string GroupHref(string groupId) => Navigator.Canonicalize(AccessRoutes.Group(groupId)).ToHref();

    private string AppRolesHref(string slug) => Navigator.Canonicalize(AccessRoutes.AppRoles(slug)).ToHref();

    private string ExplainHref(LatticeAuthorizationRule rule)
    {
        var address = AccessRoutes.Explain
            .WithQuery(AccessRoutes.SubjectQuery, rule.Subject.Id)
            .WithQuery(AccessRoutes.KindQuery, AccessRuleFormat.SubjectKindLabel(rule.Subject.Kind));
        if (!AccessRuleFormat.IsClusterWide(rule.Scope) && !AccessRuleFormat.IsAccessAdministration(rule.Scope))
        {
            address = address.WithQuery(AccessRoutes.TreeQuery, rule.Scope.TreeId);
        }

        return Navigator.Canonicalize(address).ToHref();
    }
}
