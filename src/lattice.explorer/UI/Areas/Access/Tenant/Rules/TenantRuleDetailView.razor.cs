using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// One of the tenant's tenant-tier rules (<c>/t/{tenant}/access/rules/{localId}</c>):
/// reads it from the tenant policy, declares the address not found when the
/// tenant has no rule of that local id, and edits, deletes, explains and shows
/// the history of the one it finds.
/// </summary>
public partial class TenantRuleDetailView
{
    private TenantRuleView? _rule;
    private IReadOnlyList<TenantRuleView> _rules = [];
    private AccessModelDescriptor? _model;
    private AccessFailure? _failure;
    private (string Tenant, string RuleId)? _loaded;
    private bool _editorOpen;
    private bool _confirmOpen;

    /// <summary>The rule's tenant-local id.</summary>
    [Parameter]
    [EditorRequired]
    public string RuleId { get; set; } = string.Empty;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string LayerText => _rule is { Editable: true }
        ? $"A rule of tenant {Tenant}'s own tier. It decides only where no platform rule matches."
        : "A platform rule, read-only here.";

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (_loaded is { } loaded && loaded.Tenant == Tenant && loaded.RuleId == RuleId)
        {
            return;
        }

        _loaded = (Tenant, RuleId);
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        var (tenant, ruleId) = (Tenant, RuleId);
        _failure = null;
        _rule = null;
        try
        {
            var policy = Access.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
            var rule = await policy.GetRuleAsync(tenant, ruleId, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || tenant != Tenant || ruleId != RuleId)
            {
                return;
            }

            if (rule is null)
            {
                Navigation.NotFound();
                return;
            }

            _rule = rule;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _failure = failure;
            return;
        }

        await ReadContextAsync(tenant).ConfigureAwait(true);
    }

    /// <summary>
    /// Reads what the editor needs besides the rule: the rules governing the
    /// tenant, for its shadow check, and the cluster's access model, for its
    /// subject picker. Neither is required, so a failure leaves it empty.
    /// </summary>
    private async Task ReadContextAsync(string tenant)
    {
        try
        {
            _rules = await Access.GetRulesAsync(tenant, Lifetime.Token).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _rules = [];
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }

        AccessCatalog? catalog;
        try
        {
            catalog = Services.GetService<AccessCatalog>();
        }
        catch (InvalidOperationException)
        {
            catalog = null;
        }

        if (catalog is not null)
        {
            _model = await catalog.GetAccessModelAsync(Lifetime.Token).ConfigureAwait(true);
        }
    }

    private async Task OnSaved(TenantRuleView rule)
    {
        _editorOpen = false;
        _rule = rule;
        Toasts.Show($"Rule {rule.RuleId} saved.", LtToastTone.Success);
        await ReadContextAsync(Tenant).ConfigureAwait(true);
    }

    private async Task DeleteAsync()
    {
        var rule = _rule;
        var tenant = Tenant;
        if (rule is not { Editable: true })
        {
            return;
        }

        try
        {
            var policy = Access.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
            await policy.RemoveRuleAsync(tenant, rule.RuleId, Lifetime.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
            return;
        }

        Access.Invalidate();
        if (Lifetime.IsLeft)
        {
            return;
        }

        Toasts.Show($"Rule {rule.RuleId} deleted.", LtToastTone.Success);
        Navigator.NavigateTo(Navigator.Canonicalize(AccessRoutes.TenantRules(tenant)));
    }

    /// <summary>The tenant's Explain, asked about the rule's subject on the tree it governs, or <see langword="null"/> when its subject is withheld.</summary>
    private string? ExplainHref(TenantRuleView rule)
    {
        if (rule.SubjectId is not { } subject)
        {
            return null;
        }

        var address = AccessRoutes.TenantExplain(Tenant)
            .WithQuery(AccessRoutes.SubjectQuery, subject)
            .WithQuery(AccessRoutes.KindQuery, TenantRuleFormat.SubjectKindValue(rule.SubjectKind));
        if (rule.TreeName is { } tree)
        {
            address = address.WithQuery(AccessRoutes.TreeQuery, tree);
        }

        return Href(address);
    }
}
