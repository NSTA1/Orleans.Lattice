using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// One of the tenant's tenant-tier rules (<c>/t/{tenant}/access/rules/{localId}</c>):
/// reads it from the tenant policy and declares the address not found when the
/// tenant has no rule of that local id.
/// </summary>
public partial class TenantRuleDetailView
{
    private TenantRuleView? _rule;
    private AccessFailure? _failure;
    private (string Tenant, string RuleId)? _loaded;

    /// <summary>The rule's tenant-local id.</summary>
    [Parameter]
    [EditorRequired]
    public string RuleId { get; set; } = string.Empty;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    private string LayerText => _rule is { Editable: true }
        ? $"A rule of tenant {Tenant}'s own tier."
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
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _failure = failure;
        }
    }
}
