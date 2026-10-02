using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The rules governing the tenant (<c>/t/{tenant}/access/rules</c>): reads the
/// first page of the tenant policy's listing and shows the loading, error and
/// empty states around it.
/// </summary>
public partial class TenantRulesView
{
    private static readonly TenantAccessPageRequest FirstPage = new();

    private TenantRulePage? _page;
    private AccessFailure? _failure;
    private string? _loadedTenant;

    private string CountText
    {
        get
        {
            var count = _page?.Entries.Count ?? 0;
            var more = _page?.NextPageToken is null ? string.Empty : ", more to load";
            return $"{count} {(count == 1 ? "rule" : "rules")} of tenant {Tenant}{more}";
        }
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedTenant, Tenant, StringComparison.Ordinal))
        {
            return;
        }

        _loadedTenant = Tenant;
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        var tenant = Tenant;
        _failure = null;
        _page = null;
        try
        {
            var policy = Access.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
            var page = await policy.ListRulesAsync(tenant, FirstPage, Lifetime.Token).ConfigureAwait(true);
            if (string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                _page = page ?? new TenantRulePage();
            }
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
