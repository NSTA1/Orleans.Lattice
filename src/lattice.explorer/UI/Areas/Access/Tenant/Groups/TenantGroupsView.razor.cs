using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The tenant's own groups (<c>/t/{tenant}/access/groups</c>): reads the first
/// page of the tenant directory's groups and shows the loading, error and empty
/// states around it.
/// </summary>
public partial class TenantGroupsView
{
    private static readonly TenantAccessPageRequest FirstPage = new();

    private TenantGroupPage? _page;
    private AccessFailure? _failure;
    private string? _loadedTenant;

    private string CountText
    {
        get
        {
            var count = _page?.Entries.Count ?? 0;
            var more = _page?.NextPageToken is null ? string.Empty : ", more to load";
            return $"{count} {(count == 1 ? "group" : "groups")}{more}";
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
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            var page = await directory.ListGroupsAsync(tenant, FirstPage, Lifetime.Token).ConfigureAwait(true);
            if (string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                _page = page ?? new TenantGroupPage();
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
