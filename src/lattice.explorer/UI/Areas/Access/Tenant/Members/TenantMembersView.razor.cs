using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members;

/// <summary>
/// The tenant's member set (<c>/t/{tenant}/access/members</c>): reads the first
/// page of the set from the tenant directory and shows the loading, error and
/// empty states around it.
/// </summary>
public partial class TenantMembersView
{
    private static readonly TenantAccessPageRequest FirstPage = new();

    private TenantMemberPage? _page;
    private AccessFailure? _failure;
    private string? _loadedTenant;

    private string CountText
    {
        get
        {
            var count = _page?.Entries.Count ?? 0;
            var more = _page?.NextPageToken is null ? string.Empty : ", more to load";
            return $"{count} {(count == 1 ? "member" : "members")}{more}";
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
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant members.");
            var page = await directory.ListMembersAsync(tenant, FirstPage, Lifetime.Token).ConfigureAwait(true);
            if (string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                _page = page ?? new TenantMemberPage();
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
