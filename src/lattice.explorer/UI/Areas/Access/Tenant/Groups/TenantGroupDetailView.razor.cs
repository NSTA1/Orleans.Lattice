using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// One of the tenant's own groups (<c>/t/{tenant}/access/groups/{name}</c>):
/// reads it from the tenant directory, shows its provenance with its full id, and
/// declares the address not found when the tenant has no group of that name.
/// </summary>
public partial class TenantGroupDetailView
{
    private TenantGroupDescriptor? _group;
    private AccessFailure? _failure;
    private (string Tenant, string Name)? _loaded;

    /// <summary>The group's tenant-local name.</summary>
    [Parameter]
    [EditorRequired]
    public string Name { get; set; } = string.Empty;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    private string FullId => AccessSubjectPicker.TenantGroupId(Tenant, Name);

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (_loaded is { } loaded && loaded.Tenant == Tenant && loaded.Name == Name)
        {
            return;
        }

        _loaded = (Tenant, Name);
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        var (tenant, name) = (Tenant, Name);
        _failure = null;
        _group = null;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            var group = await directory.GetGroupAsync(tenant, name, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || tenant != Tenant || name != Name)
            {
                return;
            }

            if (group is null)
            {
                Navigation.NotFound();
                return;
            }

            _group = group;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (ArgumentException) when (!Lifetime.IsLeft)
        {
            // A name that is no valid local group name names no group.
            Navigation.NotFound();
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _failure = failure;
        }
    }
}
