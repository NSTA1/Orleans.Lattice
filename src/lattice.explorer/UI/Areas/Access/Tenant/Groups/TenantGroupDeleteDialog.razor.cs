using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The confirmation a tenant group's deletion goes through: it previews the
/// tenant directory's cascade (<see cref="TenantGroupCascade"/>) when it opens,
/// deletes on a typed confirmation, and reports what the removal actually took
/// (<see cref="TenantGroupRemovalResult"/>).
/// </summary>
public partial class TenantGroupDeleteDialog
{
    private TenantGroupCascade? _cascade;
    private bool _wasOpen;

    /// <summary>The group's tenant-local name.</summary>
    [Parameter]
    [EditorRequired]
    public string Name { get; set; } = string.Empty;

    /// <summary>Whether the confirmation is open.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised when the confirmation opens or closes.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }

    /// <summary>Raised once the group is deleted, with what the removal took.</summary>
    [Parameter]
    public EventCallback<TenantGroupRemovalResult> OnDeleted { get; set; }

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var opened = Open && !_wasOpen;
        _wasOpen = Open;
        if (!opened)
        {
            return;
        }

        _cascade = null;
        var (tenant, name) = (Tenant, Name);
        try
        {
            var cascade = await TenantGroupCascade.ReadAsync(Access, tenant, name, Lifetime.Token).ConfigureAwait(true);
            if (!Lifetime.IsLeft && Open && tenant == Tenant && name == Name)
            {
                _cascade = cascade;
            }
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
    }

    private async Task OnOpenChangedAsync(bool open)
    {
        if (!open)
        {
            _wasOpen = false;
        }

        await OpenChanged.InvokeAsync(open).ConfigureAwait(true);
    }

    private async Task DeleteAsync()
    {
        var (tenant, name) = (Tenant, Name);
        TenantGroupRemovalResult result;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            result = await directory.RemoveGroupAsync(tenant, name, Lifetime.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }
        catch (TenantLastAdminSubjectException)
        {
            Toasts.Show(TenantGroupFormat.LastAdminMessage, LtToastTone.Danger);
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

        Toasts.Show(TenantGroupFormat.RemovalText(result), LtToastTone.Success);
        await OnDeleted.InvokeAsync(result).ConfigureAwait(true);
    }
}
