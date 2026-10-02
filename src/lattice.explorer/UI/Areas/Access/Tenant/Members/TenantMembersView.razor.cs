using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members;

/// <summary>
/// The tenant's member set (<c>/t/{tenant}/access/members</c>, D5): the users, the
/// tenant's own groups and the cluster groups whose subjects may act as the tenant,
/// added through the tenant-aware subject picker within the <c>MaxMemberSubjects</c>
/// cap (D13) and removed with a confirmation; and the tenant's administrators,
/// members implicitly, listed read-only with a link to where they are changed.
/// </summary>
public partial class TenantMembersView
{
    private IReadOnlyList<TenantMemberEntry> _members = [];
    private TenantMemberPage? _page;
    private TenantAccessPosture? _posture;
    private AccessModelDescriptor? _model;
    private AccessFailure? _failure;
    private IReadOnlyList<string>? _admins;
    private bool _adminsRead;
    private string? _loadedTenant;
    private bool _loadingMore;
    private bool _busy;
    private TenantSubjectKind _kind = TenantSubjectKind.User;
    private string _subjectId = string.Empty;
    private string? _addError;
    private TenantMemberEntry? _removing;
    private AccessSubjectPicker? _picker;

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal TenantAdminSubjects Admins { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    private string CountText
    {
        get
        {
            var more = _page?.NextPageToken is null ? string.Empty : ", more to load";
            return TenantGroupFormat.Count(_members.Count, "member", "members") + more;
        }
    }

    private bool AtCap => _posture is { } posture && TenantGroupFormat.AtCap(posture.MemberSubjects);

    private string? CapText => _posture is { } posture ? TenantGroupFormat.CapText(posture.MemberSubjects, "member", "members") : null;

    private string AdminsHref => Href(TenancyRoutes.TenantMembers(Tenant));

    private string AdminsState => _admins is not null ? "ready" : _adminsRead ? "unavailable" : "loading";

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedTenant, Tenant, StringComparison.Ordinal))
        {
            return;
        }

        _loadedTenant = Tenant;
        _removing = null;
        _model ??= await Catalog.GetAccessModelAsync(Lifetime.Token).ConfigureAwait(true);
        await LoadAsync().ConfigureAwait(true);
        await LoadAdminsAsync().ConfigureAwait(true);
    }

    private static string EntryKey(TenantMemberEntry entry) => TenantGroupFormat.KindValue(entry.Kind) + ":" + entry.SubjectId;

    private string GroupHref(string name) => Href(AccessRoutes.TenantGroup(Tenant, name));

    private async Task LoadAsync()
    {
        var tenant = Tenant;
        _failure = null;
        _page = null;
        _members = [];
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant members.");
            var state = await Access.GetStateAsync(tenant, Lifetime.Token).ConfigureAwait(true);
            var page = await directory.ListMembersAsync(tenant, new TenantAccessPageRequest(), Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            _posture = state.Posture;
            _page = page ?? new TenantMemberPage();
            _members = _page.Entries;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task LoadAdminsAsync()
    {
        var tenant = Tenant;
        _admins = null;
        _adminsRead = false;
        try
        {
            var admins = await Admins.ReadAsync(tenant, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            _admins = admins;
            _adminsRead = true;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
    }

    private async Task LoadMoreAsync()
    {
        if (_page?.NextPageToken is not { } token || _loadingMore)
        {
            return;
        }

        var tenant = Tenant;
        _loadingMore = true;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant members.");
            var page = await directory.ListMembersAsync(tenant, new TenantAccessPageRequest { PageToken = token }, Lifetime.Token).ConfigureAwait(true)
                ?? new TenantMemberPage();
            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            _members = [.. _members, .. page.Entries];
            _page = page;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
        }
        finally
        {
            _loadingMore = false;
        }
    }

    private async Task AddAsync()
    {
        if (_page is null || _busy || AtCap)
        {
            return;
        }

        _addError = null;
        var subjectId = _subjectId.Trim();
        if (subjectId.Length == 0)
        {
            _addError = "Choose the member.";
            return;
        }

        if (_picker is { } picker && !await picker.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        var (tenant, kind) = (Tenant, _kind);
        _busy = true;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant members.");
            await directory.AddMemberAsync(tenant, subjectId, kind, Lifetime.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }
        catch (LatticeQuotaExceededException)
        {
            Access.Invalidate();
            _addError = $"Tenant {tenant} is at its cap of members, so {subjectId} was not added.";
            return;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _addError = TenantGroupFormat.RefusalMessage(exception, failure);
            return;
        }
        finally
        {
            _busy = false;
        }

        Access.Invalidate();
        _subjectId = string.Empty;
        Toasts.Show($"{subjectId} is now a member of tenant {tenant}.", LtToastTone.Success);
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task RemoveAsync()
    {
        var entry = _removing;
        _removing = null;
        if (entry is null)
        {
            return;
        }

        var tenant = Tenant;
        _busy = true;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant members.");
            await directory.RemoveMemberAsync(tenant, entry.SubjectId, entry.Kind, Lifetime.Token).ConfigureAwait(true);
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
        finally
        {
            _busy = false;
        }

        Access.Invalidate();
        Toasts.Show($"{entry.SubjectId} is no longer a member of tenant {tenant}.", LtToastTone.Success);
        await LoadAsync().ConfigureAwait(true);
    }
}
