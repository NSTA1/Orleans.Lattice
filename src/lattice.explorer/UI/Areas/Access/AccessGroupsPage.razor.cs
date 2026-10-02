using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The group list (<c>/access/groups</c>): the group catalogue in pages, a search,
/// and a "New group" form that validates the id against the identity directory
/// before it writes anything, and refuses an id that already names a group.
/// </summary>
public partial class AccessGroupsPage
{
    private const int PageSize = 200;

    private List<AuthGroup>? _groups;
    private IReadOnlyList<AuthGroup>? _visible;
    private List<AuthGroup>? _visibleSource;
    private string? _visibleSearch;
    private string? _next;
    private AccessFailure? _failure;
    private AccessModelDescriptor? _model;
    private string _search = string.Empty;
    private bool _loadingMore;
    private bool _createOpen;
    private bool _saving;
    private string _newId = string.Empty;
    private string _newName = string.Empty;
    private string? _idError;
    private string? _formError;
    private bool _loaded;
    private string? _loadedScope;
    private LtNameInput? _idBox;
    private AccessGroupNameSource? _existingGroups;
    private readonly AccessTenantGate _gate = new();

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal TenantAccessCatalog TenantAccess { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private bool MembershipEditable => _model?.LocalMembershipEffective != false;

    private IReadOnlyList<AuthGroup> Visible
    {
        get
        {
            if (_groups is null)
            {
                return [];
            }

            // Memoised per (loaded groups, search), so the table keeps one items
            // instance across renders and re-reads its rows only when they change.
            if (_visible is not null && ReferenceEquals(_visibleSource, _groups) && _visibleSearch == _search)
            {
                return _visible;
            }

            var term = _search.Trim();
            _visible = term.Length == 0
                ? _groups
                : [.. _groups.Where(group =>
                    group.GroupId.Contains(term, StringComparison.OrdinalIgnoreCase)
                    || (group.DisplayName?.Contains(term, StringComparison.OrdinalIgnoreCase) ?? false))];
            _visibleSource = _groups;
            _visibleSearch = _search;
            return _visible;
        }
    }

    private string CountText
    {
        get
        {
            var loaded = _groups?.Count ?? 0;
            var shown = Visible.Count;
            var more = _next is null ? string.Empty : ", more to load";
            return shown == loaded ? $"{loaded} groups{more}" : $"{shown} of {loaded} groups{more}";
        }
    }

    /// <summary>
    /// The tenant the page's address is rooted at, or <see langword="null"/> on
    /// the cluster-wide page. Groups belong to no tenant unless the tenant's access
    /// administration is delegated to the caller, so otherwise a tenant-rooted page
    /// lists none and says where they are.
    /// </summary>
    private string? Scope => Address.Tenant;

    /// <summary>
    /// The opening of the line a tenant-rooted page shows when the tenant's access
    /// administration is not delegated to the caller: why it lists no groups.
    /// </summary>
    /// <param name="state">The caller's standing towards the tenant.</param>
    /// <returns>The sentence.</returns>
    internal static string TenantCaveat(TenantAccessState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        return state.Standing switch
        {
            TenantAccessStanding.NotPermitted =>
                $"Tenant {state.Tenant}'s own groups are administered by its administrators, and you are not one of them; cluster groups belong to the whole cluster, not to one tenant.",
            TenantAccessStanding.Off =>
                $"Delegated tenant access administration is off, so groups belong to the whole cluster, not to one tenant, and tenant {state.Tenant}'s Access pages do not list them.",
            _ => $"Groups belong to the whole cluster, not to one tenant, so tenant {state.Tenant}'s Access pages do not list them.",
        };
    }

    private string ClusterWideHref => Navigator.Canonicalize(AccessRoutes.Groups.WithTenant(null)).ToHref();

    private string ClusterWideCreateHref => Navigator.Canonicalize(AccessRoutes.Groups.WithTenant(null).WithQuery(AccessRoutes.NewQuery, "true")).ToHref();

    private bool CreateRequested => string.Equals(Address.GetQuery(AccessRoutes.NewQuery), "true", StringComparison.Ordinal);

    private string IdHint => _model is { DirectoryAvailable: true } model
        ? (string.IsNullOrWhiteSpace(model.DirectoryExplanation)
            ? $"The id of a group in the identity directory ({AccessPrincipalValidation.DirectoryName(model)}) that is not defined here yet."
            : model.DirectoryExplanation)
        : "No identity directory is configured, so the id is used as typed. It must not name a group that already exists.";

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        base.OnInitialized();
        _existingGroups = new AccessGroupNameSource(Catalog);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        // The page is reused when only the tenant root changes, so what it shows
        // follows the address, not the instance.
        if (_loaded && string.Equals(_loadedScope, Scope, StringComparison.Ordinal))
        {
            return;
        }

        _loaded = true;
        _loadedScope = Scope;
        if (await _gate.ResolveAsync(TenantAccess, Scope).ConfigureAwait(true))
        {
            // The tenant's own groups: its view reads them.
            _createOpen = false;
            return;
        }

        _model ??= await Catalog.GetAccessModelAsync(CancellationToken.None).ConfigureAwait(true);
        _createOpen = Scope is null && MembershipEditable && string.Equals(Address.GetQuery(AccessRoutes.NewQuery), "true", StringComparison.Ordinal);
        if (Scope is null)
        {
            await LoadFirstPageAsync().ConfigureAwait(true);
        }
        else
        {
            _failure = null;
            _groups = null;
            _next = null;
        }
    }

    private async Task LoadFirstPageAsync()
    {
        _failure = null;
        _groups = null;
        _next = null;
        try
        {
            var page = await Catalog.Admin.ListGroupsAsync(new AuthPageRequest { PageSize = PageSize }).ConfigureAwait(true);
            _groups = [.. page.Entries];
            _next = page.NextPageToken;
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task LoadMoreAsync()
    {
        if (_next is null || _groups is null)
        {
            return;
        }

        _loadingMore = true;
        try
        {
            var page = await Catalog.Admin.ListGroupsAsync(new AuthPageRequest { PageSize = PageSize, PageToken = _next }).ConfigureAwait(true);
            _groups = [.. _groups, .. page.Entries];
            _next = page.NextPageToken;
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
        }
        finally
        {
            _loadingMore = false;
        }
    }

    private void OpenCreate()
    {
        _newId = string.Empty;
        _newName = string.Empty;
        _idError = null;
        _formError = null;
        _createOpen = true;
    }

    private Task<string?> ValidateInDirectoryAsync(string id, CancellationToken cancellationToken) =>
        AccessPrincipalValidation.ValidateAsync(Catalog.Admin, _model, id, DirectoryPrincipalKind.Group, cancellationToken);

    private async Task CreateAsync()
    {
        if (_saving)
        {
            return;
        }

        _idError = null;
        _formError = null;
        var id = _newId.Trim();
        if (id.Length == 0)
        {
            _idError = "Enter the group id.";
            return;
        }

        _saving = true;
        try
        {
            // The field's own checks - already a group, not in the directory - answer
            // first, beside the id; the exact look-up below covers a group the field
            // could not see, and the server validates again on write.
            if (_idBox is { } box)
            {
                if (!await box.ConfirmAsync().ConfigureAwait(true))
                {
                    return;
                }
            }
            else
            {
                _idError = await ValidateInDirectoryAsync(id, CancellationToken.None).ConfigureAwait(true);
                if (_idError is not null)
                {
                    return;
                }
            }

            if (await Catalog.Admin.GetGroupAsync(id).ConfigureAwait(true) is not null)
            {
                _idError = DuplicateMessage(id);
                return;
            }

            var name = _newName.Trim();
            await Catalog.Admin.UpsertGroupAsync(new AuthGroup { GroupId = id, DisplayName = name.Length == 0 ? null : name }).ConfigureAwait(true);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            if (failure.Kind is AccessFailureKind.DirectoryValidation or AccessFailureKind.Invalid)
            {
                _idError = failure.Message;
            }
            else
            {
                _formError = CreateRefusal(failure);
            }

            return;
        }
        finally
        {
            _saving = false;
        }

        _createOpen = false;
        Catalog.Invalidate();
        Toasts.Show($"Group {id} created.", LtToastTone.Success);
        Navigator.NavigateTo(Navigator.Canonicalize(AccessRoutes.Group(id)));
    }

    /// <summary>The sentence a duplicate group id is refused with.</summary>
    /// <param name="id">The id.</param>
    internal static string DuplicateMessage(string id) => $"A group named {id} already exists.";

    /// <summary>The sentence a refused create is shown with in the dialog, which stays open with the id kept.</summary>
    /// <param name="failure">The classified failure.</param>
    internal static string CreateRefusal(AccessFailure failure)
    {
        ArgumentNullException.ThrowIfNull(failure);
        var reason = failure.Message.Length == 0 ? "the cluster refused it." : char.ToLowerInvariant(failure.Message[0]) + failure.Message[1..];
        return "The group was not created: " + reason;
    }

    private string GroupHref(string groupId) => Navigator.Canonicalize(AccessRoutes.Group(groupId)).ToHref();
}
