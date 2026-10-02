using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The tenant's own groups (<c>/t/{tenant}/access/groups</c>): the tenant
/// directory's groups in pages, with each one's direct members and the rules and
/// app role bindings that name it; a create form validated against the local
/// group-name grammar (D1) and the tenant's <c>MaxGroups</c> cap (D13); and a
/// delete confirmed with the cascade it will apply.
/// </summary>
public partial class TenantGroupsView
{
    /// <summary>How many groups' usage is read at once.</summary>
    internal const int UsageBatch = 8;

    private static readonly Func<TenantGroupUsage, int?> MemberFigure = static usage => usage.MemberCount;
    private static readonly Func<TenantGroupUsage, int?> RuleFigure = static usage => usage.RuleCount;
    private static readonly Func<TenantGroupUsage, int?> AppRoleFigure = static usage => usage.AppRoleCount;

    private readonly Dictionary<string, TenantGroupUsage> _usage = new(StringComparer.Ordinal);
    private IReadOnlyList<TenantGroupDescriptor> _groups = [];
    private TenantGroupPage? _page;
    private TenantAccessPosture? _posture;
    private AccessFailure? _failure;
    private string? _loadedTenant;
    private bool _loadingMore;
    private bool _createOpen;
    private bool _saving;
    private string _newName = string.Empty;
    private string _newDisplayName = string.Empty;
    private string? _nameError;
    private string? _formError;
    private string? _deleting;
    private LtNameInput? _nameBox;
    private TenantGroupSuggestionSource? _existing;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    private string CountText
    {
        get
        {
            var more = _page?.NextPageToken is null ? string.Empty : ", more to load";
            return TenantGroupFormat.Count(_groups.Count, "group", "groups") + more;
        }
    }

    private bool AtCap => _posture is { } posture && TenantGroupFormat.AtCap(posture.Groups);

    private string? CapText => _posture is { } posture ? TenantGroupFormat.CapText(posture.Groups, "group", "groups") : null;

    private bool CanCreate => Access.Directory is not null && _failure is null && _page is not null && !AtCap;

    private string NameHint => $"1 to {LatticeTenantGroupId.MaxNameLength} lower-case letters, digits, '-', '_' or '.'. Tenant {Tenant} must not have a group of this name already.";

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedTenant, Tenant, StringComparison.Ordinal))
        {
            return;
        }

        _loadedTenant = Tenant;
        _existing = new TenantGroupSuggestionSource(Access, Tenant);
        _createOpen = false;
        _deleting = null;
        await LoadAsync().ConfigureAwait(true);
    }

    private string GroupHref(string name) => Href(AccessRoutes.TenantGroup(Tenant, name));

    private string Figure(TenantGroupDescriptor group, Func<TenantGroupUsage, int?> figure) =>
        _usage.TryGetValue(group.Name, out var usage)
            ? figure(usage) is { } value ? value.ToString(System.Globalization.CultureInfo.InvariantCulture) : "-"
            : string.Empty;

    private async Task LoadAsync()
    {
        var tenant = Tenant;
        _failure = null;
        _page = null;
        _groups = [];
        _usage.Clear();
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            var state = await Access.GetStateAsync(tenant, Lifetime.Token).ConfigureAwait(true);
            var page = await directory.ListGroupsAsync(tenant, new TenantAccessPageRequest(), Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            _posture = state.Posture;
            _page = page ?? new TenantGroupPage();
            _groups = _page.Entries;
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

        await ReadUsageAsync(tenant, _groups).ConfigureAwait(true);
    }

    private async Task LoadMoreAsync()
    {
        if (_page?.NextPageToken is not { } token || _loadingMore)
        {
            return;
        }

        var tenant = Tenant;
        _loadingMore = true;
        IReadOnlyList<TenantGroupDescriptor> added;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            var page = await directory.ListGroupsAsync(tenant, new TenantAccessPageRequest { PageToken = token }, Lifetime.Token).ConfigureAwait(true)
                ?? new TenantGroupPage();
            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            added = page.Entries;
            _groups = [.. _groups, .. added];
            _page = page;
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
            _loadingMore = false;
        }

        await ReadUsageAsync(tenant, added).ConfigureAwait(true);
    }

    /// <summary>
    /// Reads each group's usage a few at a time, re-rendering after each batch, so
    /// the table is shown at once and its figures fill in.
    /// </summary>
    private async Task ReadUsageAsync(string tenant, IReadOnlyList<TenantGroupDescriptor> groups)
    {
        var batch = new List<Task<TenantGroupUsage>>(UsageBatch);
        for (var start = 0; start < groups.Count; start += UsageBatch)
        {
            batch.Clear();
            var end = Math.Min(groups.Count, start + UsageBatch);
            for (var i = start; i < end; i++)
            {
                batch.Add(TenantGroupUsage.ReadAsync(Access, tenant, groups[i].Name, Lifetime.Token));
            }

            TenantGroupUsage[] read;
            try
            {
                read = await Task.WhenAll(batch).ConfigureAwait(true);
            }
            catch (OperationCanceledException) when (Lifetime.IsLeft)
            {
                return;
            }

            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            for (var i = 0; i < read.Length; i++)
            {
                _usage[groups[start + i].Name] = read[i];
            }

            StateHasChanged();
        }
    }

    private void OpenCreate()
    {
        if (!CanCreate)
        {
            return;
        }

        _newName = string.Empty;
        _newDisplayName = string.Empty;
        _nameError = null;
        _formError = null;
        _createOpen = true;
    }

    private Task<string?> ValidateNameAsync(string name, CancellationToken cancellationToken) =>
        Task.FromResult(TenantGroupFormat.NameError(Tenant, name));

    /// <summary>Checks the name against the grammar as it is typed, so a bad character is named at once.</summary>
    private void OnNameChanged(string value)
    {
        _newName = value;
        _formError = null;
        _nameError = value.Length == 0 ? null : TenantGroupFormat.NameError(Tenant, value);
    }

    private async Task CreateAsync()
    {
        if (_saving)
        {
            return;
        }

        var tenant = Tenant;
        _nameError = null;
        _formError = null;
        var name = _newName.Trim();
        if (TenantGroupFormat.NameError(tenant, name) is { } grammar)
        {
            _nameError = grammar;
            return;
        }

        _saving = true;
        TenantGroupDescriptor created;
        try
        {
            // The field's own checks - the grammar, a group the tenant already has -
            // answer first, beside the name; the exact look-up covers a group the
            // field could not see, and the directory validates again on write.
            if (_nameBox is { } box && !await box.ConfirmAsync().ConfigureAwait(true))
            {
                return;
            }

            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            if (await directory.GetGroupAsync(tenant, name, Lifetime.Token).ConfigureAwait(true) is not null)
            {
                _nameError = $"Tenant {tenant} already has a group named {name}.";
                return;
            }

            var displayName = _newDisplayName.Trim();
            created = await directory.UpsertGroupAsync(
                tenant,
                new TenantGroupDescriptor { Name = name, DisplayName = displayName.Length == 0 ? null : displayName },
                Lifetime.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }
        catch (LatticeQuotaExceededException)
        {
            _formError = $"The group was not created: tenant {tenant} is at its cap of groups. Remove one, or ask a platform operator to raise the cap.";
            Access.Invalidate();
            return;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            if (failure.Kind == AccessFailureKind.Invalid)
            {
                _nameError = failure.Message;
            }
            else
            {
                _formError = "The group was not created: " + failure.Message;
            }

            return;
        }
        finally
        {
            _saving = false;
        }

        _createOpen = false;
        Access.Invalidate();
        Toasts.Show($"Group {created.Name} created.", LtToastTone.Success);
        Navigator.NavigateTo(Navigator.Canonicalize(AccessRoutes.TenantGroup(tenant, created.Name)));
    }

    private void ConfirmDelete(string name) => _deleting = name;

    private async Task OnDeletedAsync(TenantGroupRemovalResult result)
    {
        _deleting = null;
        _loadedTenant = Tenant;
        await LoadAsync().ConfigureAwait(true);
    }
}
