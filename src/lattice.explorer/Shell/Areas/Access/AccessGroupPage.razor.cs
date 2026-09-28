using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// One group (<c>/access/groups/{id}</c>): rename it, add and remove direct
/// members (users or nested groups), see the groups it belongs to and the rules
/// that apply to it, and delete it. Member changes are refused while the cluster
/// resolves membership from tokens alone.
/// </summary>
public partial class AccessGroupPage
{
    private ExplorerAddress? _loaded;
    private AuthGroup? _group;
    private List<string>? _members;
    private IReadOnlyList<string>? _parents;
    private IReadOnlyList<LatticeAuthorizationRule> _rules = [];
    private HashSet<string> _knownGroups = new(StringComparer.Ordinal);
    private AccessFailure? _failure;
    private AccessModelDescriptor? _model;
    private string _displayName = string.Empty;
    private string? _displayNameError;
    private LatticeSubjectSelectorKind _memberKind = LatticeSubjectSelectorKind.User;
    private string _memberId = string.Empty;
    private string? _memberError;
    private string? _removing;
    private bool _deleteOpen;
    private bool _busy;

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    private string? GroupId => Address.Path.Count > 1 ? Address.Path[1] : null;

    private bool MembershipEditable => _model?.LocalMembershipEffective != false;

    private string ExplainHref => Navigator.Canonicalize(AccessRoutes.Explain
        .WithQuery(AccessRoutes.SubjectQuery, GroupId)
        .WithQuery(AccessRoutes.KindQuery, "group")
        .WithQuery(AccessRoutes.ViewQuery, AccessRoutes.PermissionsView)).ToHref();

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Equals(_loaded, Address))
        {
            return;
        }

        _loaded = Address;
        _model ??= await Catalog.GetAccessModelAsync(CancellationToken.None).ConfigureAwait(true);
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        _failure = null;
        _group = null;
        _members = null;
        var groupId = GroupId;
        if (string.IsNullOrEmpty(groupId))
        {
            Navigation.NotFound();
            return;
        }

        try
        {
            var group = await Catalog.Admin.GetGroupAsync(groupId).ConfigureAwait(true);
            if (group is null)
            {
                Navigation.NotFound();
                return;
            }

            var members = await Catalog.Admin.ListGroupMembersAsync(groupId).ConfigureAwait(true);
            var parents = await Catalog.Admin.ListSubjectGroupsAsync(groupId).ConfigureAwait(true);
            var permissions = await Catalog.Admin.EffectivePermissionsAsync(groupId, LatticeSubjectSelectorKind.Group).ConfigureAwait(true);
            var known = await Catalog.GetGroupsAsync(CancellationToken.None).ConfigureAwait(true);

            _group = group;
            _displayName = group.DisplayName ?? string.Empty;
            _members = [.. members];
            _parents = parents;
            _rules = AccessRuleFormat.InPrecedenceOrder(permissions.Rules);
            _knownGroups = known.Select(entry => entry.GroupId).ToHashSet(StringComparer.Ordinal);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private bool IsKnownGroup(string memberId) => _knownGroups.Contains(memberId);

    private string GroupHref(string groupId) => Navigator.Canonicalize(AccessRoutes.Group(groupId)).ToHref();

    private async Task SaveDisplayNameAsync()
    {
        if (_group is null || _busy)
        {
            return;
        }

        _busy = true;
        _displayNameError = null;
        try
        {
            var name = _displayName.Trim();
            var updated = _group with { DisplayName = name.Length == 0 ? null : name };
            await Catalog.Admin.UpsertGroupAsync(updated).ConfigureAwait(true);
            _group = updated;
            Catalog.Invalidate();
            Toasts.Show($"Group {updated.GroupId} renamed.", LtToastTone.Success);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            _displayNameError = failure.Message;
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task AddMemberAsync()
    {
        if (_group is null || _members is null || _busy || !MembershipEditable)
        {
            return;
        }

        _memberError = null;
        var memberId = _memberId.Trim();
        if (memberId.Length == 0)
        {
            _memberError = "Enter the member's id.";
            return;
        }

        if (string.Equals(memberId, _group.GroupId, StringComparison.Ordinal))
        {
            _memberError = "A group cannot be a member of itself.";
            return;
        }

        var kind = _memberKind == LatticeSubjectSelectorKind.Group ? MembershipMemberKind.Group : MembershipMemberKind.User;
        _busy = true;
        try
        {
            _memberError = await AccessPrincipalValidation.ValidateAsync(
                Catalog.Admin,
                _model,
                memberId,
                kind == MembershipMemberKind.Group ? DirectoryPrincipalKind.Group : DirectoryPrincipalKind.User).ConfigureAwait(true);
            if (_memberError is not null)
            {
                return;
            }

            await Catalog.Admin.AddMemberAsync(_group.GroupId, memberId, kind).ConfigureAwait(true);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            if (failure.Kind is AccessFailureKind.DirectoryValidation or AccessFailureKind.Invalid)
            {
                _memberError = failure.Message;
            }
            else
            {
                Toasts.Show(failure.Message, LtToastTone.Danger);
            }

            return;
        }
        finally
        {
            _busy = false;
        }

        if (!_members.Contains(memberId, StringComparer.Ordinal))
        {
            // A new list, not an in-place change: the table re-reads its rows only when its items change.
            _members = [.. _members.Append(memberId).Order(StringComparer.Ordinal)];
        }

        if (kind == MembershipMemberKind.Group)
        {
            _knownGroups.Add(memberId);
        }

        _memberId = string.Empty;
        Catalog.Invalidate();
        Toasts.Show($"{memberId} added to {_group.GroupId}.", LtToastTone.Success);
    }

    private void ConfirmRemove(string memberId) => _removing = memberId;

    private async Task RemoveMemberAsync()
    {
        var memberId = _removing;
        _removing = null;
        if (_group is null || _members is null || memberId is null || !MembershipEditable)
        {
            return;
        }

        _busy = true;
        try
        {
            await Catalog.Admin.RemoveMemberAsync(_group.GroupId, memberId).ConfigureAwait(true);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
            return;
        }
        finally
        {
            _busy = false;
        }

        _members = [.. _members.Where(member => !string.Equals(member, memberId, StringComparison.Ordinal))];
        Catalog.Invalidate();
        Toasts.Show($"{memberId} removed from {_group.GroupId}.", LtToastTone.Success);
    }

    private async Task DeleteAsync()
    {
        if (_group is null)
        {
            return;
        }

        var groupId = _group.GroupId;
        try
        {
            await Catalog.Admin.RemoveGroupAsync(groupId).ConfigureAwait(true);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
            return;
        }

        Catalog.Invalidate();
        Toasts.Show($"Group {groupId} deleted.", LtToastTone.Success);
        Navigator.NavigateTo(Navigator.Canonicalize(AccessRoutes.Groups));
    }
}
