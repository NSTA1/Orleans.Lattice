using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Access.Workspace;

/// <summary>
/// The Groups sub-surface: the membership directory's groups, the create / edit
/// form, and a selected group's direct members.
/// </summary>
public sealed partial class AccessWorkspace
{
    private readonly List<AuthGroup> _groups = [];
    private readonly List<string> _directMembers = [];
    private MembershipMemberKind _memberKind = MembershipMemberKind.User;

    // The selected group's friendly display name, captured at selection time so the
    // 'Direct members of X' heading and the client-side add / remove status banner
    // render the name (falling back to the id) without re-resolving.
    private string _selectedGroupDisplayName = string.Empty;

    /// <summary>The loaded page of groups, in server order.</summary>
    public IReadOnlyList<AuthGroup> Groups => _groups;

    /// <summary>The selected group's direct members, in server order.</summary>
    public IReadOnlyList<string> DirectMembers => _directMembers;

    /// <summary>The continuation token for the next page of groups, or <see langword="null"/>.</summary>
    public string? GroupsNextToken { get; private set; }

    /// <summary>The selected group's id, or <see langword="null"/> when none is selected.</summary>
    public string? SelectedGroupId { get; private set; }

    /// <summary>Whether the form is editing an existing group rather than creating one.</summary>
    public bool EditingExistingGroup { get; private set; }

    /// <summary>Whether the group create / edit form is open.</summary>
    public bool GroupFormOpen { get; private set; }

    /// <summary>The group id being created or edited.</summary>
    public string GroupIdInput { get; set; } = string.Empty;

    /// <summary>The optional display name being created or edited.</summary>
    public string GroupDisplayInput { get; set; } = string.Empty;

    /// <summary>The inline reason a create was blocked, or <see langword="null"/>.</summary>
    public string? GroupCreateError { get; private set; }

    /// <summary>The member id being added to the selected group.</summary>
    public string MemberIdInput { get; set; } = string.Empty;

    /// <summary>
    /// Bridges the shared subject picker (which speaks
    /// <see cref="LatticeSubjectSelectorKind"/>) to the membership member kind.
    /// Both enums share User=0/Group=1 semantics.
    /// </summary>
    public LatticeSubjectSelectorKind MemberPickerKind
    {
        get => _memberKind == MembershipMemberKind.Group
            ? LatticeSubjectSelectorKind.Group
            : LatticeSubjectSelectorKind.User;
        set => _memberKind = value == LatticeSubjectSelectorKind.Group
            ? MembershipMemberKind.Group
            : MembershipMemberKind.User;
    }

    /// <summary>
    /// The friendly label for the currently selected group: its captured display
    /// name, or the group id when no display name is set.
    /// </summary>
    public string SelectedGroupLabel =>
        string.IsNullOrWhiteSpace(_selectedGroupDisplayName)
            ? SelectedGroupId ?? string.Empty
            : _selectedGroupDisplayName;

    /// <summary>Loads the next page of groups.</summary>
    public Task LoadMoreGroupsAsync() => LoadGroupsAsync(reset: false);

    /// <summary>Opens the empty create form (the "New group" call to action).</summary>
    public void NewGroup()
    {
        if (!MembershipEditable)
        {
            return;
        }

        ResetGroupForm();
        GroupFormOpen = true;
        RaiseChanged();
    }

    /// <summary>Opens the form pre-filled to edit an existing group (and loads its members).</summary>
    /// <param name="group">The group to edit.</param>
    public async Task EditGroupAsync(AuthGroup group)
    {
        await SelectGroupAsync(group);
        GroupFormOpen = true;
        RaiseChanged();
    }

    /// <summary>Selects a group, pre-filling the form and loading its direct members.</summary>
    /// <param name="group">The group to select.</param>
    public async Task SelectGroupAsync(AuthGroup group)
    {
        ArgumentNullException.ThrowIfNull(group);

        SelectedGroupId = group.GroupId;
        EditingExistingGroup = true;
        GroupIdInput = group.GroupId;
        GroupDisplayInput = group.DisplayName ?? string.Empty;
        _selectedGroupDisplayName = group.DisplayName ?? string.Empty;
        GroupCreateError = null;
        MemberIdInput = string.Empty;
        _memberKind = MembershipMemberKind.User;

        await LoadDirectMembersAsync();
        RaiseChanged();
    }

    /// <summary>Closes the form without saving.</summary>
    public void CancelGroupForm()
    {
        ResetGroupForm();
        GroupFormOpen = false;
        RaiseChanged();
    }

    /// <summary>
    /// Auto-fills the New group display-name field from a directory selection,
    /// but only when the picker surfaced a meaningful name (the model already
    /// yields empty for a cleared or free-text selection or one that merely
    /// echoes the id), so an operator's own edit is never clobbered.
    /// </summary>
    /// <param name="displayName">The display name the picker surfaced.</param>
    public void OnGroupDisplayNameSuggested(string displayName)
    {
        if (!string.IsNullOrWhiteSpace(displayName))
        {
            GroupDisplayInput = displayName;
            RaiseChanged();
        }
    }

    /// <summary>Creates or updates the group described by the form.</summary>
    public async Task SaveGroupAsync()
    {
        if (Busy || !Allowed || !MembershipEditable || string.IsNullOrWhiteSpace(GroupIdInput))
        {
            return;
        }

        Busy = true;
        try
        {
            GroupCreateError = null;

            // Fail closed for a NEW group: when a directory is available the chosen /
            // entered id must resolve to a real group, otherwise the create is blocked
            // with an inline reason. The edit path (an existing group) skips this.
            if (!EditingExistingGroup)
            {
                var decision = await AccessModel.ValidateAsync(GroupIdInput, DirectoryPrincipalKind.Group);
                if (decision.IsBlocked)
                {
                    GroupCreateError = decision.Reason;
                    return;
                }
            }

            var group = new AuthGroup
            {
                GroupId = GroupIdInput.Trim(),
                DisplayName = string.IsNullOrWhiteSpace(GroupDisplayInput) ? null : GroupDisplayInput.Trim(),
            };
            LastResult = await _domain.Membership.UpsertGroupAsync(group);
            if (LastResult.IsSuccess)
            {
                // Replace the server's raw-id success message with a friendly,
                // display-name status line composed client-side.
                var label = string.IsNullOrWhiteSpace(group.DisplayName) ? group.GroupId : group.DisplayName;
                LastResult = AccessOperationResult.Success($"Saved group '{label}'.");

                // Repopulate the list, then keep the just-saved group selected and
                // highlighted (and load its direct members) so the operator sees the
                // result of their action.
                await LoadGroupsCoreAsync(reset: true);
                await SelectGroupAsync(group);
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    /// <summary>Deletes the selected group.</summary>
    public async Task DeleteGroupAsync()
    {
        if (Busy || !Allowed || !MembershipEditable || SelectedGroupId is null)
        {
            return;
        }

        Busy = true;
        try
        {
            // Capture a friendly label before the reset clears the form fields.
            var label = SelectedGroupLabel;
            LastResult = await _domain.Membership.DeleteGroupAsync(SelectedGroupId);
            if (LastResult.IsSuccess)
            {
                LastResult = AccessOperationResult.Success($"Deleted group '{label}'.");
                ResetGroupForm();
                GroupFormOpen = false;
                await LoadGroupsCoreAsync(reset: true);
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    /// <summary>Adds the entered member to the selected group.</summary>
    public async Task AddMemberAsync()
    {
        if (Busy || !Allowed || !MembershipEditable || SelectedGroupId is null
            || string.IsNullOrWhiteSpace(MemberIdInput))
        {
            return;
        }

        Busy = true;
        try
        {
            var memberId = MemberIdInput.Trim();
            LastResult = await _domain.Membership.AddMemberAsync(SelectedGroupId, memberId, _memberKind);
            if (LastResult.IsSuccess)
            {
                // Replace the server's raw-id success message with a friendly,
                // display-name status line resolved client-side.
                var memberLabel = await Labels.ResolveLabelAsync(memberId);
                LastResult = AccessOperationResult.Success($"Added {memberLabel} to {SelectedGroupLabel}.");
                MemberIdInput = string.Empty;
                await LoadDirectMembersAsync();
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    /// <summary>Removes a member from the selected group.</summary>
    /// <param name="memberId">The member id to remove.</param>
    public async Task RemoveMemberAsync(string memberId)
    {
        if (Busy || !Allowed || !MembershipEditable || SelectedGroupId is null)
        {
            return;
        }

        Busy = true;
        try
        {
            LastResult = await _domain.Membership.RemoveMemberAsync(SelectedGroupId, memberId);
            if (LastResult.IsSuccess)
            {
                // Replace the server's raw-id success message with a friendly,
                // display-name status line resolved client-side.
                var memberLabel = await Labels.ResolveLabelAsync(memberId);
                LastResult = AccessOperationResult.Success($"Removed {memberLabel} from {SelectedGroupLabel}.");
                await LoadDirectMembersAsync();
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    private async Task LoadGroupsAsync(bool reset)
    {
        if (Busy || !Allowed)
        {
            return;
        }

        Busy = true;
        try
        {
            await LoadGroupsCoreAsync(reset);
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    // The core list load without the busy guard, so a mutation (which already holds
    // the busy flag) can repopulate the list before re-selecting the affected group.
    private async Task LoadGroupsCoreAsync(bool reset)
    {
        if (!Allowed)
        {
            return;
        }

        var view = await _domain.Membership.ListGroupsAsync(pageToken: reset ? null : GroupsNextToken);
        if (!view.IsSuccess)
        {
            LastResult = ToResult(view.Status, view.Message);
            return;
        }

        if (reset)
        {
            _groups.Clear();
        }

        _groups.AddRange(view.Entries);
        GroupsNextToken = view.NextPageToken;
    }

    private void ResetGroupForm()
    {
        SelectedGroupId = null;
        EditingExistingGroup = false;
        GroupIdInput = string.Empty;
        GroupDisplayInput = string.Empty;
        _selectedGroupDisplayName = string.Empty;
        GroupCreateError = null;
        _directMembers.Clear();
        MemberIdInput = string.Empty;
        _memberKind = MembershipMemberKind.User;
    }

    private async Task LoadDirectMembersAsync()
    {
        if (SelectedGroupId is null)
        {
            return;
        }

        var view = await _domain.Membership.ListDirectMembersAsync(SelectedGroupId);
        _directMembers.Clear();
        if (view.IsSuccess)
        {
            _directMembers.AddRange(view.Entries);
            // Warm the label cache for every member in view (bounded by the page) so
            // each row upgrades from its raw id to a friendly display name on render.
            await Labels.ResolveManyAsync(_directMembers);
        }
        else
        {
            LastResult = ToResult(view.Status, view.Message);
        }
    }
}
