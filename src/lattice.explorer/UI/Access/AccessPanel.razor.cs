using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Access;

/// <summary>
/// The Access (membership &amp; access-control) area's interactive panel. Drives
/// the membership and policy admin services over the auth-admin control plane and
/// the facade-computed Explain / EffectivePermissions introspection. Every action
/// is gated on the advisory <see cref="ExplorerCapabilities.AuthAdminAllowed"/>
/// flag (rendering disabled, not hidden, when denied) and folds a server denial
/// into a clean status banner rather than surfacing an unhandled error.
/// </summary>
public partial class AccessPanel : ComponentBase, IDisposable
{
    /// <summary>The active sub-tab of the Access area.</summary>
    private enum AccessTab
    {
        Groups,
        Policies,
        Explain,
    }

    private AccessTab _tab = AccessTab.Groups;
    private bool _busy;
    private bool _allowed;
    private bool _authenticationRequired;
    private AccessOperationResult? _lastResult;

    // The extracted create-form / access-state model: directory availability, the
    // provider explanation, the auth-mode + enforcement banner, and the
    // resolve-and-block decision for a new principal. Constructed in
    // OnInitializedAsync once the injected membership service is available.
    private AccessCreateModel _accessModel = null!;

    // Whether locally-defined group and member editing is meaningful for this
    // cluster. False only when the access model was read successfully and reports
    // token-only membership, in which case the editing surface is disabled but stays
    // read-only viewable. The server remains the enforcement point.
    private bool _membershipEditable => _accessModel.MembershipEditingEnabled;

    // ----- Tree selection (shared by the Policies and Explain tabs) -----
    private const int TreePageSize = 200;
    private readonly List<CatalogItem> _trees = new();
    private bool _treesLoading;
    private string? _treesError;
    private string? _selectedTreeId;

    // ----- Groups -----
    private readonly List<AuthGroup> _groups = new();
    private string? _groupsNextToken;
    private string? _selectedGroupId;
    private bool _editingExistingGroup;
    private bool _groupFormOpen;
    private string _groupIdInput = string.Empty;
    private string _groupDisplayInput = string.Empty;
    private string? _groupCreateError;
    private readonly List<string> _directMembers = new();
    private string _memberIdInput = string.Empty;
    private MembershipMemberKind _memberKind = MembershipMemberKind.User;

    // The selected group's friendly display name, captured at selection time so the
    // 'Direct members of X' heading and the client-side add / remove status banner
    // render the name (falling back to the id) without re-resolving.
    private string _selectedGroupDisplayName = string.Empty;

    // Bridges the shared SubjectPicker (which speaks LatticeSubjectSelectorKind)
    // to the membership member kind. Both enums share User=0/Group=1 semantics.
    private LatticeSubjectSelectorKind MemberPickerKind
    {
        get => _memberKind == MembershipMemberKind.Group
            ? LatticeSubjectSelectorKind.Group
            : LatticeSubjectSelectorKind.User;
        set => _memberKind = value == LatticeSubjectSelectorKind.Group
            ? MembershipMemberKind.Group
            : MembershipMemberKind.User;
    }

    // ----- Policies -----
    private readonly List<LatticeAuthorizationRule> _rules = new();
    private IReadOnlyList<RankedRule> _rankedRules = Array.Empty<RankedRule>();
    private string? _rulesNextToken;
    private bool _ruleFormOpen;
    private bool _editingExistingRule;
    private string _ruleIdInput = string.Empty;
    private LatticeSubjectSelectorKind _ruleSubjectKind = LatticeSubjectSelectorKind.User;
    private string _ruleSubjectId = string.Empty;
    private LatticeScopeKind _ruleScopeKind = LatticeScopeKind.Tree;
    private string _ruleScopeKeyOrPrefix = string.Empty;
    private readonly HashSet<LatticeOperation> _ruleOperations = new();
    private LatticeEffect _ruleEffect = LatticeEffect.Allow;

    // ----- Explain / Effective -----
    private LatticeSubjectSelectorKind _explainSubjectKind = LatticeSubjectSelectorKind.User;
    private string _explainSubjectId = string.Empty;
    private LatticeOperation _explainOperation = LatticeOperation.Read;
    private LatticeScopeKind _explainScopeKind = LatticeScopeKind.Tree;
    private string _explainScopeKeyOrPrefix = string.Empty;
    private AuthExplanation? _explanation;
    private IReadOnlyList<RankedRule> _explainRankedRules = Array.Empty<RankedRule>();
    private AuthEffectivePermissions? _effective;
    private IReadOnlyList<RankedRule> _effectiveRankedRules = Array.Empty<RankedRule>();

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _accessModel = new AccessCreateModel(Membership);
        _allowed = Capabilities.Current.AuthAdminAllowed;
        _authenticationRequired = Capabilities.Current.AuthAdminAuthenticationRequired;
        Capabilities.Changed += OnCapabilitiesChanged;
        if (_allowed)
        {
            await LoadAccessModelAsync();
            await LoadTreesAsync();
            await ReloadAsync();
        }
    }

    private void OnCapabilitiesChanged()
    {
        var allowed = Capabilities.Current.AuthAdminAllowed;
        var authenticationRequired = Capabilities.Current.AuthAdminAuthenticationRequired;
        if (allowed == _allowed && authenticationRequired == _authenticationRequired)
        {
            return;
        }

        _allowed = allowed;
        _authenticationRequired = authenticationRequired;
        InvokeAsync(async () =>
        {
            // When the gate freshly opens (for example after the connection reaches
            // the cluster or an admin signs in) populate the tree list so the
            // tree-scoped tabs are usable without a manual refresh, and read the
            // access model so the create forms and enforcement banner are accurate.
            if (_allowed && _trees.Count == 0)
            {
                await LoadAccessModelAsync();
                await LoadTreesAsync();
            }

            StateHasChanged();
        });
    }

    // ----- Tree selection (shared by Policies and Explain) -----

    /// <summary>
    /// Loads the full tree catalog into the shared left selection panel through the
    /// same state-API connection the Explore area uses. Trees are the policy scope
    /// unit, so only trees (not views or tag indexes) are listed, and restore-shadow
    /// trees are filtered out. A discovery failure surfaces as a retryable error
    /// rather than an unhandled exception.
    /// </summary>
    private async Task LoadTreesAsync()
    {
        _treesLoading = true;
        _treesError = null;
        await InvokeAsync(StateHasChanged);

        try
        {
            var loaded = new List<CatalogItem>();
            string? token = null;
            do
            {
                var page = await CatalogReader.LoadAsync(CatalogKind.Trees, token, TreePageSize);
                foreach (var item in page.Items)
                {
                    if (!item.IsRestoreShadow)
                    {
                        loaded.Add(item);
                    }
                }

                token = page.NextPageToken;
            }
            while (token is not null);

            _trees.Clear();
            _trees.AddRange(loaded);
        }
        catch (Exception ex)
        {
            _treesError = ex.Message;
        }
        finally
        {
            _treesLoading = false;
            await InvokeAsync(StateHasChanged);
        }
    }

    private Task RefreshTreesAsync() => LoadTreesAsync();

    /// <summary>
    /// Selects a tree from the shared panel, pinning it as the active tree for rule
    /// authoring (Policies) and Explain. Selection is presentation state only; it
    /// does not itself issue a request.
    /// </summary>
    private void SelectTree(string treeId)
    {
        if (_busy)
        {
            return;
        }

        _selectedTreeId = treeId;
    }

    private async Task SetTab(AccessTab tab)
    {
        if (_tab == tab)
        {
            return;
        }

        _tab = tab;
        _lastResult = null;

        // Leaving a tab closes any open create/edit form so the user always returns
        // to the list-first view with an explicit call to action.
        _groupFormOpen = false;
        _ruleFormOpen = false;

        // Load the newly activated tab's data if it has not been loaded yet, so the
        // list (and, for the tree-scoped tabs, the subject drop-down) is populated
        // without requiring a manual Refresh.
        await LoadForTabAsync(force: false);
    }

    private async Task ReloadAsync()
    {
        _lastResult = null;
        await LoadAccessModelAsync();
        await LoadForTabAsync(force: true);
    }

    /// <summary>
    /// Reads the cluster's best-effort access model into <see cref="_accessModel"/>
    /// so the create forms know whether to fail closed against a directory, what a
    /// valid id is for this deployment, and whether the active authorizer actually
    /// enforces the recorded rules and membership. A denial or transport failure
    /// folds into the safe unavailable snapshot rather than throwing.
    /// </summary>
    private async Task LoadAccessModelAsync()
    {
        if (!_allowed)
        {
            return;
        }

        _accessModel.Apply(await Membership.GetAccessModelAsync());
    }

    /// <summary>
    /// Loads the data the active tab needs. The Policies and Explain tabs also load
    /// the groups so the shared subject drop-down is populated. When
    /// <paramref name="force"/> is false the membership lists are only loaded if
    /// still empty, so switching tabs does not clobber data already in view.
    /// </summary>
    private async Task LoadForTabAsync(bool force)
    {
        switch (_tab)
        {
            case AccessTab.Groups:
                if (force || _groups.Count == 0)
                {
                    await LoadGroupsAsync(reset: true);
                }

                break;
            case AccessTab.Policies:
                if (force || _rules.Count == 0)
                {
                    await LoadRulesAsync(reset: true);
                }

                await LoadSubjectListsAsync(force);
                break;
            case AccessTab.Explain:
                await LoadSubjectListsAsync(force);
                break;
            default:
                break;
        }
    }

    /// <summary>
    /// Loads the groups that back the shared subject drop-down on the
    /// Policies and Explain tabs. Uses the guarded loaders, so it is safe to call
    /// when not already busy.
    /// </summary>
    private async Task LoadSubjectListsAsync(bool force)
    {
        if (force || _groups.Count == 0)
        {
            await LoadGroupsAsync(reset: true);
        }
    }

    // ----- Groups -----

    private async Task LoadGroupsAsync(bool reset)
    {
        if (_busy || !_allowed)
        {
            return;
        }

        _busy = true;
        try
        {
            await LoadGroupsCoreAsync(reset);
        }
        finally
        {
            _busy = false;
        }
    }

    // The core list load without the busy guard, so a mutation (which already holds
    // the busy flag) can repopulate the list before re-selecting the affected group.
    private async Task LoadGroupsCoreAsync(bool reset)
    {
        if (!_allowed)
        {
            return;
        }

        var view = await Membership.ListGroupsAsync(pageToken: reset ? null : _groupsNextToken);
        if (!view.IsSuccess)
        {
            _lastResult = ToResult(view.Status, view.Message);
            return;
        }

        if (reset)
        {
            _groups.Clear();
        }

        _groups.AddRange(view.Entries);
        _groupsNextToken = view.NextPageToken;
    }

    private Task LoadMoreGroupsAsync() => LoadGroupsAsync(reset: false);

    private async Task SelectGroupAsync(AuthGroup group)
    {
        _selectedGroupId = group.GroupId;
        _editingExistingGroup = true;
        _groupIdInput = group.GroupId;
        _groupDisplayInput = group.DisplayName ?? string.Empty;
        _selectedGroupDisplayName = group.DisplayName ?? string.Empty;
        _groupCreateError = null;
        _memberIdInput = string.Empty;
        _memberKind = MembershipMemberKind.User;

        await LoadDirectMembersAsync();
    }

    // Opens the empty create form (the "New group" call to action).
    private void NewGroup()
    {
        if (!_membershipEditable)
        {
            return;
        }

        ResetGroupForm();
        _groupFormOpen = true;
    }

    // Opens the form pre-filled to edit an existing group (and load its members).
    private async Task EditGroupAsync(AuthGroup group)
    {
        await SelectGroupAsync(group);
        _groupFormOpen = true;
    }

    // Closes the form without saving.
    private void CancelGroupForm()
    {
        ResetGroupForm();
        _groupFormOpen = false;
    }

    private void ResetGroupForm()
    {
        _selectedGroupId = null;
        _editingExistingGroup = false;
        _groupIdInput = string.Empty;
        _groupDisplayInput = string.Empty;
        _selectedGroupDisplayName = string.Empty;
        _groupCreateError = null;
        _directMembers.Clear();
        _memberIdInput = string.Empty;
        _memberKind = MembershipMemberKind.User;
    }

    /// <summary>
    /// Auto-fills the New group display-name field from a directory selection, but
    /// only when the picker surfaced a meaningful name (the model already yields
    /// empty for a cleared or free-text selection or one that merely echoes the
    /// id), so an operator's own edit is never clobbered.
    /// </summary>
    private void OnGroupDisplayNameSuggested(string displayName)
    {
        if (!string.IsNullOrWhiteSpace(displayName))
        {
            _groupDisplayInput = displayName;
        }
    }

    private async Task LoadDirectMembersAsync()
    {
        if (_selectedGroupId is null)
        {
            return;
        }

        var view = await Membership.ListDirectMembersAsync(_selectedGroupId);
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
            _lastResult = ToResult(view.Status, view.Message);
        }
    }

    private async Task SaveGroupAsync()
    {
        if (_busy || !_allowed || !_membershipEditable || string.IsNullOrWhiteSpace(_groupIdInput))
        {
            return;
        }

        _busy = true;
        try
        {
            _groupCreateError = null;

            // Fail closed for a NEW group: when a directory is available the chosen /
            // entered id must resolve to a real group, otherwise the create is blocked
            // with an inline reason. The edit path (an existing group) skips this.
            if (!_editingExistingGroup)
            {
                var decision = await _accessModel.ValidateAsync(_groupIdInput, DirectoryPrincipalKind.Group);
                if (decision.IsBlocked)
                {
                    _groupCreateError = decision.Reason;
                    return;
                }
            }

            var group = new AuthGroup
            {
                GroupId = _groupIdInput.Trim(),
                DisplayName = string.IsNullOrWhiteSpace(_groupDisplayInput) ? null : _groupDisplayInput.Trim(),
            };
            _lastResult = await Membership.UpsertGroupAsync(group);
            if (_lastResult.IsSuccess)
            {
                // Replace the server's raw-id success message with a friendly,
                // display-name status line composed client-side.
                var label = string.IsNullOrWhiteSpace(group.DisplayName) ? group.GroupId : group.DisplayName;
                _lastResult = AccessOperationResult.Success($"Saved group '{label}'.");

                // Repopulate the list, then keep the just-saved group selected and
                // highlighted (and load its direct members) so the operator sees the
                // result of their action.
                await LoadGroupsCoreAsync(reset: true);
                await SelectGroupAsync(group);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task DeleteGroupAsync()
    {
        if (_busy || !_allowed || !_membershipEditable || _selectedGroupId is null)
        {
            return;
        }

        _busy = true;
        try
        {
            // Capture a friendly label before the reset clears the form fields.
            var label = SelectedGroupLabel;
            _lastResult = await Membership.DeleteGroupAsync(_selectedGroupId);
            if (_lastResult.IsSuccess)
            {
                _lastResult = AccessOperationResult.Success($"Deleted group '{label}'.");
                ResetGroupForm();
                _groupFormOpen = false;
                await LoadGroupsCoreAsync(reset: true);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task AddMemberAsync()
    {
        if (_busy || !_allowed || !_membershipEditable || _selectedGroupId is null || string.IsNullOrWhiteSpace(_memberIdInput))
        {
            return;
        }

        _busy = true;
        try
        {
            var memberId = _memberIdInput.Trim();
            _lastResult = await Membership.AddMemberAsync(_selectedGroupId, memberId, _memberKind);
            if (_lastResult.IsSuccess)
            {
                // Replace the server's raw-id success message with a friendly,
                // display-name status line resolved client-side.
                var memberLabel = await Labels.ResolveLabelAsync(memberId);
                _lastResult = AccessOperationResult.Success($"Added {memberLabel} to {SelectedGroupLabel}.");
                _memberIdInput = string.Empty;
                await LoadDirectMembersAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task RemoveMemberAsync(string memberId)
    {
        if (_busy || !_allowed || !_membershipEditable || _selectedGroupId is null)
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await Membership.RemoveMemberAsync(_selectedGroupId, memberId);
            if (_lastResult.IsSuccess)
            {
                // Replace the server's raw-id success message with a friendly,
                // display-name status line resolved client-side.
                var memberLabel = await Labels.ResolveLabelAsync(memberId);
                _lastResult = AccessOperationResult.Success($"Removed {memberLabel} from {SelectedGroupLabel}.");
                await LoadDirectMembersAsync();
            }
        }
        finally
        {
            _busy = false;
        }
    }

    // ----- Policies -----

    private async Task LoadRulesAsync(bool reset)
    {
        if (_busy || !_allowed)
        {
            return;
        }

        _busy = true;
        try
        {
            await LoadRulesCoreAsync(reset);
        }
        finally
        {
            _busy = false;
        }
    }

    // The core list load without the busy guard, so a mutation (which already holds
    // the busy flag) can repopulate the rule table.
    private async Task LoadRulesCoreAsync(bool reset)
    {
        if (!_allowed)
        {
            return;
        }

        var view = await Policy.ListRulesAsync(pageToken: reset ? null : _rulesNextToken);
        if (!view.IsSuccess)
        {
            _lastResult = ToResult(view.Status, view.Message);
            return;
        }

        if (reset)
        {
            _rules.Clear();
        }

        _rules.AddRange(view.Entries);
        _rulesNextToken = view.NextPageToken;
        _rankedRules = RulePrecedence.Rank(_rules);
        await ResolveRuleSubjectsAsync(_rankedRules);
    }

    private Task LoadMoreRulesAsync() => LoadRulesAsync(reset: false);

    private void EditRule(LatticeAuthorizationRule rule)
    {
        _ruleFormOpen = true;
        _editingExistingRule = true;
        _ruleIdInput = rule.RuleId;
        _ruleSubjectKind = rule.Subject.Kind;
        _ruleSubjectId = rule.Subject.Id;
        _selectedTreeId = rule.Scope.TreeId;
        _ruleScopeKind = rule.Scope.Kind;
        _ruleScopeKeyOrPrefix = rule.Scope.KeyOrPrefix ?? string.Empty;
        _ruleEffect = rule.Effect;
        _ruleOperations.Clear();
        foreach (var option in AccessRuleFormat.Operations)
        {
            if ((rule.Operations & option.Flag) == option.Flag)
            {
                _ruleOperations.Add(option.Flag);
            }
        }
    }

    private void ResetRuleForm()
    {
        _editingExistingRule = false;
        _ruleIdInput = string.Empty;
        _ruleSubjectKind = LatticeSubjectSelectorKind.User;
        _ruleSubjectId = string.Empty;
        _ruleScopeKind = LatticeScopeKind.Tree;
        _ruleScopeKeyOrPrefix = string.Empty;
        _ruleOperations.Clear();
        _ruleEffect = LatticeEffect.Allow;
    }

    private void NewRule()
    {
        ResetRuleForm();
        _ruleFormOpen = true;
    }

    private void CancelRuleForm()
    {
        ResetRuleForm();
        _ruleFormOpen = false;
    }

    private void ToggleOperation(LatticeOperation flag, bool enabled)
    {
        if (enabled)
        {
            _ruleOperations.Add(flag);
        }
        else
        {
            _ruleOperations.Remove(flag);
        }
    }

    private bool CanSaveRule() =>
        !string.IsNullOrWhiteSpace(_ruleIdInput)
        && !string.IsNullOrWhiteSpace(_ruleSubjectId)
        && !string.IsNullOrWhiteSpace(_selectedTreeId)
        && _ruleOperations.Count > 0
        && (_ruleScopeKind == LatticeScopeKind.Tree || !string.IsNullOrWhiteSpace(_ruleScopeKeyOrPrefix));

    private async Task SaveRuleAsync()
    {
        if (_busy || !_allowed || !CanSaveRule())
        {
            return;
        }

        _busy = true;
        try
        {
            var subject = _ruleSubjectKind == LatticeSubjectSelectorKind.Group
                ? LatticeSubjectSelector.Group(_ruleSubjectId.Trim())
                : LatticeSubjectSelector.User(_ruleSubjectId.Trim());
            var scope = BuildScope(_ruleScopeKind, _selectedTreeId!.Trim(), _ruleScopeKeyOrPrefix);
            var operations = CombineOperations(_ruleOperations);
            var rule = new LatticeAuthorizationRule(_ruleIdInput.Trim(), subject, scope, operations, _ruleEffect);

            _lastResult = await Policy.PutRuleAsync(rule);
            if (_lastResult.IsSuccess)
            {
                await LoadRulesCoreAsync(reset: true);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task DeleteRuleAsync()
    {
        if (_busy || !_allowed || !_editingExistingRule || string.IsNullOrWhiteSpace(_selectedTreeId) || string.IsNullOrWhiteSpace(_ruleIdInput))
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await Policy.DeleteRuleAsync(_selectedTreeId.Trim(), _ruleIdInput.Trim());
            if (_lastResult.IsSuccess)
            {
                ResetRuleForm();
                _ruleFormOpen = false;
                await LoadRulesCoreAsync(reset: true);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    // ----- Explain / Effective -----

    private async Task RunExplainAsync()
    {
        if (_busy || !_allowed || string.IsNullOrWhiteSpace(_explainSubjectId) || string.IsNullOrWhiteSpace(_selectedTreeId))
        {
            return;
        }

        _busy = true;
        try
        {
            _effective = null;
            _effectiveRankedRules = Array.Empty<RankedRule>();
            var scope = BuildScope(_explainScopeKind, _selectedTreeId!.Trim(), _explainScopeKeyOrPrefix);
            var view = await Policy.ExplainAsync(_explainSubjectId.Trim(), _explainOperation, scope, _explainSubjectKind);
            if (view.IsSuccess && view.Explanation is not null)
            {
                _explanation = view.Explanation;
                _explainRankedRules = RulePrecedence.Rank(view.Explanation.MatchedRules);
                await ResolveRuleSubjectsAsync(_explainRankedRules);
                await Labels.ResolveLabelAsync(view.Explanation.SubjectId);
                await Labels.ResolveManyAsync(view.Explanation.GroupIds);
                _lastResult = null;
            }
            else
            {
                _explanation = null;
                _explainRankedRules = Array.Empty<RankedRule>();
                _lastResult = ToResult(view.Status, view.Message);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task RunEffectiveAsync()
    {
        if (_busy || !_allowed || string.IsNullOrWhiteSpace(_explainSubjectId))
        {
            return;
        }

        _busy = true;
        try
        {
            _explanation = null;
            _explainRankedRules = Array.Empty<RankedRule>();
            var view = await Policy.EffectivePermissionsAsync(_explainSubjectId.Trim());
            if (view.IsSuccess && view.Permissions is not null)
            {
                _effective = view.Permissions;
                _effectiveRankedRules = RulePrecedence.Rank(view.Permissions.Rules);
                await ResolveRuleSubjectsAsync(_effectiveRankedRules);
                await Labels.ResolveLabelAsync(view.Permissions.SubjectId);
                await Labels.ResolveManyAsync(view.Permissions.GroupIds);
                _lastResult = null;
            }
            else
            {
                _effective = null;
                _effectiveRankedRules = Array.Empty<RankedRule>();
                _lastResult = ToResult(view.Status, view.Message);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    // ----- Helpers -----

    /// <summary>
    /// The friendly label for the currently selected group: its captured display
    /// name, or the group id when no display name is set.
    /// </summary>
    private string SelectedGroupLabel =>
        string.IsNullOrWhiteSpace(_selectedGroupDisplayName)
            ? _selectedGroupId ?? string.Empty
            : _selectedGroupDisplayName;

    // Warms the label cache for the subject id of every ranked rule about to be
    // rendered, so each subject cell upgrades from its raw id to a friendly name.
    // Bounded by the loaded rule page and only run on data load, never per render.
    private async Task ResolveRuleSubjectsAsync(IReadOnlyList<RankedRule> ranked)
    {
        if (ranked.Count == 0)
        {
            return;
        }

        var ids = new List<string>(ranked.Count);
        foreach (var rule in ranked)
        {
            ids.Add(rule.Rule.Subject.Id);
        }

        await Labels.ResolveManyAsync(ids);
    }

    private static LatticeScope BuildScope(LatticeScopeKind kind, string treeId, string keyOrPrefix) => kind switch
    {
        LatticeScopeKind.Key => LatticeScope.Key(treeId, keyOrPrefix.Trim()),
        LatticeScopeKind.Prefix => LatticeScope.Prefix(treeId, keyOrPrefix.Trim()),
        _ => LatticeScope.Tree(treeId),
    };

    private static LatticeOperation CombineOperations(IEnumerable<LatticeOperation> flags)
    {
        var combined = LatticeOperation.None;
        foreach (var flag in flags)
        {
            combined |= flag;
        }

        return combined;
    }

    private static AccessOperationResult ToResult(AccessOperationStatus status, string message) => status switch
    {
        AccessOperationStatus.Denied => AccessOperationResult.Denied(message),
        _ => AccessOperationResult.Failure(message),
    };

    private static string ResultClass(AccessOperationStatus status) => status switch
    {
        AccessOperationStatus.Succeeded => "is-success",
        AccessOperationStatus.Denied => "is-denied",
        _ => "is-failed",
    };

    /// <inheritdoc />
    public void Dispose() => Capabilities.Changed -= OnCapabilitiesChanged;
}
