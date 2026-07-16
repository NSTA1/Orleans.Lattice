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
        Users,
        Groups,
        Policies,
        Explain,
    }

    private AccessTab _tab = AccessTab.Users;
    private bool _busy;
    private bool _allowed;
    private AccessOperationResult? _lastResult;

    // ----- Tree selection (shared by the Policies and Explain tabs) -----
    private const int TreePageSize = 200;
    private readonly List<CatalogItem> _trees = new();
    private bool _treesLoading;
    private string? _treesError;
    private string? _selectedTreeId;

    // ----- Users -----
    private readonly List<AuthUser> _users = new();
    private string? _usersNextToken;
    private string? _selectedUserId;
    private bool _editingExistingUser;
    private string _userIdInput = string.Empty;
    private string _userDisplayInput = string.Empty;

    // ----- Groups -----
    private readonly List<AuthGroup> _groups = new();
    private string? _groupsNextToken;
    private string? _selectedGroupId;
    private bool _editingExistingGroup;
    private string _groupIdInput = string.Empty;
    private string _groupDisplayInput = string.Empty;
    private readonly List<string> _directMembers = new();
    private string _memberIdInput = string.Empty;
    private MembershipMemberKind _memberKind = MembershipMemberKind.User;

    // ----- Policies -----
    private readonly List<LatticeAuthorizationRule> _rules = new();
    private IReadOnlyList<RankedRule> _rankedRules = Array.Empty<RankedRule>();
    private string? _rulesNextToken;
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
        _allowed = Capabilities.Current.AuthAdminAllowed;
        Capabilities.Changed += OnCapabilitiesChanged;
        if (_allowed)
        {
            await LoadTreesAsync();
            await ReloadAsync();
        }
    }

    private void OnCapabilitiesChanged()
    {
        var allowed = Capabilities.Current.AuthAdminAllowed;
        if (allowed == _allowed)
        {
            return;
        }

        _allowed = allowed;
        InvokeAsync(async () =>
        {
            // When the gate freshly opens (for example after the connection reaches
            // the cluster or an admin signs in) populate the tree list so the
            // tree-scoped tabs are usable without a manual refresh.
            if (_allowed && _trees.Count == 0)
            {
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

        // Load the newly activated tab's data if it has not been loaded yet, so the
        // list (and, for the tree-scoped tabs, the subject drop-down) is populated
        // without requiring a manual Refresh.
        await LoadForTabAsync(force: false);
    }

    private async Task ReloadAsync()
    {
        _lastResult = null;
        await LoadForTabAsync(force: true);
    }

    /// <summary>
    /// Loads the data the active tab needs. The Policies and Explain tabs also load
    /// the users and groups so the shared subject drop-down is populated. When
    /// <paramref name="force"/> is false the membership lists are only loaded if
    /// still empty, so switching tabs does not clobber data already in view.
    /// </summary>
    private async Task LoadForTabAsync(bool force)
    {
        switch (_tab)
        {
            case AccessTab.Users:
                if (force || _users.Count == 0)
                {
                    await LoadUsersAsync(reset: true);
                }

                break;
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
    /// Loads the users and groups that back the shared subject drop-down on the
    /// Policies and Explain tabs. Uses the guarded loaders, so it is safe to call
    /// when not already busy.
    /// </summary>
    private async Task LoadSubjectListsAsync(bool force)
    {
        if (force || _users.Count == 0)
        {
            await LoadUsersAsync(reset: true);
        }

        if (force || _groups.Count == 0)
        {
            await LoadGroupsAsync(reset: true);
        }
    }

    // ----- Users -----

    private async Task LoadUsersAsync(bool reset)
    {
        if (_busy || !_allowed)
        {
            return;
        }

        _busy = true;
        try
        {
            await LoadUsersCoreAsync(reset);
        }
        finally
        {
            _busy = false;
        }
    }

    // The core list load without the busy guard, so a mutation (which already holds
    // the busy flag) can repopulate the list before re-selecting the affected item.
    private async Task LoadUsersCoreAsync(bool reset)
    {
        if (!_allowed)
        {
            return;
        }

        var view = await Membership.ListUsersAsync(pageToken: reset ? null : _usersNextToken);
        if (!view.IsSuccess)
        {
            _lastResult = ToResult(view.Status, view.Message);
            return;
        }

        if (reset)
        {
            _users.Clear();
        }

        _users.AddRange(view.Entries);
        _usersNextToken = view.NextPageToken;
    }

    private Task LoadMoreUsersAsync() => LoadUsersAsync(reset: false);

    private void SelectUser(AuthUser user)
    {
        _selectedUserId = user.UserId;
        _editingExistingUser = true;
        _userIdInput = user.UserId;
        _userDisplayInput = user.DisplayName ?? string.Empty;
    }

    private void ResetUserForm()
    {
        _selectedUserId = null;
        _editingExistingUser = false;
        _userIdInput = string.Empty;
        _userDisplayInput = string.Empty;
    }

    private async Task SaveUserAsync()
    {
        if (_busy || !_allowed || string.IsNullOrWhiteSpace(_userIdInput))
        {
            return;
        }

        _busy = true;
        try
        {
            var user = new AuthUser
            {
                UserId = _userIdInput.Trim(),
                DisplayName = string.IsNullOrWhiteSpace(_userDisplayInput) ? null : _userDisplayInput.Trim(),
            };
            _lastResult = await Membership.UpsertUserAsync(user);
            if (_lastResult.IsSuccess)
            {
                // Repopulate the list, then keep the just-saved user selected and
                // highlighted so the operator sees the result of their action.
                await LoadUsersCoreAsync(reset: true);
                SelectUser(user);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task DeleteUserAsync()
    {
        if (_busy || !_allowed || _selectedUserId is null)
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await Membership.DeleteUserAsync(_selectedUserId);
            if (_lastResult.IsSuccess)
            {
                ResetUserForm();
                await LoadUsersCoreAsync(reset: true);
            }
        }
        finally
        {
            _busy = false;
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
        await LoadDirectMembersAsync();
    }

    private void ResetGroupForm()
    {
        _selectedGroupId = null;
        _editingExistingGroup = false;
        _groupIdInput = string.Empty;
        _groupDisplayInput = string.Empty;
        _directMembers.Clear();
        _memberIdInput = string.Empty;
        _memberKind = MembershipMemberKind.User;
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
        }
        else
        {
            _lastResult = ToResult(view.Status, view.Message);
        }
    }

    private async Task SaveGroupAsync()
    {
        if (_busy || !_allowed || string.IsNullOrWhiteSpace(_groupIdInput))
        {
            return;
        }

        _busy = true;
        try
        {
            var group = new AuthGroup
            {
                GroupId = _groupIdInput.Trim(),
                DisplayName = string.IsNullOrWhiteSpace(_groupDisplayInput) ? null : _groupDisplayInput.Trim(),
            };
            _lastResult = await Membership.UpsertGroupAsync(group);
            if (_lastResult.IsSuccess)
            {
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
        if (_busy || !_allowed || _selectedGroupId is null)
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await Membership.DeleteGroupAsync(_selectedGroupId);
            if (_lastResult.IsSuccess)
            {
                ResetGroupForm();
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
        if (_busy || !_allowed || _selectedGroupId is null || string.IsNullOrWhiteSpace(_memberIdInput))
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await Membership.AddMemberAsync(_selectedGroupId, _memberIdInput.Trim(), _memberKind);
            if (_lastResult.IsSuccess)
            {
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
        if (_busy || !_allowed || _selectedGroupId is null)
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await Membership.RemoveMemberAsync(_selectedGroupId, memberId);
            if (_lastResult.IsSuccess)
            {
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
    }

    private Task LoadMoreRulesAsync() => LoadRulesAsync(reset: false);

    private void EditRule(LatticeAuthorizationRule rule)
    {
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
            var view = await Policy.ExplainAsync(_explainSubjectId.Trim(), _explainOperation, scope);
            if (view.IsSuccess && view.Explanation is not null)
            {
                _explanation = view.Explanation;
                _explainRankedRules = RulePrecedence.Rank(view.Explanation.MatchedRules);
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
