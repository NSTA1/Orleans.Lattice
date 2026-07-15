using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
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
    private string _ruleTreeId = string.Empty;
    private LatticeScopeKind _ruleScopeKind = LatticeScopeKind.Tree;
    private string _ruleScopeKeyOrPrefix = string.Empty;
    private readonly HashSet<LatticeOperation> _ruleOperations = new();
    private LatticeEffect _ruleEffect = LatticeEffect.Allow;

    // ----- Explain / Effective -----
    private string _explainSubjectId = string.Empty;
    private LatticeOperation _explainOperation = LatticeOperation.Read;
    private string _explainTreeId = string.Empty;
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
        InvokeAsync(StateHasChanged);
    }

    private void SetTab(AccessTab tab)
    {
        if (_tab == tab)
        {
            return;
        }

        _tab = tab;
        _lastResult = null;
    }

    private async Task ReloadAsync()
    {
        _lastResult = null;
        switch (_tab)
        {
            case AccessTab.Users:
                await LoadUsersAsync(reset: true);
                break;
            case AccessTab.Groups:
                await LoadGroupsAsync(reset: true);
                break;
            case AccessTab.Policies:
                await LoadRulesAsync(reset: true);
                break;
            default:
                break;
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
        finally
        {
            _busy = false;
        }
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
                ResetUserForm();
                await LoadUsersAsync(reset: true);
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
                await LoadUsersAsync(reset: true);
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
        finally
        {
            _busy = false;
        }
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
                await LoadGroupsAsync(reset: true);
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
                await LoadGroupsAsync(reset: true);
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
        finally
        {
            _busy = false;
        }
    }

    private Task LoadMoreRulesAsync() => LoadRulesAsync(reset: false);

    private void EditRule(LatticeAuthorizationRule rule)
    {
        _editingExistingRule = true;
        _ruleIdInput = rule.RuleId;
        _ruleSubjectKind = rule.Subject.Kind;
        _ruleSubjectId = rule.Subject.Id;
        _ruleTreeId = rule.Scope.TreeId;
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
        _ruleTreeId = string.Empty;
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
        && !string.IsNullOrWhiteSpace(_ruleTreeId)
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
            var scope = BuildScope(_ruleScopeKind, _ruleTreeId.Trim(), _ruleScopeKeyOrPrefix);
            var operations = CombineOperations(_ruleOperations);
            var rule = new LatticeAuthorizationRule(_ruleIdInput.Trim(), subject, scope, operations, _ruleEffect);

            _lastResult = await Policy.PutRuleAsync(rule);
            if (_lastResult.IsSuccess)
            {
                await LoadRulesAsync(reset: true);
            }
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task DeleteRuleAsync()
    {
        if (_busy || !_allowed || !_editingExistingRule || string.IsNullOrWhiteSpace(_ruleTreeId) || string.IsNullOrWhiteSpace(_ruleIdInput))
        {
            return;
        }

        _busy = true;
        try
        {
            _lastResult = await Policy.DeleteRuleAsync(_ruleTreeId.Trim(), _ruleIdInput.Trim());
            if (_lastResult.IsSuccess)
            {
                ResetRuleForm();
                await LoadRulesAsync(reset: true);
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
        if (_busy || !_allowed || string.IsNullOrWhiteSpace(_explainSubjectId) || string.IsNullOrWhiteSpace(_explainTreeId))
        {
            return;
        }

        _busy = true;
        try
        {
            _effective = null;
            _effectiveRankedRules = Array.Empty<RankedRule>();
            var scope = BuildScope(_explainScopeKind, _explainTreeId.Trim(), _explainScopeKeyOrPrefix);
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
