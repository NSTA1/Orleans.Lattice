using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Access.Workspace;

/// <summary>
/// The Policies sub-surface: the precedence-ranked rule table for the selected
/// tree, and the rule authoring form (including the access-administration
/// delegation and all-trees affordances).
/// </summary>
public sealed partial class AccessWorkspace
{
    private readonly List<LatticeAuthorizationRule> _rules = [];
    private readonly HashSet<LatticeOperation> _ruleOperations = [];

    /// <summary>The loaded page of rules, in server order.</summary>
    public IReadOnlyList<LatticeAuthorizationRule> Rules => _rules;

    /// <summary>The loaded rules in precedence order, highest first.</summary>
    public IReadOnlyList<RankedRule> RankedRules { get; private set; } = [];

    /// <summary>The continuation token for the next page of rules, or <see langword="null"/>.</summary>
    public string? RulesNextToken { get; private set; }

    /// <summary>Whether the rule create / edit form is open.</summary>
    public bool RuleFormOpen { get; private set; }

    /// <summary>Whether the form is editing an existing rule rather than creating one.</summary>
    public bool EditingExistingRule { get; private set; }

    /// <summary>The rule id being created or edited.</summary>
    public string RuleIdInput { get; set; } = string.Empty;

    /// <summary>The subject kind the rule applies to.</summary>
    public LatticeSubjectSelectorKind RuleSubjectKind { get; set; } = LatticeSubjectSelectorKind.User;

    /// <summary>The subject id the rule applies to.</summary>
    public string RuleSubjectId { get; set; } = string.Empty;

    /// <summary>The rule's scope kind: whole tree, prefix, or key.</summary>
    public LatticeScopeKind RuleScopeKind { get; set; } = LatticeScopeKind.Tree;

    /// <summary>The rule's key or prefix, when the scope kind is not whole-tree.</summary>
    public string RuleScopeKeyOrPrefix { get; set; } = string.Empty;

    /// <summary>The rule's effect: allow or deny.</summary>
    public LatticeEffect RuleEffect { get; set; } = LatticeEffect.Allow;

    /// <summary>
    /// When true, the rule form authors the access-administration delegation
    /// grant instead of an ordinary rule: a whole-tree Admin Allow rule on the
    /// reserved policy tree (sys-auth-policy) for the chosen subject. The
    /// affordance supplies the reserved tree, whole-tree scope, and Admin
    /// operation, so the operator does not select a tree, scope, or operation
    /// set. The server still authorizes the write and only accepts it when the
    /// cluster's delegation option is enabled.
    /// </summary>
    public bool RuleDelegateAccessAdmin { get; set; }

    /// <summary>
    /// When true, the rule form authors an all-trees (cluster-wide) grant
    /// instead of an ordinary rule: a whole-tree rule over the all-trees
    /// sentinel ("*") for the chosen subject, carrying the selected operations
    /// and effect. The affordance supplies the sentinel scope, so the operator
    /// does not select a tree or scope but still chooses operations and
    /// Allow/Deny. Mutually exclusive with the access-administration delegation
    /// affordance. The rule is only enforced when the cluster's all-trees grants
    /// option is enabled; otherwise it is inert.
    /// </summary>
    public bool RuleAllTrees { get; set; }

    /// <summary>Whether <paramref name="flag"/> is currently selected on the rule form.</summary>
    /// <param name="flag">The operation to test.</param>
    public bool HasRuleOperation(LatticeOperation flag) => _ruleOperations.Contains(flag);

    /// <summary>Loads the next page of rules.</summary>
    public Task LoadMoreRulesAsync() => LoadRulesAsync(reset: false);

    /// <summary>Opens the empty rule create form.</summary>
    public void NewRule()
    {
        ResetRuleForm();
        RuleFormOpen = true;
        RaiseChanged();
    }

    /// <summary>Closes the rule form without saving.</summary>
    public void CancelRuleForm()
    {
        ResetRuleForm();
        RuleFormOpen = false;
        RaiseChanged();
    }

    /// <summary>Opens the rule form pre-filled from an existing rule.</summary>
    /// <param name="rule">The rule to edit.</param>
    public void EditRule(LatticeAuthorizationRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);

        RuleFormOpen = true;
        EditingExistingRule = true;
        RuleIdInput = rule.RuleId;
        RuleSubjectKind = rule.Subject.Kind;
        RuleSubjectId = rule.Subject.Id;
        SelectedTreeId = rule.Scope.TreeId;
        RuleScopeKind = rule.Scope.Kind;
        RuleScopeKeyOrPrefix = rule.Scope.KeyOrPrefix ?? string.Empty;
        RuleEffect = rule.Effect;
        // Reflect an existing access-administration delegation grant so the form
        // identifies it: a whole-tree Admin rule on the reserved policy tree.
        RuleDelegateAccessAdmin =
            rule.Scope.Kind == LatticeScopeKind.Tree
            && string.Equals(rule.Scope.TreeId, LatticeAuthReservedTrees.PolicyTreeId, StringComparison.Ordinal)
            && rule.Operations == LatticeOperation.Admin;
        // Reflect an existing all-trees grant so the form identifies it: a
        // whole-tree rule over the all-trees sentinel ("*").
        RuleAllTrees =
            rule.Scope.Kind == LatticeScopeKind.Tree
            && string.Equals(rule.Scope.TreeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal);
        _ruleOperations.Clear();
        foreach (var option in Views.AccessRuleFormat.Operations)
        {
            if ((rule.Operations & option.Flag) == option.Flag)
            {
                _ruleOperations.Add(option.Flag);
            }
        }

        RaiseChanged();
    }

    /// <summary>Adds or removes an operation flag on the rule form.</summary>
    /// <param name="flag">The operation to toggle.</param>
    /// <param name="enabled">Whether the operation is selected.</param>
    public void ToggleOperation(LatticeOperation flag, bool enabled)
    {
        if (enabled)
        {
            _ruleOperations.Add(flag);
        }
        else
        {
            _ruleOperations.Remove(flag);
        }

        RaiseChanged();
    }

    /// <summary>Whether the rule form currently describes a saveable rule.</summary>
    public bool CanSaveRule()
    {
        if (string.IsNullOrWhiteSpace(RuleIdInput) || string.IsNullOrWhiteSpace(RuleSubjectId))
        {
            return false;
        }

        // The delegation affordance supplies the reserved policy tree, whole-tree
        // scope, and the Admin operation, so no tree selection or operation choice
        // is required to author it.
        if (RuleDelegateAccessAdmin)
        {
            return true;
        }

        // The all-trees affordance supplies the sentinel scope, so no tree
        // selection is required; the operator still chooses at least one operation.
        if (RuleAllTrees)
        {
            return _ruleOperations.Count > 0;
        }

        return !string.IsNullOrWhiteSpace(SelectedTreeId)
            && _ruleOperations.Count > 0
            && (RuleScopeKind == LatticeScopeKind.Tree || !string.IsNullOrWhiteSpace(RuleScopeKeyOrPrefix));
    }

    /// <summary>Writes the rule described by the form.</summary>
    public async Task SaveRuleAsync()
    {
        if (Busy || !Allowed || !CanSaveRule())
        {
            return;
        }

        Busy = true;
        try
        {
            var subject = RuleSubjectKind == LatticeSubjectSelectorKind.Group
                ? LatticeSubjectSelector.Group(RuleSubjectId.Trim())
                : LatticeSubjectSelector.User(RuleSubjectId.Trim());

            LatticeAuthorizationRule rule;
            if (RuleDelegateAccessAdmin)
            {
                // Author the access-administration delegation grant: a whole-tree
                // Admin Allow rule on the reserved policy tree for the chosen
                // subject. The server accepts it only when the cluster's delegation
                // option is enabled; otherwise its rejection surfaces below.
                rule = AccessCreateModel.BuildAccessAdministrationRule(RuleIdInput.Trim(), subject);
            }
            else if (RuleAllTrees)
            {
                // Author an all-trees (cluster-wide) grant: a whole-tree rule over
                // the all-trees sentinel ("*") carrying the chosen operations and
                // effect. The server records it always, but only enforces it when
                // the cluster's all-trees grants option is enabled.
                rule = AccessCreateModel.BuildAllTreesRule(
                    RuleIdInput.Trim(), subject, CombineOperations(_ruleOperations), RuleEffect);
            }
            else
            {
                var scope = BuildScope(RuleScopeKind, SelectedTreeId!.Trim(), RuleScopeKeyOrPrefix);
                var operations = CombineOperations(_ruleOperations);
                rule = new LatticeAuthorizationRule(RuleIdInput.Trim(), subject, scope, operations, RuleEffect);
            }

            LastResult = await _domain.Policy.PutRuleAsync(rule);
            if (LastResult.IsSuccess)
            {
                await LoadRulesCoreAsync(reset: true);
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    /// <summary>Deletes the rule the form is editing.</summary>
    public async Task DeleteRuleAsync()
    {
        if (Busy || !Allowed || !EditingExistingRule || string.IsNullOrWhiteSpace(SelectedTreeId)
            || string.IsNullOrWhiteSpace(RuleIdInput))
        {
            return;
        }

        Busy = true;
        try
        {
            LastResult = await _domain.Policy.DeleteRuleAsync(SelectedTreeId.Trim(), RuleIdInput.Trim());
            if (LastResult.IsSuccess)
            {
                ResetRuleForm();
                RuleFormOpen = false;
                await LoadRulesCoreAsync(reset: true);
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    private async Task LoadRulesAsync(bool reset)
    {
        if (Busy || !Allowed)
        {
            return;
        }

        Busy = true;
        try
        {
            await LoadRulesCoreAsync(reset);
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    // The core list load without the busy guard, so a mutation (which already holds
    // the busy flag) can repopulate the rule table.
    private async Task LoadRulesCoreAsync(bool reset)
    {
        if (!Allowed)
        {
            return;
        }

        var view = await _domain.Policy.ListRulesAsync(pageToken: reset ? null : RulesNextToken);
        if (!view.IsSuccess)
        {
            LastResult = ToResult(view.Status, view.Message);
            return;
        }

        if (reset)
        {
            _rules.Clear();
        }

        _rules.AddRange(view.Entries);
        RulesNextToken = view.NextPageToken;
        RankedRules = RulePrecedence.Rank(_rules);
        await ResolveRuleSubjectsAsync(RankedRules);
    }

    private void ResetRuleForm()
    {
        EditingExistingRule = false;
        RuleIdInput = string.Empty;
        RuleSubjectKind = LatticeSubjectSelectorKind.User;
        RuleSubjectId = string.Empty;
        RuleScopeKind = LatticeScopeKind.Tree;
        RuleScopeKeyOrPrefix = string.Empty;
        _ruleOperations.Clear();
        RuleEffect = LatticeEffect.Allow;
        RuleDelegateAccessAdmin = false;
        RuleAllTrees = false;
    }
}
