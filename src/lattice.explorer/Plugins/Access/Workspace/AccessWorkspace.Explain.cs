using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Access.Workspace;

/// <summary>
/// The Explain sub-surface: the facade-computed decision explanation for one
/// subject / operation / scope, and the effective-permissions listing for one
/// subject.
/// </summary>
/// <remarks>
/// The surface never re-implements a verdict. It renders the facade's
/// <c>Allowed</c> flag verbatim, and the precedence ranking beside it is a
/// presentation-only aid.
/// </remarks>
public sealed partial class AccessWorkspace
{
    /// <summary>The subject kind the explanation is computed for.</summary>
    public LatticeSubjectSelectorKind ExplainSubjectKind { get; set; } = LatticeSubjectSelectorKind.User;

    /// <summary>The subject id the explanation is computed for.</summary>
    public string ExplainSubjectId { get; set; } = string.Empty;

    /// <summary>The operation the explanation is computed for.</summary>
    public LatticeOperation ExplainOperation { get; set; } = LatticeOperation.Read;

    /// <summary>The scope kind the explanation is computed for.</summary>
    public LatticeScopeKind ExplainScopeKind { get; set; } = LatticeScopeKind.Tree;

    /// <summary>The key or prefix, when the explain scope kind is not whole-tree.</summary>
    public string ExplainScopeKeyOrPrefix { get; set; } = string.Empty;

    /// <summary>The last computed explanation, or <see langword="null"/>.</summary>
    public AuthExplanation? Explanation { get; private set; }

    /// <summary>The explanation's matched rules in precedence order.</summary>
    public IReadOnlyList<RankedRule> ExplainRankedRules { get; private set; } = [];

    /// <summary>The last computed effective permissions, or <see langword="null"/>.</summary>
    public AuthEffectivePermissions? Effective { get; private set; }

    /// <summary>The effective permissions' rules in precedence order.</summary>
    public IReadOnlyList<RankedRule> EffectiveRankedRules { get; private set; } = [];

    /// <summary>Whether Explain can currently be run.</summary>
    public bool CanExplain =>
        !Busy && Allowed
        && !string.IsNullOrWhiteSpace(ExplainSubjectId)
        && !string.IsNullOrWhiteSpace(SelectedTreeId);

    /// <summary>Whether effective permissions can currently be listed.</summary>
    public bool CanListEffective => !Busy && Allowed && !string.IsNullOrWhiteSpace(ExplainSubjectId);

    /// <summary>Computes the facade's decision explanation for the entered subject, operation, and scope.</summary>
    public async Task RunExplainAsync()
    {
        if (!CanExplain)
        {
            return;
        }

        Busy = true;
        try
        {
            Effective = null;
            EffectiveRankedRules = [];
            var scope = BuildScope(ExplainScopeKind, SelectedTreeId!.Trim(), ExplainScopeKeyOrPrefix);
            var view = await _domain.Policy.ExplainAsync(
                ExplainSubjectId.Trim(), ExplainOperation, scope, ExplainSubjectKind);
            if (view.IsSuccess && view.Explanation is not null)
            {
                Explanation = view.Explanation;
                ExplainRankedRules = RulePrecedence.Rank(view.Explanation.MatchedRules);
                await ResolveRuleSubjectsAsync(ExplainRankedRules);
                await Labels.ResolveLabelAsync(view.Explanation.SubjectId);
                await Labels.ResolveManyAsync(view.Explanation.GroupIds);
                LastResult = null;
            }
            else
            {
                Explanation = null;
                ExplainRankedRules = [];
                LastResult = ToResult(view.Status, view.Message);
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }

    /// <summary>Lists the entered subject's effective permissions across the cluster.</summary>
    public async Task RunEffectiveAsync()
    {
        if (!CanListEffective)
        {
            return;
        }

        Busy = true;
        try
        {
            Explanation = null;
            ExplainRankedRules = [];
            var view = await _domain.Policy.EffectivePermissionsAsync(ExplainSubjectId.Trim());
            if (view.IsSuccess && view.Permissions is not null)
            {
                Effective = view.Permissions;
                EffectiveRankedRules = RulePrecedence.Rank(view.Permissions.Rules);
                await ResolveRuleSubjectsAsync(EffectiveRankedRules);
                await Labels.ResolveLabelAsync(view.Permissions.SubjectId);
                await Labels.ResolveManyAsync(view.Permissions.GroupIds);
                LastResult = null;
            }
            else
            {
                Effective = null;
                EffectiveRankedRules = [];
                LastResult = ToResult(view.Status, view.Message);
            }
        }
        finally
        {
            Busy = false;
            RaiseChanged();
        }
    }
}
