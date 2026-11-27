using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// Creates or edits an authored authorization rule through
/// <see cref="ILatticeAuthAdmin.PutRuleAsync"/>. It never edits an app-owned rule:
/// pages do not offer it one, and a new id under the app prefix is refused.
/// A directory validation failure is shown beside the subject and an app-owned
/// id beside the rule id; any other refusal is shown as the form's error.
/// </summary>
public partial class AccessRuleEditor
{
    private static readonly IReadOnlyList<IGrouping<AccessOperationGroup, AccessOperationOption>> OperationGroups =
        [.. AccessRuleFormat.Operations.GroupBy(option => option.Group)];

    private AccessRuleDraft _draft = AccessRuleDraft.New();
    private AccessRuleDraftErrors _errors = new();
    private LatticeAuthorizationRule? _editing;
    private bool _initialised;
    private bool _saving;
    private LtNameInput? _ruleIdBox;
    private LtComboBox? _treeBox;
    private AccessSubjectPicker? _subjectPicker;
    private AccessRuleIdSuggestionSource? _ruleIds;
    private string? _ruleIdsTree;

    /// <summary>The rule to edit, or <see langword="null"/> to create one.</summary>
    [Parameter]
    public LatticeAuthorizationRule? Rule { get; set; }

    /// <summary>The cluster's access model, which decides the scopes and directory search offered.</summary>
    [Parameter]
    public AccessModelDescriptor? Model { get; set; }

    /// <summary>
    /// The tenant the editing page's address is rooted at, or <see langword="null"/>
    /// on a cluster-wide page: the rule ids checked for a clash are that tenant's.
    /// </summary>
    [Parameter]
    public string? Tenant { get; set; }

    /// <summary>Raised with the saved rule.</summary>
    [Parameter]
    public EventCallback<LatticeAuthorizationRule> OnSaved { get; set; }

    /// <summary>Raised when the operator cancels.</summary>
    [Parameter]
    public EventCallback OnCancel { get; set; }

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    private static IReadOnlyList<LtSelectOption> EffectOptions { get; } =
    [
        new("allow", "Allow"),
        new("deny", "Deny"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> BaseScopeOptions =
    [
        new(AccessRuleDraft.TreeScope, "Whole tree"),
        new(AccessRuleDraft.PrefixScope, "Key prefix in a tree"),
        new(AccessRuleDraft.KeyScope, "Single key in a tree"),
        new(AccessRuleDraft.ClusterScope, "All trees (cluster-wide)"),
        new(AccessRuleDraft.TenantCapabilityScope, "One tenant's telemetry (tenant capability)"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> DelegationScopeOptions =
    [
        .. BaseScopeOptions,
        new(AccessRuleDraft.AccessAdministrationScope, "Access administration (delegation)"),
    ];

    private IReadOnlyList<LtSelectOption> ScopeOptions =>
        Model?.AccessAdministrationDelegationEnabled == true || _draft.ScopeKind == AccessRuleDraft.AccessAdministrationScope
            ? DelegationScopeOptions
            : BaseScopeOptions;

    /// <summary>
    /// The tenant whose group the subject names, or <see langword="null"/>: a rule
    /// naming a tenant group may govern only that tenant's own trees, so the Tree
    /// field then offers those, by full id.
    /// </summary>
    private string? SubjectTenant =>
        _draft.SubjectKind == LatticeSubjectSelectorKind.Group && LatticeTenantGroupId.TryParse(_draft.SubjectId.Trim(), out var group)
            ? group.Tenant.Value
            : null;

    private ILtSuggestionSource TreeSource => SubjectTenant is { } tenant ? Suggestions.TreesOfTenant(tenant) : Suggestions.Trees;

    private string TreeHint => SubjectTenant is { } tenant
        ? $"One of tenant {tenant}'s own trees, by its full id (t/{tenant}/...)."
        : "The logical tree id the rule governs.";

    private string? ScopeHint => _draft.ScopeKind switch
    {
        AccessRuleDraft.ClusterScope when Model?.AllTreesGrantsEnabled == false =>
            "All-trees grants are off on this cluster: a cluster-wide rule may grant App install and Telemetry, and data operations in it are refused.",
        AccessRuleDraft.ClusterScope => "Governs every tree on the cluster.",
        AccessRuleDraft.AccessAdministrationScope => "Grant Admin here to delegate access administration.",
        AccessRuleDraft.TenantCapabilityScope => "Grants Telemetry for this one tenant's series only: never another tenant's, the platform's, or the cluster's totals.",
        _ => null,
    };

    /// <summary>
    /// The rule ids already used under the governed tree, which a new id is checked against,
    /// or <see langword="null"/> while the governed tree is not known: an id is unique only
    /// within its tree, so no clash can be judged before the tree is. A new source per tree
    /// makes the id field drop a verdict reached for another tree.
    /// </summary>
    private AccessRuleIdSuggestionSource? RuleIds
    {
        get
        {
            var tree = GovernedTree.Trim();
            if (tree.Length == 0)
            {
                return null;
            }

            if (_ruleIds is null || !string.Equals(_ruleIdsTree, tree, StringComparison.Ordinal))
            {
                _ruleIds = new AccessRuleIdSuggestionSource(Catalog, () => tree, () => Tenant);
                _ruleIdsTree = tree;
            }

            return _ruleIds;
        }
    }

    /// <summary>
    /// The tree the drafted rule governs, which a rule id must be unique within: the
    /// cluster-wide and access-administration scopes govern their own sentinel trees, so
    /// an id used on another tree is free for them (rules are keyed by tree and id).
    /// </summary>
    private string GovernedTree => _draft.ScopeKind switch
    {
        AccessRuleDraft.ClusterScope => LatticeScope.ClusterWideTreeId,
        AccessRuleDraft.AccessAdministrationScope => LatticeAuthReservedTrees.PolicyTreeId,
        AccessRuleDraft.TenantCapabilityScope => TenantCapabilityTree,
        _ => _draft.TreeId,
    };

    /// <summary>
    /// The capability id a tenant capability rule governs, or empty while the tenant
    /// typed is not a tenant that has one.
    /// </summary>
    private string TenantCapabilityTree =>
        TenantId.TryParse(_draft.Tenant.Trim(), out var tenant) && !tenant.IsDefault
            ? LatticeScope.TenantCapability(tenant).TreeId
            : string.Empty;

    /// <summary>
    /// Whether the checklist offers <paramref name="option"/> for the chosen scope:
    /// a scopeless capability only with the cluster-wide scope (Telemetry also with a
    /// tenant capability scope), and nothing but Telemetry with a tenant capability
    /// scope. An option already ticked stays enabled so it can be cleared.
    /// </summary>
    private bool IsOffered(AccessOperationOption option)
    {
        if (_draft.HasOperation(option.Flag))
        {
            return true;
        }

        if (_draft.IsTenantCapability)
        {
            return option.Flag == LatticeOperation.Telemetry;
        }

        return !option.IsScopeless || _draft.IsClusterWide;
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (_initialised && ReferenceEquals(_editing, Rule))
        {
            return;
        }

        _initialised = true;
        _editing = Rule;
        _draft = Rule is null ? AccessRuleDraft.New() : AccessRuleDraft.From(Rule);
        _errors = new AccessRuleDraftErrors();
    }

    private static string GroupTitle(AccessOperationGroup group) => group switch
    {
        AccessOperationGroup.Data => "Data operations",
        AccessOperationGroup.Administration => "Administration",
        _ => "Cluster-wide capabilities",
    };

    private void OnScopeChanged(string value)
    {
        _draft.ScopeKind = value;
        if (_errors.Operations is not null)
        {
            _errors.Operations = _draft.Validate().Operations;
        }
    }

    private async Task SaveAsync()
    {
        if (_saving)
        {
            return;
        }

        _errors = _draft.Validate();
        if (_errors.HasAny || !await ConfirmPickersAsync().ConfigureAwait(true))
        {
            return;
        }

        var rule = _draft.ToRule();
        _saving = true;
        try
        {
            await Catalog.Admin.PutRuleAsync(rule).ConfigureAwait(true);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            _errors = new AccessRuleDraftErrors();
            switch (failure.Kind)
            {
                case AccessFailureKind.DirectoryValidation:
                    _errors.Subject = failure.Message;
                    break;
                case AccessFailureKind.AppOwned:
                    _errors.RuleId = failure.Message;
                    break;
                default:
                    _errors.Form = failure.Message;
                    break;
            }

            return;
        }
        finally
        {
            _saving = false;
        }

        Catalog.Invalidate();
        await OnSaved.InvokeAsync(rule);
    }

    private async Task<bool> ConfirmPickersAsync()
    {
        // Each picker shows its own message; every one is checked so all are shown at once.
        var ruleId = _draft.IsExisting || _ruleIdBox is null || await _ruleIdBox.ConfirmAsync().ConfigureAwait(true);
        var tree = _draft.IsExisting || !_draft.NeedsTree || _treeBox is null || await _treeBox.ConfirmAsync().ConfigureAwait(true);
        var subject = _subjectPicker is null || await _subjectPicker.ConfirmAsync().ConfigureAwait(true);
        return ruleId && tree && subject;
    }

    private Task CancelAsync() => OnCancel.InvokeAsync();
}
