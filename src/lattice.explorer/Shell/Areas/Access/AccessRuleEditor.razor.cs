using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

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

    /// <summary>The rule to edit, or <see langword="null"/> to create one.</summary>
    [Parameter]
    public LatticeAuthorizationRule? Rule { get; set; }

    /// <summary>The cluster's access model, which decides the scopes and directory search offered.</summary>
    [Parameter]
    public AccessModelDescriptor? Model { get; set; }

    /// <summary>Raised with the saved rule.</summary>
    [Parameter]
    public EventCallback<LatticeAuthorizationRule> OnSaved { get; set; }

    /// <summary>Raised when the operator cancels.</summary>
    [Parameter]
    public EventCallback OnCancel { get; set; }

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

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

    private string? ScopeHint => _draft.ScopeKind switch
    {
        AccessRuleDraft.ClusterScope when Model?.AllTreesGrantsEnabled == false =>
            "All-trees grants are off on this cluster: a cluster-wide rule may grant App install and Telemetry, and data operations in it are refused.",
        AccessRuleDraft.ClusterScope => "Governs every tree on the cluster.",
        AccessRuleDraft.AccessAdministrationScope => "Grant Admin here to delegate access administration.",
        _ => null,
    };

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
        if (_errors.HasAny)
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

    private Task CancelAsync() => OnCancel.InvokeAsync();
}
