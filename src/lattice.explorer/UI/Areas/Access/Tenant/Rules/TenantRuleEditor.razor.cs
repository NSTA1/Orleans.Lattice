using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// Creates or edits one of a tenant's own rules through
/// <see cref="ILatticeTenantPolicyAdmin.PutRuleAsync"/>. It offers only what the
/// tenant may author: its own trees (never an app-owned, reserved, system or other
/// tenant's tree) or every tree in it, and the data-plane operations. Before it
/// saves it shows the platform rules that would decide the rule's scope first. A
/// confinement refusal is shown beside the field that caused it, with its reason.
/// </summary>
public partial class TenantRuleEditor : IDisposable
{
    private static readonly IReadOnlyList<IGrouping<AccessOperationGroup, AccessOperationOption>> OperationGroups =
        [.. TenantRuleFormat.DataPlaneOperations.GroupBy(option => option.Group)];

    private static readonly IReadOnlyList<LtSelectOption> EffectOptions =
    [
        new("allow", "Allow"),
        new("deny", "Deny"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> ScopeOptions =
    [
        new(TenantRuleFormat.TreeScope, "Whole tree"),
        new(TenantRuleFormat.PrefixScope, "Key prefix in a tree"),
        new(TenantRuleFormat.KeyScope, "Single key in a tree"),
        new(TenantRuleFormat.TenantWideScope, "Every tree in this tenant"),
    ];

    private readonly ComponentLifetime _lifetime = new();
    private TenantRuleForm _form = TenantRuleForm.New();
    private AccessRuleDraftErrors _errors = new();
    private TenantRuleView? _editing;
    private bool _initialised;
    private bool _saving;
    private LtNameInput? _ruleIdBox;
    private LtComboBox? _treeBox;
    private AccessSubjectPicker? _subjectPicker;
    private TenantRuleIdSuggestionSource? _ruleIds;
    private ShadowCheck? _shadows;

    /// <summary>The tenant whose rule is written.</summary>
    [Parameter]
    [EditorRequired]
    public string Tenant { get; set; } = string.Empty;

    /// <summary>The tenant rule to edit, or <see langword="null"/> to create one.</summary>
    [Parameter]
    public TenantRuleView? Rule { get; set; }

    /// <summary>The rules governing the tenant, both layers: the platform rules among them are what the shadow check reads.</summary>
    [Parameter]
    public IReadOnlyList<TenantRuleView> Rules { get; set; } = [];

    /// <summary>Whether the cluster has an identity directory to search for cluster users and groups.</summary>
    [Parameter]
    public bool DirectoryAvailable { get; set; }

    /// <summary>The directory's one-line explanation of what a valid id looks like.</summary>
    [Parameter]
    public string? DirectoryExplanation { get; set; }

    /// <summary>Raised with the rule as stored.</summary>
    [Parameter]
    public EventCallback<TenantRuleView> OnSaved { get; set; }

    /// <summary>Raised when the administrator cancels.</summary>
    [Parameter]
    public EventCallback OnCancel { get; set; }

    [Inject]
    internal TenantAccessCatalog Access { get; set; } = default!;

    [Inject]
    internal TenantTreeSuggestionSource Trees { get; set; } = default!;

    private string? ScopeHint => _form.ScopeKind == TenantRuleScopeKind.TenantWide
        ? $"Governs every tree tenant {Tenant} owns, except its app-owned trees."
        : null;

    private string? SuggestedId => string.IsNullOrWhiteSpace(_form.RuleId) ? _form.SuggestId() : null;

    /// <summary>The platform rules that decide the rule's scope first, re-read only when the rule or the rules change.</summary>
    private IReadOnlyList<TenantRuleShadowHit> Shadows
    {
        get
        {
            var key = new ShadowKey(_form.SubjectId, _form.SubjectKind, _form.ScopeValue, _form.TreeName, _form.KeyOrPrefix, _form.Operations, Rules);
            if (_shadows is not { } check || check.Key != key)
            {
                check = new ShadowCheck(key, TenantRuleShadow.Find(_form, Rules));
                _shadows = check;
            }

            return check.Hits;
        }
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (_ruleIds is null || !string.Equals(_ruleIds.Tenant, Tenant, StringComparison.Ordinal))
        {
            _ruleIds = new TenantRuleIdSuggestionSource(Access, Tenant);
        }

        if (_initialised && ReferenceEquals(_editing, Rule))
        {
            return;
        }

        _initialised = true;
        _editing = Rule;
        _form = Rule is null ? TenantRuleForm.New() : TenantRuleForm.From(Rule);
        _errors = new AccessRuleDraftErrors();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    private void UseSuggestedId()
    {
        if (_form.SuggestId() is { } suggested)
        {
            _form.RuleId = suggested;
            _errors.RuleId = null;
        }
    }

    private void OnScopeChanged(string value)
    {
        _form.ScopeValue = value;
        _errors.Tree = null;
        _errors.KeyOrPrefix = null;
    }

    private void OnTreeChanged(string value)
    {
        _form.TreeName = value;
        _errors.Tree = TenantRuleFormat.TreeProblem(value);
    }

    private async Task SaveAsync()
    {
        if (_saving)
        {
            return;
        }

        _errors = _form.Validate();
        if (_errors.HasAny || !await ConfirmPickersAsync().ConfigureAwait(true))
        {
            return;
        }

        var tenant = Tenant;
        var draft = _form.ToDraft();
        TenantRuleView stored;
        _saving = true;
        try
        {
            var policy = Access.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
            stored = await policy.PutRuleAsync(tenant, draft, _lifetime.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
            return;
        }
        catch (TenantAccessConfinementException confinement)
        {
            _errors = Confined(confinement);
            return;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _errors = new AccessRuleDraftErrors { Form = failure.Message };
            return;
        }
        finally
        {
            _saving = false;
        }

        Access.Invalidate();
        if (!_lifetime.IsLeft)
        {
            await OnSaved.InvokeAsync(stored);
        }
    }

    /// <summary>Shows a confinement refusal beside the field that caused it, with its typed reason.</summary>
    /// <param name="refusal">The refusal.</param>
    /// <returns>The field errors.</returns>
    private AccessRuleDraftErrors Confined(TenantAccessConfinementException refusal)
    {
        var sentence = $"{TenantRuleFormat.ConfinementReason(refusal.Rule)}: {WithoutParameter(refusal)}";
        var errors = new AccessRuleDraftErrors();
        switch (refusal.Rule)
        {
            case TenantAccessConfinementRule.GroupNesting:
            case TenantAccessConfinementRule.ForeignTenantGroup:
                errors.Subject = sentence;
                break;
            case TenantAccessConfinementRule.RuleOperations:
                errors.Operations = sentence;
                break;
            case TenantAccessConfinementRule.ReservedRuleId:
                errors.RuleId = sentence;
                break;
            case TenantAccessConfinementRule.RuleTree when _form.NeedsTree:
                errors.Tree = sentence;
                break;
            default:
                errors.Form = sentence;
                break;
        }

        return errors;
    }

    /// <summary>The refusal's sentence without the "(Parameter 'rule')" an argument exception appends.</summary>
    private static string WithoutParameter(ArgumentException refusal)
    {
        var message = refusal.Message;
        if (refusal.ParamName is { Length: > 0 } name)
        {
            var suffix = $" (Parameter '{name}')";
            if (message.EndsWith(suffix, StringComparison.Ordinal))
            {
                return message[..^suffix.Length];
            }
        }

        return message;
    }

    private async Task<bool> ConfirmPickersAsync()
    {
        // Each picker shows its own message; every one is checked so all are shown at once.
        var ruleId = _form.IsExisting || _ruleIdBox is null || await _ruleIdBox.ConfirmAsync().ConfigureAwait(true);
        var tree = _form.IsExisting || !_form.NeedsTree || _treeBox is null || await _treeBox.ConfirmAsync().ConfigureAwait(true);
        var subject = _subjectPicker is null || await _subjectPicker.ConfirmAsync().ConfigureAwait(true);
        return ruleId && tree && subject;
    }

    private Task CancelAsync() => OnCancel.InvokeAsync();

    /// <summary>What the shadow check was last run over.</summary>
    private readonly record struct ShadowKey(
        string Subject,
        TenantSubjectKind Kind,
        string Scope,
        string Tree,
        string KeyOrPrefix,
        LatticeOperation Operations,
        IReadOnlyList<TenantRuleView> Rules);

    /// <summary>The shadow check's last answer.</summary>
    private sealed record ShadowCheck(ShadowKey Key, IReadOnlyList<TenantRuleShadowHit> Hits);
}
