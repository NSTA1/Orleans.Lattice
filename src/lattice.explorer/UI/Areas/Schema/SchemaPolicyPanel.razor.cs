using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The Policy tab: reads the tree's enforcement policy, and sets or clears it.
/// </summary>
public partial class SchemaPolicyPanel : IDisposable
{
    private static readonly IReadOnlyList<LtSelectOption> RuleKinds =
    [
        new(nameof(SchemaRuleDraftKind.Utf8), "Well-formed UTF-8"),
        new(nameof(SchemaRuleDraftKind.Json), "One JSON document"),
        new(nameof(SchemaRuleDraftKind.MaxLength), "Largest size"),
        new(nameof(SchemaRuleDraftKind.Pattern), "Matches a pattern"),
    ];

    private const string StrictOn = "On: replicated and restored values are checked too, and one that fails is diverted to the dead letters";

    private const string StrictOff = "Off: replicated and restored values are trusted and not checked";

    private const string StrictHint = "On: replicated and restored values are checked as well, and one that fails goes to the dead letters instead of being applied. Off: only direct writes are checked.";

    private readonly CancellationTokenSource _lifetime = new();
    private readonly List<LatticeSchemaRule> _draftRules = [];
    private readonly SchemaRuleDraft _builder = new();
    private IReadOnlyList<RuleRow> _rules = [];
    private LatticeSchemaPolicy? _policy;
    private string? _loadedTree;
    private string? _error;
    private string? _builderError;
    private string? _saveError;
    private bool _loaded;
    private bool _editing;
    private bool _draftStrict;
    private bool _busy;
    private bool _confirmClear;

    private SchemaMemberSuggestionSource? _members;

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal SchemaComplianceLedger Ledger { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized() => _members = new SchemaMemberSuggestionSource(Facades, () => Workspace?.TreeId);

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is { } workspace && !string.Equals(workspace.TreeId, _loadedTree, StringComparison.Ordinal))
        {
            _loadedTree = workspace.TreeId;
            _editing = false;
            await ReloadAsync();
        }
    }

    private async Task ReloadAsync()
    {
        if (Workspace is not { } workspace || !workspace.Grants.ViewPolicy)
        {
            return;
        }

        _error = null;
        _loaded = false;
        try
        {
            _policy = await Facades.RequireSchema().GetPolicyAsync(workspace.TreeId, _lifetime.Token);
            _rules = _policy is null
                ? []
                : _policy.Rules.Select((rule, index) => new RuleRow(index + 1, SchemaFormat.RuleKind(rule), SchemaFormat.RuleDetail(rule))).ToArray();
            _loaded = true;
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _error = SchemaFailure.Describe(exception, "read the policy");
        }
    }

    private void OpenEditor()
    {
        // Seed from the applied policy, so an edit starts from what is enforced
        // rather than silently replacing it with an empty form.
        _draftRules.Clear();
        if (_policy is not null)
        {
            _draftRules.AddRange(_policy.Rules);
        }

        _draftStrict = _policy?.StrictIngest ?? false;
        _builder.Reset();
        _builderError = null;
        _saveError = null;
        _editing = true;
    }

    private void CancelEditor()
    {
        _editing = false;
        _saveError = null;
        _builderError = null;
    }

    private void OnKindChanged(string value)
    {
        if (Enum.TryParse<SchemaRuleDraftKind>(value, out var kind))
        {
            _builder.Kind = kind;
            _builderError = null;
        }
    }

    private void AddRule()
    {
        if (_builder.TryBuild(out var rule, out var error))
        {
            _draftRules.Add(rule);
            _builder.Reset();
            _builderError = null;
        }
        else
        {
            _builderError = error;
        }
    }

    private void RemoveRule(int index)
    {
        if (index >= 0 && index < _draftRules.Count)
        {
            _draftRules.RemoveAt(index);
        }
    }

    private async Task SaveAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        // A rule written in the builder but not yet added would otherwise be lost.
        if (_builder.IsDirty)
        {
            if (!_builder.TryBuild(out var pending, out var error))
            {
                _builderError = error;
                return;
            }

            _draftRules.Add(pending);
            _builder.Reset();
        }

        if (_draftRules.Count == 0)
        {
            _saveError = "A policy needs at least one rule. To accept every value, clear the policy instead.";
            return;
        }

        _busy = true;
        _saveError = null;
        try
        {
            await Facades.RequireSchema().SetPolicyAsync(workspace.TreeId, new LatticeSchemaPolicy(_draftRules.ToArray(), _draftStrict), _lifetime.Token);
            _editing = false;
            Ledger.Forget(workspace.TreeId);
            _members?.Invalidate();
            Toasts.Show($"The policy of {workspace.TreeId} is saved.", LtToastTone.Success);
            await ReloadAsync();
            await workspace.RefreshAsync();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _saveError = SchemaFailure.Describe(exception, "set the policy");
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task ClearAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        _busy = true;
        try
        {
            var removed = await Facades.RequireSchema().ClearPolicyAsync(workspace.TreeId, _lifetime.Token);
            Ledger.Forget(workspace.TreeId);
            _members?.Invalidate();
            Toasts.Show(
                removed ? $"The policy of {workspace.TreeId} is cleared." : $"{workspace.TreeId} had no policy to clear.",
                removed ? LtToastTone.Success : LtToastTone.Info);
            await ReloadAsync();
            await workspace.RefreshAsync();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(SchemaFailure.Describe(exception, "clear the policy"), LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
        }
    }

    /// <summary>One rule of the applied policy, as the rule table shows it.</summary>
    /// <param name="Number">The rule's 1-based position.</param>
    /// <param name="Kind">The rule's kind, as a word.</param>
    /// <param name="Detail">What the rule requires.</param>
    internal sealed record RuleRow(int Number, string Kind, string Detail);
}
