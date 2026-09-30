using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The Policy tab: reads the tree's enforcement policy, and sets or clears it.
/// </summary>
public partial class SchemaPolicyPanel : IDisposable
{
    private const string StrictOn = "On: replicated and restored values are checked too, and one that fails is diverted to the dead letters";

    private const string StrictOff = "Off: replicated and restored values are trusted and not checked";

    private readonly ComponentLifetime _lifetime = new();
    private IReadOnlyList<RuleRow> _rules = [];
    private LatticeSchemaPolicy? _policy;
    private string? _loadedTree;
    private string? _error;
    private string? _saveError;
    private bool _loaded;
    private bool _editing;
    private bool _busy;
    private bool _confirmClear;

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
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

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
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _error = SchemaFailure.Describe(exception, "read the policy");
        }
    }

    private void OpenEditor()
    {
        // The builder seeds itself from the applied policy, so an edit starts from
        // what is enforced rather than silently replacing it with an empty form.
        _saveError = null;
        _editing = true;
    }

    private void CancelEditor()
    {
        _editing = false;
        _saveError = null;
    }

    private async Task SaveAsync(LatticeSchemaPolicy policy)
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        _busy = true;
        _saveError = null;
        try
        {
            await Facades.RequireSchema().SetPolicyAsync(workspace.TreeId, policy, _lifetime.Token);
            _editing = false;
            Ledger.Forget(workspace.TreeId);
            Toasts.Show($"The policy of {workspace.TreeId} is saved.", LtToastTone.Success);
            await ReloadAsync();
            await workspace.RefreshAsync();
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
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
            Toasts.Show(
                removed ? $"The policy of {workspace.TreeId} is cleared." : $"{workspace.TreeId} had no policy to clear.",
                removed ? LtToastTone.Success : LtToastTone.Info);
            await ReloadAsync();
            await workspace.RefreshAsync();
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
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
