using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The Remediation tab: the status page of the tree's long-running operation,
/// and the editor that starts a remediation as a staged operation.
/// </summary>
public partial class SchemaRemediationPanel : IDisposable
{
    private static readonly IReadOnlyList<LtSelectOption> StepKinds =
    [
        new(nameof(SchemaTransformStepKind.Set), "Set a member"),
        new(nameof(SchemaTransformStepKind.Remove), "Remove a member"),
        new(nameof(SchemaTransformStepKind.Rename), "Rename a member"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> ValueKinds =
    [
        new(nameof(SchemaConstantKind.Text), "Text"),
        new(nameof(SchemaConstantKind.Number), "Number"),
        new(nameof(SchemaConstantKind.Boolean), "True or false"),
        new(nameof(SchemaConstantKind.Null), "Null"),
    ];

    private readonly SchemaTransformDraft _draft = new();
    private string? _loadedTree;
    private string? _stepError;
    private string? _startError;
    private bool _confirm;

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal SchemaOperations Operations { get; set; } = default!;

    private bool Busy => Workspace is { } workspace && Operations.Find(workspace.TreeId) is { IsActive: true };

    /// <inheritdoc />
    public void Dispose()
    {
        Operations.Changed -= OnOperationChanged;
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized() => Operations.Changed += OnOperationChanged;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Workspace is { } workspace && !string.Equals(workspace.TreeId, _loadedTree, StringComparison.Ordinal))
        {
            _loadedTree = workspace.TreeId;
            _draft.Clear();
            _stepError = null;
            _startError = null;
            _confirm = false;
        }
    }

    private void OnStepKindChanged(string value)
    {
        if (Enum.TryParse<SchemaTransformStepKind>(value, out var kind))
        {
            _draft.Kind = kind;
            _stepError = null;
        }
    }

    private void OnValueKindChanged(string value)
    {
        if (Enum.TryParse<SchemaConstantKind>(value, out var kind))
        {
            _draft.ValueKind = kind;
            _stepError = null;
        }
    }

    private void AddStep() => _stepError = _draft.TryAdd(out var error) ? null : error;

    private void Review()
    {
        if (!_draft.TryBuild(out _, out var error))
        {
            _startError = error;
            return;
        }

        _startError = null;
        _confirm = true;
    }

    private void StartRemediation()
    {
        _confirm = false;
        if (Workspace is not { } workspace || workspace.Row.Policy is not { } policy)
        {
            return;
        }

        if (!_draft.TryBuild(out var transform, out var error))
        {
            _startError = error;
            return;
        }

        try
        {
            var schema = Facades.RequireSchema();
            var tree = workspace.TreeId;
            var steps = SchemaFormat.Count(_draft.Steps.Count, "step");
            Operations.Start(
                tree,
                SchemaOperationKind.Remediate,
                $"Remediating every value through {steps}",
                ct => schema.RemediateAsync(tree, transform, policy, ct));
            _draft.Clear();
            _startError = null;
        }
        catch (Exception exception)
        {
            _startError = SchemaFailure.Describe(exception, "start the remediation");
        }
    }

    private void OnOperationChanged(string treeId)
    {
        if (Workspace is { } workspace && string.Equals(treeId, workspace.TreeId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(StateHasChanged);
        }
    }
}
