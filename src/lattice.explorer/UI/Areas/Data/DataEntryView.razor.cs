using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// One entry of the open tree, read through the Core data reader and drawn with
/// a pluggable value renderer.
/// </summary>
public partial class DataEntryView : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private readonly string _headingId = LtIds.Next("lt-data-entry");
    private DataEntry? _entry;
    private DataRenderedValue? _rendered;
    private DataValueRenderer _renderer = DataValueRenderer.Auto;
    private (string StateId, string Key, int Version)? _loadedFor;
    private bool _loading;
    private bool _keyExpanded;
    private bool _valueExpanded;
    private string? _error;

    /// <summary>The key to show.</summary>
    [Parameter, EditorRequired]
    public string Key { get; set; } = string.Empty;

    /// <summary>Bumped by the keys tab when a live change touched this key, to reload it.</summary>
    [Parameter]
    public int Version { get; set; }

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private IReadOnlyList<LtSelectOption> RendererOptions =>
        [.. DataValueRendering.Offered(_entry?.CurrentMembers.Count > 0).Select(renderer => new LtSelectOption(renderer.ToString(), DataValueRendering.Label(renderer)))];

    private string Shown => _rendered is null
        ? string.Empty
        : _valueExpanded || _rendered.Content.Length <= DataValueRendering.DisplayLimit
            ? _rendered.Content
            : string.Concat(_rendered.Content.AsSpan(0, DataValueRendering.DisplayLimit), "\n...");

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var target = (workspace.Tree.StateId, Key, Version);
        if (_loadedFor == target)
        {
            return;
        }

        if (_loadedFor?.Key != Key || _loadedFor?.StateId != workspace.Tree.StateId)
        {
            _entry = null;
            _rendered = null;
            _renderer = DataValueRenderer.Auto;
            _keyExpanded = false;
            _valueExpanded = false;
        }

        _loadedFor = target;
        await LoadAsync();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    private async Task LoadAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (DataServices.Find<IDataReader>(Services) is not { } reader)
        {
            _error = "This Explorer has no state API to read entries through.";
            return;
        }

        _loading = true;
        _error = null;
        try
        {
            _entry = await reader.GetEntryAsync(workspace.Tree.StateId, Key, _lifetime.Token);
            if (_entry is { CurrentMembers.Count: > 0 } && _renderer == DataValueRenderer.Auto && _rendered is null)
            {
                _renderer = DataValueRenderer.Members;
            }

            Render();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _entry = null;
            _error = DataErrors.Describe(exception, "read this entry");
        }
        finally
        {
            _loading = false;
        }
    }

    private void SetRenderer(string value)
    {
        if (Enum.TryParse<DataValueRenderer>(value, out var renderer))
        {
            _renderer = renderer;
            _valueExpanded = false;
            Render();
        }
    }

    private void Render() => _rendered = _entry is null
        ? null
        : DataValueRendering.Render(_entry.Value, _entry.Truncated, _renderer, _entry.CurrentMembers);
}
