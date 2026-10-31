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
    private readonly ComponentLifetime _lifetime = new();
    private readonly ComponentLifetime _rightsLifetime = new();
    private readonly string _headingId = LtIds.Next("lt-data-entry");
    private DataEntry? _entry;
    private DataRenderedValue? _rendered;
    private DataValueRenderer _renderer = DataValueRenderer.Auto;
    private (string StateId, string Key, int Version)? _loadedFor;
    private bool _loading;
    private bool _keyExpanded;
    private bool _valueExpanded;
    private string? _error;
    private DataWriteRights _rights;
    private (string StateId, string Key)? _rightsFor;
    private bool _editing;
    private bool _deleting;
    private bool _listening;

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

    [Inject]
    internal DataWriteGate Gate { get; set; } = default!;

    [Inject]
    internal DataEntryCommands EntryCommands { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    /// <summary>Whether the value is edited here: a plain value, not a CRDT whose members change through typed operations.</summary>
    private bool IsEditable => _entry is not { CrdtShape: not null } && _entry is not { CurrentMembers.Count: > 0 };

    private IReadOnlyList<LtSelectOption> RendererOptions =>
        [.. DataValueRendering.Offered(_entry?.CurrentMembers.Count > 0).Select(renderer => new LtSelectOption(renderer.ToString(), DataValueRendering.Label(renderer)))];

    private string Shown => _rendered is null
        ? string.Empty
        : _valueExpanded || _rendered.Content.Length <= DataValueRendering.DisplayLimit
            ? _rendered.Content
            : string.Concat(LtTextCut.Prefix(_rendered.Content, DataValueRendering.DisplayLimit), "\n...");

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
            _rights = DataWriteRights.None;
            _editing = false;
            _deleting = false;
        }

        if (!_listening)
        {
            _listening = true;
            EntryCommands.Requested += OnCommand;
        }

        if (_rightsFor != (workspace.Tree.StateId, Key))
        {
            _ = LoadRightsAsync(workspace.Tree, Key);
        }

        _loadedFor = target;
        await LoadAsync();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        EntryCommands.Requested -= OnCommand;
        EntryCommands.HideEntry(Key);
        _rightsLifetime.Leave();
        _lifetime.Leave();
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

        var token = _lifetime.Renew();
        var key = Key;
        _loading = true;
        _error = null;
        try
        {
            var entry = await reader.GetEntryAsync(workspace.Tree.StateId, key, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _entry = entry;
            if (_renderer == DataValueRenderer.Members && _entry is not { CurrentMembers.Count: > 0 })
            {
                _renderer = DataValueRenderer.Auto;
            }

            if (_entry is { CurrentMembers.Count: > 0 } && _renderer == DataValueRenderer.Auto && _rendered is null)
            {
                _renderer = DataValueRenderer.Members;
            }

            Render();
            PublishRights();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _entry = null;
                _error = DataErrors.Describe(exception, "read this entry");
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _loading = false;
            }
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

    /// <summary>
    /// Asks the data facade what the caller may change at this key, and publishes
    /// it for the palette. Until it answers, and whenever it cannot, no write
    /// control is drawn.
    /// </summary>
    private async Task LoadRightsAsync(DataTreeEntry tree, string key)
    {
        var token = _rightsLifetime.Renew();
        _rightsFor = (tree.StateId, key);
        _rights = DataWriteRights.None;
        EntryCommands.ShowEntry(key, DataWriteRights.None);
        var rights = await Gate.ForKeyAsync(tree, key, token);
        if (token.IsCancellationRequested || _rightsLifetime.IsLeft)
        {
            return;
        }

        _rights = rights;
        PublishRights();
        if (EntryCommands.TryTake(DataEntryCommands.EditEntryId))
        {
            OpenEditor();
        }
        else if (EntryCommands.TryTake(DataEntryCommands.DeleteEntryId))
        {
            OpenDelete();
        }

        await InvokeAsync(StateHasChanged);
    }

    private void PublishRights() =>
        EntryCommands.ShowEntry(Key, new DataWriteRights(_rights.CanWrite && IsEditable, _rights.CanDelete && _entry is { IsTombstone: false }));

    private void OnCommand(string commandId) => _ = InvokeAsync(() =>
    {
        if (_lifetime.IsLeft)
        {
            return;
        }

        if (string.Equals(commandId, DataEntryCommands.EditEntryId, StringComparison.Ordinal))
        {
            OpenEditor();
        }
        else if (string.Equals(commandId, DataEntryCommands.DeleteEntryId, StringComparison.Ordinal))
        {
            OpenDelete();
        }

        StateHasChanged();
    });

    private void OpenEditor()
    {
        if (_rights.CanWrite && IsEditable)
        {
            _deleting = false;
            _editing = true;
        }
    }

    private void OpenDelete()
    {
        if (_rights.CanDelete && _entry is { IsTombstone: false })
        {
            _editing = false;
            _deleting = true;
        }
    }

    private async Task OnSavedAsync(string key)
    {
        await LoadAsync();
        PublishRights();
    }

    private async Task DeleteAsync()
    {
        _deleting = false;
        if (Workspace is not { } workspace || Gate.Data is not { } data)
        {
            Toasts.Show("This Explorer has no data API to delete through.", LtToastTone.Danger);
            return;
        }

        var key = Key;
        var token = _lifetime.Token;
        try
        {
            await data.DeleteAsync(workspace.Tree.StateId, key, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            Toasts.Show($"Entry {DataKeysPanel.Clip(key)} deleted.", LtToastTone.Success);
            await LoadAsync();
            PublishRights();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(DataErrors.Describe(exception, "delete this key"), LtToastTone.Danger);
            }
        }
    }
}
