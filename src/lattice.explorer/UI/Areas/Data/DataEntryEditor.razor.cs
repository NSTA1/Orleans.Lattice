using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Creates a new entry, or changes the value of an existing one, through the data
/// facade as the signed-in user. Checks the draft against the tree's schema policy
/// before save, and warns - showing the newer value - when the entry changed after
/// the editor opened, so the user chooses to overwrite or reload.
/// </summary>
public partial class DataEntryEditor : IDisposable
{
    private static readonly IReadOnlyList<LtSelectOption> FormatOptions =
        [.. Enum.GetValues<DataDraftFormat>().Select(format => new LtSelectOption(format.ToString(), DataDraft.Label(format)))];

    private readonly ComponentLifetime _lifetime = new();
    private readonly string _valueId = LtIds.Next("lt-data-editor-value");
    private readonly string _errorId = LtIds.Next("lt-data-editor-error");
    private bool _wasOpen;
    private bool _loading;
    private bool _saving;
    private string? _loadError;
    private string? _saveError;
    private string _key = string.Empty;
    private string? _keyError;
    private DataDraftFormat _format = DataDraftFormat.Json;
    private string _text = string.Empty;
    private string? _draftError;
    private string? _note;
    private DataSchemaCheck _schema = DataSchemaCheck.NotRead;
    private IReadOnlyList<string> _violations = [];
    private DataEntryVersion _baseline;
    private DataEntryConflict? _conflict;

    /// <summary>Whether the editor is open.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised when the editor opens or closes.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }

    /// <summary>The key to edit, or <see langword="null"/> to create a new entry.</summary>
    [Parameter]
    public string? Key { get; set; }

    /// <summary>Raised with the key once its value is saved.</summary>
    [Parameter]
    public EventCallback<string> OnSaved { get; set; }

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject]
    internal DataWriteGate Gate { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    private bool IsCreate => Key is null;

    private string Title => IsCreate ? "New entry" : "Edit entry";

    private LtDialogPlacement Placement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string ConflictTitle => _conflict?.Entry is null or { IsTombstone: true }
        ? (IsCreate || !_baseline.Exists ? "This key was written since you opened the editor." : "This entry was deleted since you opened the editor.")
        : "This entry changed since you opened the editor.";

    private string ConflictText => _conflict?.Entry is null or { IsTombstone: true } && _baseline.Exists && !IsCreate
        ? "Saving writes your value back. Reload to start from the deleted entry instead."
        : _conflict?.Entry is { IsTombstone: false } newer
            ? $"Its value was replaced at {DataFormat.Time(newer.Hlc)}. The cluster keeps whichever write lands last, so saving now replaces that newer value."
            : "Saving now replaces it.";

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Open && !_wasOpen)
        {
            _wasOpen = true;
            await StartAsync();
        }
        else if (!Open && _wasOpen)
        {
            _wasOpen = false;
            _lifetime.Renew();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    private async Task StartAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        var token = _lifetime.Renew();
        _loading = true;
        _loadError = null;
        _saveError = null;
        _conflict = null;
        _note = null;
        _key = string.Empty;
        _keyError = null;
        _violations = [];
        _schema = DataSchemaCheck.NotRead;
        try
        {
            var schema = DataSchemaCheck.LoadAsync(Services, workspace.Tree.StateId, token);
            if (IsCreate)
            {
                _baseline = default;
                _format = DataDraftFormat.Json;
                _text = string.Empty;
            }
            else
            {
                var (version, value) = await ReadCurrentAsync(workspace, Key!, token);
                if (token.IsCancellationRequested)
                {
                    return;
                }

                _baseline = version;
                Load(value);
            }

            _schema = await schema;
            if (token.IsCancellationRequested)
            {
                return;
            }

            Check();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _loadError = DataErrors.Describe(exception, "read this entry");
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

    /// <summary>Opens the draft on <paramref name="value"/>, in the format it reads best in.</summary>
    private void Load(byte[]? value)
    {
        if (value is null)
        {
            _format = DataDraftFormat.Json;
            _text = string.Empty;
            _note = "This key holds no value now; saving writes one.";
            return;
        }

        _format = DataDraft.Detect(value);
        _text = DataDraft.ToText(value, _format) ?? string.Empty;
        _note = _format == DataDraftFormat.Base64
            ? "This value is binary, so it is shown as base64 and replaced whole: paste the base64 of the new value."
            : null;
    }

    private void SetKey(string value)
    {
        _key = value;
        _keyError = null;
        _conflict = null;
    }

    private async Task<string?> ValidateKeyAsync(string key, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(key) || Workspace is not { } workspace || DataServices.Find<IDataReader>(Services) is not { } reader)
        {
            return null;
        }

        try
        {
            var entry = await reader.GetEntryAsync(workspace.Tree.StateId, key, cancellationToken);
            return entry is { IsTombstone: false } ? $"A key named {DataKeysPanel.Clip(key)} already exists in this tree. Open it to change its value." : null;
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // The check is a courtesy; the save itself detects a key written in between.
            return null;
        }
    }

    private void OnTextInput(ChangeEventArgs args)
    {
        _text = args.Value as string ?? string.Empty;
        _saveError = null;
        Check();
    }

    private void SetFormat(string value)
    {
        if (!Enum.TryParse<DataDraftFormat>(value, out var format) || format == _format)
        {
            return;
        }

        var textual = _format != DataDraftFormat.Base64 && format != DataDraftFormat.Base64;
        if (!textual)
        {
            if (!DataDraft.TryEncode(_text, _format, out var bytes, out var error))
            {
                _draftError = error;
                return;
            }

            if (DataDraft.ToText(bytes, format) is not { } converted)
            {
                _draftError = "This value is not UTF-8 text, so it can only be edited as base64.";
                return;
            }

            _text = converted;
        }
        else if (format == DataDraftFormat.Json)
        {
            _text = DataDraft.Pretty(_text);
        }

        _format = format;
        _note = null;
        Check();
    }

    private void FormatJson()
    {
        _text = DataDraft.Pretty(_text);
        Check();
    }

    /// <summary>Validates the draft and, when it encodes, previews it against the schema policy.</summary>
    private byte[]? Check()
    {
        if (!DataDraft.TryEncode(_text, _format, out var bytes, out var error))
        {
            _draftError = error;
            _violations = [];
            return null;
        }

        _draftError = null;
        _violations = _schema.Check(bytes);
        return bytes;
    }

    private Task SaveAsync() => SaveCoreAsync(overwrite: false);

    private Task OverwriteAsync() => SaveCoreAsync(overwrite: true);

    private async Task SaveCoreAsync(bool overwrite)
    {
        if (Workspace is not { } workspace || _saving)
        {
            return;
        }

        var key = IsCreate ? _key : Key!;
        if (IsCreate && string.IsNullOrEmpty(key))
        {
            _keyError = "Name the key.";
            return;
        }

        if (Check() is not { } value)
        {
            return;
        }

        if (Gate.Data is not { } data)
        {
            _saveError = "This Explorer has no data API to write through.";
            return;
        }

        var token = _lifetime.Token;
        _saving = true;
        _saveError = null;
        try
        {
            if (!overwrite)
            {
                var (current, _, entry) = await ReadVersionAsync(workspace, key, token);
                if (token.IsCancellationRequested)
                {
                    return;
                }

                if (current != _baseline)
                {
                    _conflict = new DataEntryConflict(entry, current);
                    return;
                }
            }

            await data.SetAsync(workspace.Tree.StateId, key, value, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _conflict = null;
            var stored = await data.GetAsync(workspace.Tree.StateId, key, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            var kept = stored.Found && DataEntryVersion.Body(stored.Value).AsSpan().SequenceEqual(value);
            Toasts.Show(
                kept
                    ? (IsCreate ? $"Entry {DataKeysPanel.Clip(key)} created." : $"Entry {DataKeysPanel.Clip(key)} saved.")
                    : $"The cluster accepted the write to {DataKeysPanel.Clip(key)}, but the key does not hold the value you saved. A later write replaced it, or the tree's policy diverted it; check the History and Dead letters tabs.",
                kept ? LtToastTone.Success : LtToastTone.Warning);
            await OnSaved.InvokeAsync(key);
            await CloseAsync(false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _saveError = DataErrors.Describe(exception, "write this key");
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _saving = false;
            }
        }
    }

    private async Task ReloadAsync()
    {
        if (Workspace is not { } workspace || _conflict is not { } conflict)
        {
            return;
        }

        var key = IsCreate ? _key : Key!;
        var token = _lifetime.Token;
        _saving = true;
        try
        {
            var (version, value) = await ReadCurrentAsync(workspace, key, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _baseline = version;
            _conflict = null;
            Load(value);
            Check();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _baseline = conflict.Version;
                _conflict = null;
                _saveError = DataErrors.Describe(exception, "read the newer value");
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _saving = false;
            }
        }
    }

    private async Task CloseAsync(bool open)
    {
        if (open)
        {
            return;
        }

        _lifetime.Renew();
        _saving = false;
        _loading = false;
        await OpenChanged.InvokeAsync(false);
    }

    /// <summary>The key's current version, and its whole value (body only) when it holds one.</summary>
    private async Task<(DataEntryVersion Version, byte[]? Value)> ReadCurrentAsync(DataWorkspace workspace, string key, CancellationToken token)
    {
        var (version, value, _) = await ReadVersionAsync(workspace, key, token);
        return (version, value);
    }

    private async Task<(DataEntryVersion Version, byte[]? Value, DataEntry? Entry)> ReadVersionAsync(DataWorkspace workspace, string key, CancellationToken token)
    {
        var reader = DataServices.Find<IDataReader>(Services)
            ?? throw new InvalidOperationException("This Explorer has no state API to read entries through.");
        var entry = await reader.GetEntryAsync(workspace.Tree.StateId, key, token);
        var version = DataEntryVersion.Of(entry);
        if (!version.Exists)
        {
            return (version, null, entry);
        }

        var value = entry!.Value;
        if (entry.Truncated && Gate.Data is { } data)
        {
            var full = await data.GetAsync(workspace.Tree.StateId, key, token);
            value = full.Found ? full.Value : value;
        }

        return (version, DataEntryVersion.Body(value), entry);
    }
}
