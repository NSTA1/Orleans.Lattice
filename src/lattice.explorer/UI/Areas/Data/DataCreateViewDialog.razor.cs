using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The New view dialog over one administered tree: lists the projection providers
/// the cluster's host registered (each with its description), takes a view name
/// and the provider's payload - a form when the provider describes its fields,
/// otherwise JSON - and creates the view through the tree-administration facade,
/// which authorizes the source and validates everything again.
/// </summary>
public partial class DataCreateViewDialog : IDisposable
{
    /// <summary>The largest payload the facade accepts, in bytes.</summary>
    internal const int MaxPayloadBytes = 64 * 1024;

    /// <summary>
    /// The characters a view name may not carry: the storage-unsafe key characters and
    /// the generation separator, which the cluster also refuses.
    /// </summary>
    private static readonly char[] ReservedNameCharacters = ['/', '\\', '#', '?', '~'];

    private readonly ComponentLifetime _lifetime = new();
    private readonly Dictionary<string, string> _texts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, bool> _flags = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _fieldErrors = new(StringComparer.Ordinal);
    private readonly string _jsonId = "lt-data-view-json-" + Guid.NewGuid().ToString("N")[..8];
    private IReadOnlyList<TreeViewProviderInfo>? _providers;
    private string? _loadError;
    private string? _providerKey;
    private string? _name;
    private string? _json;
    private string? _nameError;
    private string? _jsonError;
    private string? _submitError;
    private bool _busy;
    private bool _loadedOpen;

    /// <summary>Whether the dialog is open.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised when the dialog asks to close.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }


    /// <summary>Raised with the created view's status once the cluster accepted it.</summary>
    [Parameter]
    public EventCallback<TreeViewStatus> OnCreated { get; set; }

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal DataAdminGate Gate { get; set; } = default!;

    /// <summary>The tree the view is created over. Its state id is passed to the facade but never rendered.</summary>
    private DataTreeEntry Source => Workspace!.Tree;

    /// <summary>The names of the views the caller can see, so a taken name is refused before the call.</summary>
    private IReadOnlyCollection<string> ExistingNames => Workspace?.Directory.Loaded is { } entries
        ? entries.Where(entry => entry.Kind == DataTreeKind.View && entry.ViewName is not null).Select(entry => entry.ViewName!).ToHashSet(StringComparer.Ordinal)
        : [];

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement Placement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string JsonId => _jsonId;

    private TreeViewProviderInfo? Selected => _providers?.FirstOrDefault(provider => string.Equals(provider.ProviderKey, _providerKey, StringComparison.Ordinal));

    private IReadOnlyList<LtSelectOption> ProviderOptions => _providers?.Select(provider => new LtSelectOption(provider.ProviderKey, provider.ProviderKey)).ToArray() ?? [];

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (!Open || Workspace is null)
        {
            _loadedOpen = false;
            return;
        }

        if (_loadedOpen)
        {
            return;
        }

        _loadedOpen = true;
        Reset();
        var token = _lifetime.Renew();
        if (Gate.Admin is not { } admin)
        {
            _loadError = "This cluster does not let you create views.";
            return;
        }

        try
        {
            var catalog = await admin.ListViewProvidersAsync(Source.StateId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _providers = catalog.Providers.IsDefault ? [] : catalog.Providers;
            _providerKey = _providers.Count > 0 ? _providers[0].ProviderKey : null;
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _loadError = DataErrors.Describe(exception, "list the view providers");
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Builds the payload a provider receives: a JSON object from the described
    /// fields, or the typed JSON (an empty object when nothing was typed).
    /// </summary>
    /// <param name="provider">The chosen provider.</param>
    /// <param name="texts">The text and number field values, by field name.</param>
    /// <param name="flags">The boolean field values, by field name.</param>
    /// <param name="json">The typed JSON, for a provider that describes no fields.</param>
    /// <param name="fieldErrors">Receives an error per field that is missing or malformed.</param>
    /// <param name="jsonError">Receives the error when the typed JSON does not parse.</param>
    /// <returns>The UTF-8 payload, or <see langword="null"/> when an error was reported.</returns>
    internal static byte[]? BuildPayload(
        TreeViewProviderInfo provider,
        IReadOnlyDictionary<string, string> texts,
        IReadOnlyDictionary<string, bool> flags,
        string? json,
        IDictionary<string, string> fieldErrors,
        out string? jsonError)
    {
        jsonError = null;
        if (provider.PayloadFields.IsDefaultOrEmpty)
        {
            var text = string.IsNullOrWhiteSpace(json) ? "{}" : json.Trim();
            try
            {
                using var _ = JsonDocument.Parse(text);
            }
            catch (JsonException)
            {
                jsonError = "This is not valid JSON.";
                return null;
            }

            return Encoding.UTF8.GetBytes(text);
        }

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var field in provider.PayloadFields)
            {
                if (field.Kind == TreeViewProviderFieldKind.Boolean)
                {
                    writer.WriteBoolean(field.Name, flags.TryGetValue(field.Name, out var flag) && flag);
                    continue;
                }

                var value = texts.TryGetValue(field.Name, out var typed) ? typed?.Trim() : null;
                if (string.IsNullOrEmpty(value))
                {
                    if (field.Required)
                    {
                        fieldErrors[field.Name] = "Enter a value.";
                    }

                    continue;
                }

                if (field.Kind == TreeViewProviderFieldKind.Number)
                {
                    if (!decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var number))
                    {
                        fieldErrors[field.Name] = "Enter a number, such as 12 or 0.5.";
                        continue;
                    }

                    writer.WriteNumber(field.Name, number);
                }
                else
                {
                    writer.WriteString(field.Name, value);
                }
            }

            writer.WriteEndObject();
        }

        return fieldErrors.Count > 0 ? null : buffer.WrittenSpan.ToArray();
    }

    /// <summary>The reason a view name cannot be used, or <see langword="null"/> when it can.</summary>
    /// <param name="name">The typed name.</param>
    /// <param name="existing">The names already taken.</param>
    internal static string? NameError(string? name, IReadOnlyCollection<string> existing)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return "Name the view.";
        }

        if (name.Any(char.IsControl) || name.IndexOfAny(ReservedNameCharacters) >= 0)
        {
            return "A view name cannot contain /, \\, #, ? or ~.";
        }

        return existing.Contains(name) ? "A view with this name already exists." : null;
    }

    private void Reset()
    {
        _providers = null;
        _loadError = null;
        _providerKey = null;
        _name = null;
        _json = null;
        _nameError = null;
        _jsonError = null;
        _submitError = null;
        _busy = false;
        _texts.Clear();
        _flags.Clear();
        _fieldErrors.Clear();
    }

    private void OnProviderChanged(string key)
    {
        _providerKey = key;
        _texts.Clear();
        _flags.Clear();
        _fieldErrors.Clear();
        _jsonError = null;
        _submitError = null;
    }

    private async Task SubmitAsync()
    {
        if (_busy || Selected is not { } provider || Gate.Admin is not { } admin)
        {
            return;
        }

        _fieldErrors.Clear();
        _submitError = null;
        _nameError = NameError(_name, ExistingNames);
        var payload = BuildPayload(provider, _texts, _flags, _json, _fieldErrors, out _jsonError);
        if (payload is not null && payload.Length > MaxPayloadBytes)
        {
            _jsonError = "The payload is larger than the 64 KiB a view accepts.";
            payload = null;
        }

        if (_nameError is not null || payload is null)
        {
            return;
        }

        _busy = true;
        var token = _lifetime.Token;
        try
        {
            var status = await admin.CreateViewAsync(_name!.Trim(), Source.StateId, provider.ProviderKey, payload, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _busy = false;
            await OnCreated.InvokeAsync(status);
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _busy = false;
            _submitError = CreateErrorText(exception);
        }
    }

    private static string CreateErrorText(Exception exception) =>
        DataErrors.IsDenied(exception) || DataErrors.IsNotOffered(exception) || exception is KeyNotFoundException
            ? DataErrors.Describe(exception, "create this view")
            : IsRefusal(exception)
                ? "The cluster refused this view: the provider did not accept the payload, or the name cannot be used. Check both and try again."
                : DataErrors.Describe(exception, "create this view");

    private static bool IsRefusal(Exception exception) => exception switch
    {
        ArgumentException or InvalidOperationException or JsonException => true,
        Grpc.Core.RpcException rpc => rpc.StatusCode is Grpc.Core.StatusCode.InvalidArgument or Grpc.Core.StatusCode.FailedPrecondition,
        _ => exception.InnerException is Grpc.Core.RpcException inner
            && inner.StatusCode is Grpc.Core.StatusCode.InvalidArgument or Grpc.Core.StatusCode.FailedPrecondition,
    };

    private Task CancelAsync() => OnOpenChangedAsync(false);

    private async Task OnOpenChangedAsync(bool open)
    {
        if (!open && _busy)
        {
            return;
        }

        await OpenChanged.InvokeAsync(open);
    }
}
