using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Navigation.Completion;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The address line at the top of every page: the current address as a chain,
/// and - on <c>/</c>, Ctrl+K or a click - an input that completes against every
/// visible area, doubles as the command palette after <c>&gt;</c>, and navigates
/// on Enter.
/// </summary>
/// <remarks>
/// <para>
/// It is an ARIA 1.2 combobox: the input owns a listbox of grouped options,
/// arrow keys move the active option (<c>aria-activedescendant</c>), Enter chooses
/// it, and Escape restores the chain and returns focus to where the edit began.
/// A polite status region announces how many suggestions there are and which
/// sources did not answer.
/// </para>
/// <para>
/// Completions arrive progressively: each area's source is asked in parallel
/// under its own time bound, and its group appears when it answers. Groups keep
/// the directory's order however the answers race, so the list never reshuffles
/// under the pointer, and the active option is tracked by identity, not position.
/// </para>
/// </remarks>
public partial class AddressLine : IDisposable
{
    private const string AddressGroupKey = "address";
    private const string CommandsGroupKey = "commands";

    private readonly string _inputId = LtIds.Next("lt-shell-address-input");
    private readonly string _listboxId = LtIds.Next("lt-shell-address-options");
    private readonly string _hintId = LtIds.Next("lt-shell-address-hint");
    private readonly string _chainId = LtIds.Next("lt-shell-address-chain");
    private readonly List<string> _notes = [];

    private List<OptionGroup> _groups = [];
    private CancellationTokenSource? _completion;
    private ElementReference _trigger;
    private ElementReference _input;
    private bool _editing;
    private bool _chainOpen;
    private bool _focusInput;
    private bool _focusTrigger;
    private string _text = string.Empty;
    private string? _activeId;
    private string? _status;

    [CascadingParameter]
    internal ExplorerLocation? Location { get; set; }

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    [Inject]
    internal ExplorerTenancy Tenancy { get; set; } = default!;

    [Inject]
    internal AddressCompletionFanOut FanOut { get; set; } = default!;

    [Inject]
    internal TenantCompletionSource Tenants { get; set; } = default!;

    [Inject]
    internal ShellAppearance Appearance { get; set; } = default!;

    [Inject]
    internal ExplorerTenantSwitch TenantSwitch { get; set; } = default!;

    [Inject]
    internal ShellChromeInterop Interop { get; set; } = default!;

    private ExplorerLocation CurrentLocation => Location ?? ExplorerLocation.Initial;

    private bool IsListOpen => _groups.Count > 0;

    private bool IsCompact => Breakpoint == LtBreakpoint.Compact;

    // Below the small breakpoint the command palette takes the whole screen.
    private string EditingClass =>
        IsCompact && AddressInput.Read(_text).Mode == AddressQueryMode.Command
            ? "lt-shell-address-line lt-shell-address-line--editing lt-shell-address-line--sheet"
            : "lt-shell-address-line lt-shell-address-line--editing";

    private IReadOnlyList<LtChainLink> Chain => BuildChain(CurrentLocation.Address);

    /// <summary>Stops any completion still running.</summary>
    public void Dispose()
    {
        _completion?.Cancel();
        _completion?.Dispose();
        _completion = null;
    }

    /// <summary>
    /// Turns the address line into its input, holding <paramref name="text"/> or,
    /// by default, the current address selected so typing replaces it.
    /// </summary>
    /// <param name="text">The text to start with, such as <c>&gt;</c> for the palette.</param>
    internal async Task OpenAsync(string? text = null)
    {
        _editing = true;
        _focusInput = true;
        _text = text ?? CurrentLocation.Address.Format();

        if (text is null)
        {
            ClearOptions();
            _status = null;
        }
        else
        {
            await RefreshAsync();
        }

        StateHasChanged();
    }

    /// <summary>Moves focus to the address line: its input while editing, otherwise its edit control.</summary>
    /// <remarks>
    /// Through the chrome module, never <see cref="ElementReference"/>'s own focus: a render
    /// can remove the element before the request lands, and an exception here would end the
    /// circuit rather than merely lose the focus.
    /// </remarks>
    internal ValueTask FocusAsync() => Interop.FocusAsync(_editing ? _input : _trigger);

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_focusInput && _editing)
        {
            _focusInput = false;
            await Interop.FocusAndSelectAsync(_input);
        }
        else if (_focusTrigger && !_editing)
        {
            _focusTrigger = false;
            await Interop.FocusAsync(_trigger);
        }
    }

    private static IReadOnlyList<LtChainLink> BuildChain(ExplorerAddress address)
    {
        var nodes = new List<ExplorerAddress>();
        for (var node = address; node is not null; node = node.Parent)
        {
            // With a tenant root, the tenant is the root node; the bare Home above it is not shown.
            if (node.Tenant is null && node.IsHome && address.Tenant is not null)
            {
                break;
            }

            nodes.Add(node);
        }

        nodes.Reverse();

        var links = new LtChainLink[nodes.Count];
        for (var i = 0; i < nodes.Count; i++)
        {
            links[i] = new LtChainLink(NodeLabel(nodes[i], i == 0 ? null : nodes[i - 1]), nodes[i].ToHref());
        }

        return links;
    }

    private static string NodeLabel(ExplorerAddress node, ExplorerAddress? parent)
    {
        if (node.Query.Count > 0)
        {
            return "?" + string.Join('&', node.Query.Select(pair => pair.Key + "=" + pair.Value));
        }

        if (node.Path.Count > 0)
        {
            return node.Path[^1];
        }

        if (node.Area is { } area)
        {
            return area;
        }

        return node.Tenant is { } tenant ? ExplorerAddress.TenantSegment + "/" + tenant : "Home";
    }

    private Task OpenFromClickAsync() => OpenAsync();

    private void ToggleChain() => _chainOpen = !_chainOpen;

    private void CloseChain() => _chainOpen = false;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (!IsCompact)
        {
            _chainOpen = false;
        }
    }

    // Bound with @bind (get, set, oninput) rather than a one-way value plus @oninput.
    // Every keystroke is a round trip to the circuit; with a one-way value a render
    // answering an earlier keystroke wrote that older text back over keys typed since,
    // so fast typing lost characters. A bound input tells the renderer what the browser
    // already holds, so a reply never overwrites newer typing.
    private Task OnTextChangedAsync(string? text)
    {
        _text = text ?? string.Empty;
        return RefreshAsync();
    }

    private async Task OnKeyDownAsync(KeyboardEventArgs args)
    {
        switch (args.Key)
        {
            case "ArrowDown":
                MoveActive(+1);
                break;

            case "ArrowUp":
                MoveActive(-1);
                break;

            case "Enter":
                await ChooseActiveAsync();
                break;

            case "Escape":
                Close(restoreFocus: true);
                break;
        }
    }

    private void OnBlur(FocusEventArgs args)
    {
        if (_editing)
        {
            Close(restoreFocus: false);
        }
    }

    private void MoveActive(int step)
    {
        var options = _groups.SelectMany(group => group.Options).ToArray();
        if (options.Length == 0)
        {
            return;
        }

        var index = Array.FindIndex(options, option => option.ElementId == _activeId);
        index = index < 0
            ? (step > 0 ? 0 : options.Length - 1)
            : (index + step + options.Length) % options.Length;

        _activeId = options[index].ElementId;
        _status = options[index].Label + (options[index].Detail is { } detail ? ", " + detail : string.Empty);
    }

    private async Task ChooseActiveAsync()
    {
        var options = _groups.SelectMany(group => group.Options).ToArray();
        var chosen = options.FirstOrDefault(option => option.ElementId == _activeId) ?? options.FirstOrDefault();

        if (chosen is null)
        {
            _status = string.IsNullOrWhiteSpace(_text) ? "Type an address, a search, or > for a command." : "Nothing matches " + _text.Trim() + ".";
            return;
        }

        await ChooseAsync(chosen);
    }

    private async Task ChooseAsync(Option option)
    {
        Close(restoreFocus: true);

        if (option.Command is { } command)
        {
            if (command.Target is { } target)
            {
                Navigator.NavigateTo(target);
            }

            if (command.InvokeAsync is { } invoke)
            {
                await invoke(CancellationToken.None);
            }
        }
        else if (option.Target is { } target)
        {
            Navigator.NavigateTo(target);
        }
    }

    private void Close(bool restoreFocus)
    {
        Dispose();
        _editing = false;
        _focusTrigger = restoreFocus;
        _text = string.Empty;
        ClearOptions();
        _status = null;
    }

    private void ClearOptions()
    {
        _groups = [];
        _activeId = null;
        _notes.Clear();
    }

    private async Task RefreshAsync()
    {
        Dispose();
        ClearOptions();

        var input = AddressInput.Read(_text);
        var location = CurrentLocation;
        var immediate = ImmediateGroup(input, location);
        if (immediate is not null)
        {
            _groups = [immediate];
        }

        if (input.Mode == AddressQueryMode.Command)
        {
            _status = Announce(null);
            return;
        }

        if (input.Mode == AddressQueryMode.Tenant && !Tenancy.IsActive)
        {
            _notes.Add("Tenancy is off, so there is no tenant to choose.");
            _status = _notes[0];
            return;
        }

        if (input.Mode == AddressQueryMode.Search && input.Text.Length == 0)
        {
            _status = null;
            return;
        }

        var sources = AddressCompletionPlanner.SourcesFor(input.Mode, location.Entries, Tenants);
        if (sources.Count == 0)
        {
            _status = Announce(null);
            return;
        }

        var query = new AddressQuery(input.Text, input.Mode, location.Address);
        var completion = new CancellationTokenSource();
        _completion = completion;
        var token = completion.Token;
        var answered = new Dictionary<string, AddressCompletionBatch>(StringComparer.Ordinal);
        _status = "Searching.";

        try
        {
            await foreach (var batch in FanOut.CompleteAsync(query, sources, token))
            {
                if (token.IsCancellationRequested)
                {
                    return;
                }

                answered[batch.Source.Key] = batch;
                _groups = ComposeGroups(immediate, sources, answered);
                _notes.Clear();
                _notes.AddRange(Notes(sources, answered));
                if (_activeId is not null && !_groups.Any(group => group.Options.Any(option => option.ElementId == _activeId)))
                {
                    _activeId = null;
                }

                StateHasChanged();
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return;
        }

        _status = Announce(string.Join(" ", _notes));
    }

    private Option GoTo(ExplorerAddress address, string elementSuffix) =>
        new(
            ElementId(AddressGroupKey, elementSuffix),
            "Go to " + Navigator.Canonicalize(address).Format(),
            null,
            Navigator.Canonicalize(address),
            null);

    private OptionGroup? ImmediateGroup(AddressInput input, ExplorerLocation location)
    {
        switch (input.Mode)
        {
            case AddressQueryMode.Command:
                var commands = ChromeCommands.Build(location, Appearance, TenantSwitch)
                    .Concat(location.Entries.Where(entry => entry.IsVisible).SelectMany(entry => entry.Area.Commands))
                    .Where(command => command.Title.Contains(input.Text, StringComparison.OrdinalIgnoreCase)
                        || command.Id.Contains(input.Text, StringComparison.OrdinalIgnoreCase))
                    .Select((command, index) => new Option(
                        ElementId(CommandsGroupKey, index.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        command.Title,
                        command.Detail,
                        null,
                        command))
                    .ToArray();
                return commands.Length == 0 ? null : new OptionGroup(CommandsGroupKey, "Commands", commands);

            case AddressQueryMode.Address when ExplorerAddress.TryParse(input.Text, out var address):
                return new OptionGroup(AddressGroupKey, "Address", [GoTo(address, "go")]);

            case AddressQueryMode.Search when input.Text.Length > 0
                && ExplorerAddress.TryParse("/" + input.Text, out var typed)
                && Directory.Find(typed.Area) is not null:
                return new OptionGroup(AddressGroupKey, "Address", [GoTo(typed.WithTenant(location.Address.Tenant), "go")]);

            default:
                return null;
        }
    }

    private List<OptionGroup> ComposeGroups(
        OptionGroup? immediate,
        IReadOnlyList<AddressCompletionSourceEntry> sources,
        Dictionary<string, AddressCompletionBatch> answered)
    {
        var groups = new List<OptionGroup>(sources.Count + 1);
        if (immediate is not null)
        {
            groups.Add(immediate);
        }

        foreach (var source in sources)
        {
            if (answered.TryGetValue(source.Key, out var batch) && batch.Completions.Count > 0)
            {
                var options = new Option[batch.Completions.Count];
                for (var i = 0; i < options.Length; i++)
                {
                    var completion = batch.Completions[i];
                    options[i] = new Option(
                        ElementId(source.Key, i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        completion.Label,
                        completion.Detail,
                        completion.Target,
                        null);
                }

                groups.Add(new OptionGroup(source.Key, source.Name, options));
            }
        }

        return groups;
    }

    private static IEnumerable<string> Notes(
        IReadOnlyList<AddressCompletionSourceEntry> sources,
        Dictionary<string, AddressCompletionBatch> answered)
    {
        foreach (var source in sources)
        {
            if (answered.TryGetValue(source.Key, out var batch))
            {
                switch (batch.Outcome)
                {
                    case AddressCompletionOutcome.TimedOut:
                        yield return source.Name + " did not answer in time.";
                        break;
                    case AddressCompletionOutcome.Failed:
                        yield return source.Name + " could not be searched.";
                        break;
                }
            }
        }
    }

    private string Announce(string? suffix)
    {
        var count = _groups.Sum(group => group.Options.Count);
        var text = count switch
        {
            0 => "No suggestions.",
            1 => "1 suggestion.",
            _ => count.ToString(System.Globalization.CultureInfo.InvariantCulture) + " suggestions.",
        };

        return string.IsNullOrEmpty(suffix) ? text : text + " " + suffix;
    }

    private string ElementId(string groupKey, string suffix) => _listboxId + "-" + groupKey + "-" + suffix;

    private string GroupElementId(string groupKey) => _listboxId + "-" + groupKey;

    /// <summary>One suggestion: a place to go, or a command to run.</summary>
    private sealed record Option(string ElementId, string Label, string? Detail, ExplorerAddress? Target, ExplorerCommand? Command);

    /// <summary>The suggestions from one source, under its name.</summary>
    private sealed record OptionGroup(string Key, string Name, IReadOnlyList<Option> Options);
}
