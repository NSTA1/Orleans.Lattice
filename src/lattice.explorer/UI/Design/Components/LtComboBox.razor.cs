using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled type-ahead field for a value that names something the cluster
/// already knows - a tree, a region, a principal, a tenant - offering the
/// matching existing values as the user types.
/// </summary>
/// <remarks>
/// <para>
/// It is an ARIA 1.2 combobox with a listbox popup, and keyboard-complete with no
/// focus trap: Down and Up open the list and move the highlighted option
/// (<c>aria-activedescendant</c>), Enter chooses it, Escape closes the list, Home
/// and End return to editing the text, and Tab leaves. A polite status region
/// announces how many values match.
/// </para>
/// <para>
/// In <see cref="LtComboBoxMode.PickExisting"/> only a listed value is accepted:
/// leaving the field, or <see cref="ConfirmAsync"/> at submit, refuses a typed value
/// that matches nothing with an inline error. In <see cref="LtComboBoxMode.Suggest"/>
/// any text is accepted and a typed value that already exists is flagged. When the
/// source cannot list its values, or fails, the field says why and accepts what is
/// typed: a missing source never blocks the form.
/// </para>
/// <para>
/// Queries are debounced by input, never by the clock: at most one is outstanding,
/// typing cancels it, and the keys typed meanwhile collapse into one query for the
/// latest text.
/// </para>
/// </remarks>
public partial class LtComboBox : IAsyncDisposable
{
    /// <summary>The number of suggestions shown when <see cref="Limit"/> is not set.</summary>
    public const int DefaultLimit = 8;

    private readonly string _inputId = LtIds.Next("lt-combobox");
    private readonly string _listboxId = LtIds.Next("lt-combobox-list");
    private readonly string _hintId;
    private readonly string _noteId;
    private readonly string _flagId;
    private readonly string _errorId;
    private readonly LtSuggestionPump _pump;
    private ElementReference _input;
    private IJSObjectReference? _behaviour;
    private ILtSuggestionSource? _source;
    private IReadOnlyList<LtSuggestion> _items = [];
    private string?[] _optionIds = [];
    private LtSuggestionSet? _answer;
    private string? _answerText;
    private string _text = string.Empty;
    private string? _lastValue;
    private string? _note;
    private string? _flag;
    private string? _matchError;
    private string? _status;
    private int _active = -1;
    private bool _open;
    private bool _disposed;

    /// <summary>Creates the combobox.</summary>
    public LtComboBox()
    {
        _pump = new LtSuggestionPump(DeliverAsync);
        _hintId = _inputId + "-hint";
        _noteId = _inputId + "-note";
        _flagId = _inputId + "-flag";
        _errorId = _inputId + "-error";
    }

    /// <summary>The visible label.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The current value.</summary>
    [Parameter]
    public string? Value { get; set; }

    /// <summary>Raised on every edit, and when a suggestion is chosen, with the new value.</summary>
    [Parameter]
    public EventCallback<string> ValueChanged { get; set; }

    /// <summary>
    /// Raised when a value is committed: a suggestion is chosen, or Enter is pressed
    /// on typed text that the mode accepts. When set, Enter never submits the
    /// surrounding form.
    /// </summary>
    [Parameter]
    public EventCallback<string> OnCommit { get; set; }

    /// <summary>
    /// Raised when a listed suggestion is chosen, with the suggestion itself, so a
    /// form can use its detail (such as a principal's display name). It does not
    /// change what Enter does.
    /// </summary>
    [Parameter]
    public EventCallback<LtSuggestion> OnChoose { get; set; }
    /// <summary>Where the existing values come from. Without one the field is a plain text input.</summary>
    [Parameter]
    public ILtSuggestionSource? Source { get; set; }

    /// <summary>Whether only a listed value is accepted, or any text with existing values suggested.</summary>
    [Parameter]
    public LtComboBoxMode Mode { get; set; } = LtComboBoxMode.PickExisting;

    /// <summary>
    /// What one value is called, in lower case, such as <c>tree</c> or <c>region</c>;
    /// used in the messages the field shows.
    /// </summary>
    [Parameter]
    public string Noun { get; set; } = "value";

    /// <summary>
    /// In <see cref="LtComboBoxMode.Suggest"/>, the sentence shown when the typed
    /// value already exists, such as "A tree with this name already exists."
    /// Leave <see langword="null"/> for a sentence built from <see cref="Noun"/>.
    /// </summary>
    [Parameter]
    public string? ExistingMessage { get; set; }

    /// <summary>
    /// In <see cref="LtComboBoxMode.Suggest"/>, whether a typed value that already
    /// exists is refused rather than only flagged: for a new id that must be unused.
    /// </summary>
    [Parameter]
    public bool RejectExisting { get; set; }

    /// <summary>The most suggestions shown at once.</summary>
    [Parameter]
    public int Limit { get; set; } = DefaultLimit;

    /// <summary>An example of the expected input, shown while the field is empty.</summary>
    [Parameter]
    public string? Placeholder { get; set; }

    /// <summary>A short explanation shown under the input and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>
    /// A validation message from the page. When set it replaces the field's own
    /// message, the input is marked invalid, and a text mark precedes it.
    /// </summary>
    [Parameter]
    public string? Error { get; set; }

    /// <summary>Whether the value is data - a key, id or address - and is set in Cascadia Mono.</summary>
    [Parameter]
    public bool Mono { get; set; } = true;

    /// <summary>Whether the input is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>Whether the input is read-only: focusable and copyable, but neither editable nor suggesting.</summary>
    [Parameter]
    public bool ReadOnly { get; set; }

    /// <summary>
    /// Content drawn inside the field's frame before the input, such as the values
    /// a multi-value field has already chosen, so the two read as one control.
    /// When it is set the frame is drawn around both, not around the input alone.
    /// </summary>
    [Parameter]
    public RenderFragment? Leading { get; set; }

    /// <summary>
    /// Raised when Escape is pressed while the list is already closed, so a host -
    /// a floating panel holding the field - can close too. With
    /// <see cref="OpenOnFocus"/> the list is the field's dropdown rather than a
    /// suggestion, so the one Escape that closes it is raised here as well.
    /// </summary>
    /// <remarks>
    /// The field keeps every key it receives from its ancestors' key handlers and
    /// decides on the server, when the key arrives, whether Escape dismisses its
    /// host, so this holds however quickly the keys come. A render-time
    /// stop-propagation flag that followed the list's state went stale between
    /// two quick presses and swallowed the second. Left unset inside an
    /// <see cref="LtDialog"/>, the dismissal closes that dialog, as Escape
    /// anywhere else in it does.
    /// </remarks>
    [Parameter]
    public EventCallback OnDismiss { get; set; }

    /// <summary>
    /// Whether focusing the input lists the values at once, as a dropdown does,
    /// rather than waiting for typing or Down: for a short list the caller chooses
    /// from, such as the tenants they can switch between. Typing still filters it.
    /// </summary>
    [Parameter]
    public bool OpenOnFocus { get; set; }

    /// <summary>Any further attributes for the <c>input</c> element, such as <c>maxlength</c>.</summary>
    [Parameter(CaptureUnmatchedValues = true)]
    public IReadOnlyDictionary<string, object>? AdditionalAttributes { get; set; }

    [Inject]
    internal IJSRuntime JS { get; set; } = default!;

    [CascadingParameter]
    private LtDialog? Dialog { get; set; }

    /// <summary>The input element's id, for a caller that needs to point at it.</summary>
    public string InputId => _inputId;

    private bool IsListOpen => _open && _items.Count > 0 && !Disabled && !ReadOnly;

    private bool CanSuggest => Source is not null && !Disabled && !ReadOnly;

    private string? ActiveId => IsListOpen && _active >= 0 && _active < _items.Count ? OptionId(_active) : null;

    private string? EffectiveError => Error ?? _matchError;

    private string ListLabel => Label + " suggestions";

    private string InputClass => Mono ? "lt-input lt-input--mono" : "lt-input";

    private string ValueClass => Mono ? "lt-combobox__value lt-combobox__value--mono" : "lt-combobox__value";

    // A field that can list values says so while idle, as a select's chevron does.
    private bool ShowsChevron => CanSuggest;

    private string ControlClass => (Leading is not null, ShowsChevron, EffectiveError is not null) switch
    {
        (false, false, _) => "lt-combobox__control",
        (false, true, _) => "lt-combobox__control lt-combobox__control--picker",
        (true, false, false) => "lt-combobox__control lt-combobox__control--tokens",
        (true, false, true) => "lt-combobox__control lt-combobox__control--tokens lt-combobox__control--invalid",
        (true, true, false) => "lt-combobox__control lt-combobox__control--tokens lt-combobox__control--picker",
        (true, true, true) => "lt-combobox__control lt-combobox__control--tokens lt-combobox__control--picker lt-combobox__control--invalid",
    };


    private string? EnterBehaviour =>
        AdditionalAttributes is { } attributes && attributes.TryGetValue("data-lt-enter", out var given)
            ? given?.ToString()
            : OnCommit.HasDelegate ? "commit" : null;

    private bool ShowsFlag => _flag is not null && EffectiveError is null;

    private string? DescribedBy =>
        Join(Join(Join(Hint is null ? null : _hintId, _note is null ? null : _noteId), ShowsFlag ? _flagId : null), EffectiveError is null ? null : _errorId);

    /// <summary>
    /// Checks the current value against the source, showing the field's own message:
    /// call it before acting on a submit.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when <see cref="LtComboBoxMode.PickExisting"/> and the
    /// value names nothing the source lists, or when <see cref="RejectExisting"/> and it
    /// already exists; otherwise <see langword="true"/>, including an empty value
    /// (whether one is required is the page's rule) and a source that cannot list
    /// its values. A check superseded by a new value or source is refused.
    /// </returns>
    public async Task<bool> ConfirmAsync()
    {
        var text = _text;
        var source = Source;
        if (!CanSuggest || text.Length == 0)
        {
            return true;
        }

        var exists = await ExistsAsync(text).ConfigureAwait(true);
        if (_disposed || !ReferenceEquals(source, Source) || !string.Equals(text, _text, StringComparison.Ordinal))
        {
            // An answer belongs to both its text and its source, not to the next field state.
            return false;
        }

        var accepted = Judge(text, exists);
        StateHasChanged();
        return accepted;
    }

    /// <summary>Moves keyboard focus to the input.</summary>
    /// <returns>A task that completes when the focus request has been sent.</returns>
    /// <remarks>A request whose input a later render removed is a focus not taken, never a fault.</remarks>
    public ValueTask FocusAsync() => _input.FocusSafelyAsync();

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        _pump.Dispose();
        if (_behaviour is { } behaviour)
        {
            _behaviour = null;
            try
            {
                await behaviour.InvokeVoidAsync("dispose").ConfigureAwait(false);
                await behaviour.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException or ObjectDisposedException)
            {
                // Best effort: the page or the circuit has gone.
            }
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Whether <paramref name="value"/> exists in the source: <see langword="true"/>,
    /// <see langword="false"/>, or <see langword="null"/> when the source cannot say.
    /// Uses the latest answer when it was for the same text.
    /// </summary>
    /// <param name="value">The value to look for.</param>
    internal async Task<bool?> ExistsAsync(string value)
    {
        if (Source is not { } source)
        {
            return null;
        }

        var answer = _answer is not null && string.Equals(_answerText, value, StringComparison.Ordinal)
            ? _answer
            : await LookUpAsync(source, value).ConfigureAwait(true);

        if (!answer.IsAvailable)
        {
            _note = answer.UnavailableReason;
            return null;
        }

        return answer.Find(value) is not null;
    }

    /// <summary>The message for a value that names nothing listed.</summary>
    /// <param name="value">The value.</param>
    internal string NoMatchMessage(string value) => $"No {Noun} is named {value}. Choose one from the list.";

    /// <summary>The message for a value that already exists.</summary>
    /// <param name="value">The value.</param>
    internal string ExistsMessage(string value) => ExistingMessage ?? $"A {Noun} named {value} already exists.";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (!string.Equals(Value, _lastValue, StringComparison.Ordinal))
        {
            _lastValue = Value;
            _text = Value ?? string.Empty;
            _matchError = null;
            _flag = null;
            _note = null;
            _status = null;
            _items = [];
            _active = -1;
            _open = false;
            _answer = null;
            _answerText = null;
            _pump.Cancel();
        }

        if (!ReferenceEquals(Source, _source))
        {
            // A new source - another tree, another tenant - makes every answer stale.
            _source = Source;
            _pump.Cancel();
            _answer = null;
            _answerText = null;
            _items = [];
            _note = null;
            _flag = null;
            _active = -1;
        }
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (!firstRender)
        {
            return;
        }

        try
        {
            var module = await JS.InvokeAsync<IJSObjectReference?>("import", ShellDesignAssets.ComboBoxModuleSpecifier).ConfigureAwait(true);
            if (module is null || _disposed)
            {
                return;
            }

            _behaviour = await module.InvokeAsync<IJSObjectReference?>("attach", _input).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException or InvalidOperationException)
        {
            // Script is an enhancement: without it Enter on a highlighted option may
            // also submit the form, and nothing else changes.
        }
    }

    private static string? Join(string? left, string? right) =>
        left is null ? right : right is null ? left : left + " " + right;

    private static string CountSentence(int count, bool truncated) => (count, truncated) switch
    {
        (_, true) => $"More than {count} suggestions; keep typing to narrow them.",
        (1, _) => "1 suggestion.",
        _ => $"{count} suggestions.",
    };

    // Option ids are built once per index and reused on every render and keystroke.
    private string OptionId(int index)
    {
        if (index >= _optionIds.Length)
        {
            var grown = new string[Math.Max(index + 1, Math.Max(Limit, DefaultLimit))];
            Array.Copy(_optionIds, grown, _optionIds.Length);
            _optionIds = grown;
        }

        return _optionIds[index] ??= _listboxId + "-" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task OnTextChangedAsync(string? text)
    {
        _text = text ?? string.Empty;
        _lastValue = _text;
        _matchError = null;
        _flag = null;
        _open = true;
        _active = -1;
        await ValueChanged.InvokeAsync(_text).ConfigureAwait(true);
        await RequestAsync().ConfigureAwait(true);
    }

    private Task RequestAsync() =>
        CanSuggest ? _pump.RequestAsync(Source!, _text, Limit) : Task.CompletedTask;

    private async Task OnClickAsync()
    {
        if (!_open && CanSuggest)
        {
            _open = true;
            await RequestAsync().ConfigureAwait(true);
        }
    }

    private Task OnFocusAsync(FocusEventArgs args) => OpenOnFocus ? OnClickAsync() : Task.CompletedTask;

    private async Task OnKeyDownAsync(KeyboardEventArgs args)
    {
        switch (args.Key)
        {
            case "ArrowDown":
                await OpenOrMoveAsync(+1).ConfigureAwait(true);
                break;

            case "ArrowUp":
                await OpenOrMoveAsync(-1).ConfigureAwait(true);
                break;

            case "Enter":
                if (IsListOpen && _active >= 0 && _active < _items.Count)
                {
                    await ChooseAsync(_items[_active]).ConfigureAwait(true);
                }
                else
                {
                    Close();
                    if (OnCommit.HasDelegate)
                    {
                        await CommitTypedAsync().ConfigureAwait(true);
                    }
                }

                break;

            case "Escape":
                var listed = IsListOpen;
                Close();
                if (!listed || OpenOnFocus)
                {
                    await DismissAsync().ConfigureAwait(true);
                }

                break;

            case "Home":
            case "End":
                // Back to editing the text; the caret itself is the browser's.
                _active = -1;
                break;

            case "Tab":
                Close();
                break;
        }
    }

    private Task DismissAsync() =>
        OnDismiss.HasDelegate ? OnDismiss.InvokeAsync()
        : Dialog is { } dialog ? dialog.DismissFromFieldAsync()
        : Task.CompletedTask;

    private async Task OpenOrMoveAsync(int step)
    {
        if (!CanSuggest)
        {
            return;
        }

        if (!IsListOpen)
        {
            _open = true;
            if (_answer is null || !string.Equals(_answerText, _text, StringComparison.Ordinal))
            {
                _active = step > 0 ? 0 : int.MaxValue;
                await RequestAsync().ConfigureAwait(true);
                return;
            }

            _active = step > 0 ? 0 : _items.Count - 1;
            Announce();
            return;
        }

        _active = _active < 0
            ? (step > 0 ? 0 : _items.Count - 1)
            : (_active + step + _items.Count) % _items.Count;
        Announce();
    }

    private void Announce()
    {
        if (_active >= 0 && _active < _items.Count)
        {
            var item = _items[_active];
            _status = item.Detail is { } detail ? item.Value + ", " + detail : item.Value;
        }
    }

    private async Task ChooseAsync(LtSuggestion item)
    {
        _text = item.Value;
        _lastValue = item.Value;
        _matchError = null;
        _flag = Mode == LtComboBoxMode.Suggest ? ExistsMessage(item.Value) : null;
        if (Mode == LtComboBoxMode.Suggest && RejectExisting)
        {
            _matchError = _flag;
            _flag = null;
        }

        Close();
        _status = item.Value + " chosen.";
        await ValueChanged.InvokeAsync(item.Value).ConfigureAwait(true);
        await OnChoose.InvokeAsync(item).ConfigureAwait(true);
        if (OnCommit.HasDelegate && _matchError is null)
        {
            await OnCommit.InvokeAsync(item.Value).ConfigureAwait(true);
        }
    }

    private async Task CommitTypedAsync()
    {
        if (await ConfirmAsync().ConfigureAwait(true))
        {
            await OnCommit.InvokeAsync(_text).ConfigureAwait(true);
        }
    }

    private async Task OnBlurAsync(FocusEventArgs args)
    {
        Close();
        _pump.Cancel();
        if (_text.Length > 0 && CanSuggest)
        {
            await ConfirmAsync().ConfigureAwait(true);
        }
    }

    private void Close()
    {
        _open = false;
        _active = -1;
    }

    private bool Judge(string text, bool? exists)
    {
        _matchError = null;
        _flag = null;
        if (exists is not { } found)
        {
            // The source cannot say; its note is shown and the text is used as typed.
            return true;
        }

        if (Mode == LtComboBoxMode.PickExisting)
        {
            if (!found)
            {
                _matchError = NoMatchMessage(text);
            }

            return found;
        }

        if (found && RejectExisting)
        {
            _matchError = ExistsMessage(text);
            return false;
        }

        _flag = found ? ExistsMessage(text) : null;
        return true;
    }

    private async Task<LtSuggestionSet> LookUpAsync(ILtSuggestionSource source, string value)
    {
        try
        {
            var answer = await source.SuggestAsync(value, Limit, CancellationToken.None).ConfigureAwait(true);
            if (string.Equals(value, _text, StringComparison.Ordinal) && ReferenceEquals(source, Source))
            {
                Remember(value, answer);
            }

            return answer;
        }
        catch (Exception)
        {
            return LtSuggestionSet.Unavailable(LtSuggestionPump.FaultReason);
        }
    }

    private Task DeliverAsync(string text, LtSuggestionSet answer)
    {
        if (_disposed || !string.Equals(text, _text, StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        Remember(text, answer);
        if (_active == int.MaxValue)
        {
            _active = _items.Count - 1;
        }
        else if (_active >= _items.Count)
        {
            _active = -1;
        }

        if (answer.IsAvailable && Mode == LtComboBoxMode.Suggest && text.Length > 0 && answer.Find(text) is not null)
        {
            _flag = RejectExisting ? null : ExistsMessage(text);
            if (RejectExisting)
            {
                _matchError = ExistsMessage(text);
            }
        }

        if (_open)
        {
            _status = !answer.IsAvailable
                ? answer.UnavailableReason
                : _items.Count == 0
                    ? (text.Length == 0 ? $"No {Noun} to suggest." : $"No {Noun} matches {text}.")
                    : CountSentence(_items.Count, answer.Truncated);
        }

        StateHasChanged();
        return Task.CompletedTask;
    }

    private void Remember(string text, LtSuggestionSet answer)
    {
        _answer = answer;
        _answerText = text;
        _items = answer.Items;
        _note = answer.IsAvailable ? null : answer.UnavailableReason;
    }
}
