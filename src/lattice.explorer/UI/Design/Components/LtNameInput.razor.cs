using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled text box for the name of something new - a group, a rule, a tenant,
/// a snapshot's destination tree - that says inline when the name is already taken.
/// </summary>
/// <remarks>
/// <para>
/// It is a plain text input, not a combobox: it offers no list and draws no arrow,
/// because a field that names a new thing has nothing to pick. A field that names an
/// existing thing is an <see cref="LtComboBox"/> instead.
/// </para>
/// <para>
/// As the name is typed, <see cref="Existing"/> is asked whether it lists it, debounced
/// by input rather than by the clock: at most one query is outstanding and the keys
/// typed meanwhile collapse into one query for the latest text. A listed name is
/// refused with an inline error (or, when <see cref="RejectExisting"/> is off, flagged).
/// A page's own <see cref="Validate"/> check runs when the field is left and again at
/// <see cref="ConfirmAsync"/>, so a slow check, such as a directory lookup, costs one
/// call per name rather than one per key. A source or check that cannot answer is a
/// note, never a refusal: the server validates the name again when it is written.
/// </para>
/// </remarks>
public partial class LtNameInput : IDisposable
{
    /// <summary>The note shown when a check fails: the server still checks the name when it is written.</summary>
    public const string CheckFailedNote = "The name could not be checked now; it is checked again when it is saved.";

    private const int ExistsLimit = LtComboBox.DefaultLimit;

    private readonly string _inputId = LtIds.Next("lt-name-input");
    private readonly string _hintId;
    private readonly string _noteId;
    private readonly string _flagId;
    private readonly string _errorId;
    private readonly LtSuggestionPump _pump;
    private readonly ComponentLifetime _lifetime = new();
    private ElementReference _input;
    private ILtSuggestionSource? _source;
    private string _text = string.Empty;
    private string? _lastValue;
    private string? _note;
    private string? _flag;
    private string? _checkError;
    private string? _status;

    /// <summary>Creates the field.</summary>
    public LtNameInput()
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

    /// <summary>The name.</summary>
    [Parameter]
    public string? Value { get; set; }

    /// <summary>Raised on every edit with the new name.</summary>
    [Parameter]
    public EventCallback<string> ValueChanged { get; set; }

    /// <summary>
    /// The names that already exist, asked only whether it lists the typed name; never
    /// shown as a list. <see langword="null"/> when there is nothing to check against.
    /// </summary>
    [Parameter]
    public ILtSuggestionSource? Existing { get; set; }

    /// <summary>What the field names, such as <c>group</c>, for the default sentence.</summary>
    [Parameter]
    public string Noun { get; set; } = "name";

    /// <summary>
    /// The sentence shown when the typed name already exists; defaults to
    /// "A {noun} named {name} already exists."
    /// </summary>
    [Parameter]
    public string? ExistingMessage { get; set; }

    /// <summary>
    /// Whether an existing name is refused (the default) rather than only flagged, for
    /// a field where naming an existing thing is allowed but worth saying.
    /// </summary>
    [Parameter]
    public bool RejectExisting { get; set; } = true;

    /// <summary>
    /// The page's own check of a name, run when the field is left and at
    /// <see cref="ConfirmAsync"/>: it returns the sentence to show, or
    /// <see langword="null"/> when the name may be used.
    /// </summary>
    [Parameter]
    public Func<string, CancellationToken, Task<string?>>? Validate { get; set; }

    /// <summary>An example of the expected name, shown while the field is empty.</summary>
    [Parameter]
    public string? Placeholder { get; set; }

    /// <summary>A short explanation shown under the input and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>The page's own validation message; it replaces the field's.</summary>
    [Parameter]
    public string? Error { get; set; }

    /// <summary>Whether the name is set in Cascadia Mono, as ids are.</summary>
    [Parameter]
    public bool Mono { get; set; } = true;

    /// <summary>Whether the input is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>Whether the input is read-only: focusable and copyable, but not editable.</summary>
    [Parameter]
    public bool ReadOnly { get; set; }

    /// <summary>Any further attributes for the <c>input</c> element, such as <c>maxlength</c>.</summary>
    [Parameter(CaptureUnmatchedValues = true)]
    public IReadOnlyDictionary<string, object>? AdditionalAttributes { get; set; }

    /// <summary>The input element's id, for a caller that needs to point at it.</summary>
    public string InputId => _inputId;

    private bool CanCheck => !Disabled && !ReadOnly;

    private string? EffectiveError => Error ?? _checkError;

    private bool ShowsFlag => _flag is not null && EffectiveError is null;

    private string InputClass => Mono ? "lt-input lt-input--mono" : "lt-input";

    private string? DescribedBy =>
        Join(Join(Join(Hint is null ? null : _hintId, _note is null ? null : _noteId), ShowsFlag ? _flagId : null), EffectiveError is null ? null : _errorId);

    /// <summary>
    /// Checks the current name - that it is not already taken, then the page's own
    /// <see cref="Validate"/> - showing the field's own message: call it before acting
    /// on a submit.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when the name is refused, or changed while it was being
    /// checked; otherwise <see langword="true"/>, including an empty name (whether one is
    /// required is the page's rule) and a check that could not answer.
    /// </returns>
    public async Task<bool> ConfirmAsync()
    {
        var text = _text.Trim();
        if (!CanCheck || text.Length == 0)
        {
            return true;
        }

        _pump.Cancel();
        var accepted = await CheckAsync(text).ConfigureAwait(true);
        if (!string.Equals(text, _text.Trim(), StringComparison.Ordinal))
        {
            // The text changed while the check ran; the newer text is judged on its own.
            return false;
        }

        Refresh();
        return accepted;
    }

    /// <summary>Moves keyboard focus to the input.</summary>
    /// <returns>A task that completes when the focus request has been answered or refused.</returns>
    /// <remarks>A request whose input a later render removed is a focus not taken, never a fault.</remarks>
    public ValueTask FocusAsync() => _input.FocusSafelyAsync();

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        _pump.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>The sentence shown for a name that already exists.</summary>
    /// <param name="value">The name.</param>
    internal string ExistsMessage(string value) => ExistingMessage ?? $"A {Noun} named {value} already exists.";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (!string.Equals(Value, _lastValue, StringComparison.Ordinal))
        {
            _lastValue = Value;
            _text = Value ?? string.Empty;
            ClearVerdict();
        }

        if (!ReferenceEquals(Existing, _source))
        {
            // Another source - another tree, another tenant - makes every verdict stale.
            _source = Existing;
            _pump.Cancel();
            ClearVerdict();
        }
    }

    private static string? Join(string? left, string? right) =>
        left is null ? right : right is null ? left : left + " " + right;

    private void ClearVerdict()
    {
        _checkError = null;
        _flag = null;
        _note = null;
        _status = null;
    }

    private async Task OnInputAsync(ChangeEventArgs args)
    {
        _text = args.Value as string ?? string.Empty;
        _lastValue = _text;
        ClearVerdict();
        await ValueChanged.InvokeAsync(_text).ConfigureAwait(true);

        var text = _text.Trim();
        if (text.Length == 0 || !CanCheck || Existing is not { } source)
        {
            _pump.Cancel();
            return;
        }

        await _pump.RequestAsync(source, text, ExistsLimit).ConfigureAwait(true);
    }

    private async Task OnBlurAsync(FocusEventArgs args)
    {
        var text = _text.Trim();
        if (text.Length == 0 || !CanCheck)
        {
            return;
        }

        _pump.Cancel();
        await CheckAsync(text).ConfigureAwait(true);
        Refresh();
    }

    private Task DeliverAsync(string text, LtSuggestionSet answer)
    {
        if (_lifetime.IsLeft || !string.Equals(text, _text.Trim(), StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        Judge(text, Exists(answer, text));
        Refresh();
        return Task.CompletedTask;
    }

    private async Task<bool> CheckAsync(string text)
    {
        var token = _lifetime.Token;
        bool? exists = null;
        if (Existing is { } source)
        {
            try
            {
                exists = Exists(await source.SuggestAsync(text, ExistsLimit, token).ConfigureAwait(true), text);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception)
            {
                _note = CheckFailedNote;
            }
        }

        if (_lifetime.IsLeft || !string.Equals(text, _text.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        if (!Judge(text, exists))
        {
            return false;
        }

        if (Validate is not { } validate)
        {
            return true;
        }

        string? refusal;
        try
        {
            refusal = await validate(text, token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return false;
        }
        catch (Exception)
        {
            _note = CheckFailedNote;
            return true;
        }

        if (_lifetime.IsLeft || !string.Equals(text, _text.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        _checkError = refusal;
        if (refusal is not null)
        {
            _status = refusal;
        }

        return refusal is null;
    }

    private bool? Exists(LtSuggestionSet answer, string text)
    {
        if (!answer.IsAvailable)
        {
            _note = answer.UnavailableReason;
            return null;
        }

        _note = null;
        return answer.Find(text) is not null;
    }

    private bool Judge(string text, bool? exists)
    {
        _checkError = null;
        _flag = null;
        if (exists != true)
        {
            return true;
        }

        var sentence = ExistsMessage(text);
        _status = sentence;
        if (RejectExisting)
        {
            _checkError = sentence;
            return false;
        }

        _flag = sentence;
        return true;
    }

    private void Refresh()
    {
        if (!_lifetime.IsLeft)
        {
            StateHasChanged();
        }
    }
}
