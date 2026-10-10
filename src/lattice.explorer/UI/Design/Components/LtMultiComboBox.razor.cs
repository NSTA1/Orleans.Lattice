using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled type-ahead field for a list of values the cluster already knows,
/// such as allowed region ids or admin subjects: the chosen values as removable
/// chips, and a combobox that adds the next one.
/// </summary>
/// <remarks>
/// A value is added by choosing a suggestion, by Enter, or by typing or pasting
/// a comma after it. In <see cref="LtComboBoxMode.PickExisting"/> a value the
/// source does not list is refused with an inline error and left in the input to
/// correct; when the source cannot list its values the field says why and adds
/// what is typed. A mixed-validity paste keeps both refused values and unfinished text.
/// Duplicates are ignored. Each chip's remove control names the
/// value it removes.
/// </remarks>
public partial class LtMultiComboBox
{
    private static readonly char[] Separators = [',', ';', '\n', '\r', '\t'];

    private LtComboBox? _box;
    private IReadOnlyList<string> _values = [];
    private IReadOnlyList<string>? _lastValues;
    private string _text = string.Empty;
    private string? _error;

    /// <summary>The visible label.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The chosen values, in the order they were added.</summary>
    [Parameter]
    public IReadOnlyList<string>? Values { get; set; }

    /// <summary>Raised with the new list whenever a value is added or removed.</summary>
    [Parameter]
    public EventCallback<IReadOnlyList<string>> ValuesChanged { get; set; }

    /// <summary>Where the existing values come from. Without one any text is added.</summary>
    [Parameter]
    public ILtSuggestionSource? Source { get; set; }

    /// <summary>Whether only listed values are added, or any text with existing values suggested.</summary>
    [Parameter]
    public LtComboBoxMode Mode { get; set; } = LtComboBoxMode.PickExisting;

    /// <summary>What one value is called, in lower case, such as <c>region</c>.</summary>
    [Parameter]
    public string Noun { get; set; } = "value";

    /// <summary>The most suggestions shown at once.</summary>
    [Parameter]
    public int Limit { get; set; } = LtComboBox.DefaultLimit;

    /// <summary>An example of the expected input, shown while the input is empty.</summary>
    [Parameter]
    public string? Placeholder { get; set; }

    /// <summary>A short explanation shown under the input and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>A validation message from the page; it replaces the field's own message.</summary>
    [Parameter]
    public string? Error { get; set; }

    /// <summary>Whether the values are data - ids - and are set in Cascadia Mono.</summary>
    [Parameter]
    public bool Mono { get; set; } = true;

    /// <summary>Whether the field is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    private string ChipValueClass => Mono ? "lt-combobox__chip-value lt-combobox__chip-value--mono" : "lt-combobox__chip-value";

    /// <summary>
    /// Adds whatever is still typed in the input, as Enter would: call it before
    /// acting on a submit, so a value typed but not yet added is not lost.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> when the typed text was refused (it names nothing the
    /// source lists, in <see cref="LtComboBoxMode.PickExisting"/>); otherwise
    /// <see langword="true"/>.
    /// </returns>
    public async Task<bool> ConfirmAsync()
    {
        if (_text.Trim().Length == 0)
        {
            return true;
        }

        await AddAsync(_text).ConfigureAwait(true);
        return _error is null;
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (!ReferenceEquals(Values, _lastValues))
        {
            _lastValues = Values;
            _values = Values ?? [];
        }
    }

    private async Task OnTextChangedAsync(string text)
    {
        _text = text;
        _error = null;
        var comma = text.LastIndexOfAny(Separators);
        if (comma >= 0)
        {
            // A typed or pasted separator adds everything before it.
            var rest = text[(comma + 1)..];
            await AddAsync(text[..comma]).ConfigureAwait(true);
            if (_error is null)
            {
                _text = rest;
            }
            else if (!string.IsNullOrWhiteSpace(rest))
            {
                _text += ", " + rest.TrimStart();
            }
        }
    }

    private async Task AddAsync(string text)
    {
        _error = null;
        var added = new List<string>(_values.Count + 1);
        added.AddRange(_values);
        var refused = new List<string>();
        foreach (var part in text.Split(Separators, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (added.Contains(part, StringComparer.Ordinal))
            {
                continue;
            }

            var exists = Source is null || _box is null ? null : await _box.ExistsAsync(part).ConfigureAwait(true);
            if (Mode == LtComboBoxMode.PickExisting && exists == false)
            {
                refused.Add(part);
                _error ??= _box!.NoMatchMessage(part);
                continue;
            }

            added.Add(part);
        }

        _text = string.Join(", ", refused);
        if (added.Count != _values.Count)
        {
            _values = added;
            _lastValues = added;
            await ValuesChanged.InvokeAsync(added).ConfigureAwait(true);
        }
    }

    private async Task RemoveAsync(string value)
    {
        var remaining = new List<string>(_values.Count);
        foreach (var candidate in _values)
        {
            if (!string.Equals(candidate, value, StringComparison.Ordinal))
            {
                remaining.Add(candidate);
            }
        }

        _values = remaining;
        _lastValues = remaining;
        await ValuesChanged.InvokeAsync(remaining).ConfigureAwait(true);
        if (_box is not null)
        {
            await _box.FocusAsync().ConfigureAwait(true);
        }
    }
}
