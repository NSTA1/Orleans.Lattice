using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled single-line text input on sunken paper, with an optional hint and
/// an error that is announced with the input and marked by text, not colour
/// alone.
/// </summary>
/// <remarks>
/// The label is always visible: an Operate screen is read by people who return
/// to it infrequently, and a placeholder is not a label. Set <see cref="Mono"/>
/// for anything the user types that is a key, a tree id or an address.
/// </remarks>
public partial class LtTextInput
{
    private readonly string _id = LtIds.Next("lt-input");
    private ElementReference _input;

    /// <summary>The visible label.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The current value.</summary>
    [Parameter]
    public string? Value { get; set; }

    /// <summary>Raised on every edit with the new value.</summary>
    [Parameter]
    public EventCallback<string> ValueChanged { get; set; }

    /// <summary>An example of the expected input, shown while the field is empty.</summary>
    [Parameter]
    public string? Placeholder { get; set; }

    /// <summary>A short explanation shown under the input and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>
    /// A validation message. When set the input is marked invalid, the message is
    /// announced with it, and a text mark precedes it so it survives greyscale.
    /// </summary>
    [Parameter]
    public string? Error { get; set; }

    /// <summary>Whether the value is data - a key, id or address - and is set in Cascadia Mono.</summary>
    [Parameter]
    public bool Mono { get; set; }

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
    public string InputId => _id;

    private string HintId => _id + "-hint";

    private string ErrorId => _id + "-error";

    private string InputClass => Mono ? "lt-input lt-input--mono" : "lt-input";

    private string? DescribedBy => (Hint, Error) switch
    {
        (null, null) => null,
        (not null, null) => HintId,
        (null, not null) => ErrorId,
        _ => HintId + " " + ErrorId,
    };

    /// <summary>Moves keyboard focus to the input.</summary>
    /// <remarks>
    /// A focus the browser refuses - the input removed by a later render before the request
    /// arrives, or the circuit closing - is ignored rather than thrown, so it can never end
    /// the circuit.
    /// </remarks>
    /// <returns>A task that completes when the focus request has been answered or refused.</returns>
    public ValueTask FocusAsync() => _input.FocusSafelyAsync();

    private Task HandleInputAsync(ChangeEventArgs args)
    {
        Value = args.Value as string ?? string.Empty;
        return ValueChanged.InvokeAsync(Value);
    }
}
