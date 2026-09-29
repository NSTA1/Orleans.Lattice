using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled choice from a short, fixed list, rendered as the platform's native
/// <c>select</c> so keyboard, type-ahead and screen-reader behaviour are the
/// platform's own.
/// </summary>
public partial class LtSelect
{
    private readonly string _id = LtIds.Next("lt-select");

    /// <summary>The visible label.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The options, in display order.</summary>
    [Parameter, EditorRequired]
    public IReadOnlyList<LtSelectOption> Options { get; set; } = [];

    /// <summary>The chosen option's value.</summary>
    [Parameter]
    public string? Value { get; set; }

    /// <summary>Raised with the newly chosen option's value.</summary>
    [Parameter]
    public EventCallback<string> ValueChanged { get; set; }

    /// <summary>A short explanation shown under the control and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>Whether the control is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>Any further attributes for the <c>select</c> element.</summary>
    [Parameter(CaptureUnmatchedValues = true)]
    public IReadOnlyDictionary<string, object>? AdditionalAttributes { get; set; }

    private string HintId => _id + "-hint";

    private bool IsSelected(LtSelectOption option) =>
        string.Equals(option.Value, Value, StringComparison.Ordinal);

    private Task HandleChangeAsync(ChangeEventArgs args)
    {
        Value = args.Value as string ?? string.Empty;
        return ValueChanged.InvokeAsync(Value);
    }
}
