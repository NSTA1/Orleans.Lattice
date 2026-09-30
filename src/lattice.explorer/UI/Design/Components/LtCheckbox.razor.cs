using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled checkbox for a choice that takes effect when a form is submitted.
/// For a setting that takes effect immediately, use <see cref="LtSwitch"/>.
/// </summary>
/// <remarks>
/// A native checkbox, restyled: Space toggles it and its label is a click
/// target. Its boundary is drawn in <c>--lt-op-control-border</c>, which clears
/// the WCAG 2.2 non-text contrast minimum on every surface.
/// </remarks>
public partial class LtCheckbox
{
    private readonly string _id = LtIds.Next("lt-check");

    /// <summary>The visible label.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>Whether the box is checked.</summary>
    [Parameter]
    public bool Checked { get; set; }

    /// <summary>Raised with the new state when the box is toggled.</summary>
    [Parameter]
    public EventCallback<bool> CheckedChanged { get; set; }

    /// <summary>A short explanation shown under the label and announced with the box.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>Whether the box is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>Any further attributes for the <c>input</c> element.</summary>
    [Parameter(CaptureUnmatchedValues = true)]
    public IReadOnlyDictionary<string, object>? AdditionalAttributes { get; set; }

    private string HintId => _id + "-hint";

    private Task HandleChangeAsync(ChangeEventArgs args)
    {
        Checked = args.Value is true || (args.Value is string text && bool.TryParse(text, out var parsed) && parsed);
        return CheckedChanged.InvokeAsync(Checked);
    }
}
