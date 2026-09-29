using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// An on/off setting that takes effect immediately, such as enabling an app.
/// For a choice submitted with a form, use <see cref="LtCheckbox"/>.
/// </summary>
/// <remarks>
/// Rendered as a <c>button</c> with the <c>switch</c> role, so its accessible
/// name is its label and <c>aria-checked</c> carries its state. The state is
/// also written beside the track as text ("On" or "Off"), so it never depends
/// on the track's colour or the thumb's position.
/// </remarks>
public partial class LtSwitch
{
    private readonly string _id = LtIds.Next("lt-switch");

    /// <summary>The visible label, which is also the switch's accessible name.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>Whether the switch is on.</summary>
    [Parameter]
    public bool Checked { get; set; }

    /// <summary>Raised with the new state when the switch is toggled.</summary>
    [Parameter]
    public EventCallback<bool> CheckedChanged { get; set; }

    /// <summary>A short explanation shown under the switch and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>The visible state text while on. Defaults to "On".</summary>
    [Parameter]
    public string OnText { get; set; } = "On";

    /// <summary>The visible state text while off. Defaults to "Off".</summary>
    [Parameter]
    public string OffText { get; set; } = "Off";

    /// <summary>Whether the switch is disabled. A disabled switch does not toggle.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>Any further attributes for the <c>button</c> element.</summary>
    [Parameter(CaptureUnmatchedValues = true)]
    public IReadOnlyDictionary<string, object>? AdditionalAttributes { get; set; }

    private string HintId => _id + "-hint";

    private Task ToggleAsync()
    {
        if (Disabled)
        {
            return Task.CompletedTask;
        }

        Checked = !Checked;
        return CheckedChanged.InvokeAsync(Checked);
    }
}
