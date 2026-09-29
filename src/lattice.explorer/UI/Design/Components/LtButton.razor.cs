using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The design system's one button, outlined or quiet, drawn as DESIGN.md
/// describes: an ink border on the page, the soft marker behind it on hover,
/// and the marker filling it while pressed.
/// </summary>
/// <remarks>
/// It renders a native <c>button</c>, so Enter and Space, focus, and the
/// disabled state all behave as the platform defines them. The primitive owns
/// the <c>class</c> and <c>type</c> attributes; any other attribute passes
/// through unchanged.
/// </remarks>
public partial class LtButton
{
    /// <summary>The button's label.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>The button's visual weight. Defaults to <see cref="LtButtonVariant.Outlined"/>.</summary>
    [Parameter]
    public LtButtonVariant Variant { get; set; } = LtButtonVariant.Outlined;

    /// <summary>Whether the button submits its form. Defaults to <see cref="LtButtonType.Button"/>.</summary>
    [Parameter]
    public LtButtonType Type { get; set; } = LtButtonType.Button;

    /// <summary>Whether the button is disabled. A disabled button raises no click.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>
    /// For a toggle button, whether it is pressed; rendered as <c>aria-pressed</c>.
    /// Leave <see langword="null"/> for an ordinary button.
    /// </summary>
    [Parameter]
    public bool? Pressed { get; set; }

    /// <summary>Raised when the button is activated by pointer, Enter or Space.</summary>
    [Parameter]
    public EventCallback<MouseEventArgs> OnClick { get; set; }

    /// <summary>Any further attributes, such as <c>aria-label</c> or <c>aria-describedby</c>.</summary>
    [Parameter(CaptureUnmatchedValues = true)]
    public IReadOnlyDictionary<string, object>? AdditionalAttributes { get; set; }

    private string TypeAttribute => Type == LtButtonType.Submit ? "submit" : "button";

    private string? AriaPressed => Pressed switch
    {
        true => "true",
        false => "false",
        null => null,
    };

    private string CssClass => Variant switch
    {
        LtButtonVariant.Quiet => "lt-btn lt-btn--quiet",
        LtButtonVariant.Destructive => "lt-btn lt-btn--destructive",
        _ => "lt-btn",
    };

    private Task HandleClickAsync(MouseEventArgs args) =>
        Disabled ? Task.CompletedTask : OnClick.InvokeAsync(args);
}
