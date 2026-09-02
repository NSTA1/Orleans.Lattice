using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The workbench area's view: one heading and one focusable control.
/// <para>
/// Written as a hand-rolled <see cref="ComponentBase"/> rather than a <c>.razor</c>
/// file deliberately - it keeps the journey head's extra surface to a single
/// reviewable render method with no Razor compilation in a test project. It starts at
/// <c>h2</c> because the shell owns the surface's one <c>h1</c>, and it renders a real
/// button so the area contributes a focusable stop rather than an inert block.
/// </para>
/// </summary>
internal sealed class JourneyWorkbenchView : ComponentBase
{
    /// <summary>The heading this area renders, used as the journey's arrival proof.</summary>
    internal const string Heading = "Workbench bench";

    /// <summary>The label of the area's own focusable control.</summary>
    internal const string ActionLabel = "Run workbench check";

    /// <inheritdoc />
    protected override void BuildRenderTree(RenderTreeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.OpenElement(0, "section");

        builder.OpenElement(1, "h2");
        builder.AddContent(2, Heading);
        builder.CloseElement();

        builder.OpenElement(3, "button");
        builder.AddAttribute(4, "type", "button");
        builder.AddContent(5, ActionLabel);
        builder.CloseElement();

        builder.CloseElement();
    }
}
