using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The ledger area's view. Reaching it at all is the assertion; what it renders only
/// has to be unmistakably itself.
/// </summary>
internal sealed class JourneyLedgerView : ComponentBase
{
    /// <summary>The heading this area renders, used as the journey's arrival proof.</summary>
    internal const string Heading = "Ledger entries";

    /// <inheritdoc />
    protected override void BuildRenderTree(RenderTreeBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.OpenElement(0, "section");

        builder.OpenElement(1, "h2");
        builder.AddContent(2, Heading);
        builder.CloseElement();

        builder.CloseElement();
    }
}
