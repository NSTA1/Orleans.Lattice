using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// What a page shows when there is nothing to list: a hollow node - the bottom
/// element, "nothing yet" - beside a heading, a sentence saying why, and the
/// action that would change it. No illustration, no card.
/// </summary>
public partial class LtEmptyState
{
    /// <summary>The heading, such as "No apps installed".</summary>
    [Parameter, EditorRequired]
    public string Title { get; set; } = string.Empty;

    /// <summary>Why the list is empty, and what the reader can do about it.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }

    /// <summary>The action that would fill the list, such as a button to browse sources.</summary>
    [Parameter]
    public RenderFragment? Actions { get; set; }

    /// <summary>The heading level, 2 to 4, so the page outline stays correct. Defaults to 2.</summary>
    [Parameter]
    public int HeadingLevel { get; set; } = 2;

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (HeadingLevel is < 2 or > 4)
        {
            throw new ArgumentOutOfRangeException(
                nameof(HeadingLevel), HeadingLevel, "An empty state's heading level must be 2, 3 or 4.");
        }
    }
}
