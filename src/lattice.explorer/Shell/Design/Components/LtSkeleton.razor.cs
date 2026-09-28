using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Design.Components;

/// <summary>
/// A loading placeholder: hairline-weight bars on sunken paper in the shape of
/// the content to come, announced once as a status message. The bars pulse
/// gently, and hold still when the reader asks for reduced motion.
/// </summary>
public partial class LtSkeleton
{
    /// <summary>The largest number of placeholder lines a skeleton draws.</summary>
    public const int MaximumLines = 12;

    /// <summary>How many placeholder lines to draw, 1 to <see cref="MaximumLines"/>. Defaults to 3.</summary>
    [Parameter]
    public int Lines { get; set; } = 3;

    /// <summary>What is loading, as announced to assistive technology. Defaults to "Loading".</summary>
    [Parameter]
    public string Label { get; set; } = "Loading";

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Lines is < 1 or > MaximumLines)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Lines), Lines, $"A skeleton draws between 1 and {MaximumLines} lines.");
        }
    }
}
