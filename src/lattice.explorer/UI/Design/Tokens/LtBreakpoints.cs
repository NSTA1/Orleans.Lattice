namespace Orleans.Lattice.Explorer.UI.Design.Tokens;

/// <summary>
/// The Shell's layout widths: the only .NET copy of the numbers
/// <c>lattice-breakpoints.css</c> declares, and held equal to it by
/// <c>ShellBreakpointHygieneTests</c>.
/// </summary>
/// <remarks>
/// They follow the documentation site's own layout (DESIGN.md, "Layout"): the
/// sidebar is a slide-in panel below 768px and narrows between 768px and 1200px.
/// The widths are measured against the <see cref="ContainerName"/> size
/// container on the Shell root rather than the viewport, so the layout responds
/// to the width the Shell is actually given.
/// </remarks>
internal static class LtBreakpoints
{
    /// <summary>The narrowest width, in CSS pixels, that lays out as <see cref="LtBreakpoint.Medium"/>.</summary>
    public const int MediumMinimumWidth = 768;

    /// <summary>The narrowest width, in CSS pixels, that lays out as <see cref="LtBreakpoint.Expanded"/>.</summary>
    public const int ExpandedMinimumWidth = 1200;

    /// <summary>The size container the breakpoint queries measure.</summary>
    public const string ContainerName = "lt-viewport";

    /// <summary>The custom property that publishes <see cref="MediumMinimumWidth"/> to CSS.</summary>
    public const string MediumMinimumWidthCustomProperty = "--lt-op-breakpoint-medium";

    /// <summary>The custom property that publishes <see cref="ExpandedMinimumWidth"/> to CSS.</summary>
    public const string ExpandedMinimumWidthCustomProperty = "--lt-op-breakpoint-expanded";

    /// <summary>The named width for a measured width.</summary>
    /// <param name="width">The width in CSS pixels; negative values are treated as zero.</param>
    public static LtBreakpoint Resolve(double width) =>
        width >= ExpandedMinimumWidth ? LtBreakpoint.Expanded
        : width >= MediumMinimumWidth ? LtBreakpoint.Medium
        : LtBreakpoint.Compact;
}
