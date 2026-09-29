namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// The cascading value through which the Shell's layout tells the primitives
/// which width band it is laid out in, so a primitive such as
/// <see cref="LtTable{TItem}"/> can render its compact form below the small
/// breakpoint.
/// </summary>
/// <remarks>
/// The layout measures its own root and cascades a
/// <see cref="Tokens.LtBreakpoint"/> under <see cref="Name"/>. Outside the layout,
/// or before the width is known, nothing is cascaded and a primitive renders its
/// expanded form, so a page never depends on script to be usable.
/// </remarks>
internal static class LtBreakpointCascade
{
    /// <summary>The name the width band is cascaded under.</summary>
    public const string Name = "Orleans.Lattice.Explorer.Breakpoint";
}
