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

    /// <summary>
    /// The name under which the layout cascades <see langword="true"/> while it has not
    /// measured its width yet. A chrome component whose markup differs by band draws
    /// both forms then, marked with the breakpoint layer's <c>lt-only-compact</c> and
    /// <c>lt-medium-up</c> utilities, so the first paint is right at any width. Nothing
    /// is cascaded under it outside the layout, so a component outside it draws one form.
    /// </summary>
    public const string PendingName = "Orleans.Lattice.Explorer.BreakpointPending";
}
