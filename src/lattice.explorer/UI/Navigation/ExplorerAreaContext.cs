namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// Which area the shell is currently showing, cascaded to everything rendered
/// inside it.
/// </summary>
/// <remarks>
/// <para>
/// An area plugin renders inside a frame it did not build, and two of its
/// decisions depend on that frame: what to call its sub-surfaces without
/// repeating the rail's word for the area, and which region its sub-surface
/// strip controls. Cascading the answer is what lets a plugin get both right
/// without taking a dependency on the shell.
/// </para>
/// <para>
/// Immutable and compared by value, so a re-render that resolves the same area
/// hands the same context down and nothing beneath it re-renders for it.
/// </para>
/// </remarks>
/// <param name="Slug">
/// The area's canonical lower-case route slug, as it appears in the address.
/// </param>
/// <param name="Label">
/// The area's display label, as the rail spells it. The word a nested surface
/// must not repeat.
/// </param>
public sealed record ExplorerAreaContext(string Slug, string Label);
