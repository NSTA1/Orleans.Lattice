namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One entry of a refinement note's <c>## Deliberate abstraction gaps</c>
/// section, as the refinement index (contract C6 of #5148) records it.
/// </summary>
/// <param name="Title">
/// The bullet's bold lead-in without its closing full stop, or, for a bullet
/// or paragraph with no bold lead-in, its first sentence.
/// </param>
/// <param name="Symbols">
/// The production symbols the entry names in backticks: <c>Type.Member</c>
/// forms whose type is declared under <c>src/</c>, and bare type names declared
/// there. Sorted ordinally, without duplicates; empty when the entry names none.
/// </param>
internal sealed record RefinementIndexGap(string Title, IReadOnlyList<string> Symbols);
