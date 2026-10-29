namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One row of the unmodelled-grain register, <c>spec/UNMODELLED.md</c>: a
/// production grain no refinement note names, and why.
/// </summary>
/// <param name="Grain">The grain class's simple name.</param>
/// <param name="Package">The directory under <c>src/</c> that declares it.</param>
/// <param name="Reason">Why no specification models it.</param>
/// <param name="LineNumber">The 1-based line of the row in the register, for failure messages.</param>
internal sealed record RefinementSurfaceCensusEntry(string Grain, string Package, string Reason, int LineNumber);
