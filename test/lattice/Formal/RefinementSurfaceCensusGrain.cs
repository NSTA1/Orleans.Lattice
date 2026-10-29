namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One production grain implementation class found by
/// <see cref="RefinementSurfaceCensus"/>. A partial class declared across
/// several files is one grain.
/// </summary>
/// <param name="Name">The class's simple name, without generic parameters.</param>
/// <param name="Package">The directory under <c>src/</c> that declares it, which is its package.</param>
/// <param name="SourcePath">The <c>/</c>-separated path, relative to the source root, of the first file declaring it.</param>
/// <param name="IsAbstract">True for an abstract base grain, which nothing activates directly.</param>
/// <param name="Contract">
/// The grain's own contract interface, <c>I</c> followed by its name, when its
/// base list implements one; otherwise <see langword="null"/>.
/// </param>
internal sealed record RefinementSurfaceCensusGrain(string Name, string Package, string SourcePath, bool IsAbstract, string? Contract);
