namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>One row of <c>coverage-exemptions.tsv</c>: a catalogue feature the gate does not require a hit for.</summary>
/// <param name="Id">The feature id.</param>
/// <param name="Reason">Why no journey can exercise it.</param>
/// <param name="Line">The row's line in the file.</param>
public sealed record CoverageExemption(string Id, string Reason, int Line);
