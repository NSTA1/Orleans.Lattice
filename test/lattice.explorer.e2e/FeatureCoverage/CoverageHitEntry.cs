namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>One feature a run proved.</summary>
/// <param name="Feature">The feature id.</param>
/// <param name="Scopes">The scopes (chapters or steps) whose completion committed it.</param>
public sealed record CoverageHitEntry(string Feature, IReadOnlyList<string> Scopes);
