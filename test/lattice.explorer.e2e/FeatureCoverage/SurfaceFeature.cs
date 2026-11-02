namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>One feature the Explorer's assemblies declare, as the reflected half of the catalogue names it.</summary>
/// <param name="Id">The catalogue id, such as <c>command:data.refresh</c>.</param>
/// <param name="Kind">The catalogue kind: one of <see cref="FeatureKinds.Reflected"/>.</param>
/// <param name="Source">Where it was found: a <c>Type.Member</c>, or a markup file for a markup-only tab.</param>
public sealed record SurfaceFeature(string Id, string Kind, string Source);
