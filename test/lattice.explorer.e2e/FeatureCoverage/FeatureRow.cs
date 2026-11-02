namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>One row of <c>explorer-features.tsv</c>: one feature of the Explorer.</summary>
/// <param name="Id">The feature id, prefixed by its kind: <c>route:/data</c>, <c>use-case:data/export-streamed</c>.</param>
/// <param name="Kind">The kind (<see cref="FeatureKinds"/>).</param>
/// <param name="Source">
/// Where the feature is defined: the declaring member for a reflected row (or <c>pending:#N</c>
/// while the gap feature that adds it has not merged), or the doc anchor
/// (<c>docs/lattice.explorer/areas.md#data</c>) for a curated row.
/// </param>
/// <param name="Description">What the feature is.</param>
/// <param name="Journey">The chapter that owns proving it (<see cref="Story.StoryOutline"/>).</param>
/// <param name="Line">The row's line in the file.</param>
public sealed record FeatureRow(string Id, string Kind, string Source, string Description, string Journey, int Line)
{
    /// <summary>The source prefix of a reflected row whose gap feature has not merged yet.</summary>
    public const string PendingPrefix = "pending:#";

    /// <summary>Whether the row names a feature a gap item adds but has not merged.</summary>
    public bool IsPending => Source.StartsWith(PendingPrefix, StringComparison.Ordinal);
}
