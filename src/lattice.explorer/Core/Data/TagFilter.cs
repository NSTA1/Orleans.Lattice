namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// A Data-tab filter that restricts a table's rows to those tagged with
/// <see cref="Tag"/> in the tag index named <see cref="IndexName"/>. The index
/// name is the clean name surfaced by the tag-index catalog; the explorer never
/// sees the internal index-tree naming convention.
/// </summary>
/// <param name="IndexName">The clean tag-index name to filter through.</param>
/// <param name="Tag">The tag value whose member rows are shown.</param>
public sealed record TagFilter(string IndexName, string Tag);
