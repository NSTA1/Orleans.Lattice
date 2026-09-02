namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A repository's membership coverage, read from the vector-membership tree in one
/// scan by <see cref="RepoContextVectorWriter.LoadCoverageAsync"/>. It separates the
/// two kinds of enable-wins flag that share the tree: <see cref="Embedded"/> sources
/// carry a real landed vector, while <see cref="Contentless"/> sources were considered
/// and found to have no embeddable passage (an empty or whitespace-only file, or one
/// that chunks to zero windows) and are recorded only so the always-on gap sweep and
/// unchanged-file selection stop re-driving them forever. Both sets carry plain
/// 16-character source identifiers (the contentless marker's reserved prefix is
/// stripped), so a caller probes either with the identifier produced by
/// <see cref="VectorCodec.SourceId(string)"/>. A file is a gap only when it is in
/// neither set - see <see cref="IsCovered"/>.
/// </summary>
/// <param name="Embedded">Source identifiers with a real landed vector.</param>
/// <param name="Contentless">Source identifiers considered but carrying no embeddable passage.</param>
internal readonly record struct RepoContextEmbeddingCoverage(
    IReadOnlySet<string> Embedded,
    IReadOnlySet<string> Contentless)
{
    /// <summary>
    /// Whether <paramref name="sourceId"/> is covered - either it has a real embedding
    /// or it was considered and recorded as contentless - so the gap sweep and
    /// unchanged-file selection should not treat it as a missing embedding.
    /// </summary>
    /// <param name="sourceId">The 16-character source identifier to probe.</param>
    /// <returns><see langword="true"/> when the source is embedded or contentless-marked.</returns>
    public bool IsCovered(string sourceId) =>
        Embedded.Contains(sourceId) || Contentless.Contains(sourceId);

    /// <summary>
    /// Coverage that knows nothing. Used when a coverage probe could not be
    /// completed, so a caller can degrade deliberately rather than treat a failed
    /// probe as authoritative: nothing is covered, so a caller that would re-embed
    /// on the strength of it must instead skip the gap sweep for that pass.
    /// </summary>
    public static RepoContextEmbeddingCoverage Empty { get; } =
        new(new HashSet<string>(StringComparer.Ordinal), new HashSet<string>(StringComparer.Ordinal));
}
