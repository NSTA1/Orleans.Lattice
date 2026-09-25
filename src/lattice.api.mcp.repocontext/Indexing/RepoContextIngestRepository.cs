namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The per-repository state <see cref="RepoContextIngestReporter"/> keeps once a
/// repository has begun its first pass on this silo: the precomputed
/// <c>repository</c> tag, and the monotonic timestamps its last-completed-pass age
/// gauge reads.
/// </summary>
internal sealed class RepoContextIngestRepository
{
    /// <summary>Creates the state for a repository first seen at <paramref name="firstSeenTimestamp"/>.</summary>
    /// <param name="tag">The precomputed <c>repository</c> tag.</param>
    /// <param name="firstSeenTimestamp">The monotonic timestamp its first pass began at.</param>
    public RepoContextIngestRepository(KeyValuePair<string, object?> tag, long firstSeenTimestamp)
    {
        Tag = tag;
        FirstSeenTimestamp = firstSeenTimestamp;
    }

    /// <summary>
    /// The <c>repository</c> tag, built once so each emission reuses it rather than
    /// re-boxing the pair on every add.
    /// </summary>
    public KeyValuePair<string, object?> Tag { get; }

    /// <summary>The monotonic timestamp the repository's first pass on this silo began at.</summary>
    public long FirstSeenTimestamp { get; }

    /// <summary>
    /// The monotonic timestamp its newest pass completed at, or <see langword="null"/>
    /// when none has. Read and written under the reporter's gate.
    /// </summary>
    public long? LastCompletedTimestamp { get; set; }
}
