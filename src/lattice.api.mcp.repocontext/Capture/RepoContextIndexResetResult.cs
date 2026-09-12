namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of <c>repocontext_reset_index</c>: the repository whose code index
/// was dropped and how many entries were tombstoned across the code-index trees
/// (structural, symbol, content, cross-reference, session, and every vector
/// tree). The repository root marker is preserved - rewritten with its
/// index-derived fields cleared so the repository stays listed by
/// <c>repocontext_list_repos</c> - and so is never counted here, and neither is
/// any record in the agent-memory tree. A count of zero means no code index was
/// present.
/// </summary>
/// <remarks>
/// An MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextIndexResetResult
{
    /// <summary>The repository identity whose code index was reset.</summary>
    public required string RepoId { get; init; }

    /// <summary>
    /// The total number of entries tombstoned across the code-index trees. The
    /// repository root marker is preserved rather than deleted, so it is not
    /// counted; records in the memory tree are preserved and are never counted
    /// here either.
    /// </summary>
    public required int EntriesDeleted { get; init; }

    /// <summary>
    /// The wall-clock duration of the reset in milliseconds.
    /// <para>
    /// This exists because the reset's own completion had no report at all: a
    /// caller could observe that it was invoked and never that it finished, so
    /// the only way to learn the outcome was to run a later, unrelated
    /// onboarding and infer it from the shape of that job's counters
    /// (<c>filesAdded == filesScanned</c> with <c>filesUnchanged: 0</c> implies
    /// the store was empty, so the reset landed). Inferring a destructive
    /// operation's success from a different operation's telemetry is not a
    /// confirmation channel. A returned duration makes a slow reset observably
    /// slow rather than indistinguishable from one that hung.
    /// </para>
    /// </summary>
    public required long ElapsedMilliseconds { get; init; }

    /// <summary>
    /// The code-index trees this reset swept, in the order they were swept. Named
    /// rather than counted so the caller can see exactly what was dropped instead
    /// of trusting that the sweep covered what it should have.
    /// </summary>
    public required IReadOnlyList<string> TreesSwept { get; init; }

    /// <summary>
    /// Always <see langword="true"/>: the durable agent-memory tree is preserved
    /// by design and is never swept. It is reported explicitly because confirming
    /// what survived matters as much as confirming what was dropped - a caller
    /// running a repair on a repository whose accumulated decisions and gotchas
    /// are the reason it is being repaired rather than removed needs that stated,
    /// not assumed from documentation.
    /// </summary>
    public required bool MemoryPreserved { get; init; }

    /// <summary>
    /// Whether the repository root marker's index-derived registers
    /// (<c>lastIngested</c>, <c>fileCount</c>, <c>indexedCommit</c>) were cleared,
    /// so <c>repocontext_list_repos</c> reports the documented post-reset
    /// signature of three nulls. <see langword="false"/> only when there was no
    /// marker to clear, which for a repository that had an index means it was
    /// re-derived after the sweep instead.
    /// </summary>
    public required bool CensusCleared { get; init; }
}
