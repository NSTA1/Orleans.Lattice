namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The seam behind the reminder-driven self-index grain that decides where a
/// repository's content comes from before a generation is built. Implementations
/// stage content (or confirm the staged content is already current) and hand back a
/// job request pointing at it.
/// <para>
/// An implementation must be fail-closed: when it cannot stage content it returns
/// <see cref="RepoContextSourceOutcome.Failed"/> rather than throwing or degrading
/// to a different source, so a partial or unauthorised fetch leaves the last-good
/// index serving untouched.
/// </para>
/// </summary>
internal interface IRepoContextIndexSource
{
    /// <summary>The strategy this source implements.</summary>
    RepoContextSourceKind Kind { get; }

    /// <summary>
    /// Stages the content for the next generation of <paramref name="request"/>'s
    /// repository and reports what the caller should do next.
    /// </summary>
    /// <param name="request">The job request as configured for the repository. Must
    /// not be <see langword="null"/>.</param>
    /// <param name="lastIndexedCommitSha">The commit SHA stamped on the last
    /// successfully completed generation, or <see langword="null"/> when the
    /// repository has never completed one.</param>
    /// <param name="cancellationToken">Cancels the preparation.</param>
    /// <returns>The preparation outcome; never <see langword="null"/>.</returns>
    ValueTask<RepoContextSourcePreparation> PrepareAsync(
        RepoIndexJobRequest request,
        string? lastIndexedCommitSha,
        CancellationToken cancellationToken);
}
