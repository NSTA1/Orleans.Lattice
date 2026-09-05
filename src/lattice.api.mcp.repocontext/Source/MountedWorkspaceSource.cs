namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default source strategy and the one every repository uses unless a git
/// source is explicitly configured for it: the content is whatever is mounted under
/// the fail-closed workspace boundary at the request's repository root, so there is
/// nothing to stage and nothing to fetch.
/// <para>
/// Preparation is therefore unconditionally
/// <see cref="RepoContextSourceOutcome.Proceed"/> with the request unchanged and no
/// commit anchor, which is exactly the behaviour that existed before the source
/// seam was introduced.
/// </para>
/// </summary>
internal sealed class MountedWorkspaceSource : IRepoContextIndexSource
{
    /// <inheritdoc />
    public RepoContextSourceKind Kind => RepoContextSourceKind.MountedWorkspace;

    /// <inheritdoc />
    public ValueTask<RepoContextSourcePreparation> PrepareAsync(
        RepoIndexJobRequest request,
        string? lastIndexedCommitSha,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        // The mounted tree is the content: there is no staging step, no revision to
        // compare, and no commit anchor to stamp. The walk itself decides what
        // changed.
        return ValueTask.FromResult(
            RepoContextSourcePreparation.Proceed(RepoContextSourceKind.MountedWorkspace, request, commitSha: null));
    }
}
