using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The single seam that decides which source strategy owns a repository, and the
/// only place a repository can move between them. Everything upstream - the
/// reminder-driven self-index grain, the <c>add_repo</c> tool - asks this gate
/// rather than testing the configuration itself, so the mount-versus-git mutual
/// exclusion is enforced in exactly one place.
/// <para>
/// A repository declared git-sourced is never silently walked from a mount: when
/// its git preparation fails, the gate propagates the failure rather than falling
/// back, because a fallback would index whatever happened to be on disk under a
/// configuration that says the truth lives in a remote.
/// </para>
/// </summary>
internal sealed class RepoContextIndexSourceGate(
    RepoContextGitSourceRegistry registry,
    MountedWorkspaceSource mountedSource,
    GitRemoteSource gitSource,
    RepoContextIndexingOptions indexingOptions,
    ILogger<RepoContextIndexSourceGate> logger)
{
    /// <summary>Whether any repository at all is git-sourced.</summary>
    public bool HasGitSources => !registry.IsEmpty;

    /// <summary>The staging root git work trees are created under.</summary>
    public string StagingRoot => registry.StagingRoot;

    /// <summary>
    /// Whether <paramref name="repoId"/> is git-sourced, and therefore may not be
    /// registered against a mounted path.
    /// </summary>
    /// <param name="repoId">The repository identity to test. Must not be
    /// <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the repository is git-sourced.</returns>
    public bool IsGitSourced(string repoId) => registry.IsGitSourced(repoId);

    /// <summary>
    /// Which strategy owns <paramref name="repoId"/>.
    /// </summary>
    /// <param name="repoId">The repository identity to classify. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The owning source kind.</returns>
    public RepoContextSourceKind KindFor(string repoId) =>
        registry.IsGitSourced(repoId) ? RepoContextSourceKind.GitRemote : RepoContextSourceKind.MountedWorkspace;

    /// <summary>
    /// The refresh cadence for <paramref name="repoId"/>: the git source's own
    /// interval when it is git-sourced, otherwise <paramref name="fallback"/> (the
    /// shared reconcile interval the mounted path uses).
    /// </summary>
    /// <param name="repoId">The repository identity. Must not be <see langword="null"/>.</param>
    /// <param name="fallback">The interval to use for a mounted repository.</param>
    /// <returns>The interval to schedule the next refresh at.</returns>
    public TimeSpan RefreshIntervalFor(string repoId, TimeSpan fallback) =>
        registry.Find(repoId)?.RefreshInterval ?? fallback;

    /// <summary>
    /// The job request a git-sourced repository indexes with, before the fetch
    /// rewrites its root. Used to onboard a git-sourced repository that was never
    /// registered through <c>add_repo</c>: the mounted root is a placeholder the git
    /// source replaces with its staging work tree.
    /// </summary>
    /// <param name="source">The configured git source. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The seed job request for the repository.</returns>
    public RepoIndexJobRequest SeedRequest(RepoContextGitSourceOptions source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new RepoIndexJobRequest
        {
            RepoId = source.RepoId,
            RepoRoot = GitRemoteSource.WorkTreePath(registry.StagingRoot, source.RepoId),
            IncludeGlobs = source.IncludeGlobs,
            ExcludeGlobs = source.ExcludeGlobs,
            RespectGitignore = false,
            ExcludeBinary = source.ExcludeBinary,
            AllowPrune = false,
        };
    }

    /// <summary>
    /// The seed job request for a git-sourced repository identified by id, or
    /// <see langword="null"/> when the repository is not git-sourced.
    /// </summary>
    /// <param name="repoId">The repository identity. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The seed job request, or <see langword="null"/>.</returns>
    public RepoIndexJobRequest? SeedRequestFor(string repoId)
    {
        var source = registry.Find(repoId);
        return source is null ? null : SeedRequest(source);
    }

    /// <summary>
    /// Every configured git source, so the arming service can onboard each one.
    /// </summary>
    /// <returns>The configured git sources; empty when the feature is not enabled.</returns>
    public IReadOnlyCollection<RepoContextGitSourceOptions> GitSources => registry.Sources;

    /// <summary>
    /// Prepares the content for the next generation of
    /// <paramref name="request"/>'s repository through whichever strategy owns it.
    /// </summary>
    /// <param name="request">The configured job request. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="lastIndexedCommitSha">The commit stamped on the last completed
    /// generation, or <see langword="null"/> when there is none.</param>
    /// <param name="cancellationToken">Cancels the preparation.</param>
    /// <returns>The preparation outcome; never <see langword="null"/>.</returns>
    public ValueTask<RepoContextSourcePreparation> PrepareAsync(
        RepoIndexJobRequest request,
        string? lastIndexedCommitSha,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (!registry.IsGitSourced(request.RepoId))
        {
            return mountedSource.PrepareAsync(request, lastIndexedCommitSha, cancellationToken);
        }

        if (indexingOptions.Role != RepoContextIndexingRole.Hub)
        {
            // A spoke receives the index over the replication plane and never touches
            // source content. Standing down here (rather than walking the staged
            // tree) is what makes a spoke genuinely mount-free.
            logger.LogDebug(
                "Repo {RepoId}: git-sourced repository is inert on a {Role} node.",
                request.RepoId, indexingOptions.Role);
            return ValueTask.FromResult(RepoContextSourcePreparation.Failed(
                RepoContextSourceKind.GitRemote,
                "the indexing role is not Hub, so no git fetch is performed"));
        }

        return gitSource.PrepareAsync(request, lastIndexedCommitSha, cancellationToken);
    }
}
