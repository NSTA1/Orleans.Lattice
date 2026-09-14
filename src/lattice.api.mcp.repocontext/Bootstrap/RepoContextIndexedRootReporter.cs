using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Reports the resolved workspace root and every registered repository's indexed root
/// once the host is up, so that what the box is actually about is readable from the log
/// rather than only derivable by asking a tool that happens to refuse a path.
/// <para>
/// A repository's id defaults to the final segment of the path it was registered from,
/// which makes the id look like it names the tree. It does not. A stack composed from a
/// git worktree with the default workspace root mounts the worktree COLLECTION, so the
/// registerable child is the worktree's generated name - and a repository registered
/// under the base repository's id then indexes the worktree while every observable
/// health signal stays green: the ingest marker is current, the file count is plausible,
/// and search returns well-ranked real hits. The index is genuinely healthy and simply
/// about a different tree. See issue #2617.
/// </para>
/// <para>
/// This is observability only: it reads and logs, it changes no behaviour, and it never
/// fails startup. A root this reporter would flag is still a root the host is entitled
/// to serve, and refusing to start would convert a reporting gap into an outage.
/// </para>
/// </summary>
/// <param name="store">The context store the repository listing is read from.</param>
/// <param name="workspaceGuard">The guard holding the canonicalised workspace roots.</param>
/// <param name="logger">The log sink for the startup report.</param>
internal sealed class RepoContextIndexedRootReporter(
    RepoContextStore store,
    RepoContextWorkspaceGuard workspaceGuard,
    ILogger<RepoContextIndexedRootReporter> logger) : BackgroundService
{
    /// <summary>
    /// Reports whether a repository's id disagrees with the final segment of the root it
    /// was indexed from - the exact shape of the defect in issue #2617, where the id read
    /// <c>lattice</c> and the root read <c>/workspace/bucket4-merge</c>.
    /// <para>
    /// It is a MISMATCH SIGNAL, not a verdict, and the distinction is load-bearing:
    /// <c>repocontext_add_repo</c> accepts an explicit id, so a deliberate registration
    /// may legitimately disagree with its folder name. Treating that as an error would
    /// make the check wrong on a supported configuration and train an operator to ignore
    /// it, which costs more than the check returns. It reports <see langword="false"/>
    /// for an absent root, because "never indexed" is a different state with its own
    /// reporting and is not evidence of a mismatch.
    /// </para>
    /// </summary>
    /// <param name="repoId">The id the repository is registered under.</param>
    /// <param name="indexedRoot">The resolved root it was indexed from, or null.</param>
    /// <returns><see langword="true"/> when both are present and disagree.</returns>
    internal static bool IsIdRootMismatch(string repoId, string? indexedRoot)
    {
        if (string.IsNullOrWhiteSpace(repoId) || string.IsNullOrWhiteSpace(indexedRoot))
        {
            return false;
        }

        var segment = indexedRoot
            .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)[^1];

        if (segment.Length == 0)
        {
            return false;
        }

        var comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;

        return !segment.Equals(repoId, comparison);
    }

    /// <summary>
    /// Logs the resolved workspace roots and each registered repository's indexed root,
    /// flagging any repository whose id disagrees with its root's final segment.
    /// </summary>
    /// <param name="stoppingToken">Cancels the report when the host is shutting down.</param>
    /// <returns>A task that completes when the report has been written.</returns>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var roots = workspaceGuard.AllowedRoots;
            logger.LogInformation(
                "Repository-context workspace boundary: {RootCount} resolved root(s) [{Roots}]; "
                + "enforcing: {IsEnforcing}. These are the CANONICALISED mounts (symlinks and "
                + "'..' segments already resolved), so they are what the host is really serving "
                + "rather than what it was configured with.",
                roots.Count,
                string.Join(", ", roots),
                workspaceGuard.IsEnforcing);

            var listing = await store.ListReposAsync(stoppingToken).ConfigureAwait(false);

            if (listing.Repos.Count == 0)
            {
                logger.LogInformation(
                    "Repository-context indexed roots: no repository is registered yet. A repository "
                    + "still in its FIRST ingest is absent from this listing until its structural "
                    + "records materialise, so this is not evidence that onboarding failed.");
                return;
            }

            foreach (var repo in listing.Repos)
            {
                logger.LogInformation(
                    "Repository-context indexed root: id '{RepoId}' is indexed from '{IndexedRoot}'.",
                    repo.RepoId,
                    repo.IndexedRoot ?? "(none - never indexed, or its index was reset)");

                if (IsIdRootMismatch(repo.RepoId, repo.IndexedRoot))
                {
                    // Deliberately a warning rather than a failure. An explicit id supplied to
                    // repocontext_add_repo legitimately disagrees with the folder name, so this
                    // cannot be an error without being wrong on a supported configuration. The
                    // line carries both values because the entire failure mode is that neither
                    // was visible anywhere a caller reads first.
                    logger.LogWarning(
                        "Repository-context id/root MISMATCH: repository '{RepoId}' is indexed from "
                        + "'{IndexedRoot}', whose final path segment is not '{RepoId}'. Every record "
                        + "served under this id describes THAT directory. This is expected when the id "
                        + "was supplied explicitly to 'repocontext_add_repo'; it is the signature of a "
                        + "misconfigured workspace root otherwise - notably a stack composed from a git "
                        + "worktree with the default REPO_PATH, which mounts the worktree collection and "
                        + "registers the worktree under the base repository's id. Confirm the root is "
                        + "the tree you mean: no other health signal distinguishes the two states. "
                        + "See issue #2617.",
                        repo.RepoId,
                        repo.IndexedRoot,
                        repo.RepoId);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown during the report. Nothing to say and nothing wrong.
        }
#pragma warning disable CA1031 // Observability must never take the host down.
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Repository-context indexed-root report could not be produced. The host is unaffected: "
                + "this reporter only reads and logs. Query 'repocontext_list_repos' for the same "
                + "information, which now carries each repository's indexed root.");
        }
#pragma warning restore CA1031
    }
}
