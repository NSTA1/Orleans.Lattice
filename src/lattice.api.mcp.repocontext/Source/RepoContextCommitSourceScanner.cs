using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Supplies a bootstrap run's scan set from the resolved commit's tree when the run
/// is git-sourced (its request carries a commit SHA), and defers to the filesystem
/// walk otherwise.
/// <para>
/// This is what makes deletion exact for a git-sourced repository. The scan set is
/// the commit's tracked files, so the plan's "stored but not scanned" set is exactly
/// the files deleted between the last indexed commit and this one - never a file
/// that merely happened to be missing from a mount at scan time.
/// </para>
/// </summary>
internal sealed class RepoContextCommitSourceScanner(
    IRepoContextGitFetcher fetcher,
    RepoContextGitSourceRegistry registry,
    ILogger<RepoContextCommitSourceScanner> logger) : IRepoContextSourceScanner
{
    /// <inheritdoc />
    public IReadOnlyList<RepoFileEntry>? TryScan(
        RepoContextBootstrapRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (string.IsNullOrWhiteSpace(request.CommitSha))
        {
            return null;
        }

        var source = registry.Find(request.RepoId);
        var entries = fetcher.ScanCommit(
            request.RepoRoot,
            request.CommitSha,
            source?.IncludeGlobs ?? request.IncludeGlobs,
            source?.ExcludeGlobs ?? request.ExcludeGlobs,
            source?.ExcludeBinary ?? request.ExcludeBinary,
            cancellationToken);

        logger.LogInformation(
            "Repo {RepoId}: scanned {FileCount} file(s) from commit {CommitSha}.",
            request.RepoId, entries.Count, request.CommitSha);

        return entries;
    }
}
