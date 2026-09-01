using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The opt-in, hub-only source strategy: fetch a configured git ref into a staging
/// work tree and index the commit it resolves to, so the configuration declares the
/// truth (remote plus ref) and the resolved commit SHA anchors every generation
/// built from it.
/// <para>
/// Every failure path is closed. A repository with no resolvable credential, an
/// unreachable remote, a ref that does not resolve, a fetch that overruns its
/// timeout, or a role that may not fetch all yield
/// <see cref="RepoContextSourceOutcome.Failed"/>: nothing is indexed, nothing is
/// pruned, the last-good index keeps serving, and no other source is attempted.
/// </para>
/// </summary>
internal sealed class GitRemoteSource(
    RepoContextGitSourceRegistry registry,
    IRepoContextGitCredentialProvider credentials,
    IRepoContextGitFetcher fetcher,
    RepoContextIndexingOptions indexingOptions,
    ILogger<GitRemoteSource> logger) : IRepoContextIndexSource
{
    // One in-flight fetch per repository. The transport is synchronous and a hung
    // remote can outlive its timeout, so a later reminder must not stack a second
    // fetch onto the same staging tree - it stands down instead.
    private readonly ConcurrentDictionary<string, byte> _inFlight = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public RepoContextSourceKind Kind => RepoContextSourceKind.GitRemote;

    /// <summary>
    /// The staging work tree for <paramref name="repoId"/> under
    /// <paramref name="stagingRoot"/>. The directory name is a sanitised repository
    /// id suffixed with a short content digest of the raw id, so two ids that
    /// sanitise to the same characters still get distinct trees.
    /// </summary>
    /// <param name="stagingRoot">The staging root. Must not be <see langword="null"/>.</param>
    /// <param name="repoId">The repository identity. Must not be <see langword="null"/>.</param>
    /// <returns>The absolute staging work tree path.</returns>
    internal static string WorkTreePath(string stagingRoot, string repoId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stagingRoot);
        ArgumentException.ThrowIfNullOrWhiteSpace(repoId);

        var sanitised = new char[repoId.Length];
        for (var i = 0; i < repoId.Length; i++)
        {
            var c = repoId[i];
            sanitised[i] = char.IsAsciiLetterOrDigit(c) || c is '-' or '_' ? c : '_';
        }

        // "xx128:" prefix stripped, then eight hex characters: enough to separate
        // ids that sanitise identically without producing an unreadable directory.
        var digest = FileDigest.Compute(Encoding.UTF8.GetBytes(repoId));
        var suffix = digest[(digest.IndexOf(':') + 1)..][..8];
        return Path.Combine(stagingRoot, new string(sanitised) + "-" + suffix);
    }

    /// <inheritdoc />
    public async ValueTask<RepoContextSourcePreparation> PrepareAsync(
        RepoIndexJobRequest request,
        string? lastIndexedCommitSha,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        // The narrowest seam that performs the outbound fetch is also where the
        // hub-only rule is enforced: a spoke never reaches out to a git host even if
        // a misconfiguration routed it here.
        if (indexingOptions.Role != RepoContextIndexingRole.Hub)
        {
            return Fail(request.RepoId, "the indexing role is not Hub, so no git fetch is performed");
        }

        var source = registry.Find(request.RepoId);
        if (source is null)
        {
            return Fail(request.RepoId, "no git source is configured for the repository");
        }

        if (string.IsNullOrWhiteSpace(source.RemoteUrl))
        {
            return Fail(request.RepoId, "the git remote url is not configured");
        }

        var credential = await credentials.ResolveAsync(source, cancellationToken).ConfigureAwait(false);
        if (credential is null)
        {
            return Fail(request.RepoId, "no credential is available for the configured git remote");
        }

        if (!_inFlight.TryAdd(request.RepoId, 0))
        {
            return Fail(request.RepoId, "a previous fetch for the repository is still running");
        }

        // Ownership of the in-flight flag transfers to the orphaned fetch when one
        // overruns its timeout: releasing it here would let the very next refresh
        // stack a second fetch onto a staging tree the first one is still writing.
        var orphaned = false;
        try
        {
            var workTree = WorkTreePath(registry.StagingRoot, source.RepoId);
            var fetchRequest = new RepoContextGitFetchRequest
            {
                Source = source,
                Credential = credential,
                WorkTreePath = workTree,
                LastIndexedCommitSha = lastIndexedCommitSha,
            };

            var result = await RunFetchAsync(source, fetchRequest, credential, cancellationToken)
                .ConfigureAwait(false);
            if (result is null)
            {
                orphaned = true;
                return Fail(request.RepoId, "the git fetch did not complete within its configured timeout");
            }

            if (!result.CheckedOut
                && string.Equals(result.CommitSha, lastIndexedCommitSha, StringComparison.OrdinalIgnoreCase))
            {
                logger.LogDebug(
                    "Repo {RepoId}: git source is already at commit {CommitSha}; refresh is a no-op.",
                    source.RepoId, result.CommitSha);
                return RepoContextSourcePreparation.UpToDate(RepoContextSourceKind.GitRemote, result.CommitSha);
            }

            logger.LogInformation(
                "Repo {RepoId}: git source staged commit {CommitSha} from ref {Reference} "
                + "(previous {PreviousCommitSha}; diff {DiffState} +{Added} ~{Modified} -{Deleted}).",
                source.RepoId, result.CommitSha, source.Reference, result.PreviousCommitSha ?? "none",
                result.DiffAvailable ? "available" : "unavailable",
                result.Added, result.Modified, result.Deleted);

            return RepoContextSourcePreparation.Proceed(
                RepoContextSourceKind.GitRemote,
                Rewrite(request, source, workTree, result.CommitSha),
                result.CommitSha);
        }
        catch (RepoContextGitSourceException ex)
        {
            return Fail(request.RepoId, "the git fetch failed: " + ex.Message);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return Fail(
                request.RepoId,
                "the git fetch failed: " + RepoContextSecretRedactor.Redact(ex.Message, credential));
        }
        finally
        {
            if (!orphaned)
            {
                _inFlight.TryRemove(request.RepoId, out _);
            }
        }
    }

    /// <summary>
    /// Rewrites the job request to index the staged work tree at the resolved commit,
    /// with the git source's own filters. The commit rides on the request so every
    /// artefact the run produces - the scan input, the reconcile plan, and the
    /// repository node it stamps - is anchored to the same revision.
    /// <para>
    /// Directory-modification-time pruning and gitignore handling are both switched
    /// off: the commit tree already contains exactly the tracked files at the resolved
    /// commit, so both heuristics would only add imprecision.
    /// </para>
    /// </summary>
    private static RepoIndexJobRequest Rewrite(
        RepoIndexJobRequest request, RepoContextGitSourceOptions source, string workTree, string commitSha) =>
        request with
        {
            RepoRoot = workTree,
            CommitSha = commitSha,
            IncludeGlobs = source.IncludeGlobs,
            ExcludeGlobs = source.ExcludeGlobs,
            RespectGitignore = false,
            ExcludeBinary = source.ExcludeBinary,
            AllowPrune = false,
        };

    /// <summary>
    /// Runs the synchronous transport off the calling turn and bounds it with the
    /// configured timeout. A timed-out fetch returns <see langword="null"/>; its task
    /// is left to drain in the background with its exception observed, and it holds
    /// the per-repository in-flight flag until it drains so the next refresh stands
    /// down instead of stacking a second fetch onto the same staging tree.
    /// </summary>
    private async Task<RepoContextGitFetchResult?> RunFetchAsync(
        RepoContextGitSourceOptions source,
        RepoContextGitFetchRequest fetchRequest,
        RepoContextGitCredential credential,
        CancellationToken cancellationToken)
    {
        var fetch = Task.Run(() => fetcher.Fetch(fetchRequest, cancellationToken), cancellationToken);
        var completed = await Task.WhenAny(fetch, Task.Delay(source.FetchTimeout, cancellationToken))
            .ConfigureAwait(false);

        if (!ReferenceEquals(completed, fetch))
        {
            var repoId = source.RepoId;
            _ = fetch.ContinueWith(
                t =>
                {
                    _inFlight.TryRemove(repoId, out _);
                    logger.LogWarning(
                        "Repo {RepoId}: an overrunning git fetch finished after its timeout ({Outcome}).",
                        repoId,
                        t.IsFaulted
                            ? RepoContextSecretRedactor.Redact(t.Exception?.GetBaseException().Message, credential)
                            : "completed");
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return null;
        }

        return await fetch.ConfigureAwait(false);
    }

    private RepoContextSourcePreparation Fail(string repoId, string reason)
    {
        logger.LogWarning(
            "Repo {RepoId}: git source stood down - {Reason}. The last-good index keeps serving.",
            repoId, reason);
        return RepoContextSourcePreparation.Failed(RepoContextSourceKind.GitRemote, reason);
    }
}
