namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A transport double that resolves a scripted commit without touching a git host,
/// so a test can drive the source seam's control flow - proceed, no-op, fail closed -
/// deterministically and offline.
/// </summary>
internal sealed class StubGitFetcher : IRepoContextGitFetcher
{
    private readonly Queue<RepoContextGitFetchResult> _scripted = new();
    private readonly RepoContextGitFetchResult? _standing;

    /// <summary>
    /// Creates a fetcher that returns <paramref name="standing"/> for every call.
    /// </summary>
    /// <param name="standing">The result every fetch resolves to.</param>
    public StubGitFetcher(RepoContextGitFetchResult standing)
    {
        ArgumentNullException.ThrowIfNull(standing);
        _standing = standing;
    }

    /// <summary>
    /// Creates a fetcher that walks <paramref name="scripted"/> one call at a time and
    /// throws once the script is exhausted, so an unexpected extra fetch is loud.
    /// </summary>
    /// <param name="scripted">The results, in call order.</param>
    public StubGitFetcher(params RepoContextGitFetchResult[] scripted)
    {
        ArgumentNullException.ThrowIfNull(scripted);
        foreach (var result in scripted)
        {
            _scripted.Enqueue(result);
        }
    }

    /// <summary>The number of fetches this transport was asked to perform.</summary>
    public int FetchCount { get; private set; }

    /// <summary>The work tree the most recent fetch was pointed at.</summary>
    public string? LastWorkTreePath { get; private set; }

    /// <summary>The last-indexed commit the most recent fetch was handed.</summary>
    public string? LastIndexedCommitSha { get; private set; }

    /// <summary>A commit result that stages <paramref name="commitSha"/> as a new generation.</summary>
    /// <param name="commitSha">The resolved commit.</param>
    /// <returns>The result.</returns>
    public static RepoContextGitFetchResult Staged(string commitSha) =>
        new() { CommitSha = commitSha, CheckedOut = true };

    /// <summary>A result asserting the ref has not moved off <paramref name="commitSha"/>.</summary>
    /// <param name="commitSha">The resolved commit.</param>
    /// <returns>The result.</returns>
    public static RepoContextGitFetchResult Unchanged(string commitSha) =>
        new() { CommitSha = commitSha, CheckedOut = false };

    /// <inheritdoc />
    public RepoContextGitFetchResult Fetch(RepoContextGitFetchRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        FetchCount++;
        LastWorkTreePath = request.WorkTreePath;
        LastIndexedCommitSha = request.LastIndexedCommitSha;

        if (_standing is not null)
        {
            return _standing;
        }

        return _scripted.Count > 0
            ? _scripted.Dequeue()
            : throw new RepoContextGitSourceException("The scripted transport received an unexpected extra fetch.");
    }

    /// <inheritdoc />
    public IReadOnlyList<RepoFileEntry> ScanCommit(
        string workTreePath,
        string commitSha,
        IReadOnlyList<string>? includeGlobs,
        IReadOnlyList<string>? excludeGlobs,
        bool excludeBinary,
        CancellationToken cancellationToken) => [];
}
