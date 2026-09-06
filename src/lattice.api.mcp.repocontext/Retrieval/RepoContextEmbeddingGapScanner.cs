namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The read-only, allocation-frugal probe the always-on self-heal sweep uses to
/// decide whether a repository has a structural file whose embedding never landed.
/// <para>
/// The scanner walks the structural file range one bounded page at a time with
/// <see cref="LatticeExtensions.ScanKeysAsync"/> - keys only, no file node or
/// embedding crosses the grain boundary - and probes coverage for exactly that
/// page's file keys with a single bounded point-read
/// (<see cref="RepoContextVectorWriter.ProbeCoveredSourceIdsAsync"/>). Membership
/// is never scanned as a whole set, so a churn-bloated membership tree can never
/// force an unbounded sorted-range scan past the response deadline (issue #1556);
/// the sweep's cost is a function of the page size, not the tree size. Each file
/// key maps to its source identifier by the same
/// <see cref="VectorCodec.SourceId(string)"/> used at embed time, and the page is
/// checked in key order so the first file whose identifier is absent from the
/// probed covered set is reported as a gap.
/// </para>
/// </summary>
internal sealed class RepoContextEmbeddingGapScanner
{
    private readonly IGrainFactory _grainFactory;
    private readonly RepoContextVectorWriter _writer;

    /// <summary>Creates the embedding gap scanner.</summary>
    /// <param name="grainFactory">The grain factory used to reach the structural tree. Must not be <see langword="null"/>.</param>
    /// <param name="writer">The vector writer used to point-probe membership coverage. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextEmbeddingGapScanner(IGrainFactory grainFactory, RepoContextVectorWriter writer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(writer);
        _grainFactory = grainFactory;
        _writer = writer;
    }

    /// <summary>
    /// Scans one bounded page of the repository's structural file keys and reports
    /// whether any file in that page has no live embedding. The scan is keys-only
    /// and resumable: a caller checkpoints <see cref="GapScanPage.NextResumeKey"/>
    /// and passes it back as <paramref name="resumeKeyInclusive"/> to continue the
    /// walk where it left off. Coverage for the page's files is resolved with a
    /// single bounded point-probe, so no read in the sweep scales with the
    /// membership tree size. The page is checked in key order and the first missing
    /// file is reported as a gap.
    /// </summary>
    /// <param name="repoId">The repository to scan. Must not be <see langword="null"/>.</param>
    /// <param name="resumeKeyInclusive">The inclusive key to resume from, or <see langword="null"/> to start at the first file.</param>
    /// <param name="pageSize">The maximum number of file keys to inspect in this page. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>Whether a gap was found, whether more files remain, and the resume key for the next page.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    public async Task<GapScanPage> ScanFilePageAsync(
        string repoId,
        string? resumeKeyInclusive,
        int pageSize,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var filesPrefix = RepoContextKeys.FilesPrefix(repoId);
        var start = resumeKeyInclusive ?? filesPrefix;
        var end = RepoContextPortability.PrefixUpperBound(filesPrefix);

        // Collect this page's file keys (bounded by pageSize), then resolve coverage
        // for exactly those keys with one bounded point-probe, so the sweep never
        // holds an unbounded membership scan open (issue #1556).
        //
        // One entry beyond the page bound is probed so has-more is derived from the
        // range itself rather than from the page having filled. Those are not the
        // same thing: a range holding exactly pageSize keys fills the page and is
        // simultaneously exhausted, and inferring more from the fill alone would
        // hand back a resume key into an empty remainder and buy a wasted scan on
        // every sweep. This mirrors RepoContextPortability.EnumerateAsync.
        var pageKeys = new List<string>(pageSize);
        var hasMore = false;
        await foreach (var key in tree
            .ScanKeysAsync(start, end, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (pageKeys.Count == pageSize)
            {
                hasMore = true;
                break;
            }

            pageKeys.Add(key);
        }

        if (pageKeys.Count == 0)
        {
            return new GapScanPage(GapFound: false, HasMore: false, NextResumeKey: null);
        }

        var covered = await _writer
            .ProbeCoveredSourceIdsAsync(repoId, pageKeys, cancellationToken)
            .ConfigureAwait(false);

        foreach (var key in pageKeys)
        {
            if (!covered.Contains(VectorCodec.SourceId(key)))
            {
                // First file with no live embedding: the repository has a gap. The
                // caller re-drives the whole index, so there is no need to keep
                // scanning this repository or to hand back a mid-repository resume
                // point.
                return new GapScanPage(GapFound: true, HasMore: false, NextResumeKey: null);
            }
        }

        // No gap in this page. Has-more was derived from the range while scanning,
        // so a page that filled a now-exhausted range correctly reports completion.
        var nextResumeKey = hasMore ? pageKeys[^1] + "\u0000" : null;
        return new GapScanPage(GapFound: false, HasMore: hasMore, NextResumeKey: nextResumeKey);
    }
}

/// <summary>
/// The outcome of scanning one page of a repository's structural file keys for a
/// missing embedding.
/// </summary>
/// <param name="GapFound">Whether the page contained a file with no live embedding.</param>
/// <param name="HasMore">Whether more files remain to scan after this page.</param>
/// <param name="NextResumeKey">The inclusive key to resume the next page from, or <see langword="null"/> when the walk is complete or a gap ended it.</param>
internal readonly record struct GapScanPage(bool GapFound, bool HasMore, string? NextResumeKey);
