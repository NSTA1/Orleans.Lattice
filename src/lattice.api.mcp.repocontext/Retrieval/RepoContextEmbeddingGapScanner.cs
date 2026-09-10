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

        if (!covered.AbsenceIsConclusive)
        {
            // The store's read-path access gate removed keys from the probe, so a
            // file's absence from the covered set is not evidence that its embedding
            // is missing (issue #2277). Reporting a gap on that basis would re-drive
            // the WHOLE repository index on every sweep, forever, healing nothing -
            // the sweep's own re-drive is what makes a false negative here so much
            // more expensive than a missed page. Report no gap and end the walk, and
            // flag the page so the caller says why rather than recording a clean
            // sweep this scan did not earn.
            return new GapScanPage(GapFound: false, HasMore: false, NextResumeKey: null)
            {
                CoverageUnavailable = true,
                PrunedByAccessGate = covered.PrunedByAccessGate,
            };
        }

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

    /// <summary>
    /// Re-derives the coverage digest from an authoritative whole-set membership
    /// scan. This is the exhaustive O(sources) read that
    /// <see cref="ScanWithDigestAsync"/> replaced on the detection path, retained as
    /// a slow-cadence correctness backstop that bounds how far the digest can drift
    /// from the membership tree it mirrors.
    /// </summary>
    /// <param name="repoId">The repository to audit. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the audit.</param>
    /// <returns><see langword="true"/> when the digest was re-derived.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public Task<bool> AuditCoverageDigestAsync(string repoId, CancellationToken cancellationToken)
        => _writer.AuditCoverageDigestAsync(repoId, cancellationToken);

    /// <summary>
    /// Detects coverage gaps across the <b>whole</b> repository from the per-page
    /// coverage digest, and returns the identity of every uncovered file rather than
    /// only the fact that one exists (issue #2486).
    /// <para>
    /// This replaces two costs at once. Detection no longer point-probes membership
    /// per source, so it reads a fixed
    /// <see cref="RepoContextCoveragePage.PageCount"/> digest rows whatever the
    /// corpus size, and it touches the membership tree - the write-ahead-log
    /// replay-debt hotspot of issue #2071 - not at all. And repair is no longer a
    /// whole-repository re-ingest: the returned keys <b>are</b> the repair queue, so
    /// one missing vector costs one embed.
    /// </para>
    /// <para>
    /// The repair queue is derived from the digest on every pass rather than
    /// persisted as a durable work list. A persisted queue would be a third thing to
    /// keep consistent with membership and the digest, and would drift silently when
    /// an entry was enqueued and then covered by an ordinary reconcile; a derived set
    /// cannot drift, because it is recomputed from the two sources of truth each time
    /// and is empty exactly when nothing is missing.
    /// </para>
    /// <para>
    /// When no digest is available - not yet built, unreadable, or pruned by the
    /// read-path access gate - the scan reports
    /// <see cref="CoverageDigestScan.DigestAvailable"/> false and asserts nothing, so
    /// the caller keeps its existing probe-based behaviour instead of treating an
    /// absent digest as a repository-wide gap.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository to scan. Must not be <see langword="null"/>.</param>
    /// <param name="maxMissing">The maximum number of missing file keys to return. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxMissing"/> is not positive.</exception>
    public async Task<CoverageDigestScan> ScanWithDigestAsync(
        string repoId, int maxMissing, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMissing);

        var digest = await _writer.LoadCoverageDigestAsync(repoId, cancellationToken).ConfigureAwait(false);
        if (!digest.IsBuilt)
        {
            return new CoverageDigestScan(false, [], 0, 0, false);
        }

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var filesPrefix = RepoContextKeys.FilesPrefix(repoId);
        var end = RepoContextPortability.PrefixUpperBound(filesPrefix);

        var missing = new List<string>();
        var considered = 0;
        var truncated = false;
        await foreach (var key in tree
            .ScanKeysAsync(filesPrefix, end, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            considered++;
            if (digest.IsCovered(VectorCodec.SourceId(key)))
            {
                continue;
            }

            if (missing.Count == maxMissing)
            {
                truncated = true;
                break;
            }

            missing.Add(key);
        }

        return new CoverageDigestScan(true, missing, considered, digest.PagesRead, truncated);
    }
}

/// <summary>
/// The outcome of a whole-repository coverage scan served from the per-page
/// coverage digest: which files are uncovered, how the scan was paid for, and
/// whether the digest could be used at all.
/// </summary>
/// <param name="DigestAvailable">
/// Whether a usable digest answered the scan. When false, every other field is
/// meaningless and the caller must fall back to the membership probe rather than
/// read an empty <paramref name="MissingFileKeys"/> as "no gap" or a full structural
/// range as "all missing".
/// </param>
/// <param name="MissingFileKeys">The structural file keys with no coverage - the targeted repair queue, in key order.</param>
/// <param name="FilesConsidered">How many structural file keys the scan classified.</param>
/// <param name="DigestRowsRead">
/// How many digest rows the scan read. This is the detection cost the item exists to
/// bound, reported so a caller (and a test) can compare it against
/// <paramref name="FilesConsidered"/> at two corpus sizes and see that it does not
/// move with the corpus.
/// </param>
/// <param name="Truncated">Whether the missing set was capped, so more uncovered files remain for the next pass.</param>
internal readonly record struct CoverageDigestScan(
    bool DigestAvailable,
    IReadOnlyList<string> MissingFileKeys,
    int FilesConsidered,
    int DigestRowsRead,
    bool Truncated)
{
    /// <summary>Whether the scan found at least one uncovered file.</summary>
    public bool GapFound => DigestAvailable && MissingFileKeys.Count > 0;
}

/// <summary>
/// The outcome of scanning one page of a repository's structural file keys for a
/// missing embedding.
/// </summary>
/// <param name="GapFound">Whether the page contained a file with no live embedding.</param>
/// <param name="HasMore">Whether more files remain to scan after this page.</param>
/// <param name="NextResumeKey">The inclusive key to resume the next page from, or <see langword="null"/> when the walk is complete or a gap ended it.</param>
internal readonly record struct GapScanPage(bool GapFound, bool HasMore, string? NextResumeKey)
{
    /// <summary>
    /// Whether this page could not be classified because the store's read-path
    /// access gate pruned keys from the coverage probe (issue #2277), so
    /// <see cref="GapFound"/> being <see langword="false"/> means "not measured"
    /// rather than "clean".
    /// <para>
    /// Reported separately rather than by returning a gap, because the caller's
    /// response to a gap is to re-drive the entire repository index: a false gap
    /// here costs a full re-index on every sweep and heals nothing, which is a
    /// strictly worse failure than declining to classify one page.
    /// </para>
    /// </summary>
    public bool CoverageUnavailable { get; init; }

    /// <summary>
    /// How many of the page's probed keys the access gate removed. Zero unless
    /// <see cref="CoverageUnavailable"/> is <see langword="true"/>.
    /// </summary>
    public int PrunedByAccessGate { get; init; }
}
