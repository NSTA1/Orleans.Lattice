namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The read-only, allocation-frugal probe the always-on self-heal sweep uses to
/// decide whether a repository has a structural file whose embedding never landed.
/// <para>
/// Presence is judged from the add-wins membership set, which holds only the
/// 16-character source identifiers and never the embeddings themselves. The
/// scanner loads that set once per repository (a single read that never transfers
/// a vector payload) and then walks the structural file range one bounded page at
/// a time with <see cref="LatticeExtensions.ScanKeysAsync"/> - keys only, no file
/// node or embedding crosses the grain boundary. Each file key maps to its source
/// identifier by the same <see cref="VectorCodec.SourceId(string)"/> used at embed
/// time, probed in memory against the loaded set with one reused buffer. The walk
/// stops at the first file whose identifier is absent, so a repository with a gap
/// is detected without reading the rest of its files.
/// </para>
/// </summary>
internal sealed class RepoContextEmbeddingGapScanner
{
    private readonly IGrainFactory _grainFactory;
    private readonly RepoContextVectorWriter _writer;

    /// <summary>Creates the embedding gap scanner.</summary>
    /// <param name="grainFactory">The grain factory used to reach the structural tree. Must not be <see langword="null"/>.</param>
    /// <param name="writer">The vector writer used to load the membership set. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Any argument is null.</exception>
    public RepoContextEmbeddingGapScanner(IGrainFactory grainFactory, RepoContextVectorWriter writer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(writer);
        _grainFactory = grainFactory;
        _writer = writer;
    }

    /// <summary>
    /// Loads the repository's live embedded-source membership set once, so a whole
    /// sweep of file pages can probe presence in memory against it without a read
    /// per file. The set carries only source identifiers, never embeddings.
    /// </summary>
    /// <param name="repoId">The repository whose membership to load. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The live membership set of embedded source identifiers.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public Task<OrSet> LoadEmbeddedAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _writer.LoadEmbeddedMembersAsync(repoId, cancellationToken);
    }

    /// <summary>
    /// Scans one bounded page of the repository's structural file keys and reports
    /// whether any file in that page has no live embedding. The scan is keys-only
    /// and resumable: a caller checkpoints <see cref="GapScanPage.NextResumeKey"/>
    /// and passes it back as <paramref name="resumeKeyInclusive"/> to continue the
    /// walk where it left off. The walk stops at the first missing file so a gap is
    /// found without reading the rest of the page.
    /// </summary>
    /// <param name="repoId">The repository to scan. Must not be <see langword="null"/>.</param>
    /// <param name="embedded">The membership set loaded by <see cref="LoadEmbeddedAsync"/>. Must not be <see langword="null"/>.</param>
    /// <param name="resumeKeyInclusive">The inclusive key to resume from, or <see langword="null"/> to start at the first file.</param>
    /// <param name="pageSize">The maximum number of file keys to inspect in this page. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>Whether a gap was found, whether more files remain, and the resume key for the next page.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> or <paramref name="embedded"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    public async Task<GapScanPage> ScanFilePageAsync(
        string repoId,
        OrSet embedded,
        string? resumeKeyInclusive,
        int pageSize,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(embedded);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        var filesPrefix = RepoContextKeys.FilesPrefix(repoId);
        var start = resumeKeyInclusive ?? filesPrefix;
        var end = RepoContextPortability.PrefixUpperBound(filesPrefix);

        var probe = new byte[VectorCodec.SourceIdByteLength];
        var inspected = 0;
        string? lastKey = null;

        await foreach (var key in tree
            .ScanKeysAsync(start, end, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            lastKey = key;
            inspected++;

            var sourceId = VectorCodec.SourceId(key);
            System.Text.Encoding.UTF8.GetBytes(sourceId, probe);
            if (!embedded.Contains(probe))
            {
                // First file with no live embedding: the repository has a gap. The
                // caller re-drives the whole index, so there is no need to keep
                // scanning this repository or to hand back a mid-repository resume
                // point.
                return new GapScanPage(GapFound: true, HasMore: false, NextResumeKey: null);
            }

            if (inspected >= pageSize)
            {
                break;
            }
        }

        // No gap in this page. If the page filled, more files may remain, so hand
        // back the successor of the last key as the next resume point; otherwise
        // the repository's file range is exhausted.
        var hasMore = inspected >= pageSize && lastKey is not null;
        var nextResumeKey = hasMore ? lastKey + "\u0000" : null;
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
