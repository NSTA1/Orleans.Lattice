using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The immutable result of loading a repository's vector-coverage digest: the
/// covered source identifiers reconstructed from a fixed number of digest pages,
/// plus whether the digest is usable at all.
/// </summary>
/// <param name="IsBuilt">
/// Whether the digest has been built for this repository. A digest that has never
/// been built reports no coverage, and a caller must <b>not</b> read that as "every
/// source is missing" - it must fall back to the membership probe and seed the
/// digest from it.
/// </param>
/// <param name="Embedded">The 64-bit source identifiers with a live embedding.</param>
/// <param name="Contentless">The 64-bit source identifiers carrying a contentless marker.</param>
/// <param name="PagesRead">How many digest rows the load actually read - the detection cost, reported so a test can assert it against corpus size.</param>
/// <param name="Presence">
/// Which of the three observable digest states this load found. Distinguishes a
/// digest that is present but consumed by nothing from one that is genuinely
/// absent; see <see cref="RepoContextCoverageDigestPresence"/>.
/// </param>
internal readonly record struct RepoContextCoverageDigest(
    bool IsBuilt,
    IReadOnlySet<ulong> Embedded,
    IReadOnlySet<ulong> Contentless,
    int PagesRead,
    RepoContextCoverageDigestPresence Presence = RepoContextCoverageDigestPresence.Absent)
{
    /// <summary>An unbuilt digest, which asserts nothing about coverage.</summary>
    internal static RepoContextCoverageDigest Unbuilt { get; } =
        new(false, new HashSet<ulong>(), new HashSet<ulong>(), 0);

    /// <summary>
    /// An unbuilt digest whose pages nevertheless exist in the tree, so it is
    /// present and consumed by nothing rather than genuinely absent. Behaves
    /// identically to <see cref="Unbuilt"/> for every consumer - it still asserts no
    /// coverage - and differs only in what it lets an operator be told.
    /// </summary>
    /// <param name="pagesRead">How many rows the presence probe read.</param>
    internal static RepoContextCoverageDigest UnconsumedPagesPresent(int pagesRead)
        => new(
            false,
            new HashSet<ulong>(),
            new HashSet<ulong>(),
            pagesRead,
            RepoContextCoverageDigestPresence.PresentButUnconsumed);

    /// <summary>
    /// Reports whether a source identifier is covered - embedded or marked
    /// contentless - mirroring
    /// <see cref="RepoContextEmbeddingCoverage.IsCovered(string)"/> on the digest's
    /// parsed identifiers.
    /// </summary>
    /// <param name="sourceId">The 16-character source identifier to classify.</param>
    internal bool IsCovered(string sourceId)
        => RepoContextCoveragePage.TryParse(sourceId, out var value)
            && (Embedded.Contains(value) || Contentless.Contains(value));

    /// <summary>
    /// Projects the digest onto a bounded candidate set, producing exactly the
    /// <see cref="RepoContextEmbeddingCoverage"/> a membership probe of those same
    /// candidates would have produced - at no membership reads at all. Restricting
    /// to the candidates keeps the returned coverage the same shape every existing
    /// consumer already handles, so nothing downstream has to learn about the digest.
    /// </summary>
    /// <param name="candidateSourceIds">The 16-character source identifiers under consideration. Must not be <see langword="null"/>.</param>
    internal RepoContextEmbeddingCoverage ProjectOnto(IEnumerable<string> candidateSourceIds)
    {
        ArgumentNullException.ThrowIfNull(candidateSourceIds);

        var embedded = new HashSet<string>(StringComparer.Ordinal);
        var contentless = new HashSet<string>(StringComparer.Ordinal);
        foreach (var sourceId in candidateSourceIds)
        {
            if (!RepoContextCoveragePage.TryParse(sourceId, out var value))
            {
                continue;
            }

            if (Embedded.Contains(value))
            {
                embedded.Add(sourceId);
            }

            if (Contentless.Contains(value))
            {
                contentless.Add(sourceId);
            }
        }

        return new RepoContextEmbeddingCoverage(embedded, contentless);
    }
}

/// <summary>
/// How a repository's coverage digest presents to the consumers that read it.
/// <para>
/// The middle member is the whole point of this enum. A derived accelerator has a
/// reachable state in which it is <b>present, well-formed, and read by nothing</b>:
/// <see cref="RepoContextCoverageDigestStore.RebuildAsync"/> writes the pages first
/// and the built-state marker last, deliberately, so that a crash in that window
/// leaves an under-reporting digest rather than an over-reporting one. The cost of
/// that ordering is a state in which 256 correct, decodable pages sit in the tree
/// and every consumer ignores them, because the marker they gate on is absent.
/// </para>
/// <para>
/// Collapsing that state into either neighbour is what makes it invisible.
/// Reported as <see cref="Consumed"/> it would be a false claim of health; reported
/// as <see cref="Absent"/> - which is what a bare "is the marker there?" check
/// yields - it is indistinguishable from an ordinary first-run bootstrap, so a
/// digest that has silently stopped being read looks exactly like one that has not
/// been built yet. The distinction costs one extra page sweep on a path that is
/// about to do an O(sources) rebuild anyway.
/// </para>
/// </summary>
internal enum RepoContextCoverageDigestPresence
{
    /// <summary>No digest rows and no marker: a genuine first-run bootstrap.</summary>
    Absent = 0,

    /// <summary>
    /// Digest pages exist and decode, but the built-state marker does not, so no
    /// consumer reads them. Every pass silently falls back to the membership probe
    /// while the tree looks healthy to any human inspecting it.
    /// </summary>
    PresentButUnconsumed = 1,

    /// <summary>Built, marked, and actually answering detection.</summary>
    Consumed = 2,
}


/// compacted mirror of the membership tree, partitioned into
/// <see cref="RepoContextCoveragePage.PageCount"/> fixed pages, held in its own
/// <see cref="RepoContextTrees.VectorCoverage"/> tree.
/// <para>
/// <b>The cost model this exists to change.</b> Detecting a coverage gap used to
/// cost two membership point-reads per indexed source, so it was O(sources) and
/// had to be rationed onto a multi-hour cadence; and the membership tree it read is
/// the write-ahead-log replay-debt hotspot behind the symbol re-embed loop (issue
/// #2071), so shortening the detection window made the hotspot worse. Reading this
/// digest costs <see cref="RepoContextCoveragePage.PageCount"/> rows on a different
/// tree, whatever the corpus size, and yields the <b>identity</b> of every missing
/// source rather than only the fact that one exists - which is what turns the
/// remedy from a whole-repository re-ingest into one embed per missing vector.
/// </para>
/// <para>
/// <b>The safety invariant, and the write ordering that enforces it.</b> The digest
/// may under-report coverage but must never over-report it. Under-reporting yields
/// a false gap, which costs one redundant, idempotent embed and self-corrects;
/// over-reporting masks a real gap, which is silent and permanent. So coverage is
/// added to membership <b>first</b> and to the digest second, and removed from the
/// digest <b>first</b> and from membership second. Every interleaving and every
/// crash point therefore leaves the digest a subset of membership, never a superset.
/// </para>
/// </summary>
/// <param name="grainFactory">The grain factory used to reach the digest tree. Must not be <see langword="null"/>.</param>
/// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
internal sealed class RepoContextCoverageDigestStore(
    IGrainFactory grainFactory,
    ILogger<RepoContextCoverageDigestStore> logger)
{
    /// <summary>
    /// How many digest page keys are fetched per multi-get. The digest is a fixed
    /// 256 rows, so this only bounds the per-call fan-out, never the total.
    /// </summary>
    private const int PageReadBatchSize = 64;

    private static readonly byte[] StateMarker = [RepoContextCoveragePage.SchemaVersion];

    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ILogger<RepoContextCoverageDigestStore> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private ILattice Tree => _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorCoverage);

    /// <summary>
    /// Loads a repository's whole digest in a fixed number of row reads. Returns
    /// <see cref="RepoContextCoverageDigest.Unbuilt"/> when no digest has been built
    /// for the repository, and also when the load faults - a derived accelerator
    /// that cannot be read must degrade to the membership probe, never to a claim
    /// that nothing is covered.
    /// </summary>
    /// <param name="repoId">The repository whose digest to load. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the load.</param>
    internal async Task<RepoContextCoverageDigest> LoadAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        try
        {
            var tree = Tree;
            var state = await tree.GetAsync(RepoContextKeys.VectorCoverageState(repoId), cancellationToken)
                .ConfigureAwait(false);
            if (state is null || state.Length == 0)
            {
                return await ClassifyUnbuiltAsync(tree, repoId, cancellationToken).ConfigureAwait(false);
            }

            var embedded = new HashSet<ulong>();
            var contentless = new HashSet<ulong>();
            var pagesRead = 1;

            for (var start = 0; start < RepoContextCoveragePage.PageCount; start += PageReadBatchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var batch = new List<string>(PageReadBatchSize);
                for (var page = start; page < Math.Min(start + PageReadBatchSize, RepoContextCoveragePage.PageCount); page++)
                {
                    batch.Add(RepoContextKeys.VectorCoveragePage(repoId, page));
                }

                pagesRead += batch.Count;
                var result = await tree.GetManyWithGateAccountingAsync(batch, cancellationToken).ConfigureAwait(false);

                // #2277 fail-closed, applied to the digest's own reads. A page the
                // access gate pruned decodes as an empty page, which under-reports
                // coverage - and here that is NOT the cheap direction, because the
                // caller's response to an uncovered source is to re-embed it. So a
                // pruned read makes the whole digest unusable for this pass and the
                // caller falls back to the membership probe, exactly as it would if
                // no digest existed.
                if (!result.IsComplete)
                {
                    _logger.LogWarning(
                        "Repo {RepoId}: the read-path access gate pruned {Pruned} vector-coverage digest page(s), " +
                        "so the digest cannot be read as authoritative this pass; falling back to the membership probe.",
                        repoId, result.PrunedByAccessGate);
                    return RepoContextCoverageDigest.Unbuilt;
                }

                foreach (var row in result.Values.Values)
                {
                    if (RepoContextCoveragePage.TryDecode(row, out var pageEmbedded, out var pageContentless))
                    {
                        embedded.UnionWith(pageEmbedded);
                        contentless.UnionWith(pageContentless);
                    }
                }
            }

            return new RepoContextCoverageDigest(
                true, embedded, contentless, pagesRead, RepoContextCoverageDigestPresence.Consumed);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "Repo {RepoId}: vector-coverage digest load failed; this pass falls back to the membership probe.",
                repoId);
            return RepoContextCoverageDigest.Unbuilt;
        }
    }

    /// <summary>
    /// Distinguishes a digest that is genuinely absent from one whose pages are
    /// present, well-formed, and read by nothing because the built-state marker is
    /// missing.
    /// <para>
    /// Both states behave identically - the caller falls back to the membership
    /// probe either way - so this exists purely so the fallback can be <b>reported</b>
    /// truthfully. Without it, a digest that stopped being consumed logs exactly like
    /// a first-run bootstrap, and the two are then indistinguishable for as long as
    /// nobody reads the tree by hand. The sweep is a bounded 256 rows and only runs
    /// on the unbuilt path, which is about to pay an O(sources) rebuild regardless.
    /// </para>
    /// </summary>
    private async Task<RepoContextCoverageDigest> ClassifyUnbuiltAsync(
        ILattice tree, string repoId, CancellationToken cancellationToken)
    {
        var pagesFound = 0;
        var pagesRead = 1;

        try
        {
            for (var start = 0; start < RepoContextCoveragePage.PageCount; start += PageReadBatchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var batch = new List<string>(PageReadBatchSize);
                for (var page = start;
                     page < Math.Min(start + PageReadBatchSize, RepoContextCoveragePage.PageCount);
                     page++)
                {
                    batch.Add(RepoContextKeys.VectorCoveragePage(repoId, page));
                }

                pagesRead += batch.Count;
                foreach (var row in (await tree.GetManyAsync(batch, cancellationToken).ConfigureAwait(false)).Values)
                {
                    if (RepoContextCoveragePage.TryDecode(row, out var pageEmbedded, out var pageContentless)
                        && (pageEmbedded.Count > 0 || pageContentless.Count > 0))
                    {
                        pagesFound++;
                    }
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The presence probe is diagnostic only. Failing it must not change the
            // outcome, which is "fall back to the probe" either way.
            _logger.LogDebug(ex, "Repo {RepoId}: vector-coverage digest presence probe failed.", repoId);
            return RepoContextCoverageDigest.Unbuilt;
        }

        if (pagesFound == 0)
        {
            _logger.LogDebug(
                "Repo {RepoId}: no vector-coverage digest present; this pass uses the membership probe and seeds one.",
                repoId);
            return RepoContextCoverageDigest.Unbuilt;
        }

        _logger.LogWarning(
            "Repo {RepoId}: {PagesFound} vector-coverage digest page(s) are present and decode cleanly, but the " +
            "built-state marker is absent, so no consumer reads them and every pass silently falls back to the " +
            "O(sources) membership probe. This is the signature of a rebuild that was interrupted after writing " +
            "pages and before marking them consumable; the digest will be re-derived and re-marked.",
            repoId, pagesFound);
        return RepoContextCoverageDigest.UnconsumedPagesPresent(pagesRead);
    }

    /// <summary>
    /// Adds coverage to the digest, touching only the pages the identifiers land on.
    /// Idempotent: re-recording an already-covered source rewrites nothing, which is
    /// exactly why the digest holds a set rather than a counter - the membership
    /// write path reports no per-key transition, so a counter would drift upward on
    /// the routine re-add of an unchanged file.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="embeddedSourceIds">Source identifiers that now carry a live embedding.</param>
    /// <param name="contentlessSourceIds">Source identifiers that now carry a contentless marker.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    internal Task RecordCoveredAsync(
        string repoId,
        IEnumerable<string>? embeddedSourceIds,
        IEnumerable<string>? contentlessSourceIds,
        CancellationToken cancellationToken)
        => MutateAsync(repoId, embeddedSourceIds, contentlessSourceIds, covered: true, cancellationToken);

    /// <summary>
    /// Removes coverage from the digest. Called <b>before</b> the matching
    /// membership removal, so a crash between the two leaves the digest reporting
    /// less coverage than membership holds, which costs a redundant embed rather
    /// than masking a gap.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="embeddedSourceIds">Source identifiers whose embedding is being retired.</param>
    /// <param name="contentlessSourceIds">Source identifiers whose contentless marker is being cleared.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    internal Task RecordUncoveredAsync(
        string repoId,
        IEnumerable<string>? embeddedSourceIds,
        IEnumerable<string>? contentlessSourceIds,
        CancellationToken cancellationToken)
        => MutateAsync(repoId, embeddedSourceIds, contentlessSourceIds, covered: false, cancellationToken);

    /// <summary>
    /// Rewrites a repository's whole digest from an authoritative membership
    /// snapshot and marks it built. This is the one-time bootstrap on a deployment
    /// whose membership predates the digest, and the periodic audit that reconciles
    /// any drift; both are the only paths that still pay an O(sources) membership
    /// read.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="coverage">The authoritative membership coverage to mirror. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    /// <returns>
    /// <see langword="true"/> when the digest was re-derived and marked built;
    /// <see langword="false"/> when the supplied coverage was refused as
    /// inconclusive and the digest was left exactly as it was.
    /// </returns>
    internal async Task<bool> RebuildAsync(
        string repoId, RepoContextEmbeddingCoverage coverage, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        // A rebuild MIRRORS its input, so it can only ever be as trustworthy as the
        // read that produced it. A coverage whose absences are not conclusive - the
        // read-path access gate pruned probed keys, or restricted the scanned range -
        // is not merely incomplete here: mirroring it would write "these sources are
        // not covered" for every source the gate withheld, mark that built, and turn a
        // transient authorization condition into a durable, authoritative, and
        // self-refreshing lie. Every later pass would then re-embed the whole withheld
        // set while reporting success.
        //
        // Refusing leaves the digest in whatever state it was already in. When that is
        // unbuilt, callers fall back to the per-source membership probe, which IS gate
        // accounted and degrades deliberately - which is exactly the behaviour the
        // unbuilt state already means downstream, so no new state is invented.
        if (!coverage.AbsenceIsConclusive)
        {
            _logger.LogWarning(
                "Repo {RepoId}: refusing to rebuild the vector-coverage digest because the membership read it " +
                "would mirror is not conclusive - the read-path access gate pruned {Pruned} probed key(s) and " +
                "reported range coverage {RangeCoverage}. Seeding from it would record every withheld source as " +
                "uncovered and mark that authoritative. The digest is left unchanged and this pass falls back to " +
                "the per-source membership probe.",
                repoId, coverage.PrunedByAccessGate, coverage.RangeGateCoverage);
            return false;
        }

        var pages = new Dictionary<int, (HashSet<ulong> Embedded, HashSet<ulong> Contentless)>();
        foreach (var sourceId in coverage.Embedded)
        {
            if (RepoContextCoveragePage.TryParse(sourceId, out var value))
            {
                Bucket(pages, RepoContextCoveragePage.PageOf(value)).Embedded.Add(value);
            }
        }

        foreach (var sourceId in coverage.Contentless)
        {
            if (RepoContextCoveragePage.TryParse(sourceId, out var value))
            {
                Bucket(pages, RepoContextCoveragePage.PageOf(value)).Contentless.Add(value);
            }
        }

        var tree = Tree;
        for (var page = 0; page < RepoContextCoveragePage.PageCount; page++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = RepoContextKeys.VectorCoveragePage(repoId, page);
            if (pages.TryGetValue(page, out var bucket))
            {
                await tree.SetAsync(key, RepoContextCoveragePage.Encode(bucket.Embedded, bucket.Contentless), cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await tree.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }

        // The state marker is written last, so a rebuild interrupted part way leaves
        // the digest unbuilt and the next pass repeats it, rather than publishing a
        // half-written digest that would over-report the pages it never reached.
        await tree.SetAsync(RepoContextKeys.VectorCoverageState(repoId), StateMarker, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Repo {RepoId}: vector-coverage digest rebuilt from membership - {Embedded} embedded and {Contentless} " +
            "contentless source(s) across {Pages} page(s). Gap detection now reads a fixed number of digest rows " +
            "per pass instead of two membership reads per source.",
            repoId, coverage.Embedded.Count, coverage.Contentless.Count, RepoContextCoveragePage.PageCount);
        return true;
    }

    /// <summary>
    /// Drops a repository's whole digest, including its built marker, so the next
    /// pass re-derives it from membership. Used when the digest can no longer be
    /// trusted to be a subset of membership.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the reset.</param>
    internal async Task ResetAsync(string repoId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var tree = Tree;

        // The marker goes first: while it is absent the digest asserts nothing, so an
        // interrupted reset degrades to the membership probe instead of exposing the
        // half-deleted pages as authoritative.
        await tree.DeleteAsync(RepoContextKeys.VectorCoverageState(repoId), cancellationToken).ConfigureAwait(false);
        for (var page = 0; page < RepoContextCoveragePage.PageCount; page++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await tree.DeleteAsync(RepoContextKeys.VectorCoveragePage(repoId, page), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static (HashSet<ulong> Embedded, HashSet<ulong> Contentless) Bucket(
        Dictionary<int, (HashSet<ulong> Embedded, HashSet<ulong> Contentless)> pages, int page)
    {
        if (!pages.TryGetValue(page, out var bucket))
        {
            bucket = ([], []);
            pages[page] = bucket;
        }

        return bucket;
    }

    private async Task MutateAsync(
        string repoId,
        IEnumerable<string>? embeddedSourceIds,
        IEnumerable<string>? contentlessSourceIds,
        bool covered,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var touched = new Dictionary<int, (HashSet<ulong> Embedded, HashSet<ulong> Contentless)>();
        Collect(embeddedSourceIds, touched, contentless: false);
        Collect(contentlessSourceIds, touched, contentless: true);
        if (touched.Count == 0)
        {
            return;
        }

        try
        {
            var tree = Tree;

            // A digest that was never built must stay unbuilt: seeding it from the
            // slice of sources one pass happened to touch would publish a digest that
            // omits every source the pass did not see, and an omission reads as a gap.
            var state = await tree.GetAsync(RepoContextKeys.VectorCoverageState(repoId), cancellationToken)
                .ConfigureAwait(false);
            if (state is null || state.Length == 0)
            {
                return;
            }

            // The pages this mutation touches, read in batches rather than one call
            // per page. A pass that adds a few thousand sources touches nearly all 256
            // pages, and a point read each would put 256 round trips on the ingest's
            // critical path purely to maintain a projection - the kind of write-path
            // cost that would quietly hand back the read-path saving this item bought.
            var keys = new List<string>(touched.Count);
            foreach (var page in touched.Keys)
            {
                keys.Add(RepoContextKeys.VectorCoveragePage(repoId, page));
            }

            var existingPages = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            for (var offset = 0; offset < keys.Count; offset += PageReadBatchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var batch = keys.GetRange(offset, Math.Min(PageReadBatchSize, keys.Count - offset));
                foreach (var (key, value) in await tree.GetManyAsync(batch, cancellationToken).ConfigureAwait(false))
                {
                    existingPages[key] = value;
                }
            }

            var writes = new List<KeyValuePair<string, byte[]>>();
            var deletes = new List<string>();
            foreach (var (page, delta) in touched)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var key = RepoContextKeys.VectorCoveragePage(repoId, page);
                RepoContextCoveragePage.TryDecode(
                    existingPages.GetValueOrDefault(key), out var embedded, out var contentless);

                var changed = false;
                foreach (var value in delta.Embedded)
                {
                    changed |= covered ? embedded.Add(value) : embedded.Remove(value);
                }

                foreach (var value in delta.Contentless)
                {
                    changed |= covered ? contentless.Add(value) : contentless.Remove(value);
                }

                if (!changed)
                {
                    continue;
                }

                if (embedded.Count == 0 && contentless.Count == 0)
                {
                    deletes.Add(key);
                    continue;
                }

                writes.Add(new KeyValuePair<string, byte[]>(
                    key, RepoContextCoveragePage.Encode(embedded, contentless)));
            }

            if (writes.Count == 0 && deletes.Count == 0)
            {
                // The whole mutation was a no-op, which is the common case: every
                // reconcile re-offers its unchanged files. Suppressing the write here
                // is what keeps a converged repository from rewriting the digest on
                // every pass forever.
                return;
            }

            // Atomic so no reader ever sees a half-applied mutation. The operation id
            // is fresh per call rather than derived from the delta: re-attaching to a
            // prior saga would replay a stale read-modify-write, and the digest's own
            // periodic audit is the recovery mechanism for a lost mutation anyway.
            await tree.SetManyAtomicAsync(
                writes,
                deletes,
                "vcov-" + Guid.NewGuid().ToString("N"),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A failed digest maintenance write must not fail the ingest that drove
            // it. Losing an add leaves the digest under-reporting (a redundant embed
            // next pass); losing a removal is repaired by the periodic audit.
            _logger.LogWarning(
                ex,
                "Repo {RepoId}: vector-coverage digest maintenance write failed; the digest may under-report " +
                "coverage until the next audit, which costs a redundant embed and never masks a gap.",
                repoId);
        }
    }

    private static void Collect(
        IEnumerable<string>? sourceIds,
        Dictionary<int, (HashSet<ulong> Embedded, HashSet<ulong> Contentless)> touched,
        bool contentless)
    {
        if (sourceIds is null)
        {
            return;
        }

        foreach (var sourceId in sourceIds)
        {
            if (!RepoContextCoveragePage.TryParse(sourceId, out var value))
            {
                continue;
            }

            var bucket = Bucket(touched, RepoContextCoveragePage.PageOf(value));
            if (contentless)
            {
                bucket.Contentless.Add(value);
            }
            else
            {
                bucket.Embedded.Add(value);
            }
        }
    }
}
