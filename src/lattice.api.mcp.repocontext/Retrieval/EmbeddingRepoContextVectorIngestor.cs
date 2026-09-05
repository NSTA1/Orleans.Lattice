using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The real bootstrap-time vectorisation seam: the
/// <see cref="IRepoContextVectorIngestor"/> that embeds the files a bootstrap run
/// added or updated - and the per-symbol records the reconcile captured - and
/// stores their vectors on the reserved vector trees. It replaces the default
/// <see cref="NoOpRepoContextVectorIngestor"/> so a run wires straight through from
/// the structural walk to a searchable semantic index, with no change to the tool.
/// <para>
/// <b>Chunked and symbol-granular.</b> A file is embedded as several overlapping
/// windows (see <see cref="RepoContextTextChunker"/>) rather than one leading-window
/// vector, so content deep in a large file is searchable; each window is a passage
/// whose canonical record is the file, so a hit hydrates and de-duplicates to the
/// file. A symbol is embedded as its own single passage (kind, name, and signature)
/// so a symbol-level query lands on the declaring symbol.
/// </para>
/// <para>
/// <b>Fail-closed and honest.</b> When no <see cref="IEmbeddingProvider"/> is
/// configured, the provider is unreachable
/// (<see cref="IEmbeddingProvider.IsAvailableAsync"/> is false), or an embed call
/// returns an unsuccessful <see cref="EmbeddingResult"/>, the ingestor simply
/// records nothing and returns - it never throws out of a bootstrap run and never
/// stores an unembedded or wrong-space vector. Search then degrades to structural
/// or keyword recall over the records the structural walk already captured.
/// </para>
/// </summary>
internal sealed class EmbeddingRepoContextVectorIngestor : IRepoContextVectorIngestor
{
    /// <summary>
    /// The maximum number of characters of a file's content that are read for
    /// embedding. A file longer than this is truncated to its leading window before
    /// chunking, which bounds the memory a single very large file uses; the
    /// chunker's own per-file window cap bounds how many of those characters are
    /// actually embedded.
    /// </summary>
    internal const int MaxEmbedChars = 64 * 1024;

    /// <summary>
    /// The maximum number of passages embedded in a single request to the provider.
    /// A bootstrap run over a real repository has thousands of passages once files
    /// are chunked and symbols are embedded; sending them all in one call builds a
    /// multi-megabyte request that can exceed the provider's HTTP timeout and
    /// fail-close the whole run. Batching bounds each request's size and duration
    /// and lets vectors land incrementally, so a slow or partial provider still
    /// yields a searchable index instead of nothing.
    /// </summary>
    internal const int EmbedBatchSize = 32;

    /// <summary>
    /// How many <i>consecutive</i> batch record failures the arm tolerates before it
    /// gives the remaining batches up for this pass. A single failure is unlucky and
    /// the batches after it are worth attempting; a run of them means the vector
    /// plane is saturated - characteristically a batched CRDT apply timing out - and
    /// every further batch adds load to a store that is already failing while landing
    /// nothing. Stopping early costs nothing durable: a deferred source is simply left
    /// unmarked and the next reconcile re-embeds it idempotently.
    /// </summary>
    internal const int MaxConsecutiveBatchFailures = 3;

    /// <summary>
    /// The recorded-marker set used when the real one could not be read, so a
    /// failed read degrades to "no marker evidence this pass" rather than failing
    /// the arm. Shared and immutable because it is only ever read.
    /// </summary>
    private static readonly IReadOnlySet<string> EmptyMemoryKeys =
        new HashSet<string>(StringComparer.Ordinal);

    private readonly RepoContextVectorWriter _writer;
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;
    private readonly IEmbeddingProvider? _embeddingProvider;
    private readonly ILogger<EmbeddingRepoContextVectorIngestor> _logger;

    /// <summary>Creates the embedding vector ingestor.</summary>
    /// <param name="writer">The writer that persists vectors onto the reserved trees. Must not be <see langword="null"/>.</param>
    /// <param name="grainFactory">The grain factory used to enumerate the symbol tree for symbol embedding. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode symbol records during symbol embedding. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger used to record fail-closed fallbacks. Must not be <see langword="null"/>.</param>
    /// <param name="embeddingProvider">The embedding provider, or <see langword="null"/> when the host bound none (search then degrades to keyword recall).</param>
    /// <exception cref="ArgumentNullException"><paramref name="writer"/>, <paramref name="grainFactory"/>, <paramref name="serializer"/>, or <paramref name="logger"/> is null.</exception>
    public EmbeddingRepoContextVectorIngestor(
        RepoContextVectorWriter writer,
        IGrainFactory grainFactory,
        Serializer serializer,
        ILogger<EmbeddingRepoContextVectorIngestor> logger,
        IEmbeddingProvider? embeddingProvider = null)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);
        _writer = writer;
        _grainFactory = grainFactory;
        _serializer = serializer;
        _logger = logger;
        _embeddingProvider = embeddingProvider;
    }

    /// <inheritdoc />
    public async ValueTask<RepoFileVectorIngestOutcome> IngestAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> changedFiles,
        IReadOnlyList<RepoFileEntry> unchangedFiles,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(repoRoot);
        ArgumentNullException.ThrowIfNull(changedFiles);
        ArgumentNullException.ThrowIfNull(unchangedFiles);

        // Gate on the provider before any membership read: with no provider there
        // is nothing to embed and no reason to load the membership set, so an
        // unchanged file is never falsely flagged as missing an embedding.
        if (_embeddingProvider is null || (changedFiles.Count == 0 && unchangedFiles.Count == 0))
        {
            return RepoFileVectorIngestOutcome.None;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "Skipping bootstrap vectorisation for repository {RepoId}: the embedding provider is unavailable. Search will use keyword recall.",
                repoId);
            return RepoFileVectorIngestOutcome.None;
        }

        // Probe coverage for exactly the candidate files (changed + unchanged) with a
        // bounded point-read, so a churn-bloated membership tree can never force an
        // unbounded sorted-range scan past the response deadline (issue #1556). Every
        // source id consulted downstream - SelectFilesToEmbed's unchanged-file check
        // and the contentless mark/unmark below - is drawn from this candidate set.
        var candidateKeys = new List<string>(changedFiles.Count + unchangedFiles.Count);
        foreach (var file in changedFiles)
        {
            candidateKeys.Add(RepoContextKeys.File(repoId, file.RelativePath));
        }

        foreach (var file in unchangedFiles)
        {
            candidateKeys.Add(RepoContextKeys.File(repoId, file.RelativePath));
        }

        // Losing this probe must not cost the whole arm. Without coverage we cannot
        // tell an embedded file from a missing one, so the gap sweep is skipped for
        // this pass rather than guessed at - guessing "uncovered" would re-embed the
        // entire repository. The changed files are embedded regardless, because they
        // are re-embedded whatever their coverage says, so the pass still does its
        // primary job and the back-fill simply resumes next reconcile.
        //
        // This is the file-arm twin of the symbol arm's per-page probe guard. Both
        // were needed: guarding only the arm that happened to be failing at the time
        // left this one to become the next thing that broke, which is exactly what
        // happened on the live deployment once the symbol arm was fixed.
        RepoContextEmbeddingCoverage coverage;
        var coverageProbeFailed = false;
        var gapsSelected = 0;
        try
        {
            coverage = await _writer.ProbeCoverageAsync(repoId, candidateKeys, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            coverageProbeFailed = true;
            coverage = RepoContextEmbeddingCoverage.Empty;
            _logger.LogWarning(
                ex,
                "Repo {RepoId}: the embedding-coverage probe failed; embedding the {Changed} changed file(s) "
                + "and deferring the gap sweep over {Unchanged} unchanged file(s) to the next reconcile.",
                repoId,
                changedFiles.Count,
                unchangedFiles.Count);
        }

        var toEmbed = coverageProbeFailed
            ? new List<RepoFileEntry>(changedFiles)
            : SelectFilesToEmbed(repoId, coverage, changedFiles, unchangedFiles, out gapsSelected);
        if (toEmbed.Count == 0)
        {
            return new RepoFileVectorIngestOutcome(0, gapsSelected, !coverageProbeFailed);
        }

        var sources = new List<EmbeddingSource>(toEmbed.Count);
        List<string>? contentlessToMark = null;
        List<string>? contentfulToUnmark = null;
        foreach (var file in toEmbed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sourceKey = RepoContextKeys.File(repoId, file.RelativePath);
            var text = await ReadContentAsync(repoRoot, file.RelativePath, cancellationToken).ConfigureAwait(false);
            if (text is null)
            {
                // A transient read failure (IO or permission), not a contentless
                // file: leave it uncovered so a later pass retries it once the file
                // is readable, rather than marking it considered.
                continue;
            }

            var windows = string.IsNullOrWhiteSpace(text)
                ? Array.Empty<string>()
                : RepoContextTextChunker.Chunk(text);
            if (windows.Count == 0)
            {
                // Read, but with no embeddable passage (empty or whitespace-only, or
                // it chunked to zero windows). Record a "considered, no passages"
                // marker so the always-on gap sweep and the unchanged-file selection
                // stop treating this file as a missing embedding and re-driving the
                // index on every reconcile. Skip the write when it is already marked.
                if (!coverage.Contentless.Contains(VectorCodec.SourceId(sourceKey)))
                {
                    (contentlessToMark ??= new List<string>()).Add(sourceKey);
                }

                continue;
            }

            // The file carries content. If it was previously marked contentless (it
            // just gained content), clear that marker so its real embedding covers
            // it - and so a failed embed leaves it uncovered and retryable rather
            // than falsely covered by a stale marker.
            var sourceId = VectorCodec.SourceId(sourceKey);
            if (coverage.Contentless.Contains(sourceId))
            {
                (contentfulToUnmark ??= new List<string>()).Add(sourceId);
            }

            sources.Add(new EmbeddingSource(sourceKey, windows));
        }

        var embedded = await EmbedAndStoreAsync(repoId, sources, onProgress, cancellationToken)
            .ConfigureAwait(false);

        // The contentless markers are the file arm's equivalent bookkeeping: losing
        // them costs a redundant re-read of an empty file next pass, never
        // correctness, so they must not take a successful pass down with them.
        try
        {
            if (contentlessToMark is not null)
            {
                await _writer.MarkContentlessAsync(repoId, contentlessToMark, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (contentfulToUnmark is not null)
            {
                foreach (var sourceId in contentfulToUnmark)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await _writer.UnmarkContentlessAsync(repoId, sourceId, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "Repo {RepoId}: could not update the contentless markers; they are re-evaluated on the next "
                + "reconcile and no embedding is affected.",
                repoId);
        }

        if (embedded == 0 && sources.Count > 0)
        {
            _logger.LogInformation(
                "Skipping bootstrap vectorisation for repository {RepoId}: no embedding batch succeeded. Search will use keyword recall.",
                repoId);
        }

        return new RepoFileVectorIngestOutcome(embedded, gapsSelected, !coverageProbeFailed);
    }

    /// <inheritdoc />
    public async Task<int> IngestSymbolsAsync(
        string repoId,
        IReadOnlyCollection<string> changedSymbolKeys,
        IReadOnlyCollection<string> prunedSymbolKeys,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(changedSymbolKeys);
        ArgumentNullException.ThrowIfNull(prunedSymbolKeys);

        // Retire a pruned symbol's embedding regardless of the provider: a symbol
        // the reconcile removed must drop its vector, or the membership count drifts
        // high. Retirement only deletes stored records, so it needs no embedder.
        foreach (var key in prunedSymbolKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _writer.RetireAsync(repoId, key, cancellationToken).ConfigureAwait(false);
        }

        if (_embeddingProvider is null)
        {
            return 0;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "Skipping symbol vectorisation for repository {RepoId}: the embedding provider is unavailable. Search will use keyword recall.",
                repoId);
            return 0;
        }

        // A symbol is (re-)embedded when its declaration changed this pass or when
        // it has no live embedding yet (a new symbol, or a back-fill of symbols
        // captured before symbol embedding existed). Presence is judged from the
        // add-wins membership set, probed per page with a bounded point-read so a
        // churn-bloated membership tree can never force an unbounded sorted-range
        // scan past the response deadline (issue #1556); an already-embedded,
        // unchanged symbol is skipped without a payload read.
        var changed = new HashSet<string>(changedSymbolKeys, StringComparer.Ordinal);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var prefix = RepoContextKeys.SymbolsPrefix(repoId);
        var sources = new List<EmbeddingSource>();

        string? token = null;
        var probeFailures = 0;
        Exception? firstProbeFailure = null;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            var pageKeys = new List<string>(page.Records.Count);
            foreach (var record in page.Records)
            {
                if (record.Value is not null)
                {
                    pageKeys.Add(record.Key);
                }
            }

            // The coverage probe reads the membership tree, which is the busiest
            // tree in the plane during a reconcile - the gap sweep drives a point
            // read per page across the whole symbol space - so this call is the one
            // that times out under load. Losing it must cost one page, not the
            // whole arm: without coverage for this page we cannot tell embedded
            // from missing, so we skip the page rather than guess, and the next
            // pass picks up whatever it was hiding.
            IReadOnlySet<string> embeddedMembers;
            try
            {
                embeddedMembers = await _writer
                    .ProbeEmbeddedMembersAsync(repoId, pageKeys, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                firstProbeFailure ??= ex;
                probeFailures++;
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: the embedding-coverage probe failed for a page of {Count} symbol(s); skipping "
                    + "the page and continuing. Its symbols are re-checked on the next reconcile.",
                    repoId,
                    pageKeys.Count);
                token = page.HasMore ? page.ContinuationToken : null;
                continue;
            }

            foreach (var record in page.Records)
            {
                if (record.Value is null)
                {
                    continue;
                }

                var sourceKey = record.Key;
                if (!changed.Contains(sourceKey) && embeddedMembers.Contains(VectorCodec.SourceId(sourceKey)))
                {
                    continue;
                }

                var text = BuildSymbolText(_serializer.Deserialize<SymbolRecord>(record.Value));
                if (string.IsNullOrWhiteSpace(text))
                {
                    continue;
                }

                sources.Add(new EmbeddingSource(sourceKey, new[] { text }));
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        var symbolsEmbedded = await EmbedAndStoreAsync(repoId, sources, onProgress: null, cancellationToken)
            .ConfigureAwait(false);

        // Same rule as the batch boundary: a pass that achieved nothing at all
        // still has to surface its fault, but one that made progress counts as
        // progress even though part of the symbol space went unexamined.
        if (symbolsEmbedded == 0 && firstProbeFailure is not null)
        {
            _logger.LogWarning(
                "Repo {RepoId}: {Failed} coverage probe(s) failed and nothing was embedded; surfacing the first "
                + "fault so the arm reports incomplete.",
                repoId,
                probeFailures);
            throw firstProbeFailure;
        }

        return symbolsEmbedded;
    }

    /// <summary>
    /// Embeds the repository's durable agent-memory entries as their own passages
    /// (issue #1878). Mirrors the symbol path: an entry changed this pass is
    /// re-embedded, one retired this pass has its vector retired, and any entry
    /// with no live embedding is back-filled - which is what converts an existing
    /// store, captured entirely before memory embedding existed, without a
    /// re-walk.
    /// </summary>
    public async Task<int> IngestMemoryAsync(
        string repoId,
        IReadOnlyCollection<string> changedMemoryKeys,
        IReadOnlyCollection<string> retiredMemoryKeys,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(changedMemoryKeys);
        ArgumentNullException.ThrowIfNull(retiredMemoryKeys);

        // Retire regardless of the provider: an entry that was forgotten or
        // expired must drop its vector, or the membership count drifts high and
        // the semantic path ranks a key that no longer hydrates. Retirement only
        // deletes stored records, so it needs no embedder.
        foreach (var key in retiredMemoryKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _writer.RetireAsync(repoId, key, cancellationToken).ConfigureAwait(false);
        }

        if (_embeddingProvider is null)
        {
            return 0;
        }

        if (!await _embeddingProvider.IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogInformation(
                "Skipping memory vectorisation for repository {RepoId}: the embedding provider is unavailable. Search will use keyword recall.",
                repoId);
            return 0;
        }

        var changed = new HashSet<string>(changedMemoryKeys, StringComparer.Ordinal);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);
        var prefix = RepoContextKeys.MemoryPrefix(repoId);
        var sources = new List<EmbeddingSource>();

        // The embedded-key markers this repository already holds, read once and
        // used twice: as a skip signal during the walk below, and as the recorded
        // half of the orphan set afterwards.
        //
        // Consulting it as a skip signal is what stops an entry being re-embedded
        // forever. The source-id flag probed per page is written by AddMembersAsync,
        // which shares the membership tree with the file and symbol arms' gap
        // sweeps and is the write that times out under that load; when it does not
        // land, an entry looks un-embedded on every later pass even though its
        // vectors are stored. This marker is written by a small targeted call that
        // does not contend with the sweep, so it survives exactly the pressure that
        // loses the flag. Either being present is sufficient evidence, and each is
        // only ever written after the corresponding vectors landed.
        //
        // The load is itself a whole-set range scan over the membership tree, so it
        // is one more thing that can time out under the very pressure it exists to
        // tolerate. Losing it degrades to the previous behaviour - the source-id
        // flag alone - rather than failing the arm, and an empty recorded set also
        // makes the orphan sweep a no-op, which is the safe direction: it declines
        // to retire anything rather than mistaking an unreadable set for an empty
        // one and sweeping live embeddings away.
        IReadOnlySet<string> recordedMemoryKeys;
        try
        {
            recordedMemoryKeys = await _writer
                .LoadEmbeddedMemoryKeysAsync(repoId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            recordedMemoryKeys = EmptyMemoryKeys;
            _logger.LogWarning(
                ex,
                "Repo {RepoId}: could not read the embedded-memory-key markers; falling back to the source-id "
                + "flag alone for this pass and skipping the orphan sweep.",
                repoId);
        }

        // Every memory key that is live right now. Collected during the same walk
        // that selects what to embed, so the orphan sweep below costs one extra
        // set rather than a second pass over the store.
        var liveKeys = new HashSet<string>(StringComparer.Ordinal);

        string? token = null;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await RepoContextPortability
                .EnumerateAsync(tree, prefix, token, RepoContextPortability.DefaultPageSize, vectorExport: null, cancellationToken)
                .ConfigureAwait(false);

            var pageKeys = new List<string>(page.Records.Count);
            foreach (var record in page.Records)
            {
                if (record.Value is not null)
                {
                    pageKeys.Add(record.Key);
                    liveKeys.Add(record.Key);
                }
            }

            // The last of the four membership reads in this file to need a guard.
            // Unlike the symbol arm's equivalent, a failure here need not skip the
            // page: the marker set loaded above is an independent source of the
            // same evidence, so the walk can fall back to it alone and still make
            // the right decision for most entries. Only an entry that has neither
            // signal is re-embedded, which is idempotent.
            IReadOnlySet<string> embeddedMembers;
            try
            {
                embeddedMembers = await _writer
                    .ProbeEmbeddedMembersAsync(repoId, pageKeys, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                embeddedMembers = EmptyMemoryKeys;
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: the embedded-member probe failed for a page of {Count} memory entr(ies); "
                    + "falling back to the embedded-key markers alone for this page.",
                    repoId,
                    pageKeys.Count);
            }

            foreach (var record in page.Records)
            {
                if (record.Value is null)
                {
                    continue;
                }

                var sourceKey = record.Key;
                if (!changed.Contains(sourceKey)
                    && (embeddedMembers.Contains(VectorCodec.SourceId(sourceKey))
                        || recordedMemoryKeys.Contains(sourceKey)))
                {
                    continue;
                }

                // The memory value is an MvRegister blob whose concurrent values are
                // serialized MemoryRecords, not a bare record: fold it exactly as the
                // projection does so the embedded passage reflects the same converged
                // entry that recall and keyword search return. Deserializing the
                // envelope directly as a MemoryRecord would read the wrong shape.
                var folded = RepoContextMemoryCodec.Fold(record.Value, _serializer);
                if (folded is null)
                {
                    continue;
                }

                var text = BuildMemoryText(folded);
                if (string.IsNullOrWhiteSpace(text))
                {
                    continue;
                }

                // A memory body is prose and can be long, so chunk it exactly as a
                // file is chunked rather than truncating: a gotcha's operative
                // detail is as often at its end (the fix, the corollaries) as at
                // its start, and a single truncated passage would drop it.
                var windows = RepoContextTextChunker.Chunk(text);
                if (windows.Count == 0)
                {
                    continue;
                }

                sources.Add(new EmbeddingSource(sourceKey, windows));
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        await SweepOrphanedMemoryVectorsAsync(repoId, liveKeys, recordedMemoryKeys, cancellationToken)
            .ConfigureAwait(false);

        var landed = await EmbedAndStoreReportingLandedAsync(repoId, sources, onProgress: null, cancellationToken)
            .ConfigureAwait(false);

        // Record only what actually landed. Marking every source the pass intended
        // to embed would assert an embedding that a failed batch never stored, and
        // this marker is the recorded half of the orphan set (recorded - live), so
        // a false entry there is a phantom record rather than a wasted write. It
        // matters more since each batch gained its own failure boundary: the arm
        // now survives a failed batch and reaches this line, where previously the
        // whole arm unwound and nothing was marked at all.
        if (landed.Count > 0)
        {
            // Bookkeeping, not the work itself: the vectors are already stored and
            // their membership recorded, so losing this write costs one redundant
            // re-embed on a later pass and nothing else. Failing the whole run over
            // it would discard a pass that genuinely succeeded - the same mistake,
            // in the same code path, as every other seam guarded here.
            try
            {
                await _writer.MarkMemoryEmbeddedAsync(repoId, landed, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: could not record the embedded-key marker for {Count} memory entr(ies); "
                    + "their vectors are stored and they will simply be re-checked on the next reconcile.",
                    repoId,
                    landed.Count);
            }
        }

        return landed.Count;
    }

    /// <summary>
    /// Retires the embeddings of memory entries that no longer exist.
    /// <para>
    /// An entry removed through <c>forget</c> has its vector retired on the spot by
    /// the store, but an entry that simply <b>expires by its own time-to-live</b> -
    /// a coordination handoff written with <c>ttlSeconds</c>, say - vanishes with no
    /// code path observing it. Without this sweep its vector would survive its
    /// entry indefinitely, inflating the membership tally and spending ranking slots
    /// on a key that no longer hydrates. Vectorising memory is only a complete
    /// feature with this half present.
    /// </para>
    /// <para>
    /// The orphan set is exactly (recorded - live), both of which this pass already
    /// holds: the recorded keys were loaded once by the caller for its skip check,
    /// and the live keys were collected during the enumeration above. So the sweep
    /// adds no read at all and touches only entries that actually vanished.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository being swept.</param>
    /// <param name="liveKeys">The memory keys observed live during this pass's walk.</param>
    /// <param name="recorded">
    /// The embedded-key markers, already loaded by the caller for its skip check so
    /// the sweep costs no additional read.
    /// </param>
    /// <param name="cancellationToken">Cancels the sweep.</param>
    private async Task SweepOrphanedMemoryVectorsAsync(
        string repoId,
        HashSet<string> liveKeys,
        IReadOnlySet<string> recorded,
        CancellationToken cancellationToken)
    {
        if (recorded.Count == 0)
        {
            return;
        }

        var swept = 0;
        foreach (var key in recorded)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (liveKeys.Contains(key))
            {
                continue;
            }

            await _writer.RetireAsync(repoId, key, cancellationToken).ConfigureAwait(false);
            await _writer.UnmarkMemoryEmbeddedAsync(repoId, key, cancellationToken).ConfigureAwait(false);
            swept++;
        }

        if (swept > 0)
        {
            _logger.LogInformation(
                "Repo {RepoId}: retired {Swept} orphaned memory embedding(s) whose entries no longer exist.",
                repoId, swept);
        }
    }

    /// <summary>
    /// Builds the passage text for a memory entry: its topic, title, tags and
    /// body. The title and body carry the meaning; the topic and tags are
    /// included because they are how an agent actually reaches for a memory
    /// ("the gotcha about allocation probes"), and they are short enough that
    /// prepending them costs almost nothing against the body.
    /// </summary>
    private static string BuildMemoryText(MemoryRecord record)
    {
        var title = RepoContextValues.ReadString(record.Title);
        var body = RepoContextValues.ReadString(record.Body);
        var kind = record.Kind == MemoryKind.Unspecified ? "memory" : record.Kind.ToString();

        var builder = new StringBuilder();
        builder.Append(kind).Append(' ').Append(record.Topic).Append('/').Append(record.Id);

        if (!string.IsNullOrWhiteSpace(title))
        {
            builder.Append('\n').Append(title);
        }

        var tags = RepoContextEntryProjection.ReadElements(record.Tags);
        if (tags.Count > 0)
        {
            builder.Append("\ntags: ").AppendJoin(", ", tags);
        }

        if (!string.IsNullOrWhiteSpace(body))
        {
            builder.Append('\n').Append(body);
        }

        var text = builder.ToString();
        return text.Length > MaxEmbedChars ? text[..MaxEmbedChars] : text;
    }

    /// <summary>
    /// Embeds every source's passages in bounded batches and stores each source's
    /// vectors as a unit once all of its passages have landed. Batching is flat
    /// across sources, so a call that mixes many small files with a few large ones
    /// still packs full requests; a source whose passages span a failed and a
    /// succeeded batch is left incomplete and re-embedded on the next pass, so a
    /// stored source always carries its whole current passage set. Membership is
    /// recorded after each batch, so an interruption leaves at most one batch of
    /// vectors unrecorded while presence still implies a durable vector.
    /// <para>
    /// <b>Each batch carries its own failure boundary.</b> A store or membership
    /// write can time out when the vector plane is under load, and unwinding the
    /// whole call on the first such fault discarded every batch not yet reached -
    /// so a pass banked almost nothing, the next pass rebuilt the same queue and
    /// failed in the same place, and a large back-fill could never finish however
    /// many passes it was given (issue #1933). A failing batch is therefore logged
    /// and skipped: its sources stay unmarked and are retried on the next pass,
    /// which is already the contract for an interrupted batch.
    /// </para>
    /// <para>
    /// The fault is re-thrown only when <b>nothing</b> landed, so the caller can
    /// still tell a wholly broken arm from a productive one. Surfacing it after a
    /// partial pass would keep a run that genuinely advanced permanently red.
    /// </para>
    /// </summary>
    private async Task<int> EmbedAndStoreAsync(
        string repoId,
        IReadOnlyList<EmbeddingSource> sources,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken)
        => (await EmbedAndStoreReportingLandedAsync(repoId, sources, onProgress, cancellationToken)
            .ConfigureAwait(false)).Count;

    /// <summary>
    /// The variant of <see cref="EmbedAndStoreAsync"/> that reports <b>which</b>
    /// sources actually landed, rather than only how many.
    /// <para>
    /// A caller that records a marker per source needs the identities, not a
    /// count: marking a source whose batch failed would assert an embedding that
    /// does not exist. The memory arm's embedded-key marker is exactly that kind
    /// of caller - it is what makes the orphan set (recorded - live) computable -
    /// so a false entry there is a phantom record, not merely a wasted write.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository being embedded.</param>
    /// <param name="sources">The sources to embed.</param>
    /// <param name="onProgress">Optional incremental progress callback.</param>
    /// <param name="cancellationToken">Cancels the pass.</param>
    /// <returns>The source keys whose vectors were stored and whose membership was recorded.</returns>
    private async Task<List<string>> EmbedAndStoreReportingLandedAsync(
        string repoId,
        IReadOnlyList<EmbeddingSource> sources,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken)
    {
        var landed = new List<string>();
        if (sources.Count == 0)
        {
            return landed;
        }

        // Flatten every source's passages into one unit list, remembering each
        // unit's owning source and slot, so a source's vectors can be reassembled
        // in order once its units land - even across batch boundaries.
        var unitTexts = new List<string>();
        var unitOwner = new List<int>();
        var unitSlot = new List<int>();
        var slots = new ReadOnlyMemory<float>[sources.Count][];
        var filled = new int[sources.Count];
        var spaces = new EmbeddingSpace?[sources.Count];
        for (var s = 0; s < sources.Count; s++)
        {
            var units = sources[s].Units;
            slots[s] = new ReadOnlyMemory<float>[units.Count];
            for (var u = 0; u < units.Count; u++)
            {
                unitTexts.Add(units[u]);
                unitOwner.Add(s);
                unitSlot.Add(u);
            }
        }

        var embedded = 0;
        var batchEmbedded = 0;
        var failedBatches = 0;
        var consecutiveBatchFailures = 0;
        Exception? firstBatchFailure = null;
        var pendingMembers = new List<string>();
        for (var start = 0; start < unitTexts.Count; start += EmbedBatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(EmbedBatchSize, unitTexts.Count - start);
            var batchTexts = unitTexts.GetRange(start, count);

            var result = await _embeddingProvider!
                .EmbedAsync(batchTexts, EmbeddingTextType.Passage, cancellationToken)
                .ConfigureAwait(false);
            if (!result.Succeeded || result.Vectors.Count != count)
            {
                _logger.LogInformation(
                    "Bootstrap vectorisation for repository {RepoId} skipped a batch of {Count} passage(s): the embedding call did not succeed ({Error}). Those sources fall back to keyword recall.",
                    repoId,
                    count,
                    result.Error ?? "no vectors returned");
                continue;
            }

            var completed = new List<int>();
            for (var i = 0; i < count; i++)
            {
                var owner = unitOwner[start + i];
                slots[owner][unitSlot[start + i]] = result.Vectors[i];
                spaces[owner] = result.Space;
                if (++filled[owner] == slots[owner].Length)
                {
                    completed.Add(owner);
                }
            }

            // Store and record this batch under its own failure boundary. A store
            // or membership write can time out when the vector plane is under
            // load, and before this that exception unwound the whole arm - so a
            // pass discarded every batch it had not reached yet, the next pass
            // rebuilt the same queue and died at the same place, and a large
            // back-fill could never finish however many passes it was given.
            // Losing one batch costs one batch: its sources stay unmarked and are
            // retried next pass, which is already the contract.
            try
            {
                foreach (var owner in completed)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await _writer
                        .StoreAsync(repoId, sources[owner].SourceKey, spaces[owner]!, slots[owner], cancellationToken)
                        .ConfigureAwait(false);
                    pendingMembers.Add(sources[owner].SourceKey);
                    batchEmbedded++;
                }

                if (pendingMembers.Count > 0)
                {
                    // Record membership for the sources completed in this batch, after
                    // their vectors have landed. The writer lands the whole batch in one
                    // batched CRDT write (one read to mint the deltas, one apply), not
                    // one round trip per source.
                    await _writer.AddMembersAsync(repoId, pendingMembers, cancellationToken).ConfigureAwait(false);

                    // Only now is a source genuinely landed: its vectors are stored
                    // AND its membership recorded. Reporting it before the membership
                    // write would let a caller mark an embedding the store cannot see.
                    landed.AddRange(pendingMembers);
                    pendingMembers.Clear();
                }

                embedded += batchEmbedded;
                consecutiveBatchFailures = 0;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A source whose vectors landed but whose membership write did not
                // is simply unmarked, so the next pass re-embeds it idempotently.
                // Nothing here is left half-recorded in a way a later pass cannot
                // repair, which is what makes continuing safe rather than merely
                // convenient.
                firstBatchFailure ??= ex;
                failedBatches++;
                consecutiveBatchFailures++;
                pendingMembers.Clear();
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: a batch of {Count} passage(s) could not be recorded; its sources stay unmarked "
                    + "and are retried on the next reconcile. Continuing with the remaining batches.",
                    repoId,
                    count);

                // Consecutive record failures mean the vector plane is saturated, not
                // that one batch was unlucky. Driving the remaining batches into it
                // adds load to a store that is already timing out and lands nothing,
                // so stop the arm here and let the next reconcile retry from a
                // quieter store. Whatever already landed is kept, and every deferred
                // source is simply unmarked, so the next pass picks it up.
                if (consecutiveBatchFailures >= MaxConsecutiveBatchFailures)
                {
                    var deferred = unitTexts.Count - (start + count);
                    _logger.LogWarning(
                        "Repo {RepoId}: {Failures} consecutive batches failed to record; the vector plane looks "
                        + "saturated, so deferring the remaining {Deferred} passage(s) to the next reconcile "
                        + "rather than adding load.",
                        repoId,
                        consecutiveBatchFailures,
                        deferred < 0 ? 0 : deferred);
                    break;
                }

                continue;
            }
            finally
            {
                batchEmbedded = 0;
            }

            // Surface incremental progress after each batch lands, so a long
            // vectorisation pass reports a rising count instead of appearing frozen.
            if (onProgress is not null)
            {
                await onProgress(embedded, cancellationToken).ConfigureAwait(false);
            }
        }

        // Surfacing the fault only when nothing landed is what makes a partial pass
        // count as forward progress: the caller logs the arm incomplete and fails
        // the run, so reporting a fault after a productive pass would keep a run
        // that genuinely advanced permanently red.
        if (embedded == 0 && firstBatchFailure is not null)
        {
            _logger.LogWarning(
                "Repo {RepoId}: every one of the {Failed} batch(es) that produced vectors failed to record them; "
                + "surfacing the first fault so the arm reports incomplete.",
                repoId,
                failedBatches);
            throw firstBatchFailure;
        }

        return landed;
    }

    /// <summary>
    /// Builds the passage text for a symbol: its kind, fully-qualified name, and -
    /// when present - its declaration signature. The name and signature carry the
    /// symbol's meaning for retrieval; the kind disambiguates a type from a member
    /// of the same name.
    /// </summary>
    private static string BuildSymbolText(SymbolRecord record)
    {
        var kind = record.Kind == SymbolKind.Unspecified
            ? "symbol"
            : record.Kind.ToString();
        var signature = RepoContextValues.ReadString(record.Signature);
        return string.IsNullOrWhiteSpace(signature)
            ? $"{kind} {record.FullyQualifiedName}"
            : $"{kind} {record.FullyQualifiedName}\n{signature}";
    }

    private static async Task<string?> ReadContentAsync(
        string repoRoot, string relativePath, CancellationToken cancellationToken)
    {
        var fullPath = Path.Combine(repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        try
        {
            var content = await File.ReadAllTextAsync(fullPath, cancellationToken).ConfigureAwait(false);
            return content.Length > MaxEmbedChars ? content[..MaxEmbedChars] : content;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }

    /// <summary>
    /// Builds the list of files to embed: every changed file (its content moved,
    /// so any prior vector is stale) plus every unchanged file that is not yet
    /// covered. The unchanged set heals a vectorise a prior run left incomplete -
    /// the structural digest was committed but the embedding never landed - without
    /// re-embedding the files that already have a vector.
    /// <para>
    /// Coverage is judged from the add-wins membership set (loaded once by the
    /// caller), which holds only 16-character source identifiers and never the
    /// embeddings themselves. A file is covered when it has a real embedding or a
    /// contentless "considered, no passages" marker, so an empty or whitespace-only
    /// file is not re-selected on every reconcile once it has been considered. That
    /// avoids both an existence round-trip per unchanged file and pulling any vector
    /// payload back across the grain boundary.
    /// </para>
    /// </summary>
    private static List<RepoFileEntry> SelectFilesToEmbed(
        string repoId,
        RepoContextEmbeddingCoverage coverage,
        IReadOnlyList<RepoFileEntry> changedFiles,
        IReadOnlyList<RepoFileEntry> unchangedFiles,
        out int gapsSelected)
    {
        var toEmbed = new List<RepoFileEntry>(changedFiles.Count + unchangedFiles.Count);
        toEmbed.AddRange(changedFiles);

        gapsSelected = 0;
        foreach (var file in unchangedFiles)
        {
            var sourceId = VectorCodec.SourceId(RepoContextKeys.File(repoId, file.RelativePath));
            if (!coverage.IsCovered(sourceId))
            {
                toEmbed.Add(file);
                gapsSelected++;
            }
        }

        return toEmbed;
    }

    /// <inheritdoc />
    public async Task RetireAsync(
        string repoId,
        IReadOnlyList<string> removedPaths,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(removedPaths);

        // Retirement only deletes stored records, so it runs regardless of the
        // embedding provider: a file removed while the provider is down must still
        // drop its vector, or the membership count would drift high.
        foreach (var path in removedPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _writer
                .RetireAsync(repoId, RepoContextKeys.File(repoId, path), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A source to embed: the canonical record key its vectors hydrate to, and the
    /// ordered passages (a file's overlapping windows, or a symbol's single
    /// passage) that become its unit vectors.
    /// </summary>
    private readonly record struct EmbeddingSource(string SourceKey, IReadOnlyList<string> Units);
}
