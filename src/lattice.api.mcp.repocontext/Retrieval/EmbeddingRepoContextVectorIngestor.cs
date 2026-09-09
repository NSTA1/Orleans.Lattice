using System.Collections.Concurrent;
using System.IO;
using System.Linq;
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
    /// The most passes the symbol arm will ever skip its gap back-fill after the
    /// vector plane looked saturated. The skip budget doubles with each consecutive
    /// saturated pass (1, 2, 4, ...) and is clamped here, so a plane that stays
    /// saturated is still re-probed regularly rather than abandoned, and a
    /// transiently unlucky pass costs one skipped back-fill.
    /// </summary>
    internal const int MaxSymbolGapScanBackoffPasses = 8;

    /// <summary>
    /// The most passes the file arm will ever skip its gap back-fill after the
    /// vector plane looked saturated, or after its own selection was caught
    /// repeating work the previous pass already landed. Same budget and same
    /// doubling as <see cref="MaxSymbolGapScanBackoffPasses"/>, because it is the
    /// same failure on the other arm (issue #2208).
    /// </summary>
    internal const int MaxFileGapScanBackoffPasses = 8;

    /// <summary>
    /// The ingest arm names carried into every batched embed-and-store log line.
    /// <para>
    /// The three arms share one embed body, so before these existed its warnings
    /// named only the repository. A batch-record failure that repeats every
    /// reconcile then could not be attributed to an arm from a deployed
    /// container's log at all, which is the same defect class as issue #2253: a
    /// message that cannot distinguish the states it is meant to report on.
    /// </para>
    /// </summary>
    internal const string FileArm = "file";

    /// <inheritdoc cref="FileArm"/>
    internal const string SymbolArm = "symbol";

    /// <inheritdoc cref="FileArm"/>
    internal const string MemoryArm = "memory";

    /// <summary>
    /// The stand-in coverage set used when real coverage could not be read, or was
    /// deliberately not read, so a missing probe degrades to "no coverage evidence
    /// this pass" rather than failing the arm. Shared and immutable because it is
    /// only ever read.
    /// </summary>
    private static readonly IReadOnlySet<string> EmptyKeySet =
        new HashSet<string>(StringComparer.Ordinal);

    private readonly RepoContextVectorWriter _writer;
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;
    private readonly IEmbeddingProvider? _embeddingProvider;
    private readonly ILogger<EmbeddingRepoContextVectorIngestor> _logger;

    /// <summary>
    /// The symbol arm's per-repository gap-back-fill backoff, carried across
    /// reconcile passes because the ingestor is a singleton.
    /// <para>
    /// The arm's back-fill is a whole-symbol-space walk with a membership probe per
    /// page, and every symbol it selects costs an embed, a vector store, and a
    /// membership write. When the membership tree is saturated those writes time
    /// out, the symbols stay unmarked, and the next pass selects the very same set -
    /// so the arm drives the failing tree exactly as hard again, and that load is
    /// itself what keeps the writes failing (issue #2071). The batch loop already
    /// refuses to add load <i>within</i> a pass once
    /// <see cref="MaxConsecutiveBatchFailures"/> consecutive batches fail to record;
    /// this is the same rule applied <i>across</i> passes, which is the timescale
    /// the loop actually runs on.
    /// </para>
    /// <para>
    /// A skipped pass still embeds every symbol the reconcile reported as CHANGED -
    /// correctness is never deferred, only the opportunistic back-fill of symbols
    /// that already have vectors and are merely missing a flag. The entry is
    /// removed by the first full pass that completes without saturation, so the arm
    /// returns to normal the moment the plane recovers.
    /// </para>
    /// </summary>
    private readonly ConcurrentDictionary<string, SymbolGapScanBackoff> _symbolGapScanBackoff = new();

    /// <summary>
    /// The source keys the previous pass's gap back-fill embedded AND recorded as
    /// landed, kept per repository so the next pass can tell whether its own
    /// selection is new work or the same work over again.
    /// <para>
    /// This is the arm's real loop detector, and it exists because the obvious one
    /// does not fire. <see cref="MaxConsecutiveBatchFailures"/> watches for batches
    /// that FAIL, but the re-embed loop is built entirely out of batches that
    /// SUCCEED: the embed completes, the vectors store, the membership write
    /// returns, the source is reported landed - and the next pass's probe still
    /// cannot see the flag, because the membership tree is so far beyond its WAL
    /// replay budget that the write is not observable by the time the next pass
    /// asks. Nothing on the failure path ever trips, so the arm re-selects the same
    /// sources forever while believing every pass succeeded (issues #2071, #2078).
    /// </para>
    /// <para>
    /// Re-selecting a source this pass that the LAST pass already landed is
    /// therefore the signature to watch: it means the flag write did not stick, and
    /// no amount of repeating it will help. Backing off then is what breaks the
    /// cycle, because the re-embeds are themselves the write load keeping the tree
    /// from draining - stopping lets replay catch up, which is what makes the flags
    /// observable again.
    /// </para>
    /// </summary>
    private readonly ConcurrentDictionary<string, IReadOnlySet<string>> _lastGapLanded = new();

    /// <summary>
    /// The file arm's per-repository cross-pass gap-selection history, kept because
    /// the ingestor is a singleton. It exists purely to instrument the
    /// never-converging back-fill of issue #2208: on each pass it measures how this
    /// pass's gap selection overlaps the previous pass, how the rolling union of
    /// gap-selected files grows against the walked corpus, and how many files the
    /// previous pass both selected and LANDED are being re-selected now - the file-arm
    /// reading of the symbol arm's re-embed-loop signature. That last measurement is
    /// no longer only a measurement: it is the signal the file arm's gap-scan backoff
    /// consumes (<see cref="_fileGapScanBackoff"/>), because measuring the loop and
    /// then discarding the reading is exactly how the arm went on re-embedding the
    /// same closed pool for 179 consecutive passes.
    /// </summary>
    private readonly ConcurrentDictionary<string, FileGapHistory> _fileGapHistory = new();

    /// <summary>
    /// The file arm's per-repository gap-back-fill backoff, carried across reconcile
    /// passes because the ingestor is a singleton.
    /// <para>
    /// This is the file-arm twin of <see cref="_symbolGapScanBackoff"/>, and it
    /// exists because issue #2208 is issues #2071/#2078 on the other arm. The symbol
    /// arm was given a cross-pass backoff and converged; the file arm was left
    /// without one and did not, so a deployed repository kept re-embedding the same
    /// closed pool of files on every zero-change pass, indefinitely - the rolling
    /// union of its gap selections stayed flat while passes kept entering and
    /// leaving it, which is a set being re-selected rather than fresh loss.
    /// </para>
    /// <para>
    /// The trigger is the same pair as the symbol arm's: a pass that deferred
    /// batches because the plane looked saturated, or a pass whose gap selection
    /// repeats what the previous pass already embedded AND recorded. The second is
    /// the one that fires here, because this loop is built entirely out of batches
    /// that SUCCEED - see <see cref="_lastGapLanded"/>, which explains the mechanism
    /// in full.
    /// </para>
    /// <para>
    /// A skipped pass still embeds every file the reconcile reported as CHANGED.
    /// Only the opportunistic back-fill of unchanged files stands down, and only
    /// until a pass that actually runs it completes without saturation.
    /// </para>
    /// </summary>
    private readonly ConcurrentDictionary<string, FileGapScanBackoff> _fileGapScanBackoff = new();

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
        var gapSelectedFiles = new List<RepoFileEntry>();

        // Claimed before the selection so it governs this whole pass, exactly as the
        // symbol arm claims its own. The coverage probe still runs: it is a bounded
        // point-read, the contentless mark/unmark below needs it, and it is the
        // embed-and-store write load - not the probe - that keeps the membership tree
        // beyond its replay budget. Only the back-fill selection stands down.
        var skipGapScan = ClaimFileGapScanSkip(repoId);
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

        var toEmbed = coverageProbeFailed || skipGapScan
            ? new List<RepoFileEntry>(changedFiles)
            : SelectFilesToEmbed(repoId, coverage, changedFiles, unchangedFiles, out gapsSelected, out gapSelectedFiles);
        if (toEmbed.Count == 0)
        {
            // An early return still has to fold this pass into the backoff, or a
            // repository whose reconcile changes nothing would never clear a budget
            // it was granted, and never accrue one either.
            RecordFileGapScanOutcome(repoId, saturated: false, skippedGapScan: skipGapScan);
            NoteAndLogUnmeasuredGapShape(
                repoId, coverageProbeFailed, skipGapScan, changedFiles.Count, unchangedFiles.Count);
            return new RepoFileVectorIngestOutcome(0, gapsSelected, !coverageProbeFailed, Deferred: false, skipGapScan);
        }

        var sources = new List<EmbeddingSource>(toEmbed.Count);
        List<string>? contentlessToMark = null;
        List<string>? contentfulToUnmark = null;
        List<string>? unreadable = null;
        List<string>? racedWithDeletion = null;
        foreach (var file in toEmbed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sourceKey = RepoContextKeys.File(repoId, file.RelativePath);
            var read = await ReadContentAsync(repoRoot, file.RelativePath, cancellationToken).ConfigureAwait(false);
            var text = read.Text;
            if (text is null)
            {
                if (read.Absent)
                {
                    // Deleted between the walk that enumerated it and the read that
                    // would have embedded it. This needs no retirement path and is not
                    // a fault: the next walk does not enumerate it, so the plan
                    // classifies it removed and it is never offered to the gap sweep
                    // again. Counting it with the unreadable files below would
                    // conflate a race that self-heals in one pass with a fault that
                    // never does, which is the conflation that cost #2208 four rounds
                    // of investigation (issue #2269).
                    (racedWithDeletion ??= new List<string>()).Add(file.RelativePath);
                    continue;
                }

                // A read failure on a file that IS still present - an exclusive lock,
                // a bad sector, or a permission this process does not hold. It is left
                // uncovered so a later pass retries it once the file is readable,
                // rather than marked considered.
                //
                // That retry is right for a TRANSIENT failure and silently wrong for a
                // PERSISTENT one. Such a file is enumerated by every walk, so the
                // always-on gap sweep re-selects it on every pass forever, at zero
                // contention - a permanent gap set that is indistinguishable from a
                // broken presence check or from write-path loss. Naming the files is
                // what separates those cases in the field (issue #2208).
                //
                // Retiring it with a marker is deliberately NOT done here. A
                // permission fault is fixable by an operator and does not change the
                // file's digest, so a retired file would return to the unchanged set,
                // be excluded by its own marker, and never be embedded - trading a
                // loud non-convergence for a silent coverage hole (issue #2269).
                (unreadable ??= new List<string>()).Add(file.RelativePath);
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

        if (racedWithDeletion is not null)
        {
            // Reported separately from the unreadable files, and at Information,
            // because this population needs no action and clears itself: these files
            // are gone, so the next walk does not enumerate them and the gap sweep is
            // never offered them again. Folding them into the warning above would make
            // its count - whose whole diagnostic value is that a REPEATING value means
            // a permanent gap set - rise and fall with ordinary build churn.
            _logger.LogInformation(
                "Repo {RepoId}: {Count} of the {Selected} file(s) selected for embedding were deleted between "
                + "the walk that enumerated them and the read that would have embedded them. They need no "
                + "retry: the next walk does not enumerate them. sample: {Sample}",
                repoId,
                racedWithDeletion.Count,
                toEmbed.Count,
                string.Join(", ", racedWithDeletion.Take(10)));
        }

        if (unreadable is not null)
        {
            // One line per pass, not one per file: a build tree can make hundreds
            // unreadable at once and the count is the signal, not each name.
            _logger.LogWarning(
                "Repo {RepoId}: {Count} of the {Selected} file(s) selected for embedding are still present but "
                + "could not be read, so they stay uncovered and the gap sweep re-selects them on the next pass. "
                + "A count that repeats at the same value across passes is a PERMANENT gap set - files that can "
                + "never be embedded - not a saturated vector plane. Files that were merely deleted mid-pass are "
                + "counted separately and are not included here. sample: {Sample}",
                repoId,
                unreadable.Count,
                toEmbed.Count,
                string.Join(", ", unreadable.Take(10)));
        }

        var stalledGapProgress = false;
        EmbedOutcome embedOutcome;
        try
        {
            embedOutcome = await EmbedAndStoreReportingLandedAsync(repoId, FileArm, sources, onProgress, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A pass that landed NOTHING still throws, so the arm reports incomplete -
            // but it is also the most saturated pass there is, and the backoff has to
            // see it. Recording only the returned outcome would miss exactly the case
            // the re-embed loop shows up in.
            RecordFileGapScanOutcome(repoId, saturated: true, skippedGapScan: skipGapScan);
            throw;
        }

        var embedded = embedOutcome.Landed.Count;

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

        // Measures the shape of the gap selection directly (issue #2208): how it
        // overlaps the previous pass, how its rolling union grows against the walked
        // corpus, and - the sharpest signal - how many files the PREVIOUS pass both
        // selected and LANDED are being re-selected now. That last is the symbol arm's
        // re-embed-loop signature (DetectStalledGapProgress, issues #2071/#2078): a
        // write that returned success but is not observable on the next pass. The file
        // arm now backs off on it, so this call both reports the shape and returns the
        // verdict RecordFileGapScanOutcome below acts on.
        if (gapSelectedFiles.Count > 0)
        {
            stalledGapProgress = await LogGapDiagnosticsAsync(
                repoId,
                gapSelectedFiles,
                embedOutcome.Landed,
                coverage,
                changedFileCount: changedFiles.Count,
                walkedFiles: changedFiles.Count + unchangedFiles.Count,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            NoteAndLogUnmeasuredGapShape(
                repoId, coverageProbeFailed, skipGapScan, changedFiles.Count, unchangedFiles.Count);
        }

        // Fold this pass into the backoff. A pass that deferred batches saw the plane
        // saturated directly; a pass whose selection repeats what the last pass
        // already landed saw the same saturation through its only visible symptom,
        // because this loop is built out of batches that succeed. Either way,
        // re-embedding the same set again cannot help and is itself the write load
        // keeping the membership tree past its replay budget (issue #2208).
        RecordFileGapScanOutcome(
            repoId, embedOutcome.Saturated || stalledGapProgress, skippedGapScan: skipGapScan);

        return new RepoFileVectorIngestOutcome(
            embedded, gapsSelected, !coverageProbeFailed, embedOutcome.Saturated, skipGapScan);
    }

    /// <summary>
    /// Records that a pass measured no gap-set shape, and says which of the three
    /// mutually exclusive reasons produced it.
    /// <para>
    /// The reason is the whole point. An empty gap selection is reached by causes
    /// that mean OPPOSITE things - the probe failed so nothing was attempted, the
    /// probe succeeded and found nothing left to do, or the back-fill was skipped
    /// under backoff and never asked - and a pass that does not name which is
    /// indistinguishable from the others in a deployed container's log. This is the
    /// line an operator reads to tell "quiet because converged" from "quiet because
    /// backed off" (issues #2208, #2253).
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository whose pass measured nothing.</param>
    /// <param name="coverageProbeFailed">Whether the coverage probe failed, so no gap sweep was attempted.</param>
    /// <param name="skippedGapScan">Whether the back-fill was skipped under the gap-scan backoff.</param>
    /// <param name="changedFiles">Files the reconcile reported changed this pass.</param>
    /// <param name="unchangedFiles">Files the reconcile reported unchanged this pass.</param>
    private void NoteAndLogUnmeasuredGapShape(
        string repoId, bool coverageProbeFailed, bool skippedGapScan, int changedFiles, int unchangedFiles)
    {
        // This pass measured no gap shape, so it advances no history. Record that it
        // happened, or the next measured pass compares itself against a pass that is
        // not the preceding one while reporting it as "previous" - and an entrant
        // measured across an unknown number of unmeasured passes is not alarmable,
        // because anything could have happened in the interval (issue #2292). A
        // skipped pass is an unmeasured pass in exactly that sense, so the backoff
        // cannot manufacture a spurious regression warning on the pass that follows
        // it.
        _fileGapHistory.GetOrAdd(repoId, static _ => new FileGapHistory()).NoteUnmeasuredPass();

        var reason = coverageProbeFailed
            ? "the embedding-coverage probe failed, so no gap sweep was attempted and this pass advanced "
              + "the back-fill by nothing. This is NOT convergence"
            : skippedGapScan
                ? "the gap back-fill was SKIPPED under the file-arm backoff, so no gap sweep was attempted "
                  + "and this pass advanced the back-fill by nothing. This is NOT convergence"
                : "the coverage probe succeeded and selected no gap files, so every walked file is already "
                  + "covered or contentless. This IS convergence for the file arm";

        _logger.LogInformation(
            "Repo {RepoId}: back-fill gap set shape not measured this pass: {Reason} (walked={Walked} file(s), "
            + "changed={Changed}, unchanged={Unchanged}, coverageProbe={Probe}, gapScanSkipped={Skipped}).",
            repoId,
            reason,
            changedFiles + unchangedFiles,
            changedFiles,
            unchangedFiles,
            coverageProbeFailed ? "failed" : "succeeded",
            skippedGapScan);
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

        // When the previous pass gave the plane up as saturated, this pass embeds
        // only the symbols the reconcile named as changed and leaves the gap
        // back-fill alone: no membership probe per page, and no re-embed of symbols
        // that already have vectors and are only missing a flag. That is what lets
        // the membership tree drain, so the writes the back-fill needs can finally
        // land instead of the arm re-driving a failing tree every pass forever.
        var skipGapScan = ClaimSymbolGapScanSkip(repoId);
        if (skipGapScan)
        {
            _logger.LogInformation(
                "Repo {RepoId}: the vector plane looked saturated on a recent pass, so this pass embeds only the "
                + "{Changed} changed symbol(s) and defers the gap back-fill to let the membership tree drain.",
                repoId,
                changed.Count);
        }

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var prefix = RepoContextKeys.SymbolsPrefix(repoId);
        var sources = new List<EmbeddingSource>();

        // The sources this pass selected because their flag was missing, as opposed
        // to because the reconcile changed them. Only these can evidence the loop:
        // a changed symbol is legitimately re-embedded every time it changes.
        var gapSelected = new HashSet<string>(StringComparer.Ordinal);

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
            IReadOnlySet<string> embeddedMembers = EmptyKeySet;
            if (!skipGapScan)
            {
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
            }

            foreach (var record in page.Records)
            {
                if (record.Value is null)
                {
                    continue;
                }

                var sourceKey = record.Key;
                var selectedByGapScan = !changed.Contains(sourceKey);
                if (selectedByGapScan
                    && (skipGapScan || embeddedMembers.Contains(VectorCodec.SourceId(sourceKey))))
                {
                    continue;
                }

                var text = BuildSymbolText(_serializer.Deserialize<SymbolRecord>(record.Value));
                if (string.IsNullOrWhiteSpace(text))
                {
                    continue;
                }

                if (selectedByGapScan)
                {
                    gapSelected.Add(sourceKey);
                }

                sources.Add(new EmbeddingSource(sourceKey, new[] { text }));
            }

            token = page.HasMore ? page.ContinuationToken : null;
        }
        while (token is not null);

        EmbedOutcome outcome;
        try
        {
            outcome = await EmbedAndStoreReportingLandedAsync(repoId, SymbolArm, sources, onProgress: null, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A pass that landed NOTHING still throws, so the arm reports
            // incomplete - but it is also the most saturated pass there is, and the
            // backoff has to see it. Recording only the returned outcome would miss
            // exactly the case the re-embed loop actually shows up in.
            RecordSymbolGapScanOutcome(repoId, saturated: true, skippedGapScan: skipGapScan);
            throw;
        }

        var symbolsEmbedded = outcome.Landed.Count;

        // A pass whose gap selection repeats what the last pass already landed is
        // the re-embed loop, however successful each individual batch looked.
        var repeated = DetectStalledGapProgress(repoId, gapSelected, skipGapScan);
        RecordSymbolGapScanOutcome(repoId, outcome.Saturated || repeated, skipGapScan);

        // Remember only what THIS pass both selected via the gap scan and landed, so
        // the next pass compares against work that genuinely reported success. A
        // skipped pass ran no gap scan and must not overwrite the record, or the
        // evidence of the loop would be erased by the backoff that detected it.
        if (!skipGapScan)
        {
            var landedFromGap = new HashSet<string>(StringComparer.Ordinal);
            foreach (var sourceKey in outcome.Landed)
            {
                if (gapSelected.Contains(sourceKey))
                {
                    landedFromGap.Add(sourceKey);
                }
            }

            _lastGapLanded[repoId] = landedFromGap;
        }

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
    /// Reports whether this pass's gap selection repeats work the previous pass
    /// already landed, which is the signature of the re-embed loop.
    /// <para>
    /// A source the last pass embedded, stored, and recorded membership for should
    /// not appear in this pass's gap selection at all - its flag is supposed to be
    /// visible now. When a substantial share of them reappear, the membership
    /// writes are not becoming observable and repeating them cannot help, so the
    /// arm treats it exactly like saturation and stands down for a few passes.
    /// </para>
    /// <para>
    /// The threshold is a majority rather than any single repeat, because a handful
    /// of legitimate stragglers (a write that raced this pass's probe, a source
    /// re-changed in between) must not be mistaken for the loop. The loop shows up
    /// as nearly the whole set returning, pass after pass.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository whose pass is being judged.</param>
    /// <param name="gapSelected">The source keys this pass selected because their flag was missing.</param>
    /// <param name="skippedGapScan">Whether this pass skipped its gap scan, in which case it carries no evidence.</param>
    /// <returns><see langword="true"/> when the selection repeats the previous pass's landed work.</returns>
    private bool DetectStalledGapProgress(
        string repoId, HashSet<string> gapSelected, bool skippedGapScan)
    {
        // A skipped pass never ran the selection, so an empty set is an artefact of
        // the backoff rather than evidence about the plane.
        if (skippedGapScan
            || gapSelected.Count == 0
            || !_lastGapLanded.TryGetValue(repoId, out var previouslyLanded)
            || previouslyLanded.Count == 0)
        {
            return false;
        }

        var repeats = 0;
        foreach (var sourceKey in previouslyLanded)
        {
            if (gapSelected.Contains(sourceKey))
            {
                repeats++;
            }
        }

        if (repeats * 2 < previouslyLanded.Count)
        {
            return false;
        }

        _logger.LogWarning(
            "Repo {RepoId}: {Repeats} of the {Landed} symbol(s) the previous pass embedded AND recorded are being "
            + "selected again, so the membership writes are not becoming observable and re-embedding them cannot "
            + "help. Treating this as a saturated plane and standing the gap back-fill down.",
            repoId,
            repeats,
            previouslyLanded.Count);

        return true;
    }

    /// <summary>
    /// Consumes one pass of the symbol arm's gap-back-fill skip budget, if any is
    /// outstanding, and reports whether this pass should skip the back-fill.
    /// </summary>
    /// <param name="repoId">The repository about to run its symbol arm.</param>
    /// <returns><see langword="true"/> when this pass must embed only changed symbols.</returns>
    private bool ClaimSymbolGapScanSkip(string repoId)
    {
        if (!_symbolGapScanBackoff.TryGetValue(repoId, out var backoff) || backoff.Remaining <= 0)
        {
            return false;
        }

        // A concurrent pass may have consumed the same budget; either outcome is
        // sound, so a single compare-and-swap attempt is enough - a lost race just
        // means the other pass took the skip and this one does the back-fill.
        var next = backoff with { Remaining = backoff.Remaining - 1 };
        return _symbolGapScanBackoff.TryUpdate(repoId, next, backoff);
    }

    /// <summary>
    /// Folds one symbol-arm pass into the gap-back-fill backoff: a saturated pass
    /// doubles the skip budget (clamped by <see cref="MaxSymbolGapScanBackoffPasses"/>),
    /// while a clean pass that actually ran the back-fill clears it outright.
    /// </summary>
    /// <param name="repoId">The repository whose pass just finished.</param>
    /// <param name="saturated">Whether the pass deferred batches because the vector plane looked saturated.</param>
    /// <param name="skippedGapScan">Whether the pass skipped the gap back-fill, so it is no evidence the plane recovered.</param>
    private void RecordSymbolGapScanOutcome(string repoId, bool saturated, bool skippedGapScan)
    {
        if (saturated)
        {
            var updated = _symbolGapScanBackoff.AddOrUpdate(
                repoId,
                _ => new SymbolGapScanBackoff(Remaining: 1, Streak: 1),
                (_, current) =>
                {
                    var streak = Math.Min(current.Streak + 1, 30);
                    var budget = Math.Min(1 << Math.Min(streak - 1, 30), MaxSymbolGapScanBackoffPasses);
                    return new SymbolGapScanBackoff(budget, streak);
                });

            _logger.LogWarning(
                "Repo {RepoId}: the symbol arm deferred batches because the vector plane looked saturated "
                + "(consecutive saturated passes: {Streak}); skipping the gap back-fill for the next {Passes} pass(es) "
                + "so the membership tree can drain. Changed symbols are still embedded meanwhile.",
                repoId,
                updated.Streak,
                updated.Remaining);
            return;
        }

        // Only a pass that actually ran the back-fill is evidence the plane
        // recovered; a skipped pass never touched the membership tree hard enough
        // to find out, so it must not clear the budget it was granted by.
        if (!skippedGapScan && _symbolGapScanBackoff.TryRemove(repoId, out _))
        {
            _logger.LogInformation(
                "Repo {RepoId}: the symbol arm completed a full gap back-fill without saturation; backoff cleared.",
                repoId);
        }
    }

    /// <summary>
    /// The symbol arm's outstanding gap-back-fill skip budget for one repository.
    /// </summary>
    /// <param name="Remaining">How many further passes must skip the back-fill.</param>
    /// <param name="Streak">Consecutive saturated passes, which sets the next budget.</param>
    private readonly record struct SymbolGapScanBackoff(int Remaining, int Streak);

    /// <summary>
    /// Consumes one pass of the file arm's gap-back-fill skip budget, if any is
    /// outstanding, and reports whether this pass should skip the back-fill.
    /// </summary>
    /// <param name="repoId">The repository about to run its file arm.</param>
    /// <returns><see langword="true"/> when this pass must embed only changed files.</returns>
    private bool ClaimFileGapScanSkip(string repoId)
    {
        if (!_fileGapScanBackoff.TryGetValue(repoId, out var backoff) || backoff.Remaining <= 0)
        {
            return false;
        }

        // A concurrent pass may have consumed the same budget; either outcome is
        // sound, so a single compare-and-swap attempt is enough - a lost race just
        // means the other pass took the skip and this one does the back-fill.
        var next = backoff with { Remaining = backoff.Remaining - 1 };
        return _fileGapScanBackoff.TryUpdate(repoId, next, backoff);
    }

    /// <summary>
    /// Folds one file-arm pass into the gap-back-fill backoff: a saturated pass
    /// doubles the skip budget (clamped by <see cref="MaxFileGapScanBackoffPasses"/>),
    /// while a clean pass that actually ran the back-fill clears it outright.
    /// </summary>
    /// <param name="repoId">The repository whose pass just finished.</param>
    /// <param name="saturated">Whether the pass deferred batches, or re-selected work the previous pass already landed.</param>
    /// <param name="skippedGapScan">Whether the pass skipped the gap back-fill, so it is no evidence the plane recovered.</param>
    private void RecordFileGapScanOutcome(string repoId, bool saturated, bool skippedGapScan)
    {
        if (saturated)
        {
            var updated = _fileGapScanBackoff.AddOrUpdate(
                repoId,
                _ => new FileGapScanBackoff(Remaining: 1, Streak: 1),
                (_, current) =>
                {
                    var streak = Math.Min(current.Streak + 1, 30);
                    var budget = Math.Min(1 << Math.Min(streak - 1, 30), MaxFileGapScanBackoffPasses);
                    return new FileGapScanBackoff(budget, streak);
                });

            _logger.LogWarning(
                "Repo {RepoId}: the file arm's gap back-fill is not making progress against the vector plane "
                + "(consecutive saturated passes: {Streak}); skipping the gap back-fill for the next {Passes} pass(es) "
                + "so the membership tree can drain. Changed files are still embedded meanwhile (issue #2208).",
                repoId,
                updated.Streak,
                updated.Remaining);
            return;
        }

        // Only a pass that actually ran the back-fill is evidence the plane
        // recovered; a skipped pass never touched the membership tree hard enough to
        // find out, so it must not clear the budget it was granted by. Without this
        // guard the first skip would fake recovery and the backoff would disarm
        // itself, leaving the loop exactly as it was.
        if (!skippedGapScan && _fileGapScanBackoff.TryRemove(repoId, out _))
        {
            _logger.LogInformation(
                "Repo {RepoId}: the file arm completed a full gap back-fill without saturation; backoff cleared.",
                repoId);
        }
    }

    /// <summary>
    /// The file arm's outstanding gap-back-fill skip budget for one repository, or
    /// zero when the arm is running its back-fill normally.
    /// <para>
    /// Exposed so a convergence test can assert that a quiet pass reached zero
    /// selected gaps by SCANNING and finding none, rather than by never scanning.
    /// Without it, <c>GapsSelected == 0</c> stops discriminating between those two
    /// states the moment this arm can skip, and the acceptance tests for issue #2208
    /// would pass while the very regression they exist to catch was fully masked.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository to report on.</param>
    /// <returns>The number of further passes that will skip the gap back-fill.</returns>
    internal int FileGapScanBackoffRemaining(string repoId)
        => _fileGapScanBackoff.TryGetValue(repoId, out var backoff) ? backoff.Remaining : 0;

    /// <summary>
    /// The file arm's outstanding gap-back-fill skip budget for one repository.
    /// </summary>
    /// <param name="Remaining">How many further passes must skip the back-fill.</param>
    /// <param name="Streak">Consecutive saturated passes, which sets the next budget.</param>
    private readonly record struct FileGapScanBackoff(int Remaining, int Streak);

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
        // The load is itself a range scan over the membership tree, so it is one
        // more thing that can fail under the very pressure it exists to tolerate.
        // The load walks the marker range in small, resumable pages: a page fault
        // banks the pages already read and resumes from them next pass, so the walk
        // completes within a bounded number of passes instead of restarting from
        // the beginning and never finishing (issue #2071). The two halves of the
        // result are used differently, which is why they are reported separately -
        // the partial keys are always safe as a skip signal (a marker is only ever
        // written after its vectors landed), but only a COMPLETE set may drive the
        // orphan sweep, or an unread page would look like a retired entry.
        var markers = await _writer
            .LoadEmbeddedMemoryKeysAsync(repoId, cancellationToken)
            .ConfigureAwait(false);
        var recordedMemoryKeys = markers.Keys;
        if (!markers.Complete)
        {
            _logger.LogWarning(
                markers.Fault,
                "Repo {RepoId}: the embedded-memory-key marker scan did not finish this pass; using the "
                + "{Count} marker(s) banked so far as a skip signal, resuming the walk on the next reconcile, "
                + "and deferring the orphan sweep until the set is complete. Passes so far: {Passes}.",
                repoId,
                recordedMemoryKeys.Count,
                markers.Passes);
        }
        else
        {
            // Logged deliberately, and at information rather than debug: the
            // failure this fix addresses shows up as the scan NEVER completing, and
            // "the warning stopped" is a much weaker signal than "the range was
            // exhausted", because the warning also stops if the scan is never
            // reached at all. The pass count distinguishes a walk that resumed
            // banked progress from one that happened to finish in a single call.
            _logger.LogInformation(
                "Repo {RepoId}: the embedded-memory-key marker scan exhausted the range after {Passes} pass(es), "
                + "recording {Count} marker(s); the orphan sweep can run.",
                repoId,
                markers.Passes,
                recordedMemoryKeys.Count);
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
                embeddedMembers = EmptyKeySet;
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

        // The sweep retires what is recorded but no longer live, so it may only run
        // on a COMPLETE recorded set: an unread page is indistinguishable from a
        // retired entry, and an incomplete set simply means the walk resumes next
        // pass. Declining to sweep is the safe direction - it retires nothing
        // rather than risking a live embedding.
        if (markers.Complete)
        {
            await SweepOrphanedMemoryVectorsAsync(repoId, liveKeys, recordedMemoryKeys, cancellationToken)
                .ConfigureAwait(false);
        }

        var landed = (await EmbedAndStoreReportingLandedAsync(repoId, MemoryArm, sources, onProgress: null, cancellationToken)
            .ConfigureAwait(false)).Landed;

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
    /// <para>
    /// It reports <b>which</b> sources actually landed rather than only how many.
    /// A caller that records a marker per source needs the identities, not a
    /// count: marking a source whose batch failed would assert an embedding that
    /// does not exist. The memory arm's embedded-key marker is exactly that kind
    /// of caller - it is what makes the orphan set (recorded - live) computable -
    /// so a false entry there is a phantom record, not merely a wasted write.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository being embedded.</param>
    /// <param name="arm">
    /// Which ingest arm is embedding, one of <see cref="FileArm"/>,
    /// <see cref="SymbolArm"/>, or <see cref="MemoryArm"/>. Every warning this
    /// method raises names it. All three arms share this one body, so a warning
    /// that reported only the repository was unattributable in a deployed
    /// container: the batch-record failure repeats every reconcile and an operator
    /// could not tell which arm was failing, which is the same "the log cannot
    /// distinguish two different states" defect issue #2253 was filed over.
    /// </param>
    /// <param name="sources">The sources to embed.</param>
    /// <param name="onProgress">Optional incremental progress callback.</param>
    /// <param name="cancellationToken">Cancels the pass.</param>
    /// <returns>The source keys whose vectors were stored and whose membership was recorded, and whether the vector plane looked saturated.</returns>
    private async Task<EmbedOutcome> EmbedAndStoreReportingLandedAsync(
        string repoId,
        string arm,
        IReadOnlyList<EmbeddingSource> sources,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken)
    {
        var landed = new List<string>();
        if (sources.Count == 0)
        {
            return new EmbedOutcome(landed, Saturated: false);
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
        var saturated = false;
        Exception? firstBatchFailure = null;
        var pendingMembers = new List<string>();

        // Naming a failed batch's sources is what separates a deterministic write
        // fault - a key range served by a permanently stalled leaf, say - from
        // ordinary contention that will drain, and that ambiguity is what left the
        // never-converging back-fill unattributed for four rounds of investigation
        // (issue #2208). Both failure paths need it, so both compute it the same way.
        // Units are contiguous per source, so comparing against the last key
        // deduplicates without a set.
        List<string> NameBatchSources(int from, int length)
        {
            var batchSources = new List<string>();
            for (var i = 0; i < length; i++)
            {
                var ownerKey = sources[unitOwner[from + i]].SourceKey;
                if (batchSources.Count == 0 || !string.Equals(batchSources[^1], ownerKey, StringComparison.Ordinal))
                {
                    batchSources.Add(ownerKey);
                }
            }

            return batchSources;
        }

        // Consecutive failures mean the vector plane is saturated, not that one batch
        // was unlucky. Driving the remaining batches into it adds load to a store that
        // is already failing and lands nothing, so the arm stops here and lets the next
        // reconcile retry from a quieter store. Whatever already landed is kept, and
        // every deferred source is simply unmarked, so the next pass picks it up.
        // The stage word keeps the two failure kinds distinguishable in the log while
        // rendering the record case exactly as it did before.
        void ReportSaturationDeferral(int from, int length, string stage)
        {
            var deferred = unitTexts.Count - (from + length);
            _logger.LogWarning(
                "Repo {RepoId}: {Failures} consecutive {Arm}-arm batches failed to {Stage}; the vector plane "
                + "looks saturated, so deferring the remaining {Deferred} passage(s) to the next reconcile "
                + "rather than adding load.",
                repoId,
                consecutiveBatchFailures,
                arm,
                stage,
                deferred < 0 ? 0 : deferred);
        }

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
                // An embedding call that fails strands exactly the sources carrying a
                // unit in this batch: none of them reaches a full slot set, so none
                // completes, none has its membership recorded, and the always-on gap
                // sweep re-selects every one of them on every later pass. That is the
                // same loss a record failure causes, so it is accounted the same way.
                // Before this it incremented neither counter, so it could not trip the
                // saturation break however many times it fired, and it was logged at
                // Information - which made a partial failure silent, since the batches
                // that did land suppress the arm's "no embedding batch succeeded" line
                // and leave a healthy-looking outcome behind (issue #2272).
                var failedSources = NameBatchSources(start, count);
                failedBatches++;
                consecutiveBatchFailures++;

                _logger.LogWarning(
                    "Repo {RepoId}: the {Arm} arm could not embed a batch of {Count} passage(s) spanning "
                    + "{Sources} source(s): the embedding call did not succeed ({Error}). They stay unmarked "
                    + "and are retried on the next reconcile. sample: {Sample}",
                    repoId,
                    arm,
                    count,
                    failedSources.Count,
                    result.Error ?? "no vectors returned",
                    string.Join(", ", failedSources.Take(6)));

                if (consecutiveBatchFailures >= MaxConsecutiveBatchFailures)
                {
                    saturated = true;
                    ReportSaturationDeferral(start, count, "embed");
                    break;
                }

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

                // Name the batch's sources, so a residue that persists across passes can
                // be told apart from contention that will drain (see NameBatchSources).
                var batchSources = NameBatchSources(start, count);

                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: the {Arm} arm could not record a batch of {Count} passage(s) spanning "
                    + "{Sources} source(s); they stay unmarked and are retried on the next reconcile. Continuing "
                    + "with the remaining batches. sample: {Sample}",
                    repoId,
                    arm,
                    count,
                    batchSources.Count,
                    string.Join(", ", batchSources.Take(6)));

                if (consecutiveBatchFailures >= MaxConsecutiveBatchFailures)
                {
                    saturated = true;
                    ReportSaturationDeferral(start, count, "record");
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
                "Repo {RepoId}: every one of the {Failed} {Arm}-arm batch(es) that produced vectors failed to "
                + "record them; surfacing the first fault so the arm reports incomplete.",
                repoId,
                failedBatches,
                arm);
            throw firstBatchFailure;
        }

        return new EmbedOutcome(landed, saturated);
    }

    /// <summary>
    /// What one batched embed-and-store pass achieved: the sources that genuinely
    /// landed (vectors stored <i>and</i> membership recorded), and whether the pass
    /// gave up early because the vector plane looked saturated.
    /// </summary>
    /// <param name="Landed">The source keys whose vectors were stored and whose membership was recorded.</param>
    /// <param name="Saturated">
    /// <see langword="true"/> when the pass hit
    /// <see cref="MaxConsecutiveBatchFailures"/> consecutive record failures and
    /// deferred its remaining batches. It is the arm's one deterministic saturation
    /// signal, and the caller uses it to decide whether to drive the same tree
    /// again on the next pass.
    /// </param>
    private readonly record struct EmbedOutcome(List<string> Landed, bool Saturated);

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

    /// <summary>
    /// What reading a selected file's content produced. The absent case is kept
    /// distinct from the unreadable case because their futures differ completely: a
    /// file that has been deleted is not enumerated by the next walk, so it is
    /// classified removed and never offered to the gap sweep again, while a file that
    /// is still present but cannot be read is offered by every walk and re-selected
    /// on every pass (issue #2269). Collapsing both to null made a self-healing race
    /// indistinguishable from a permanent fault.
    /// </summary>
    /// <param name="Text">The content read, truncated to the embedding limit, or
    /// <see langword="null"/> when the read did not succeed.</param>
    /// <param name="Absent">Whether the read failed because the file was no longer
    /// there, as opposed to being present and unreadable.</param>
    private readonly record struct FileReadResult(string? Text, bool Absent)
    {
        /// <summary>The file is still present but could not be read.</summary>
        public static FileReadResult Unreadable { get; } = new(null, Absent: false);

        /// <summary>The file was gone by the time the read reached it.</summary>
        public static FileReadResult Missing { get; } = new(null, Absent: true);
    }

    private static async Task<FileReadResult> ReadContentAsync(
        string repoRoot, string relativePath, CancellationToken cancellationToken)
    {
        var fullPath = Path.Combine(repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        try
        {
            var content = await File.ReadAllTextAsync(fullPath, cancellationToken).ConfigureAwait(false);
            return new FileReadResult(
                content.Length > MaxEmbedChars ? content[..MaxEmbedChars] : content,
                Absent: false);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            // Both derive from IOException, so this filter has to precede the
            // IOException arm below or a deleted file would be reported as a fault.
            return FileReadResult.Missing;
        }
        catch (IOException)
        {
            return FileReadResult.Unreadable;
        }
        catch (UnauthorizedAccessException)
        {
            return FileReadResult.Unreadable;
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
        out int gapsSelected,
        out List<RepoFileEntry> gapSelectedFiles)
    {
        var toEmbed = new List<RepoFileEntry>(changedFiles.Count + unchangedFiles.Count);
        toEmbed.AddRange(changedFiles);

        gapsSelected = 0;
        gapSelectedFiles = new List<RepoFileEntry>();
        foreach (var file in unchangedFiles)
        {
            var sourceId = VectorCodec.SourceId(RepoContextKeys.File(repoId, file.RelativePath));
            if (!coverage.IsCovered(sourceId))
            {
                toEmbed.Add(file);
                gapSelectedFiles.Add(file);
                gapsSelected++;
            }
        }

        return toEmbed;
    }

    /// <summary>
    /// The order-independent set-digest of the unchanged files the back-fill
    /// selected as uncovered gaps: the XOR of each file's 64-bit
    /// <see cref="VectorCodec.SourceId"/>, so the digest depends on <i>which</i> files
    /// were selected and not on the order they were walked. Two consecutive quiet
    /// passes that re-select the same set produce the same digest; a rotating set
    /// produces a changing one. This is the identity the flat gap <i>count</i> in the
    /// pass log cannot supply, and it is what discriminates a broken presence check
    /// (same set every pass) from ongoing vector loss (a changing set) from the logs
    /// alone. XOR means a file present an even number of times cancels, which the gap
    /// selection never produces (each unchanged file is considered once), so the
    /// digest is a faithful set fingerprint here.
    /// </summary>
    internal static ulong GapSetDigest(string repoId, IReadOnlyList<RepoFileEntry> gapSelectedFiles)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(gapSelectedFiles);

        ulong digest = 0;
        foreach (var file in gapSelectedFiles)
        {
            var sourceId = VectorCodec.SourceId(RepoContextKeys.File(repoId, file.RelativePath));
            digest ^= Convert.ToUInt64(sourceId, 16);
        }

        return digest;
    }

    /// <summary>
    /// Which arm of the built-in perturbation control a pass's symptom count belongs
    /// to, determined by whether the <i>previous</i> pass ran the coverage read-back.
    /// The read-back is the only store touch the instrumentation adds, so bucketing
    /// each pass's gap count by whether it was preceded by a read-back measures the
    /// read-back's own effect on the next pass rather than assuming it away (issue
    /// #2208).
    /// </summary>
    internal enum GapReadBackArm
    {
        /// <summary>The first observed pass: it has no predecessor, so it labels neither arm.</summary>
        Seed,

        /// <summary>Arm A - the previous pass ran the read-back, so this pass's probe met a warmed grain.</summary>
        PriorReadBackRan,

        /// <summary>Arm B - the previous pass skipped the read-back, so this pass's probe met whatever state the inter-pass gap left.</summary>
        PriorReadBackSkipped,
    }

    /// <summary>
    /// The parity rule for the alternating coverage read-back: it runs on
    /// even-numbered passes (2nd, 4th, ...) and is skipped on odd ones. Alternating it
    /// turns the instrument into its own two-arm control - a pass preceded by a
    /// read-back versus one that was not - so the read-back's effect on the next pass's
    /// observability is measured, not argued. A pure function of the 1-based pass
    /// ordinal so it is deterministic and unit-testable.
    /// </summary>
    internal static bool GapReadBackRunsOnPass(int passOrdinal) => (passOrdinal % 2) == 0;

    /// <summary>
    /// Classifies a pass's symptom count into the perturbation-control arm defined by
    /// whether its <i>predecessor</i> ran the read-back, following
    /// <see cref="GapReadBackRunsOnPass"/>. The first pass has no predecessor and is
    /// <see cref="GapReadBackArm.Seed"/>. A pure function of the 1-based pass ordinal.
    /// </summary>
    internal static GapReadBackArm ClassifyGapReadBackArm(int passOrdinal)
        => passOrdinal < 2
            ? GapReadBackArm.Seed
            : GapReadBackRunsOnPass(passOrdinal - 1)
                ? GapReadBackArm.PriorReadBackRan
                : GapReadBackArm.PriorReadBackSkipped;

    /// <summary>
    /// The most per-shard groups the shard-distribution line enumerates. The group
    /// count is already bounded by <c>min(K, P)</c>, and <c>P</c> is itself bounded by
    /// <see cref="LatticeOptions.MaxPhysicalShardsPerTree"/>, so this is a second,
    /// unconditional ceiling rather than the only one: a pathological gap set over a
    /// pathological shard space cannot flood the log however both move. The line
    /// always reports <see cref="GapShardDistribution.DistinctShards"/> alongside, so
    /// a truncated enumeration is visibly truncated and the totals stay complete.
    /// </summary>
    internal const int MaxReportedGapShardGroups = 64;

    /// <summary>
    /// The most individual sources the shard-distribution line names with their
    /// resolved shard. This is the hand-checking arm - it exists so the derivation can
    /// be reproduced offline from the log for a few members - not the measurement,
    /// which is the whole-set histogram and the two statistics beside it.
    /// </summary>
    internal const int MaxReportedGapShardSources = 16;

    /// <summary>
    /// How a gap set distributes over the membership tree's <b>physical</b> shards,
    /// with the two statistics issue #2287 pre-registers as the discriminator between
    /// its surviving candidates.
    /// <para>
    /// <b>Why physical and not virtual.</b> Routing is two-stage: a key hashes into one
    /// of <see cref="VirtualShardCount"/> virtual slots (4096 by default), and the
    /// map sends that slot to a physical shard. With ~4096 slots over ~64 shards about
    /// 64 virtual slots land on each physical shard, so keys sharing one <i>physical</i>
    /// shard still occupy distinct <i>virtual</i> slots. A virtual-slot histogram
    /// therefore reads as "scattered" under <b>both</b> candidates and discriminates
    /// nothing; reporting its null as evidence against the hash-partitioned-subset
    /// candidate would be a false refutation. Only <see cref="ShardMap.Resolve"/>,
    /// which applies both stages, answers the question that was asked.
    /// </para>
    /// <para>
    /// <b>What the two statistics are for.</b> <see cref="DistinctShards"/> (D) and
    /// <see cref="LargestShardGroup"/> (M) are compared against a null simulated by
    /// drawing <see cref="Sources"/> keys from the same population the gap set was
    /// drawn from, and the candidate is judged on a quantile of that null. They are
    /// deliberately <i>not</i> compared against an intuition about the gap-set size:
    /// independent hashing of K=43 keys over P=63 shards occupies about 31 distinct
    /// shards, not 43, so "far fewer than 43" fires when nothing is wrong. The
    /// occupancy expectation is <c>E[D] = P * (1 - (1 - 1/P)^K)</c>, and this record
    /// reports every term of it - K as <see cref="Sources"/> and P as
    /// <see cref="PhysicalShardCount"/> - so the threshold is recomputed from the log
    /// rather than assumed.
    /// </para>
    /// </summary>
    /// <param name="Sources">K: gap sources whose membership key resolved to a shard. The whole gap set, not a prefix.</param>
    /// <param name="DistinctShards">D: distinct physical shards the gap set occupies.</param>
    /// <param name="LargestShardGroup">M: how many gap sources share the most-occupied single physical shard.</param>
    /// <param name="PhysicalShardCount">P: distinct physical shards the tree's map references, so the null is parameterised from the tree rather than assumed.</param>
    /// <param name="VirtualShardCount">The map's virtual slot count, which makes the virtual-to-physical fan-out readable from the log.</param>
    /// <param name="MapVersion">The map version the assignments were computed against; a change between passes invalidates a cross-pass comparison.</param>
    /// <param name="ReportedGroups">How many of the <paramref name="DistinctShards"/> groups the detail string enumerates.</param>
    /// <param name="GroupDetail">Bounded <c>shard:count</c> enumeration, densest first then by ascending shard index.</param>
    /// <param name="ReportedSources">How many of the <paramref name="Sources"/> the per-source detail names.</param>
    /// <param name="SourceDetail">Bounded <c>path=shard</c> enumeration for offline hand-checking, in ordinal path order.</param>
    internal readonly record struct GapShardDistribution(
        int Sources,
        int DistinctShards,
        int LargestShardGroup,
        int PhysicalShardCount,
        int VirtualShardCount,
        long MapVersion,
        int ReportedGroups,
        string GroupDetail,
        int ReportedSources,
        string SourceDetail);

    /// <summary>
    /// Resolves every gap source's membership key to its physical shard through
    /// <paramref name="map"/> and summarises the distribution. Pure, deterministic,
    /// and allocation-bounded, so the statistics are unit-testable without a silo.
    /// <para>
    /// The scan is over the <b>whole</b> <paramref name="gapSelectedFiles"/> set:
    /// the counting arm never truncates, because D and M are the measurement and a
    /// prefix would silently bias both downward. Only the two human-readable detail
    /// strings are capped, and each reports the total it was drawn from, so
    /// truncation can never be mistaken for a complete enumeration.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository whose gap set is being summarised.</param>
    /// <param name="gapSelectedFiles">This pass's whole gap selection.</param>
    /// <param name="map">The membership tree's effective shard map, as one snapshot for every assignment.</param>
    /// <param name="maxReportedGroups">Ceiling on the enumerated per-shard groups.</param>
    /// <param name="maxReportedSources">Ceiling on the enumerated per-source assignments.</param>
    internal static GapShardDistribution SummariseGapShardDistribution(
        string repoId,
        IReadOnlyList<RepoFileEntry> gapSelectedFiles,
        ShardMap map,
        int maxReportedGroups = MaxReportedGapShardGroups,
        int maxReportedSources = MaxReportedGapShardSources)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(gapSelectedFiles);
        ArgumentNullException.ThrowIfNull(map);

        // One snapshot for every assignment. The map is resolved once by the caller and
        // never re-read inside this loop, so a remap landing mid-pass cannot split the
        // set across two mappings: the assignments are atomic with respect to the map
        // by construction, which is stronger than observing MapVersion either side and
        // discarding the result when it moved. MapVersion is still reported, because a
        // remap BETWEEN passes is what invalidates a cross-pass comparison.
        var perShard = new Dictionary<int, int>();
        var assignments = new List<(string Path, int Shard)>(
            Math.Min(gapSelectedFiles.Count, Math.Max(0, maxReportedSources)));

        var resolved = 0;
        foreach (var file in gapSelectedFiles)
        {
            var membershipKey = RepoContextKeys.VectorMembership(
                repoId, VectorCodec.SourceId(RepoContextKeys.File(repoId, file.RelativePath)));
            var shard = map.Resolve(membershipKey);

            resolved++;
            perShard[shard] = perShard.TryGetValue(shard, out var seen) ? seen + 1 : 1;

            if (assignments.Count < maxReportedSources)
            {
                assignments.Add((file.RelativePath, shard));
            }
        }

        var largest = 0;
        foreach (var count in perShard.Values)
        {
            if (count > largest) largest = count;
        }

        var groups = perShard
            .OrderByDescending(static pair => pair.Value)
            .ThenBy(static pair => pair.Key)
            .Take(Math.Max(0, maxReportedGroups))
            .Select(static pair => $"{pair.Key}:{pair.Value}")
            .ToArray();

        var sources = assignments
            .OrderBy(static a => a.Path, StringComparer.Ordinal)
            .Select(static a => $"{a.Path}={a.Shard}")
            .ToArray();

        return new GapShardDistribution(
            Sources: resolved,
            DistinctShards: perShard.Count,
            LargestShardGroup: largest,
            PhysicalShardCount: map.GetPhysicalShardIndices().Count,
            VirtualShardCount: map.VirtualShardCount,
            MapVersion: map.Version,
            ReportedGroups: groups.Length,
            GroupDetail: string.Join(", ", groups),
            ReportedSources: sources.Length,
            SourceDetail: string.Join(", ", sources));
    }

    /// <summary>
    /// Emits the physical-shard distribution of this pass's gap set (issue #2287), so
    /// the question "is the stranded set hash-partitioned onto particular shards, or
    /// spread proportionally?" is answerable from a deployed container's log without
    /// re-deriving anything offline.
    /// <para>
    /// <b>Why this cannot perturb the control arm it sits beside.</b> The only call it
    /// makes is <see cref="ILattice.GetRoutingAsync(CancellationToken)"/>, which
    /// resolves the tree alias and shard map from the registry tree and is served from
    /// the activation's own cache after the first call. It reads no membership entry
    /// and touches no membership leaf, so it cannot warm the coverage read path that
    /// the alternating read-back probe measures. It therefore runs on <b>every</b>
    /// pass, including the arm-B passes that deliberately skip the read-back, and the
    /// A/B comparison stays valid.
    /// </para>
    /// <para>
    /// Best-effort and self-contained: a routing failure is logged and swallowed here
    /// rather than propagating, because the caller's remaining work includes the
    /// read-back durability signal and losing that to an unrelated failure would cost
    /// the control arm a data point on every affected pass.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository whose gap set is being measured.</param>
    /// <param name="gapSelectedFiles">This pass's whole gap selection.</param>
    /// <param name="cancellationToken">Cancels the routing resolve.</param>
    private async Task LogGapShardDistributionAsync(
        string repoId,
        IReadOnlyList<RepoFileEntry> gapSelectedFiles,
        CancellationToken cancellationToken)
    {
        try
        {
            var routing = await _grainFactory
                .GetGrain<ILattice>(RepoContextTrees.VectorMembership)
                .GetRoutingAsync(cancellationToken)
                .ConfigureAwait(false);

            var distribution = SummariseGapShardDistribution(repoId, gapSelectedFiles, routing.Map);

            _logger.LogInformation(
                "Repo {RepoId}: back-fill gap set physical-shard distribution over tree {Tree} "
                + "(physicalTreeId={PhysicalTreeId}, mapVersion={MapVersion}). K={Sources} gap source(s) over "
                + "P={PhysicalShardCount} physical shard(s) (virtualSlots={VirtualShardCount}, so ~{FanOut} "
                + "virtual slot(s) per physical shard - a VIRTUAL-slot histogram would read as scattered under "
                + "either candidate and must not be substituted for this one). D={DistinctShards} distinct "
                + "shard(s) occupied, M={LargestShardGroup} in the largest single-shard group. Compare D and M "
                + "against a null simulated by drawing K keys from THIS pass's unchanged population and reject on "
                + "a quantile of that null, not against a bare percentage: independent hashing gives "
                + "E[D] = P * (1 - (1 - 1/P)^K), which is well below K, so a proportional set is not evidence of "
                + "clustering. groups({ReportedGroups} of {DistinctShards} shown, shard:count, densest first): "
                + "{GroupDetail}. sources({ReportedSources} of {Sources} shown, path=shard, for offline "
                + "hand-checking only): {SourceDetail}",
                repoId,
                RepoContextTrees.VectorMembership,
                routing.PhysicalTreeId,
                distribution.MapVersion,
                distribution.Sources,
                distribution.PhysicalShardCount,
                distribution.VirtualShardCount,
                distribution.PhysicalShardCount > 0
                    ? distribution.VirtualShardCount / distribution.PhysicalShardCount
                    : 0,
                distribution.DistinctShards,
                distribution.LargestShardGroup,
                distribution.ReportedGroups,
                distribution.DistinctShards,
                distribution.GroupDetail,
                distribution.ReportedSources,
                distribution.Sources,
                distribution.SourceDetail);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "Repo {RepoId}: could not resolve the membership tree's physical shard map, so this pass's "
                + "gap-set shard distribution is unavailable (issue #2287). Diagnostic only: the pass, the gap "
                + "set shape line, and the read-back control arm are all unaffected.",
                repoId);
        }
    }

    /// <summary>
    /// Records this pass's gap selection into the per-repository history and emits the
    /// shape of the never-converging back-fill (issue #2208) as two structured lines,
    /// so a live deployment answers what the per-pass count cannot. Diagnostic only -
    /// it changes neither what is embedded nor the pass outcome - and best-effort: any
    /// probe failure is logged and swallowed.
    /// <para>
    /// The first line is the <b>set shape</b> the field data calls for: the count and
    /// order-independent digest of this pass's gaps, the overlap with the previous pass
    /// (with the entered/left fringe), and the rolling union of gap-selected files
    /// across passes measured against the walked corpus. A union that stays small means
    /// the churn is confined to a stable-ish subset; a union climbing toward the corpus
    /// means the store is losing writes across the whole repository.
    /// </para>
    /// <para>
    /// The second line is the <b>durability signal</b>: how many files the previous
    /// pass both selected AND landed (embedded with membership recorded) are being
    /// re-selected now, paired with an immediate coverage read-back of this pass's
    /// gaps. The read-back is the only store touch this instrumentation adds, so it
    /// runs on <b>alternate passes</b> (see <see cref="GapReadBackRunsOnPass"/>): the
    /// gap counts then split into a pass-preceded-by-a-read-back arm and a not arm, so
    /// the read-back's own effect on the next pass's observability is measured rather
    /// than assumed away. This is the file-arm reading of the symbol arm's
    /// re-embed-loop signature (<see cref="DetectStalledGapProgress"/>, issues
    /// #2071/#2078). A majority re-selected while the immediate read-back sees them
    /// covered is a write that lands but does not stay observable - the
    /// WAL-replay-budget loop; a low read-back is a write that is not durable.
    /// </para>
    /// <para>
    /// The majority-re-selected reading is also what this method now <b>returns</b>.
    /// It was measured here and discarded, which is precisely why the file arm went
    /// on re-embedding the same closed pool for 179 consecutive passes while every
    /// batch reported success: the loop was instrumented but nothing consumed the
    /// instrument. The threshold is a majority rather than any single repeat, for the
    /// same reason the symbol arm uses one - a handful of legitimate stragglers must
    /// not be mistaken for the loop, which shows up as nearly the whole set returning.
    /// </para>
    /// <para>
    /// The set shape also carries the <b>entrant partition</b> (issue #2292). This
    /// pass's gaps are split three ways against the previous measured pass, summing
    /// exactly to the gap count: <i>persisted</i> (a gap then and a gap now - the
    /// non-converging residue), <i>regressed</i> (positively observed <b>covered</b>
    /// then and a gap now), and <i>noPriorCoverage</i> (neither - a file the previous
    /// pass did not walk, or one it embedded as changed without ever observing it
    /// covered). The distinction is load-bearing: the fringe this line reported before
    /// was a set-difference of <i>gap sets</i>, and absence from a gap set conflates
    /// "was observed covered" with "was never considered", so it could be non-zero
    /// with no coverage lost at all. Only <i>regressed</i> is a coverage regression,
    /// and it is the discrimination the probe seam is structurally unable to make,
    /// because that seam sees one batch and cannot know what was covered last pass.
    /// The prior covered set is taken from the coverage probe the pass already
    /// performs, so this adds no store read: the covered side was computed every pass
    /// and thrown away.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository whose pass is being measured.</param>
    /// <param name="gapSelectedFiles">The files this pass selected because their coverage flag was missing.</param>
    /// <param name="landedKeys">The source keys this pass embedded and recorded.</param>
    /// <param name="coverage">This pass's observed coverage.</param>
    /// <param name="changedFileCount">Files the reconcile changed this pass.</param>
    /// <param name="walkedFiles">Files walked this pass, for the union-versus-corpus reading.</param>
    /// <param name="cancellationToken">Cancels the read-back probe.</param>
    /// <returns><see langword="true"/> when this pass's selection repeats a majority of what the previous pass landed.</returns>
    private async Task<bool> LogGapDiagnosticsAsync(
        string repoId,
        IReadOnlyList<RepoFileEntry> gapSelectedFiles,
        IReadOnlyCollection<string> landedKeys,
        RepoContextEmbeddingCoverage coverage,
        int changedFileCount,
        int walkedFiles,
        CancellationToken cancellationToken)
    {
        var stalled = false;
        try
        {
            var selectedKeys = new HashSet<string>(StringComparer.Ordinal);
            foreach (var file in gapSelectedFiles)
            {
                selectedKeys.Add(RepoContextKeys.File(repoId, file.RelativePath));
            }

            // Only the gap files that actually landed this pass, mirroring the symbol
            // arm's rule that the next pass compares against work that reported success.
            var landedFromGap = new HashSet<string>(StringComparer.Ordinal);
            foreach (var key in landedKeys)
            {
                if (selectedKeys.Contains(key))
                {
                    landedFromGap.Add(key);
                }
            }

            var history = _fileGapHistory.GetOrAdd(repoId, static _ => new FileGapHistory());
            var stats = history.Observe(selectedKeys, landedFromGap, coverage, changedFileCount, walkedFiles);

            // The verdict the backoff consumes. Same majority rule as the symbol arm's
            // DetectStalledGapProgress, and for the same reason: a source the last pass
            // embedded, stored, and recorded membership for should not be selected
            // again at all, so a majority of them returning means the flag writes are
            // not becoming observable and repeating them cannot help.
            stalled = stats.PreviousLanded > 0 && stats.LandedRepeats * 2 >= stats.PreviousLanded;
            if (stalled)
            {
                _logger.LogWarning(
                    "Repo {RepoId}: {Repeats} of the {Landed} file(s) the previous pass embedded AND recorded are "
                    + "being selected again, so the membership writes are not becoming observable and re-embedding "
                    + "them cannot help. Treating this as a saturated plane and standing the gap back-fill down "
                    + "(issue #2208).",
                    repoId,
                    stats.LandedRepeats,
                    stats.PreviousLanded);
            }

            // The read-back is the only store touch this instrumentation adds, so run
            // it on alternate passes: a pass preceded by a read-back (arm A) versus one
            // that was not (arm B) then measures the read-back's own effect on the next
            // pass's observability instead of assuming it away (issue #2208). The arm
            // label and parity rule are logged so the two buckets can be split without
            // guessing which pass ran the probe.
            var runReadBack = GapReadBackRunsOnPass(stats.Passes);
            var arm = ClassifyGapReadBackArm(stats.Passes);

            const int SampleSize = 16;
            var sample = gapSelectedFiles
                .Select(static file => file.RelativePath)
                .OrderBy(static path => path, StringComparer.Ordinal)
                .Take(SampleSize)
                .ToArray();

            _logger.LogInformation(
                "Repo {RepoId}: back-fill gap set shape. selected={GapCount} digest={GapDigest}; vs previous "
                + "selected={PrevCount}: overlap={Overlap} entered={Entered} left={Left}; entered splits "
                + "regressed={Regressed} + noPriorCoverage={NoPriorCoverage} (regressed was positively observed "
                + "COVERED on the previous measured pass, so only it is a coverage regression; noPriorCoverage was "
                + "neither a gap nor observed covered then, which a new or changed file is benignly). "
                + "unmeasuredPassesSincePrevious={Unmeasured} prevChanged={PrevChanged}{CoveredNote}; "
                + "rolling union={UnionSize} "
                + "over {Passes} pass(es) against {WalkedFiles} walked file(s){UnionNote}. "
                + "readBackThisPass={RunReadBack} (rule: runs on even pass ordinals); "
                + "controlArm={Arm} (this selected count's bucket: PriorReadBackRan=A, PriorReadBackSkipped=B, "
                + "Seed=first pass). sample: {GapSample}",
                repoId,
                stats.CurrentCount,
                GapSetDigest(repoId, gapSelectedFiles).ToString("x16"),
                stats.PreviousCount,
                stats.Overlap,
                stats.Entered,
                stats.Left,
                stats.Regressed,
                stats.NoPriorCoverage,
                stats.UnmeasuredPassesSincePrevious,
                stats.PreviousChangedFileCount,
                stats.PreviousCoveredSaturated
                    ? " (prior coverage tracking saturated, so regressed is a floor)"
                    : string.Empty,
                stats.UnionCount,
                stats.Passes,
                walkedFiles,
                stats.UnionSaturated ? " (union tracking saturated)" : string.Empty,
                runReadBack,
                arm,
                string.Join(", ", sample));

            // The physical-shard distribution of the WHOLE gap set (issue #2287),
            // emitted immediately after the shape line and BEFORE the read-back branch
            // below returns on arm-B passes, so it is collected on every pass rather
            // than only on the passes that probe. It reads routing metadata only, never
            // a membership entry, so running it on both arms cannot perturb the A/B
            // control the read-back parity implements.
            await LogGapShardDistributionAsync(repoId, gapSelectedFiles, cancellationToken)
                .ConfigureAwait(false);

            if (stats.RegressionIsUnexplained)
            {
                // The one arm of this instrumentation that is worth waking somebody
                // for. Every benign covered-to-uncovered transition in the file arm
                // requires a content change: the only path that clears coverage
                // short of retirement is the contentless unmark, which fires when a
                // contentless-marked file GAINS content and is therefore a changed
                // file, and a removed file is not walked so it cannot be a gap. With
                // both passes reporting nothing changed and no unmeasured pass
                // between them, this arm has no benign reading left.
                //
                // Deliberately narrower than the raw entrant count, for the reason
                // #2287 established: a warning that can fire benignly fires
                // constantly, is muted, and takes the real signal with it.
                _logger.LogWarning(
                    "Repo {RepoId}: {Regressed} file(s) that the previous measured pass observed as COVERED are "
                    + "gaps again, over an input in which neither pass changed a file and with no unmeasured pass "
                    + "between them. A gap means the coverage probe positively observed the source as uncovered, "
                    + "and file membership is enable-wins and retires down one path that no unchanged pass takes, "
                    + "so the flag cannot have been cleared: either the read did not return it or the covered set "
                    + "is computed differently between passes. Both are read-path instabilities (issues "
                    + "#2208/#2292). gapTotal={GapCount} = persisted={Overlap} + regressed + "
                    + "noPriorCoverage={NoPriorCoverage}; sample: {RegressedSample}",
                    repoId,
                    stats.Regressed,
                    stats.CurrentCount,
                    stats.Overlap,
                    stats.NoPriorCoverage,
                    string.Join(", ", stats.RegressedSample));
            }

            if (!runReadBack)
            {
                // Arm-B pass: deliberately no read-back, so the next pass's coverage
                // probe meets the store untouched by this instrumentation.
                return stalled;
            }

            int visibleNow;
            try
            {
                var covered = await _writer
                    .ProbeCoveredSourceIdsAsync(repoId, selectedKeys.ToList(), cancellationToken)
                    .ConfigureAwait(false);
                visibleNow = 0;
                foreach (var key in selectedKeys)
                {
                    if (covered.Contains(VectorCodec.SourceId(key)))
                    {
                        visibleNow++;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: back-fill coverage read-back probe failed; the durability signal for this "
                    + "pass is unavailable but the pass is unaffected.",
                    repoId);
                return stalled;
            }

            _logger.LogInformation(
                "Repo {RepoId}: back-fill durability signal (pass {Passes}, readBackThisPass=true). of {PrevLanded} "
                + "file(s) the previous pass embedded AND recorded, {LandedRepeats} are re-selected now; immediate "
                + "coverage read-back sees {VisibleNow} of {GapCount} of this pass's gaps as covered. A majority "
                + "re-selected with a high read-back is a write that lands but does not stay observable (the "
                + "WAL-replay-budget re-embed loop, cf. issues #2071/#2078 on the symbol arm; the file arm now "
                + "backs off on the same signal, issue #2208); a low read-back is a write that is not durable.",
                repoId,
                stats.Passes,
                stats.PreviousLanded,
                stats.LandedRepeats,
                visibleNow,
                stats.CurrentCount);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                ex,
                "Repo {RepoId}: back-fill gap diagnostics failed; diagnostic only and the pass is unaffected.",
                repoId);
        }

        return stalled;
    }

    /// <summary>
    /// The file arm's per-repository cross-pass gap-selection history. All access is
    /// serialized on the instance because, although reconcile passes for one
    /// repository run one at a time, the ingestor is a singleton and nothing in the
    /// type system enforces that. Purely diagnostic (issue #2208).
    /// </summary>
    internal sealed class FileGapHistory
    {
        /// <summary>
        /// A ceiling on the rolling union so a pathological loss case that keeps
        /// selecting fresh files cannot grow the set without bound. Well above any
        /// real corpus, so it never clips a genuine measurement; when exceeded the
        /// count alone still tells the story and the line is flagged saturated.
        /// </summary>
        internal const int MaxUnionTracked = 200_000;

        /// <summary>
        /// A ceiling on the retained prior covered set, mirroring
        /// <see cref="MaxUnionTracked"/>. Truncation can only cause a covered source
        /// to be missed, never invented, so a saturated set can only <b>under</b>-report
        /// the regressed arm - the direction that keeps the warning sound, since a
        /// false alarm is the failure mode that would get it muted (issue #2292).
        /// </summary>
        internal const int MaxCoveredTracked = 200_000;

        /// <summary>How many regressed keys the warning names, following the bounded-sample rule.</summary>
        internal const int RegressedSampleSize = 16;

        private readonly object _gate = new();
        private readonly HashSet<string> _union = new(StringComparer.Ordinal);
        private HashSet<string> _previousSelected = new(StringComparer.Ordinal);
        private HashSet<string> _previousLanded = new(StringComparer.Ordinal);
        private HashSet<string> _previousCovered = new(StringComparer.Ordinal);
        private int _previousChangedFileCount;
        private bool _previousCoveredSaturated;
        private int _unmeasuredSincePrevious;
        private int _passes;
        private bool _unionSaturated;

        /// <summary>
        /// Records that a reconcile pass produced no gap-shape measurement, so it
        /// advanced no history. Without this the next measured pass would compare
        /// itself against a pass that is not the preceding one while reporting it as
        /// "previous", and a regression measured across an unmeasured interval is not
        /// alarmable because anything could have happened in it (issue #2292).
        /// </summary>
        internal void NoteUnmeasuredPass()
        {
            lock (_gate)
            {
                _unmeasuredSincePrevious++;
            }
        }

        /// <summary>
        /// Folds one pass's gap selection into the history and returns its shape
        /// relative to the previous pass and the accumulated union.
        /// </summary>
        /// <param name="selectedKeys">The canonical source keys this pass selected as gaps.</param>
        /// <param name="landedFromGap">The subset of <paramref name="selectedKeys"/> that landed this pass.</param>
        /// <param name="walkedFiles">The number of files walked this pass, for the union-versus-corpus reading.</param>
        /// <param name="coverage">This pass's observed coverage, retained so the next pass can tell a regression from a file it never observed covered.</param>
        /// <param name="changedFileCount">Files the reconcile changed this pass, retained so the regression warning can require that neither pass changed anything.</param>
        /// <returns>The measured shape of this pass.</returns>
        internal FileGapStats Observe(
            HashSet<string> selectedKeys,
            HashSet<string> landedFromGap,
            RepoContextEmbeddingCoverage coverage,
            int changedFileCount,
            int walkedFiles)
        {
            ArgumentNullException.ThrowIfNull(selectedKeys);
            ArgumentNullException.ThrowIfNull(landedFromGap);

            lock (_gate)
            {
                var overlap = 0;
                List<string>? regressedKeys = null;
                foreach (var key in selectedKeys)
                {
                    if (_previousSelected.Contains(key))
                    {
                        overlap++;
                        continue;
                    }

                    // Not a gap last pass. That is NOT the same as having been covered
                    // last pass, which is the conflation this partition exists to
                    // remove: a file is absent from a gap set both when it was
                    // observed covered and when it was never considered at all. Only
                    // the first is a coverage regression (issue #2292).
                    if (_previousCovered.Contains(VectorCodec.SourceId(key)))
                    {
                        (regressedKeys ??= new List<string>()).Add(key);
                    }
                }

                var regressed = regressedKeys?.Count ?? 0;

                // Ordered before truncation, so the sample is a deterministic prefix
                // of the regressed set rather than an arbitrary subset of it: two
                // passes reporting the same files report the same sample.
                var regressedSample = regressedKeys is null
                    ? Array.Empty<string>()
                    : regressedKeys
                        .OrderBy(static key => key, StringComparer.Ordinal)
                        .Take(RegressedSampleSize)
                        .ToArray();

                var landedRepeats = 0;
                foreach (var key in _previousLanded)
                {
                    if (selectedKeys.Contains(key))
                    {
                        landedRepeats++;
                    }
                }

                if (!_unionSaturated)
                {
                    foreach (var key in selectedKeys)
                    {
                        if (_union.Count >= MaxUnionTracked)
                        {
                            _unionSaturated = true;
                            break;
                        }

                        _union.Add(key);
                    }
                }

                _passes++;
                var stats = new FileGapStats(
                    CurrentCount: selectedKeys.Count,
                    PreviousCount: _previousSelected.Count,
                    Overlap: overlap,
                    Entered: selectedKeys.Count - overlap,
                    Left: _previousSelected.Count - overlap,
                    Regressed: regressed,
                    NoPriorCoverage: selectedKeys.Count - overlap - regressed,
                    PreviousChangedFileCount: _previousChangedFileCount,
                    UnmeasuredPassesSincePrevious: _unmeasuredSincePrevious,
                    PreviousCoveredSaturated: _previousCoveredSaturated,
                    ChangedFileCount: changedFileCount,
                    RegressedSample: regressedSample,
                    PreviousLanded: _previousLanded.Count,
                    LandedRepeats: landedRepeats,
                    UnionCount: _union.Count,
                    Passes: _passes,
                    WalkedFiles: walkedFiles,
                    UnionSaturated: _unionSaturated);

                _previousSelected = selectedKeys;
                _previousLanded = landedFromGap;
                (_previousCovered, _previousCoveredSaturated) = SnapshotCoverage(coverage);
                _previousChangedFileCount = changedFileCount;
                _unmeasuredSincePrevious = 0;
                return stats;
            }
        }

        /// <summary>
        /// Copies this pass's observed covered source identifiers, bounded by
        /// <see cref="MaxCoveredTracked"/>. The identifiers are what the coverage
        /// probe already returned, so nothing is read from the store to build this.
        /// </summary>
        private static (HashSet<string> Covered, bool Saturated) SnapshotCoverage(
            RepoContextEmbeddingCoverage coverage)
        {
            var covered = new HashSet<string>(StringComparer.Ordinal);
            var saturated = false;
            foreach (var set in new[] { coverage.Embedded, coverage.Contentless })
            {
                foreach (var sourceId in set)
                {
                    if (covered.Count >= MaxCoveredTracked)
                    {
                        saturated = true;
                        return (covered, saturated);
                    }

                    covered.Add(sourceId);
                }
            }

            return (covered, saturated);
        }
    }

    /// <summary>The measured shape of one file-arm gap-selection pass (issue #2208).</summary>
    /// <param name="CurrentCount">Gap files selected this pass.</param>
    /// <param name="PreviousCount">Gap files selected the previous pass.</param>
    /// <param name="Overlap">Files selected both this pass and the previous pass.</param>
    /// <param name="Entered">Files selected this pass that were not selected the previous pass. Kept for continuity with the pass log, but it is <b>not</b> a coverage-regression count: it is the sum of <paramref name="Regressed"/> and <paramref name="NoPriorCoverage"/>.</param>
    /// <param name="Left">Files selected the previous pass that are not selected this pass.</param>
    /// <param name="Regressed">Files selected this pass that the previous measured pass positively observed as COVERED. The only arm of the entrant fringe that is a coverage regression, and the discrimination the per-batch probe seam cannot make.</param>
    /// <param name="NoPriorCoverage">Files selected this pass that the previous measured pass neither selected as a gap nor observed as covered: a file it did not walk, or one it embedded as changed without observing coverage. Benign.</param>
    /// <param name="PreviousChangedFileCount">Files the reconcile changed on the previous measured pass.</param>
    /// <param name="UnmeasuredPassesSincePrevious">Reconcile passes since the previous measured pass that produced no gap shape, so "previous" is not the preceding pass when this is non-zero.</param>
    /// <param name="PreviousCoveredSaturated">Whether the retained prior covered set hit its ceiling, in which case <paramref name="Regressed"/> is a floor.</param>
    /// <param name="ChangedFileCount">Files the reconcile changed this pass.</param>
    /// <param name="RegressedSample">A bounded sample of the regressed keys, for the warning.</param>
    /// <param name="PreviousLanded">Files the previous pass both selected and landed.</param>
    /// <param name="LandedRepeats">Previously-landed files being re-selected this pass - the re-embed-loop signature.</param>
    /// <param name="UnionCount">Distinct files selected across all observed passes.</param>
    /// <param name="Passes">Passes observed for this repository.</param>
    /// <param name="WalkedFiles">Files walked this pass, for reading the union against the corpus.</param>
    /// <param name="UnionSaturated">Whether the union hit its tracking ceiling.</param>
    internal readonly record struct FileGapStats(
        int CurrentCount,
        int PreviousCount,
        int Overlap,
        int Entered,
        int Left,
        int Regressed,
        int NoPriorCoverage,
        int PreviousChangedFileCount,
        int UnmeasuredPassesSincePrevious,
        bool PreviousCoveredSaturated,
        int ChangedFileCount,
        IReadOnlyList<string> RegressedSample,
        int PreviousLanded,
        int LandedRepeats,
        int UnionCount,
        int Passes,
        int WalkedFiles,
        bool UnionSaturated)
    {
        /// <summary>
        /// Whether this pass's regressed arm has no benign explanation, which is the
        /// only condition worth warning on (issue #2292).
        /// <para>
        /// Every covered-to-uncovered transition the file arm can produce legitimately
        /// requires a content change. The single path that clears coverage short of
        /// retirement is the contentless unmark, which fires when a contentless-marked
        /// file gains content and is therefore a changed file; a removed file is not
        /// walked, so it cannot appear as a gap at all. Requiring that neither the
        /// current nor the previous measured pass changed a file therefore removes
        /// every benign reading, and requiring that no unmeasured pass sits between
        /// them keeps the comparison anchored to the pass it names.
        /// </para>
        /// <para>
        /// Deliberately much narrower than <see cref="Entered"/>. A warning that can
        /// fire benignly fires on nearly every pass, is muted, and takes the real
        /// signal with it - the reason the membership probe's accounting counts a
        /// short read rather than alarming on it (issue #2287).
        /// </para>
        /// </summary>
        public bool RegressionIsUnexplained =>
            Regressed > 0
            && Passes > 1
            && ChangedFileCount == 0
            && PreviousChangedFileCount == 0
            && UnmeasuredPassesSincePrevious == 0;
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
