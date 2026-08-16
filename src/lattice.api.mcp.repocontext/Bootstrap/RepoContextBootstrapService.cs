using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The coordinator behind the <c>repocontext_bootstrap</c> tool: it walks a
/// repository, reconciles the scan against the structural records already stored
/// for that repository, and applies exactly the difference - creating new file
/// nodes, updating changed ones, and pruning nodes whose files are gone - using
/// the core atomic batch write primitive.
/// <para>
/// <b>Idempotent and resumable.</b> Every file node stores a content digest, so a
/// re-run over an unchanged tree computes an empty plan and writes nothing; a
/// changed tree writes only the changed files; and a crashed run resumes cleanly
/// because the next attempt sees the already-persisted files as unchanged and
/// skips them. Writes are committed in bounded chunks via
/// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, IReadOnlyList{string}, string, CancellationToken)"/>,
/// each keyed by a deterministic operation id derived from the chunk's exact keys
/// and content, so re-submitting an identical chunk safely re-attaches to the
/// original all-or-nothing saga instead of duplicating work.
/// </para>
/// <para>
/// <b>Vectorisation boundary.</b> Structural ingestion is the whole deliverable
/// here. Changed files are offered to the injected
/// <see cref="IRepoContextVectorIngestor"/> seam, whose shipped binding is a
/// no-op: the vector record shape and the vector write / retrieval path are owned
/// by separate work, so bootstrap does not persist vectors and does not race that
/// surface.
/// </para>
/// </summary>
internal sealed class RepoContextBootstrapService
{
    private const int WriteChunkSize = 256;

    /// <summary>
    /// How many additional files must embed between vectorising heartbeat log
    /// lines. The vectorising pass reports progress per embedding batch; throttling
    /// the log to one line per this many freshly embedded files keeps a large
    /// re-embed observable in the log without emitting a line per batch.
    /// </summary>
    private const int VectorisingHeartbeatInterval = 100;

    /// <summary>
    /// How often the concurrent walk-progress pump samples and reports the running
    /// hashed-file count while the (synchronous) walk is in flight. Short enough to
    /// feel live, long enough that a fast walk emits only a handful of reports.
    /// </summary>
    private static readonly TimeSpan WalkProgressInterval = TimeSpan.FromMilliseconds(500);

    private readonly IGrainFactory _grainFactory;
    private readonly Serializer<FileNode> _fileNodeSerializer;
    private readonly Serializer<RepoNode> _repoNodeSerializer;
    private readonly IRepoContextVectorIngestor _vectorIngestor;
    private readonly RepoContextSymbolReconciler _symbolReconciler;
    private readonly ISymbolExtractor _symbolExtractor;
    private readonly RepoContextWorkspaceGuard _workspaceGuard;
    private readonly TimeProvider _timeProvider;
    private readonly RepoContextIndexingOptions _options;
    private readonly ILogger<RepoContextBootstrapService> _logger;

    /// <summary>
    /// The per-repository cross-walk pruning cache, keyed by repository id. Each entry
    /// holds the directory-modification-time snapshot the previous walk observed and the
    /// wall-clock tick of the last full (unpruned) sweep. It lives only in this singleton's
    /// memory, so a process restart starts every repository cold - the first post-restart
    /// walk is a full one, which is correct by construction.
    /// </summary>
    private readonly ConcurrentDictionary<string, PruneCacheEntry> _pruneCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Creates the bootstrap coordinator.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to reach the structural
    /// Lattice tree. Must not be <see langword="null"/>.</param>
    /// <param name="fileNodeSerializer">The Orleans serializer for
    /// <see cref="FileNode"/>. Must not be <see langword="null"/>.</param>
    /// <param name="repoNodeSerializer">The Orleans serializer for
    /// <see cref="RepoNode"/>. Must not be <see langword="null"/>.</param>
    /// <param name="vectorIngestor">The vectorisation seam (a no-op by default).
    /// Must not be <see langword="null"/>.</param>
    /// <param name="symbolReconciler">The per-symbol structural reconciler that
    /// extracts and prunes symbol records for changed and removed files. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="symbolExtractor">The language-dispatching symbol extractor, used
    /// to decide which content-unchanged files are symbol back-fill candidates. Must
    /// not be <see langword="null"/>.</param>
    /// <param name="workspaceGuard">The fail-closed workspace boundary that every
    /// ingestion path is resolved and bounds-checked through. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="timeProvider">The clock used to schedule the periodic full sweep
    /// that backstops directory-modification-time pruning. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="options">The background-indexing cadence knobs, whose
    /// <see cref="RepoContextIndexingOptions.FullWalkInterval"/> bounds how stale an
    /// in-place content edit can be before a full sweep catches it. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="logger">The logger. Must not be <see langword="null"/>.</param>
    public RepoContextBootstrapService(
        IGrainFactory grainFactory,
        Serializer<FileNode> fileNodeSerializer,
        Serializer<RepoNode> repoNodeSerializer,
        IRepoContextVectorIngestor vectorIngestor,
        RepoContextSymbolReconciler symbolReconciler,
        ISymbolExtractor symbolExtractor,
        RepoContextWorkspaceGuard workspaceGuard,
        TimeProvider timeProvider,
        RepoContextIndexingOptions options,
        ILogger<RepoContextBootstrapService> logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(fileNodeSerializer);
        ArgumentNullException.ThrowIfNull(repoNodeSerializer);
        ArgumentNullException.ThrowIfNull(vectorIngestor);
        ArgumentNullException.ThrowIfNull(symbolReconciler);
        ArgumentNullException.ThrowIfNull(symbolExtractor);
        ArgumentNullException.ThrowIfNull(workspaceGuard);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _grainFactory = grainFactory;
        _fileNodeSerializer = fileNodeSerializer;
        _repoNodeSerializer = repoNodeSerializer;
        _vectorIngestor = vectorIngestor;
        _symbolReconciler = symbolReconciler;
        _symbolExtractor = symbolExtractor;
        _workspaceGuard = workspaceGuard;
        _timeProvider = timeProvider;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Runs one idempotent ingestion pass and returns a summary of what changed.
    /// </summary>
    /// <param name="request">The ingestion inputs. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>A summary of files scanned, added, updated, removed, and unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is null.</exception>
    /// <exception cref="ArgumentException">The request omits a repository root or id.</exception>
    public Task<RepoContextBootstrapResult> RunAsync(
        RepoContextBootstrapRequest request,
        CancellationToken cancellationToken = default)
        => RunAsync(request, progress: null, cancellationToken);

    /// <summary>
    /// Runs one idempotent ingestion pass, reporting incremental progress through
    /// the supplied sink, and returns a summary of what changed.
    /// </summary>
    /// <param name="request">The ingestion inputs. Must not be <see langword="null"/>.</param>
    /// <param name="progress">An optional sink that receives phase and counter
    /// deltas as the run proceeds; <see langword="null"/> reports nothing.</param>
    /// <param name="cancellationToken">Cancels the run.</param>
    /// <returns>A summary of files scanned, added, updated, removed, and unchanged.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is null.</exception>
    /// <exception cref="ArgumentException">The request omits a repository root or id.</exception>
    public async Task<RepoContextBootstrapResult> RunAsync(
        RepoContextBootstrapRequest request,
        IRepoIndexProgressSink? progress,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrWhiteSpace(request.RepoRoot))
        {
            throw new ArgumentException("The repository root must be provided.", nameof(request));
        }

        if (string.IsNullOrWhiteSpace(request.RepoId))
        {
            throw new ArgumentException("The repository id must be provided.", nameof(request));
        }

        var stopwatch = Stopwatch.StartNew();
        var repoRoot = _workspaceGuard.Resolve(request.RepoRoot);
        var repoId = request.RepoId;
        var phase = RepoIndexPhase.Walking;

        try
        {
            // Read the facts already stored for this repository's files before the
            // walk, so the walk can apply its stat fast-path: an unchanged file
            // (matching size, older modification time than its ingest anchor) is
            // skipped without a read. The digest projection of the same map drives
            // the reconciliation diff.
            var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
            var storedMeta = await ReadStoredMetaAsync(tree, repoId, cancellationToken)
                .ConfigureAwait(false);

            await ReportAsync(progress, new RepoIndexProgressUpdate { Phase = RepoIndexPhase.Walking }, cancellationToken)
                .ConfigureAwait(false);

            // Build the cross-walk pruning context for this run. Pruning is opt-in
            // per request: only the continuous background reconcile enables it, so
            // an explicit onboarding or re-bootstrap always forces a full, exact
            // walk. When pruning is allowed and a prior directory-modification-time
            // snapshot exists (and a full sweep is not due), the walk skips the
            // per-file stat of unchanged directories. Even then a full sweep is
            // forced when the repository is cold (no snapshot) or the configured
            // full-walk interval has elapsed since the last one, so an in-place
            // content edit - which does not bump a directory's modification time and
            // is invisible to pruning - is still caught within that bound.
            var nowTicks = _timeProvider.GetUtcNow().UtcTicks;
            _pruneCache.TryGetValue(repoId, out var priorPrune);
            var lastFullSweepTicks = priorPrune?.LastFullSweepTicks ?? 0;
            var forceFull = !request.AllowPrune
                || priorPrune?.DirectoryMtimes is not { Count: > 0 }
                || nowTicks - lastFullSweepTicks >= _options.FullWalkInterval.Ticks;
            var pruning = new RepoWalkPruning
            {
                PreviousDirectoryMtimes = priorPrune?.DirectoryMtimes,
                ForceFull = forceFull,
            };

            // The walk is synchronous, so a run with a progress sink drives a
            // concurrent pump that samples the running processed-file count and
            // reports it. The walker only writes the latest count (a single lock-free
            // volatile write per processed file); the pump owns every grain report,
            // so FilesScanned climbs during the walk instead of staying frozen at
            // zero, and reports never reorder or pile up.
            IReadOnlyList<RepoFileEntry> scanned;
            if (progress is null)
            {
                scanned = RepoTreeWalker.Walk(
                    repoRoot, request.IncludeGlobs, request.ExcludeGlobs,
                    request.RespectGitignore, request.ExcludeBinary, onProgress: null, cancellationToken,
                    knownFiles: storedMeta, pruning: pruning);
            }
            else
            {
                var hashedSoFar = 0;
                using var walkComplete = new CancellationTokenSource();
                var walkPump = PumpWalkProgressAsync(
                    progress, () => Volatile.Read(ref hashedSoFar), walkComplete.Token, cancellationToken);
                try
                {
                    scanned = RepoTreeWalker.Walk(
                        repoRoot, request.IncludeGlobs, request.ExcludeGlobs,
                        request.RespectGitignore, request.ExcludeBinary,
                        done => Volatile.Write(ref hashedSoFar, done), cancellationToken,
                        knownFiles: storedMeta, pruning: pruning);
                }
                finally
                {
                    walkComplete.Cancel();
                    await walkPump.ConfigureAwait(false);
                }
            }

            // Store the snapshot this walk observed for the next run. The walk records a
            // modification time for every directory it visits, pruned or not, so the
            // snapshot is always complete and self-heals as the tree changes. Advance the
            // last-full-sweep marker only when this run actually forced a full sweep.
            var updatedSnapshot = new PruneCacheEntry(
                pruning.CurrentDirectoryMtimes,
                forceFull ? nowTicks : lastFullSweepTicks);

            _logger.LogInformation(
                "Repo {RepoId}: walk complete - {Scanned} files in {Elapsed} ms ({Mode}; pruned {PrunedDirs} dir(s), {PrunedFiles} file(s)).",
                repoId, scanned.Count, stopwatch.ElapsedMilliseconds,
                forceFull ? "full sweep" : "pruned",
                pruning.PrunedDirectoryCount, pruning.PrunedFileCount);

            phase = RepoIndexPhase.Reconciling;
            await ReportAsync(
                progress,
                new RepoIndexProgressUpdate { Phase = RepoIndexPhase.Reconciling, FilesScanned = scanned.Count },
                cancellationToken).ConfigureAwait(false);

            var storedDigests = ProjectDigests(storedMeta);
            var plan = RepoContextBootstrapPlan.Compute(storedDigests, scanned);

            // Metadata-changed files are content-unchanged (only their ingest anchor
            // is refreshed), so callers see them within the unchanged tally, and they
            // join the unchanged set offered to the ingestor for embedding-gap
            // back-fill - never re-embedded, but eligible to fill a missing vector.
            var unchangedCount = plan.Unchanged.Count + plan.MetadataChanged.Count;
            var unchangedForBackfill = new List<RepoFileEntry>(unchangedCount);
            unchangedForBackfill.AddRange(plan.Unchanged);
            unchangedForBackfill.AddRange(plan.MetadataChanged);

            var symbolsCaptured = 0;
            IReadOnlyCollection<string> changedSymbolKeys = Array.Empty<string>();
            IReadOnlyCollection<string> prunedSymbolKeys = Array.Empty<string>();

            // The symbol back-fill self-heal: content-unchanged, supported-language
            // files whose node was never symbol-processed (it predates symbol
            // extraction, or a prior run stopped before the symbol phase). Extracting
            // them keeps a repository indexed before this feature converge on a
            // complete symbol projection without re-embedding or otherwise touching
            // the files that already have one. It is drawn from the pure-unchanged set
            // only (not the anchor-refreshed metadata-changed set), so a back-filled
            // node is written exactly once - by the back-fill loop - and never also by
            // the metadata-changed loop.
            var symbolBackfill = SelectSymbolBackfill(plan.Unchanged, storedMeta);

            if (!plan.IsNoOp || symbolBackfill.Count > 0)
            {
                phase = RepoIndexPhase.Applying;
                var chunksTotal = ComputeChunkCount(plan, symbolBackfill.Count);
                _logger.LogInformation(
                    "Repo {RepoId}: plan - {Added} added, {Updated} updated, {MetadataChanged} anchor-refreshed, {Removed} removed, {Unchanged} unchanged, {Backfill} symbol back-fill; {Chunks} chunk(s) to commit.",
                    repoId, plan.Added.Count, plan.Updated.Count, plan.MetadataChanged.Count, plan.RemovedPaths.Count, plan.Unchanged.Count, symbolBackfill.Count, chunksTotal);
                await ReportAsync(
                    progress,
                    new RepoIndexProgressUpdate
                    {
                        Phase = RepoIndexPhase.Applying,
                        FilesAdded = plan.Added.Count,
                        FilesUpdated = plan.Updated.Count,
                        FilesRemoved = plan.RemovedPaths.Count,
                        FilesUnchanged = unchangedCount,
                        ChunksTotal = chunksTotal,
                        ChunksCommitted = 0,
                    },
                    cancellationToken).ConfigureAwait(false);

                // Retire a removed file's vector before deleting its structural
                // record. Retiring first means a crash between the two steps leaves
                // the structural record in place, so the next run re-drives the
                // same removal (idempotently) rather than orphaning a vector - which
                // keeps the membership set an honest tally of live embeddings.
                await _vectorIngestor.RetireAsync(repoId, plan.RemovedPaths, cancellationToken)
                    .ConfigureAwait(false);

                // Reconcile the per-symbol structural records BEFORE the file nodes
                // are rewritten. Ordering symbols first makes the pass resumable: a
                // crash between the symbol write and the file-node write leaves the
                // file node with its old digest (and no processed marker), so the next
                // run re-detects the file as changed - or as an un-processed back-fill
                // candidate - and re-drives the idempotent symbol upsert. The declared
                // set the reconcile computes is stamped onto each rewritten file node
                // so a later incremental pass knows which symbols a changed or removed
                // file used to declare.
                var symbolResult = await _symbolReconciler.ReconcileAsync(
                    repoId, repoRoot, plan.Added, plan.Updated, plan.RemovedPaths, symbolBackfill, storedMeta, cancellationToken)
                    .ConfigureAwait(false);
                symbolsCaptured = symbolResult.SymbolsCaptured;
                changedSymbolKeys = symbolResult.ChangedSymbolKeys;
                prunedSymbolKeys = symbolResult.PrunedSymbolKeys;
                var declaredEncoded = BuildDeclaredEncoded(
                    symbolResult.DeclaredByPath, plan.MetadataChanged, storedMeta);

                // Exactly the files the reconcile extracted (supported and readable)
                // are stamped as symbol-processed, so a file it could not read is not
                // marked and is retried on the next pass.
                var processedPaths = new HashSet<string>(symbolResult.DeclaredByPath.Keys, StringComparer.Ordinal);

                await ApplyPlanAsync(
                    tree, repoId, plan, symbolBackfill, declaredEncoded, processedPaths, storedMeta, progress, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await ReportAsync(
                    progress,
                    new RepoIndexProgressUpdate { FilesUnchanged = unchangedCount },
                    cancellationToken).ConfigureAwait(false);
            }

            // Vectorising always runs, even when the structural plan is a no-op. An
            // earlier run may have committed a file's structural record but been
            // interrupted before its embedding landed, so the file now looks
            // unchanged yet has no vector. Offering the unchanged set lets the
            // ingestor back-fill exactly those gaps while re-embedding nothing that
            // already has a live vector.
            phase = RepoIndexPhase.Vectorising;
            var changed = new List<RepoFileEntry>(plan.Added.Count + plan.Updated.Count);
            changed.AddRange(plan.Added);
            changed.AddRange(plan.Updated);
            await ReportAsync(progress, new RepoIndexProgressUpdate { Phase = RepoIndexPhase.Vectorising }, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogInformation(
                "Repo {RepoId}: vectorising {Changed} changed file(s); scanning {Unchanged} unchanged for embedding gaps.",
                repoId, changed.Count, unchangedForBackfill.Count);
            var lastVectorisingHeartbeat = 0;
            var embedded = await _vectorIngestor.IngestAsync(
                repoId,
                repoRoot,
                changed,
                unchangedForBackfill,
                (count, ct) =>
                {
                    if (count - lastVectorisingHeartbeat >= VectorisingHeartbeatInterval)
                    {
                        lastVectorisingHeartbeat = count;
                        _logger.LogInformation(
                            "Repo {RepoId}: vectorising progress - {Embedded} file(s) embedded after {Elapsed} ms.",
                            repoId, count, stopwatch.ElapsedMilliseconds);
                    }

                    return ReportAsync(
                        progress, new RepoIndexProgressUpdate { FilesEmbedded = count }, ct);
                },
                cancellationToken).ConfigureAwait(false);
            await ReportAsync(progress, new RepoIndexProgressUpdate { FilesEmbedded = embedded }, cancellationToken)
                .ConfigureAwait(false);

            // Embed the per-symbol records as their own passages. This runs even when
            // the structural plan was a no-op: a symbol upserted or pruned this pass is
            // refreshed or retired, and any symbol still lacking a live embedding - a
            // repository captured before symbol embedding existed - is back-filled, so
            // symbol-level recall converges without a re-walk.
            var symbolsEmbedded = await _vectorIngestor.IngestSymbolsAsync(
                repoId, changedSymbolKeys, prunedSymbolKeys, cancellationToken)
                .ConfigureAwait(false);
            if (symbolsEmbedded > 0)
            {
                _logger.LogInformation(
                    "Repo {RepoId}: embedded {Symbols} symbol passage(s).", repoId, symbolsEmbedded);
            }

            // The run reconciled and applied cleanly, so publish the walk's directory
            // snapshot as the pruning baseline for the next run. Deferring the publish to
            // the success path means a run that fails during apply leaves the previous
            // baseline intact, so the next run does not wrongly prune a directory whose
            // changes were never committed.
            _pruneCache[repoId] = updatedSnapshot;

            _logger.LogInformation(
                "Bootstrap of repository {RepoId} scanned {Scanned} files: {Added} added, {Updated} updated, {Removed} removed, {Unchanged} unchanged in {Elapsed} ms.",
                repoId,
                scanned.Count,
                plan.Added.Count,
                plan.Updated.Count,
                plan.RemovedPaths.Count,
                unchangedCount,
                stopwatch.ElapsedMilliseconds);

            return new RepoContextBootstrapResult
            {
                RepoId = repoId,
                FilesScanned = scanned.Count,
                FilesAdded = plan.Added.Count,
                FilesUpdated = plan.Updated.Count,
                FilesRemoved = plan.RemovedPaths.Count,
                FilesUnchanged = unchangedCount,
                SymbolsCaptured = symbolsCaptured,
                ElapsedMilliseconds = stopwatch.ElapsedMilliseconds,
            };
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();
            _logger.LogInformation(
                "Repo {RepoId}: indexing cancelled during the {Phase} phase after {Elapsed} ms; durable structural writes already committed are preserved and a re-run resumes from the first uncommitted chunk.",
                repoId, phase, stopwatch.ElapsedMilliseconds);
            throw;
        }
    }

    /// <summary>
    /// One repository's cross-walk pruning baseline: the directory-modification-time
    /// snapshot the last successful walk observed, and the wall-clock tick of the last
    /// full (unpruned) sweep so the periodic force-full backstop can be scheduled.
    /// </summary>
    /// <param name="DirectoryMtimes">The repository-relative directory to modification-time
    /// snapshot from the last successful walk.</param>
    /// <param name="LastFullSweepTicks">The UTC tick at which the last full sweep ran.</param>
    private sealed record PruneCacheEntry(
        IReadOnlyDictionary<string, long> DirectoryMtimes,
        long LastFullSweepTicks);

    /// <summary>
    /// Selects the symbol back-fill candidates from the content-unchanged set: files
    /// whose language a symbol extractor supports but whose stored node was never
    /// stamped as symbol-processed. These are files indexed before symbol extraction
    /// existed (or a file a prior run stopped short of processing), so extracting
    /// them lets a pre-existing index converge on a complete symbol projection
    /// without re-reading the files that already have one. Drawing only from the
    /// pure-unchanged set (not the anchor-refreshed metadata-changed set) guarantees
    /// a back-filled node is written exactly once per pass.
    /// </summary>
    /// <param name="unchanged">The content-unchanged files from the plan.</param>
    /// <param name="storedMeta">The stored per-file metadata, consulted for the
    /// symbol-processed marker.</param>
    /// <returns>The unchanged files eligible for symbol back-fill.</returns>
    private List<RepoFileEntry> SelectSymbolBackfill(
        IReadOnlyList<RepoFileEntry> unchanged, IReadOnlyDictionary<string, StoredFileMeta> storedMeta)
    {
        var backfill = new List<RepoFileEntry>();
        foreach (var entry in unchanged)
        {
            if (_symbolExtractor.Supports(entry.Language) && !StoredProcessed(storedMeta, entry.RelativePath))
            {
                backfill.Add(entry);
            }
        }

        return backfill;
    }

    /// <summary>
    /// The number of atomic write chunks <see cref="ApplyPlanAsync"/> will commit
    /// for a plan: one per <see cref="WriteChunkSize"/> upserts, where the upserts
    /// are the repository root marker plus every added, updated, and
    /// anchor-refreshed file, plus every symbol back-fill file whose node is being
    /// rewritten. Deletes ride with the first chunk (the marker guarantees at least
    /// one), so they add no chunk of their own.
    /// </summary>
    /// <param name="plan">The reconciliation plan.</param>
    /// <param name="backfillCount">The number of symbol back-fill files whose nodes
    /// are being rewritten in this pass.</param>
    /// <returns>The total chunk count.</returns>
    private static int ComputeChunkCount(RepoContextBootstrapPlan plan, int backfillCount)
    {
        var upsertCount = 1 + plan.Added.Count + plan.Updated.Count + plan.MetadataChanged.Count + backfillCount;
        return (upsertCount + WriteChunkSize - 1) / WriteChunkSize;
    }

    private static ValueTask ReportAsync(
        IRepoIndexProgressSink? sink, RepoIndexProgressUpdate update, CancellationToken cancellationToken)
        => sink is null ? ValueTask.CompletedTask : sink.ReportAsync(update, cancellationToken);

    /// <summary>
    /// Samples <paramref name="currentCount"/> on a fixed cadence while the walk is
    /// in flight and reports each changed value as a <c>FilesScanned</c> delta, then
    /// emits one final authoritative report once <paramref name="walkComplete"/> is
    /// signalled. Running the reporting on this single pump (rather than from inside
    /// the walker's parallel loop) keeps every grain report ordered and coalesced,
    /// so a fast walk emits only a handful of reports and the count never goes
    /// backwards.
    /// </summary>
    private static async Task PumpWalkProgressAsync(
        IRepoIndexProgressSink progress,
        Func<int> currentCount,
        CancellationToken walkComplete,
        CancellationToken cancellationToken)
    {
        var lastReported = -1;
        try
        {
            while (!walkComplete.IsCancellationRequested)
            {
                await Task.Delay(WalkProgressInterval, walkComplete).ConfigureAwait(false);
                var current = currentCount();
                if (current != lastReported)
                {
                    lastReported = current;
                    await progress.ReportAsync(
                        new RepoIndexProgressUpdate { FilesScanned = current }, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (walkComplete.IsCancellationRequested)
        {
            // The walk finished and signalled completion; fall through to the final
            // report below rather than surfacing the sampling delay's cancellation.
        }

        var final = currentCount();
        if (final != lastReported)
        {
            try
            {
                await progress.ReportAsync(
                    new RepoIndexProgressUpdate { FilesScanned = final }, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // The run itself was cancelled (host shutdown or removal); the walk's
                // partial count is not worth reporting as the run is unwinding.
            }
        }
    }

    /// <summary>
    /// Projects the digest-only view the reconciliation diff needs from the fuller
    /// stored-meta map the walk consumes, so a single structural read serves both.
    /// </summary>
    private static Dictionary<string, string> ProjectDigests(
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta)
    {
        var digests = new Dictionary<string, string>(storedMeta.Count, StringComparer.Ordinal);
        foreach (var (path, meta) in storedMeta)
        {
            digests[path] = meta.Digest;
        }

        return digests;
    }

    /// <summary>
    /// Reads the reconcile-relevant facts already stored for each of the
    /// repository's files - digest, language, size, and the ingest hybrid-logical
    /// clock's wall component (the fast-path anchor, recovered from the digest
    /// register's order key) - keyed by repository-relative path. A single structural
    /// range scan feeds both the walk's stat fast-path and the reconciliation diff.
    /// </summary>
    private async Task<Dictionary<string, StoredFileMeta>> ReadStoredMetaAsync(
        ILattice tree,
        string repoId,
        CancellationToken cancellationToken)
    {
        var prefix = RepoContextKeys.FilesPrefix(repoId);
        var endExclusive = PrefixUpperBound(prefix);
        var meta = new Dictionary<string, StoredFileMeta>(StringComparer.Ordinal);

        // Resilient streaming scan: ScanEntriesAsync reopens a fresh cursor over
        // the still-live range on a transient EnumerationAbortedException (silo
        // failover, cold start, idle expiry, scale-down) and resumes without gaps
        // or duplicates, so the reconcile diff reads the full stored-meta range
        // rather than aborting part-way and mistaking un-read files for deletions.
        await foreach (var entry in tree
            .ScanEntriesAsync(prefix, endExclusive, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!RepoContextKeys.TryParse(entry.Key, out var parsed)
                || parsed.Kind != RepoContextRecordKind.File
                || parsed.Path is not { } path)
            {
                continue;
            }

            var node = _fileNodeSerializer.Deserialize(entry.Value);
            var digest = RepoContextValues.ReadString(node.Digest);
            if (digest is not null)
            {
                meta[path] = new StoredFileMeta(
                    digest,
                    RepoContextValues.ReadString(node.Language) ?? string.Empty,
                    RepoContextValues.ReadInt64(node.SizeBytes) ?? -1,
                    RepoContextValues.ReadHlcWallTicks(node.Digest) ?? 0,
                    DeclaredSymbolNames.Decode(RepoContextValues.ReadString(node.DeclaredSymbols)),
                    RepoContextValues.ReadString(node.SymbolsProcessed) is not null);
            }
        }

        return meta;
    }

    private async Task ApplyPlanAsync(
        ILattice tree,
        string repoId,
        RepoContextBootstrapPlan plan,
        IReadOnlyList<RepoFileEntry> symbolBackfill,
        IReadOnlyDictionary<string, string> declaredEncoded,
        IReadOnlySet<string> processedPaths,
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta,
        IRepoIndexProgressSink? progress,
        CancellationToken cancellationToken)
    {
        var ingestToken = DateTimeOffset.UtcNow.ToString("O");
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        var upserts = new List<KeyValuePair<string, byte[]>>(
            plan.Added.Count + plan.Updated.Count + plan.MetadataChanged.Count + symbolBackfill.Count + 1);

        // Refresh the repository root marker in the same pass that mutates its
        // files. The live file count is the full scanned set, so list_repos can
        // report it without a per-call subtree scan.
        clock = HybridLogicalClock.Tick(clock);
        upserts.Add(new KeyValuePair<string, byte[]>(
            RepoContextKeys.Repo(repoId), BuildRepoNode(repoId, plan.LiveFileCount, ingestToken, clock)));

        foreach (var entry in plan.Added)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry), processedPaths.Contains(entry.RelativePath), ingestToken, clock)));
        }

        foreach (var entry in plan.Updated)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry), processedPaths.Contains(entry.RelativePath), ingestToken, clock)));
        }

        // Metadata-changed files are content-identical, so rewriting their node with
        // a fresh clock advances the ingest anchor (the register order key) without
        // changing any value - the fast-path skips them on the next reconcile. The
        // reconcile did not re-extract them, so their prior symbol-processed marker
        // is carried forward from the stored node rather than recomputed.
        foreach (var entry in plan.MetadataChanged)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry), StoredProcessed(storedMeta, entry.RelativePath), ingestToken, clock)));
        }

        // Symbol back-fill files are content-unchanged, so rewriting their node adds
        // the freshly extracted declared set and stamps the processed marker without
        // otherwise altering the file. Only files the reconcile actually extracted
        // (present in processedPaths) are written, so a file it could not read is
        // left unmarked and retried on the next pass.
        foreach (var entry in symbolBackfill)
        {
            if (!processedPaths.Contains(entry.RelativePath))
            {
                continue;
            }

            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry), symbolsProcessed: true, ingestToken, clock)));
        }

        var deletes = new List<string>(plan.RemovedPaths.Count);
        foreach (var path in plan.RemovedPaths)
        {
            deletes.Add(RepoContextKeys.File(repoId, path));
        }

        // Commit in bounded chunks; each chunk is an all-or-nothing atomic batch
        // keyed by a deterministic operation id so an interrupted run's retry
        // re-attaches to the original saga rather than duplicating writes. Deletes
        // ride with the first chunk so a pure prune still commits atomically.
        var chunkIndex = 0;
        var remainingDeletes = deletes;
        for (var offset = 0; offset < upserts.Count; offset += WriteChunkSize)
        {
            var chunk = upserts.GetRange(offset, Math.Min(WriteChunkSize, upserts.Count - offset));
            var chunkDeletes = remainingDeletes;
            remainingDeletes = [];

            var operationId = BuildOperationId(repoId, chunkIndex, chunk, chunkDeletes);
            await tree.SetManyAtomicAsync(chunk, chunkDeletes, operationId, cancellationToken)
                .ConfigureAwait(false);
            chunkIndex++;
            await ReportAsync(
                progress, new RepoIndexProgressUpdate { ChunksCommitted = chunkIndex }, cancellationToken)
                .ConfigureAwait(false);
        }

        if (remainingDeletes.Count != 0)
        {
            var operationId = BuildOperationId(repoId, chunkIndex, [], remainingDeletes);
            await tree.SetManyAtomicAsync([], remainingDeletes, operationId, cancellationToken)
                .ConfigureAwait(false);
            chunkIndex++;
            await ReportAsync(
                progress, new RepoIndexProgressUpdate { ChunksCommitted = chunkIndex }, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private byte[] BuildFileNode(
        string repoId, RepoFileEntry entry, string? declaredEncoded, bool symbolsProcessed, string ingestToken, HybridLogicalClock clock)
    {
        var node = new FileNode
        {
            RepoId = repoId,
            Path = entry.RelativePath,
            Digest = RepoContextValues.Lww(entry.Digest, clock),
            Language = RepoContextValues.Lww(entry.Language, clock),
            SizeBytes = RepoContextValues.Lww(entry.SizeBytes, clock),
            LastIngested = RepoContextValues.Lww(ingestToken, clock),
        };
        if (!string.IsNullOrEmpty(declaredEncoded))
        {
            node = node with { DeclaredSymbols = RepoContextValues.Lww(declaredEncoded, clock) };
        }

        // Stamp the presence marker that records the file was run through symbol
        // extraction, distinct from the declared set (which is empty for a supported
        // file that happens to declare no symbols). Its presence is what keeps the
        // back-fill scan from re-selecting an already-processed file.
        if (symbolsProcessed)
        {
            node = node with { SymbolsProcessed = RepoContextValues.Lww("1", clock) };
        }

        return _fileNodeSerializer.SerializeToArray(node);
    }

    /// <summary>
    /// Whether the stored node for <paramref name="path"/> already carries the
    /// symbol-processed marker, used to carry that marker forward when an
    /// anchor-refreshed file's node is rewritten without re-extraction.
    /// </summary>
    private static bool StoredProcessed(IReadOnlyDictionary<string, StoredFileMeta> storedMeta, string path) =>
        storedMeta.TryGetValue(path, out var meta) && meta.SymbolsProcessed;

    /// <summary>
    /// Resolves the encoded declared-symbol string a file node should carry, or
    /// <see langword="null"/> when the file declares no symbols (or none were
    /// computed for it this pass).
    /// </summary>
    private static string? DeclaredFor(IReadOnlyDictionary<string, string> declaredEncoded, RepoFileEntry entry) =>
        declaredEncoded.TryGetValue(entry.RelativePath, out var encoded) ? encoded : null;

    /// <summary>
    /// Builds the per-file encoded declared-symbol projection to stamp onto the
    /// rewritten file nodes: the freshly extracted set for every added and updated
    /// file, plus the carried-forward stored set for each content-unchanged
    /// metadata-refreshed file (whose node is rewritten but which the reconcile did
    /// not re-extract, so its prior declared set must be preserved).
    /// </summary>
    private static Dictionary<string, string> BuildDeclaredEncoded(
        IReadOnlyDictionary<string, IReadOnlyList<string>> declaredByPath,
        IReadOnlyList<RepoFileEntry> metadataChanged,
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (path, names) in declaredByPath)
        {
            map[path] = DeclaredSymbolNames.Encode(names);
        }

        foreach (var entry in metadataChanged)
        {
            if (map.ContainsKey(entry.RelativePath))
            {
                continue;
            }

            if (storedMeta.TryGetValue(entry.RelativePath, out var meta) && meta.DeclaredSymbols.Count != 0)
            {
                map[entry.RelativePath] = DeclaredSymbolNames.Encode(meta.DeclaredSymbols);
            }
        }

        return map;
    }

    private byte[] BuildRepoNode(string repoId, int liveFileCount, string ingestToken, HybridLogicalClock clock)
    {
        var node = new RepoNode
        {
            RepoId = repoId,
            LastIngested = RepoContextValues.Lww(ingestToken, clock),
            FileCount = RepoContextValues.Lww(liveFileCount, clock),
        };
        return _repoNodeSerializer.SerializeToArray(node);
    }

    /// <summary>
    /// Derives a deterministic, filesystem-safe operation id from a chunk's exact
    /// keys and content, so an identical retry re-attaches to the original atomic
    /// saga while any genuine content change starts a fresh one.
    /// </summary>
    private static string BuildOperationId(
        string repoId,
        int chunkIndex,
        IReadOnlyList<KeyValuePair<string, byte[]>> upserts,
        IReadOnlyList<string> deletes)
    {
        var builder = new StringBuilder();
        builder.Append(repoId).Append('\n').Append(chunkIndex);
        foreach (var upsert in upserts)
        {
            builder.Append("\nU").Append(upsert.Key).Append('=').Append(FileDigest.Compute(upsert.Value));
        }

        foreach (var delete in deletes)
        {
            builder.Append("\nD").Append(delete);
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(builder.ToString()));
        return "rcb-" + Convert.ToHexStringLower(hash.AsSpan(0, 16));
    }

    private static string PrefixUpperBound(string prefix)
    {
        // The exclusive upper bound of a prefix range is the prefix with its last
        // character incremented, which sorts immediately after every key the
        // prefix covers.
        var last = prefix[^1];
        return string.Concat(prefix.AsSpan(0, prefix.Length - 1), ((char)(last + 1)).ToString());
    }
}
