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
    private readonly RepoContextContentReconciler _contentReconciler;
    private readonly ISymbolExtractor _symbolExtractor;
    private readonly RepoContextWorkspaceGuard _workspaceGuard;
    private readonly TimeProvider _timeProvider;
    private readonly RepoContextIndexingOptions _options;
    private readonly ILogger<RepoContextBootstrapService> _logger;
    private readonly IRepoContextSourceScanner? _sourceScanner;

    /// <summary>
    /// The per-repository cross-walk pruning cache, keyed by repository id. Each entry
    /// holds the directory-modification-time snapshot the previous walk observed and the
    /// wall-clock tick of the last full (unpruned) sweep. It lives only in this singleton's
    /// memory, so a process restart starts every repository cold - the first post-restart
    /// walk is a full one, which is correct by construction.
    /// </summary>
    private readonly ConcurrentDictionary<string, PruneCacheEntry> _pruneCache = new(StringComparer.Ordinal);

    /// <summary>
    /// Repositories already told, once each, that their full sweeps are paced by pass
    /// count rather than by the wall clock the operator configured (issue #2048). Logging
    /// it every pass would drown the useful signal, and the condition is a standing
    /// property of the deployment rather than an event.
    /// </summary>
    private readonly ConcurrentDictionary<string, byte> _fullWalkPacingReported = new(StringComparer.Ordinal);

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
    /// <param name="contentReconciler">The per-file content-projection reconciler that
    /// projects and prunes searchable body text for changed and removed files, so the
    /// keyword search path can rank over file content. Must not be
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
    /// <param name="sourceScanner">An optional source strategy that supplies the
    /// run's scan set instead of the filesystem walk. A git-ref-sourced run uses it
    /// to enumerate the resolved commit's tree, which makes the reconcile's
    /// add / modify / delete changeset exact rather than inferred from absence on
    /// disk. <see langword="null"/> (the default) means every run walks the tree,
    /// which is the mounted-workspace behaviour.</param>
    public RepoContextBootstrapService(
        IGrainFactory grainFactory,
        Serializer<FileNode> fileNodeSerializer,
        Serializer<RepoNode> repoNodeSerializer,
        IRepoContextVectorIngestor vectorIngestor,
        RepoContextSymbolReconciler symbolReconciler,
        RepoContextContentReconciler contentReconciler,
        ISymbolExtractor symbolExtractor,
        RepoContextWorkspaceGuard workspaceGuard,
        TimeProvider timeProvider,
        RepoContextIndexingOptions options,
        ILogger<RepoContextBootstrapService> logger,
        IRepoContextSourceScanner? sourceScanner = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(fileNodeSerializer);
        ArgumentNullException.ThrowIfNull(repoNodeSerializer);
        ArgumentNullException.ThrowIfNull(vectorIngestor);
        ArgumentNullException.ThrowIfNull(symbolReconciler);
        ArgumentNullException.ThrowIfNull(contentReconciler);
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
        _contentReconciler = contentReconciler;
        _symbolExtractor = symbolExtractor;
        _workspaceGuard = workspaceGuard;
        _timeProvider = timeProvider;
        _options = options;
        _logger = logger;
        _sourceScanner = sourceScanner;
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
            // forced when the repository is cold (no snapshot) or the full-walk
            // deadline has arrived, so an in-place content edit - which does not bump
            // a directory's modification time and is invisible to pruning - is still
            // caught within that bound.
            //
            // The deadline is counted in PASSES, not wall clock (issue #2048). The
            // reconcile is single-flight, so the real spacing between two walks is the
            // larger of the configured spacing and the previous pass's duration. On a
            // repository whose pass outruns the configured spacing - the ordinary case
            // once it is large enough to want pruning at all - a wall-clock deadline is
            // therefore already past on arrival at every single pass, so every pass
            // forced a full sweep and the prune snapshot was written each run and read
            // for nothing. Counting passes makes RepoContextIndexingOptions.PruningCanEngage
            // genuinely sufficient rather than merely necessary: the deadline no longer
            // depends on how long a pass takes.
            var nowTicks = _timeProvider.GetUtcNow().UtcTicks;
            _pruneCache.TryGetValue(repoId, out var priorPrune);
            var lastFullSweepTicks = priorPrune?.LastFullSweepTicks ?? 0;
            var passesSinceFullSweep = priorPrune?.PassesSinceFullSweep ?? 0;
            var forceFull = !request.AllowPrune
                || priorPrune?.DirectoryMtimes is not { Count: > 0 }
                || passesSinceFullSweep + 1 >= _options.PassesPerFullWalk;
            var pruning = new RepoWalkPruning
            {
                PreviousDirectoryMtimes = priorPrune?.DirectoryMtimes,
                ForceFull = forceFull,
            };

            // The embedding-gap scan cadence (issue #2049). Re-probing the whole
            // content-unchanged set costs two membership reads per indexed source, so
            // on a converged repository it dominates the pass while finding nothing.
            // Scan when the run is an explicit onboarding, when the caller knows a
            // vector is missing, while the repository has not yet been observed
            // converged, or when the periodic cadence is due. A converged repository
            // still heals promptly: the self-index grain's out-of-band paged sweep
            // sets ForceEmbeddingGapScan the moment it finds a real gap.
            var coverageConverged = priorPrune?.CoverageConverged ?? false;
            var passesSinceGapScan = priorPrune?.PassesSinceGapScan ?? 0;
            var gapScanDue = !request.AllowPrune
                || request.ForceEmbeddingGapScan
                || !coverageConverged
                || passesSinceGapScan + 1 >= _options.PassesPerEmbeddingGapScan;

            // The walk is synchronous, so a run with a progress sink drives a
            // concurrent pump that samples the running processed-file count and
            // reports it. The walker only writes the latest count (a single lock-free
            // volatile write per processed file); the pump owns every grain report,
            // so FilesScanned climbs during the walk instead of staying frozen at
            // zero, and reports never reorder or pile up.
            IReadOnlyList<RepoFileEntry> scanned;
            var commitScan = _sourceScanner?.TryScan(request, cancellationToken);
            if (commitScan is not null)
            {
                // A git-ref-sourced run: the scan set is the resolved commit's tree,
                // read from the object database. Nothing is stat-ed, nothing is
                // pruned, and "stored but not scanned" means "deleted in this commit"
                // rather than "missing from the mount right now", which is what makes
                // the delta exact and the removal non-destructive.
                scanned = commitScan;
                await ReportAsync(
                    progress,
                    new RepoIndexProgressUpdate { Phase = RepoIndexPhase.Walking, FilesScanned = scanned.Count },
                    cancellationToken).ConfigureAwait(false);
            }
            else if (progress is null)
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
            // last-full-sweep marker, and reset the pass counter, only when this run
            // actually forced a full sweep.
            var updatedSnapshot = new PruneCacheEntry(
                pruning.CurrentDirectoryMtimes,
                forceFull ? nowTicks : lastFullSweepTicks,
                forceFull ? 0 : passesSinceFullSweep + 1,
                gapScanDue ? 0 : passesSinceGapScan + 1,
                coverageConverged);

            // A full sweep whose wall clock overran the operator's configured interval
            // is the observable symptom of issue #2048: passes are slower than the
            // spacing the interval was written against, so the wall-clock reading of
            // FullWalkInterval cannot be honoured and the pass count is what holds the
            // bound. Say so once per repository rather than silently diverging from
            // what the configuration reads like.
            if (forceFull
                && _options.PruningCanEngage
                && lastFullSweepTicks > 0
                && nowTicks - lastFullSweepTicks > _options.FullWalkInterval.Ticks
                && _fullWalkPacingReported.TryAdd(repoId, 0))
            {
                _logger.LogInformation(
                    "Repo {RepoId}: full sweeps are paced by pass count, not wall clock - {Passes} pass(es) took "
                    + "{Elapsed} ms against a configured full-walk interval of {Configured} ms. Pruning still "
                    + "engages on the intervening passes; widen LATTICE_RECONCILE_INTERVAL_SECONDS or "
                    + "LATTICE_FULL_WALK_INTERVAL_SECONDS if the wall-clock bound matters more than the pass count.",
                    repoId,
                    passesSinceFullSweep + 1,
                    (nowTicks - lastFullSweepTicks) / TimeSpan.TicksPerMillisecond,
                    (long)_options.FullWalkInterval.TotalMilliseconds);
            }

            _logger.LogInformation(
                "Repo {RepoId}: scan complete - {Scanned} files in {Elapsed} ms ({Mode}; pruned {PrunedDirs} dir(s), {PrunedFiles} file(s)).",
                repoId, scanned.Count, stopwatch.ElapsedMilliseconds,
                commitScan is not null
                    ? "commit " + request.CommitSha
                    : forceFull ? "full sweep" : "pruned",
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

            // The content back-fill self-heal: content-unchanged text files whose node
            // was never content-processed (it predates the content projection, or a
            // prior run stopped before the content phase). Projecting them lets a
            // repository indexed before this feature converge on a complete content
            // projection without re-reading the files that already have one. Like the
            // symbol back-fill it is drawn from the pure-unchanged set only, and it is a
            // different file set (content covers every text file; symbols only supported
            // languages), so the two lists are unified before the file nodes are written
            // to guarantee each node is rewritten exactly once.
            var contentBackfill = SelectContentBackfill(plan.Unchanged, storedMeta);

            // The cross-reference back-fill self-heal: content-unchanged,
            // symbol-processed files whose node was never cross-referenced (it predates
            // the reverse cross-reference index). Their symbol records already carry
            // the outbound references, but because their content never changes the
            // incremental delta never fires, so their reverse edges are never built -
            // leaving inbound-dependent and test lookups permanently empty for a
            // pre-existing index. Force-seeding the reverse edges from the stored
            // records converges the index without re-parsing the files, and it is drawn
            // from the pure-unchanged set only so a back-filled node is written exactly
            // once. It is unified with the other back-fills so a file selected by more
            // than one has its node rewritten a single time with every marker resolved.
            var xrefBackfill = SelectXrefBackfill(plan.Unchanged, storedMeta);
            var backfill = UnifyBackfill(symbolBackfill, contentBackfill, xrefBackfill);

            // Always log the reconcile plan, including a fully converged no-op pass.
            // Gating this line on "did anything change" is what made a converged pass
            // (0 added / 0 updated / 0 removed, legitimately nothing to do) and a pass
            // that never measured (a skipped arm) indistinguishable in the log
            // (#2088): a reader polling for a "0 added, 0 updated, 0 removed" line to
            // confirm convergence was waiting for a line that by construction never
            // appeared. On a diagnostic path, "measured nothing" must be positively
            // reported rather than inferred from silence. Counting the chunks is a pure
            // tally, so it is safe to compute on a no-op pass; the apply work below
            // stays gated so a no-op still commits nothing.
            var chunksTotal = ComputeChunkCount(plan, backfill.Count);
            _logger.LogInformation(
                "Repo {RepoId}: plan - {Added} added, {Updated} updated, {MetadataChanged} anchor-refreshed, {Removed} removed, {Unchanged} unchanged, {SymbolBackfill} symbol back-fill, {ContentBackfill} content back-fill, {XrefBackfill} xref back-fill; {Chunks} chunk(s) to commit.",
                repoId, plan.Added.Count, plan.Updated.Count, plan.MetadataChanged.Count, plan.RemovedPaths.Count, plan.Unchanged.Count, symbolBackfill.Count, contentBackfill.Count, xrefBackfill.Count, chunksTotal);

            if (!plan.IsNoOp || backfill.Count > 0)
            {
                phase = RepoIndexPhase.Applying;
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

                // Force-seed the reverse cross-reference edges for the back-fill files
                // whose symbol records were populated before the reverse index existed.
                // It runs after the symbol reconcile so any records that pass upserted
                // are already in place, and before the file nodes are rewritten so a
                // crash between the seed and the node write leaves the file without its
                // cross-referenced marker and the next run re-selects it (the seed is
                // idempotent, so re-driving it is safe). Freshly symbol-processed files
                // (added, updated, or symbol back-fill) already have their reverse edges
                // built by the incremental delta above, so only the xref-only back-fill
                // set is seeded here.
                var crossSeededPaths = await _symbolReconciler.SeedCrossReferencesAsync(
                    repoId, xrefBackfill, storedMeta, cancellationToken)
                    .ConfigureAwait(false);

                // Reconcile the per-file content projection in the same pass, before the
                // file nodes are rewritten. It is decoupled from embeddings on purpose -
                // its whole point is to give the keyword/degraded search path file
                // content to rank over - and resumable for the same reason as symbols: a
                // node written without the content marker is re-selected by the content
                // back-fill next pass.
                var contentResult = await _contentReconciler.ReconcileAsync(
                    repoId, repoRoot, plan.Added, plan.Updated, plan.RemovedPaths, contentBackfill, cancellationToken)
                    .ConfigureAwait(false);
                await ReportAsync(
                    progress,
                    new RepoIndexProgressUpdate { FilesContentProjected = contentResult.ContentCaptured },
                    cancellationToken).ConfigureAwait(false);

                var declaredEncoded = BuildDeclaredEncoded(
                    symbolResult.DeclaredByPath, plan.MetadataChanged, backfill, storedMeta);

                // Exactly the files each reconcile processed (supported and readable)
                // are stamped with the matching processed marker, so a file it could not
                // read is not marked and is retried on the next pass.
                var symbolProcessedPaths = new HashSet<string>(symbolResult.DeclaredByPath.Keys, StringComparer.Ordinal);
                var contentProcessedPaths = contentResult.ProcessedPaths;

                // The files whose reverse edges are live after this pass: those the
                // xref back-fill force-seeded, plus every file the symbol reconcile
                // freshly processed (its edges were built by the incremental delta), so
                // both sets stamp the cross-referenced marker and neither is re-selected.
                var crossReferencedPaths = new HashSet<string>(crossSeededPaths, StringComparer.Ordinal);
                crossReferencedPaths.UnionWith(symbolProcessedPaths);

                await ApplyPlanAsync(
                    tree, repoId, plan, backfill, declaredEncoded, symbolProcessedPaths, contentProcessedPaths, crossReferencedPaths, contentResult.TokenCountsByPath, storedMeta, request.CommitSha, progress, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                // A git-ref-sourced generation stamps its commit even when the plan is
                // a no-op, so the anchor an operator (and every spoke) reads always
                // names the revision actually served - a commit that only touched
                // filtered-out paths still moves the anchor forward.
                if (!string.IsNullOrWhiteSpace(request.CommitSha))
                {
                    await tree.SetAsync(
                        RepoContextKeys.Repo(repoId),
                        BuildRepoNode(
                            repoId,
                            plan.LiveFileCount,
                            DateTimeOffset.UtcNow.ToString("O"),
                            request.CommitSha,
                            HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
                        cancellationToken).ConfigureAwait(false);
                }

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

            // Offer the unchanged set only when a gap scan is due. When it is not, the
            // changed files still embed - they are re-embedded whatever their coverage
            // says - and the whole-corpus coverage probe is simply not paid for.
            var unchangedOffered = gapScanDue
                ? unchangedForBackfill
                : (IReadOnlyList<RepoFileEntry>)Array.Empty<RepoFileEntry>();
            await ReportAsync(progress, new RepoIndexProgressUpdate { Phase = RepoIndexPhase.Vectorising }, cancellationToken)
                .ConfigureAwait(false);
            if (gapScanDue)
            {
                _logger.LogInformation(
                    "Repo {RepoId}: vectorising {Changed} changed file(s); scanning {Unchanged} unchanged for embedding gaps.",
                    repoId, changed.Count, unchangedForBackfill.Count);
            }
            else
            {
                _logger.LogInformation(
                    "Repo {RepoId}: vectorising {Changed} changed file(s); skipping the embedding-gap scan over "
                    + "{Unchanged} unchanged file(s) - coverage was last observed complete and the next scheduled "
                    + "scan is {Remaining} pass(es) away.",
                    repoId,
                    changed.Count,
                    unchangedForBackfill.Count,
                    Math.Max(0, _options.PassesPerEmbeddingGapScan - (passesSinceGapScan + 1)));
            }

            var lastVectorisingHeartbeat = 0;

            // Failures are collected rather than swallowed. The first is rethrown
            // once every arm has had its turn, so the run is still reported as
            // failed and retried - it just no longer costs the other arms their
            // pass. Silently reporting success here would be the exact
            // partial-view defect this sweep exists to remove.
            Exception? armFailure = null;

            var embedded = 0;
            var fileIngest = RepoFileVectorIngestOutcome.None;
            try
            {
                fileIngest = await _vectorIngestor.IngestAsync(
                    repoId,
                    repoRoot,
                    changed,
                    unchangedOffered,
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
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                armFailure = ex;
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: file vectorisation did not complete this pass; continuing with the "
                    + "remaining embedding arms and retrying it on the next reconcile.",
                    repoId);
            }

            embedded = fileIngest.FilesEmbedded;

            // Convergence is only ever asserted from a pass that actually looked. A
            // deferred scan carries the previous verdict forward unchanged; a failed
            // arm, a failed coverage probe, or any gap found clears it, so the next
            // pass scans again until the repository is observed clean once more.
            if (gapScanDue)
            {
                var converged = armFailure is null && fileIngest.Converged;
                updatedSnapshot = updatedSnapshot with { CoverageConverged = converged };
                if (converged != coverageConverged)
                {
                    _logger.LogInformation(
                        converged
                            ? "Repo {RepoId}: embedding coverage is complete; the gap scan now runs every "
                              + "{Passes} pass(es) unless a gap is detected out of band."
                            : "Repo {RepoId}: embedding coverage is incomplete; the gap scan runs on every pass "
                              + "until it is clean (cadence would otherwise be {Passes} pass(es)).",
                        repoId,
                        _options.PassesPerEmbeddingGapScan);
                }
            }

            await ReportAsync(progress, new RepoIndexProgressUpdate { FilesEmbedded = embedded }, cancellationToken)
                .ConfigureAwait(false);

            // Embed the per-symbol records as their own passages. This runs even when
            // the structural plan was a no-op: a symbol upserted or pruned this pass is
            // refreshed or retired, and any symbol still lacking a live embedding - a
            // repository captured before symbol embedding existed - is back-filled, so
            // symbol-level recall converges without a re-walk.
            // Each embedding arm is attempted independently. A transient failure
            // in one - characteristically a response timeout while the vector
            // trees are still replaying - must not veto the others, because every
            // arm is idempotent and back-fills whatever it missed on the next
            // pass. Before this, a symbol-arm timeout aborted the whole run, and
            // on a large repository that happened on EVERY pass: measured on a
            // real deployment, 65 runs started, 65 failed, 0 completed, so the
            // memory arm below never executed once and the feature could not
            // converge in production at all.
            try
            {
                var symbolsEmbedded = await _vectorIngestor.IngestSymbolsAsync(
                    repoId, changedSymbolKeys, prunedSymbolKeys, cancellationToken)
                    .ConfigureAwait(false);

                // Log the symbol-embedding tally unconditionally, including the zero
                // case. Suppressing zero (#2088) made "this pass embedded no symbol
                // passages because the set was already converged" indistinguishable
                // from "the symbol arm never ran": on a healthy steady-state
                // repository the tally is legitimately 0 on almost every pass, so the
                // suppressed line was exactly the evidence an operator needed to
                // confirm the arm executed and found nothing to do.
                _logger.LogInformation(
                    "Repo {RepoId}: embedded {Symbols} symbol passage(s).", repoId, symbolsEmbedded);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                armFailure ??= ex;
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: symbol vectorisation did not complete this pass; continuing with the "
                    + "remaining embedding arms and retrying it on the next reconcile.",
                    repoId);
            }

            // Embed the durable agent-memory entries as their own passages, so a
            // natural-language search ranks captured decisions, gotchas and
            // conventions alongside code instead of silently omitting them
            // (issue #1878). Like symbols, this runs even when the structural
            // plan was a no-op, and back-fills any entry lacking a live
            // embedding - which is what converts a store captured entirely
            // before memory embedding existed, with no re-walk.
            //
            // Memory is written through the tools rather than the walk, so the
            // ingestor gets no per-pass changed set. The change signal instead
            // comes from the write side: RepoContextStore retires an entry's
            // vector on every remember, update, and forget (both the hard delete
            // and the lapse), so a revised entry looks un-embedded and this
            // back-fill re-embeds it from its current text on the next reconcile.
            // That needs no digest and no dirty-set.
            //
            // RESIDUAL, stated rather than hidden: an entry that expires by its
            // own TTL rather than through an explicit forget - a coordination
            // handoff written with ttlSeconds, say - vanishes from the tree with
            // no code path observing it, so its vector is never retired. That is
            // fail-safe: the semantic path drops a hit that no longer hydrates
            // via its !entry.Exists guard, so the cost is an inflated membership
            // tally and an occasional wasted ranking slot, never a dead key
            // returned to a caller. A prune IS possible - VectorMetadataRecord
            // keeps each vector's SourceKey - but not free: memory source ids
            // are not separable from file and symbol ids in the membership set,
            // so it would take a full metadata scan on a path that otherwise
            // touches only what changed. Left for a deliberate sweep rather than
            // paid on every reconcile.
            try
            {
                var memoryEmbedded = await _vectorIngestor.IngestMemoryAsync(
                    repoId, Array.Empty<string>(), Array.Empty<string>(), cancellationToken)
                    .ConfigureAwait(false);
                if (memoryEmbedded > 0)
                {
                    _logger.LogInformation(
                        "Repo {RepoId}: embedded {Entries} memory passage(s).", repoId, memoryEmbedded);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                armFailure ??= ex;
                _logger.LogWarning(
                    ex,
                    "Repo {RepoId}: memory vectorisation did not complete this pass; it will be retried on "
                    + "the next reconcile.",
                    repoId);
            }

            // Every arm has had its turn. If any failed, surface the first failure
            // so the run is reported as failed and re-driven; the arms that did
            // succeed keep their work either way.
            if (armFailure is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(armFailure).Throw();
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
    /// snapshot the last successful walk observed, the wall-clock tick of the last full
    /// (unpruned) sweep, and the pass counters that pace the periodic full sweep and the
    /// periodic embedding-gap scan.
    /// </summary>
    /// <param name="DirectoryMtimes">The repository-relative directory to modification-time
    /// snapshot from the last successful walk.</param>
    /// <param name="LastFullSweepTicks">The UTC tick at which the last full sweep ran. Kept
    /// for diagnostics only: the deadline itself is counted in passes, because pass
    /// duration - not the configured interval - sets the real spacing between two walks
    /// (issue #2048).</param>
    /// <param name="PassesSinceFullSweep">How many consented passes have completed since
    /// the last full sweep. The next pass forces a full sweep once this reaches
    /// <see cref="RepoContextIndexingOptions.PassesPerFullWalk"/> minus one.</param>
    /// <param name="PassesSinceGapScan">How many consented passes have completed since the
    /// last whole-repository embedding-gap scan, pacing it against
    /// <see cref="RepoContextIndexingOptions.PassesPerEmbeddingGapScan"/>.</param>
    /// <param name="CoverageConverged">Whether the last gap scan that actually ran proved
    /// every content-unchanged file has a live vector. While false the scan runs on every
    /// pass, so a repository still filling in its embeddings is never throttled.</param>
    private sealed record PruneCacheEntry(
        IReadOnlyDictionary<string, long> DirectoryMtimes,
        long LastFullSweepTicks,
        int PassesSinceFullSweep,
        int PassesSinceGapScan,
        bool CoverageConverged);

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
    /// Selects the content back-fill candidates from the content-unchanged set: text
    /// files whose stored node was never stamped as content-processed, plus files
    /// projected before the token-count register existed (their stored token count is
    /// negative). These are files indexed before the content projection - or the token
    /// count - existed (or a file a prior run stopped short of processing), so
    /// projecting them lets a pre-existing index converge on a complete content
    /// projection and complete token counts without re-reading the files that already
    /// have both. Every walked file is a text file (the walk excludes binary), so -
    /// unlike the symbol back-fill - there is no language filter. Drawing only from the
    /// pure-unchanged set guarantees a back-filled node is written exactly once per
    /// pass.
    /// </summary>
    /// <param name="unchanged">The content-unchanged files from the plan.</param>
    /// <param name="storedMeta">The stored per-file metadata, consulted for the
    /// content-processed marker and the stored token count.</param>
    /// <returns>The unchanged files eligible for content back-fill.</returns>
    private static List<RepoFileEntry> SelectContentBackfill(
        IReadOnlyList<RepoFileEntry> unchanged, IReadOnlyDictionary<string, StoredFileMeta> storedMeta)
    {
        var backfill = new List<RepoFileEntry>();
        foreach (var entry in unchanged)
        {
            // Select a file whose content was never projected, and also one that was
            // projected before the token-count register existed (its stored count is
            // negative): re-reading it now computes and stamps the missing count, so a
            // pre-existing index converges on complete token counts. A file already
            // carrying both markers is skipped, so the migration re-reads each file at
            // most once.
            if (!StoredContentProcessed(storedMeta, entry.RelativePath)
                || StoredTokenCount(storedMeta, entry.RelativePath) is null)
            {
                backfill.Add(entry);
            }
        }

        return backfill;
    }

    /// <summary>
    /// Selects the cross-reference back-fill candidates from the content-unchanged set:
    /// supported-language files whose stored node was stamped symbol-processed but never
    /// cross-referenced. These are files indexed before the reverse cross-reference
    /// index existed - their symbol records already carry the outbound references, but
    /// because their content never changes the incremental delta never rebuilds the
    /// reverse edges, so the reverse index stays empty for them. Force-seeding those
    /// edges from the stored records lets a pre-existing index converge on a complete
    /// reverse projection without re-parsing the files. The language filter mirrors the
    /// symbol back-fill (only supported files ever declare symbols), and drawing only
    /// from the pure-unchanged set guarantees a back-filled node is written exactly once
    /// per pass.
    /// </summary>
    /// <param name="unchanged">The content-unchanged files from the plan.</param>
    /// <param name="storedMeta">The stored per-file metadata, consulted for the
    /// symbol-processed and cross-referenced markers.</param>
    /// <returns>The unchanged files eligible for cross-reference back-fill.</returns>
    private List<RepoFileEntry> SelectXrefBackfill(
        IReadOnlyList<RepoFileEntry> unchanged, IReadOnlyDictionary<string, StoredFileMeta> storedMeta)
    {
        var backfill = new List<RepoFileEntry>();
        foreach (var entry in unchanged)
        {
            // A file must be symbol-processed (so its records and stored references
            // exist) yet not cross-referenced (so its reverse edges were never built)
            // to be a seed candidate. A file still awaiting symbol back-fill is covered
            // by the symbol path, which builds its reverse edges through the delta, so
            // it is intentionally excluded here.
            if (_symbolExtractor.Supports(entry.Language)
                && StoredProcessed(storedMeta, entry.RelativePath)
                && !StoredCrossReferenced(storedMeta, entry.RelativePath))
            {
                backfill.Add(entry);
            }
        }

        return backfill;
    }

    /// <summary>
    /// Unifies the symbol, content, and cross-reference back-fill lists into a single
    /// distinct set of files (by repository-relative path), so a file selected by more
    /// than one back-fill has its node rewritten exactly once with every marker
    /// resolved. Order is preserved from the symbol list first, then the content-only
    /// additions, then the cross-reference-only additions.
    /// </summary>
    /// <param name="symbolBackfill">The symbol back-fill candidates.</param>
    /// <param name="contentBackfill">The content back-fill candidates.</param>
    /// <param name="xrefBackfill">The cross-reference back-fill candidates.</param>
    /// <returns>The distinct union of the three lists.</returns>
    private static List<RepoFileEntry> UnifyBackfill(
        IReadOnlyList<RepoFileEntry> symbolBackfill,
        IReadOnlyList<RepoFileEntry> contentBackfill,
        IReadOnlyList<RepoFileEntry> xrefBackfill)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var unified = new List<RepoFileEntry>(
            symbolBackfill.Count + contentBackfill.Count + xrefBackfill.Count);
        foreach (var entry in symbolBackfill)
        {
            if (seen.Add(entry.RelativePath))
            {
                unified.Add(entry);
            }
        }

        foreach (var entry in contentBackfill)
        {
            if (seen.Add(entry.RelativePath))
            {
                unified.Add(entry);
            }
        }

        foreach (var entry in xrefBackfill)
        {
            if (seen.Add(entry.RelativePath))
            {
                unified.Add(entry);
            }
        }

        return unified;
    }
    /// <summary>
    /// Computes the number of durable write chunks a reconciliation pass takes
    /// for a plan: one per <see cref="WriteChunkSize"/> upserts, where the upserts
    /// are the repository root marker plus every added, updated, and
    /// anchor-refreshed file, plus every back-fill file whose node is being
    /// rewritten. Deletes ride with the first chunk (the marker guarantees at least
    /// one), so they add no chunk of their own.
    /// </summary>
    /// <param name="plan">The reconciliation plan.</param>
    /// <param name="backfillCount">The number of back-fill files whose nodes
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
        var endExclusive = RepoContextPortability.PrefixUpperBound(prefix);
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
                    RepoContextValues.ReadString(node.SymbolsProcessed) is not null,
                    RepoContextValues.ReadString(node.ContentProcessed) is not null,
                    RepoContextValues.ReadInt64(node.TokenCount) ?? -1,
                    RepoContextValues.ReadString(node.CrossReferenced) is not null);
            }
        }

        return meta;
    }

    private async Task ApplyPlanAsync(
        ILattice tree,
        string repoId,
        RepoContextBootstrapPlan plan,
        IReadOnlyList<RepoFileEntry> backfill,
        IReadOnlyDictionary<string, string> declaredEncoded,
        IReadOnlySet<string> symbolProcessedPaths,
        IReadOnlySet<string> contentProcessedPaths,
        IReadOnlySet<string> crossReferencedPaths,
        IReadOnlyDictionary<string, int> tokenCountsByPath,
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta,
        string? commitSha,
        IRepoIndexProgressSink? progress,
        CancellationToken cancellationToken)
    {
        var ingestToken = DateTimeOffset.UtcNow.ToString("O");
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        var upserts = new List<KeyValuePair<string, byte[]>>(
            plan.Added.Count + plan.Updated.Count + plan.MetadataChanged.Count + backfill.Count + 1);

        // Refresh the repository root marker in the same pass that mutates its
        // files. The live file count is the full scanned set, so list_repos can
        // report it without a per-call subtree scan.
        clock = HybridLogicalClock.Tick(clock);
        upserts.Add(new KeyValuePair<string, byte[]>(
            RepoContextKeys.Repo(repoId), BuildRepoNode(repoId, plan.LiveFileCount, ingestToken, commitSha, clock)));

        foreach (var entry in plan.Added)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry),
                    symbolProcessedPaths.Contains(entry.RelativePath),
                    contentProcessedPaths.Contains(entry.RelativePath),
                    ResolveTokenCount(entry.RelativePath, contentProcessedPaths.Contains(entry.RelativePath), tokenCountsByPath, storedMeta),
                    crossReferencedPaths.Contains(entry.RelativePath),
                    ingestToken, clock)));
        }

        foreach (var entry in plan.Updated)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry),
                    symbolProcessedPaths.Contains(entry.RelativePath),
                    contentProcessedPaths.Contains(entry.RelativePath),
                    ResolveTokenCount(entry.RelativePath, contentProcessedPaths.Contains(entry.RelativePath), tokenCountsByPath, storedMeta),
                    crossReferencedPaths.Contains(entry.RelativePath),
                    ingestToken, clock)));
        }

        // Metadata-changed files are content-identical, so rewriting their node with
        // a fresh clock advances the ingest anchor (the register order key) without
        // changing any value - the fast-path skips them on the next reconcile. Neither
        // reconcile re-processed them, so their prior symbol- and content-processed
        // markers are carried forward from the stored node rather than recomputed.
        foreach (var entry in plan.MetadataChanged)
        {
            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, entry.RelativePath),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry),
                    StoredProcessed(storedMeta, entry.RelativePath),
                    StoredContentProcessed(storedMeta, entry.RelativePath),
                    ResolveTokenCount(entry.RelativePath, false, tokenCountsByPath, storedMeta),
                    StoredCrossReferenced(storedMeta, entry.RelativePath),
                    ingestToken, clock)));
        }

        // Back-fill files are content-unchanged, so rewriting their node stamps
        // whichever processed markers this pass earned (freshly, from the symbol or
        // content reconcile) or carries forward from the stored node, without
        // otherwise altering the file. A node is written only when at least one
        // reconcile actually processed the file this pass; a file neither reconcile
        // could read is left untouched and retried on the next pass.
        foreach (var entry in backfill)
        {
            var path = entry.RelativePath;
            var symbolNow = symbolProcessedPaths.Contains(path);
            var contentNow = contentProcessedPaths.Contains(path);
            var crossNow = crossReferencedPaths.Contains(path);
            if (!symbolNow && !contentNow && !crossNow)
            {
                continue;
            }

            clock = HybridLogicalClock.Tick(clock);
            upserts.Add(new KeyValuePair<string, byte[]>(
                RepoContextKeys.File(repoId, path),
                BuildFileNode(repoId, entry, DeclaredFor(declaredEncoded, entry),
                    symbolNow || StoredProcessed(storedMeta, path),
                    contentNow || StoredContentProcessed(storedMeta, path),
                    ResolveTokenCount(path, contentNow, tokenCountsByPath, storedMeta),
                    crossNow || StoredCrossReferenced(storedMeta, path),
                    ingestToken, clock)));
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
        string repoId, RepoFileEntry entry, string? declaredEncoded, bool symbolsProcessed, bool contentProcessed, long? tokenCount, bool crossReferenced, string ingestToken, HybridLogicalClock clock)
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

        // Stamp the content-processed presence marker, which keeps the content
        // back-fill scan from re-selecting a file whose body text was already
        // projected into the content tree.
        if (contentProcessed)
        {
            node = node with { ContentProcessed = RepoContextValues.Lww("1", clock) };
        }

        // Stamp the per-file token count - either freshly computed this pass or carried
        // forward from the stored node - so a full-node rewrite never drops it. A null
        // count (a brand-new file the reconcile could not read, or a legacy node with
        // none stored) leaves the register empty for the back-fill to fill later.
        if (tokenCount is { } tokens)
        {
            node = node with { TokenCount = RepoContextValues.Lww(tokens, clock) };
        }

        // Stamp the cross-referenced presence marker, which keeps the cross-reference
        // back-fill scan from re-selecting a file whose declared symbols' reverse edges
        // were already projected into the cross-reference index.
        if (crossReferenced)
        {
            node = node with { CrossReferenced = RepoContextValues.Lww("1", clock) };
        }

        return _fileNodeSerializer.SerializeToArray(node);
    }

    /// <summary>
    /// Whether the stored node for <paramref name="path"/> already carries the
    /// symbol-processed marker, used to carry that marker forward when an
    /// anchor-refreshed or content-only back-filled file's node is rewritten without
    /// symbol re-extraction.
    /// </summary>
    private static bool StoredProcessed(IReadOnlyDictionary<string, StoredFileMeta> storedMeta, string path) =>
        storedMeta.TryGetValue(path, out var meta) && meta.SymbolsProcessed;

    /// <summary>
    /// Whether the stored node for <paramref name="path"/> already carries the
    /// content-processed marker, used to carry that marker forward when an
    /// anchor-refreshed or symbol-only back-filled file's node is rewritten without
    /// content re-projection.
    /// </summary>
    private static bool StoredContentProcessed(IReadOnlyDictionary<string, StoredFileMeta> storedMeta, string path) =>
        storedMeta.TryGetValue(path, out var meta) && meta.ContentProcessed;

    /// <summary>
    /// Whether the stored node for <paramref name="path"/> already carries the
    /// cross-referenced marker, used both to keep the cross-reference back-fill from
    /// re-selecting an already-seeded file and to carry that marker forward when an
    /// anchor-refreshed or other back-filled file's node is rewritten without
    /// re-seeding its reverse edges.
    /// </summary>
    private static bool StoredCrossReferenced(IReadOnlyDictionary<string, StoredFileMeta> storedMeta, string path) =>
        storedMeta.TryGetValue(path, out var meta) && meta.CrossReferenced;

    /// <summary>
    /// The token count stored for <paramref name="path"/>'s node, or
    /// <see langword="null"/> when none was recorded (a node written before the
    /// token-count register existed, whose stored count is negative). Used both to
    /// re-select such a node for the content back-fill and to carry its count forward
    /// when a node is rewritten without re-reading the file.
    /// </summary>
    private static long? StoredTokenCount(IReadOnlyDictionary<string, StoredFileMeta> storedMeta, string path) =>
        storedMeta.TryGetValue(path, out var meta) && meta.TokenCount >= 0 ? meta.TokenCount : null;

    /// <summary>
    /// Resolves the token count to stamp on a rewritten file node: the count freshly
    /// computed by the content reconcile this pass when the file was content-processed,
    /// otherwise the count carried forward from the stored node (which is
    /// <see langword="null"/> for a legacy or brand-new node with none stored, leaving
    /// the register empty for a later back-fill).
    /// </summary>
    /// <param name="path">The repository-relative file path.</param>
    /// <param name="contentProcessedThisPass">Whether the content reconcile projected
    /// this file this pass (so a fresh count exists for it).</param>
    /// <param name="freshCounts">The per-path token counts computed this pass.</param>
    /// <param name="storedMeta">The stored per-file metadata, consulted for a
    /// carry-forward count.</param>
    private static long? ResolveTokenCount(
        string path,
        bool contentProcessedThisPass,
        IReadOnlyDictionary<string, int> freshCounts,
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta) =>
        contentProcessedThisPass && freshCounts.TryGetValue(path, out var fresh)
            ? fresh
            : StoredTokenCount(storedMeta, path);

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
    /// file, plus the carried-forward stored set for each content-unchanged file
    /// whose node is rewritten but which the symbol reconcile did not re-extract - the
    /// anchor-refreshed metadata-changed files and the back-fill files (a content-only
    /// back-fill file has no fresh symbol extraction, so its prior declared set must
    /// be preserved rather than blanked by the full-node overwrite).
    /// </summary>
    private static Dictionary<string, string> BuildDeclaredEncoded(
        IReadOnlyDictionary<string, IReadOnlyList<string>> declaredByPath,
        IReadOnlyList<RepoFileEntry> metadataChanged,
        IReadOnlyList<RepoFileEntry> backfill,
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (path, names) in declaredByPath)
        {
            map[path] = DeclaredSymbolNames.Encode(names);
        }

        CarryForwardStoredDeclared(map, metadataChanged, storedMeta);
        CarryForwardStoredDeclared(map, backfill, storedMeta);
        return map;
    }

    /// <summary>
    /// Carries forward the stored declared-symbol set for each file in
    /// <paramref name="entries"/> that this pass did not freshly extract, so a
    /// full-node overwrite of a file whose symbols were not re-read preserves its
    /// prior declared set instead of blanking it.
    /// </summary>
    private static void CarryForwardStoredDeclared(
        Dictionary<string, string> map,
        IReadOnlyList<RepoFileEntry> entries,
        IReadOnlyDictionary<string, StoredFileMeta> storedMeta)
    {
        foreach (var entry in entries)
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
    }

    private byte[] BuildRepoNode(
        string repoId, int liveFileCount, string ingestToken, string? commitSha, HybridLogicalClock clock)
    {
        var node = new RepoNode
        {
            RepoId = repoId,
            LastIngested = RepoContextValues.Lww(ingestToken, clock),
            FileCount = RepoContextValues.Lww(liveFileCount, clock),

            // Left unset for a mounted-workspace run: a mount has no verifiable
            // revision, and writing a placeholder would make the anchor a lie.
            IndexedCommit = string.IsNullOrWhiteSpace(commitSha)
                ? new BoundedRegister()
                : RepoContextValues.Lww(commitSha, clock),
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

}
