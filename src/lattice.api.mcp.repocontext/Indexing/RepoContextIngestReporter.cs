using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Publishes the repository-context <b>ingest</b> instrument family: files scanned,
/// the per-file plan outcome, files embedded, symbols embedded, files
/// content-projected, reconcile passes by outcome, pass duration, and the age of the
/// newest completed pass.
/// <para>
/// <b>Why this exists (issue #3151).</b> Before this family, an embedder idle
/// because ingest was COMPLETE and one idle because ingest was WEDGED were
/// indistinguishable from <c>/metrics</c>: both showed no embed-related series
/// moving and a healthy readiness probe. The only thing that separated them was the
/// <c>repocontext_index_status</c> MCP tool, which a dashboard or an alert rule
/// cannot call and which is not always present in an operator's toolset. These
/// instruments carry the counters that tool already reports, so a no-change
/// reconcile reads from <c>/metrics</c> alone as "scanned non-zero, embedded zero,
/// pass completed", and a stalled ingest reads as a
/// <see cref="LastCompletedPassAgeInstrumentName"/> series that climbs without
/// bound while <see cref="PassesInstrumentName"/> stops advancing.
/// </para>
/// <para>
/// <b>Counters advance while a pass is still running.</b> The per-file and
/// per-symbol counters are fed from the run's progress reports as they arrive, not
/// once at the end, so a long symbol back-fill is visible as a rising
/// <see cref="SymbolsEmbeddedInstrumentName"/> rather than as silence until it
/// finishes. Each report carries a run-cumulative figure; the pass converts it to a
/// delta against its own high-water mark, so a figure reported twice, or a smaller
/// figure reported after a larger one, is never counted twice or subtracted.
/// </para>
/// <para>
/// <b>Priming is per repository, on first sight.</b> The <c>repository</c> tag is
/// runtime-valued, so the constructor cannot know which series to mint. Every
/// series for a repository is minted at zero when its first pass BEGINS on this
/// silo, before any figure is recorded, so from then on an arm reading zero is a
/// measured zero rather than an arm that never fired. A repository with no series
/// at all has not started a pass on this silo, which is a statement about
/// placement, not about the arm.
/// </para>
/// <para>
/// <b>Cardinality.</b> The <c>repository</c> tag is bounded by the number of
/// repositories onboarded on this host, an operator-chosen set, following the
/// precedent of <c>repocontext.ann.build.slice</c>. It is the tag that makes a
/// stall attributable: a silo-wide "last completed pass" would let a healthy
/// repository hide a wedged one. No path, symbol key, or file content is ever
/// carried.
/// </para>
/// </summary>
internal sealed class RepoContextIngestReporter : IDisposable
{
    /// <summary>Files the walk scanned, cumulative across passes.</summary>
    internal const string FilesScannedInstrumentName = "repocontext.ingest.files_scanned";

    /// <summary>Files the reconcile planned, partitioned by plan outcome.</summary>
    internal const string FilesInstrumentName = "repocontext.ingest.files";

    /// <summary>Files whose vectors were stored.</summary>
    internal const string FilesEmbeddedInstrumentName = "repocontext.ingest.files_embedded";

    /// <summary>Symbol passages whose vectors were stored.</summary>
    internal const string SymbolsEmbeddedInstrumentName = "repocontext.ingest.symbols_embedded";

    /// <summary>Files whose searchable content projection was written.</summary>
    internal const string FilesContentProjectedInstrumentName = "repocontext.ingest.files_content_projected";

    /// <summary>Reconcile passes, partitioned by how the pass ended.</summary>
    internal const string PassesInstrumentName = "repocontext.ingest.passes";

    /// <summary>Wall-clock seconds one reconcile pass took, by how it ended.</summary>
    internal const string PassDurationInstrumentName = "repocontext.ingest.pass.duration";

    /// <summary>Seconds since the newest completed pass, per repository.</summary>
    internal const string LastCompletedPassAgeInstrumentName = "repocontext.ingest.last_completed_pass_age";

    /// <summary>The tag key naming the repository.</summary>
    internal const string RepositoryTagKey = "repository";

    /// <summary>The tag key carrying an outcome partition.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>The plan outcome for a newly ingested file.</summary>
    internal const string FileAddedTag = "added";

    /// <summary>The plan outcome for a file whose record was rewritten.</summary>
    internal const string FileUpdatedTag = "updated";

    /// <summary>The plan outcome for a stored file pruned from the index.</summary>
    internal const string FileRemovedTag = "removed";

    /// <summary>The plan outcome for a file left untouched.</summary>
    internal const string FileUnchangedTag = "unchanged";

    /// <summary>The pass outcome for a pass that reconciled the whole tree.</summary>
    internal const string PassCompletedTag = "completed";

    /// <summary>The pass outcome for a pass that stopped on an error.</summary>
    internal const string PassFailedTag = "failed";

    /// <summary>
    /// The pass outcome for a pass cancelled by host shutdown or repository removal.
    /// Counted in its own arm so a shutdown is never read as a failure, and so the
    /// three arms together account for every pass that began.
    /// </summary>
    internal const string PassCancelledTag = "cancelled";

    // Declared above every instrument it constructs, and every instrument is built
    // from this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _filesScanned;
    private readonly Counter<long> _files;
    private readonly Counter<long> _filesEmbedded;
    private readonly Counter<long> _symbolsEmbedded;
    private readonly Counter<long> _filesContentProjected;
    private readonly Counter<long> _passes;
    private readonly Histogram<double> _passDuration;

    private readonly Lock _gate = new();

    // The clock pass durations and the age gauge measure against, injectable so a
    // test pins elapsed time deterministically. Monotonic timestamps, not
    // wall-clock, so a clock adjustment cannot fabricate or erase a stall.
    private readonly TimeProvider _time;
    private readonly Dictionary<string, RepoContextIngestRepository> _repositories = new(StringComparer.Ordinal);

    // DECLARED LAST, BELOW _gate, _time AND _repositories, AND THAT IS LOAD-BEARING.
    // An observable instrument's callback runs during instrument publication as well
    // as on every later collection, and field initialisers run in declaration order,
    // so an observable declared above the state its callback reads observes that
    // state as null and takes its own series off the wire. See
    // ObservableInstrumentDeclarationOrderTests and the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly ObservableGauge<double> _lastCompletedPassAge;

    /// <summary>Creates the reporter and its instruments.</summary>
    /// <param name="timeProvider">
    /// The clock durations and ages are measured against. Defaults to
    /// <see cref="TimeProvider.System"/>; supplied by tests.
    /// </param>
    public RepoContextIngestReporter(TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _filesScanned = _meter.CreateCounter<long>(
            FilesScannedInstrumentName,
            unit: "{file}",
            description:
                "Files the repository-context indexing walk discovered after filtering, cumulative across "
                + "reconcile passes and tagged by 'repository'. Advances while a pass is still walking. A "
                + "converged repository still advances this on every pass, because every pass re-scans the "
                + "tree: scanned rising with files_embedded flat is a completed no-change reconcile, not a "
                + "stall (issue #3151).");
        _files = _meter.CreateCounter<long>(
            FilesInstrumentName,
            unit: "{file}",
            description:
                "Files the reconcile planned, partitioned by 'outcome': 'added' (newly ingested), 'updated' "
                + "(record rewritten), 'removed' (stored file pruned) or 'unchanged' (left untouched), and "
                + "tagged by 'repository'. All four arms are primed at zero when a repository's first pass "
                + "begins on this silo.");
        _filesEmbedded = _meter.CreateCounter<long>(
            FilesEmbeddedInstrumentName,
            unit: "{file}",
            description:
                "Files whose vectors were stored by the file embedding arm, tagged by 'repository'. The "
                + "headline 'is embedding happening' series. Advances while a pass is still vectorising.");
        _symbolsEmbedded = _meter.CreateCounter<long>(
            SymbolsEmbeddedInstrumentName,
            unit: "{symbol}",
            description:
                "Symbol passages whose vectors were stored by the symbol embedding arm, tagged by "
                + "'repository'. The symbol arm can run long after file coverage is complete, so this series "
                + "rising while files_embedded is flat is a healthy back-fill, not a stall.");
        _filesContentProjected = _meter.CreateCounter<long>(
            FilesContentProjectedInstrumentName,
            unit: "{file}",
            description:
                "Files whose searchable content projection was written (added, updated and back-filled "
                + "files), tagged by 'repository'.");
        _passes = _meter.CreateCounter<long>(
            PassesInstrumentName,
            unit: "{pass}",
            description:
                "Repository-context reconcile passes, partitioned by 'outcome': 'completed' (reconciled the "
                + "whole tree), 'failed' (stopped on an error) or 'cancelled' (host shutdown or repository "
                + "removal), and tagged by 'repository'. The three arms account for every pass that began; "
                + "all three are primed at zero on a repository's first pass on this silo.");
        _passDuration = _meter.CreateHistogram<double>(
            PassDurationInstrumentName,
            unit: "s",
            description:
                "Wall-clock seconds one repository-context reconcile pass took, from the moment the runner "
                + "began it to the moment it settled, tagged by 'repository' and by the same 'outcome' as "
                + "repocontext.ingest.passes. Not primed: a primed histogram would fabricate a zero-second "
                + "pass.");

        _lastCompletedPassAge = _meter.CreateObservableGauge(
            LastCompletedPassAgeInstrumentName,
            ObserveLastCompletedPassAge,
            unit: "s",
            description:
                "Seconds since the newest completed reconcile pass for each 'repository' this silo has begun "
                + "a pass for, or - until one completes - since the first pass began. The alertable 'ingest "
                + "has stalled' signal (issue #3151): on a healthy repository it stays below the reconcile "
                + "interval plus one pass duration, and it climbs without bound while passes fail, hang or "
                + "never finish. On a multi-silo host read the minimum across silos, because a repository "
                + "whose job grain moved keeps its last reading growing on the silo it left.");
    }

    /// <summary>The meter the family is published on. Exposed for test listeners.</summary>
    internal Meter Meter => _meter;

    /// <summary>
    /// Begins accounting for one reconcile pass, minting every series for the
    /// repository at zero if this is its first pass on this silo.
    /// </summary>
    /// <param name="repoId">The repository the pass reconciles. Must not be <see langword="null"/>.</param>
    /// <returns>The pass, which the runner feeds progress into and settles exactly once.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    public RepoContextIngestPass BeginPass(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        var started = _time.GetTimestamp();
        return new RepoContextIngestPass(this, ResolveRepository(repoId, started), started);
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();

    /// <summary>
    /// Records the growth in each run-cumulative figure on <paramref name="update"/>
    /// beyond <paramref name="pass"/>'s high-water marks.
    /// </summary>
    internal void Observe(RepoContextIngestPass pass, in RepoIndexProgressUpdate update)
    {
        var r = pass.Repository.Tag;
        lock (pass.Sync)
        {
            if (pass.Settled)
            {
                return;
            }

            // One emission site per instrument arm, each naming its tag constant
            // literally, so the repository-wide priming gate can resolve every arm.
            var scanned = RepoContextIngestPass.Advance(ref pass.FilesScanned, update.FilesScanned);
            if (scanned > 0)
            {
                _filesScanned.Add(scanned, r, LatticeTenantLabel.Platform);
            }

            var added = RepoContextIngestPass.Advance(ref pass.FilesAdded, update.FilesAdded);
            if (added > 0)
            {
                _files.Add(added, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileAddedTag), LatticeTenantLabel.Platform);
            }

            var updated = RepoContextIngestPass.Advance(ref pass.FilesUpdated, update.FilesUpdated);
            if (updated > 0)
            {
                _files.Add(updated, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileUpdatedTag), LatticeTenantLabel.Platform);
            }

            var removed = RepoContextIngestPass.Advance(ref pass.FilesRemoved, update.FilesRemoved);
            if (removed > 0)
            {
                _files.Add(removed, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileRemovedTag), LatticeTenantLabel.Platform);
            }

            var unchanged = RepoContextIngestPass.Advance(ref pass.FilesUnchanged, update.FilesUnchanged);
            if (unchanged > 0)
            {
                _files.Add(unchanged, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileUnchangedTag), LatticeTenantLabel.Platform);
            }

            var embedded = RepoContextIngestPass.Advance(ref pass.FilesEmbedded, update.FilesEmbedded);
            if (embedded > 0)
            {
                _filesEmbedded.Add(embedded, r, LatticeTenantLabel.Platform);
            }

            var symbols = RepoContextIngestPass.Advance(ref pass.SymbolsEmbedded, update.SymbolsEmbedded);
            if (symbols > 0)
            {
                _symbolsEmbedded.Add(symbols, r, LatticeTenantLabel.Platform);
            }

            var projected = RepoContextIngestPass.Advance(ref pass.FilesContentProjected, update.FilesContentProjected);
            if (projected > 0)
            {
                _filesContentProjected.Add(projected, r, LatticeTenantLabel.Platform);
            }
        }
    }

    /// <summary>Settles <paramref name="pass"/> as completed, if it has not settled already.</summary>
    internal void Complete(RepoContextIngestPass pass)
    {
        if (!pass.TrySettle())
        {
            return;
        }

        var now = _time.GetTimestamp();
        var seconds = _time.GetElapsedTime(pass.StartedTimestamp, now).TotalSeconds;
        var r = pass.Repository.Tag;
        lock (_gate)
        {
            pass.Repository.LastCompletedTimestamp = now;
        }

        _passes.Add(1, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassCompletedTag), LatticeTenantLabel.Platform);
        _passDuration.Record(seconds, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassCompletedTag), LatticeTenantLabel.Platform);
    }

    /// <summary>Settles <paramref name="pass"/> as failed, if it has not settled already.</summary>
    internal void Fail(RepoContextIngestPass pass)
    {
        if (!pass.TrySettle())
        {
            return;
        }

        var seconds = _time.GetElapsedTime(pass.StartedTimestamp).TotalSeconds;
        var r = pass.Repository.Tag;
        _passes.Add(1, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassFailedTag), LatticeTenantLabel.Platform);
        _passDuration.Record(seconds, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassFailedTag), LatticeTenantLabel.Platform);
    }

    /// <summary>Settles <paramref name="pass"/> as cancelled, if it has not settled already.</summary>
    internal void Cancel(RepoContextIngestPass pass)
    {
        if (!pass.TrySettle())
        {
            return;
        }

        var seconds = _time.GetElapsedTime(pass.StartedTimestamp).TotalSeconds;
        var r = pass.Repository.Tag;
        _passes.Add(1, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassCancelledTag), LatticeTenantLabel.Platform);
        _passDuration.Record(seconds, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassCancelledTag), LatticeTenantLabel.Platform);
    }

    private RepoContextIngestRepository ResolveRepository(string repoId, long now)
    {
        lock (_gate)
        {
            if (_repositories.TryGetValue(repoId, out var known))
            {
                return known;
            }

            var repository = new RepoContextIngestRepository(
                new KeyValuePair<string, object?>(RepositoryTagKey, repoId), now);
            _repositories[repoId] = repository;

            // Pre-mint every counter series for the repository with a zero-valued add,
            // written out one arm at a time with literal tag constants: the priming
            // gate reads emission sites syntactically, so an arm primed through a loop
            // variable would be invisible to it.
            var r = repository.Tag;
            _filesScanned.Add(0, r, LatticeTenantLabel.Platform);
            _files.Add(0, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileAddedTag), LatticeTenantLabel.Platform);
            _files.Add(0, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileUpdatedTag), LatticeTenantLabel.Platform);
            _files.Add(0, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileRemovedTag), LatticeTenantLabel.Platform);
            _files.Add(0, r, new KeyValuePair<string, object?>(OutcomeTagKey, FileUnchangedTag), LatticeTenantLabel.Platform);
            _filesEmbedded.Add(0, r, LatticeTenantLabel.Platform);
            _symbolsEmbedded.Add(0, r, LatticeTenantLabel.Platform);
            _filesContentProjected.Add(0, r, LatticeTenantLabel.Platform);
            _passes.Add(0, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassCompletedTag), LatticeTenantLabel.Platform);
            _passes.Add(0, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassFailedTag), LatticeTenantLabel.Platform);
            _passes.Add(0, r, new KeyValuePair<string, object?>(OutcomeTagKey, PassCancelledTag), LatticeTenantLabel.Platform);
            return repository;
        }
    }

    /// <summary>
    /// The age-gauge callback: one reading per repository this silo has begun a pass
    /// for, measured from its newest completed pass or, until one completes, from its
    /// first pass beginning.
    /// </summary>
    private IEnumerable<Measurement<double>> ObserveLastCompletedPassAge()
    {
        var now = _time.GetTimestamp();

        lock (_gate)
        {
            var measurements = new List<Measurement<double>>(_repositories.Count);
            foreach (var repository in _repositories.Values)
            {
                var since = repository.LastCompletedTimestamp ?? repository.FirstSeenTimestamp;
                measurements.Add(new Measurement<double>(
                    _time.GetElapsedTime(since, now).TotalSeconds,
                    repository.Tag,
                    LatticeTenantLabel.Platform));
            }

            return measurements;
        }
    }
}
