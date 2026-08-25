using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Microsoft.Extensions.Logging;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// Initial-state queries used by the dashboard before opening a live
/// subscription, plus the helpers that build a
/// <see cref="PartSummaryUpdate"/> / <see cref="ChaosOverview"/> from
/// raw backend state. Components call these once in
/// <c>OnInitializedAsync</c> to seed the UI, then switch to the
/// matching <c>Subscribe*</c> feed for live deltas.
/// </summary>
public sealed partial class DashboardBroadcaster
{
    /// <summary>
    /// Default time-to-live for the memoised initial-parts snapshot. Tests
    /// inject a custom value via the internal ctor for deterministic
    /// memoisation / refresh assertions.
    /// </summary>
    private static readonly TimeSpan DefaultSnapshotCacheTtl = TimeSpan.FromSeconds(1);

    private readonly object _snapshotGate = new();
    private Task<IReadOnlyList<PartSummaryUpdate>>? _cachedSnapshot;
    private DateTime _snapshotExpiresUtc = DateTime.MinValue;

    /// <summary>
    /// Builds a fresh snapshot for every part in the lattice backend.
    /// Components call this in <c>OnInitializedAsync</c> before starting
    /// their live subscription. The result is memoised for the configured
    /// snapshot TTL so concurrent / reconnecting consumers share one build
    /// instead of each re-folding the fact tree; callers still observe their
    /// own <paramref name="cancellationToken"/> while sharing the underlying
    /// build.
    /// </summary>
    public Task<IReadOnlyList<PartSummaryUpdate>> GetInitialPartsAsync(CancellationToken cancellationToken = default)
    {
        Task<IReadOnlyList<PartSummaryUpdate>> snapshot;
        lock (_snapshotGate)
        {
            // Never launch an overlapping build: while a build is in flight,
            // every caller shares it regardless of expiry. Only once it has
            // completed and the TTL has elapsed does the next caller start a
            // fresh one. Expiry is stamped on successful completion (not on
            // start), so a slow build is never treated as instantly stale -
            // which previously let concurrent callers stack up overlapping
            // full rebuilds and saturate the fact tree.
            if (_cachedSnapshot is null ||
                (_cachedSnapshot.IsCompleted && DateTime.UtcNow >= _snapshotExpiresUtc))
            {
                // Bind the shared build to the broadcaster lifetime, not one
                // caller's token, so an abandoning caller can't cancel the
                // build for everyone sharing it.
                var build = BuildAllPartsAsync(_shutdownCts.Token);
                _cachedSnapshot = build;
                _ = build.ContinueWith(
                    static (_, state) =>
                    {
                        var self = (DashboardBroadcaster)state!;
                        lock (self._snapshotGate)
                        {
                            self._snapshotExpiresUtc = DateTime.UtcNow + self._snapshotCacheTtl;
                        }
                    },
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            snapshot = _cachedSnapshot;
        }
        return snapshot.WaitAsync(cancellationToken);
    }

    private async Task<IReadOnlyList<PartSummaryUpdate>> BuildAllPartsAsync(CancellationToken cancellationToken)
    {
        // Read the library-maintained folded compliance view (mfg-compliance)
        // rather than re-folding the whole mfg-facts tree on the request path.
        // The maintainer keeps the view current directly off the source WAL, so
        // a direct SetMany seed (which raises no FactRouted) is materialised
        // without any sample-side upkeep. A host without the view subsystem
        // returns an empty directory.
        var view = await _viewFactory.GetAsync(ComplianceFoldProjection.ViewName, cancellationToken);
        if (view is null)
        {
            return Array.Empty<PartSummaryUpdate>();
        }

        // One contiguous scan of the view: one pre-folded accumulator per part,
        // already skipping the reserved internal accumulator / membership rows
        // (LatticeView starts every scan above the NUL-prefixed range). Each
        // accumulator carries the fact-derived half of the summary - lattice
        // state, latest stage, fact count.
        var parts = new List<(PartSerialNumber Serial, ComplianceAccumulator Accumulator)>();
        await foreach (var entry in view.ScanEntriesAsync(cancellationToken: cancellationToken))
        {
            parts.Add((new PartSerialNumber(entry.Key), Accumulators.Deserialize(entry.Value)));
        }

        if (parts.Count == 0)
        {
            return Array.Empty<PartSummaryUpdate>();
        }

        // Join the baseline half per part. BaselineState cannot be derived from
        // mfg-facts: the baseline backend folds facts in arrival order
        // (FactsInArrivalOrder), deliberately diverging from the HLC-ordered
        // lattice fold - that divergence is the whole point of the demo (the
        // red-row highlight and the WatchDivergence stream). So it is joined
        // from the baseline backend at read time. Bounded fan-out into a
        // pre-sized array avoids opening one concurrent grain call per part.
        var baseline = _router.GetBackend("baseline");
        var results = new PartSummaryUpdate[parts.Count];
        await Parallel.ForAsync(
            0,
            parts.Count,
            new ParallelOptions { MaxDegreeOfParallelism = 32, CancellationToken = cancellationToken },
            async (index, ct) =>
            {
                var (serial, accumulator) = parts[index];
                var baselineState = await baseline.GetStateAsync(serial, ct);
                results[index] = new PartSummaryUpdate
                {
                    Serial = serial,
                    Family = InferFamily(serial),
                    LatestStage = accumulator.LatestStage,
                    BaselineState = baselineState,
                    LatticeState = accumulator.State,
                    FactCount = accumulator.FactCount,
                };
            });

        return results;
    }

    /// <summary>
    /// Resolves the full part directory by scanning the fact tree (keys only,
    /// deduped by serial prefix). Used by the background view-vs-tree
    /// reconciliation pass to discover parts written directly to the tree
    /// (bypassing <see cref="FederationRouter"/>) that the materialised view has
    /// not yet folded.
    /// </summary>
    private async Task<IReadOnlyList<PartSerialNumber>> ResolvePartSerialsAsync(CancellationToken cancellationToken)
    {
        var lattice = _router.GetBackend("lattice");
        return await lattice.ListPartsAsync(cancellationToken);
    }

    /// <summary>Reads the current chaos overview (used by the banner on initial render).</summary>
    public async Task<ChaosOverview> GetChaosOverviewAsync(CancellationToken cancellationToken = default)
    {
        var sites = await _router.ListSitesAsync();
        var backends = await _router.ListBackendChaosAsync();
        var partitioned = await _grainFactory
            .GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey)
            .IsPartitionedAsync();
        var replicationDisconnected = await _grainFactory
            .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey)
            .IsDisconnectedAsync();
        return BuildOverview(sites, backends, partitioned, replicationDisconnected);
    }

    /// <summary>
    /// Returns every part currently in a divergent state - baseline
    /// disagrees with lattice. Used by the <c>WatchDivergence</c> gRPC
    /// stream to seed its initial snapshot before switching to the live
    /// subscription.
    /// </summary>
    public async Task<IReadOnlyList<DivergenceEvent>> GetInitialDivergenceAsync(CancellationToken cancellationToken = default)
    {
        var initial = await GetInitialPartsAsync(cancellationToken);
        var results = new List<DivergenceEvent>();
        foreach (var part in initial)
        {
            if (part.Diverges)
            {
                results.Add(new DivergenceEvent
                {
                    Serial = part.Serial,
                    BaselineState = part.BaselineState,
                    LatticeState = part.LatticeState,
                    Resolved = false,
                });
            }
        }
        return results;
    }

    private async Task<PartSummaryUpdate> BuildSummaryAsync(
        PartSerialNumber serial,
        CancellationToken cancellationToken)
    {
        var baseline = _router.GetBackend("baseline");
        var lattice = _router.GetBackend("lattice");

        // One lattice-tree enumeration per summary: fetch facts once and
        // fold them locally for the lattice state. Opening a second
        // concurrent enumerator (via lattice.GetStateAsync, which itself
        // calls GetFactsAsync) multiplies the pressure on the tree grain.
        // Enumerator aborts (cold-start, scale-down, idle-expiry) are
        // recovered transparently inside LatticeFactBackend via the
        // resilient ScanEntriesAsync wrapper.
        var baselineStateTask = baseline.GetStateAsync(serial, cancellationToken);
        var factsTask = lattice.GetFactsAsync(serial, cancellationToken);
        await Task.WhenAll(baselineStateTask, factsTask);

        var facts = factsTask.Result;
        var latticeState = ComplianceFold.Fold(facts);
        // "Latest stage" reflects the part's furthest-along lifecycle
        // milestone, not just the last ProcessStepCompleted. The facts
        // list is HLC-ascending so the tail is the newest fact; map it to a
        // ProcessStage via the shared ProcessStageMap the folded projection
        // also uses, so an FAI-accepted part reads as FAI rather than
        // whatever its last process step was.
        var latestStage = facts.Count == 0 ? null : ProcessStageMap.Of(facts[^1]);

        return new PartSummaryUpdate
        {
            Serial = serial,
            Family = InferFamily(serial),
            LatestStage = latestStage,
            BaselineState = baselineStateTask.Result,
            LatticeState = latticeState,
            FactCount = facts.Count,
        };
    }

    private static ChaosOverview BuildOverview(
        IReadOnlyList<SiteState> sites,
        IReadOnlyList<BackendChaosState> backends,
        bool partitionActive,
        bool replicationDisconnected)
    {
        var paused = 0;
        var delayed = 0;
        var reordering = 0;
        foreach (var site in sites)
        {
            if (site.Config.IsPaused) paused++;
            if (site.Config.DelayMs > 0) delayed++;
            if (site.Config.ReorderEnabled) reordering++;
        }

        var flaky = new List<string>();
        foreach (var backend in backends)
        {
            if (backend.Config != BackendChaosConfig.Nominal)
            {
                flaky.Add(backend.Name);
            }
        }

        return new ChaosOverview
        {
            PausedSites = paused,
            DelayedSites = delayed,
            ReorderingSites = reordering,
            FlakyBackends = flaky,
            PartitionActive = partitionActive,
            ReplicationDisconnected = replicationDisconnected,
        };
    }

    private static string InferFamily(PartSerialNumber serial)
    {
        var value = serial.Value;
        var lastDash = value.LastIndexOf('-');
        if (lastDash <= 0)
        {
            return value;
        }
        var yearDash = value.LastIndexOf('-', lastDash - 1);
        return yearDash > 0 ? value[..yearDash] : value;
    }
}
