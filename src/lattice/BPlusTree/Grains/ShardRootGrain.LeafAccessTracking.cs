using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Timers;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.BPlusTree.Grains;

// Leaf-access tracking on the shard root (issue #332, F-012).
//
// Problem. LeafCacheGrain is a [StatelessWorker] read-through cache. After a
// silo restart every cache activation is cold, so the first read of each leaf
// pays activation plus a full delta pull from the primary leaf. On a tree with
// a skewed read distribution that is a latency spike concentrated exactly on
// the leaves that matter most.
//
// Oracle. Rather than a recency list, this ranks leaves by observed read
// frequency: each routed cache read increments the target leaf's visit count in
// a bounded histogram (see LeafAccessFrequencyModel). That asks "what fraction
// of reads land here?" instead of "what happened to be touched last", which is
// the right question for a B+ tree under a skewed or cyclic key distribution -
// a leaf touched once just before shutdown outranks a genuinely hot leaf under
// LRU, but not under frequency. Measured on held-out synthetic traces,
// frequency recovered 96% of the true hot set on a skewed trace and 98% on a
// cyclic one, against 56% and 53% for recency.
//
// A first-order Markov chain ranked by personalised PageRank was built and
// measured first; it never beat the plain histogram (see the "Why not a Markov
// chain" note on LeafAccessFrequencyModel) and cost roughly 100 KB resident per
// activation, so the transition rows were removed.
//
// Why the shard root drives it. ShardRootGrain is the *only* caller of
// ILeafCacheGrain, so (a) it observes every cache-routed read and can build the
// histogram with no new plumbing, and (b) the [StatelessWorker] activations it
// creates during warm-up land on its own silo - the same silo that will serve
// the subsequent reads. No silo-lifecycle infrastructure is needed.
//
// Hot path. Disabled (`LeafCachePreWarmCount = 0`) costs two predictable,
// never-taken branches and zero allocations. Enabled - which is the DEFAULT,
// since `DefaultLeafCachePreWarmCount` is 8 - costs one null check plus an
// O(1), allocation-free record into a bounded dictionary. The read path never
// awaits a storage write: a coalescing grain timer
// (`LatticeOptions.LeafAccessModelFlushIntervalMs`, default 30 s) persists a
// compact snapshot, and clean deactivation flushes once more. This mirrors the
// dirty-leaf coalescing design in ShardRootGrain.DirtyLeaves.cs exactly.
//
// Loss bound. An ungraceful silo kill loses at most one flush window of
// observations. The model self-heals from live traffic, and a missing model
// simply pre-warms nothing - it can never produce a wrong answer, only a less
// useful one.
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Maximum concurrent leaf-cache priming RPCs issued by one shard root's
    /// warm-up. Deliberately smaller than
    /// <c>LatticeGrain.MaxWarmUpParallelism</c> (32) because that gate bounds
    /// the *cluster-wide* shard fan-out, and every shard root multiplies
    /// against it: 32 shards x 8 leaves is already 256 in-flight priming calls.
    /// </summary>
    private const int MaxLeafPreWarmParallelism = 8;

    /// <summary>
    /// The live histogram, or <see langword="null"/> when leaf-access tracking
    /// is disabled or not yet initialized. Read on the hot path, so it is a
    /// plain field check rather than an options lookup.
    /// </summary>
    private LeafAccessFrequencyModel? _leafAccessModel;

    /// <summary>
    /// Set once the first read has resolved the feature's settings, so a
    /// disabled shard never re-enters the slow initialization path.
    /// </summary>
    private bool _leafAccessInitialized;

    /// <summary>Effective settings, resolved once during initialization.</summary>
    private LeafAccessTrackingSettings _leafAccessSettings = LeafAccessTrackingSettings.Disabled;

    /// <summary>Coalescing persistence timer; null when unarmed or disabled.</summary>
    private IDisposable? _leafAccessFlushTimer;

    /// <summary>Guards against overlapping model flushes across turns.</summary>
    private bool _leafAccessFlushInFlight;

    /// <summary>
    /// Consecutive failed model flushes on this activation. Reset by any flush that
    /// lands, so only an unbroken run of failures counts toward suspension.
    /// </summary>
    private int _leafAccessFlushConsecutiveFailures;

    /// <summary>
    /// Latched once the model flush loop has given up for this activation. Checked by
    /// <see cref="EnsureLeafAccessFlushTimerArmed"/> so warm-up cannot silently re-arm
    /// a loop that has already been suspended.
    /// </summary>
    private bool _leafAccessFlushSuspended;

    /// <summary>
    /// Builds the metric tag set for this feature's instruments:
    /// <c>(tree, shard, tenant)</c>, matching every other shard-root
    /// instrument so a telemetry query joins across them cleanly.
    /// <para>
    /// Deliberately built per call rather than reusing the activation-cached
    /// <c>GetMetricTags()</c>. Both emission sites are cold - the throttled
    /// model flush (seconds apart at best) and the once-per-activation
    /// pre-warm - so the array allocation is immaterial, while naming
    /// <see cref="LatticeTenantLabel.ForTree(string)"/> at the site keeps the
    /// tenant dimension directly verifiable by the tenant-dimension hygiene
    /// gate instead of needing an allow-list entry.
    /// </para>
    /// </summary>
    private KeyValuePair<string, object?>[] LeafAccessMetricTags() =>
    [
        new(LatticeMetrics.TagTree, TreeId),
        new(LatticeMetrics.TagShard, ShardIndex),
        LatticeTenantLabel.ForTree(TreeId),
    ];

    /// <summary>
    /// Records that a routed read resolved to <paramref name="leafId"/>.
    /// <para>
    /// This is the hot path. When the feature is disabled - which is NOT the
    /// default, since <see cref="LatticeOptions.LeafCachePreWarmCount"/> defaults
    /// to 8 - the whole method is two predictable branches that allocate nothing.
    /// When
    /// enabled, the steady state is one null check plus an O(1) record.
    /// Aggressively inlined so a disabled shard pays no call overhead either.
    /// </para>
    /// </summary>
    /// <param name="leafId">The leaf whose cache served (or will serve) the read.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordLeafAccess(GrainId leafId)
    {
        var model = _leafAccessModel;
        if (model is not null)
        {
            model.Record(leafId);
            return;
        }
        if (!_leafAccessInitialized)
        {
            InitializeLeafAccessTrackingSlow(leafId);
        }
    }

    /// <summary>
    /// One-time initialization on the first routed read: resolves the feature
    /// settings, restores any persisted histogram from shard state, and arms the
    /// coalescing flush timer.
    /// <para>
    /// Never inlined - it runs at most once per activation and its bulk would
    /// otherwise bloat every traversal call site. Failures are swallowed and
    /// the feature is left disabled for the activation: pre-warm is an
    /// optimization, and no read may fail because of it.
    /// </para>
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void InitializeLeafAccessTrackingSlow(GrainId leafId)
    {
        // Set first: a throw below must not re-enter this path on every read.
        _leafAccessInitialized = true;
        try
        {
            // TreeId parses the grain key, which can throw on a malformed key
            // in a unit-test harness - hence the surrounding try/catch.
            _leafAccessSettings = optionsResolver.GetLeafAccessTrackingSettings(TreeId);
            if (!_leafAccessSettings.IsEnabled)
            {
                return;
            }

            var model = LeafAccessFrequencyModel.Restore(state.State.LeafAccessModel);
            _leafAccessModel = model;
            EnsureLeafAccessFlushTimerArmed();
            model.Record(leafId);
        }
        catch (Exception ex)
        {
            _leafAccessModel = null;
            logger.LogDebug(ex,
                "Could not initialize leaf-access tracking on shard {ShardKey}; leaf-cache pre-warm is inactive for this activation.",
                context.GrainId.Key.ToString());
        }
    }

    /// <summary>
    /// Arms the coalescing model-flush timer on first use. Idempotent. A flush
    /// interval of zero leaves the timer unarmed, so the model is persisted only
    /// on clean deactivation.
    /// <para>
    /// Wrapped in try/catch for the same reason as the dirty-leaf timer: a unit
    /// test with a substituted <see cref="IGrainContext"/> has no grain runtime
    /// to register a timer against, and the in-memory tracking path must still
    /// be exercisable there.
    /// </para>
    /// </summary>
    private void EnsureLeafAccessFlushTimerArmed()
    {
        if (_leafAccessFlushTimer is not null) return;
        if (_leafAccessFlushSuspended) return;

        var intervalMs = _leafAccessSettings.FlushIntervalMs;
        if (intervalMs <= 0) return;

        try
        {
            var period = TimeSpan.FromMilliseconds(intervalMs);
            _leafAccessFlushTimer = this.RegisterGrainTimer(
                OnLeafAccessFlushTimerTickAsync,
                new GrainTimerCreationOptions(dueTime: period, period: period));
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex,
                "Could not register leaf-access model flush timer on shard {ShardKey} (likely a test harness without a grain runtime); falling back to flush-on-deactivate.",
                context.GrainId.Key.ToString());
        }
    }

    private async Task OnLeafAccessFlushTimerTickAsync(CancellationToken cancellationToken)
    {
        if (_leafAccessModel is not { IsDirty: true }) return;
        try
        {
            await FlushLeafAccessModelAsync();
            _leafAccessFlushConsecutiveFailures = 0;
        }
        catch (Exception ex)
        {
            _leafAccessFlushConsecutiveFailures++;
            if (_leafAccessFlushConsecutiveFailures < MaxConsecutiveFlushFailures)
            {
                logger.LogDebug(ex,
                    "Coalesced leaf-access model flush failed for shard {ShardKey} ({FailureCount} of {FailureCeiling} consecutive); will retry on next tick.",
                    context.GrainId.Key.ToString(),
                    _leafAccessFlushConsecutiveFailures,
                    MaxConsecutiveFlushFailures);
                return;
            }

            // Give up for this activation. Debug was the only report this loop
            // ever made, so a permanently-failing shard root was invisible above
            // the storage provider (issue 2419); the suspension warning is the
            // operator-visible signal, and it fires once rather than per tick.
            _leafAccessFlushSuspended = true;
            _leafAccessFlushTimer?.Dispose();
            _leafAccessFlushTimer = null;
            ReportFlushRetriesSuspended("leaf-access", ex);
        }
    }

    /// <summary>
    /// Captures a bounded snapshot of the histogram into shard state and
    /// persists it in one write. No-op when the model is absent, unchanged, or a
    /// flush is already in flight.
    /// </summary>
    /// <remarks>
    /// Retry classification (issue 2419). A failed write falls into two classes
    /// that call for opposite handling, and the distinction is observable in the
    /// exception type:
    /// <list type="bullet">
    ///   <item><description>
    ///     A transient storage fault (contention, a locked database file) may
    ///     well succeed next tick, so the window is <b>kept</b> dirty and
    ///     retried.
    ///   </description></item>
    ///   <item><description>
    ///     An <see cref="Orleans.Storage.InconsistentStateException"/> means this
    ///     activation's ETag no longer matches the stored row. Nothing this
    ///     activation can write will ever match it again, so retrying the same
    ///     window is futile - it is <b>dropped</b> instead. That is exactly what
    ///     the deactivation flush below already does with the same failure, for
    ///     the reason stated there: the model is an optimization whose loss
    ///     costs a colder next start and never a wrong answer.
    ///   </description></item>
    /// </list>
    /// Both classes still propagate to the caller, so both count toward the
    /// consecutive-failure ceiling. Dropping the window must not silently
    /// disarm the bound - a permanently poisoned activation stops writing
    /// because it hit the ceiling, not because each individual window was
    /// quietly discarded.
    /// </remarks>
    private async Task FlushLeafAccessModelAsync()
    {
        var model = _leafAccessModel;
        if (model is null || !model.IsDirty || _leafAccessFlushInFlight) return;

        _leafAccessFlushInFlight = true;
        try
        {
            state.State.LeafAccessModel = model.CaptureSnapshot();
            try
            {
                await WriteShardStateAsync();
            }
            catch (InconsistentStateException)
            {
                // Drop this window: see the classification note above. Marking
                // it persisted is a statement about retry policy, not about
                // durability - the observations are gone, and the model rebuilds
                // itself from live traffic.
                model.MarkPersisted();
                throw;
            }

            // Only clear the dirty flag once the write actually landed, so a
            // failed flush is retried rather than silently dropped.
            model.MarkPersisted();
            LatticeMetrics.LeafAccessModelLeaves.Record(model.TrackedLeafCount, LeafAccessMetricTags());
        }
        finally
        {
            _leafAccessFlushInFlight = false;
        }
    }

    /// <summary>
    /// Final model flush invoked from <c>OnDeactivateAsync</c>. Disposes the
    /// timer first so it cannot fire mid-deactivation, then persists. Failures
    /// are logged and swallowed - losing the model costs a colder next start,
    /// never correctness, and must not block deactivation.
    /// </summary>
    private async Task FlushLeafAccessModelOnDeactivateAsync(CancellationToken cancellationToken)
    {
        _leafAccessFlushTimer?.Dispose();
        _leafAccessFlushTimer = null;

        if (_leafAccessModel is not { IsDirty: true }) return;
        try
        {
            await FlushLeafAccessModelAsync();
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex,
                "Final leaf-access model flush failed for shard {ShardKey} during deactivation; the model will rebuild from live traffic after the next activation.",
                context.GrainId.Key.ToString());
        }
    }

    /// <summary>
    /// Primes the top-ranked leaf caches for this shard, best-effort. Invoked
    /// from <see cref="WarmUpAsync"/> after the root node has been activated.
    /// <para>
    /// Ranks the persisted histogram by visit count, then fans out
    /// bounded <c>PreWarmAsync</c> calls through the same
    /// <c>ResolveLeafCacheGrain</c> path the read hot path uses, so the
    /// activations created are the very ones the reads will hit. Every
    /// individual failure is swallowed: a leaf that has since been merged away,
    /// or a silo that rejects the activation, must never fail warm-up.
    /// </para>
    /// </summary>
    private async Task PreWarmLeafCachesAsync()
    {
        LeafAccessTrackingSettings settings;
        LeafAccessFrequencyModel model;
        try
        {
            settings = optionsResolver.GetLeafAccessTrackingSettings(TreeId);
            if (!settings.IsEnabled) return;

            // Warm-up may be the very first call on this activation, before any
            // read has restored the model - restore it here so a cold silo
            // (exactly the case this feature exists for) still has a ranking.
            model = _leafAccessModel ??= LeafAccessFrequencyModel.Restore(state.State.LeafAccessModel);
            _leafAccessInitialized = true;
            _leafAccessSettings = settings;

            // Warm-up is normally the FIRST call on a fresh activation, so it -
            // not the first read - is what brings tracking online. Arm the
            // coalescing flush timer here too, otherwise everything this
            // activation learns would survive only a clean deactivation, and an
            // unclean silo kill is precisely the restart this feature exists to
            // soften. Idempotent, and a no-op at a zero flush interval.
            EnsureLeafAccessFlushTimerArmed();
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex,
                "Could not resolve leaf-cache pre-warm settings for shard {ShardKey}; skipping pre-warm.",
                context.GrainId.Key.ToString());
            return;
        }

        var tags = LeafAccessMetricTags();
        var startTicks = Environment.TickCount64;

        var ranked = model.RankTopLeaves(settings.PreWarmCount);
        if (ranked.Length == 0)
        {
            // Pre-warm is ENABLED but has nothing to rank, which is the case the
            // feature most needs to be visible in: the access-frequency model
            // reaches disk on a clean deactivation or a coalescing flush, so an
            // unclean silo kill - precisely the restart this feature exists to
            // soften - can leave no ranking and turn every later pre-warm into a
            // silent no-op.
            //
            // Record the duration observation anyway. That is what makes the two
            // states distinguishable WITHOUT enabling debug logging: a non-zero
            // observation count on cache_prewarm_duration with a flat-zero
            // cache_prewarmed total means "ran, ranked nothing", while no
            // observations at all means the pre-warm never ran. Returning early
            // recorded neither, so an operator could not tell a disabled feature
            // from a dead one.
            LatticeMetrics.LeafCachePreWarmDurationMs.Record(Environment.TickCount64 - startTicks, tags);
            logger.LogDebug(
                "Leaf-cache pre-warm for shard {ShardKey} ranked no leaves, so nothing was warmed; "
                + "the access-frequency model is empty (typically after an unclean restart).",
                context.GrainId.Key.ToString());
            return;
        }

        var warmed = 0;

        using var gate = new SemaphoreSlim(MaxLeafPreWarmParallelism, MaxLeafPreWarmParallelism);
        var tasks = new Task<bool>[ranked.Length];
        for (var i = 0; i < ranked.Length; i++)
        {
            tasks[i] = PreWarmOneLeafAsync(ranked[i], gate);
        }

        var results = await Task.WhenAll(tasks).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        foreach (var ok in results)
        {
            if (ok) warmed++;
        }

        LatticeMetrics.LeafCachePreWarmed.Add(warmed, tags);
        LatticeMetrics.LeafCachePreWarmDurationMs.Record(Environment.TickCount64 - startTicks, tags);
        logger.LogDebug(
            "Pre-warmed {Warmed}/{Requested} leaf caches for shard {ShardKey}.",
            warmed, ranked.Length, context.GrainId.Key.ToString());
        return;

        async Task<bool> PreWarmOneLeafAsync(GrainId leafId, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            try
            {
                var cache = ResolveLeafCacheGrain(leafId);
                await ShardActivationRetry
                    .RunAsync(cache.PreWarmAsync, CancellationToken.None)
                    .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
                return true;
            }
            catch (Exception ex)
            {
                // Best-effort by contract: a leaf that has been merged away, or
                // a transient storage fault, must not fail WarmUpAsync.
                logger.LogDebug(ex,
                    "Leaf-cache pre-warm failed for leaf {LeafId} on shard {ShardKey}; continuing.",
                    leafId.ToString(), context.GrainId.Key.ToString());
                return false;
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
