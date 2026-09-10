using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Activation-hook partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Runs the
/// activation-time WAL materialiser that rebuilds the in-memory
/// projection (the per-activation runtime entry cache and the
/// per-leaf saga pending-tx machinery) from the durable per-shard
/// write-ahead log, then publishes the leaf's projection cursor so the
/// per-shard WAL GC sees the leaf the moment activation completes.
/// <para>
/// The materialiser is the activation-time WAL recovery seam, gated
/// by the persisted <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionCheckpointOffset"/>:
/// every WAL entry strictly after the checkpoint is replayed back
/// through <see cref="ILeafProjection.Apply(in LatticeMutation)"/>, the
/// pending-tx map is reconstructed deterministically from prepared
/// mutations whose terminals have not yet replayed, and the persisted
/// checkpoint is advanced under
/// <see cref="ILeafProjection.SetCheckpointOffsetAsync(long, CancellationToken)"/>'s
/// <c>MinUnresolvedPrepareOffset - 1</c> clamp so the next activation
/// never silently advances past a prepare whose terminal is still
/// outstanding.
/// </para>
/// <para>
/// Replay short-circuits to a no-op on two preconditions: the tree id
/// must have been seeded (system-tree leaves and pre-init activations
/// are skipped); and the WAL head must strictly exceed the persisted
/// checkpoint (otherwise there is nothing to replay). The
/// commit-log adapter (<see cref="ICommitLogReader"/>) is registered
/// unconditionally by <c>AddLattice</c> via the in-core
/// <c>WalCommitLogReader</c> default, so the activation hook can
/// always rely on it being resolvable from DI.
/// </para>
/// <para>
/// Before reading any WAL slice the materialiser consults
/// <see cref="ILatticeFallOffLogDetector"/> to classify the gap
/// between the persisted checkpoint and the WAL head/tail. If the
/// detector returns anything other than
/// <see cref="FallOffLogDecision.TailReplay"/> (WAL trimmed past the
/// checkpoint, replay budget exceeded, or projection retention
/// elapsed), the materialiser surfaces
/// <see cref="LeafProjectionStaleException"/> immediately. V1 does
/// not integrate the snapshot-then-WAL or full-rebuild recovery
/// paths; those are tracked as a follow-up so this commit can land
/// the dominant correctness path (tail replay) without taking on
/// snapshot-storage integration in the same change.
/// </para>
/// <para>
/// Replay failures propagate. A leaf that comes online with a stale
/// projection silently violates the saga reader-isolation contract
/// (a continuous reader could observe a half-applied saga across a
/// reactivation), so the activation hook surfaces the exception
/// rather than swallowing it. Cursor-publish errors remain swallowed
/// (the cursor is monotonic and the next foreground flush retries
/// via the lazy-on-flush path) - that contract did not change.
/// </para>
/// <para>
/// V1 single-partition assumption: the materialiser reads WAL
/// partition <c>0</c> only. The existing core test cluster and the
/// single-cluster production deployment configure
/// <c>LatticeReplicationOptions.ReplogPartitions = 1</c>, so every
/// per-key write and every saga terminal-mark for every chain shard
/// lands in partition 0 and the single-partition read recovers the
/// full state. Multi-partition fan-out (i.e. iterating
/// <c>[0, ReplogPartitions)</c> on activation, or hoisting the
/// materialiser into a per-shard driver that dispatches by leaf
/// ownership) is deliberately out of scope for this commit and
/// tracked as a follow-up so the saga reader-isolation promotion
/// can land without taking on the full WAL-routing reconciliation
/// in the same change.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Whether THIS activation replayed the whole readable WAL window (cold)
    /// rather than resuming above a snapshot or cache anchor (warm). Latched
    /// from the same replay-start override that tags
    /// <see cref="LatticeMetrics.LeafActivationReplays"/>, so the deactivation
    /// observation and the activation counter can never disagree about which
    /// arm an activation belongs to (issue #2280). Defaults to <c>false</c>:
    /// an activation that took no replay permit is counted on neither arm by
    /// the activation counter and is reported as warm here.
    /// </summary>
    private bool _activationWasCold;

    /// <summary>
    /// Exact post-filter count of entries THIS activation took through the
    /// projection rebuild seam, accumulated across every WAL partition it
    /// replayed. Reported on the deactivation log line (issue #2280) so a
    /// reader can tell "banked nothing because it did no work" from "banked
    /// nothing because it did not pass its existing checkpoint" - which on a
    /// cold replay is the arithmetically forced case, not a fault.
    /// </summary>
    private long _replayEntriesAppliedThisActivation;

    /// <summary>
    /// Maximum number of WAL entries the activation-time replay reads
    /// per <see cref="ILeafReplayCoordinatorGrain.ReadSliceAsync"/>
    /// invocation. Bounds the worst-case replay memory footprint for a
    /// long-tailed WAL and lets the activation hook interleave RPC
    /// progress across multiple slice fetches.
    /// </summary>
    private const int ReplaySliceBudget = 256;

    /// <summary>
    /// V1 WAL partition the activation-time replay reads from. Retained
    /// as a name for the legacy single-partition shape (default
    /// <see cref="LatticeOptions.WalPartitions"/> = 1); under multi-
    /// partition replay the activation hook iterates
    /// <c>[0, WalPartitions)</c> and threads each partition through
    /// <see cref="LatticeApplyOffsetContext.BeginScope(int, long)"/>
    /// so the per-partition projection-checkpoint clamp can scope to
    /// the correct partition's offset space.
    /// </summary>
    private const int ReplayWalPartition = 0;

    /// <summary>
    /// Per-silo (process-wide) ceiling on concurrent activation-time leaf
    /// materialiser replays, lazily sized from
    /// <see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/> on the
    /// first activation that resolves options. A reactivation storm (issue
    /// #1030) would otherwise fan out an unbounded number of WAL replays and
    /// starve the foreground request path; this semaphore bounds the in-flight
    /// replay count so the storm queues instead of stampeding the thread pool.
    /// </summary>
    private static SemaphoreSlim? _replayConcurrencyGate;

    /// <summary>Initialisation guard for <see cref="_replayConcurrencyGate"/>.</summary>
    private static readonly object _replayConcurrencyGateLock = new();

    /// <summary>
    /// Test-only view of the process-wide replay concurrency gate, or
    /// <see langword="null"/> before the first activation with a tree id has
    /// sized it. Exposed so a regression test can assert that a fault raised
    /// after a permit is acquired does not permanently reduce the gate (issue
    /// #2256): the gate is sized once and never re-created, so a lost permit is
    /// lost for the lifetime of the process and its exhaustion is silent.
    /// </summary>
    internal static SemaphoreSlim? ReplayConcurrencyGateForTest => Volatile.Read(ref _replayConcurrencyGate);

    /// <summary>
    /// Lazily resolves the per-silo replay concurrency gate from
    /// <paramref name="options"/>. A non-positive
    /// <see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/> resolves
    /// to <see cref="Environment.ProcessorCount"/>. The gate is sized once on
    /// first use and is a process-wide structural constant thereafter.
    /// <para>
    /// <b><see cref="Environment.ProcessorCount"/> does not always honour the
    /// container CPU quota, and this gate is where that bites (issue #2278).</b>
    /// It is cgroup-aware <em>by default</em>, but <c>DOTNET_PROCESSOR_COUNT</c>
    /// (and <c>System.GC.HeapCount</c> under a configured heap count) is an
    /// explicit override that takes precedence over the cgroup-derived value.
    /// A host that sets it higher than the quota - which is an ordinary thing to
    /// do, and invisible from inside the process - sizes this gate above the CPU
    /// the process can actually obtain, and every permit it hands out is a
    /// concurrent WHOLE-WINDOW replay: a CPU-bound deserialise-and-apply loop.
    /// Observed in the deployed repo-context host at 16 permits against a
    /// 6-CPU quota (2.67x) under workstation GC, which produced thread-pool
    /// starvation, activations cancelled by the runtime mid-replay, and - because
    /// an interrupted rebuild latches neither capture signal in
    /// <c>TryCaptureSnapshotOnDeactivateAsync</c> - leaves that could never bank
    /// a snapshot and so re-entered a cold whole-window replay on every
    /// activation. Oversubscribing this gate is therefore not merely slow: it is
    /// self-reinforcing, because the replay it makes too slow is the very work
    /// whose completion would have made the next one cheap.
    /// </para>
    /// <para>
    /// Nothing here can read the cgroup quota portably, and it deliberately does
    /// not try: <c>DOTNET_PROCESSOR_COUNT</c> is a documented, supported override
    /// doing exactly what it is specified to do, so library code that reached
    /// past it would silently defeat an operator instruction that every other
    /// .NET subsystem in the process obeys, leaving the process holding two
    /// conflicting beliefs about its own CPU count (ruled out on issue #2279).
    /// What this does instead is make the number <em>observable</em>: the
    /// resolved ceiling is logged once alongside the configured option and
    /// <see cref="Environment.ProcessorCount"/>, so an operator diagnosing a
    /// replay storm can read the figure the process actually chose rather than
    /// inferring it from the host's vCPU count. The sizing remedy needs no code
    /// at all - pin
    /// <see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/>
    /// explicitly wherever the quota and <see cref="Environment.ProcessorCount"/>
    /// can disagree, since it already takes precedence over the default.
    /// </para>
    /// </summary>
    private static SemaphoreSlim ResolveReplayConcurrencyGate(LatticeOptions options, Func<ILogger?> loggerAccessor)
    {
        var existing = Volatile.Read(ref _replayConcurrencyGate);
        if (existing is not null)
            return existing;

        bool sizedHere;
        int max;
        lock (_replayConcurrencyGateLock)
        {
            if (_replayConcurrencyGate is null)
            {
                max = options.WalMaterialiserMaxConcurrentReplays;
                if (max <= 0)
                    max = Environment.ProcessorCount;
                _replayConcurrencyGate = new SemaphoreSlim(max, max);
                sizedHere = true;
            }
            else
            {
                (sizedHere, max) = (false, 0);
            }
        }

        if (sizedHere)
            LogResolvedReplayConcurrencyGate(max, options.WalMaterialiserMaxConcurrentReplays, loggerAccessor);

        return Volatile.Read(ref _replayConcurrencyGate)!;
    }

    /// <summary>
    /// Emits the one-per-process record of the resolved gate ceiling.
    /// <para>
    /// Three properties of this method are load-bearing rather than stylistic.
    /// It runs <b>outside</b> the initialisation lock, because a logging sink is
    /// arbitrary code and holding the lock across it would serialise every other
    /// activation racing to resolve the same gate behind a slow sink. It takes
    /// the logger as a <see cref="Func{TResult}"/> and invokes it only on the
    /// sizing path, so the overwhelming majority of activations - which find the
    /// gate already built and return before reaching here - never resolve a
    /// logger for it at all; that also keeps the permit-leak regression fixture
    /// for issue #2256 measuring what it claims to, since resolving a logger
    /// earlier on the acquisition path would move that fixture's injected fault
    /// to before the permit is taken and quietly void its instrument. And it
    /// swallows everything, because issue #2256 established here that a throwing
    /// logging sink is a real environmental fault on this exact path; an
    /// observability improvement that can itself fail an activation is a
    /// regression, not an improvement.
    /// </para>
    /// <para>
    /// Exposed as <c>internal</c> rather than <c>private</c> so the swallow can
    /// be pinned by a test. The gate itself is sized once per process and has no
    /// reset seam, so a fixture that tried to observe this line by driving a
    /// real activation would pass or fail on test-execution order - the same
    /// order-dependent flake shape the meter-field convention exists to prevent.
    /// Calling the emitter directly is deterministic and tests the property that
    /// can actually regress.
    /// </para>
    /// </summary>
    internal static void LogResolvedReplayConcurrencyGate(int max, int configured, Func<ILogger?> loggerAccessor)
    {
        try
        {
            var logger = loggerAccessor();
            if (logger is null || !logger.IsEnabled(LogLevel.Information))
                return;

            logger.LogInformation(
                "Leaf WAL replay concurrency gate sized to {MaxConcurrentReplays} permit(s) for this silo. "
                + "Configured WalMaterialiserMaxConcurrentReplays={ConfiguredMaxConcurrentReplays} "
                + "(non-positive means unset, in which case the ceiling follows Environment.ProcessorCount), "
                + "and Environment.ProcessorCount reports {ProcessorCount}. Each permit admits one whole-window "
                + "WAL replay, which is CPU bound, so a ceiling above the CPU this process can actually obtain "
                + "oversubscribes it. Environment.ProcessorCount honours a container CPU quota only while "
                + "DOTNET_PROCESSOR_COUNT does not override it, so compare these figures against the container's "
                + "real quota rather than assuming the runtime already reflects it, and pin "
                + "WalMaterialiserMaxConcurrentReplays explicitly on a constrained host. The gate is sized once "
                + "per process and is never re-created or topped up.",
                max,
                configured,
                Environment.ProcessorCount);
        }
        catch
        {
            // Deliberately swallowed - see the summary above.
        }
    }

    /// <summary>
    /// Acquires a permit from the per-silo replay concurrency gate, returning
    /// the semaphore so the caller can release it once the replay completes.
    /// Returns <c>null</c> for a leaf with no tree id (a no-op activation that
    /// does no replay and must not consume a permit).
    /// </summary>
    /// <remarks>
    /// The caller must enter the <c>try</c> whose <c>finally</c> releases the
    /// permit as the very next statement. The gate is sized once by
    /// <see cref="ResolveReplayConcurrencyGate"/> and is never re-created or
    /// topped up, so a permit lost between the acquisition and that region is
    /// lost for the lifetime of the process, and the resulting exhaustion is a
    /// silent wait rather than a fault (issue #2256).
    /// </remarks>
    private async Task<SemaphoreSlim?> AcquireReplayPermitAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(state.State.TreeId))
            return null;

        var options = await GetOptionsAsync();
        var gate = ResolveReplayConcurrencyGate(options, ResolveLogger);
        await gate.WaitAsync(cancellationToken);
        return gate;
    }

    /// <summary>
    /// Activation hook. Runs the WAL materialiser to bring the
    /// in-memory projection (the per-activation runtime entry cache
    /// plus the per-leaf saga pending-tx map) up to the WAL head, then
    /// publishes the leaf's projection cursor so the per-shard WAL
    /// GC observes the leaf eagerly. No-op when the leaf has not been
    /// seeded with a tree id.
    /// </summary>
    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        // Step 0 - try to rehydrate the in-memory entry cache from a
        // persisted leaf snapshot. The snapshot is the safety net for
        // WAL retention fall-off: if a previous maintenance tick wrote
        // a snapshot whose offset exceeds this leaf's persisted
        // ProjectionCheckpointOffset, we hydrate the cache from the
        // snapshot and let the tail replay below cover only the
        // (snapshot, head] suffix. When no snapshot is present (or it
        // is older than the persisted checkpoint), this step is a
        // no-op and the existing WAL-tail-replay path runs unchanged.
        var rehydratedFromSnapshot = await TryRehydrateFromSnapshotAsync(cancellationToken);

        // Step 0.5 - cache/checkpoint coherence reset. The entry
        // cache is per-activation only; it is rebuilt
        // from the WAL on every activation and never persisted. The
        // persisted ProjectionCheckpointOffset, in contrast, survives
        // across activations. When the leaf reactivates after a silo
        // restart (or any cold start) without a snapshot, the cache
        // starts empty but the persisted checkpoint still claims that
        // every offset through N has been applied. Replaying only
        // (N, head] would silently drop offsets 0..N from the rebuilt
        // cache. Compute a local replay-start sentinel (-1) so the WAL
        // replay below covers the entire readable window. We pass this
        // as an override rather than mutating the persisted slot, so
        // every external observer of the checkpoint (digest, snapshot
        // capture guard, materialiser-lag math, fall-off-log detector)
        // continues to see the pre-activation value until the replay's
        // own SetCheckpointOffsetAsync advances it through the normal
        // flush path. The reset is gated on (a) the snapshot rehydrate
        // not having populated the cache (it already advanced the
        // checkpoint to the snapshot offset and that anchor is
        // honoured), and (b) the cache being empty - if some upstream
        // seam (e.g. a future hot-restart path, a sibling-at-birth
        // attach, or a test seeding the cache for unit-test purposes)
        // has already populated the cache, the checkpoint is by
        // definition coherent with it and must not be overridden.
        long? replayCheckpointOverride =
            (!rehydratedFromSnapshot && Cache.Count == 0) ? -1L : null;

        // Step 1 - drive the dormant ILeafProjection.Apply seam over
        // the WAL slice between the persisted checkpoint and the
        // current head. Failures propagate: a leaf that comes online
        // with a stale projection silently violates the saga
        // reader-isolation contract, and the host's grain activation
        // pipeline will retry the activation rather than serve reads
        // from a half-applied state.
        //
        // The replay runs under a per-silo concurrency permit (issue
        // #1030): under a burst that reactivates or splits many leaves
        // at once, an unbounded fan-out of WAL replays saturates every
        // silo thread and starves the foreground request path. The
        // permit caps how many leaf replays run concurrently so a
        // reactivation storm degrades into a bounded queue. A no-op
        // activation (no tree id) takes no permit.
        bool advanced;
        SemaphoreSlim? replayPermit = null;

        // The acquisition sits INSIDE the try, whose finally is the only thing
        // that returns the permit (issue #2256). The observation block below
        // used to run outside it, so a throw from the metric add, the
        // tenant-label lookup, the totals sample, the logger resolution, the
        // IsEnabled probe or the templated call itself lost the permit for the
        // lifetime of the process: the gate is sized once by
        // ResolveReplayConcurrencyGate and is never re-created or topped up. It
        // defaults to Environment.ProcessorCount, which is cgroup-aware only
        // when DOTNET_PROCESSOR_COUNT does not override it (issue #2278), so on
        // a 2-vCPU host two such throws - ever - stop the silo activating
        // leaves entirely, and the symptom is a silent wait on WaitAsync rather
        // than an error. Note the override cuts both ways: it can also size the
        // gate ABOVE the quota, which does not exhaust it but oversubscribes
        // the CPU behind it. A throwing logging sink is transient and
        // environmental, which is exactly the fault a unit test never sees.
        //
        // The acquisition moved inside the try for issue #2280 and this
        // STRENGTHENS the #2256 invariant rather than relaxing it: there is now
        // no window at all between acquiring the permit and the region that
        // releases it, where before there was a one-statement gap. It is done
        // so a cancellation delivered while QUEUED ON the permit is observed.
        // That window is not incidental - its width is set by the very
        // saturation issue #2280 is about, so under the conditions of interest
        // it is plausibly the DOMINANT one, and leaving it uncounted would
        // reproduce in the instrument the same blindness it was built to end.
        try
        {
            replayPermit = await AcquireReplayPermitAsync(cancellationToken);

            if (replayPermit is not null)
            {
                // The cold/warm discriminator is precisely the replay-start
                // override computed at step 0.5: a -1 sentinel means neither the
                // snapshot rehydrate nor a pre-populated cache supplied an anchor,
                // so this activation replays the whole readable WAL window (cold);
                // a null override means the activation resumed above an anchor and
                // replays only the tail (warm). Nothing else needs to be computed
                // or plumbed - the value is already in scope, which is why the
                // discriminator belongs on the existing counter rather than on a
                // second one (issue #2148).
                //
                // This is pure observation and does not need to hold a permit; it
                // sits inside the guarded region because being inside it is what
                // makes the release unconditional, not because it needs the gate.
                var cold = replayCheckpointOverride == -1L;
                _activationWasCold = cold;
                var replayTreeId = state.State.TreeId!;
                LatticeMetrics.LeafActivationReplays.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, replayTreeId),
                    cold ? LatticeMetrics.ActivationTemperatureCold : LatticeMetrics.ActivationTemperatureWarm,
                    LatticeTenantLabel.ForTree(replayTreeId));

                // ... and sample the same two totals into the log, because a
                // counter is only readable where something is scraping the
                // process, and the deployed host exposes no metrics endpoint
                // (issue #2148). Observe unconditionally - before any logger or
                // level check - so the totals account for every permitted replay
                // whether or not a line is emitted for it.
                var temperatureSample = ObserveLeafActivationReplay(
                    replayTreeId, cold, this.GetGrainId(), Stopwatch.GetTimestamp());
                if (temperatureSample is { } totals)
                {
                    // Gate on IsEnabled as the over-budget warning does: the
                    // templated call would otherwise allocate a params object[]
                    // and box both totals even when the line is filtered out.
                    var temperatureLogger = ResolveLogger();
                    if (temperatureLogger is not null && temperatureLogger.IsEnabled(LogLevel.Information))
                    {
                        temperatureLogger.LogInformation(
                            "Leaf activation replays for tree '{TreeId}' since this silo started: {ColdReplays} cold "
                            + "(no snapshot rehydrate and an empty entry cache, so the whole readable WAL window is "
                            + "replayed) across {DistinctColdQualifier}{DistinctColdLeaves} distinct leaves, and "
                            + "{WarmReplays} warm (resumed above a snapshot or cache anchor). These are "
                            + "CUMULATIVE process-wide totals, not a count since the previous line: the line is "
                            + "rate-limited to one per tree per {IntervalSeconds}s, so any single line yields the "
                            + "cold:warm ratio and any two yield the rate between them. Activations of a leaf with no "
                            + "tree id bound take no replay permit and are counted on neither arm. Compare the cold "
                            + "total against the distinct count to read the arm's SHAPE: roughly equal means a "
                            + "one-time first-activation cost spread broadly, whereas a cold total far above the "
                            + "distinct count means the same few leaves are going cold repeatedly, which is a "
                            + "snapshot or rehydrate defect rather than an expected cost. Informational: a "
                            + "cold replay is correct, just more expensive than a warm one.",
                            replayTreeId,
                            totals.Cold,
                            // "at least" is load-bearing: past the cap the
                            // distinct count is a floor, and a reader who took
                            // it as exact would compute a cold:distinct ratio
                            // that is too high and read a broad arm as a loop.
                            totals.DistinctColdLeavesSaturated ? "at least " : string.Empty,
                            totals.DistinctColdLeaves,
                            totals.Warm,
                            (long)ActivationTemperatureLogInterval.TotalSeconds);
                    }
                }
            }

            advanced = await ReplayWalSinceCheckpointAsync(replayCheckpointOverride, cancellationToken);

            // The reset half of the cold-replay-loop streak (issue #2280). This
            // site is the exact complement of the catch below: the guarded
            // replay region completed, so this leaf is out of the loop and its
            // run of consecutive cancellations ends here.
            //
            // Unconditional, including on the warm arm and on a leaf with no
            // tree id. Any successful activation is an intervening success, and
            // a leaf that never recorded a streak removes nothing.
            ForgetColdReplayCancellations(this.GetGrainId());
        }
        catch (Exception ex)
        {
            // Activation-failure observation (issue #2280). OBSERVE AND
            // RETHROW - never swallow. "Failures propagate" above is
            // load-bearing: an activation that ate its cancellation would come
            // online over a half-applied projection, which is exactly the
            // #1535 no-loss violation the snapshot coverage gate exists to
            // prevent. This catch adds a counter and changes nothing else.
            //
            // This site exists because the deactivation-time observation is
            // STRUCTURALLY BLIND to the population #2280 is about. Orleans
            // does not run OnDeactivateAsync when OnActivateAsync throws
            // (measured on 10.2.2 with a positive control), and a cancelled
            // cold replay throws OperationCanceledException out of activation
            // via ThrowIfCancellationRequested below. Without this counter a
            // cancelled cold replay would read as zero at every rate of
            // occurrence, including the highest.
            //
            // A cancellation delivered while still QUEUED ON the replay permit
            // is now counted too, under its own reason value rather than folded
            // in with a cancellation that had actually begun replaying. The two
            // are different events - one lost work in progress, the other never
            // started - and blurring them would leave the instrument unable to
            // answer the question it exists for. The queue window matters
            // because its width is set by the saturation issue #2280 is about,
            // so under the conditions of interest it is plausibly the larger of
            // the two.
            if (state.State.TreeId is { Length: > 0 } failedTreeId)
            {
                // replayPermit is still null exactly when the acquisition
                // itself did not return - and a null permit cannot mean "no
                // tree id" here, because that case is excluded by the guard
                // above.
                var reason = ex is not OperationCanceledException
                    ? LatticeMetrics.ActivationFailureFaulted
                    : replayPermit is null
                        ? LatticeMetrics.ActivationFailureCanceledAwaitingPermit
                        : LatticeMetrics.ActivationFailureCanceled;

                LatticeMetrics.LeafActivationFailures.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, failedTreeId),
                    replayCheckpointOverride == -1L ? LatticeMetrics.ActivationTemperatureCold : LatticeMetrics.ActivationTemperatureWarm,
                    reason,
                    LatticeTenantLabel.ForTree(failedTreeId));

                // Loop detection and escalation (issue #2280, direction 4).
                //
                // The counter above is an AGGREGATE. It cannot distinguish one
                // leaf cancelled five times from five leaves cancelled once,
                // and those are a defect and a cost respectively - the first is
                // a leaf whose cancellation reproduces exactly the condition
                // that caused it, which is the self-reinforcing loop this issue
                // is about. Only a per-leaf run of CONSECUTIVE cancellations
                // separates them, so that is what is tracked here.
                //
                // Restricted to the COLD arm on purpose: a warm activation
                // resumed above a snapshot or cache anchor, so its cancellation
                // does not reproduce coldness and is not this pathology.
                if (replayCheckpointOverride == -1L && ex is OperationCanceledException)
                {
                    // The whole escalation is observation, and an observation
                    // must NEVER replace the fault it observes. Without this
                    // guard a throwing logging sink - transient, environmental,
                    // and the exact fault the #2256 permit leak was caused by -
                    // would escape this catch in place of the
                    // OperationCanceledException, rewriting a cancelled
                    // activation as a faulted one upstream and destroying the
                    // very signal this change exists to create.
                    try
                    {
                        EscalateColdReplayCancellation(
                            failedTreeId,
                            this.GetGrainId(),
                            awaitingPermit: replayPermit is null,
                            Stopwatch.GetTimestamp());
                    }
                    catch
                    {
                        // Intentionally swallowed. See above: losing the
                        // diagnostic is a bounded loss, losing the exception is
                        // not.
                    }
                }
            }

            throw;
        }
        finally
        {
            replayPermit?.Release();
        }

        // Step 1.4 - publish this activation's same-silo revision cookie
        // now that the in-memory projection has been rebuilt (issue #2151).
        //
        // The cookie is the mechanism LeafCacheGrain.RefreshAsync uses to
        // decide whether its snapshot is stale, and it is bumped from every
        // state-advancing FOREGROUND site on the leaf. The snapshot
        // rehydrate and WAL replay above are not foreground sites: neither
        // ILeafProjection.Apply nor TryRehydrateFromSnapshotAsync bumps, so
        // before this call a leaf that came back from a projection rebuild
        // (which deactivates, so the next activation replays) had NO
        // registry entry at all. The cache's guard requires an entry to
        // take the revision branch, so a cache still holding a cookie from
        // the previous activation fell through to the TTL gate instead and
        // kept serving its existing snapshot until the TTL elapsed. The
        // removal-on-deactivate comment claims that fall-through reaches
        // "the cross-grain refresh path"; without this publish it reaches a
        // gate that can return early.
        //
        // Publishing unconditionally - rather than only when the replay
        // advanced - keeps the rule simple and the entry's presence tied to
        // the activation rather than to what the WAL happened to contain: a
        // cache cannot tell the two apart, and a leaf whose replay applied
        // nothing may still hold state a cache has drifted from. The cost is
        // one registry entry per live leaf, which is the bound the
        // removal-on-deactivate already maintains.
        //
        // This is only safe because activations are seeded from disjoint
        // cookie ranges (see BumpLocalRevision). Publishing here while
        // activations still restarted from zero would ARM the ABA
        // collision, by making the entry present with a value a previous
        // activation had already published.
        BumpLocalRevision();

        // Step 1.5 - if the fall-off-log detector raised the
        // SnapshotPending advisory while classifying the replay path,
        // proactively capture the leaf's projection into the dedicated
        // snapshot grain now. Doing this once per (advisory-firing)
        // activation amortises capture cost over the whole activation
        // cycle and ensures that an active leaf whose checkpoint sits
        // close to the WAL tail is durably snapshotted before any
        // subsequent WAL trim can fall through the gap. Capture errors
        // are best-effort and must not block the leaf coming online -
        // the next periodic recheck (FlushPendingCheckpointAsync) or
        // the next reactivation will retry.
        if (_activationSnapshotPending)
        {
            _activationSnapshotPending = false;
            await TryCaptureSnapshotForAdvisoryAsync();
        }

        // Step 2 - eagerly publish the cursor IFF the materialiser did
        // not already advance the checkpoint. SetCheckpointOffsetAsync
        // routes through FlushPendingCheckpointAsync which already
        // publishes the cursor on every persist; an explicit publish
        // here would be a redundant (idempotent but wasteful) RPC. On
        // the no-replay path (no new entries since checkpoint) we
        // still want to publish so the GC sees the leaf eagerly.
        //
        // Publishing no durable pin on this path is DELIBERATE, and is
        // load-bearing in the safe direction rather than an oversight -
        // do not "tidy" it by adding a publish call here. A data-free
        // leaf can reach this return with its clock still at Zero,
        // because the replay bumps a partition's max-applied offset for
        // entries it SKIPS as belonging to another leaf's key range. Were
        // it to publish, the frontier half would be Zero again (identical
        // to what stands, so no gain) while the offset half would be
        // strictly HIGHER - and a higher offset raises the WAL GC's
        // offset floor, which REDUCES retention. That is exactly the
        // direction the coverage gate on
        // SeedDurableMaterialiserFrontierAsync now forbids (issue 2150),
        // so a publish here would reintroduce a weaker form of it.
        // Nothing is left unprotected in the meantime: both tree-id birth
        // seams (SetTreeIdAsync and InitializeSiblingAsync) await
        // SeedDurableMaterialiserBlockPinAsync before any routed write
        // makes the leaf's data reachable, and that Zero frontier
        // disables the WAL GC's cursor-trim branch outright until the
        // leaf first checkpoints. The leaf is holding that branch OFF,
        // not holding a weak floor - so there is no window here in which
        // it has no pin at all.
        if (advanced)
            return;

        try
        {
            // Skip leaves whose projection has never advanced
            // (registering at HLC zero would pin the WAL trim point at
            // offset zero forever on a leaf that has never seen a
            // write), and reuse the same gating as the lazy-on-flush
            // path so the consumer-id format and reporter resolution
            // stay in exactly one place.
            var clock = state.State.Clock;
            if (clock <= HybridLogicalClock.Zero)
            {
                // Never-checkpointed leaf: skip the in-memory registry (a
                // Zero cursor would pin offset zero forever) but still seed
                // a durable Zero "block" pin so the WAL GC retains this
                // leaf's WAL head across a restart until it checkpoints.
                await SeedDurableMaterialiserFrontierAsync();
                return;
            }

            await ReportCursorIfActiveAsync();
        }
        catch (Exception ex)
        {
            // Cursor-publish failures are non-fatal: the cursor is
            // monotonic so the next successful foreground flush
            // catches up via the lazy-on-flush path. Materialiser
            // failures, in contrast, are fatal (they propagate above)
            // because correctness - not progress - is at stake.
            //
            // Always count the failure (issue #1030: keep the true rate
            // observable), but rate-limit the warning log to at most one per
            // silo per CursorFailLogIntervalTicks so a reactivation storm
            // against a saturated silo cannot self-amplify into a log flood.
            LatticeMetrics.LeafActivationCursorPublishFailures.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
                LatticeTenantLabel.ForTree(state.State.TreeId));

            if (ShouldLogCursorPublishFailure())
            {
                var logger = context.ActivationServices?
                    .GetService<ILoggerFactory>()?
                    .CreateLogger<BPlusLeafGrain>();
                logger?.LogWarning(
                    ex,
                    "Eager cursor registration failed during activation for leaf {GrainId}; will retry on next checkpoint flush.",
                    context.GrainId);
            }
        }
    }

    /// <summary>
    /// Minimum interval, in ticks, between activation cursor-publish-failure
    /// warning logs across the whole silo. Bounds the log rate during a
    /// reactivation storm (issue #1030) while every failure is still counted by
    /// <see cref="LatticeMetrics.LeafActivationCursorPublishFailures"/>.
    /// </summary>
    private static readonly long CursorFailLogIntervalTicks = TimeSpan.FromSeconds(1).Ticks;

    /// <summary>Last UTC tick a cursor-publish-failure warning was logged (silo-wide).</summary>
    private static long _lastCursorFailLogTicks;

    /// <summary>
    /// Per-silo token check for the cursor-publish-failure warning: returns
    /// <c>true</c> at most once per <see cref="CursorFailLogIntervalTicks"/>.
    /// Uses an interlocked compare-and-swap so concurrent reactivations never
    /// race past the gate together.
    /// </summary>
    private static bool ShouldLogCursorPublishFailure()
    {
        var now = DateTime.UtcNow.Ticks;
        var last = Volatile.Read(ref _lastCursorFailLogTicks);
        if (now - last < CursorFailLogIntervalTicks)
        {
            return false;
        }

        return Interlocked.CompareExchange(ref _lastCursorFailLogTicks, now, last) == last;
    }

    /// <summary>
    /// Drives the dormant <see cref="ILeafProjection.Apply(in LatticeMutation)"/>
    /// seam over every WAL entry strictly after
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionCheckpointOffset"/>
    /// and at-or-before the WAL head, then advances the persisted
    /// checkpoint via
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync(long, CancellationToken)"/>.
    /// The checkpoint advance is clamped behind any unresolved
    /// prepared-saga mutation rebuilt during this replay, so a
    /// subsequent activation re-emits the prepare exactly once when
    /// its terminal mark eventually surfaces.
    /// </summary>
    /// <returns>
    /// <c>true</c> if the materialiser advanced the persisted
    /// checkpoint (and therefore SetCheckpointOffsetAsync already
    /// published the leaf's cursor via FlushPendingCheckpointAsync);
    /// <c>false</c> if the replay was a no-op or every replayed
    /// offset was clamped behind an unresolved prepare. The caller
    /// uses this signal to decide whether the explicit
    /// activation-time cursor publish would be redundant.
    /// </returns>
    /// <remarks>
    /// Per-entry filter: <see cref="ShouldApplyDuringReplay(in LatticeMutation, int?, string?, string?, ShardMap?)"/>
    /// drops entries whose <see cref="LatticeMutation.ShardIndex"/>
    /// does not match this leaf's persisted shard, and entries whose
    /// key falls outside this leaf's persisted
    /// [<see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.LowKeyInclusive"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.HighKeyExclusive"/>) range. The
    /// filter is keyed on persisted ownership identity, not on
    /// authorship - a leaf born from a split must apply WAL entries
    /// that fall in its current range even when those entries were
    /// authored by the donor pre-split (the rebuild-from-WAL
    /// scenario). DeleteRange / TxCommit / TxAbort are applied
    /// unconditionally; unknown <see cref="MutationKind"/> values are
    /// dropped (defensive forward-compat).
    /// </remarks>
    private async Task<bool> ReplayWalSinceCheckpointAsync(long? checkpointOverride, CancellationToken cancellationToken)
    {
        try
        {
            return await ReplayWalSinceCheckpointCoreAsync(checkpointOverride, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Bank whatever this cold rebuild re-read before it was cut short
            // (issue #2280). This is the ONLY reachable banking point on this
            // path: Orleans does not run OnDeactivateAsync when OnActivateAsync
            // throws, and a cancelled cold replay leaves activation BY throwing,
            // so the graceful-deactivation capture hook never sees it. Without
            // this, the whole re-read prefix is discarded and the next
            // activation starts again from the WAL start - the loop the
            // SELF-REINFORCING COLD REPLAY LOOP diagnostic names.
            //
            // CancellationToken.None, deliberately: the incoming token is
            // already cancelled, so passing it through would abandon the very
            // write that makes the cancellation survivable. The capture is a
            // single blob write, and TryCaptureSnapshotForAdvisoryAsync's
            // fault-swallowing contract does not apply here - a failure to bank
            // must not mask the cancellation, so any fault is swallowed
            // explicitly below and the original cancellation is rethrown.
            try
            {
                await TryBankColdReplayProgressAsync(CancellationToken.None);
            }
            catch (Exception bankFault)
            {
                ResolveLogger()?.LogWarning(
                    bankFault,
                    "Failed to bank cold-replay progress for leaf '{LeafId}' of tree '{TreeId}' after the "
                    + "replay was cancelled. The activation still fails as it did before; the only loss is "
                    + "that the next activation re-reads the prefix this one had already absorbed.",
                    context.GrainId.ToString(),
                    state.State.TreeId ?? "<unset>");
            }

            throw;
        }
    }

    /// <inheritdoc cref="ReplayWalSinceCheckpointAsync"/>
    private async Task<bool> ReplayWalSinceCheckpointCoreAsync(long? checkpointOverride, CancellationToken cancellationToken)
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return false;

        var resolvedOptions = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolvedOptions.WalPartitions);
        var detector = context.ActivationServices?.GetService<ILatticeFallOffLogDetector>();
        var projection = (ILeafProjection)this;

        // Resolve the leaf's current slot-ownership view once for the whole
        // replay. After an adaptive split a post-split write routed to the
        // donor shard for an already-moved slot is shadow-forwarded into the
        // target's WAL but keeps the DONOR's source stamp
        // (mutation.ShardIndex = donor). Gating Set/Delete/Tombstone replay on
        // the stamped ShardIndex alone (issue #909) drops such a record on a
        // cold reactivation from a checkpoint that pre-dates the forward,
        // resurrecting a drained value or losing a tombstone. The fix resolves
        // ownership positively by the key's virtual slot under the current
        // routing map (mirroring the snapshot-leaf fix for issue #907): a
        // record is owned by this leaf iff the current map routes its key's
        // slot to this leaf's shard. The map is fetched best-effort and is
        // only trusted when it actually references this leaf's shard, so a
        // registry hiccup or a foreign physical shard space can never cause a
        // leaf to reject its own writes - in that case replay falls back to
        // the legacy stamp-based axis.
        var replayShardMap = await ResolveReplayShardMapAsync(treeId);

        var anyAdvanced = false;
        // Pass 1: per-partition tail replay, deferring every saga
        // terminal (TxCommit / TxAbort) into a shared list so the
        // _pendingTx bucket is fully populated across every partition
        // before any terminal drains it. See the DeferredTerminal
        // docstring for the saga atomicity rationale.
        // Per-partition max-applied is tracked so the final reconciled
        // SetCheckpointOffsetAsync (after pass 2's terminals lift the
        // pending-tx clamps) advances each partition's checkpoint to
        // the actual highest offset observed during replay, not the
        // pass-1 clamped value.
        var deferredTerminals = new List<DeferredTerminal>();
        var deferredOffsets = new DeferredOffsetLedger(partitionCount);
        var perPartitionMaxApplied = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++) perPartitionMaxApplied[p] = -1L;

        // Pass-1 absorb frontier. A deferred terminal is only safe to apply in
        // place while pass 1 is still running once every OTHER partition has
        // been fully absorbed, because that is exactly when the terminal's
        // cross-partition dependencies are all present: every saga prepare has
        // landed in _pendingTx and every range-delete target is in the Cache.
        // Within the terminal's own partition the entries below its offset are
        // already applied (the WAL is read in offset order) and the entries
        // above it were appended after it, so they are not dependencies.
        // Partitions are absorbed in index order, so the condition holds for a
        // single-partition tree throughout, and for the last partition of a
        // multi-partition tree; every other terminal stays deferred to pass 2
        // exactly as before.
        var partitionsAbsorbed = 0;

        // Pass-1 sweep order (issue #2089). Pass 1 can only drain a deferred
        // terminal in place for the partition it absorbs LAST, because only
        // then are every other partition's prepares and range-delete targets
        // already in the cache. Every other partition must defer its terminals
        // to pass 2 - so an unresolved saga prepare pins its incremental flush
        // ceiling at (prepare - 1) for the whole of pass 1, and an activation
        // torn down before pass 2 completes banks nothing at all and replays
        // the identical range on the next activation.
        //
        // Sweeping in fixed index order hands that single drain-eligible slot
        // to partition N-1 regardless of where the backlog actually is.
        // Ordering the sweep by backlog ascending gives it instead to the
        // partition with the MOST to replay: the one least likely to finish
        // inside the activation window, and therefore the one that gains most
        // from banking progress incrementally as it scans.
        //
        // This NARROWS the livelock, it does not remove it - the other N-1
        // partitions still cannot drain in pass 1. Removing it needs a durable
        // record of unresolved deferred work so a resumed replay need not
        // re-read it; see issue #2089.
        var sweep = await BuildPassOneSweepOrderAsync(
            treeId, partitionCount, checkpointOverride, cancellationToken);

        // Issue #2165. Reconstruct the deferred work a previous activation
        // recorded durably, BEFORE pass 1 reads anything. This is what makes
        // the checkpoint advance that recorded it safe: the prepares go back
        // into _pendingTx and the terminals into the pass-2 list without the
        // WAL below the checkpoint being re-read. Records at offsets this
        // replay will re-read anyway are dropped rather than restored, so
        // nothing is applied twice.
        RestoreUnresolvedReplayWork(
            projection, partitionCount, checkpointOverride, deferredTerminals);

        foreach (var (partition, probedHead) in sweep)
        {
            // Per-partition checkpoint: a leaf whose persisted state
            // pre-dates the per-partition slot falls back to the
            // scalar ProjectionCheckpointOffset for partition 0 only;
            // every other partition starts at the -1 "nothing applied"
            // sentinel. The cold-start cache-empty override (see step
            // 0.5 in OnActivateAsync) drives every partition to -1
            // because the cache rebuild covers the full readable
            // window of every partition.
            long persistedCheckpoint = GetPersistedCheckpointForPartition(partition);
            var checkpoint = checkpointOverride ?? persistedCheckpoint;

            // Durable-frontier fall-off guard (issue #945: silent durable data
            // loss). The cold-cache-reset override (checkpointOverride = -1, set
            // by OnActivateAsync step 0.5 when the per-activation cache starts
            // empty and no snapshot rehydrated) deliberately drives the replay
            // from the absolute start so the full readable window is rebuilt,
            // and it is also the value handed to the fall-off-log detector
            // below. Feeding the detector -1 intentionally suppresses its
            // shared-WAL replay-budget heuristic (a sibling-populated partition
            // would otherwise trip the budget against this leaf's full range),
            // but it ALSO blinds the detector's WAL-trim trigger
            // (checkpoint > 0 && tail > checkpoint), because -1 is read as
            // "nothing to lose". For a leaf that genuinely has a durable
            // projection checkpoint, that blindness is unsafe: if the WAL has
            // been trimmed past the durable checkpoint and no snapshot covers
            // the gap, a cold replay rebuilds the leaf from only the surviving
            // WAL suffix - dropping every key the trim removed - and then
            // advances the persisted checkpoint and the durable materialiser
            // pin over the lost data. The advanced pin licenses the WAL GC to
            // trim further behind it, laundering the loss across the whole tree.
            // Re-check the trim trigger here against the DURABLE checkpoint and
            // surface the gap as a stale projection rather than silently
            // materialising it away. Gated on the cold-reset override so the
            // warm/snapshot-rehydrated path (where checkpoint == persisted) is
            // unchanged - the detector already covers it.
            //
            // Loss condition is tail > checkpoint + 1, NOT the detector's looser
            // tail > checkpoint. The replay reads strictly past the checkpoint
            // (ReplayPartitionAsync passes fromExclusive = checkpoint), so the
            // FIRST offset this leaf still needs is checkpoint + 1; the entry AT
            // the checkpoint is already applied and harmless to lose. tail is the
            // oldest still-readable offset (ICommitLogReader.GetTailOffsetAsync).
            // When tail == checkpoint + 1 only the already-applied prefix was
            // trimmed and the entire needed (checkpoint, head] window survives -
            // this is the legitimate "durable floor kept the live tail" shape
            // (issue #919), which must replay cleanly, not throw. Loss is real
            // only when the first needed offset itself fell off the log, i.e.
            // tail > checkpoint + 1. This guard and the fall-off-log detector's
            // WAL-trim trigger (LatticeFallOffLogDetector.ClassifyAsync) must use
            // the SAME exact boundary: the detector's SnapshotThenWal decision is
            // NOT a soft rebuild-policy hint - it throws LeafProjectionStaleException
            // below (the SnapshotThenWal/FullRebuild recovery paths are not yet
            // integrated). Before the coverage-gated WAL GC, the detector's looser
            // tail > checkpoint formula never bit because a snapshot-covered leaf
            // could not settle at tail == checkpoint + 1; now it can (the offset
            // floor trims the already-applied checkpoint entry once covered), so
            // the detector was aligned to tail > checkpoint + 1 to match this guard.
            if (checkpointOverride is { } coldReplayStart
                && coldReplayStart < persistedCheckpoint
                && persistedCheckpoint > 0)
            {
                var trimCoordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                    $"{treeId}/{partition}");
                var tail = await trimCoordinator.GetTailOffsetAsync(cancellationToken);
                if (tail > persistedCheckpoint + 1)
                {
                    throw new LeafProjectionStaleException(
                        $"Leaf projection for tree '{treeId}' partition {partition} cannot be rebuilt " +
                        $"from the WAL: the durable projection checkpoint (offset {persistedCheckpoint}) " +
                        $"has fallen off the log (oldest readable offset {tail}) and no covering snapshot " +
                        "is available, so a cold replay would silently rebuild the leaf over the lost " +
                        "prefix and advance the materialiser pin past unrecoverable data. " +
                        "Operator-driven projection rebuild is required.");
                }

                // Residual liveness signal (#1542). Reaching here means this
                // partition is a genuine cold rebuild over a pre-existing durable
                // checkpoint (persistedCheckpoint > 0) whose full prefix still
                // survives in the readable WAL - the guard above ruled out a
                // fallen-off prefix, and no snapshot rehydrated (step 0.5 chose
                // the -1 override only when the cache started empty and
                // unhydrated). The replay below therefore reconstructs the entire
                // readable window into the cache, so the cache faithfully holds
                // the checkpointed prefix and a graceful-deactivation capture may
                // safely stamp coverage. This closes the gap #1537 leaves for an
                // already-converged, snapshot-less leaf (checkpoint already at
                // head, so no forward advance sets _checkpointAdvancedThisActivation)
                // that would otherwise hold its Zero block pin - and its shared
                // WAL - forever. A brand-new leaf has no pre-existing checkpoint
                // (persistedCheckpoint == 0), never enters this block, and so its
                // foreground writes are never auto-covered on deactivation.
                _cacheRebuiltFromWalStartThisActivation = true;
            }

#if LATTICE_DIAG
            DiagSink.Write($"[DIAG replay-enter] gid={context.GrainId} treeId={treeId} partition={partition} shardIndex={state.State.ShardIndex} " +
                $"low='{state.State.LowKeyInclusive ?? "<null>"}' high='{state.State.HighKeyExclusive ?? "<null>"}' " +
                $"checkpoint={checkpoint} entryCount={Cache.Count}");
#endif

            if (detector is not null)
            {
                var decision = await detector.ClassifyAsync(
                    treeId,
                    partition,
                    checkpoint,
                    TimeSpan.Zero,
                    resolvedOptions,
                    cancellationToken);

                switch (decision)
                {
                    case FallOffLogDecision.TailReplay:
                        // NOTHING IS RETIRED HERE. Retirement is the other arm
                        // of the same decision that commits a report, and that
                        // decision is made on this leaf's applied-entry count
                        // in ReplayPartitionAsync (issue #2149). Retiring on
                        // the detector's verdict instead means retiring on the
                        // partition-wide gap, and the two quantities disagree
                        // over exactly one window - gap > budget while
                        // applied <= budget - in which the leaf is in budget,
                        // emits nothing, and yet keeps a stamp carrying up to
                        // an hour of accumulated backoff (#2100). Its next
                        // genuine fault then reports late. Splitting commit
                        // and retire across two quantities is what opened that
                        // window; they are kept on one quantity below.
                        break;
                    case FallOffLogDecision.SnapshotPending:
                        _activationSnapshotPending = true;
                        break;
                    case FallOffLogDecision.TailReplayOverBudget:
                        // A cost trigger fired (replay gap over
                        // MaxLeafReplayEntries, or projection age over
                        // LeafProjectionRetention) but the WAL still covers
                        // every offset this leaf needs, so the replay below
                        // converges to exactly the same projection. Replay
                        // regardless: a long activation is recoverable,
                        // refusing to activate is not (#1738).
                        //
                        // NOTHING IS WARNED OR METERED HERE (issue #2149).
                        // The gap trigger is expressed in partition-wide,
                        // pre-filter offsets while MaxLeafReplayEntries is a
                        // per-leaf, POST-filter budget, so warning here
                        // reported a quantity that was not this leaf's work:
                        // at the measured fan-out of ~1,350 leaves per
                        // partition it fired 19,639 times in 6.26 hours for
                        // leaves whose real work was one to two orders of
                        // magnitude BELOW budget. Both the warning and the
                        // LeafActivationOverBudgetReplays counter now live in
                        // ReplayPartitionAsync, where the entries this leaf
                        // actually applies are counted for free during the
                        // replay that is happening anyway.
                        //
                        // NOTHING IS ELECTED HERE EITHER (issue #2291). This
                        // arm used to raise an over-budget CANDIDATE that gated
                        // the stall check in ReplayPartitionAsync. Doing so
                        // conjoined a partition-wide quantity onto a per-leaf
                        // convergence fault, which made that fault unreportable
                        // on any partition shallower than MaxLeafReplayEntries -
                        // see the STALL (FAULT) CHECK there for why no budget
                        // value could have closed that blind spot.
                        break;
                    case FallOffLogDecision.SnapshotThenWal:
                    case FallOffLogDecision.FullRebuildFromWal:
                    case FallOffLogDecision.Fail:
                    default:
                        throw new LeafProjectionStaleException(
                            $"Leaf projection for tree '{treeId}' partition {partition} cannot be recovered " +
                            $"from the WAL alone (decision={decision}, persistedCheckpoint={checkpoint}): the " +
                            "write-ahead log has been trimmed past an offset this leaf still needs and no " +
                            "covering snapshot is available, so replaying the surviving suffix would rebuild " +
                            "the leaf over the lost prefix. Snapshot-then-WAL and full-rebuild recovery paths " +
                            "are not yet integrated; operator-driven rebuild is required.");
                }
            }

            var (advanced, maxApplied) = await ReplayPartitionAsync(treeId, partition, checkpoint, projection, deferredTerminals, deferredOffsets, partitionsAbsorbed == partitionCount - 1, replayShardMap, resolvedOptions.WalReplayMaxRecordsPerTurn, resolvedOptions.MaxDurableUnresolvedReplayWork, probedHead, resolvedOptions.MaxLeafReplayEntries, cancellationToken);
            partitionsAbsorbed++;
            if (advanced)
                anyAdvanced = true;
            if (maxApplied > perPartitionMaxApplied[partition])
                perPartitionMaxApplied[partition] = maxApplied;
        }

        // Pass 2: drain every deferred saga terminal (and DeleteRange
        // tombstone) in arrival order across partitions. By this
        // point pass 1 has fully populated every saga's pending bucket
        // in _pendingTx and every Set/Delete is in the Cache, so each
        // terminal's ApplyTxCommit / ApplyTxAbort observes the
        // complete prepared-mutation set and each range-tombstone's
        // ApplyDeleteRange iterates the full pre-tombstone Cache.
        // Ordering across partitions does not matter because each
        // terminal's id keys directly into the pending-bucket map;
        // within a single partition's deferred list the arrival order
        // is preserved by the append order.
        //
        // Each drained terminal is struck off the deferred ledger and its
        // partition's now-recovered ceiling is flushed immediately (issue
        // #1831). The drain is cheap per terminal but the list can be long, so
        // banking the recovered prefix as it shrinks means a teardown during
        // pass 2 keeps the progress the drain has already earned instead of
        // discarding all of it. Both safety clamps still bound every flush:
        // the ceiling stays below the next unresolved deferred offset in that
        // partition and below any unresolved prepare in it.
        foreach (var terminal in deferredTerminals)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (LatticeApplyOffsetContext.BeginScope(terminal.Partition, terminal.Offset))
            {
                projection.Apply(terminal.Mutation);
            }

            deferredOffsets.Resolve(terminal.Partition, terminal.Offset);
            ResolveUnresolvedReplayWork(terminal.Partition, terminal.Offset);
            if (await TryFlushRecoveredCeilingAsync(
                terminal.Partition,
                perPartitionMaxApplied[terminal.Partition],
                deferredOffsets,
                projection,
                cancellationToken))
            {
                anyAdvanced = true;
            }
        }

        // Pass 2.5 (issue #2190). Self-terminalise any saga prepare still
        // resident after the deferred terminals drained whose saga the registry
        // has already decided but whose terminal never reached this leaf. This
        // lands the decision locally through ApplyTxCommit / ApplyTxAbort, so the
        // prepare's clamp on the flush ceiling lifts as a consequence of the
        // effect landing and the final reconciliation below can bank the freed
        // prefix instead of the leaf re-reading the same window on every future
        // activation. A prepare the registry has not decided - or whose decision
        // has aged out of retention - is left resident and still clamps, exactly
        // as before.
        await SelfTerminaliseResolvedPreparesAsync(cancellationToken);

        // Final reconciliation: every pending-tx clamp has lifted now
        // that the terminals have drained, so the per-partition
        // checkpoint can advance to the actual maxApplied observed
        // during pass 1. Without this step, a partition whose
        // pass-1 SetCheckpointOffsetAsync was clamped behind an
        // unresolved prepare (whose terminal would later land in
        // pass 2) would stay at the clamped offset forever, even
        // after the terminal drained its pending bucket - the next
        // activation would needlessly re-replay the prefix that was
        // already absorbed into the Cache.
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var maxApplied = perPartitionMaxApplied[partition];
            if (maxApplied <= GetPersistedCheckpointForPartition(partition))
                continue;
            using (LatticeApplyOffsetContext.BeginScope(partition, maxApplied))
            {
                var current = GetCurrentCheckpointForPartition(partition);
                if (maxApplied > current)
                {
                    await projection.SetCheckpointOffsetAsync(maxApplied, cancellationToken);
                    anyAdvanced = true;
                }
            }
        }

        return anyAdvanced;
    }

    /// <summary>
    /// Mutation deferred during pass 1 of the activation-time replay
    /// to be applied in pass 2 once every partition's per-key Set /
    /// Delete entries have been absorbed into the leaf's Cache and
    /// every prepare into <c>_pendingTx</c>. Covers the two mutation
    /// shapes whose apply semantics depend on the global per-shard
    /// state being fully reconstructed:
    /// <para>
    ///     <see cref="MutationKind.TxCommit"/> /
    ///     <see cref="MutationKind.TxAbort"/> - the saga's per-key
    ///     prepares fan out across multiple WAL partitions while the
    ///     terminal lands in a single (shard-routed) partition. Per-
    ///     partition independent replay would observe the terminal
    ///     before some of its prepares had been absorbed into
    ///     <c>_pendingTx</c>, so the terminal's <c>ApplyTxCommit</c>
    ///     would drain an incomplete bucket and the late-arriving
    ///     prepares would be added to <c>_pendingTx</c> after the
    ///     <c>_recentlyTerminal</c> dedup had already accepted the
    ///     txid - leaving the late prepares stranded and silently
    ///     invisible.
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.DeleteRange"/> - the tombstone
    ///     mutation iterates the leaf's Cache at apply time to
    ///     tombstone every in-range key. A range tombstone in
    ///     partition <c>P_t</c> whose target Set entries live in
    ///     partition <c>P_s</c> would see an empty Cache during
    ///     pass 1 (when partition <c>P_t</c> happens to replay
    ///     before partition <c>P_s</c>), tombstone nothing, and let
    ///     the Sets in <c>P_s</c> become visible. Deferring to pass 2
    ///     restores the tombstone-after-its-targets ordering invariant.
    /// </para>
    /// <para>
    ///     Both rationales are about entries in OTHER partitions that pass 1
    ///     has not absorbed yet, so both lapse once every other partition has
    ///     been fully absorbed: every prepare is then in <c>_pendingTx</c> and
    ///     every range-delete target is in the Cache, and the entries above the
    ///     terminal in its own partition were appended after it, so they are
    ///     not dependencies. Pass 1 therefore applies such a mutation in place
    ///     rather than deferring it (issue #1831), which keeps the incremental
    ///     flush ceiling moving for the remainder of the scan. The atomicity
    ///     contract above is unchanged: a terminal is only ever applied when
    ///     its complete prepared-mutation set is already reconstructed.
    /// </para>
    /// </summary>
    private readonly record struct DeferredTerminal(
        int Partition,
        long Offset,
        LatticeMutation Mutation);

    /// <summary>
    /// The minimum interval between over-budget replay warnings for one
    /// (tree, leaf, partition). Chosen so a cold start reports each replaying
    /// leaf partition roughly once a minute rather than once per re-activation
    /// attempt. This is the interval for a key's <b>second</b> line; each
    /// subsequent line for the same key doubles it, up to
    /// <see cref="OverBudgetLogIntervalCeiling"/>.
    /// </summary>
    internal static readonly TimeSpan OverBudgetLogInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Upper bound on the backed-off per-key interval. A leaf partition that
    /// keeps replaying over budget still reports periodically, because the
    /// warning's own fault criterion ("a checkpoint that does NOT advance
    /// across repeats for the SAME leaf is a fault") needs at least two
    /// comparable lines. Backoff makes those lines rarer, never absent, and a
    /// wider gap between them makes a non-advancing checkpoint more conclusive
    /// rather than less.
    /// </summary>
    internal static readonly TimeSpan OverBudgetLogIntervalCeiling = TimeSpan.FromHours(1);

    /// <summary>
    /// The most detail lines one tree may emit in a single
    /// <see cref="OverBudgetLogInterval"/> window, over and above the
    /// first-ever line for a leaf partition, which is never withheld.
    /// <para>
    /// This is the aggregate bound the per-key throttle alone cannot provide.
    /// A per-key throttle bounds each <i>key</i>, but a tree with L leaves and
    /// P WAL partitions has L x P keys, so the total was bounded only by the
    /// size of the tree: on one deployment this single warning was 12,364 of
    /// 26,800 container log lines (46 percent), rolling away the older entries
    /// that were the evidence needed to diagnose the condition producing it
    /// (issue #2100). With this cap the sustained cost of the warning is a
    /// constant per tree per window instead of a function of tree size.
    /// </para>
    /// </summary>
    internal const int OverBudgetDetailLogsPerTreeWindow = 8;

    /// <summary>
    /// Soft cap on <see cref="OverBudgetTreeWindows"/>, swept the same way and
    /// for the same reason as <see cref="OverBudgetLogStampCapacity"/>. Keyed
    /// by tree, so it is small in any ordinary deployment; the cap exists so a
    /// host that creates trees dynamically cannot grow a static map without
    /// bound.
    /// </summary>
    private const int OverBudgetTreeWindowCapacity = 256;

    /// <summary>
    /// How long a tree's window may be retained while it still owes a summary
    /// line before <see cref="PruneOverBudgetTreeWindows"/> drops it anyway.
    /// Without this bound a quiet tree's unreported tally pins its entry
    /// permanently, so the sweep frees nothing and the map grows with every
    /// distinct tree id the process ever sees.
    /// </summary>
    private static readonly TimeSpan OverBudgetTreeWindowRetention = OverBudgetLogIntervalCeiling;

    /// <summary>
    /// When <see cref="PruneOverBudgetLogStamps"/> last ran from the capacity
    /// trigger, or zero if it never has.
    /// </summary>
    private static long _overBudgetLogStampsSweptAtTicks;

    /// <summary>
    /// When <see cref="PruneOverBudgetTreeWindows"/> last ran from the capacity
    /// trigger, or zero if it never has.
    /// </summary>
    private static long _overBudgetTreeWindowsSweptAtTicks;

    /// <summary>
    /// Rate-limits a capacity sweep to at most once per
    /// <see cref="OverBudgetLogInterval"/>, and to one caller at a time.
    /// <para>
    /// A sweep is O(n) over its map, and the capacity trigger fires on every
    /// occurrence that misses while the map is at capacity - so without this
    /// guard a map that is legitimately full turns every novel key into a full
    /// scan. That is the same shape of defect as the log volume this whole gate
    /// exists to bound: work proportional to the size of the deployment on the
    /// path that is supposed to be bounding it.
    /// </para>
    /// </summary>
    /// <param name="lastSweptAtTicks">The sweep's own last-run timestamp field.</param>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    /// <returns><see langword="true"/> when this caller should run the sweep.</returns>
    private static bool TryEnterCapacitySweep(ref long lastSweptAtTicks, long now)
    {
        var last = Interlocked.Read(ref lastSweptAtTicks);
        if (last != 0 && Stopwatch.GetElapsedTime(last, now) < OverBudgetLogInterval)
        {
            return false;
        }

        // Losing the swap means another caller is sweeping right now, which is
        // as good as having swept: the sweep is opportunistic housekeeping, not
        // an admission gate.
        return Interlocked.CompareExchange(ref lastSweptAtTicks, now, last) == last;
    }

    /// <summary>
    /// Soft cap on <see cref="OverBudgetLogStamps"/>. Reaching it triggers an
    /// opportunistic sweep that <b>retires</b> entries whose own backed-off
    /// interval has already elapsed and drops entries that have been retired
    /// for longer than <see cref="OverBudgetLogStampRetention"/>. The cap is a
    /// bound on retained keys, not a hard admission limit - a burst of more
    /// than this many distinct leaf partitions inside one interval is allowed
    /// to exceed it rather than silently losing suppression for the overflow.
    /// </summary>
    private const int OverBudgetLogStampCapacity = 4096;

    /// <summary>
    /// How long a retired stamp (see <see cref="OverBudgetLogStamp"/>) is kept
    /// after the key's last line, purely to deny that key the novelty exemption
    /// in <see cref="ClassifyOverBudgetReplayLog(string, string, int, long)"/>.
    /// <para>
    /// This is what makes novelty a durable property rather than an artefact of
    /// the sweep. Before it, <c>firstEver</c> meant only "no stamp exists", and
    /// <see cref="PruneOverBudgetLogStamps"/> deletes stamps for capacity - so
    /// at the scale this bound exists for, where the map sits at capacity and
    /// churns, an evicted key re-entered cap-exempt on its next occurrence and
    /// a one-time novel burst became a recurring one whose volume again scaled
    /// with the size of the tree. Retiring rather than deleting keeps the key's
    /// identity for a bounded window, so only a key that has genuinely been
    /// silent for the whole retention is treated as novel again - which is also
    /// the correct reading: a fresh run of the condition after that long a gap
    /// is a new report, not a repeat.
    /// </para>
    /// </summary>
    private static readonly TimeSpan OverBudgetLogStampRetention = OverBudgetLogIntervalCeiling;

    /// <summary>
    /// Last-logged timestamps for the over-budget replay warning, keyed by tree,
    /// leaf, and WAL partition. Static because the point is to suppress across
    /// the repeated ACTIVATIONS of the same leaf - per-activation state would
    /// reset every time and suppress nothing.
    /// <para>
    /// The leaf id is part of the key (issue #2023). Without it the key was
    /// (tree, WAL partition), which is <b>not</b> a leaf: the partition ordinal
    /// is iterated <c>[0, WalPartitions)</c> inside every leaf's activation, so
    /// the first leaf to trip the budget suppressed the warning for every other
    /// leaf in that tree and partition for a full minute.
    /// </para>
    /// <para>
    /// Growth is bounded by the number of distinct (tree, leaf, partition)
    /// triples that trip the budget within one backed-off interval, not by the
    /// leaf count and not by the attempt count, because
    /// <see cref="ShouldLogOverBudgetReplay(string, string, int)"/>
    /// sweeps entries whose interval has elapsed once the map reaches
    /// <see cref="OverBudgetLogStampCapacity"/>.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<(string TreeId, string LeafId, int Partition), OverBudgetLogStamp> OverBudgetLogStamps = new();

    /// <summary>
    /// When a leaf partition last reported replaying over budget, and how many
    /// times it has reported in the current run of the condition.
    /// <para>
    /// The count drives the per-key backoff. It is what turns a permanently
    /// replaying leaf partition from a fixed one-line-per-minute cost into a
    /// decaying one, so a condition that persists for hours costs
    /// logarithmically many lines rather than linearly many.
    /// </para>
    /// <para>
    /// A count of zero means the stamp is <b>retired</b>: the key has reported
    /// before but is no longer suppressing anything, either because its own
    /// interval aged out under <see cref="PruneOverBudgetLogStamps"/> or
    /// because the leaf activated within budget again
    /// (<see cref="RetireOverBudgetLogStamp"/>). A retired stamp is due
    /// immediately, so backoff decays rather than being carried into a new run
    /// of the condition, and it still denies the key the novelty exemption
    /// until it is dropped after <see cref="OverBudgetLogStampRetention"/>.
    /// </para>
    /// </summary>
    /// <param name="LoggedAtTicks">
    /// The <see cref="Stopwatch.GetTimestamp"/> reading at which this key last
    /// emitted a line.
    /// </param>
    /// <param name="ConsecutiveLogs">
    /// How many lines this key has emitted in the current run of the condition,
    /// counting from its first-ever one, or zero when the stamp is retired.
    /// Incremented only when a detail line is actually emitted, so a line the
    /// aggregate cap withheld does not advance the backoff.
    /// </param>
    private readonly record struct OverBudgetLogStamp(long LoggedAtTicks, int ConsecutiveLogs);

    /// <summary>
    /// The aggregate detail-line budget for one tree, and the accounting for
    /// the lines that budget withheld. Mutable and guarded by locking the
    /// instance itself, because the roll-and-decide has to be one step: two
    /// activations racing on a plain interlocked counter could each conclude it
    /// had opened the window and both reset the suppressed count, losing the
    /// summary the reset exists to report.
    /// </summary>
    private sealed class OverBudgetTreeWindow
    {
        /// <summary>The <see cref="Stopwatch.GetTimestamp"/> reading the current window opened at.</summary>
        public long WindowStartedAtTicks;

        /// <summary>Detail lines already emitted for this tree in the current window.</summary>
        public int DetailsLogged;

        /// <summary>Detail lines the aggregate cap withheld in the current window.</summary>
        public int Suppressed;

        /// <summary>
        /// Set under this instance's own lock when
        /// <see cref="PruneOverBudgetTreeWindows"/> evicts it, so a caller that
        /// took this instance from <see cref="OverBudgetTreeWindows"/> just
        /// before the sweep can see that it is no longer the tree's window and
        /// take a fresh one. Without it a tally accrued in that gap would be
        /// discarded and the tree's budget would silently reset.
        /// </summary>
        public bool Removed;
    }

    /// <summary>
    /// Per-tree aggregate detail-line budgets. Static for the same reason
    /// <see cref="OverBudgetLogStamps"/> is: the flood being bounded spans
    /// repeated activations, so per-activation state would reset every time and
    /// bound nothing.
    /// </summary>
    private static readonly ConcurrentDictionary<string, OverBudgetTreeWindow> OverBudgetTreeWindows = new();

    /// <summary>
    /// What the over-budget replay logging gate decided for one occurrence.
    /// </summary>
    /// <param name="LogDetail">
    /// <see langword="true"/> when the full per-leaf warning should be emitted.
    /// </param>
    /// <param name="SuppressedInClosedWindow">
    /// How many detail lines the aggregate cap withheld in the window that this
    /// occurrence just closed, or zero when no window closed or none were
    /// withheld. A positive value must be reported as a single summary line, so
    /// that capping is visible in the log rather than silent.
    /// </param>
    internal readonly record struct OverBudgetReplayLogDecision(bool LogDetail, int SuppressedInClosedWindow);

    /// <summary>
    /// True when the over-budget replay warning for this leaf partition is due
    /// again. Suppression is deliberately best-effort under races: two silos may
    /// each emit one line, which is fine - the goal is to stop a re-activation
    /// storm flooding the log, not to guarantee exactly-once logging. The metric
    /// remains the exact count. Internal so the suppression itself is testable
    /// rather than only observable through log output.
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">
    /// The leaf's grain id. Load-bearing: it is what makes successive warnings
    /// comparable, so an operator can evaluate the "checkpoint does not advance"
    /// fault criterion the warning states (issue #2023).
    /// </param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <returns><see langword="true"/> when the warning should be emitted.</returns>
    internal static bool ShouldLogOverBudgetReplay(string treeId, string leafId, int partition)
    {
        var key = (treeId, leafId, partition);
        var now = Stopwatch.GetTimestamp();

        if (!TryReserveOverBudgetReplayLog(key, now, out var observed, out var firstEver))
        {
            return false;
        }

        // The single-tier entry point has no aggregate cap to consult, so the
        // line it reserves is always emitted and the stamp is committed here.
        return firstEver || TryCommitOverBudgetLogStamp(key, observed, now);
    }

    /// <summary>
    /// Throttle stamps for the stalled-replay FAULT warning, kept in a map of
    /// their own rather than sharing <see cref="OverBudgetLogStamps"/>.
    /// <para>
    /// Separation is load-bearing (issue #2149). The cost warning and the fault
    /// warning are keyed identically, so a shared map would let a cost line for
    /// a leaf swallow the fault line for the same leaf for a whole
    /// <see cref="OverBudgetLogInterval"/> - silencing the only signal that
    /// found the livelocked leaf of issue #2165, which was 0.25% of warnings in
    /// one measurement window and 59% in the next. Two maps cost one dictionary
    /// and make the fault line unsuppressible by cost noise.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> StalledReplayLogStamps = new();

    /// <summary>
    /// True when the stalled-replay fault warning for this leaf partition is due
    /// again. Throttled on the same interval as the cost warning but through an
    /// independent stamp map, so cost noise can never suppress a fault.
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <returns><see langword="true"/> when the warning should be emitted.</returns>
    internal static bool ShouldLogStalledReplay(string treeId, string leafId, int partition)
        => ShouldLogThrottled(StalledReplayLogStamps, treeId, leafId, partition);

    /// <summary>
    /// Shared body of the per-(tree, leaf, partition) log throttles. Suppression
    /// is deliberately best-effort under races: two silos may each emit one
    /// line, which is fine - the goal is to stop a re-activation storm flooding
    /// the log, not to guarantee exactly-once logging.
    /// </summary>
    /// <param name="stamps">The stamp map to evaluate and update.</param>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <returns><see langword="true"/> when the warning should be emitted.</returns>
    private static bool ShouldLogThrottled(
        ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> stamps,
        string treeId,
        string leafId,
        int partition)
    {
        var now = Stopwatch.GetTimestamp();
        var key = (treeId, leafId, partition);
        if (!stamps.TryGetValue(key, out var last))
        {
            if (stamps.Count >= OverBudgetLogStampCapacity)
            {
                PruneLogStamps(stamps, now);
            }

            return stamps.TryAdd(key, now);
        }

        if (Stopwatch.GetElapsedTime(last, now) < OverBudgetLogInterval)
        {
            return false;
        }

        return stamps.TryUpdate(key, now, last);
    }

    /// <summary>
    /// Drops every stamp that has already aged past
    /// <see cref="OverBudgetLogInterval"/>. Such an entry would permit the
    /// next warning anyway, so removing it is semantically free - it keeps the
    /// map's retained size tracking the leaf partitions that are currently
    /// tripping the condition rather than every leaf that ever did.
    /// </summary>
    /// <param name="stamps">The stamp map to prune.</param>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    private static void PruneLogStamps(
        ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> stamps,
        long now)
    {
        foreach (var stamp in stamps)
        {
            if (Stopwatch.GetElapsedTime(stamp.Value, now) >= OverBudgetLogInterval)
            {
                stamps.TryRemove(stamp);
            }
        }
    }

    /// <summary>
    /// Retires the stamp for a leaf partition that has just activated
    /// <b>within</b> budget, so the next run of the condition starts from the
    /// base interval instead of inheriting an hour of accumulated backoff.
    /// <para>
    /// Retiring rather than removing is deliberate. Removing would make the key
    /// look novel again, and a novel key is exempt from the aggregate cap, so a
    /// leaf that flapped in and out of budget could re-enter cap-exempt on every
    /// cycle - the exact defect
    /// <see cref="OverBudgetLogStampRetention"/> exists to prevent. A retired
    /// stamp is due immediately, so the regression is still reported promptly;
    /// it simply counts as a repeat rather than a first report, which is what it
    /// is.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal that replayed in budget.</param>
    internal static void RetireOverBudgetLogStamp(string treeId, string leafId, int partition)
    {
        var key = (treeId, leafId, partition);
        if (OverBudgetLogStamps.TryGetValue(key, out var last) && last.ConsecutiveLogs != 0)
        {
            OverBudgetLogStamps.TryUpdate(key, last with { ConsecutiveLogs = 0 }, last);
        }
    }

    /// <summary>
    /// Peeks whether the per-key throttle would allow a line for this key,
    /// <b>without</b> committing the stamp that starts its next backoff.
    /// <para>
    /// The split is load-bearing. The aggregate cap runs after this check and
    /// may still withhold the line, and a key that emitted nothing must not pay
    /// the backoff penalty for it: on a tree with a thousand replaying leaf
    /// partitions, committing here drove every key to the one-hour ceiling
    /// within a handful of rounds while only
    /// <see cref="OverBudgetDetailLogsPerTreeWindow"/> lines an interval were
    /// emitted for the whole tree, so a specific livelocked leaf was named
    /// roughly once ever. That defeats the warning's own fault criterion, which
    /// needs two comparable lines for the SAME leaf. With the commit deferred to
    /// <see cref="TryCommitOverBudgetLogStamp"/>, capped keys genuinely stay due
    /// and rotate through the budget, and the cap alone bounds the volume.
    /// </para>
    /// <para>
    /// A genuinely novel key is the one exception: it is never withheld by the
    /// cap, so its line is certain and its stamp is added here. That also keeps
    /// the <see cref="ConcurrentDictionary{TKey, TValue}.TryAdd"/> race the
    /// single-winner behaviour it has always had - the loser is covered by the
    /// winner's line.
    /// </para>
    /// </summary>
    /// <param name="key">The (tree, leaf, partition) triple being evaluated.</param>
    /// <param name="now">
    /// The <see cref="Stopwatch.GetTimestamp"/> reading to evaluate against.
    /// Passed in rather than read here so the throttle's time-dependent
    /// behaviour is testable without sleeping through a real interval.
    /// </param>
    /// <param name="observed">
    /// The stamp this decision was made against, to be presented back to
    /// <see cref="TryCommitOverBudgetLogStamp"/> as a compare-and-swap witness.
    /// Meaningless when <paramref name="firstEver"/> is <see langword="true"/>.
    /// </param>
    /// <param name="firstEver">
    /// Set to <see langword="true"/> when this key had no stamp at all, so the
    /// occurrence is genuinely novel rather than a repeat, and the stamp has
    /// already been committed. Load-bearing: a novel occurrence is exempt from
    /// the aggregate cap in
    /// <see cref="ClassifyOverBudgetReplayLog(string, string, int, long)"/>,
    /// because bounding volume must never cost the first report of a condition
    /// on a leaf that has not reported one before.
    /// </param>
    /// <returns><see langword="true"/> when a line is due for this key.</returns>
    private static bool TryReserveOverBudgetReplayLog(
        (string TreeId, string LeafId, int Partition) key,
        long now,
        out OverBudgetLogStamp observed,
        out bool firstEver)
    {
        if (!OverBudgetLogStamps.TryGetValue(key, out var last))
        {
            if (OverBudgetLogStamps.Count >= OverBudgetLogStampCapacity
                && TryEnterCapacitySweep(ref _overBudgetLogStampsSweptAtTicks, now))
            {
                PruneOverBudgetLogStamps(now);
            }

            observed = default;
            firstEver = OverBudgetLogStamps.TryAdd(key, new OverBudgetLogStamp(now, 1));
            if (firstEver)
            {
                return true;
            }

            // Lost the add race: the winner is emitting this key's first line,
            // so this occurrence is a repeat and is withheld by the throttle.
            return false;
        }

        observed = last;
        firstEver = false;

        // A retired stamp (count zero) is due immediately: it records only that
        // the key has been seen, not that it is suppressing.
        return last.ConsecutiveLogs == 0
            || Stopwatch.GetElapsedTime(last.LoggedAtTicks, now) >= OverBudgetLogIntervalFor(last.ConsecutiveLogs);
    }

    /// <summary>
    /// Commits the stamp for a line that is actually being emitted, advancing
    /// the key's backoff. Called only once the aggregate cap has agreed to the
    /// line, so a withheld occurrence leaves the stamp untouched and stays due.
    /// </summary>
    /// <param name="key">The (tree, leaf, partition) triple being emitted for.</param>
    /// <param name="observed">
    /// The stamp <see cref="TryReserveOverBudgetReplayLog"/> decided against,
    /// used as the compare-and-swap witness.
    /// </param>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    /// <returns>
    /// <see langword="false"/> when the stamp moved underneath this occurrence,
    /// meaning a concurrent activation for the same key emitted the line
    /// instead. Suppression is deliberately best-effort under races, but losing
    /// the swap is precisely the signal that this occurrence is the duplicate.
    /// </returns>
    private static bool TryCommitOverBudgetLogStamp(
        (string TreeId, string LeafId, int Partition) key,
        OverBudgetLogStamp observed,
        long now)
    {
        // Saturate rather than wrap: the interval is already pinned to the
        // ceiling long before the count could overflow, so clamping costs
        // nothing and removes the overflow case entirely.
        var next = observed.ConsecutiveLogs < int.MaxValue ? observed.ConsecutiveLogs + 1 : observed.ConsecutiveLogs;
        return OverBudgetLogStamps.TryUpdate(key, new OverBudgetLogStamp(now, next), observed);
    }

    /// <summary>
    /// The backed-off interval a key must wait before reporting again, given
    /// how many times it has already reported. Doubles per line from
    /// <see cref="OverBudgetLogInterval"/> and saturates at
    /// <see cref="OverBudgetLogIntervalCeiling"/>.
    /// </summary>
    /// <param name="consecutiveLogs">Lines this key has emitted so far, at least 1.</param>
    /// <returns>The interval before the key's next line is due.</returns>
    private static TimeSpan OverBudgetLogIntervalFor(int consecutiveLogs)
    {
        // Cap the shift well before it could overflow the tick count; the
        // ceiling clamp below has taken over many doublings earlier anyway.
        var shift = Math.Min(Math.Max(consecutiveLogs - 1, 0), 16);
        var ticks = OverBudgetLogInterval.Ticks << shift;
        return ticks >= OverBudgetLogIntervalCeiling.Ticks
            ? OverBudgetLogIntervalCeiling
            : TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    /// Decides whether one over-budget occurrence is logged in full, and
    /// whether a summary line is owed for a window that has just closed.
    /// <para>
    /// Two tiers. The per-key throttle above stops one leaf partition
    /// re-flooding across its own re-activations. This adds the aggregate bound
    /// that a per-key throttle structurally cannot provide, because a tree with
    /// L leaves and P partitions has L x P keys and so L x P times the per-key
    /// rate (issue #2100).
    /// </para>
    /// <para>
    /// <b>The novelty exemption is the load-bearing part.</b> A first-ever
    /// occurrence for a leaf partition always logs, whatever the aggregate
    /// budget says. Bounding volume by dropping first occurrences would make
    /// the log quiet by destroying exactly the signal the warning exists to
    /// carry, and this warning is currently the only field-visible evidence of
    /// the condition in issue #2098. Only <i>repeats</i> are ever withheld by
    /// the cap, and when they are, the count is reported as a summary line, so
    /// capping is not silent while the tree keeps replaying. That summary is
    /// carried by a later occurrence, so a tree's final withheld tally goes
    /// unreported; see <see cref="FlushClosedOverBudgetWindow"/>. The metric
    /// remains the exact census.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <returns>The logging decision for this occurrence.</returns>
    internal static OverBudgetReplayLogDecision ClassifyOverBudgetReplayLog(string treeId, string leafId, int partition)
        => ClassifyOverBudgetReplayLog(treeId, leafId, partition, Stopwatch.GetTimestamp());

    /// <summary>
    /// Decides whether one over-budget occurrence is logged in full, evaluated
    /// against an explicit timestamp.
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <param name="now">
    /// The <see cref="Stopwatch.GetTimestamp"/> reading to evaluate against.
    /// Passed in rather than read here so the window roll and the backoff are
    /// testable without sleeping through a real interval.
    /// </param>
    /// <returns>The logging decision for this occurrence.</returns>
    internal static OverBudgetReplayLogDecision ClassifyOverBudgetReplayLog(string treeId, string leafId, int partition, long now)
    {
        var key = (treeId, leafId, partition);
        if (!TryReserveOverBudgetReplayLog(key, now, out var observed, out var firstEver))
        {
            // Withheld by the per-key throttle, which is not the aggregate cap
            // and is not itself summarised: the exact census is the metric.
            //
            // This occurrence still flushes a summary the tree already owes.
            // Doing so here rather than only on the logging path matters: keys
            // the cap withheld keep their stamp, so they stay due and normally
            // flush the tally promptly, but every key that did log has backed
            // off. Without this, a tally could sit unreported behind those
            // backed-off keys, and a bound nobody is told about reads exactly
            // like the signal having been dropped.
            return new OverBudgetReplayLogDecision(false, FlushClosedOverBudgetWindow(treeId, now));
        }

        if (OverBudgetTreeWindows.Count >= OverBudgetTreeWindowCapacity
            && !OverBudgetTreeWindows.ContainsKey(treeId)
            && TryEnterCapacitySweep(ref _overBudgetTreeWindowsSweptAtTicks, now))
        {
            PruneOverBudgetTreeWindows(now);
        }

        while (true)
        {
            // The state-carrying overload with a static factory. A capturing
            // lambda here would allocate a closure on EVERY occurrence,
            // including ones the gate then withholds - a rate limiter that
            // allocates per occurrence on the path it exists to bound just
            // trades log volume for garbage.
            var window = OverBudgetTreeWindows.GetOrAdd(
                treeId,
                static (_, startedAt) => new OverBudgetTreeWindow { WindowStartedAtTicks = startedAt },
                now);

            lock (window)
            {
                // Evicted by the sweep between the lookup and the lock. The
                // instance is already out of the map, so a fresh GetOrAdd
                // cannot hand back the same one and this cannot spin.
                if (window.Removed)
                {
                    continue;
                }

                var suppressedInClosedWindow = RollWindowIfClosed(window, now);

                // Novel first, and unconditionally. A novel occurrence still
                // spends budget, so that a burst of new leaves crowds out
                // repeats rather than the other way round, but it is never
                // itself withheld.
                if (!firstEver && window.DetailsLogged >= OverBudgetDetailLogsPerTreeWindow)
                {
                    // Withheld by the cap. The stamp is deliberately left
                    // untouched, so this key stays due and takes a slot in a
                    // later window rather than backing off having said nothing.
                    window.Suppressed++;
                    return new OverBudgetReplayLogDecision(false, suppressedInClosedWindow);
                }

                // The line is going out, so the backoff may advance. A novel
                // key committed its stamp when it was added.
                if (!firstEver && !TryCommitOverBudgetLogStamp(key, observed, now))
                {
                    // A concurrent activation for the same key won the swap and
                    // is emitting the line. This occurrence is the duplicate: it
                    // neither logs nor counts as withheld by the cap.
                    return new OverBudgetReplayLogDecision(false, suppressedInClosedWindow);
                }

                window.DetailsLogged++;
                return new OverBudgetReplayLogDecision(true, suppressedInClosedWindow);
            }
        }
    }

    /// <summary>
    /// Rolls <paramref name="window"/> if its interval has elapsed, returning
    /// the number of detail lines the closed window withheld (zero when the
    /// window is still open, or closed having withheld nothing).
    /// </summary>
    /// <param name="window">The tree's window. The caller must hold its lock.</param>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    /// <returns>The count owed as a summary line, or zero.</returns>
    private static int RollWindowIfClosed(OverBudgetTreeWindow window, long now)
    {
        if (Stopwatch.GetElapsedTime(window.WindowStartedAtTicks, now) < OverBudgetLogInterval)
        {
            return 0;
        }

        var suppressed = window.Suppressed;
        window.WindowStartedAtTicks = now;
        window.DetailsLogged = 0;
        window.Suppressed = 0;
        return suppressed;
    }

    /// <summary>
    /// Returns any summary a tree's closed window owes, without opening one.
    /// Used on the path where the per-key throttle withheld the occurrence, so
    /// that a withheld tally is reported by the next occurrence on the tree
    /// whether or not that occurrence is itself logged.
    /// <para>
    /// Deliberately does not create a window: a tree whose occurrences are all
    /// withheld by the per-key throttle owes nothing, and creating an entry for
    /// it would grow the map without changing what is logged.
    /// </para>
    /// <para>
    /// <b>The flush is occurrence-driven, and that is a real limitation.</b>
    /// This path and the window roll are both reached only from a later
    /// occurrence on the same tree, so a tree that withholds repeats and then
    /// falls quiet - the condition resolves, the leaf deactivates, or the
    /// replay completes - never emits that last tally, and its window is
    /// retained rather than pruned precisely because it still owes one. So
    /// "capping is never silent" holds while a tree keeps replaying and does
    /// NOT hold for a tree's final window, which is the condition-resolved case
    /// a reader looking back through the log most wants. The metric is the
    /// exact census there. Closing the gap would need a timer or a background
    /// flush on a path that deliberately has neither, and a leaf deactivating
    /// is not the event to hang one on: the window is per TREE, so one leaf
    /// going away while its siblings still replay would roll the window early
    /// rather than close it.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree to flush.</param>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    /// <returns>The count owed as a summary line, or zero.</returns>
    private static int FlushClosedOverBudgetWindow(string treeId, long now)
    {
        if (!OverBudgetTreeWindows.TryGetValue(treeId, out var window))
        {
            return 0;
        }

        lock (window)
        {
            // Evicted by the sweep between the lookup and the lock: it is no
            // longer the tree's window and owes this caller nothing.
            return window.Removed ? 0 : RollWindowIfClosed(window, now);
        }
    }

    /// <summary>
    /// Drops every <see cref="OverBudgetLogStamps"/> entry whose own backed-off
    /// interval has already elapsed. Such an entry would permit the next
    /// warning anyway, so removing it is semantically free - it keeps the map's
    /// retained size tracking the leaf partitions that are currently tripping
    /// the budget rather than every leaf that ever did.
    /// <para>
    /// The sweep must test each entry's <b>own</b> interval rather than the
    /// base <see cref="OverBudgetLogInterval"/>. A key that has backed off is
    /// still actively suppressing, so evicting it early would both reset its
    /// backoff and make it look novel again - and a novel key is exempt from
    /// the aggregate cap, which would turn the sweep into a way to defeat both
    /// bounds at once.
    /// </para>
    /// </summary>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    /// <remarks>
    /// Internal rather than private so the retirement semantics - that a swept
    /// key is due again but is <b>not</b> novel - can be proven directly by a
    /// test instead of inferred from capacity pressure.
    /// </remarks>
    internal static void PruneOverBudgetLogStamps(long now)
    {
        foreach (var stamp in OverBudgetLogStamps)
        {
            var elapsed = Stopwatch.GetElapsedTime(stamp.Value.LoggedAtTicks, now);

            if (stamp.Value.ConsecutiveLogs == 0)
            {
                // Already retired. Drop it only once the key has been silent for
                // the whole retention, at which point treating its next
                // occurrence as novel is correct rather than an artefact.
                if (elapsed >= OverBudgetLogStampRetention)
                {
                    OverBudgetLogStamps.TryRemove(stamp);
                }

                continue;
            }

            if (elapsed >= OverBudgetLogIntervalFor(stamp.Value.ConsecutiveLogs))
            {
                OverBudgetLogStamps.TryUpdate(stamp.Key, stamp.Value with { ConsecutiveLogs = 0 }, stamp.Value);
            }
        }
    }

    /// <summary>
    /// Drops every <see cref="OverBudgetTreeWindows"/> entry that is closed and
    /// owes no summary line, plus any entry older than
    /// <see cref="OverBudgetTreeWindowRetention"/> whether or not it owes one.
    /// <para>
    /// The age escape hatch is what makes the sweep able to free anything at
    /// all. A tree that withholds repeats and then falls quiet keeps
    /// <see cref="OverBudgetTreeWindow.Suppressed"/> non-zero forever, because
    /// the flush is occurrence-driven (see
    /// <see cref="FlushClosedOverBudgetWindow"/>) - so a skip-if-owing rule
    /// alone pins exactly the entries that will never be reclaimed, and past
    /// the capacity the map grows monotonically with the number of distinct
    /// tree ids the process has ever seen. An hour-stale tally has no
    /// diagnostic value and the metric is the documented exact census, so
    /// dropping it is the right trade against unbounded growth.
    /// </para>
    /// </summary>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    /// <remarks>
    /// Internal rather than private so the age escape hatch can be proven
    /// directly by a test instead of inferred from capacity pressure.
    /// </remarks>
    internal static void PruneOverBudgetTreeWindows(long now)
    {
        foreach (var entry in OverBudgetTreeWindows)
        {
            var window = entry.Value;
            lock (window)
            {
                var elapsed = Stopwatch.GetElapsedTime(window.WindowStartedAtTicks, now);
                if (elapsed < OverBudgetLogInterval)
                {
                    continue;
                }

                if (window.Suppressed != 0 && elapsed < OverBudgetTreeWindowRetention)
                {
                    continue;
                }

                // Mark and remove under the same lock. Checking here and
                // removing after the lock was released let a tally accrued in
                // the gap be discarded silently, resetting the tree's budget.
                window.Removed = true;
                OverBudgetTreeWindows.TryRemove(entry);
            }
        }
    }

    /// <summary>
    /// The minimum interval between activation-temperature sample lines for one
    /// tree. Matched to <see cref="OverBudgetLogInterval"/> so the two lines a
    /// replaying tree can emit stay on the same cadence.
    /// </summary>
    private static readonly TimeSpan ActivationTemperatureLogInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Soft cap on <see cref="ActivationTemperatureLogStamps"/>, with the same
    /// contract as <see cref="OverBudgetLogStampCapacity"/>: reaching it
    /// triggers an opportunistic sweep of entries older than
    /// <see cref="ActivationTemperatureLogInterval"/>, which is free to drop
    /// because an aged-out stamp suppresses nothing.
    /// </summary>
    private const int ActivationTemperatureLogStampCapacity = 4096;

    /// <summary>
    /// Hard cap on the per-tree distinct-cold-leaf set. Reaching it stops the
    /// set growing and latches the reported count as a floor, so the memory a
    /// pathological tree can cost is fixed rather than one entry per leaf.
    /// <para>
    /// 512 is chosen so the distinction the count exists to draw survives at
    /// realistic sizes: the deployed cold arm that prompted issue #2278 was 254
    /// replays on one tree, which resolves exactly here instead of saturating
    /// and collapsing to "at least N". A cap below the arm being diagnosed
    /// would report a floor in precisely the case the reader cares about, which
    /// is the one shape that cannot distinguish a broad arm from a loop.
    /// </para>
    /// <para>
    /// Deliberately not a metric dimension. The distinct population is
    /// unbounded in principle and leaf identity is high-cardinality, so it is
    /// carried in the sample line only - which is also where it is useful,
    /// since the deployed host exposes no metrics endpoint (issue #2148).
    /// </para>
    /// </summary>
    private const int DistinctColdLeafCapacity = 512;

    /// <summary>
    /// Last-emitted timestamps for the activation-temperature sample line,
    /// keyed by <b>tree only</b>. Static for the same reason as
    /// <see cref="OverBudgetLogStamps"/> - the point is to suppress across
    /// repeated activations, and per-activation state would reset every time
    /// and suppress nothing.
    /// <para>
    /// The leaf id is deliberately <b>not</b> part of this key, which is the
    /// opposite of the choice <see cref="OverBudgetLogStamps"/> documents for
    /// issue #2023. That reasoning does not transfer: the over-budget warning
    /// reports a per-leaf checkpoint, and comparing checkpoints across lines
    /// naming different leaves is meaningless, so its key must name the leaf.
    /// This line reports a per-tree aggregate that is already summed over every
    /// leaf, so successive lines for one tree are comparable by construction,
    /// and keying by leaf would emit one line per leaf per interval - the flood
    /// the throttle exists to prevent.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<string, long> ActivationTemperatureLogStamps = new(StringComparer.Ordinal);

    /// <summary>
    /// Cumulative per-tree cold and warm activation-replay totals for the life
    /// of the process. Growth is bounded by the number of distinct trees the
    /// silo activates leaves for - the same bound
    /// <see cref="LatticeMetrics.TagTree"/> already assumes - so, unlike the
    /// stamp maps, this one is never swept: dropping an entry would reset the
    /// totals, and a line whose totals restarted at zero would report a ratio
    /// for an arbitrary sub-window while claiming to be cumulative.
    /// </summary>
    private static readonly ConcurrentDictionary<string, ActivationTemperatureTotals> ActivationTemperatureTotalsByTree = new(StringComparer.Ordinal);

    /// <summary>
    /// One tree's running cold and warm activation-replay totals. Each arm is
    /// exact under concurrency; the returned pair is a sample, so a concurrent
    /// activation on another silo thread may already have advanced the other
    /// arm by the time the pair is read. That is harmless for the ratio, which
    /// is a long-run quantity, and the counter remains the exact count.
    /// </summary>
    private sealed class ActivationTemperatureTotals
    {
        private readonly HashSet<GrainId> _coldLeaves = [];
        private long _cold;
        private long _warm;
        private bool _coldLeavesSaturated;

        /// <summary>
        /// Records one replay on the arm selected by <paramref name="cold"/>
        /// and returns the totals as observed immediately afterwards.
        /// </summary>
        /// <param name="cold">Whether this replay was a cold one.</param>
        /// <param name="leafId">
        /// The activating leaf, used to accumulate the distinct-cold-leaf
        /// population. Only consulted on the cold arm.
        /// </param>
        public ActivationTemperatureSample Add(bool cold, GrainId leafId)
        {
            long coldTotal;
            long warmTotal;
            int distinctCold;
            bool saturated;

            // The distinct set is not lock-free, so the whole update is taken
            // under one lock rather than mixing Interlocked with a guarded set
            // and reading a torn combination out the other side. This runs once
            // per permitted activation replay, which is orders of magnitude
            // rarer than a read.
            lock (_coldLeaves)
            {
                if (cold)
                {
                    _cold++;

                    // Bounded on purpose (see the field's capacity constant):
                    // once saturated the set stops growing and the count is
                    // reported as a floor, so a pathological tree costs a fixed
                    // amount of memory rather than one entry per leaf.
                    if (!_coldLeavesSaturated)
                    {
                        _coldLeaves.Add(leafId);
                        if (_coldLeaves.Count >= DistinctColdLeafCapacity)
                        {
                            _coldLeavesSaturated = true;
                        }
                    }
                }
                else
                {
                    _warm++;
                }

                coldTotal = _cold;
                warmTotal = _warm;
                distinctCold = _coldLeaves.Count;
                saturated = _coldLeavesSaturated;
            }

            return new ActivationTemperatureSample(coldTotal, warmTotal, distinctCold, saturated);
        }
    }

    /// <summary>
    /// A tree's cumulative activation-replay totals, plus the shape of its cold
    /// arm.
    /// </summary>
    /// <param name="Cold">Cumulative cold replays for the tree.</param>
    /// <param name="Warm">Cumulative warm replays for the tree.</param>
    /// <param name="DistinctColdLeaves">
    /// How many <em>distinct</em> leaves make up <paramref name="Cold"/>, capped
    /// at <see cref="DistinctColdLeafCapacity"/>.
    /// </param>
    /// <param name="DistinctColdLeavesSaturated">
    /// <see langword="true"/> when the cap was reached, so
    /// <paramref name="DistinctColdLeaves"/> is a floor rather than an exact
    /// count.
    /// </param>
    internal readonly record struct ActivationTemperatureSample(
        long Cold,
        long Warm,
        int DistinctColdLeaves,
        bool DistinctColdLeavesSaturated);

    /// <summary>
    /// Records one permitted activation replay against its tree's cumulative
    /// cold / warm totals, and returns those totals when the sample line is due
    /// again for that tree - or <see langword="null"/> when the line is
    /// currently suppressed.
    /// <para>
    /// Accumulation is unconditional and happens <b>before</b> the throttle is
    /// consulted, which is the property that makes the design work. A
    /// rate-limited line that announced each activation would undercount by an
    /// unknown factor, because suppression is invisible in the output; a
    /// rate-limited <b>sample of a counter</b> does not, because every
    /// suppressed activation is still in the totals the next emitted line
    /// prints. So any one line yields the cold:warm ratio and any two yield the
    /// rate between them.
    /// </para>
    /// <para>
    /// Suppression is best-effort under races, exactly as
    /// <see cref="ShouldLogOverBudgetReplay"/> is: two threads may each emit a
    /// line, which costs a duplicate sample and nothing else. Internal so the
    /// accumulate-then-throttle composition is testable as one unit rather than
    /// re-assembled by a test in an order production does not use.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree the activating leaf belongs to.</param>
    /// <param name="cold">
    /// <see langword="true"/> when the activation replays from the <c>-1</c>
    /// sentinel with no snapshot or cache anchor.
    /// </param>
    /// <param name="now">
    /// The <see cref="Stopwatch.GetTimestamp"/> reading to evaluate the
    /// throttle at. Supplied by the caller so a test can advance time
    /// deterministically instead of waiting out the interval.
    /// </param>
    /// <returns>
    /// The tree's cumulative <c>(cold, warm)</c> totals when a line is due,
    /// otherwise <see langword="null"/>.
    /// </returns>
    internal static ActivationTemperatureSample? ObserveLeafActivationReplay(
        string treeId, bool cold, GrainId leafId, long now)
    {
        var totals = ActivationTemperatureTotalsByTree
            .GetOrAdd(treeId, static _ => new ActivationTemperatureTotals())
            .Add(cold, leafId);

        return ShouldLogActivationTemperature(treeId, now) ? totals : null;
    }

    /// <summary>
    /// True when the activation-temperature sample line is due again for
    /// <paramref name="treeId"/>. Same shape as
    /// <see cref="ShouldLogOverBudgetReplay"/> - a sibling stamp map, its own
    /// interval, and the same aged-out sweep at capacity.
    /// </summary>
    /// <param name="treeId">The tree the activating leaf belongs to.</param>
    /// <param name="now">The timestamp to evaluate the interval against.</param>
    /// <returns><see langword="true"/> when the line should be emitted.</returns>
    private static bool ShouldLogActivationTemperature(string treeId, long now)
    {
        if (!ActivationTemperatureLogStamps.TryGetValue(treeId, out var last))
        {
            if (ActivationTemperatureLogStamps.Count >= ActivationTemperatureLogStampCapacity)
            {
                PruneActivationTemperatureLogStamps(now);
            }

            return ActivationTemperatureLogStamps.TryAdd(treeId, now);
        }

        if (Stopwatch.GetElapsedTime(last, now) < ActivationTemperatureLogInterval)
        {
            return false;
        }

        return ActivationTemperatureLogStamps.TryUpdate(treeId, now, last);
    }

    /// <summary>
    /// Drops every <see cref="ActivationTemperatureLogStamps"/> entry that has
    /// already aged past <see cref="ActivationTemperatureLogInterval"/>. Such an
    /// entry would permit the next line anyway, so removing it is semantically
    /// free. The cumulative totals are a separate map and are never swept.
    /// </summary>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    private static void PruneActivationTemperatureLogStamps(long now)
    {
        foreach (var stamp in ActivationTemperatureLogStamps)
        {
            if (Stopwatch.GetElapsedTime(stamp.Value, now) >= ActivationTemperatureLogInterval)
            {
                ActivationTemperatureLogStamps.TryRemove(stamp);
            }
        }
    }

    /// <summary>
    /// How many cold activations of one leaf must be cancelled IN A ROW, with no
    /// successful activation in between, before the self-reinforcing cold replay
    /// loop of issue #2280 is escalated.
    /// <para>
    /// <b>Calibrated from the measured distribution, not chosen.</b> The field
    /// measurement on issue #2278 recorded 79 runtime cancellations over roughly
    /// 40 minutes, all on <c>bplusleaf</c>, across 67 distinct leaves: 55 leaves
    /// cancelled ONCE, 12 cancelled TWICE, and NONE more than twice. That is
    /// leaves occasionally repeating, not leaves trapped. A threshold of 2 would
    /// therefore fire on 12 of 67 leaves - roughly a fifth of the population -
    /// in a NORMAL 40-minute window, and a warning that fires always is a
    /// warning that gets muted, taking the real signal with it. 3 sits one above
    /// the highest value the field has produced, so this diagnostic is not
    /// expected to fire at all in healthy operation, which is what lets a single
    /// occurrence be treated as a finding.
    /// </para>
    /// <para>
    /// It is also robust to a real ambiguity in that evidence: the log never
    /// recorded whether the 12 twice-cancelled leaves activated successfully in
    /// between, so each reads as either two streaks of 1 or one streak of 2 -
    /// and NEITHER reading reaches 3. A threshold of 2 would have had to guess.
    /// </para>
    /// <para>
    /// (If you are re-deriving this: the figures originally published on #2278 -
    /// "27 cancelled more than once, one four times" - were impossible on their
    /// own arithmetic, since 79 cancellations spread over 67 distinct leaves
    /// leaves a surplus of only 12. Use the corrected distribution above.)
    /// </para>
    /// </summary>
    internal const int ColdReplayLoopThreshold = 3;

    /// <summary>
    /// The most leaves whose cold-cancellation streaks are tracked at once,
    /// sized like <see cref="DistinctColdLeafCapacity"/> to bound silo-static
    /// memory.
    /// <para>
    /// The saturation behaviour is deliberately ASYMMETRIC: at capacity the map
    /// stops admitting NEW leaves, while leaves already tracked keep counting.
    /// So saturation can only ever cause a false negative - the diagnostic is a
    /// floor, never an overstatement. That is the right way round for a signal
    /// whose entire value is that it can be believed when it fires.
    /// </para>
    /// </summary>
    private const int ColdReplayLoopStreakCapacity = 4096;

    /// <summary>
    /// Minimum interval between cold-replay-loop warnings for the same leaf.
    /// The counter is not throttled: a log line has a flood to prevent and a
    /// counter does not.
    /// </summary>
    private static readonly TimeSpan ColdReplayLoopLogInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Per-leaf runs of consecutive cold cancellations, silo-scoped.
    /// <para>
    /// <b>Why consecutive, and why the reset exists.</b> A CUMULATIVE count with
    /// a fixed threshold is not merely noisier - it is guaranteed to fire
    /// falsely given enough uptime. At the measured rate of 79 cancellations per
    /// 40 minutes a perfectly healthy leaf that is occasionally cancelled
    /// accumulates without bound, so it reaches ANY fixed threshold eventually.
    /// That makes the threshold a function of PROCESS AGE rather than of leaf
    /// health, which is precisely the property a diagnostic must not have.
    /// Resetting on a successful activation is what makes the count mean "this
    /// leaf cannot escape under its own power" instead of "this process has been
    /// up a while".
    /// </para>
    /// <para>
    /// The reset is therefore load-bearing, and it is also the part a later
    /// simplifier is most likely to remove as redundant bookkeeping. It is not.
    /// </para>
    /// <para>
    /// Static for the same reason the stamp maps are: the entire point is to
    /// remember across activations, and a failed activation destroys its grain
    /// instance, so per-activation state would reset every time and observe
    /// nothing.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<GrainId, ColdReplayCancellationStreak> ColdReplayCancellationStreaks = new();

    /// <summary>
    /// Last-emitted timestamps for the cold-replay-loop warning, keyed by leaf.
    /// </summary>
    private static readonly ConcurrentDictionary<GrainId, long> ColdReplayLoopLogStamps = new();

    /// <summary>
    /// One leaf's run of consecutive cold cancellations, split by whether the
    /// activation had begun replaying or was still queued for the replay permit.
    /// </summary>
    /// <param name="ConsecutiveCancellations">
    /// Cold cancellations since this leaf last activated successfully.
    /// </param>
    /// <param name="DuringReplay">
    /// How many of them cancelled a replay already in progress, losing work.
    /// </param>
    /// <param name="AwaitingPermit">
    /// How many of them were cancelled while still queued for the replay permit,
    /// having done no work to lose. Carried separately because the two call for
    /// different remedies - a leaf starved at the gate is a concurrency problem,
    /// a leaf cut off mid-replay is a bounding problem - and a single total would
    /// let one masquerade as the other.
    /// </param>
    internal readonly record struct ColdReplayLoopSample(
        int ConsecutiveCancellations,
        int DuringReplay,
        int AwaitingPermit);

    /// <summary>
    /// One leaf's mutable streak. Updated under its own lock: the three fields
    /// must advance together or a reader could observe a total that disagrees
    /// with its own split.
    /// </summary>
    private sealed class ColdReplayCancellationStreak
    {
        private int _consecutive;
        private int _duringReplay;
        private int _awaitingPermit;

        public ColdReplayLoopSample Record(bool awaitingPermit)
        {
            lock (this)
            {
                _consecutive++;
                if (awaitingPermit)
                {
                    _awaitingPermit++;
                }
                else
                {
                    _duringReplay++;
                }

                return new ColdReplayLoopSample(_consecutive, _duringReplay, _awaitingPermit);
            }
        }
    }

    /// <summary>
    /// Records one cold cancellation against <paramref name="leafId"/>'s streak
    /// and returns the streak as observed immediately afterwards, or
    /// <see langword="null"/> when the streak map is saturated and this leaf was
    /// not already being tracked (see
    /// <see cref="ColdReplayLoopStreakCapacity"/>).
    /// </summary>
    /// <param name="leafId">The leaf whose activation was cancelled.</param>
    /// <param name="awaitingPermit">
    /// Whether the cancellation arrived while the activation was still queued
    /// for the replay permit rather than replaying.
    /// </param>
    /// <returns>The streak after recording, or <see langword="null"/>.</returns>
    internal static ColdReplayLoopSample? ObserveColdReplayCancellation(GrainId leafId, bool awaitingPermit)
    {
        if (!ColdReplayCancellationStreaks.TryGetValue(leafId, out var streak))
        {
            if (ColdReplayCancellationStreaks.Count >= ColdReplayLoopStreakCapacity)
            {
                return null;
            }

            streak = ColdReplayCancellationStreaks.GetOrAdd(
                leafId, static _ => new ColdReplayCancellationStreak());
        }

        return streak.Record(awaitingPermit);
    }

    /// <summary>
    /// Clears <paramref name="leafId"/>'s cold-cancellation streak because the
    /// leaf activated successfully. This is the half of the mechanism that makes
    /// the count consecutive rather than cumulative - see
    /// <see cref="ColdReplayCancellationStreaks"/> for why that distinction
    /// decides whether the threshold measures leaf health or process age.
    /// <para>
    /// The warning's throttle stamp is dropped with it, so a leaf that recovers
    /// and later falls back into the loop warns immediately instead of being
    /// silenced by the interval left over from its previous run.
    /// </para>
    /// </summary>
    /// <param name="leafId">The leaf that activated successfully.</param>
    internal static void ForgetColdReplayCancellations(GrainId leafId)
    {
        ColdReplayCancellationStreaks.TryRemove(leafId, out _);
        ColdReplayLoopLogStamps.TryRemove(leafId, out _);
    }

    /// <summary>
    /// True when the cold-replay-loop warning is due again for
    /// <paramref name="leafId"/>. Same shape as the activation-temperature
    /// throttle: a sibling stamp map, its own interval, and the same aged-out
    /// sweep at capacity.
    /// </summary>
    /// <param name="leafId">The leaf the warning would name.</param>
    /// <param name="now">The timestamp to evaluate the interval against.</param>
    /// <returns><see langword="true"/> when the line should be emitted.</returns>
    private static bool ShouldLogColdReplayLoop(GrainId leafId, long now)
    {
        if (!ColdReplayLoopLogStamps.TryGetValue(leafId, out var last))
        {
            if (ColdReplayLoopLogStamps.Count >= ColdReplayLoopStreakCapacity)
            {
                PruneColdReplayLoopLogStamps(now);
            }

            return ColdReplayLoopLogStamps.TryAdd(leafId, now);
        }

        if (Stopwatch.GetElapsedTime(last, now) < ColdReplayLoopLogInterval)
        {
            return false;
        }

        return ColdReplayLoopLogStamps.TryUpdate(leafId, now, last);
    }

    /// <summary>
    /// Drops every <see cref="ColdReplayLoopLogStamps"/> entry that has already
    /// aged past <see cref="ColdReplayLoopLogInterval"/>. Such an entry would
    /// permit the next line anyway, so removing it is semantically free. The
    /// streaks themselves are a separate map and are never swept - dropping one
    /// would silently forgive a leaf that has not recovered.
    /// </summary>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    private static void PruneColdReplayLoopLogStamps(long now)
    {
        foreach (var stamp in ColdReplayLoopLogStamps)
        {
            if (Stopwatch.GetElapsedTime(stamp.Value, now) >= ColdReplayLoopLogInterval)
            {
                ColdReplayLoopLogStamps.TryRemove(stamp);
            }
        }
    }

    /// <summary>
    /// Records one cold cancellation against this leaf's streak and, once the
    /// streak reaches <see cref="ColdReplayLoopThreshold"/>, emits the counter
    /// and the throttled warning that name the self-reinforcing cold replay loop
    /// of issue #2280.
    /// <para>
    /// The counter fires on EVERY cancellation at or above the threshold, so a
    /// leaf that stays stuck carries a rate rather than a single edge; the
    /// warning is throttled per leaf because a log line does have a flood to
    /// prevent and a counter does not.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree the cancelled leaf belongs to.</param>
    /// <param name="leafId">The cancelled leaf.</param>
    /// <param name="awaitingPermit">
    /// Whether the cancellation arrived while still queued for the replay
    /// permit rather than mid-replay.
    /// </param>
    /// <param name="now">
    /// The <see cref="Stopwatch.GetTimestamp"/> reading to evaluate the warning
    /// throttle at. Supplied by the caller so a test can advance time
    /// deterministically instead of waiting out the interval.
    /// </param>
    private void EscalateColdReplayCancellation(
        string treeId, GrainId leafId, bool awaitingPermit, long now)
    {
        var streak = ObserveColdReplayCancellation(leafId, awaitingPermit);
        if (streak is not { } sample || sample.ConsecutiveCancellations < ColdReplayLoopThreshold)
        {
            return;
        }

        LatticeMetrics.LeafColdReplayLoop.Add(
            1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
            LatticeTenantLabel.ForTree(treeId));

        if (!ShouldLogColdReplayLoop(leafId, now))
        {
            return;
        }

        var logger = ResolveLogger();
        if (logger is null || !logger.IsEnabled(LogLevel.Warning))
        {
            return;
        }

        // The line names the pathology in full, because the deployed host
        // exposes no metrics endpoint (issue #2148) and because this loop
        // previously ran for the entire life of a container without emitting a
        // single line that named it - it was found by correlating two unrelated
        // counters, which is not a thing an operator can be expected to do.
        logger.LogWarning(
            "SELF-REINFORCING COLD REPLAY LOOP: leaf '{LeafId}' of tree '{TreeId}' has now had "
            + "{ConsecutiveCancellations} cold activations cancelled in a row with no successful "
            + "activation in between ({DuringReplay} cancelled mid-replay, {AwaitingPermit} cancelled "
            + "while still queued for a replay permit), which is at or past the escalation threshold of "
            + "{Threshold}. A cold activation replays the whole readable WAL window; when it is "
            + "cancelled it latches neither signal the snapshot capture gate requires, so no snapshot is "
            + "banked, the next activation finds no anchor and replays the whole window again. The "
            + "condition that causes the cancellation is therefore REPRODUCED BY the cancellation, and "
            + "this leaf is not expected to escape on its own. The count is CONSECUTIVE and resets on "
            + "any successful activation, so it measures this leaf's health and not how long this "
            + "process has been up. The threshold is set one above the highest value seen in the field "
            + "measurement behind issue #2280, so this line is not expected to appear in normal "
            + "operation. Remedies are tracked as issues #2411 (bounding a cold replay), #2279 (replay "
            + "concurrency oversubscription) and #2256 (replay permit leak); a high "
            + "queued-for-permit share points at the latter two, a high mid-replay share at the first. "
            + "This is a DIAGNOSTIC: nothing here changes the leaf's behaviour, and the activation "
            + "still fails as it did before.",
            leafId,
            treeId,
            sample.ConsecutiveCancellations,
            sample.DuringReplay,
            sample.AwaitingPermit,
            ColdReplayLoopThreshold);
    }

    /// <summary>
    /// Silo-scoped record of the persisted checkpoint each leaf partition was
    /// last seen replaying from, used to tell a slow replay from a STALLED one
    /// (issue #2149, fault shape of issue #2165).
    /// <para>
    /// The old over-budget warning stated the fault criterion in prose - "a
    /// checkpoint that does NOT advance across repeats of this warning for the
    /// SAME leaf and partition is a fault" - and left an operator to evaluate it
    /// by hand across a log. That is exactly the evaluation that found the leaf
    /// of issue #2165 (livelocked 6h52m at an unchanging checkpoint, losing
    /// ~15.8 writes/hour), and exactly the evaluation that could not be made
    /// once 19,604 benign lines were mixed in among the 35 that mattered. This
    /// map performs it in the process instead.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<(string TreeId, string LeafId, int Partition), ReplayCheckpointObservation> ReplayCheckpointObservations = new();

    /// <summary>
    /// One leaf partition's most recent replay-checkpoint observation: the
    /// checkpoint itself, when this silo first saw that value for this leaf
    /// partition, and how many consecutive replays have re-entered from it
    /// without it moving.
    /// </summary>
    /// <param name="Checkpoint">The persisted checkpoint last observed.</param>
    /// <param name="FirstObservedAt">
    /// <see cref="Stopwatch.GetTimestamp"/> at the first observation of this
    /// checkpoint value, which is where the current stall run starts.
    /// </param>
    /// <param name="Repeats">
    /// Consecutive re-entries from an unchanged checkpoint. Zero on the first
    /// observation, which is not yet evidence of anything.
    /// </param>
    private readonly record struct ReplayCheckpointObservation(long Checkpoint, long FirstObservedAt, int Repeats);

    /// <summary>
    /// The verdict on one replay-checkpoint observation: whether the leaf
    /// partition re-entered replay from an unchanged checkpoint, and if so how
    /// many consecutive times and over what span.
    /// <para>
    /// <see cref="Repeats"/> and <see cref="Span"/> exist because a single
    /// repeat and a permanent freeze are the same event in isolation and must
    /// not read the same (issue #2285). An activation torn down mid-replay by a
    /// cancellation or a timeout produces a short burst that then stops; a leaf
    /// that genuinely cannot converge keeps reporting with a rising count over
    /// a widening span.
    /// </para>
    /// </summary>
    /// <param name="IsStall">Whether the checkpoint was unchanged on a repeat.</param>
    /// <param name="Repeats">Consecutive unchanged re-entries, 1 on the first stall.</param>
    /// <param name="Span">Elapsed time since this checkpoint was first observed.</param>
    internal readonly record struct ReplayStallObservation(bool IsStall, int Repeats, TimeSpan Span);

    /// <summary>
    /// Soft cap on <see cref="ReplayCheckpointObservations"/>. Unlike the log
    /// stamps there is no age at which an observation is free to drop - a stall
    /// is detected by comparing against an ARBITRARILY old prior observation -
    /// so overflow is handled by shedding entries rather than by pruning old
    /// ones. Which entries are shed is not arbitrary; see
    /// <see cref="EvictReplayCheckpointObservations"/>.
    /// </summary>
    internal const int ReplayCheckpointObservationCapacity = 8192;

    /// <summary>
    /// The point overflow eviction must get the map back below, so that the
    /// next eviction is at least this many insertions away. Without a low-water
    /// mark an eviction that freed only a handful of entries would leave the map
    /// at capacity and re-run its full scan on nearly every subsequent insert.
    /// </summary>
    private const int ReplayCheckpointObservationLowWater = ReplayCheckpointObservationCapacity / 2;

    /// <summary>
    /// Live entry count of <see cref="ReplayCheckpointObservations"/>. Test seam
    /// only, so the memory bound the capacity exists to enforce can be asserted
    /// rather than assumed.
    /// </summary>
    internal static int ReplayCheckpointObservationCountForTests => ReplayCheckpointObservations.Count;

    /// <summary>
    /// Records the checkpoint this leaf partition is replaying from and reports
    /// whether it is UNCHANGED since the previous observation on this silo -
    /// that is, whether the previous activation of this same leaf partition
    /// made no durable forward progress at all.
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <param name="checkpoint">The persisted checkpoint this replay starts from.</param>
    /// <returns>
    /// A <see cref="ReplayStallObservation"/> whose <c>IsStall</c> is
    /// <see langword="true"/> when a previous observation exists for this leaf
    /// partition and its checkpoint is identical, carrying the consecutive
    /// repeat count and the span since that checkpoint was first seen.
    /// <c>IsStall</c> is <see langword="false"/> on the first observation
    /// (nothing to compare against) and whenever the checkpoint has advanced.
    /// </returns>
    internal static ReplayStallObservation NoteReplayCheckpointObservation(string treeId, string leafId, int partition, long checkpoint)
    {
        var key = (treeId, leafId, partition);
        var now = Stopwatch.GetTimestamp();
        var stalled = ReplayCheckpointObservations.TryGetValue(key, out var previous)
            && previous.Checkpoint == checkpoint;

        if (!stalled)
        {
            if (ReplayCheckpointObservations.Count >= ReplayCheckpointObservationCapacity)
            {
                EvictReplayCheckpointObservations();
            }

            ReplayCheckpointObservations[key] = new ReplayCheckpointObservation(checkpoint, now, 0);
            return default;
        }

        var repeats = previous.Repeats + 1;
        ReplayCheckpointObservations[key] = previous with { Repeats = repeats };
        return new ReplayStallObservation(
            true,
            repeats,
            Stopwatch.GetElapsedTime(previous.FirstObservedAt, now));
    }

    /// <summary>
    /// Sheds observations when the map overflows, preferring the entries that
    /// carry NO stall run.
    /// <para>
    /// The map previously shed everything wholesale, on the ground that
    /// "clearing loses at most one repeat of the fault warning per affected
    /// leaf: a genuinely stuck leaf re-activates continuously and is re-observed
    /// on its next attempt". That was true when an observation held only a
    /// checkpoint. It stopped being true when issue #2285 added
    /// <see cref="ReplayCheckpointObservation.Repeats"/> and
    /// <see cref="ReplayCheckpointObservation.FirstObservedAt"/>: those are the
    /// two quantities the stall line now instructs an operator to judge a freeze
    /// by, and a wholesale clear restarts both, so the stuck leaf re-presents at
    /// repeat 1 over a zero span - wearing the exact signature of the transient
    /// burst that #2285 was mistakenly filed over. The observation immediately
    /// after a clear also compares against nothing, so it reports no stall and
    /// the counter the same line advertises as "the exact census of the
    /// condition" is not incremented.
    /// </para>
    /// <para>
    /// The asymmetry that fixes it: a converging entry is fully reconstructed by
    /// its very next observation, and a stall run is not reconstructible at all -
    /// it is the accumulated history. So evict the reconstructible entries and
    /// keep the rest. On a healthy silo virtually every entry is converging, so
    /// this sheds virtually everything, exactly as before.
    /// </para>
    /// <para>
    /// The wholesale clear survives as the fallback, which is what keeps the cap
    /// a real bound: if too few entries were reclaimable the map is emptied
    /// anyway, so a silo whose tracked leaf partitions are overwhelmingly
    /// stalling degrades to precisely today's behaviour and never grows past
    /// capacity. The low-water mark is what stops the scan being quadratic - it
    /// guarantees at least <see cref="ReplayCheckpointObservationLowWater"/>
    /// insertions before the next eviction, rather than letting an eviction that
    /// freed a handful of entries leave the map at capacity to re-scan on the
    /// next insert.
    /// </para>
    /// </summary>
    private static void EvictReplayCheckpointObservations()
    {
        foreach (var observation in ReplayCheckpointObservations)
        {
            if (observation.Value.Repeats == 0)
            {
                // Pair-wise removal, so an entry that acquired a stall run
                // between this scan reading it and removing it is left alone
                // rather than silently discarded.
                ReplayCheckpointObservations.TryRemove(observation);
            }
        }

        if (ReplayCheckpointObservations.Count > ReplayCheckpointObservationLowWater)
        {
            ReplayCheckpointObservations.Clear();
        }
    }

    /// <summary>
    /// Clears the throttle and observation state these warnings keep across
    /// activations. Test seam only: the maps are static and silo-scoped, so a
    /// test that asserts on first-observation or first-warning behaviour needs
    /// them empty regardless of what ran before it.
    /// </summary>
    internal static void ResetReplayWarningStateForTests()
    {
        OverBudgetLogStamps.Clear();
        StalledReplayLogStamps.Clear();
        ReplayCheckpointObservations.Clear();
        ColdReplayCancellationStreaks.Clear();
        ColdReplayLoopLogStamps.Clear();
    }

    /// <summary>
    /// Replay-scoped ledger of the WAL offsets whose mutations were deferred
    /// out of pass 1, kept per partition and <b>resolvable</b>: an offset is
    /// struck off the moment its mutation is actually applied, so the
    /// incremental-flush ceiling recovers instead of staying pinned behind the
    /// first deferred mutation for the whole replay (issue #1831).
    /// <para>
    /// Replaces the monotonically non-increasing <c>lowestDeferredOffset</c>
    /// scalar the incremental flush used to clamp against. That scalar was
    /// only ever lowered, so the first <see cref="MutationKind.DeleteRange"/> /
    /// <see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>
    /// a partition emitted pinned its ceiling for the entire remainder of the
    /// replay, leaving all durable progress to the post-pass-2 reconciliation -
    /// which only runs when the whole replay fits inside the activation window.
    /// </para>
    /// <para>
    /// <b>Hot-path shape.</b> Offsets arrive in strictly increasing order
    /// within a partition (the WAL is read in offset order), so each
    /// partition's buffer is sorted and the lowest unresolved offset is the
    /// entry at a head cursor - an O(1) array read, allocation-free, evaluated
    /// once per slice. Buffers are allocated lazily per partition and grown by
    /// doubling, so the ledger's cost tracks the number of DEFERRED offsets and
    /// never the number of replayed records.
    /// </para>
    /// </summary>
    internal sealed class DeferredOffsetLedger
    {
        /// <summary>
        /// Marks a struck-off slot. WAL offsets are non-negative, so <c>-1</c>
        /// cannot collide with a real offset - including offset <c>0</c>, which
        /// a cold replay under the checkpoint override genuinely reads.
        /// </summary>
        private const long ResolvedSlot = -1L;

        private const int InitialCapacity = 4;

        private readonly long[]?[] _offsets;
        private readonly int[] _counts;
        private readonly int[] _heads;

        /// <summary>
        /// Creates a ledger covering <paramref name="partitionCount"/> WAL
        /// partitions. No per-partition buffer is allocated until that
        /// partition actually defers something.
        /// </summary>
        internal DeferredOffsetLedger(int partitionCount)
        {
            _offsets = new long[partitionCount][];
            _counts = new int[partitionCount];
            _heads = new int[partitionCount];
        }

        /// <summary>
        /// Records <paramref name="offset"/> as deferred (and therefore
        /// unapplied) under <paramref name="partition"/>. Callers append in
        /// increasing offset order within a partition.
        /// </summary>
        internal void Add(int partition, long offset)
        {
            var buffer = _offsets[partition];
            var count = _counts[partition];
            if (buffer is null)
            {
                buffer = new long[InitialCapacity];
                _offsets[partition] = buffer;
            }
            else if (count == buffer.Length)
            {
                var grown = new long[buffer.Length * 2];
                Array.Copy(buffer, grown, count);
                _offsets[partition] = grown;
                buffer = grown;
            }

            buffer[count] = offset;
            _counts[partition] = count + 1;
        }

        /// <summary>
        /// Strikes <paramref name="offset"/> off <paramref name="partition"/>'s
        /// unresolved set once its mutation has been applied. Resolution
        /// normally arrives in the same order the offsets were added, which is
        /// the O(1) head-advance path; an out-of-order resolution marks its
        /// slot and the head skips it when it gets there. An offset that was
        /// never added, or that was already resolved, is ignored.
        /// </summary>
        internal void Resolve(int partition, long offset)
        {
            var buffer = _offsets[partition];
            if (buffer is null)
                return;

            var count = _counts[partition];
            var head = _heads[partition];
            for (var i = head; i < count; i++)
            {
                if (buffer[i] != offset)
                    continue;
                buffer[i] = ResolvedSlot;
                break;
            }

            while (head < count && buffer[head] == ResolvedSlot)
                head++;
            _heads[partition] = head;
        }

        /// <summary>
        /// The lowest still-unresolved deferred offset in
        /// <paramref name="partition"/>, or <see cref="long.MaxValue"/> when
        /// the partition holds none. The incremental flush clamps strictly
        /// below this value, so a partition with nothing outstanding is free to
        /// advance to its applied frontier.
        /// </summary>
        internal long MinUnresolved(int partition)
        {
            var head = _heads[partition];
            return head < _counts[partition] ? _offsets[partition]![head] : long.MaxValue;
        }
    }

    /// <summary>
    /// Flushes <paramref name="partition"/>'s projection checkpoint up to the
    /// highest offset below which every entry is fully applied, and returns
    /// <c>true</c> when the durable position actually advanced. Shared by the
    /// per-slice flush in pass 1 and the per-terminal flush in pass 2 so both
    /// compute the ceiling from one place and can never drift apart.
    /// <para>
    /// The ceiling is the minimum of three bounds, so it can never license a
    /// checkpoint (or the durable materialiser pin it drives) past an offset
    /// that is not yet applied:
    /// </para>
    /// <list type="number">
    /// <item><paramref name="maxApplied"/> - the highest offset this replay has
    /// reached in the partition.</item>
    /// <item>One below the partition's lowest still-unresolved deferred offset
    /// (<see cref="DeferredOffsetLedger.MinUnresolved"/>), because a deferred
    /// mutation is applied only when it drains.</item>
    /// <item>One below any unresolved saga prepare in the partition
    /// (<see cref="MinUnresolvedPrepareOffsetForPartition"/>), because a
    /// resumed replay must re-read the prepare to rebuild <c>_pendingTx</c>.
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> applies this
    /// clamp internally too; applying it here as well keeps the cadence from
    /// issuing redundant force-flushes while a prepare sits open below
    /// <paramref name="maxApplied"/>.</item>
    /// </list>
    /// <para>
    /// The flush is skipped unless it strictly advances the partition's current
    /// position, which also keeps
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/>'s monotonic guard
    /// and its idempotent-re-assert force-flush out of the loop. Persisting is
    /// coalesced per <c>MaterialiserCheckpointInterval</c> /
    /// <c>MaterialiserCheckpointEntries</c> inside that seam and drives the
    /// periodic snapshot capture.
    /// </para>
    /// </summary>
    private async Task<bool> TryFlushRecoveredCeilingAsync(
        int partition,
        long maxApplied,
        DeferredOffsetLedger deferredOffsets,
        ILeafProjection projection,
        CancellationToken cancellationToken)
    {
        if (maxApplied < 0)
            return false;

        var ceiling = maxApplied;
        var minDeferred = deferredOffsets.MinUnresolved(partition);
        if (minDeferred != long.MaxValue && minDeferred - 1 < ceiling)
            ceiling = minDeferred - 1;
        if (MinUnresolvedPrepareOffsetForPartition(partition) is long minPrepare && minPrepare - 1 < ceiling)
            ceiling = minPrepare - 1;

        // Record the re-read frontier BEFORE the monotonic short-circuit below.
        // On a cold rebuild the checkpoint still sits at its persisted value
        // while this activation re-reads from offset 0, so every ceiling below
        // that value is real progress the checkpoint cannot express - and the
        // `return false` below is exactly where it was being discarded (issue
        // #2280). Recording it here banks nothing on its own; it makes the
        // progress REPRESENTABLE so a mid-replay snapshot capture can bank it.
        // The checkpoint itself is untouched and stays strictly monotonic.
        if (_cacheRebuiltFromWalStartThisActivation)
            RecordColdReplayFrontier(partition, ceiling, partition + 1);

        if (ceiling <= GetCurrentCheckpointForPartition(partition))
            return false;

        using (LatticeApplyOffsetContext.BeginScope(partition, ceiling))
        {
            await projection.SetCheckpointOffsetAsync(ceiling, cancellationToken);
        }

        return true;
    }

    /// <summary>
    /// Builds the order in which pass 1 absorbs the WAL partitions, together
    /// with the head offset probed for each.
    /// <para>
    /// Only the partition absorbed LAST is drain-eligible (its cross-partition
    /// dependencies are all present by then), so which partition occupies that
    /// slot decides which one can resolve its own saga prepares during pass 1
    /// and keep banking durable progress. Ordering by backlog ascending awards
    /// the slot to the partition with the most to replay - see issue #2089.
    /// </para>
    /// <para>
    /// The head probe is the same call <see cref="ReplayPartitionAsync"/>
    /// already makes, hoisted so it can inform the ordering and then handed
    /// back down, so ordering costs no additional grain calls. A probe fault
    /// is caught PER PARTITION: the partitions that did probe still inform the
    /// order, the ones that did not keep their natural position and are handed
    /// back with a <c>null</c> head so <see cref="ReplayPartitionAsync"/>
    /// re-probes and surfaces the real fault in its own turn - which is exactly
    /// today's failure behaviour, after the partitions ahead of it have banked.
    /// The fault is logged at Warning naming the partitions, so a degraded
    /// ordering is never silent; see issues #2082 and #2089.
    /// </para>
    /// <para>
    /// A partition sitting at the "nothing applied" sentinel is likewise never
    /// awarded the drain slot: its apparent gap is the whole shard partition's
    /// WAL length rather than this leaf's own pending work, so it is not a
    /// comparable backlog. See the comment on the sentinel test below.
    /// </para>
    /// </summary>
    private async Task<List<(int Partition, long? ProbedHead)>> BuildPassOneSweepOrderAsync(
        string treeId,
        int partitionCount,
        long? checkpointOverride,
        CancellationToken cancellationToken)
    {
        var order = new List<(int Partition, long? ProbedHead)>(partitionCount);

        // A single-partition tree is drain-eligible throughout; there is
        // nothing to order and no reason to spend a probe.
        if (partitionCount <= 1)
        {
            order.Add((0, null));
            return order;
        }

        var heads = new long[partitionCount];
        var backlogs = new long[partitionCount];
        var probed = new bool[partitionCount];
        var comparable = new bool[partitionCount];
        List<int>? unprobed = null;
        Exception? firstProbeFault = null;

        for (var p = 0; p < partitionCount; p++)
        {
            try
            {
                var head = await grainFactory
                    .GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/{p}")
                    .GetHeadOffsetAsync(cancellationToken);

                var checkpoint = checkpointOverride ?? GetPersistedCheckpointForPartition(p);
                heads[p] = head;
                probed[p] = true;

                // A partition at the "nothing applied" sentinel (-1) has no
                // backlog COMPARABLE WITH one that holds a real checkpoint.
                // Its apparent gap is head + 1, which measures the whole shard
                // partition's WAL rather than this leaf's own pending work:
                // the materialiser's per-leaf range filter drops every entry
                // outside this leaf's key range on iteration, so the real cost
                // is bounded by the leaf's range, not by the head. Ranking a
                // MIXED set on that number awards the single drain-eligible
                // slot to the partition with the LEAST applied - and on legacy
                // state, where every non-zero partition reads -1, it would do
                // so systematically. LatticeFallOffLogDetector guards its own
                // budget trigger with the same `checkpointOffset >= 0` test
                // and for the same reason (the c2-vi split-sibling incident).
                //
                // This is about COMPARABILITY, not about the sentinel being
                // unrankable in itself - see the all-sentinel case below.
                comparable[p] = checkpoint >= 0;
                backlogs[p] = comparable[p] && head > checkpoint ? head - checkpoint : 0L;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                // Fail soft, per partition. Aborting the whole sweep here would
                // be strictly worse than today: nothing has been banked yet, so
                // a transient probe fault would cost every partition's progress
                // rather than only its own. Swallowing it silently would be the
                // fault-masking shape issue #2082 closed on the trimmed-prefix
                // probe. So: degrade this partition's ordering only, keep its
                // natural position, and log it.
                (unprobed ??= []).Add(p);
                firstProbeFault ??= ex;
            }
        }

        if (unprobed is not null)
        {
            context.ActivationServices?
                .GetService<ILoggerFactory>()?
                .CreateLogger<BPlusLeafGrain>()?
                .LogWarning(
                    firstProbeFault,
                    "Replay sweep-order head probe failed for tree {Tree} partition(s) {Partitions} on leaf {GrainId}; those partitions keep their natural sweep position and will be re-probed during replay. Pass-1 drain eligibility may be awarded to a smaller backlog than intended.",
                    treeId,
                    string.Join(",", unprobed),
                    context.GrainId);
        }

        // ALL-SENTINEL CASE. When no partition holds a real checkpoint, every
        // partition shares the SAME baseline, so head + 1 is a valid relative
        // measure of the work each has to read and the comparability objection
        // above does not apply - it concerns mixing two different baselines,
        // not the sentinel itself. This is the dominant case in practice: the
        // cold-start cache-empty override (step 0.5 of OnActivateAsync) drives
        // checkpointOverride to -1 for every partition, which is precisely the
        // activation with the most to replay and therefore the one this
        // ordering exists to help. Excluding sentinel partitions wholesale
        // would collapse the sweep to index order there and make issue #2089's
        // ordering inert in the only case that matters - while still passing a
        // mixed-baseline test. (Legacy state, where partition 0 reads the
        // scalar slot and the rest read -1, is MIXED, so it keeps the strict
        // guard above and the sentinel partitions never take the drain slot.)
        var anyComparable = false;
        for (var p = 0; p < partitionCount; p++)
            anyComparable |= comparable[p];

        if (!anyComparable)
        {
            for (var p = 0; p < partitionCount; p++)
            {
                if (!probed[p])
                    continue;
                comparable[p] = true;
                backlogs[p] = heads[p];
            }
        }

        var indices = new int[partitionCount];
        for (var p = 0; p < partitionCount; p++)
            indices[p] = p;

        // Ascending by backlog, partition index as the tie-break so the order
        // is deterministic and an evenly spread backlog keeps index order.
        // A partition whose backlog is not KNOWN sorts strictly first - either
        // because its head probe faulted, or because it sits at the "nothing
        // applied" sentinel and has no comparable gap. Neither is ever awarded
        // the single drain-eligible slot on the strength of a backlog we do
        // not actually know.
        Array.Sort(indices, (a, b) =>
        {
            if (comparable[a] != comparable[b])
                return comparable[a] ? 1 : -1;

            var byBacklog = backlogs[a].CompareTo(backlogs[b]);
            return byBacklog != 0 ? byBacklog : a.CompareTo(b);
        });

        foreach (var p in indices)
            order.Add((p, probed[p] ? heads[p] : null));

        return order;
    }

    /// <summary>
    /// Per-partition replay inner loop extracted from
    /// <see cref="ReplayWalSinceCheckpointAsync"/>. Reads WAL slices
    /// from <paramref name="partition"/>'s coordinator strictly past
    /// <paramref name="checkpoint"/>, threads
    /// (<paramref name="partition"/>, offset) into
    /// <see cref="LatticeApplyOffsetContext.BeginScope(int, long)"/>
    /// for every Apply, then advances the per-partition projection
    /// checkpoint via the projection seam. Returns <c>true</c> when
    /// the per-partition checkpoint actually advanced.
    /// <para>
    /// <b>Pass 1 of two-pass replay.</b> Saga terminals
    /// (<see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>)
    /// and range deletes are appended to <paramref name="deferredTerminals"/>
    /// instead of being applied inline - see the <see cref="DeferredTerminal"/>
    /// docstring for the saga atomicity rationale - unless
    /// <paramref name="drainDeferredInline"/> says every other partition has
    /// already been absorbed, in which case the mutation's cross-partition
    /// dependencies are all present and it is applied in place. Per-partition
    /// checkpoint advance beyond the safe contiguous prefix is deferred to
    /// pass 2: a partition that emitted a terminal would otherwise advance its
    /// checkpoint past the still-pending prepare offsets in the OTHER
    /// partitions' pending-tx clamp range (the per-partition clamp is scoped to
    /// the partition the prepare landed in, so the terminal's partition can
    /// advance unclamped) - but the terminal itself hasn't been applied yet, so
    /// the visible-state contract requires us to wait.
    /// </para>
    /// </summary>
    private async Task<(bool Advanced, long MaxApplied)> ReplayPartitionAsync(
        string treeId,
        int partition,
        long checkpoint,
        ILeafProjection projection,
        List<DeferredTerminal> deferredTerminals,
        DeferredOffsetLedger deferredOffsets,
        bool drainDeferredInline,
        ShardMap? replayShardMap,
        int maxRecordsPerTurn,
        int maxDurableUnresolvedWork,
        long? probedHead,
        int maxLeafReplayEntries,
        CancellationToken cancellationToken)
    {
        var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
            $"{treeId}/{partition}");

        // Reuse the head the sweep-order pre-pass already probed when it has
        // one, so ordering the sweep costs no extra grain call. A head probed
        // moments ago can only be behind the true head, which simply leaves
        // the newest entries for the materialiser or the next replay.
        var head = probedHead ?? await coordinator.GetHeadOffsetAsync(cancellationToken);
        if (head <= checkpoint)
        {
            // Nothing to replay, so this leaf applied zero entries: the
            // cleanest possible in-budget activation. It ends the run for the
            // same reason, and on the same quantity, as the completion path
            // below - zero is in budget for any positive budget.
            RetireOverBudgetLogStamp(treeId, ReplicaId, partition);
            return (false, checkpoint);
        }

        // The partition-wide extent this replay scans. It is NOT this leaf's
        // work (issue #2149) - every sibling leaf pinned to this WAL partition
        // contributes to it, ~1,350 of them on the measured deployment - but it
        // IS a sound upper bound on it, and it is the quantity the detector
        // compared against MaxLeafReplayEntries. The OVER-BUDGET warning below
        // reports it alongside the quantity actually compared so the two can
        // never again be conflated from the log alone. The STALL warning does
        // NOT print the budget at all (issue #2285): that warning is not about
        // the budget, the budget is advisory and does not bound replay, and
        // printing the two side by side manufactured exactly the comparison
        // its own trailing disclaimer forbade - which is how #2285 came to be
        // filed, by a reader who quoted the line and truncated the disclaimer.
        // A disclaimer that must survive quotation to work is not a control;
        // omitting the quantity is. The budget does still ROUTE that line
        // (issue #2291): it selects the level, which is a decision about
        // warning volume and not a claim about this leaf's work. Keeping it out
        // of the message is exactly what stops it being read as one.
        var gap = head - checkpoint;

        // STALL (FAULT) CHECK - issue #2149, fault shape of issue #2165,
        // sensitivity corrected by issue #2291.
        //
        // Run BEFORE the scan, so a replay torn down by the activation deadline
        // - which is precisely the fault being detected - still reports it.
        //
        // The criterion is the one the old warning stated in prose and left an
        // operator to evaluate by hand: the checkpoint does not advance across
        // repeats for the SAME leaf and partition. n >= 2 by construction, so
        // a single cold activation never trips it.
        //
        // NOT gated on the detector's over-budget candidate any more (issue
        // #2291). Nothing about the budget decides whether this fault is
        // detected, counted, or reported; it selects only the LOG LEVEL.
        //
        // That gate was defended on two grounds. The first was a strict-subset
        // safety property: the new line can never fire where the old cost line
        // did not. The second was that it was sound on its own terms, because
        // "a frozen checkpoint whose partition gap fits inside the budget is an
        // idle leaf, not a livelock". The second claim is false, and the code a
        // few lines above is what refutes it: an idle leaf returns on the
        // head <= checkpoint check and never arrives here, so past this point
        // there is unreplayed work by construction and a frozen checkpoint is a
        // livelock at ANY gap.
        //
        // The first claim was true but bought the wrong thing. The gap is
        // partition-wide and pre-filter, shared with ~1,350 sibling leaves,
        // while convergence is a property of THIS leaf; conjoining them made a
        // permanent stall silent wherever the partition happened to be shallow.
        // That blind spot is arithmetic rather than a matter of threshold: the
        // gap can never exceed its partition's readable WAL depth, so on a
        // partition holding at most MaxLeafReplayEntries entries the
        // conjunction is UNSATISFIABLE and no leaf pinned to it could be
        // reported however completely it was stuck. No value of
        // MaxLeafReplayEntries closes that. Measured on the deployed container,
        // the visible population sat between 2.64x and 8.18x the configured cap
        // of 10,000, with not one observation inside 164% of it, so the whole
        // under-cap region was unlit.
        //
        // THE COUNTER IS WHERE THIS BIT HARDEST. The line below tells an
        // operator in as many words that the exact census of the condition is
        // orleans.lattice.leaf.activation_stalled_replays (issue #2285). While
        // the counter sat inside the over-budget conjunction that claim was
        // false: it was an exact census of the over-cap SUBSET, undercounting by
        // an amount nothing in the system could observe, and it was documented
        // as a census in the very message an operator would use to check it.
        // Counting on the convergence predicate alone is what makes the shipped
        // claim true.
        //
        // The budget survives only as a volume governor on the WARNING stream,
        // and only because the size of the under-cap population is still
        // unknown: it has never been observable, so it cannot be estimated from
        // the visible one without reading a population off the very filter that
        // hid it. Over-cap keeps Warning, so warning volume is exactly what it
        // is today; under-cap emits at Information. When the distribution is
        // unknown, prefer the option whose worst case is bounded. The counter,
        // now un-gated, is what supplies the missing number, after which
        // promoting the under-cap arm is a one-line change decided on evidence.
        //
        // Volume stays bounded where it always actually was: ShouldLogStalledReplay
        // throttles per (tree, leaf, partition) on OverBudgetLogInterval, behind
        // a capacity cap with pruning, and it throttles BOTH levels.
        if (NoteReplayCheckpointObservation(treeId, ReplicaId, partition, checkpoint) is { IsStall: true } stall)
        {
            // Counted BEFORE the log throttle, and outside it, so the counter is
            // the exact census of the condition while the warning below stays a
            // bounded sample of it (issue #2285). Inside the throttle this would
            // have measured the throttle's rate - one per (tree, leaf, partition)
            // per minute - and a burst would have been indistinguishable from a
            // steady trickle, which is the distinction the counter exists to
            // make. The leaf is deliberately not a tag: leaf count is unbounded,
            // so per-leaf detail belongs in the warning, not in a time series.
            //
            // It is outside the BUDGET too (issue #2291), which is what makes
            // "exact census" true rather than merely intended.
            LatticeMetrics.LeafActivationStalledReplays.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition),
                LatticeTenantLabel.ForTree(treeId));

            var stalledLevel = gap > maxLeafReplayEntries
                ? LogLevel.Warning
                : LogLevel.Information;
            var stalledLogger = ResolveLogger();
            if (stalledLogger is not null
                && stalledLogger.IsEnabled(stalledLevel)
                && ShouldLogStalledReplay(treeId, ReplicaId, partition))
            {
                stalledLogger.Log(
                    stalledLevel,
                    "Leaf projection for tree '{TreeId}' leaf '{Leaf}' WAL partition {Partition} re-entered "
                    + "replay WITHOUT its persisted checkpoint having advanced (persistedCheckpoint "
                    + "{Checkpoint}, unchanged across {Repeats} consecutive replay(s) of this leaf partition "
                    + "on this silo, spanning {Span}; WAL partition head {Head}, partition gap {Gap} "
                    + "entries). The previous activation banked no durable forward progress at all for this "
                    + "partition. Judge it by the repeat count and the span, NOT by this line's existence: a "
                    + "short run of repeats over a few seconds is commonly transient, an activation torn down "
                    + "mid-replay by a cancellation or a timeout, and stops on its own; a leaf that cannot "
                    + "converge keeps reporting with a rising count over a widening span, and for as long as "
                    + "that continues writes routed to it are being lost. Note the gap is the whole "
                    + "PARTITION's extent, shared with every sibling leaf pinned to it, so it is an upper "
                    + "bound on this leaf's work and not a measurement of it. This line is throttled and is "
                    + "therefore a SAMPLE; the exact census of the condition is the counter "
                    + "orleans.lattice.leaf.activation_stalled_replays.",
                    treeId,
                    ReplicaId,
                    partition,
                    checkpoint,
                    stall.Repeats,
                    stall.Span,
                    head,
                    gap);
            }
        }

        var fromExclusive = checkpoint;

        // NAMING (issue #2270): this tracks the highest offset SCANNED, not
        // applied. It is bumped below for every entry the loop reads,
        // including entries ShouldApplyDuringReplay rejects as another
        // leaf's work. The name is a known misnomer, retained here only to
        // keep this change off the merge path of concurrent work in this
        // file; the semantics it feeds (ProjectionCheckpointOffset) are
        // documented on BPlusLeafGrain.ProjectionAdmin.cs and pinned by
        // BPlusLeafGrainTests.CheckpointScanSemantics. Do NOT "correct" the
        // behaviour to match the name: advancing only over applied entries
        // would strand a leaf that owns nothing in this partition at its old
        // checkpoint forever and pin the WAL GC retention floor
        // (LatticeWalGc.ComputeMaterialiserOffsetFloorAsync) for the whole
        // tree.
        long maxApplied = checkpoint;

        // Exact, POST-filter count of the entries THIS leaf takes through the
        // projection rebuild seam - the unit MaxLeafReplayEntries is actually
        // documented in (issue #2149). Deferred terminals count: this same leaf
        // applies them in pass 2, so they are its work too.
        //
        // Counting here costs one increment per applied entry inside a scan
        // that is happening anyway. Establishing the same figure in the
        // detector, as a pre-check, would instead mean reading (checkpoint,
        // head] before the replay and then reading it again to perform the
        // replay - doubling the most expensive part of activation, and doubling
        // exactly the read whose ~30 s overrun IS the livelock of issue #2165.
        // That is why the verdict lives here and not there.
        long appliedEntries = 0;
        var overBudgetWarned = false;

        // Cooperative-yield budget (issue #1030): a long-tailed WAL would let
        // this replay monopolise its activation turn and block the silo
        // scheduler from interleaving other ready work (foreground reads,
        // health probes). Counting processed records and yielding every
        // maxRecordsPerTurn keeps a large replay cooperative. A non-positive
        // budget disables the yield (replay runs to completion uninterrupted).
        var recordsSinceYield = 0;

        // Resumable replay (issue #1513): historically the persisted
        // checkpoint was only advanced by the post-pass-2 reconciliation in
        // ReplayWalSinceCheckpointAsync, so a replay that could not finish
        // within its activation window (a large un-snapshotted WAL prefix
        // relative to the ~30 s RuntimeRequested budget) made no durable
        // progress: the deactivation discarded every applied entry and the
        // next activation restarted from the same offset, so the leaf never
        // converged and the coverage-gated WAL GC could never trim the
        // prefix. Flush the checkpoint incrementally over the strictly
        // contiguous, fully-applied prefix instead, at each slice boundary.
        // A deactivation then loses at most one flush interval, and the
        // checkpoint persist drives the existing periodic snapshot capture
        // (MaybeRunPeriodicSnapshotRecheckAsync) so the next activation can
        // rehydrate from a snapshot and resume from the last durable offset
        // rather than replaying from zero.
        //
        // The advance is bounded so it can NEVER pass an offset that is not
        // yet durably applied (the data-loss class #1492 guards), via two
        // clamps that together hold the checkpoint at the highest offset
        // below which every entry - inline, deferred, and cross-partition
        // saga - is applied:
        //   (a) below the lowest still-UNRESOLVED deferred terminal /
        //       DeleteRange offset in this partition, since those mutations
        //       are applied only when they drain (see the DeferredTerminal
        //       docstring); and
        //   (b) below any unresolved saga prepare in this partition
        //       (MinUnresolvedPrepareOffsetForPartition), because the
        //       matching terminal - even one routed to this same partition -
        //       may itself be deferred, so _pendingTx must be reconstructed
        //       by a resumed replay that re-reads the prepare.
        // TryFlushRecoveredCeilingAsync owns both clamps.
        //
        // Clamp (a) reads a RESOLVABLE ledger rather than a monotonically
        // non-increasing scalar (issue #1831). The old scalar was only ever
        // lowered, so the first deferred mutation a partition emitted pinned
        // its ceiling for the whole remainder of the replay and left every
        // further advance to the post-pass-2 reconciliation - which only runs
        // when the entire replay fits inside the activation window. A backlog
        // large enough to outrun that window therefore banked nothing, the
        // activation was torn down, and the next one replayed the identical
        // range: the #1513 livelock, reopened for any tree that uses range
        // deletes or atomic multi-key writes. With the ledger the ceiling
        // recovers the moment a deferred offset drains, here or in pass 2.

        while (fromExclusive < head)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var slice = await coordinator.ReadSliceAsync(
                fromExclusive,
                head,
                ReplaySliceBudget,
                cancellationToken);

            if (slice.Count == 0)
                break;

            foreach (var entry in slice)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (ShouldApplyDuringReplay(
                    entry.Mutation,
                    state.State.ShardIndex,
                    state.State.LowKeyInclusive,
                    state.State.HighKeyExclusive,
                    replayShardMap))
                {
                    // This entry is this leaf's own work: it either goes
                    // through ILeafProjection.Apply now, or is deferred to
                    // pass 2 where THIS leaf applies it. Either way it counts
                    // against MaxLeafReplayEntries, which is defined in
                    // exactly those terms.
                    appliedEntries++;

                    // OVER-BUDGET VERDICT (issue #2149). Emitted at the moment
                    // the exact per-leaf count first crosses the budget, and
                    // mid-scan rather than after the replay, so a teardown
                    // later in this activation cannot swallow it.
                    //
                    // This replaces the pre-check warning that compared the
                    // partition-wide gap against this per-leaf budget. At the
                    // measured fan-out of ~1,350 leaves per partition that
                    // comparison fired 19,639 times in 6.26 hours for leaves
                    // whose real work was order 10^2 against a 10^4 budget -
                    // one to two orders of magnitude BELOW budget. The count
                    // below cannot make that error: it is the same quantity
                    // the option documents.
                    //
                    // It also subsumes the detector's -1 sentinel exemption
                    // without needing one. A brand-new leaf is exempted there
                    // because head - (-1) charges it for every sibling's WAL
                    // contribution; here it is charged only for entries in its
                    // own range, so if it really does apply more than the
                    // budget, that is a true positive worth reporting.
                    if (!overBudgetWarned && maxLeafReplayEntries > 0 && appliedEntries > maxLeafReplayEntries)
                    {
                        overBudgetWarned = true;

                        // Tagged with the WAL partition as well as the tree
                        // (issue #2023). Partition is bounded by
                        // LatticeOptions.WalPartitions, so it is safe
                        // cardinality, and without it the counter cannot be
                        // split by the same axis the warning reports - leaving
                        // an operator who sees a rate spike with no way to tell
                        // whether one partition is hot or the whole tree is.
                        // The leaf identity is deliberately NOT a tag: leaf
                        // count is unbounded, so per-leaf detail belongs in the
                        // log line below, not in a time series.
                        LatticeMetrics.LeafActivationOverBudgetReplays.Add(
                            1,
                            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                            new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition),
                            LatticeTenantLabel.ForTree(treeId));

                        // Gate on IsEnabled: the templated call would otherwise
                        // allocate a params object[] and box every argument on
                        // every over-budget activation even when warnings are
                        // filtered out.
                        //
                        // Also THROTTLE per (tree, leaf, partition). A cold
                        // start on a large volume re-activates these leaves
                        // continuously, and one warning per attempt buried a
                        // real deployment in 5,752 identical lines in fifteen
                        // minutes - enough to make the log useless for spotting
                        // the faults mixed in among them. The counter above
                        // already records every occurrence, so the log's job is
                        // only to say the condition is happening and let an
                        // operator find the checkpoint; the rate is a metric
                        // concern, not a logging one.
                        //
                        // The leaf id is load-bearing in both the key and the
                        // message (issue #2023). `partition` is the WAL
                        // partition ordinal, iterated [0, WalPartitions) inside
                        // EVERY leaf's activation - it does not identify a leaf.
                        // Keyed on (tree, partition) alone, the first leaf to
                        // trip the budget suppressed the warning for every other
                        // leaf in that tree and partition for a full minute, so
                        // consecutive lines were one-per-minute samples from
                        // arbitrary DIFFERENT leaves. Their checkpoints are not
                        // comparable, which made the "checkpoint that does not
                        // advance" criterion unevaluable and produced a false
                        // livelock report. That criterion is now evaluated in
                        // process by the stall check above; the qualification
                        // stays because it is also what makes successive lines
                        // for one leaf comparable to a human reader.
                        var overBudgetLogger = ResolveLogger();
                        if (overBudgetLogger is not null
                            && overBudgetLogger.IsEnabled(LogLevel.Warning))
                        {
                            // Two-tier per-leaf backoff PLUS the per-tree
                            // aggregate cap (issue #2100). The single-tier
                            // ShouldLogOverBudgetReplay entry point is
                            // deliberately NOT used here: it consults no
                            // aggregate cap, so routing the relocated
                            // warning through it would keep the per-leaf
                            // backoff while silently dropping the per-tree
                            // bound and the withheld-summary line that
                            // reports what the bound held back. Moving the
                            // warning to per-leaf units (issue #2149) must
                            // not cost the volume bound that made it
                            // affordable.
                            var logDecision = ClassifyOverBudgetReplayLog(treeId, ReplicaId, partition);

                            if (logDecision.SuppressedInClosedWindow > 0)
                            {
                                overBudgetLogger.LogWarning(
                                    "{Suppressed} further leaf partition replay(s) beyond the configured budget "
                                    + "on tree '{TreeId}' were not logged individually in the last window, to bound "
                                    + "this warning's share of the log. Every occurrence is still counted by the "
                                    + "orleans.lattice.leaf.activation_replays_over_budget metric, which is the "
                                    + "exact census; the lines above are a bounded sample of it. A leaf partition "
                                    + "reporting for the FIRST time is never withheld by this cap.",
                                    logDecision.SuppressedInClosedWindow,
                                    treeId);
                            }

                            if (logDecision.LogDetail)
                            {
                                overBudgetLogger.LogWarning(
                                    "Leaf projection for tree '{TreeId}' leaf '{Leaf}' WAL partition {Partition} is "
                                    + "replaying beyond the configured budget: it has taken {AppliedEntries} entries "
                                    + "through its projection rebuild seam, past MaxLeafReplayEntries {Budget} "
                                    + "(persistedCheckpoint {Checkpoint}, WAL partition head {Head}, partition gap "
                                    + "{Gap} entries). AppliedEntries is this leaf's OWN post-range-filter work and "
                                    + "is the quantity compared against the budget; Gap is the whole partition's "
                                    + "extent, shared with every sibling leaf pinned to it, and is only an upper "
                                    + "bound on it. The write-ahead log still covers the whole needed window, and "
                                    + "the replay flushes its checkpoint incrementally, so an activation torn down "
                                    + "early USUALLY banks durable forward progress. That is not unconditional: an "
                                    + "unresolved saga prepare clamps the incremental flush ceiling strictly below "
                                    + "its own offset, and while one is resident the activation banks nothing at "
                                    + "all (issue #2183). Activation may take longer than usual. A leaf whose "
                                    + "checkpoint does not advance at all is "
                                    + "reported separately as a fault, not by this line.",
                                    treeId,
                                    ReplicaId,
                                    partition,
                                    appliedEntries,
                                    maxLeafReplayEntries,
                                    checkpoint,
                                    head,
                                    gap);
                            }
                        }
                    }

                    // Defer saga terminals AND DeleteRange to pass 2:
                    // - Terminals: see the DeferredTerminal docstring
                    //   for the multi-partition saga atomicity rationale.
                    // - DeleteRange: ApplyDeleteRange iterates the leaf's
                    //   Cache at the moment of apply to tombstone every
                    //   in-range key, but under multi-partition pass 1
                    //   the Cache is still being rebuilt across partitions.
                    //   A DeleteRange that lands in partition 2 but whose
                    //   target Set entries land in partition 5 would
                    //   tombstone nothing in pass 1 (Cache empty for that
                    //   key range) and then the Sets in partition 5 would
                    //   replay AFTER the tombstone, leaving the keys
                    //   visible. Deferring DeleteRange to pass 2 (after
                    //   every Set has populated the Cache) restores the
                    //   tombstone-after-its-targets ordering invariant.
                    //
                    // Both rationales are about entries in OTHER partitions
                    // that pass 1 has not absorbed yet, so both evaporate once
                    // every other partition is absorbed - which is exactly what
                    // drainDeferredInline reports. Applying in place then costs
                    // nothing in safety and keeps the flush ceiling moving for
                    // the rest of the scan (issue #1831).
                    if (entry.Mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort
                        or MutationKind.DeleteRange
                        && !drainDeferredInline)
                    {
                        deferredTerminals.Add(new DeferredTerminal(partition, entry.Offset, entry.Mutation));

                        // Issue #2165. Recording the deferred mutation durably
                        // is what lets the ceiling advance past it: a resumed
                        // activation reconstructs it from state instead of
                        // re-reading it, so this partition banks progress even
                        // though it is not the one pass 1 absorbs last. Only
                        // when the ledger is full does the offset go back on
                        // the in-memory clamp, which is the pre-#2165
                        // behaviour.
                        if (!TryRecordUnresolvedReplayWork(
                                partition, entry.Offset, entry.Mutation, maxDurableUnresolvedWork))
                        {
                            deferredOffsets.Add(partition, entry.Offset);
                        }
                    }
                    else
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG replay-apply] gid={context.GrainId} partition={partition} offset={entry.Offset} kind={entry.Mutation.Kind} key='{entry.Mutation.Key}' shardIndex={entry.Mutation.ShardIndex}");
#endif
                        using (LatticeApplyOffsetContext.BeginScope(partition, entry.Offset))
                        {
                            projection.Apply(entry.Mutation);
                        }

                        // An applied-but-unresolved saga prepare pins the
                        // ceiling for the same reason a deferred terminal does:
                        // it lives only in the activation-scoped _pendingTx
                        // bucket, which no snapshot captures. Recording it
                        // durably lifts that clamp too - see
                        // MinUnresolvedPrepareOffsetForPartition, which skips
                        // every recorded offset.
                        //
                        // Issue #2183. The prepare and the deferred terminal
                        // are NOT symmetric at the cap, though. A deferred
                        // terminal that cannot be recorded (ledger full) is
                        // safe to drop back onto the in-memory clamp: pass 2
                        // still drains it, so the clamp is released within the
                        // activation and the partition banks progress on the
                        // next one - the "slow but safe" degradation the cap
                        // was designed for. An unresolved prepare has no
                        // terminal to drain: nothing releases its clamp until
                        // its saga terminates, which for a genuinely orphaned
                        // prepare (registry InFlight forever) is never. So a
                        // prepare that is dropped at the cap pins the ceiling
                        // at (prepare - 1) PERMANENTLY, and the leaf banks zero
                        // forward progress across every future activation. This
                        // is the latent defect issue #2183 fixes, proven by the
                        // two-arm control in BPlusLeafGrainTests.ReplayFlushCeiling
                        // (a dropped prepare never advances the checkpoint; a
                        // recorded one does). It is NOT the freeze observed on
                        // the deployed repocontext leaf: that leaf's durable row
                        // was measured near-empty, so its cap was never hit, and
                        // its freeze is an activation aborted mid-replay by a
                        // digest-publish timeout (issue #2220) - a different
                        // mechanism on a disjoint path.
                        //
                        // A resident prepare must therefore be recorded
                        // unconditionally whenever the ledger is enabled: it is
                        // never safe to drop, because dropping it also risks
                        // losing an aged-out commit whose terminal truncated on
                        // another partition (the hazard #2190 preserves InFlight
                        // prepares for). The cap continues to bound the deferred
                        // terminals that CAN be safely re-read. When the ledger
                        // is disabled (cap <= 0) the in-memory clamp is the only
                        // protection and the prepare pins the ceiling exactly as
                        // before, which the no-ledger degradation tests assert.
                        if (entry.Mutation.IsPrepared)
                        {
                            if (RecordUnresolvedPreparesBeyondCap && maxDurableUnresolvedWork > 0)
                            {
                                EnsureUnresolvedPrepareRecorded(
                                    partition, entry.Offset, entry.Mutation, maxDurableUnresolvedWork);
                            }
                            else
                            {
                                TryRecordUnresolvedReplayWork(
                                    partition, entry.Offset, entry.Mutation, maxDurableUnresolvedWork);
                            }
                        }
                    }
                }
#if LATTICE_DIAG
                else
                {
                    DiagSink.Write($"[DIAG replay-skip] gid={context.GrainId} partition={partition} offset={entry.Offset} kind={entry.Mutation.Kind} key='{entry.Mutation.Key}' mutShard={entry.Mutation.ShardIndex} leafShard={state.State.ShardIndex} low='{state.State.LowKeyInclusive ?? "<null>"}' high='{state.State.HighKeyExclusive ?? "<null>"}'");
                }
#endif

                // SCANNED-through advance (issue #2270). Deliberately OUTSIDE
                // the ShouldApplyDuringReplay block above: an entry this leaf
                // skipped as another leaf's work still moves the checkpoint,
                // because the checkpoint records how far this leaf has READ
                // the partition, not how much of it was its own. Moving this
                // inside the filter is a severe regression, not a tightening -
                // see the declaration of maxApplied above.
                if (entry.Offset > maxApplied)
                    maxApplied = entry.Offset;

                if (maxRecordsPerTurn > 0 && ++recordsSinceYield >= maxRecordsPerTurn)
                {
                    recordsSinceYield = 0;
                    await Task.Yield();
                }
            }

            var lastOffset = slice[^1].Offset;
            if (lastOffset <= fromExclusive)
                break;
            fromExclusive = lastOffset;

            // Incremental checkpoint flush over the safe contiguous prefix
            // (issue #1513), clamped by TryFlushRecoveredCeilingAsync so it can
            // never license a checkpoint (or the materialiser pin) past a
            // not-yet-durably-applied offset.
            await TryFlushRecoveredCeilingAsync(
                partition,
                maxApplied,
                deferredOffsets,
                projection,
                cancellationToken);
        }

        // Pass 1 flushes the checkpoint incrementally over the strictly
        // contiguous, fully-applied prefix (the TryFlushRecoveredCeilingAsync
        // clamps above), so partial replay progress is durable across a
        // mid-replay teardown (issue #1513). What it deliberately does NOT do
        // here is advance the checkpoint to the FULL maxApplied: a partition
        // may still hold an undrained deferred terminal or an unresolved
        // prepare below maxApplied, so the remaining advance up to maxApplied
        // is left to the post-pass-2 reconciliation step in
        // ReplayWalSinceCheckpointAsync, which waits until every deferred
        // terminal has applied (lifting the pending-tx clamps) before advancing
        // each partition's persisted checkpoint to its maxApplied. The
        // incremental flush is bounded strictly below those pending offsets by
        // construction, so it never over-advances: (a) a partition that
        // observed a prepare in pass 1 is held behind the prepare's offset, and
        // (b) a partition that deferred a terminal is held behind that
        // terminal's offset until it drains. Returning the per-partition
        // maxApplied here gives the caller the data it needs to do the
        // post-pass-2 advance with full knowledge of every partition's outcome.
        // The retire arm, on the same quantity as the commit arm above and as
        // its exact complement. A leaf whose own applied-entry count came in
        // within budget has ended its run of the condition, so its throttle
        // stamp is retired and a later regression reports at the base interval
        // rather than inheriting accumulated backoff (#2100). This is
        // deliberately NOT gated on the detector's verdict: that is the
        // partition-wide gap, and retiring on it leaves the window
        // gap > budget && applied <= budget permanently un-retired.
        if (!overBudgetWarned && maxLeafReplayEntries > 0 && appliedEntries <= maxLeafReplayEntries)
            RetireOverBudgetLogStamp(treeId, ReplicaId, partition);

        // Accumulate this partition's exact post-filter work into the
        // per-activation total reported on the deactivation log line (#2280).
        // Accumulated even when the replay is later cancelled, because the
        // whole point is to distinguish "banked nothing having done nothing"
        // from "banked nothing having applied a great many entries below the
        // existing checkpoint mark".
        _replayEntriesAppliedThisActivation += appliedEntries;

        return (Advanced: maxApplied > checkpoint, MaxApplied: maxApplied);
    }

    /// <summary>
    /// Best-effort resolution of the leaf's current slot-ownership map for the
    /// activation-time replay filter. Returns the routing map published for
    /// <paramref name="treeId"/> when (a) this leaf carries a non-null
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ShardIndex"/> and (b) that shard index is
    /// actually referenced by the map's physical shard set. In every other
    /// case - a system tree, a legacy slot-less leaf, a registry lookup that
    /// returns no map or throws, or a map drawn from a foreign physical shard
    /// space - this returns <see langword="null"/> so
    /// <see cref="ShouldApplyDuringReplay"/> falls back to the legacy
    /// stamped-<see cref="LatticeMutation.ShardIndex"/> axis. The guard is what
    /// makes the map-based ownership resolution safe to enable unconditionally:
    /// a transient registry failure or a mismatched map can never cause a leaf
    /// to reject its own writes.
    /// <para>
    /// System trees (IDs starting with
    /// <see cref="LatticeConstants.SystemTreePrefix"/>) skip the registry lookup
    /// entirely. The registry is itself backed by the
    /// <see cref="LatticeConstants.RegistryTreeId"/> system tree, so a registry
    /// leaf that called back into the (non-reentrant, singleton) registry grain
    /// during its own activation - which happens inside the registry's own
    /// write turn - would deadlock. System trees never undergo the adaptive
    /// shard split that this slot-ownership resolution guards against, so the
    /// legacy stamp axis is always correct for them. This mirrors the
    /// system-tree guard every other leaf-to-registry call site uses.
    /// </para>
    /// </summary>
    private async Task<ShardMap?> ResolveReplayShardMapAsync(string treeId)
    {
        if (state.State.ShardIndex is not int leafShardIndex)
            return null;

        // The registry is backed by a system tree; a system-tree leaf must
        // never call the registry during activation or it deadlocks the
        // singleton registry grain inside its own write turn.
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            return null;

        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var map = await registry.GetShardMapAsync(treeId);
            if (map is not null && map.GetPhysicalShardIndices().Contains(leafShardIndex))
                return map;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Best-effort: a registry hiccup must never block leaf recovery.
            // Fall back to the stamp-based filter (pre-#909 behaviour).
        }

        return null;
    }

    /// <summary>
    /// Per-WAL-entry filter for the activation-time materialiser.
    /// Decides whether a given WAL entry should be replayed against
    /// this leaf's projection, keyed on the leaf's slot ownership and on
    /// the leaf's persisted [<see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.LowKeyInclusive"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.HighKeyExclusive"/>) ownership
    /// range.
    /// <para>
    ///     <see cref="MutationKind.Set"/> /
    ///     <see cref="MutationKind.Delete"/> are applied iff the entry's
    ///     <see cref="LatticeMutation.Key"/> is owned by this leaf's
    ///     shard <em>and</em> the key falls in the leaf's persisted
    ///     ownership range. Shard ownership is resolved positively by
    ///     the key's virtual slot under
    ///     <paramref name="currentShardMap"/> when one is available
    ///     (<c>currentShardMap.Resolve(key) == leafShardIndex</c>);
    ///     otherwise it falls back to the stamped
    ///     <see cref="LatticeMutation.ShardIndex"/>. Resolving by slot
    ///     rather than by the stamp is what keeps a shadow-forwarded
    ///     record (a post-split write routed to the donor for an
    ///     already-moved slot, forwarded into the target's WAL with the
    ///     donor's stamp) applied on the target leaf that now owns the
    ///     slot, while still dropping genuine sibling-shard data that a
    ///     shared WAL partition multiplexes through (its slot resolves
    ///     to another shard) and donor orphans on the donor leaf (their
    ///     slot has moved away). The range check is open on either side
    ///     - a <see langword="null"/> bound means "no constraint on
    ///     that side", used for the chain's leftmost and rightmost
    ///     leaves and for legacy state shapes that pre-date the slot.
    ///     Keying on key-range (not on authoring leaf grain id) is
    ///     essential for the rebuild-from-WAL scenario: a leaf born from
    ///     a split has no Entries until replay populates them, and the
    ///     entries that belong to it were authored by the donor sibling
    ///     pre-split. Pre-Option A leaves whose
    ///     <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ShardIndex"/> slot is null
    ///     apply unconditionally on the shard axis; leaves with both
    ///     range bounds null apply unconditionally on the range axis -
    ///     both axes preserve the legacy V1 single-leaf-per-shard
    ///     semantics so a legacy-shaped state must not start dropping
    ///     its own writes after a binary upgrade.
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.Tombstone"/> reap envelopes
    ///     authored by <c>CompactTombstonesAsync</c> are gated by the
    ///     same shard-and-range filter as <see cref="MutationKind.Set"/>
    ///     / <see cref="MutationKind.Delete"/>: a sibling leaf's reap
    ///     must not unintentionally remove keys from this leaf's
    ///     projection. Reap envelopes that pass the filter route into
    ///     <c>ApplyTombstoneReap</c> which physically removes the
    ///     stamped key iff the existing entry is still a tombstone or
    ///     an expired live entry.
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.DeleteRange"/> is applied
    ///     unconditionally. <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>'s replay
    ///     handler iterates this leaf's own entries only, so the call
    ///     is naturally a no-op on leaves that own no keys in the
    ///     range.
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.TxCommit"/> /
    ///     <see cref="MutationKind.TxAbort"/> are applied
    ///     unconditionally. The terminal's shard scope is enforced by
    ///     the writer-side partition routing, and the per-leaf
    ///     <c>_recentlyTerminal</c> dedup makes a terminal whose
    ///     pending bucket is empty a trivial no-op.
    /// </para>
    /// <para>
    ///     Unknown <see cref="MutationKind"/> values are dropped -
    ///     defensive forward-compat against future kinds whose replay
    ///     semantics the materialiser has not been taught.
    /// </para>
    /// </summary>
    internal static bool ShouldApplyDuringReplay(
        in LatticeMutation mutation,
        int? leafShardIndex,
        string? lowKeyInclusive,
        string? highKeyExclusive,
        ShardMap? currentShardMap) => mutation.Kind switch
    {
        MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone =>
            IsShardOwnedDuringReplay(mutation, leafShardIndex, currentShardMap)
            && SplitBoundary.Owns(mutation.Key, lowKeyInclusive, highKeyExclusive),
        MutationKind.DeleteRange => true,
        MutationKind.TxCommit => true,
        MutationKind.TxAbort => true,
        _ => false,
    };

    /// <summary>
    /// Shard-axis half of <see cref="ShouldApplyDuringReplay"/>. A
    /// slot-less (legacy) leaf owns every shard-axis entry. Otherwise the
    /// entry is owned iff the current routing map resolves its key's slot to
    /// this leaf's shard; when no map is available the legacy stamped
    /// <see cref="LatticeMutation.ShardIndex"/> is used instead.
    /// </summary>
    private static bool IsShardOwnedDuringReplay(
        in LatticeMutation mutation,
        int? leafShardIndex,
        ShardMap? currentShardMap)
    {
        if (leafShardIndex is not int shard)
            return true;

        if (currentShardMap is not null)
            return currentShardMap.Resolve(mutation.Key) == shard;

        return mutation.ShardIndex == shard;
    }
}
