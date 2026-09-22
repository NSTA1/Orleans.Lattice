using System.Diagnostics;
using System.Globalization;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Lattice;
using static VehicleFleetSimulator.AzureThroughput.Engine.BenchExceptionHelpers;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// The benchmark's ingest, flush and measurement engine: drains a channel
/// of key/value pairs, dispatches each batch through the configured
/// <see cref="BenchWorkloadMode"/>, and emits the per-second progress
/// lines and the FINAL line that the harness parses.
/// </summary>
/// <remarks>
/// <para>
/// This is deliberately host-agnostic. It is driven entirely through
/// <see cref="IGrainFactory"/> and <c>ILattice</c>, and it consumes a
/// <c>ChannelReader</c> rather than owning the thing that fills it. Those
/// two facts are what let the identical code run in both benchmark
/// topologies:
/// </para>
/// <list type="bullet">
/// <item><description><b>Single-silo (Layer 2).</b> The silo's TCP listener
/// accepts a producer connection, parses newline-delimited records and
/// writes them into the channel; the engine runs in-process against the
/// silo's own grain factory.</description></item>
/// <item><description><b>Multi-silo (Layer 3).</b> A separate producer
/// process joins the cluster as an Orleans client, generates records
/// straight into the channel, and runs this same engine against
/// <c>IClusterClient</c> - which implements <see cref="IGrainFactory"/>, so
/// no branching is required.</description></item>
/// </list>
/// <para>
/// Keeping one engine for both is what makes the two layers comparable: the
/// measured path is the same assembly, so a Layer 2 vs Layer 3 difference is
/// attributable to topology rather than to a reimplementation.
/// </para>
/// <para>
/// The one behavioural seam is <see cref="IBenchSaturationGate"/>. WAL
/// saturation is a silo-scoped, in-process observation, so a client host
/// installs <see cref="NoOpBenchSaturationGate"/> and forgoes the two
/// shutdown-path accounting adjustments (FX-029 and FX-038). Both are
/// recency-guarded, so on an unsaturated run the two hosts behave
/// identically; see that interface for the full rationale.
/// </para>
/// </remarks>
/// <param name="grainFactory">Grain factory or cluster client the engine
/// dispatches through.</param>
/// <param name="settings">Resolved bench settings (batch size, flush
/// cadence, concurrency, workload mode, ...).</param>
/// <param name="lifetime">Host lifetime, used to observe application
/// stopping on the drain path.</param>
/// <param name="saturationGate">WAL-saturation observations, or a no-op on
/// a host that cannot make them.</param>
/// <param name="logger">Diagnostic log sink.</param>
internal sealed class BenchIngestEngine(
    IGrainFactory grainFactory,
    IngestSettings settings,
    IHostApplicationLifetime lifetime,
    IBenchSaturationGate saturationGate,
    ILogger logger)
{
    public async Task DrainAsync(ILattice lattice, ChannelReader<KeyValuePair<string, byte[]>> reader, CancellationToken ct)
        => await DrainAsync(new[] { lattice }, reader, ct).ConfigureAwait(false);

    /// <summary>
    /// Drain the channel, spreading each flush across <paramref name="lattices"/>
    /// in round-robin order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The silo host passes a single handle and is therefore behaviourally
    /// identical to the original single-handle shape. The multi-handle form
    /// exists for the Orleans-client producer, where one handle is not enough
    /// to reach more than one silo.
    /// </para>
    /// <para>
    /// Orleans' <c>ClientMessageCenter</c> selects a gateway by bucketing on
    /// <c>TargetGrain.GetUniformHashCode()</c>, so that calls to one grain keep
    /// their order. A benchmark that drives a single tree therefore has a
    /// single grain id, lands in a single bucket, and pins every call to one
    /// gateway. <c>LatticeGrain</c> is <c>[StatelessWorker]</c>, so it then
    /// activates on that gateway's silo and every client-facing call is served
    /// by one host no matter how many are in the cluster. The shard, leaf and
    /// WAL grains still spread, so throughput does not collapse - but the
    /// front door becomes a fixed-size funnel, and a scaling sweep would
    /// measure the funnel and report its saturation as the cluster's knee.
    /// </para>
    /// <para>
    /// Each <c>IClusterClient</c> has its own bucket array, populated from
    /// <c>GatewayManager.GetLiveGateway()</c>, whose round-robin cursor starts
    /// at a random offset per client. Independent clients therefore settle on
    /// different gateways, and rotating flushes across them spreads the front
    /// door over the cluster. This is the only reason the producer builds more
    /// than one client.
    /// </para>
    /// </remarks>
    public async Task DrainAsync(IReadOnlyList<ILattice> lattices, ChannelReader<KeyValuePair<string, byte[]>> reader, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lattices);
        if (lattices.Count == 0)
        {
            throw new ArgumentException("At least one ILattice handle is required.", nameof(lattices));
        }

        var lattice = lattices[0];
        var latticeCursor = -1;
        ILattice NextLattice() => lattices.Count == 1
            ? lattice
            : lattices[(int)((uint)Interlocked.Increment(ref latticeCursor) % (uint)lattices.Count)];

        // Concurrent flush model: the drain loop fills a working batch
        // and, when the batch is full or the flush deadline elapses,
        // hands the batch off to a background flush task and starts a
        // fresh batch immediately. A SemaphoreSlim caps the number of
        // in-flight SetManyAsync calls at `FlushConcurrency` so we
        // don't unboundedly queue against the silo / WAL. The previous
        // single-flusher shape serialised every batch behind one
        // outstanding SetManyAsync, which capped throughput at
        // (1 / per-call-latency) regardless of how cheap the per-key
        // work was. Lifting the in-flight cap exposes the WAL's
        // phase-2 worker to coalesce opportunities (up to 49 batches
        // per phase-2 transaction) and lets the leaf's batched
        // commit-log seam actually run in parallel against
        // independent shards / partitions.
        var startedAt = Stopwatch.GetTimestamp();
        // Timestamp of the first SetManyAsync dispatch - set inside the
        // dispatch loop the first time a batch is actually accepted from
        // the channel. The FINAL line uses this (when present) to compute
        // an "active" average that excludes the idle window before any
        // producer connected; `startedAt` is retained so the elapsed
        // field continues to mean "wall-clock since the worker started"
        // for any external parser that already depended on it.
        long firstAcceptedAt = 0;
        long writtenTotal = 0;
        long writtenSinceReport = 0;
        long failedTotal = 0;
        long failedSinceReport = 0;
        long inFlight = 0;
        // FX-029: count entries that were discarded from the channel
        // because the producer-stop boundary coincided with a Saturated
        // signal regime. These are neither `written` nor `failed` -
        // they were never dispatched to ILattice in the first place;
        // the bench deliberately abandons them rather than feeding them
        // into the in-flight queue against a residually back-pressured
        // storage account where they would trip
        // `WalAppendDispatchTimeout` 30 s later and surface as failed=N
        // on FINAL. The trade-off is documented in the FX-029 issue
        // body: a benchmark whose entire point is measuring steady-
        // state throughput correctly drops the post-producer backlog
        // when the silo is saturated.
        long discardedTotal = 0;

        // set-point-mv only: observe the asynchronous materialised view's apply
        // lag (source WAL entries committed but not yet applied to the view) on
        // each progress line and at FINAL. The maintainer already records this
        // onto the public orleans.lattice meter once per drain pass, so the
        // reporter reads the last published sample via a passive MeterListener
        // rather than calling ILatticeView.GetLagAsync on its cadence. That
        // matters: a polling grain RPC would add traffic to the very tree the
        // cohort is measuring (and, on a saturated silo, time out and pollute
        // the cohort's exception tally), defeating the A/B comparison against
        // the plain set-point cohort. A bounded non-zero lag while writes flow,
        // draining to zero after they stop, is the operator-visible evidence
        // that the view is maintained asynchronously without taxing the primary
        // tree. The view itself is attached at startup via AddLatticeViews; this
        // probe is read-only observability over its metrics.
        ViewLagMeterProbe? mvLagProbe = null;
        if (settings.WorkloadMode == BenchWorkloadMode.SetPointMv)
        {
            mvLagProbe = new ViewLagMeterProbe("bench");
            Console.WriteLine($"[silo] mv: materialised view 'bench' (view-bench) attached to treeId={settings.TreeId}; apply lag read from orleans.lattice.view.apply_lag and reported as mvLag=N on each progress line and at MV-FINAL.");
        }

        using var flushGate = new SemaphoreSlim(settings.FlushConcurrency, settings.FlushConcurrency);
        var flushTasks = new HashSet<Task>();
        var firstDispatchLogged = 0;

        // Local helper: awaits a flush slot, then schedules the
        // SetManyAsync call on the threadpool and returns. Returning
        // the Task lets the caller decide whether to await commit
        // ordering or just track it for the FINAL drain. Crucially,
        // we `await flushGate.WaitAsync` BEFORE scheduling the work
        // and BEFORE returning, so the drain loop naturally stalls
        // when `FlushConcurrency` flushes are already in flight. The
        // semaphore is released inside the running task once
        // SetManyAsync completes (or faults), so the next caller's
        // `WaitAsync` unblocks at the right moment.
        async Task<Task> DispatchFlushAsync(List<KeyValuePair<string, byte[]>> batchToFlush)
        {
            var gateWaitStart = Stopwatch.GetTimestamp();
            await flushGate.WaitAsync(ct).ConfigureAwait(false);
            var gateWaitMs = Stopwatch.GetElapsedTime(gateWaitStart).TotalMilliseconds;
            var treeTag = new KeyValuePair<string, object?>("tree", settings.TreeId);
            BenchMetrics.DrainFlushDispatchWaitMs.Record(gateWaitMs, treeTag);
            BenchMetrics.DrainFlushDispatchSize.Record(batchToFlush.Count, treeTag);
            Interlocked.Increment(ref inFlight);
            if (Interlocked.Exchange(ref firstDispatchLogged, 1) == 0)
            {
                Interlocked.Exchange(ref firstAcceptedAt, Stopwatch.GetTimestamp());
                // One-shot: prove what the very first SetManyAsync call
                // actually got. Configured batch size is the cap; the
                // first batch may be smaller if a flush deadline hit
                // before the batch filled, so we log both.
                Console.WriteLine($"[silo:ingest] first dispatch entries={batchToFlush.Count} (configured BatchSize={settings.BatchSize})");
            }
            return Task.Run(async () =>
            {
                try
                {
                    var committed = await FlushAsync(NextLattice(), batchToFlush, ct).ConfigureAwait(false);
                    if (committed == ShutdownDiscarded)
                    {
                        // Neither accepted nor failed - shutdown back-pressure.
                    }
                    else
                    {
                        Interlocked.Add(ref writtenTotal, committed);
                        Interlocked.Add(ref writtenSinceReport, committed);
                        var failed = batchToFlush.Count - committed;
                        if (failed > 0)
                        {
                            Interlocked.Add(ref failedTotal, failed);
                            Interlocked.Add(ref failedSinceReport, failed);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Shutdown path - failures already accounted for.
                }
                finally
                {
                    Interlocked.Decrement(ref inFlight);
                    flushGate.Release();
                }
            }, CancellationToken.None);
        }

        // Reporter task: samples Interlocked counters on the report
        // cadence so a stalled drain loop (e.g. all flushers blocked
        // on the WAL) still produces a progress line. Cleanly exits
        // when `ct` is cancelled.
        var reporterCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var reporterTask = Task.Run(async () =>
        {
            var lastReport = Stopwatch.GetTimestamp();
            try
            {
                while (!reporterCts.IsCancellationRequested)
                {
                    await Task.Delay(settings.ReportInterval, reporterCts.Token).ConfigureAwait(false);
                    var now = Stopwatch.GetTimestamp();
                    var sinceLocal = now - lastReport;
                    var written = Interlocked.Exchange(ref writtenSinceReport, 0);
                    var failed = Interlocked.Exchange(ref failedSinceReport, 0);
                    var inFlightNow = Interlocked.Read(ref inFlight);
                    var totalNow = Interlocked.Read(ref writtenTotal);
                    var rate = written / Math.Max(0.001, sinceLocal / (double)Stopwatch.Frequency);
                    var elapsed = (now - startedAt) / (double)Stopwatch.Frequency;
                    var failedTag = failed > 0 ? $" failed={failed,8:N0}" : string.Empty;
                    var mvLagTag = string.Empty;
                    if (mvLagProbe is not null)
                    {
                        // Read the maintainer's last published apply-lag sample
                        // off the metrics surface - no grain RPC, so the reporter
                        // cadence cannot perturb the tree under test. -1 means the
                        // maintainer has not published a sample yet this run.
                        var lag = mvLagProbe.LatestApplyLag;
                        mvLagTag = lag < 0 ? "      mvLag=     n/a" : $" mvLag={lag,8:N0}";
                    }
                    Console.WriteLine($"[silo] t={elapsed,7:0.0}s ops={totalNow,12:N0} ops/sec={rate,10:N0} inFlight={inFlightNow,3}{failedTag}{mvLagTag}");
                    lastReport = now;
                }
            }
            catch (OperationCanceledException) { }
        }, CancellationToken.None);

        // Stall watchdog: when the WAL write pipeline wedges (writtenTotal
        // frozen while inFlight stays non-zero, or while a sustained
        // provider-failure stream accumulates without inFlight
        // advancement), self-snapshot with ClrMD and print the parked async
        // state-machine chain + thread stacks to stdout - the in-process
        // equivalent of `dumpasync` / `dotnet-stack`, exfiltrated through
        // the systemd-journald-captured silo log. Three prior
        // `TimeoutException`-based fixes each fired zero times on the
        // wedge, so this captures the actually-parked await instead of
        // bounding another guessed one. The dual-arm shape (inFlight > 0
        // OR sustained failures) is the dual-arm generalisation: Shape A
        // (inFlight=N<cap parked on a saturating account) and Shape B
        // (inFlight=0 because batches faulted, but FINAL never emits)
        // both promote to a wedge now. Shares the reporter's cancellation
        // so it stops cleanly at end of run.
        var stallWatchdog = new StallWatchdog(
            writtenTotalSnapshot: () => Interlocked.Read(ref writtenTotal),
            inFlightSnapshot: () => Interlocked.Read(ref inFlight),
            failedTotalSnapshot: () => Interlocked.Read(ref failedTotal),
            // One full WAL batch (default 100 entries) of failures per
            // poll interval is the noise floor we use to promote a
            // frozen written-total to a wedge. Below that, a single
            // straggler batch failing late in the run can still be a
            // healthy tail.
            failedDeltaThreshold: 100L,
            stallWindow: TimeSpan.FromSeconds(20),
            pollInterval: TimeSpan.FromSeconds(1));
        var stallWatchdogTask = Task.Run(() => stallWatchdog.RunAsync(reporterCts.Token), CancellationToken.None);

        var batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
        var nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);

        // Track in-flight flush tasks so the FINAL line can wait for
        // them all to drain. Add/Remove happen on different threads
        // (drain loop vs flush-completion continuations) so every
        // mutation is under the set's own lock.
        void TrackFlush(Task task)
        {
            lock (flushTasks) { flushTasks.Add(task); }
            _ = task.ContinueWith(t => { lock (flushTasks) { flushTasks.Remove(t); } }, TaskScheduler.Default);
        }

        try
        {
            while (await reader.WaitToReadAsync(ct))
            {
                while (reader.TryRead(out var entry))
                {
                    batch.Add(entry);
                    if (batch.Count >= settings.BatchSize)
                    {
                        var ready = batch;
                        batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
                        // `DispatchFlushAsync` awaits the semaphore so the
                        // drain loop pauses here until a flush slot is
                        // free. That propagates backpressure all the way
                        // up: the channel fills, the TCP reader's
                        // `WriteAsync` blocks, and the producer slows
                        // to the silo's actual write rate. The previous
                        // shape created a Task per batch unconditionally
                        // and only awaited the gate inside the Task,
                        // which let the threadpool accumulate thousands
                        // of pending flush tasks while the silo plodded
                        // along at its own rate.
                        var flushTask = await DispatchFlushAsync(ready);
                        TrackFlush(flushTask);
                        nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);
                    }
                }

                if (batch.Count > 0 && Stopwatch.GetTimestamp() >= nextFlush)
                {
                    var ready = batch;
                    batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
                    var flushTask = await DispatchFlushAsync(ready);
                    TrackFlush(flushTask);
                    nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);
                }
            }

            if (batch.Count > 0)
            {
                var ready = batch;
                batch = new List<KeyValuePair<string, byte[]>>(0);
                // FX-029: at the producer-stop boundary, if the silo
                // has been observed Saturated at any point within the
                // recent-saturation window, do NOT dispatch the residual
                // batch as a new SetManyAsync. Dispatching it would add
                // another batch to the in-flight queue against a
                // storage account that is back-pressured; the dispatch
                // would trip WalAppendDispatchTimeout 30 s later and
                // surface as failed=N on FINAL. The entries are
                // discarded instead - counted under `discardedTotal` so
                // the FINAL accounting is honest (neither `written` nor
                // `failed`). The previous failure mode is documented in
                // benchmark/azure-throughput/throughput.md section 33.4
                // and was the root cause of the WEDGE verdicts in 2/3
                // set-many cohorts of the F-086 closeout run.
                //
                // The recency check (vs. consulting GetCurrentState
                // directly) defends against the F-085 classifier's known
                // Healthy<->Saturated flap (FX-030): a tree that flapped
                // Saturated within RecentSaturationWindow is treated as
                // still-saturated for the drain decision even if the
                // current sampler tick reads Healthy. The window is
                // sized to the WalAppendDispatchTimeout the in-flight
                // batches sit on so a recently-Saturated tree is
                // assumed to have storage-side back-pressure that
                // persists at least that long.
                //
                // The check is intentionally narrow: only the FINAL
                // residual batch at producer-stop is guarded. In-flight
                // batches already dispatched through DispatchFlushAsync
                // are allowed to settle through the existing
                // Task.WhenAll(outstanding) path below; if they trip
                // WalAppendDispatchTimeout they still count as failed
                // (bounded by FlushConcurrency, so worst-case 8 batches
                // = 32k entries, vs. the unbounded post-stop channel
                // backlog that was the dominant contributor).
                var lastSat = saturationGate.LastSaturatedUtc(settings.TreeId);
                var recentlySaturated = lastSat.HasValue
                    && (DateTimeOffset.UtcNow - lastSat.Value) < RecentSaturationWindow;
                if (recentlySaturated)
                {
                    Interlocked.Add(ref discardedTotal, ready.Count);
                    Console.WriteLine($"[silo:ingest] FX-029 abandon residual batch entries={ready.Count} (last Saturated at {lastSat:O})");
                }
                else
                {
                    var flushTask = await DispatchFlushAsync(ready);
                    TrackFlush(flushTask);
                }
            }
        }
        catch (OperationCanceledException) { }

        // Drain in-flight flushes so the FINAL line reflects everything
        // that was accepted from the channel.
        Task[] outstanding;
        lock (flushTasks)
        {
            outstanding = new Task[flushTasks.Count];
            flushTasks.CopyTo(outstanding);
        }

        // FX-032 Symptom 2: in-flight-tail quiesce. The FX-029 gate
        // above guards only the residual ingest-channel batch (the
        // last batch the producer assembled but had not yet
        // dispatched). Batches that DispatchFlushAsync already
        // accepted into `flushTasks` before the producer-stop are
        // still racing the storage account at this point; under the
        // single-account 409-Conflict regime they sit parked on the
        // writer-side admission cap for up to ~30 s (WalAppendDispatchTimeout)
        // and surface as failed=N on FINAL even though the producer
        // exited cleanly. Mirror the FX-029 recency check here:
        // when the silo was Saturated within RecentSaturationWindow,
        // park on IWalSaturationSignal.WaitForHealthyAsync (bounded
        // by InFlightTailQuiesceBudget so the wait cannot consume the
        // systemd stop window before FINAL is emitted - see FX-038) so
        // the in-flight tail
        // gets a chance to settle against a recovered storage account
        // instead of bleeding through the deadline. The wait short-
        // circuits on signal recovery, on the budget timeout, or on
        // cancellation - on every exit path the existing
        // Task.WhenAll(outstanding) below releases the tail. This
        // gate is best-effort accounting (the failed=N count is
        // smaller when the storage account cools off in time) and
        // not a correctness guarantee.
        var lastSatTail = saturationGate.LastSaturatedUtc(settings.TreeId);
        var recentlySaturatedTail = lastSatTail.HasValue
            && (DateTimeOffset.UtcNow - lastSatTail.Value) < RecentSaturationWindow;
        if (recentlySaturatedTail && outstanding.Length > 0)
        {
            Console.WriteLine($"[silo:ingest] in-flight-tail quiesce: awaiting WaitForHealthyAsync for up to {InFlightTailQuiesceBudget} before releasing {outstanding.Length} in-flight flushes (last Saturated at {lastSatTail:O}).");
            using var quiesceCts = new CancellationTokenSource(InFlightTailQuiesceBudget);
            var quiesceStartTicks = Stopwatch.GetTimestamp();
            try
            {
                await saturationGate.WaitForHealthyAsync(settings.TreeId, quiesceCts.Token);
                var quiesceElapsed = Stopwatch.GetElapsedTime(quiesceStartTicks);
                Console.WriteLine($"[silo:ingest] in-flight-tail quiesce: signal recovered after {quiesceElapsed.TotalSeconds:0.0}s; releasing {outstanding.Length} in-flight flushes.");
            }
            catch (OperationCanceledException) when (quiesceCts.IsCancellationRequested)
            {
                Console.WriteLine($"[silo:ingest] in-flight-tail quiesce: budget {InFlightTailQuiesceBudget} expired without recovery; falling through to in-flight WhenAll (in-flight batches will settle through their dispatch deadlines).");
            }
        }

        // FX-038: bound the in-flight-tail release so a tail still parked
        // on a saturated account cannot consume the systemd stop window
        // and starve FINAL. Outstanding flushes left unsettled at the
        // deadline keep running detached and account themselves as
        // failed=N through their own dispatch deadlines; FINAL is emitted
        // immediately so the cohort reports HEALTHY-with-failures rather
        // than WEDGE.
        if (outstanding.Length > 0)
        {
            var whenAll = Task.WhenAll(outstanding);
            var completed = await Task.WhenAny(whenAll, Task.Delay(InFlightTailWhenAllBudget)).ConfigureAwait(false);
            if (completed == whenAll)
            {
                try { await whenAll; } catch { /* per-task failures already accounted for */ }
            }
            else
            {
                Console.WriteLine($"[silo:ingest] in-flight-tail release: budget {InFlightTailWhenAllBudget} expired with {outstanding.Length} flush(es) still settling; emitting FINAL now (unsettled batches account as failed=N through their dispatch deadlines).");
                _ = whenAll.ContinueWith(static t => { _ = t.Exception; }, TaskScheduler.Default);
            }
        }

        reporterCts.Cancel();
        try { await reporterTask; } catch { /* shutdown */ }
        try { await stallWatchdogTask; } catch { /* shutdown */ }
        reporterCts.Dispose();

        var endedAt = Stopwatch.GetTimestamp();
        var totalElapsed = (endedAt - startedAt) / (double)Stopwatch.Frequency;
        var opsFinal = Interlocked.Read(ref writtenTotal);
        var failedFinal = Interlocked.Read(ref failedTotal);
        var discardedFinal = Interlocked.Read(ref discardedTotal);
        // "Active" window: from first accepted batch to last drained flush.
        // Excludes the idle pre-connect window and is the most accurate
        // measure of sustained ingest throughput. Falls back to total
        // when nothing was accepted (silo started but no producer ever
        // connected).
        var firstAccept = Interlocked.Read(ref firstAcceptedAt);
        var activeElapsed = firstAccept != 0
            ? (endedAt - firstAccept) / (double)Stopwatch.Frequency
            : totalElapsed;
        var avgTotal = opsFinal / Math.Max(0.001, totalElapsed);
        var avgActive = opsFinal / Math.Max(0.001, activeElapsed);
        // FX-029: include `discarded=N` on the FINAL line so the cohort
        // runner and any external parser can attribute the at-shutdown
        // abandon-on-Saturated path independently of `failed=N` (which
        // counts genuine dispatch-deadline trips against the storage
        // account). A non-zero `discarded` count is the operational
        // signal that the producer's natural-stop window coincided with
        // a Saturated regime and the bench correctly chose to drop the
        // residual backlog. The token is suffix-appended and does not
        // affect the existing `ops=` / `failed=` regex parses in
        // run-cohort.ps1.
        // set-point-mv: a final apply-lag reading after the producer has
        // stopped and the foreground backlog has drained. With writes quiesced
        // the asynchronous view maintainer should catch up to the source head,
        // so a lag trending to zero here is the closeout evidence that the view
        // is eventually-consistent off the hot path rather than blocking it.
        // Read from the metrics surface (the maintainer's last published
        // apply_lag sample) - no grain RPC, so the closeout can neither add
        // shutdown-path load to the silo nor throw a shutdown-race exception
        // that would inflate the cohort's exception tally.
        if (mvLagProbe is not null)
        {
            var lagAtStop = mvLagProbe.LatestApplyLag;
            if (lagAtStop < 0)
            {
                Console.WriteLine("[silo] MV-FINAL view=bench lagAtStop=n/a (the maintainer published no apply_lag sample this run)");
            }
            else
            {
                Console.WriteLine($"[silo] MV-FINAL view=bench lagAtStop={lagAtStop:N0} (0 = the asynchronous materialised view has fully caught up to the source head)");
            }

            mvLagProbe.Dispose();
        }

        Console.WriteLine($"[silo] FINAL ops={opsFinal:N0} failed={failedFinal:N0} discarded={discardedFinal:N0} elapsed={totalElapsed:0.0}s active={activeElapsed:0.0}s ops/sec (avg)={avgTotal:N0} (active avg)={avgActive:N0}");
    }

    // Sentinel returned by FlushAsync when a SetManyAsync was rejected
    // because the silo is draining at shutdown. The dispatcher treats it
    // as "neither accepted nor failed" - the in-flight batch raced the
    // drain after the producer closed the socket and is correctly not
    // counted in either the written or the failed total.
    private const int ShutdownDiscarded = -1;

    // Bounded retry policy for the silo-side SetManyAsync call. A
    // freshly-started silo's first thousands of leaf-grain activations
    // race the placement directory and surface as
    // OrleansMessageRejectionException("Unable to create local
    // activation" / "to invalid activation"); the directory recovers
    // within a few hundred ms. The startup-reshard path (line ~497)
    // already retries this exact class on the same rationale; the hot
    // path needs the same treatment so the cold-start storm does not
    // count an entire batch (up to FlushBatchSize) as failed.
    //
    // 5 attempts total, 50 ms base, exponential * 2 capped at 800 ms,
    // with +/-25% jitter. With FlushBatchSize=4096 and the cold-start
    // window measured at ~2 s on step 8c-c-iii, this gives each
    // rejected batch up to ~4 s of grace before falling through to the
    // failed-batch counter, which is comfortably inside the cold-start
    // window without leaking into steady-state recovery.
    private const int FlushMaxAttempts = 5;
    private const int FlushRetryBaseMs = 50;
    private const int FlushRetryMaxMs = 800;

    // FX-029: time window after the most-recently observed Saturated
    // transition during which the bench's drain loop treats the silo
    // as still-saturated for the purposes of the residual-batch
    // dispatch decision. Sized to the WalAppendDispatchTimeout the
    // in-flight batches sit on so a recently-Saturated tree is
    // assumed to have storage-side back-pressure that persists at
    // least that long; a producer-stop that lands within 30 s of the
    // last Saturated transition abandons the residual batch rather
    // than dispatching it into a queue that would trip the deadline.
    // Matches LatticeOptions.DefaultWalAppendDispatchTimeout (the
    // bench inherits the library default unless an operator overrides
    // it via BENCH_WAL_APPEND_DISPATCH_TIMEOUT_SEC, in which case the
    // bench's behaviour here may slightly over- or under-shoot the
    // optimal window - acceptable for a benchmark whose entire point
    // is measuring steady-state throughput, not residual-batch
    // accounting precision).
    private static readonly TimeSpan RecentSaturationWindow = TimeSpan.FromSeconds(30);

    // FX-032 Symptom 2 / FX-038: hard ceiling on the in-flight-tail
    // quiesce wait at drain entry. After abandoning the residual
    // ingest-channel batch (FX-029) and before releasing the in-flight
    // tail via the bounded WhenAll below, the drain awaits
    // IWalSaturationSignal.WaitForHealthyAsync for at most this
    // duration when the silo was recently Saturated.
    //
    // FX-038: the binding constraint on this budget is the bench host's
    // systemd stop deadline (`lattice-silo.service` TimeoutStopSec=30s
    // and the host ShutdownTimeout, default 30s) - NOT the in-process
    // LatticeOptions.WalDrainBudget (75s). On SIGTERM the systemd unit
    // SIGKILLs the dotnet process 30s later regardless of WalDrainBudget,
    // so the FINAL line must be emitted well inside that 30s window.
    // The prior 30-second quiesce budget was sized against WalDrainBudget
    // and so was equal to TimeoutStopSec: when the tree was still
    // Saturated at the producer-stop boundary (the normal case for the
    // slow set-many-atomic saga path), WaitForHealthyAsync burned the
    // entire stop window and the process was SIGKILL'd before FINAL was
    // ever written - the WEDGE phenotype in 2/3 set-many-atomic cohorts
    // of the F-086 closeout run.
    //
    // The 10-second cap here, paired with the InFlightTailWhenAllBudget
    // bound on the subsequent Task.WhenAll, keeps the worst-case post-
    // stop drain (10s quiesce + 12s WhenAll + reporter shutdown + FINAL
    // write) comfortably under the 30s SIGKILL deadline, so FINAL always
    // emits. The wait short-circuits the moment the signal observes
    // recovery, so the common case (the account cools off within a few
    // seconds of the producer stop) returns well under the cap. On an
    // unrecovered tree the wait times out and the drain falls through to
    // the bounded WhenAll - the in-flight tail still settles, accounted
    // as failed=N through the normal dispatch-deadline path. A FINAL with
    // failed=N is strictly more useful than a WEDGE (no data): this gate
    // is best-effort accounting, not a correctness guarantee.
    private static readonly TimeSpan InFlightTailQuiesceBudget = TimeSpan.FromSeconds(10);

    // FX-038: hard ceiling on the post-quiesce Task.WhenAll(outstanding)
    // so an in-flight tail that stays parked on a still-saturated storage
    // account (each in-flight flush can sit on the writer-side admission
    // cap for up to WalAppendDispatchTimeout, default 30s) cannot itself
    // consume the systemd stop window and starve FINAL emission. When the
    // budget expires the outstanding flushes are left to settle/abandon
    // through their own dispatch deadlines and already account themselves
    // as failed=N; FINAL is emitted immediately so the cohort is reported
    // as HEALTHY-with-failures rather than wedged. Sized together with
    // InFlightTailQuiesceBudget to leave margin below TimeoutStopSec=30s.
    //
    // Overridable because that 30s stop window is a property of the Layer 2
    // host, not of the engine. Layer 2 runs the engine as a systemd unit
    // (TimeoutStopSec=30), so 12s is the right ceiling there and stays the
    // default, leaving the single-VM path byte-identical. Layer 3 runs the
    // engine inside a Container Apps *job*, which is bounded by its own
    // replicaTimeout and has no systemd stop window at all, so it can
    // afford to let the tail actually finish. That matters because the
    // budget is what decides whether trailing work lands in `ops` or in
    // `failed`: an N=2 cohort measured here drained 16 concurrent flushes
    // whose p50 was 5.7s and p99 11.2s, overran the 12s ceiling, and
    // reported failed=65,536 - exactly FlushConcurrency x BatchSize, i.e.
    // the whole in-flight budget abandoned at the deadline rather than any
    // sustained failure. Since FlushConcurrency scales with silo count,
    // that artefact grows with N and would read as "scaling gets less
    // reliable" on precisely the curve this benchmark exists to publish.
    private static readonly TimeSpan InFlightTailWhenAllBudget =
        TimeSpan.FromSeconds(ReadTailBudgetSeconds());

    private static double ReadTailBudgetSeconds()
    {
        const double DefaultSeconds = 12;
        var raw = Environment.GetEnvironmentVariable("BENCH_INFLIGHT_TAIL_BUDGET_SEC");
        if (string.IsNullOrWhiteSpace(raw)) { return DefaultSeconds; }
        return double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) && parsed > 0
            ? parsed
            : DefaultSeconds;
    }

    private async Task<int> FlushAsync(ILattice lattice, List<KeyValuePair<string, byte[]>> batch, CancellationToken ct)
    {
        var startTs = Stopwatch.GetTimestamp();
        Exception? lastRejection = null;
        var modeTag = new KeyValuePair<string, object?>("mode", BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
        var treeTag = new KeyValuePair<string, object?>("tree", settings.TreeId);
        for (var attempt = 1; attempt <= FlushMaxAttempts; attempt++)
        {
            try
            {
                await BenchWorkloadDispatcher.DispatchAsync(
                    settings.WorkloadMode,
                    lattice,
                    batch,
                    settings.AtomicBatchSize,
                    settings.FlushConcurrency,
                    ct,
                    grainFactory,
                    settings.TreeId).ConfigureAwait(false);
                var elapsedMs = Stopwatch.GetElapsedTime(startTs).TotalMilliseconds;
                BenchMetrics.LatticeOpDurationMs.Record(elapsedMs, treeTag, modeTag);
                BenchMetrics.LatticeOpRetryAttempts.Record(attempt - 1, treeTag, modeTag);
                return batch.Count;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) when (lifetime.ApplicationStopping.IsCancellationRequested && IsShutdownRejection(ex))
            {
                // Expected: producer closed the socket, the silo emitted its
                // FINAL line, and the host is now draining grain activations.
                // Any in-flight SetManyAsync that races the drain gets an
                // OrleansMessageRejectionException ("Unable to create local
                // activation" / "silo is blocking application messages"). The
                // entries those batches carried were never accepted by the
                // lattice so they are correctly not in `written`; they should
                // also not be in `failed`, because they are not a real
                // ingestion failure - they are shutdown back-pressure. Return
                // the sentinel so the dispatcher skips both counters.
                BenchMetrics.LatticeOpRetryAttempts.Record(attempt - 1, treeTag, modeTag);
                return ShutdownDiscarded;
            }
            catch (Exception ex) when (!lifetime.ApplicationStopping.IsCancellationRequested && IsOrleansMessageRejection(ex))
            {
                // Transient: the placement directory rejected the forward
                // because the target activation has not landed yet. The
                // directory recovers on its own; back off and retry.
                lastRejection = ex;
                if (attempt >= FlushMaxAttempts)
                {
                    break;
                }
                var backoffMs = Math.Min(FlushRetryMaxMs, FlushRetryBaseMs * (1 << (attempt - 1)));
                // +/-25% jitter so concurrent flushGate slots do not
                // resynchronise on the same retry wave.
                var jitter = Random.Shared.NextDouble() * 0.5 - 0.25;
                var delayMs = (int)Math.Max(1, backoffMs * (1 + jitter));
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { throw; }
            }
            catch (Exception ex)
            {
                BenchMetrics.LatticeOpRetryAttempts.Record(attempt - 1, treeTag, modeTag);
                // Surface the most common saturation-vs-bug class explicitly. A bare
                // TimeoutException out of Orleans means the SILO's own grain RPC deadline
                // (Silo+Client `ResponseTimeout`) fired before SetManyAsync returned. This
                // is NOT a wedge - it's the bench harness's outer call hitting its 30s
                // (default) ceiling because the configured offered rate exceeds the
                // sustainable Tables drain rate at this rung. The G-026 writer admission
                // cap is doing exactly what it's designed to do (queueing); the deadline
                // just happens to be shorter than the realistic worst-case admission
                // wait. Knobs: raise BENCH_RESPONSE_TIMEOUT_SEC, reduce BENCH_TICK_HZ or
                // BENCH_VEHICLE_COUNT, raise BENCH_WAL_PARTITIONS / WalMaxPendingBatches.
                if (ex is TimeoutException)
                {
                    logger.LogWarning(
                        "[silo] grain-rpc-deadline: SetManyAsync of {Count} did not return within ResponseTimeout " +
                        "(BENCH_RESPONSE_TIMEOUT_SEC={ResponseTimeoutSec}s). Offered rate exceeds sustained Tables " +
                        "drain rate at this rung; raise BENCH_RESPONSE_TIMEOUT_SEC, drop tickHz/vehicles, or tune WAL " +
                        "fan-out (BENCH_WAL_PARTITIONS / BENCH_WAL_MAX_PENDING_BATCHES). mode={Mode}",
                        batch.Count, settings.ResponseTimeoutSec, BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
                }
                else
                {
                    logger.LogWarning(ex, "[silo] flush of {Count} failed (mode={Mode})", batch.Count, BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
                }
                return 0;
            }
        }

        BenchMetrics.LatticeOpRetryAttempts.Record(FlushMaxAttempts - 1, treeTag, modeTag);
        logger.LogWarning(
            lastRejection,
            "[silo] flush of {Count} failed after {Attempts} retry attempts against transient OrleansMessageRejectionException (mode={Mode})",
            batch.Count,
            FlushMaxAttempts,
            BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode));
        return 0;
    }
}
