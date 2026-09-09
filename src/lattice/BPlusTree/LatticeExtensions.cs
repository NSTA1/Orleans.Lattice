using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Lattice;

/// <summary>
/// Extension methods for <see cref="ILattice"/>.
/// </summary>
public static class LatticeExtensions
{
    /// <summary>
    /// Default reconnect budget for <see cref="ScanKeysAsync"/> and
    /// <see cref="ScanEntriesAsync"/> when the remote enumerator is reclaimed
    /// mid-scan (silo failover, cold start, idle expiry, scale-down).
    /// Overridable per call via the <c>maxAttempts</c> parameter.
    /// </summary>
    public const int DefaultScanReconnectAttempts = 8;

    /// <summary>
    /// Default resume budget for <see cref="ScanKeysAsync"/> and
    /// <see cref="ScanEntriesAsync"/> when a shard-root page fill is abandoned
    /// mid-scan with <see cref="ScanPageStalledException"/> - deliberately far
    /// smaller than <see cref="DefaultScanReconnectAttempts"/>, and a separate
    /// counter rather than a share of it.
    /// <para>
    /// The two faults cost different amounts, so they must not draw on one
    /// budget. An <c>EnumerationAbortedException</c> is an enumerator reclaim:
    /// it is raised the instant the activation goes, so a reopen costs
    /// essentially nothing and eight of them are cheap. A stall is only raised
    /// once the whole
    /// <see cref="LatticeOptions.MaxScanPageStallDuration"/> ceiling has
    /// elapsed, so each attempt costs that ceiling - on the derived default,
    /// tens of seconds. Eight of those against one parked leaf read would be
    /// minutes of repeated shard re-entry, which is the amplification a retry
    /// under contention is rightly suspected of.
    /// </para>
    /// <para>
    /// There is no separate parameter for it: the effective stall budget is
    /// <c>min(maxAttempts, DefaultScanStallResumeAttempts)</c>, so
    /// <c>maxAttempts: 0</c> continues to mean fail-fast for both faults and a
    /// caller that raises <c>maxAttempts</c> for a long walk does not silently
    /// raise its tolerance for stalls with it.
    /// </para>
    /// <para>
    /// It is also the only bound on the resume's total wall-clock cost, which
    /// is why it is left deliberately small rather than widened now that the
    /// resume actually runs (issue 2456). There is no separate time budget: at
    /// N resume attempts a scan can spend roughly N+1 ceilings failing, plus
    /// the backoff between them, before it gives up. Callers here are driven by
    /// interval reminders rather than by a hard per-pass deadline, so an
    /// over-generous budget would not be cut off by anything - it would simply
    /// make each doomed pass slower. Two was chosen conservatively on that
    /// basis: enough to clear a transient queue, not enough to turn a scan that
    /// cannot finish into a long one.
    /// </para>
    /// </summary>
    public const int DefaultScanStallResumeAttempts = 2;

    /// <summary>
    /// Fraction of the ceiling reported by a stall that a resilient scan waits
    /// before resuming, multiplied by the attempt number.
    /// <para>
    /// Derived from the ceiling the stall itself reports
    /// (<see cref="ScanPageStalledException.TimeoutSeconds"/>) rather than set
    /// as an absolute duration, so the backoff cannot drift away from
    /// <see cref="LatticeOptions.MaxScanPageStallDuration"/> when a deployment
    /// retunes it: the two move together by construction and there is no second
    /// knob to remember.
    /// </para>
    /// <para>
    /// The fraction itself is conservative and is <em>not</em> tuned against a
    /// measurement. It is chosen to be on the timescale of the causes a stall
    /// names - a leaf replaying its WAL window from cold, an activation queued
    /// behind another call, a contended storage read - which clear in seconds,
    /// not in the milliseconds that
    /// <see cref="ComputeReconnectDelayMs"/> waits for an enumerator reclaim.
    /// Resuming on that millisecond ramp would descend onto the same still-parked
    /// read and burn another whole ceiling.
    /// </para>
    /// </summary>
    internal const double ScanStallResumeBackoffFraction = 0.25;

    internal const string StallOutcomeResumed = "resumed";

    /// <summary>
    /// The terminal stall outcome. It is the only one, because the resume is
    /// bounded by budget alone: see the resume site in
    /// <c>ScanKeysAsyncCore</c>.
    /// <para>
    /// There was a second terminal outcome, <c>no-progress</c>, recorded when a
    /// progress gate refused a stall that still had budget. That gate is gone
    /// and the label is deliberately not retained as an unrecordable constant.
    /// An instrument that has never recorded is <em>absent</em> from a scrape
    /// rather than zero, so a documented outcome that can no longer occur reads
    /// to an operator as a fact about the workload ("no stall ever failed this
    /// way") when it is really a fact about the code. Removing the label makes
    /// its disappearance a code change somebody can find, instead of a silence.
    /// </para>
    /// </summary>
    internal const string StallOutcomeBudgetExhausted = "budget-exhausted";

    /// <summary>
    /// The stall resume budget in force for a scan whose reconnect budget is
    /// <paramref name="reconnectBudget"/>. See
    /// <see cref="DefaultScanStallResumeAttempts"/> for why it is a floor over
    /// the reconnect budget rather than a parameter of its own.
    /// </summary>
    internal static int ComputeScanStallResumeBudget(int reconnectBudget) =>
        Math.Min(reconnectBudget, DefaultScanStallResumeAttempts);

    /// <summary>
    /// The delay before resuming a scan that stalled, derived from the ceiling
    /// the stall reported. See <see cref="ScanStallResumeBackoffFraction"/>.
    /// A stall carrying no usable ceiling (a default-constructed instance, or a
    /// non-finite value) falls back to
    /// <see cref="LatticeOptions.DefaultMaxScanPageDuration"/> so the wait is
    /// still derived from a real bound rather than from a literal.
    /// </summary>
    internal static int ComputeScanStallResumeDelayMs(double ceilingSeconds, int attempt)
    {
        if (attempt < 1)
        {
            return 0;
        }

        var seconds = double.IsFinite(ceilingSeconds) && ceilingSeconds > 0
            ? ceilingSeconds
            : LatticeOptions.DefaultMaxScanPageDuration.TotalSeconds;

        var delay = seconds * ScanStallResumeBackoffFraction * attempt;

        // Never wait longer than the ceiling itself: past that point the caller
        // is spending more time waiting to retry than the stall it is retrying.
        if (delay > seconds)
        {
            delay = seconds;
        }

        return (int)Math.Ceiling(delay * 1000.0);
    }

    /// <summary>
    /// Records the decision a resilient scan took on meeting a stall, so a scan
    /// that completed only after resuming is distinguishable from one that
    /// never stalled. See
    /// <see cref="LatticeMetrics.ScanStallResumptions"/>.
    /// </summary>
    internal static void RecordScanStallOutcome(ScanPageStalledException stall, string outcome)
    {
        var treeId = stall.TreeId ?? string.Empty;
        LatticeMetrics.ScanStallResumptions.Add(
            1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagPhase, stall.Phase ?? string.Empty),
            new KeyValuePair<string, object?>(LatticeMetrics.TagOutcome, outcome),
            LatticeTenantLabel.ForTree(treeId));
    }

    /// <summary>
    /// Streams sorted key-value pairs into the tree, partitioning by physical
    /// shard and flushing chunks in parallel across shards. Each shard receives
    /// its entries in key order via <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.BulkAppendAsync"/>,
    /// which appends to the right edge without splits.
    /// <para>
    /// The input <paramref name="sortedEntries"/> <b>must</b> be in ascending key order.
    /// Per-shard ordering is preserved because hash-partitioning a globally sorted
    /// stream preserves the relative order within each partition.
    /// </para>
    /// <para>
    /// Routing is resolved up front via <see cref="ILattice.GetRoutingAsync"/>,
    /// so entries are correctly partitioned by the tree's persisted
    /// <see cref="ShardMap"/> - including non-default maps produced by adaptive
    /// shard splits.
    /// </para>
    /// </summary>
    /// <param name="lattice">The tree to load into.</param>
    /// <param name="sortedEntries">Entries in ascending key order.</param>
    /// <param name="grainFactory">The grain factory (needed to address shard grains directly).</param>
    /// <param name="chunkSize">Max entries per shard before flushing (default 10 000).</param>
    /// <param name="cancellationToken">Cancellation token checked between entry enqueues and between flushes.</param>
    public static async Task BulkLoadAsync(
        this ILattice lattice,
        IAsyncEnumerable<KeyValuePair<string, byte[]>> sortedEntries,
        IGrainFactory grainFactory,
        int chunkSize = 10_000,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(sortedEntries);
        ArgumentNullException.ThrowIfNull(grainFactory);
        cancellationToken.ThrowIfCancellationRequested();

        var routing = await lattice.GetRoutingAsync(cancellationToken);
        var physicalTreeId = routing.PhysicalTreeId;
        var shardMap = routing.Map;
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // One slot object per physical shard, holding everything the flush loop
        // needs: the pending buffer, the previous flush's task, the chunk
        // counter, and the cached grain proxy. The prior form kept those four
        // fields in four parallel Dictionary<int, ...> maps keyed by the same
        // physical shard index, so a single buffered entry hashed that index
        // once and a single chunk flush hashed it six more times. The index
        // lives in a tiny dense domain (one entry per shard root, typically
        // 1-16), so a shard-indexed array answers the same question with an
        // array read and the hashing disappears entirely; a hand-built map
        // carrying a negative or pathologically large physical index still
        // falls back to a hash map inside ShardSlots. Proxies are cached once
        // so repeated chunk flushes don't rebuild grain keys or re-hit the
        // grain factory's lookup table.
        var slots = new ShardSlots<BulkLoadShardSlot>(physicalShards);
        var batchId = Guid.NewGuid().ToString("N");
        foreach (var idx in physicalShards)
        {
            slots.Set(idx, new BulkLoadShardSlot(
                new List<KeyValuePair<string, byte[]>>(chunkSize),
                grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{idx}")));
        }

        await foreach (var entry in sortedEntries.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shardIdx = shardMap.Resolve(entry.Key);
            var slot = slots.Get(shardIdx)!;
            var buffer = slot.Buffer;
            buffer.Add(entry);

            if (buffer.Count >= chunkSize)
            {
                // Wait for the previous flush to this shard to complete (preserves ordering).
                await slot.InFlight;

                var opId = $"{batchId}-{shardIdx}-{slot.ChunkCounter++}";
                slot.InFlight = slot.Shard.BulkAppendAsync(opId, buffer);
                slot.Buffer = new List<KeyValuePair<string, byte[]>>(chunkSize);
            }
        }

        // Flush remaining buffers.
        var finalTasks = new List<Task>(slots.Count);
        foreach (var (idx, slot) in slots)
        {
            if (slot.Buffer.Count > 0)
            {
                // Wait for previous in-flight for this shard, then flush.
                await slot.InFlight;
                var opId = $"{batchId}-{idx}-{slot.ChunkCounter++}";
                finalTasks.Add(slot.Shard.BulkAppendAsync(opId, slot.Buffer));
            }
            else
            {
                finalTasks.Add(slot.InFlight);
            }
        }

        await Task.WhenAll(finalTasks);
    }

    /// <summary>
    /// The per-physical-shard state a streaming bulk load carries: the pending
    /// chunk buffer, the previous chunk's in-flight flush, the chunk counter
    /// that makes each flush's operation id unique and replay-safe, and the
    /// cached shard grain proxy.
    /// </summary>
    private sealed class BulkLoadShardSlot(
        List<KeyValuePair<string, byte[]>> buffer,
        IShardRootGrain shard)
    {
        internal List<KeyValuePair<string, byte[]>> Buffer { get; set; } = buffer;

        internal Task InFlight { get; set; } = Task.CompletedTask;

        internal int ChunkCounter { get; set; }

        internal IShardRootGrain Shard { get; } = shard;
    }

    /// <summary>
    /// Subscribes to <see cref="LatticeTreeEvent"/> notifications for
    /// <paramref name="tree"/>. Each event (writes, deletes, splits,
    /// compactions, tree-lifecycle transitions, etc.) is delivered via the
    /// Orleans stream provider named <paramref name="providerName"/> (default
    /// <c>"Default"</c>) on the namespace
    /// <see cref="LatticeEventConstants.StreamNamespace"/> with stream id
    /// equal to the tree's logical id.
    /// <para>
    /// The silo must have <see cref="LatticeOptions.PublishEvents"/> enabled
    /// and the client must be connected to a cluster that has the same
    /// stream provider registered. Events are metadata-only - they carry
    /// <see cref="LatticeTreeEvent.Kind"/>, <see cref="LatticeTreeEvent.TreeId"/>,
    /// <see cref="LatticeTreeEvent.Key"/>, <see cref="LatticeTreeEvent.ShardIndex"/>,
    /// <see cref="LatticeTreeEvent.OperationId"/>, and
    /// <see cref="LatticeTreeEvent.AtUtc"/>. Use
    /// <see cref="ILattice.GetAsync(string, CancellationToken)"/> or
    /// <see cref="ILattice.GetWithVersionAsync(string, CancellationToken)"/>
    /// to read the current value for a key referenced by an event.
    /// </para>
    /// </summary>
    /// <param name="tree">The tree to subscribe to.</param>
    /// <param name="client">The Orleans cluster client that hosts the stream provider.</param>
    /// <param name="onEvent">Callback invoked for every received event. Exceptions
    /// propagate back into the Orleans stream pipeline - wrap in a try/catch if
    /// your consumer should be tolerant of its own faults.</param>
    /// <param name="providerName">Orleans stream provider name. Must match
    /// <see cref="LatticeOptions.EventStreamProviderName"/>. Defaults to
    /// <see cref="LatticeOptions.DefaultEventStreamProviderName"/>.</param>
    /// <param name="cancellationToken">Cancels the subscription handshake.</param>
    /// <returns>An Orleans stream subscription handle. Call
    /// <c>UnsubscribeAsync()</c> on it to stop receiving events.</returns>
    /// <exception cref="InvalidOperationException">Thrown when
    /// <paramref name="providerName"/> is not registered on the cluster client.</exception>
    public static Task<StreamSubscriptionHandle<LatticeTreeEvent>> SubscribeToEventsAsync(
        this ILattice tree,
        IClusterClient client,
        Func<LatticeTreeEvent, Task> onEvent,
        string providerName = LatticeOptions.DefaultEventStreamProviderName,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(onEvent);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerName);
        cancellationToken.ThrowIfCancellationRequested();

        IStreamProvider provider;
        try
        {
            provider = client.GetStreamProvider(providerName);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"No Orleans stream provider named '{providerName}' is registered on the cluster client. " +
                $"Register one via clientBuilder.AddMemoryStreams(\"{providerName}\") (or the Event Hub / Azure Queue equivalent) " +
                $"and ensure every silo hosting Lattice grains has the same provider registered.",
                ex);
        }

        var treeId = tree.GetPrimaryKeyString();
        var stream = provider.GetStream<LatticeTreeEvent>(
            StreamId.Create(LatticeEventConstants.StreamNamespace, treeId));
        return stream.SubscribeAsync((evt, _) => onEvent(evt));
    }

    /// <summary>
    /// Resilient forward/reverse key scan. Wraps <see cref="ILattice.KeysAsync"/>
    /// and transparently recovers from <c>Orleans.Runtime.EnumerationAbortedException</c>
    /// (raised when the remote enumerator on the orchestrator grain is reclaimed
    /// mid-scan due to silo failover, cold start, idle expiry, or scale-down).
    /// The wrapper tracks the last yielded key and - on abort - reopens the
    /// underlying scan with a tightened bound so the result stream is
    /// deterministic: no duplicates, no gaps, original ordering preserved.
    /// For forward scans the resume lower bound is the successor of the last
    /// yielded key (<c>lastKey + "\u0000"</c>); for reverse scans the resume
    /// upper bound becomes the last yielded key (exclusive).
    /// <para>
    /// The first reconnect is immediate; subsequent attempts apply a small
    /// linear backoff (10&#160;ms × attempt, capped at 100&#160;ms) to avoid
    /// a tight loop against a persistently-faulting orchestrator. If the
    /// retry budget is exhausted the last <c>EnumerationAbortedException</c>
    /// is rethrown verbatim. This is the recommended client API for long-running
    /// scans - <see cref="ILattice.KeysAsync"/> is retained for short,
    /// single-page reads and for internal orchestration.
    /// </para>
    /// </summary>
    /// <param name="lattice">The tree to scan.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the tree's lowest key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the tree's end.</param>
    /// <param name="reverse">If <c>true</c>, yields keys in descending order.</param>
    /// <param name="prefetch">Optional per-call override for shard prefetch; see <see cref="LatticeOptions.PrefetchKeysScan"/>.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget; defaults to <see cref="DefaultScanReconnectAttempts"/>.</param>
    /// <param name="cancellationToken">Cancellation token; honoured between reconnects and during backoff.</param>
    public static IAsyncEnumerable<string> ScanKeysAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        // Eager argument validation. The async-iterator core method below
        // defers any `throw` until first `MoveNextAsync` due to compiler
        // state-machine semantics, so the null-guard must live in this
        // non-async wrapper to surface synchronously the moment a caller
        // invokes `ScanKeysAsync(...)` (e.g. via `.GetAsyncEnumerator()`
        // without iterating).
        ArgumentNullException.ThrowIfNull(lattice);
        return ScanKeysAsyncCore(lattice, startInclusive, endExclusive, reverse, prefetch, maxAttempts, null, cancellationToken);
    }

    /// <summary>
    /// Resilient forward/reverse key scan whose keys are filtered server-side by
    /// the predicate IR <paramref name="predicate"/>. Mirrors
    /// <see cref="ScanKeysAsync"/>'s <c>EnumerationAbortedException</c> recovery,
    /// re-supplying the predicate as an explicit argument on every reconnect.
    /// </summary>
    internal static IAsyncEnumerable<string> ScanKeysWhereAsync(
        this ILattice lattice,
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        return ScanKeysAsyncCore(lattice, startInclusive, endExclusive, reverse, prefetch, maxAttempts, predicate, cancellationToken);
    }

    private static async IAsyncEnumerable<string> ScanKeysAsyncCore(
        ILattice lattice,
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool? prefetch,
        int? maxAttempts,
        LatticePredicateNode? predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var budget = maxAttempts ?? DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        // See ScanEntriesAsyncCore: a caller-established system-origin scope OR
        // credential scope is reset by Orleans in this iterator's execution flow
        // after the first physical segment completes, so each must be re-asserted
        // around every reopen or a resumed segment resolves to an anonymous subject
        // and a fail-closed gate silently truncates the scan.
        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
        var reassertCredential = LatticeCredentialContext.Current;

        string? lastKey = null;
        var attempt = 0;

        // Stall resumption. A ScanPageStalledException is a different fault from
        // an EnumerationAbortedException and is resumed on its own budget and its
        // own backoff; see DefaultScanStallResumeAttempts. The resume is gated
        // by that budget alone; see the catch below.
        var stallBudget = ComputeScanStallResumeBudget(budget);
        var stallAttempt = 0;
        var stallDelayMs = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeScanBounds(startInclusive, endExclusive, lastKey, reverse);
            using var originScope = reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;
            using var credentialScope = reassertCredential is { } entryCredential
                ? LatticeCredentialContext.With(entryCredential)
                : null;
            var enumerator = (predicate is null
                ? lattice.KeysAsync(s, e, reverse, prefetch, cancellationToken)
                : lattice.KeysWherePredicateAsync(predicate.Value, s, e, reverse, prefetch, cancellationToken))
                .GetAsyncEnumerator();
            var completedNormally = false;
            var shouldReopen = false;
            try
            {
                while (true)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (EnumerationAbortedException) when (attempt < budget)
                    {
                        attempt++;
                        shouldReopen = true;
                        break;
                    }
                    catch (ScanPageStalledException stall)
                    {
                        // The shard released itself so its queue could drain, and
                        // said so: resuming from the last continuation token is the
                        // recovery the ceiling was designed to enable. Resume while
                        // there is budget - and on budget alone.
                        //
                        // This deliberately does NOT also require the scan to have
                        // advanced since it last stalled. That gate was tried
                        // (issue 2398) and then measured (issue 2456): on the
                        // deployed build it refused 38 of 38 stalls, the budget was
                        // never once consulted, and the resume branch below
                        // executed zero times, so the indexing job still aborted
                        // and re-scanned the whole corpus. The gate is unreachable
                        // for the population that actually occurs - a cold tree
                        // replaying its WAL windows stalls at or near the origin,
                        // where lastKey is still null and no advance can have
                        // happened. Issue 2278 measured leaves read before the
                        // ceiling fired as 0,0,0,0,0,0,1,4,5.
                        //
                        // The gate's stated fear - that a second stall at the same
                        // position re-attacks the same parked read - conflates the
                        // same position with the same conditions. The ceiling
                        // exists precisely so the shard stops being held and its
                        // queue can drain, so after a backoff the position is
                        // unchanged but the shard is not. What bounds the retry is
                        // the budget and the backoff, both already present here.
                        // The gate was a third bound that only ever fired first.
                        //
                        // WHAT THIS DOES NOT DO, recorded here because the metric
                        // it corrects is easy to mistake for a cure. Where the
                        // stall is downstream of the cold WAL replay loop (issues
                        // 2280 and 2433) - a leaf that has never checkpointed and
                        // must replay a WAL window that GC cannot trim because the
                        // materialiser is behind - a resume lands back on the same
                        // leaf in the same state and will exhaust its budget. The
                        // gain there is a correct classification, not a completed
                        // scan: the scan reports budget-exhausted, meaning "tried
                        // and could not", instead of no-progress, meaning "refused
                        // to try". Do not read a fall in no-progress as recovery.
                        //
                        // WHY THE BACKOFF IS NOT ESCALATED FOR AN UNCHANGED
                        // POSITION. Charging a repeated same-position stall extra
                        // backoff was considered and rejected. It assumes the
                        // previous wait was merely too short, which is true of a
                        // transient queue but false of the replay deadlock above,
                        // where no wait of any length helps. Each attempt already
                        // costs a whole ceiling, so on the derived default the
                        // worst case is roughly three ceilings of work plus the
                        // backoff between them; escalating would add most of
                        // another ceiling of pure waiting to the case that cannot
                        // benefit from it. The budget stays deliberately small for
                        // the same reason - see DefaultScanStallResumeAttempts.
                        if (stallAttempt < stallBudget)
                        {
                            stallAttempt++;
                            stallDelayMs = ComputeScanStallResumeDelayMs(stall.TimeoutSeconds, stallAttempt);
                            RecordScanStallOutcome(stall, StallOutcomeResumed);
                            shouldReopen = true;
                            break;
                        }

                        // Out of budget: rethrow the stall verbatim. A scan that
                        // cannot be finished must never look finished, so there is
                        // no path here that ends the enumeration normally.
                        RecordScanStallOutcome(stall, StallOutcomeBudgetExhausted);
                        throw;
                    }

                    if (!hasNext)
                    {
                        completedNormally = true;
                        break;
                    }

                    lastKey = enumerator.Current;
                    yield return enumerator.Current;
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            if (completedNormally)
            {
                yield break;
            }

            if (shouldReopen)
            {
                var delayMs = stallDelayMs > 0 ? stallDelayMs : ComputeReconnectDelayMs(attempt);
                stallDelayMs = 0;
                if (delayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// Resilient forward/reverse entry scan. Wraps <see cref="ILattice.EntriesAsync"/>
    /// with the same <c>EnumerationAbortedException</c> recovery and deterministic
    /// resume semantics as <see cref="ScanKeysAsync"/>. This is the recommended
    /// client API for long-running entry exports.
    /// </summary>
    /// <param name="lattice">The tree to scan.</param>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the tree's lowest key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the tree's end.</param>
    /// <param name="reverse">If <c>true</c>, yields entries in descending key order.</param>
    /// <param name="prefetch">Optional per-call override for shard prefetch; see <see cref="LatticeOptions.PrefetchEntriesScan"/>.</param>
    /// <param name="maxAttempts">Optional per-call override for the reconnect budget; defaults to <see cref="DefaultScanReconnectAttempts"/>.</param>
    /// <param name="cancellationToken">Cancellation token; honoured between reconnects and during backoff.</param>
    public static IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        // See `ScanKeysAsync` for why the null-guard lives in a non-async
        // wrapper rather than inside the iterator core.
        ArgumentNullException.ThrowIfNull(lattice);
        return ScanEntriesAsyncCore(lattice, startInclusive, endExclusive, reverse, prefetch, maxAttempts, null, cancellationToken);
    }

    /// <summary>
    /// Resilient forward/reverse entry scan whose entries are filtered
    /// server-side by the predicate IR <paramref name="predicate"/>. Mirrors
    /// <see cref="ScanEntriesAsync"/>'s recovery, re-supplying the predicate as
    /// an explicit argument on every reconnect.
    /// </summary>
    internal static IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesWhereAsync(
        this ILattice lattice,
        LatticePredicateNode predicate,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        return ScanEntriesAsyncCore(lattice, startInclusive, endExclusive, reverse, prefetch, maxAttempts, predicate, cancellationToken);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsyncCore(
        ILattice lattice,
        string? startInclusive,
        string? endExclusive,
        bool reverse,
        bool? prefetch,
        int? maxAttempts,
        LatticePredicateNode? predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var budget = maxAttempts ?? DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        // A resilient scan emulates one logical, strongly-consistent scan as a
        // sequence of physical EntriesAsync segments, reopening after a transient
        // EnumerationAbortedException (raised, for example, when a concurrent scan
        // over the same activation evicts this enumerator). Each physical segment
        // is a fresh grain call whose server-side authorization identity is
        // resolved from the ambient RequestContext at send time. Both the caller's
        // system-origin scope (see LatticeAccessGateContext.EnterSystemOrigin) and
        // the caller's credential scope (see LatticeCredentialContext.With) live on
        // that same RequestContext, and Orleans resets the caller-established
        // RequestContext in THIS iterator's execution flow once the first segment's
        // call completes, so either scope is lost on every reopen. A resumed
        // segment would then resolve to an anonymous subject; a fail-closed access
        // gate denies its range-read and returns a reject-all key-filter, so the
        // segment completes normally with zero rows and the scan is silently
        // truncated at the resume point. This bites either identity independently:
        // a system-origin infrastructure scan, or a credential-scoped scan such as
        // the repository-context background reconcile (which stamps a fixed run
        // credential but no system-origin). Capture both once and re-assert them
        // around every segment so all segments share one stable identity.
        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
        var reassertCredential = LatticeCredentialContext.Current;

        string? lastKey = null;
        var attempt = 0;

        // See ScanKeysAsyncCore: stalls resume on their own budget and their own
        // backoff, gated by that budget alone.
        var stallBudget = ComputeScanStallResumeBudget(budget);
        var stallAttempt = 0;
        var stallDelayMs = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeScanBounds(startInclusive, endExclusive, lastKey, reverse);
            using var originScope = reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;
            using var credentialScope = reassertCredential is { } entryCredential
                ? LatticeCredentialContext.With(entryCredential)
                : null;
            var enumerator = (predicate is null
                ? lattice.EntriesAsync(s, e, reverse, prefetch, cancellationToken)
                : lattice.EntriesWherePredicateAsync(predicate.Value, s, e, reverse, prefetch, cancellationToken))
                .GetAsyncEnumerator();
            var completedNormally = false;
            var shouldReopen = false;
            try
            {
                while (true)
                {
                    bool hasNext;
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (EnumerationAbortedException) when (attempt < budget)
                    {
                        attempt++;
                        shouldReopen = true;
                        break;
                    }
                    catch (ScanPageStalledException stall)
                    {
                        // See ScanKeysAsyncCore for the reasoning, including why
                        // an unchanged continuation position neither refuses the
                        // resume nor lengthens its backoff.
                        if (stallAttempt < stallBudget)
                        {
                            stallAttempt++;
                            stallDelayMs = ComputeScanStallResumeDelayMs(stall.TimeoutSeconds, stallAttempt);
                            RecordScanStallOutcome(stall, StallOutcomeResumed);
                            shouldReopen = true;
                            break;
                        }

                        RecordScanStallOutcome(stall, StallOutcomeBudgetExhausted);
                        throw;
                    }

                    if (!hasNext)
                    {
                        completedNormally = true;
                        break;
                    }

                    lastKey = enumerator.Current.Key;
                    yield return enumerator.Current;
                }
            }
            finally
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            if (completedNormally)
            {
                yield break;
            }

            if (shouldReopen)
            {
                var delayMs = stallDelayMs > 0 ? stallDelayMs : ComputeReconnectDelayMs(attempt);
                stallDelayMs = 0;
                if (delayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>
    /// Computes the inter-reconnect backoff for a resilient scan. The first
    /// reconnect is immediate (the grain-reactivation cost already dominates
    /// and there is nothing to back off from); subsequent attempts apply a
    /// small linear ramp capped at 100&#160;ms to avoid a tight loop against
    /// a persistently-faulting orchestrator.
    /// </summary>
    private static int ComputeReconnectDelayMs(int attempt) =>
        attempt <= 1 ? 0 : Math.Min(100, 10 * attempt);

    /// <summary>
    /// Computes the resume bounds for a resilient scan given the last successfully
    /// yielded key. Forward scans tighten the lower bound to the successor of
    /// <paramref name="lastKey"/>; reverse scans tighten the upper bound to
    /// <paramref name="lastKey"/> (exclusive).
    /// </summary>
    private static (string? Start, string? End) ComputeScanBounds(
        string? originalStart, string? originalEnd, string? lastKey, bool reverse)
    {
        if (lastKey is null)
        {
            return (originalStart, originalEnd);
        }

        return reverse
            ? (originalStart, lastKey)
            : (lastKey + "\u0000", originalEnd);
    }

    // --- Scoped cursors ---
    //
    // Thin IAsyncDisposable wrappers around the underlying string-id
    // cursor surface. These do not change the durability contract of
    // the cursor grain (the cursor is still server-side and survives
    // a client crash); they only bind the close call to a using-block.
    // Callers that need to persist or share a cursor ID should keep
    // using the raw Open*CursorAsync / CloseCursorAsync shape.

    /// <summary>
    /// Opens a key-enumeration cursor and returns it as an
    /// <see cref="IAsyncDisposable"/> scope. Disposing the scope calls
    /// <see cref="ILattice.CloseCursorAsync(string, CancellationToken)"/>
    /// exactly once. Parameters mirror
    /// <see cref="ILattice.OpenKeyCursorAsync"/>.
    /// </summary>
    public static async Task<LatticeScopedCursor> OpenKeyCursorScopeAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        var cursorId = await lattice.OpenKeyCursorAsync(
            startInclusive, endExclusive, reverse, pointInTime, cancellationToken)
            .ConfigureAwait(false);
        return new LatticeScopedCursor(lattice, cursorId);
    }

    /// <summary>
    /// Opens an entry-enumeration cursor and returns it as an
    /// <see cref="IAsyncDisposable"/> scope. Parameters mirror
    /// <see cref="ILattice.OpenEntryCursorAsync"/>.
    /// </summary>
    public static async Task<LatticeScopedCursor> OpenEntryCursorScopeAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool pointInTime = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        var cursorId = await lattice.OpenEntryCursorAsync(
            startInclusive, endExclusive, reverse, pointInTime, cancellationToken)
            .ConfigureAwait(false);
        return new LatticeScopedCursor(lattice, cursorId);
    }

    /// <summary>
    /// Opens a zero-observable-writes snapshot key cursor and returns
    /// it as an <see cref="IAsyncDisposable"/> scope. Parameters mirror
    /// <see cref="ILattice.OpenSnapshotKeyCursorAsync"/>.
    /// </summary>
    public static async Task<LatticeScopedCursor> OpenSnapshotKeyCursorScopeAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        var cursorId = await lattice.OpenSnapshotKeyCursorAsync(
            startInclusive, endExclusive, reverse, cancellationToken)
            .ConfigureAwait(false);
        return new LatticeScopedCursor(lattice, cursorId);
    }

    /// <summary>
    /// Opens a zero-observable-writes snapshot entry cursor and returns
    /// it as an <see cref="IAsyncDisposable"/> scope. Parameters mirror
    /// <see cref="ILattice.OpenSnapshotEntryCursorAsync"/>.
    /// </summary>
    public static async Task<LatticeScopedCursor> OpenSnapshotEntryCursorScopeAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        var cursorId = await lattice.OpenSnapshotEntryCursorAsync(
            startInclusive, endExclusive, reverse, cancellationToken)
            .ConfigureAwait(false);
        return new LatticeScopedCursor(lattice, cursorId);
    }

    /// <summary>
    /// Opens a resumable range-delete cursor and returns it as an
    /// <see cref="IAsyncDisposable"/> scope. Parameters mirror
    /// <see cref="ILattice.OpenDeleteRangeCursorAsync"/>.
    /// </summary>
    public static async Task<LatticeScopedCursor> OpenDeleteRangeCursorScopeAsync(
        this ILattice lattice,
        string startInclusive,
        string endExclusive,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        var cursorId = await lattice.OpenDeleteRangeCursorAsync(
            startInclusive, endExclusive, cancellationToken)
            .ConfigureAwait(false);
        return new LatticeScopedCursor(lattice, cursorId);
    }

    /// <summary>
    /// Resilient range delete: drives a durable range-delete cursor over
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) to
    /// completion, tombstoning up to <paramref name="stepSize"/> keys per step,
    /// and returns the total number of keys tombstoned across the whole range.
    /// This is the delete-side analogue of <see cref="ScanKeysAsync"/>: it
    /// transparently recovers from <c>Orleans.Runtime.EnumerationAbortedException</c>
    /// (raised when the remote enumerator backing a step is reclaimed mid-drain
    /// due to silo failover, cold start, idle expiry, or scale-down) by opening
    /// a fresh cursor over the same still-live range and resuming, up to
    /// <paramref name="maxAttempts"/> times (default
    /// <see cref="DefaultScanReconnectAttempts"/>, negative clamps to zero). The
    /// first reconnect is immediate; later reconnects apply the same small linear
    /// backoff as the resilient scans. Because tombstoned keys are already gone,
    /// a reopened cursor resumes at the first surviving key with no double
    /// counting, so the returned total reflects keys actually deleted by this
    /// call. A caller-established system-origin scope (see
    /// <c>LatticeAccessGateContext.EnterSystemOrigin</c>) or credential scope (see
    /// <c>LatticeCredentialContext.With</c>) is re-asserted around every step so a
    /// reopened cursor resolves to the same subject a fail-closed gate authorized
    /// on the first step.
    /// <para>
    /// Prefer this over the raw
    /// <see cref="ILattice.OpenDeleteRangeCursorAsync"/> /
    /// <see cref="ILattice.DeleteRangeStepAsync"/> /
    /// <see cref="ILattice.CloseCursorAsync"/> shape when draining a large or
    /// unbounded range that must complete despite transient enumerator loss. The
    /// single-call <see cref="ILattice.DeleteRangeAsync(string, string, CancellationToken)"/>
    /// remains the right choice for short ranges, and the raw cursor shape for
    /// callers that persist a cursor id across a process boundary.
    /// </para>
    /// </summary>
    /// <param name="lattice">The tree to delete from. Not null.</param>
    /// <param name="startInclusive">Inclusive lower bound. Not null.</param>
    /// <param name="endExclusive">Exclusive upper bound. Not null.</param>
    /// <param name="stepSize">Maximum keys to tombstone per step. Must be positive.</param>
    /// <param name="maxAttempts">Reconnect budget override; defaults to
    /// <see cref="DefaultScanReconnectAttempts"/> when null. A negative value is
    /// clamped to zero (no reconnects).</param>
    /// <param name="cancellationToken">Cancels the drain between steps.</param>
    /// <returns>The total number of keys tombstoned across the range.</returns>
    public static async Task<int> DeleteRangeAsync(
        this ILattice lattice,
        string startInclusive,
        string endExclusive,
        int stepSize,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(stepSize);

        var budget = maxAttempts ?? DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;
        var reassertCredential = LatticeCredentialContext.Current;

        var total = 0;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string cursorId;
            using (reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null)
            using (reassertCredential is { } openCredential ? LatticeCredentialContext.With(openCredential) : null)
            {
                cursorId = await lattice
                    .OpenDeleteRangeCursorAsync(startInclusive, endExclusive, cancellationToken)
                    .ConfigureAwait(false);
            }

            var shouldReopen = false;
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    LatticeCursorDeleteProgress progress;
                    try
                    {
                        using (reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null)
                        using (reassertCredential is { } stepCredential ? LatticeCredentialContext.With(stepCredential) : null)
                        {
                            progress = await lattice
                                .DeleteRangeStepAsync(cursorId, stepSize, cancellationToken)
                                .ConfigureAwait(false);
                        }
                    }
                    catch (EnumerationAbortedException) when (attempt < budget)
                    {
                        attempt++;
                        shouldReopen = true;
                        break;
                    }

                    // Deliberately NOT extended to ScanPageStalledException, unlike
                    // the four resilient read scans above (issue 2398). Three
                    // reasons, and the first is decisive on its own:
                    //
                    // 1. There is no continuation token to resume from. A reopen
                    //    here re-issues OpenDeleteRangeCursorAsync with the
                    //    ORIGINAL startInclusive/endExclusive; only `total`, a
                    //    counter, is carried across. That is safe for an aborted
                    //    enumerator because the keys the lost cursor tombstoned are
                    //    already gone, so the reopened cursor lands on the first
                    //    surviving key - but it means a stall retry is a restart
                    //    from the beginning of the surviving range, not a resume.
                    //    The rule the read scans obey is that a scan which can
                    //    resume from a continuation token may retry and one that
                    //    would restart must not; this loop is in the second class.
                    // 2. The exception's own retriability warrant is scoped to
                    //    reads: a page fill is a pure read of a key range, so
                    //    nothing is half-applied when it is abandoned. A delete
                    //    step is not that. Tombstones are idempotent, so a retry is
                    //    probably harmless - but "probably harmless" is not the bar
                    //    for silently swallowing a timeout in a destructive drain.
                    // 3. A stall here is reachable (DeleteRangeBoundedAsync is
                    //    stall-guarded), so leaving it to propagate is a live,
                    //    intended behaviour and not an untested corner: the caller
                    //    sees the stall and decides, which is what a destructive
                    //    operation should do.

                    total += progress.DeletedThisStep;
                    if (progress.IsComplete)
                    {
                        return total;
                    }
                }
            }
            finally
            {
                // Best-effort close. A reclaimed or expired cursor may already be
                // gone; its server-side state self-expires via the cursor idle
                // TTL, so a failure here must not mask the drain result or the
                // in-flight reconnect.
                try
                {
                    await lattice.CloseCursorAsync(cursorId, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // swallow: reopen path or already-reclaimed cursor
                }
            }

            if (shouldReopen)
            {
                var delayMs = ComputeReconnectDelayMs(attempt);
                if (delayMs > 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}

