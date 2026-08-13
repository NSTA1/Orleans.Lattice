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

        // Per-physical-shard buffers, in-flight tasks, chunk counters, and grain
        // proxies. Keyed by physical shard index so sparse maps (post-split)
        // don't allocate empty slots for non-existent shards. Proxies are cached
        // once so repeated chunk flushes don't rebuild grain keys or re-hit
        // the grain factory's lookup table.
        var capacity = physicalShards.Count;
        var buffers = new Dictionary<int, List<KeyValuePair<string, byte[]>>>(capacity);
        var inFlight = new Dictionary<int, Task>(capacity);
        var chunkCounters = new Dictionary<int, int>(capacity);
        var shards = new Dictionary<int, IShardRootGrain>(capacity);
        var batchId = Guid.NewGuid().ToString("N");
        foreach (var idx in physicalShards)
        {
            buffers[idx] = new(chunkSize);
            inFlight[idx] = Task.CompletedTask;
            chunkCounters[idx] = 0;
            shards[idx] = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{idx}");
        }

        await foreach (var entry in sortedEntries.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shardIdx = shardMap.Resolve(entry.Key);
            var buffer = buffers[shardIdx];
            buffer.Add(entry);

            if (buffer.Count >= chunkSize)
            {
                // Wait for the previous flush to this shard to complete (preserves ordering).
                await inFlight[shardIdx];

                var opId = $"{batchId}-{shardIdx}-{chunkCounters[shardIdx]++}";
                inFlight[shardIdx] = shards[shardIdx].BulkAppendAsync(opId, buffer);
                buffers[shardIdx] = new(chunkSize);
            }
        }

        // Flush remaining buffers.
        var finalTasks = new List<Task>(capacity);
        foreach (var idx in physicalShards)
        {
            if (buffers[idx].Count > 0)
            {
                // Wait for previous in-flight for this shard, then flush.
                await inFlight[idx];
                var opId = $"{batchId}-{idx}-{chunkCounters[idx]++}";
                finalTasks.Add(shards[idx].BulkAppendAsync(opId, buffers[idx]));
            }
            else
            {
                finalTasks.Add(inFlight[idx]);
            }
        }

        await Task.WhenAll(finalTasks);
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

        // See ScanEntriesAsyncCore: a caller-established system-origin scope is
        // reset by Orleans in this iterator's execution flow after the first
        // physical segment completes, so it must be re-asserted around every
        // reopen or a resumed segment resolves to an anonymous subject and a
        // fail-closed gate silently truncates the scan.
        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;

        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeScanBounds(startInclusive, endExclusive, lastKey, reverse);
            using var originScope = reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;
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
                var delayMs = ComputeReconnectDelayMs(attempt);
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
        // resolved from the ambient RequestContext at send time. When the caller
        // wraps the whole scan in a system-origin scope (see
        // LatticeAccessGateContext.EnterSystemOrigin), Orleans resets the
        // caller-established RequestContext in THIS iterator's execution flow once
        // the first segment's call completes, so the scope is lost on every
        // reopen. A resumed segment would then resolve to an anonymous subject; a
        // fail-closed access gate denies its range-read and returns a reject-all
        // key-filter, so the segment completes normally with zero rows and the
        // scan is silently truncated at the resume point. Capture the caller's
        // system-origin intent once and re-assert it around every segment so all
        // segments share one stable identity.
        var reassertSystemOrigin = LatticeAccessGateContext.IsSystemOrigin;

        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeScanBounds(startInclusive, endExclusive, lastKey, reverse);
            using var originScope = reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null;
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
                var delayMs = ComputeReconnectDelayMs(attempt);
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
    /// <c>LatticeAccessGateContext.EnterSystemOrigin</c>) is re-asserted around
    /// every step so a reopened cursor resolves to the same subject a fail-closed
    /// gate authorized on the first step.
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

        var total = 0;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string cursorId;
            using (reassertSystemOrigin ? LatticeAccessGateContext.EnterSystemOrigin() : null)
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

