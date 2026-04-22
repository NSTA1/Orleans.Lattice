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
    /// its entries in key order via <see cref="IShardRootGrain.BulkAppendAsync"/>,

/// which appends to the right edge without splits.
    /// <para>
    /// The input <paramref name="sortedEntries"/> <b>must</b> be in ascending key order.
    /// Per-shard ordering is preserved because hash-partitioning a globally sorted
    /// stream preserves the relative order within each partition.
    /// </para>
    /// <para>
    /// Routing is resolved up front via <see cref="ILattice.GetRoutingAsync"/>,

/// so entries are correctly partitioned by the tree's persisted
    /// <see cref="ShardMap"/> — including non-default maps produced by adaptive
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
    /// stream provider registered. Events are metadata-only — they carry
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
    /// propagate back into the Orleans stream pipeline — wrap in a try/catch if
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
    /// The wrapper tracks the last yielded key and — on abort — reopens the
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
    /// scans — <see cref="ILattice.KeysAsync"/> is retained for short,
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
    public static async IAsyncEnumerable<string> ScanKeysAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        var budget = maxAttempts ?? DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeScanBounds(startInclusive, endExclusive, lastKey, reverse);
            var enumerator = lattice.KeysAsync(s, e, reverse, prefetch, cancellationToken).GetAsyncEnumerator();
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
    public static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ScanEntriesAsync(
        this ILattice lattice,
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        bool? prefetch = null,
        int? maxAttempts = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        var budget = maxAttempts ?? DefaultScanReconnectAttempts;
        if (budget < 0) budget = 0;

        string? lastKey = null;
        var attempt = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (s, e) = ComputeScanBounds(startInclusive, endExclusive, lastKey, reverse);
            var enumerator = lattice.EntriesAsync(s, e, reverse, prefetch, cancellationToken).GetAsyncEnumerator();
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
}
