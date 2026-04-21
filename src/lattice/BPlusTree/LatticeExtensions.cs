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
    /// Legacy streaming bulk-load overload that accepts an explicit shard
    /// count and routes via the legacy <c>XxHash32(key) % shardCount</c>
    /// formula, bypassing the per-tree <see cref="ShardMap"/>. Use the
    /// overload without <paramref name="shardCount"/> instead — it resolves
    /// the tree's persisted shard map automatically and routes correctly
    /// for trees with non-default maps (e.g. after an adaptive shard split).
    /// </summary>
    /// <param name="lattice">The tree to load into.</param>
    /// <param name="sortedEntries">Entries in ascending key order.</param>
    /// <param name="grainFactory">The grain factory (needed to address shard grains directly).</param>
    /// <param name="shardCount">Number of shards (must match <see cref="LatticeOptions.ShardCount"/>).</param>
    /// <param name="chunkSize">Max entries per shard before flushing (default 10 000).</param>
    /// <param name="cancellationToken">Cancellation token checked between entry enqueues and between flushes.</param>
    [Obsolete("Use the overload without 'shardCount'; this one bypasses the per-tree ShardMap and will mis-route entries on trees with non-default maps.")]
    public static async Task BulkLoadAsync(
        this ILattice lattice,
        IAsyncEnumerable<KeyValuePair<string, byte[]>> sortedEntries,
        IGrainFactory grainFactory,
        int shardCount,
        int chunkSize = 10_000,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(sortedEntries);
        ArgumentNullException.ThrowIfNull(grainFactory);
        cancellationToken.ThrowIfCancellationRequested();

        var treeId = lattice.GetPrimaryKeyString();

        // Per-shard buffers, in-flight tasks, and chunk counters for operation IDs.
        var buffers = new List<KeyValuePair<string, byte[]>>[shardCount];
        var inFlight = new Task[shardCount];
        var chunkCounters = new int[shardCount];
        var batchId = Guid.NewGuid().ToString("N");
        for (int i = 0; i < shardCount; i++)
        {
            buffers[i] = new(chunkSize);
            inFlight[i] = Task.CompletedTask;
        }

        await foreach (var entry in sortedEntries.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shardIdx = LatticeSharding.GetShardIndex(entry.Key, shardCount);
            buffers[shardIdx].Add(entry);

            if (buffers[shardIdx].Count >= chunkSize)
            {
                // Wait for the previous flush to this shard to complete (preserves ordering).
                await inFlight[shardIdx];

                var shard = grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{shardIdx}");
                var chunk = buffers[shardIdx];
                var opId = $"{batchId}-{shardIdx}-{chunkCounters[shardIdx]++}";
                inFlight[shardIdx] = shard.BulkAppendAsync(opId, chunk);
                buffers[shardIdx] = new(chunkSize);
            }
        }

        // Flush remaining buffers.
        var finalTasks = new List<Task>();
        for (int i = 0; i < shardCount; i++)
        {
            if (buffers[i].Count > 0)
            {
                // Wait for previous in-flight for this shard, then flush.
                await inFlight[i];
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}");
                var opId = $"{batchId}-{i}-{chunkCounters[i]++}";
                finalTasks.Add(shard.BulkAppendAsync(opId, buffers[i]));
            }
            else
            {
                finalTasks.Add(inFlight[i]);
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
}
