using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Best-effort publisher for <see cref="LatticeTreeEvent"/> notifications.
/// <para>
/// Callers are responsible for gating on the effective per-tree
/// <c>PublishEvents</c> flag via <see cref="PublishEventsGate"/> (which
/// combines the <see cref="State.TreeRegistryEntry.PublishEvents"/> override
/// and <see cref="LatticeOptions.PublishEvents"/>) before invoking
/// <see cref="PublishAsync"/>. When invoked, the stream provider named
/// <see cref="LatticeOptions.EventStreamProviderName"/> is resolved via
/// keyed-service DI and a metadata-only event is pushed on the per-tree
/// stream (namespace <see cref="LatticeEventConstants.StreamNamespace"/>).
/// </para>
/// <para>
/// <b>Failures are never propagated to the caller.</b> Missing-provider,
/// serialization, and downstream queue exceptions are logged at <c>Warning</c>
/// level and swallowed - the write path must never fail because nobody is
/// listening. Subscribers that never see an event should investigate via the
/// </para>
/// </summary>
internal static class LatticeEventPublisher
{
    /// <summary>
    /// Publishes <paramref name="evt"/> in a fire-and-forget fashion. Returns a
    /// task that <em>never faults</em> (inner exceptions are logged and
    /// swallowed). Callers must already have verified that publication is
    /// enabled for the target tree via <see cref="PublishEventsGate"/>.
    /// </summary>
    public static Task PublishAsync(
        IServiceProvider services,
        LatticeOptions options,
        LatticeTreeEvent evt,
        ILogger? logger = null)
    {
        try
        {
            var provider = services.GetKeyedService<IStreamProvider>(options.EventStreamProviderName);
            if (provider is null)
            {
                logger?.LogWarning(
                    "Lattice event publication skipped: no Orleans stream provider named '{ProviderName}' is registered on this silo. Register one via siloBuilder.AddMemoryStreams / AddEventHubStreams / etc., or disable LatticeOptions.PublishEvents.",
                    options.EventStreamProviderName);
                LatticeMetrics.EventsDropped.Add(1,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, evt.TreeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagReason, "missing_provider"),
                    LatticeTenantLabel.ForTree(evt.TreeId));
                return Task.CompletedTask;
            }

            var stream = provider.GetStream<LatticeTreeEvent>(
                StreamId.Create(LatticeEventConstants.StreamNamespace, evt.TreeId));

            return InvokeAsync(
                stream,
                evt,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, evt.TreeId),
                LatticeTenantLabel.ForTree(evt.TreeId),
                logger);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Lattice event publication threw synchronously for tree {TreeId} kind {Kind}.", evt.TreeId, evt.Kind);
            LatticeMetrics.EventsDropped.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, evt.TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagReason, "publish_error"),
                LatticeTenantLabel.ForTree(evt.TreeId));
            return Task.CompletedTask;
        }
    }

    private static async Task InvokeAsync(
        IAsyncStream<LatticeTreeEvent> stream,
        LatticeTreeEvent evt,
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag,
        ILogger? logger)
    {
        try
        {
            await stream.OnNextAsync(evt);
            LatticeMetrics.EventsPublished.Add(1,
                treeTag,
                new KeyValuePair<string, object?>(LatticeMetrics.TagKind, evt.Kind.ToString()),
                tenantTag);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Lattice event publication failed for tree {TreeId} kind {Kind}.", evt.TreeId, evt.Kind);
            LatticeMetrics.EventsDropped.Add(1,
                treeTag,
                new KeyValuePair<string, object?>(LatticeMetrics.TagReason, "publish_error"),
                tenantTag);
        }
    }

    /// <summary>
    /// Convenience constructor for a <see cref="LatticeTreeEvent"/> that also
    /// reads the ambient <c>operationId</c> (if any) out of Orleans
    /// <see cref="RequestContext"/> so saga-originated writes carry their
    /// correlation id verbatim.
    /// </summary>
    public static LatticeTreeEvent CreateEvent(
        LatticeTreeEventKind kind,
        string treeId,
        string? key = null,
        int? shardIndex = null)
    {
        var opId = RequestContext.Get(LatticeEventConstants.OperationIdRequestContextKey) as string;
        return new LatticeTreeEvent
        {
            Kind = kind,
            TreeId = treeId,
            Key = key,
            ShardIndex = shardIndex,
            OperationId = opId,
            AtUtc = DateTimeOffset.UtcNow,
        };
    }

    /// <summary>
    /// Resolves everything a publication wave shares - the keyed stream
    /// provider, the per-tree stream handle, the ambient
    /// <c>operationId</c>, and the tree/tenant metric tags - exactly
    /// <em>once</em>, returning a <see cref="BatchPublisher"/> that pushes
    /// individual events onto the already-resolved stream.
    /// <para>
    /// The per-event <see cref="PublishAsync"/> entry point re-does all of
    /// that work for every event even when the whole batch targets one tree
    /// and one stream: a keyed-service DI resolution, a
    /// <see cref="StreamId.Create"/> (which UTF8-encodes the namespace and
    /// key into fresh byte arrays), a stream-handle construction, and a
    /// <see cref="RequestContext"/> read. Batch callers that publish one
    /// event per written entry pay all of it N times for an identical
    /// result. This seam hoists it to once per batch; only the per-event
    /// <c>IAsyncStream&lt;T&gt;.OnNextAsync</c> remains in the loop.
    /// </para>
    /// <para>
    /// Per-event metric semantics are unchanged: every
    /// <see cref="BatchPublisher.PublishAsync"/> call still records exactly
    /// one <c>EventsPublished</c> or <c>EventsDropped</c> increment. Only the
    /// provider-resolution warning is logged once per batch rather than once
    /// per event, because it reports a silo-wide misconfiguration that cannot
    /// differ between entries.
    /// </para>
    /// </summary>
    public static BatchPublisher CreateBatch(
        IServiceProvider services,
        LatticeOptions options,
        string treeId,
        ILogger? logger = null)
    {
        try
        {
            var provider = services.GetKeyedService<IStreamProvider>(options.EventStreamProviderName);
            if (provider is null)
            {
                logger?.LogWarning(
                    "Lattice event publication skipped: no Orleans stream provider named '{ProviderName}' is registered on this silo. Register one via siloBuilder.AddMemoryStreams / AddEventHubStreams / etc., or disable LatticeOptions.PublishEvents.",
                    options.EventStreamProviderName);
                return new BatchPublisher(null, treeId, "missing_provider", logger);
            }

            var stream = provider.GetStream<LatticeTreeEvent>(
                StreamId.Create(LatticeEventConstants.StreamNamespace, treeId));
            return new BatchPublisher(stream, treeId, dropReason: null, logger);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Lattice event publication threw synchronously for tree {TreeId}.", treeId);
            return new BatchPublisher(null, treeId, "publish_error", logger);
        }
    }

    /// <summary>
    /// Largest number of per-entry publishes <see cref="PublishManyAsync"/>
    /// keeps in flight at once.
    /// <para>
    /// The window exists because this fan-out is bounded by <i>request size</i>,
    /// not by a routing constant: a caller may hand a single
    /// <c>SetManyAsync</c> tens of thousands of entries, and an unbounded
    /// <see cref="Task.WhenAll(IEnumerable{Task})"/> over that would hand the
    /// stream provider the entire batch at once. A fixed window collapses the
    /// round trips (which is the point) while keeping the peak in-flight count
    /// a property of this constant rather than of whatever the caller passed.
    /// </para>
    /// </summary>
    internal const int PublishWindow = 32;

    /// <summary>
    /// Publishes one <paramref name="kind"/> event per item in
    /// <paramref name="items"/>, keyed by <paramref name="keySelector"/>, with
    /// at most <see cref="PublishWindow"/> publishes in flight at a time.
    /// <para>
    /// Every publish is independent: the events of one batch carry distinct
    /// keys, and <see cref="BatchPublisher.PublishAsync"/> returns a task that
    /// never faults, so a window can be awaited with
    /// <see cref="Task.WhenAll(IEnumerable{Task})"/> without the "one fault
    /// strands the rest" hazard that makes a naive concurrent fan-out unsafe.
    /// </para>
    /// <para>
    /// Publication order within a batch is deliberately not preserved. Lattice
    /// events are documented as not totally ordered, even per key (see
    /// <c>docs/lattice/events.md</c>): they are published by the tree's front
    /// end after the write returns rather than by the shard that serialised it,
    /// and delivery order from there is the stream provider's. The entries of
    /// one batch were applied together, so there is no per-batch write order
    /// for this loop to have preserved.
    /// </para>
    /// </summary>
    /// <typeparam name="T">Batch item type.</typeparam>
    /// <param name="batch">Batch publisher resolved by <see cref="CreateBatch"/>.</param>
    /// <param name="kind">Event kind to publish for every item.</param>
    /// <param name="items">Batch items, one event each.</param>
    /// <param name="keySelector">Projects an item to the key its event carries.</param>
    internal static async Task PublishManyAsync<T>(
        BatchPublisher batch,
        LatticeTreeEventKind kind,
        IReadOnlyList<T> items,
        Func<T, string> keySelector)
    {
        var count = items.Count;
        if (count == 0)
        {
            return;
        }

        if (count == 1)
        {
            // The dominant single-entry case keeps its original shape: no
            // window list is allocated for a batch that cannot benefit.
            await batch.PublishAsync(kind, keySelector(items[0])).ConfigureAwait(false);
            return;
        }

        var window = new List<Task>(Math.Min(PublishWindow, count));
        for (var i = 0; i < count; i++)
        {
            window.Add(batch.PublishAsync(kind, keySelector(items[i])));
            if (window.Count == PublishWindow)
            {
                await Task.WhenAll(window).ConfigureAwait(false);
                window.Clear();
            }
        }

        if (window.Count > 0)
        {
            await Task.WhenAll(window).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Batch-scoped view over an already-resolved per-tree event stream. Created
    /// by <see cref="CreateBatch"/>; see that method for why the batch shape
    /// exists. Failures are swallowed exactly as they are for the per-event
    /// <see cref="PublishAsync"/> path - the write path must never fail because
    /// nobody is listening.
    /// </summary>
    public readonly struct BatchPublisher
    {
        private readonly IAsyncStream<LatticeTreeEvent>? _stream;
        private readonly string _treeId;
        private readonly string? _operationId;
        private readonly string? _dropReason;
        private readonly ILogger? _logger;
        private readonly KeyValuePair<string, object?> _treeTag;
        private readonly KeyValuePair<string, object?> _tenantTag;

        internal BatchPublisher(
            IAsyncStream<LatticeTreeEvent>? stream,
            string treeId,
            string? dropReason,
            ILogger? logger)
        {
            _stream = stream;
            _treeId = treeId;
            _dropReason = dropReason;
            _logger = logger;
            _operationId = RequestContext.Get(LatticeEventConstants.OperationIdRequestContextKey) as string;
            _treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
            _tenantTag = LatticeTenantLabel.ForTree(treeId);
        }

        /// <summary>
        /// Publishes one event on the batch's pre-resolved stream. Returns a task
        /// that never faults.
        /// </summary>
        public Task PublishAsync(LatticeTreeEventKind kind, string? key = null, int? shardIndex = null)
        {
            if (_stream is null)
            {
                LatticeMetrics.EventsDropped.Add(1,
                    _treeTag,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagReason, _dropReason ?? "publish_error"),
                    _tenantTag);
                return Task.CompletedTask;
            }

            var evt = new LatticeTreeEvent
            {
                Kind = kind,
                TreeId = _treeId,
                Key = key,
                ShardIndex = shardIndex,
                OperationId = _operationId,
                AtUtc = DateTimeOffset.UtcNow,
            };

            return InvokeAsync(_stream, evt, _treeTag, _tenantTag, _logger);
        }
    }
}
