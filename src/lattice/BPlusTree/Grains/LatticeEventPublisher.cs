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
/// stream (namespace <see cref="LatticeEventConstants.StreamNamespace"/>,
/// </para>
/// <para>
/// <b>Failures are never propagated to the caller.</b> Missing-provider,
/// serialization, and downstream queue exceptions are logged at <c>Warning</c>
/// level and swallowed — the write path must never fail because nobody is
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
                    new KeyValuePair<string, object?>(LatticeMetrics.TagReason, "missing_provider"));
                return Task.CompletedTask;
            }

            var stream = provider.GetStream<LatticeTreeEvent>(
                StreamId.Create(LatticeEventConstants.StreamNamespace, evt.TreeId));

            return InvokeAsync(stream, evt, logger);
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Lattice event publication threw synchronously for tree {TreeId} kind {Kind}.", evt.TreeId, evt.Kind);
            LatticeMetrics.EventsDropped.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, evt.TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagReason, "publish_error"));
            return Task.CompletedTask;
        }
    }

    private static async Task InvokeAsync(IAsyncStream<LatticeTreeEvent> stream, LatticeTreeEvent evt, ILogger? logger)
    {
        try
        {
            await stream.OnNextAsync(evt);
            LatticeMetrics.EventsPublished.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, evt.TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagKind, evt.Kind.ToString()));
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Lattice event publication failed for tree {TreeId} kind {Kind}.", evt.TreeId, evt.Kind);
            LatticeMetrics.EventsDropped.Add(1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, evt.TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagReason, "publish_error"));
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
}
