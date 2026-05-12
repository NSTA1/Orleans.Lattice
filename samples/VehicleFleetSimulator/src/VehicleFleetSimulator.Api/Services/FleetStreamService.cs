using Grpc.Core;
using Microsoft.Extensions.ObjectPool;
using VehicleFleetSimulator.Api.Grpc;
using VehicleFleetSimulator.Api.Streams;

namespace VehicleFleetSimulator.Api.Services;

/// <summary>
/// gRPC implementation of <c>FleetStream</c>. Each call registers a bounded subscription with the
/// in-process hub and pumps messages until the client cancels. Drop counts are surfaced via the
/// trailing <c>dropped-count</c> metadata header.
/// </summary>
public sealed class FleetStreamService : FleetStream.FleetStreamBase
{
    private const int ChannelCapacity = 1024;

    // Coalesce up to ~50 ms or 256 messages per outbound frame. At 1000 msg/s with 50 vehicles
    // this collapses 250 wire frames/s into ~20 batches/s, slashing per-message protobuf
    // decode + allocation cost on the single-threaded WebAssembly consumer.
    private const int BatchMaxMessages = 256;
    private const long BatchWindowMs = 50;

    // Pool the per-batch staging list. The hot path is one Get/Return per drain cycle (~20/s
    // per subscriber); without pooling each cycle would allocate a fresh List<TelemetryMessage>
    // plus its backing array.
    private static readonly ObjectPool<List<TelemetryMessage>> BatchListPool =
        new DefaultObjectPool<List<TelemetryMessage>>(new BatchListPolicy(), maximumRetained: 32);

    private readonly IFleetStreamHub _hub;

    public FleetStreamService(IFleetStreamHub hub)
    {
        _hub = hub;
    }

    public override async Task SubscribeTelemetry(
        TelemetryFilter request,
        IServerStreamWriter<TelemetryBatch> responseStream,
        ServerCallContext context)
    {
        using var subscription = _hub.SubscribeTelemetry(request, ChannelCapacity);
        var reader = subscription.Channel.Reader;
        var ct = context.CancellationToken;
        try
        {
            while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                var staging = BatchListPool.Get();
                try
                {
                    var deadline = Environment.TickCount64 + BatchWindowMs;
                    // Drain whatever's already queued; only block on first message via WaitToReadAsync above.
                    while (staging.Count < BatchMaxMessages && reader.TryRead(out var msg))
                    {
                        staging.Add(msg);
                    }
                    // Continue topping up the batch within the time window without blocking longer than necessary.
                    while (staging.Count < BatchMaxMessages)
                    {
                        var remaining = deadline - Environment.TickCount64;
                        if (remaining <= 0) break;
                        using var slice = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        slice.CancelAfter((int)remaining);
                        bool more;
                        try
                        {
                            more = await reader.WaitToReadAsync(slice.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                        {
                            // Window elapsed; flush what we have.
                            break;
                        }
                        if (!more) break;
                        while (staging.Count < BatchMaxMessages && reader.TryRead(out var msg))
                        {
                            staging.Add(msg);
                        }
                    }

                    if (staging.Count == 0) continue;

                    var batch = new TelemetryBatch();
                    batch.Messages.Capacity = staging.Count;
                    batch.Messages.AddRange(staging);
                    await responseStream.WriteAsync(batch).ConfigureAwait(false);
                }
                finally
                {
                    BatchListPool.Return(staging);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal client disconnect.
        }
        finally
        {
            context.ResponseTrailers.Add("dropped-count", subscription.DroppedCount.ToString());
        }
    }

    private sealed class BatchListPolicy : IPooledObjectPolicy<List<TelemetryMessage>>
    {
        // Sized to comfortably hold a typical 50 ms drain at 1000 msg/s without growing.
        // The drain loop hard-caps at BatchMaxMessages so capacity settles at 256 / 512
        // and never grows unboundedly - no need to evict large lists.
        private const int InitialCapacity = 64;

        public List<TelemetryMessage> Create() => new(InitialCapacity);

        public bool Return(List<TelemetryMessage> obj)
        {
            obj.Clear();
            return true;
        }
    }

    public override async Task SubscribeEvents(
        EventFilter request,
        IServerStreamWriter<VehicleEventMessage> responseStream,
        ServerCallContext context)
    {
        using var subscription = _hub.SubscribeEvents(request, ChannelCapacity);
        try
        {
            await foreach (var msg in subscription.Channel.Reader.ReadAllAsync(context.CancellationToken))
            {
                await responseStream.WriteAsync(msg);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            context.ResponseTrailers.Add("dropped-count", subscription.DroppedCount.ToString());
        }
    }
}
