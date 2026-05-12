using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Http;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Api.Services;

/// <summary>
/// Lightweight server-sent-events (SSE) pub-sub for "things changed" pings used by the Control
/// flyout. Deliberately separate from the gRPC events stream because:
///
/// 1. ConfigChanged / CityMoved are operator events, not vehicle events; piping them through the
///    existing <see cref="VehicleEvent"/> hierarchy would mean adding new proto messages, mapping
///    code on both ends, and breaking the assumption that <c>VehicleEventMessage.VehicleId</c> is
///    always meaningful.
/// 2. SSE is trivial to consume from JS (browser) and from <c>HttpClient</c> in .NET - no client
///    libraries required.
///
/// Each connected client owns a small bounded channel; full channels drop the oldest payload, which
/// is fine for "the config changed" pings (the latest value is always correct).
/// </summary>
public sealed class SimulationEventBroadcaster
{
    private readonly ConcurrentDictionary<Guid, Channel<string>> _clients = new();

    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    /// <summary>Stream events to a single SSE client until <paramref name="ct"/> is cancelled
    /// (typically by the client disconnecting). Caller is responsible for setting the
    /// <c>Content-Type: text/event-stream</c> header before invoking.</summary>
    public async Task WriteToAsync(HttpResponse response, CancellationToken ct)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(64)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });
        _clients[id] = channel;

        try
        {
            // Initial comment line keeps proxies from holding the response. Browsers treat lines
            // starting with ':' as SSE comments.
            await response.WriteAsync(": connected\n\n", ct).ConfigureAwait(false);
            await response.Body.FlushAsync(ct).ConfigureAwait(false);

            // Periodic keep-alive ping so the connection survives idle proxies (Cloudflare, nginx
            // default to 60s idle close). 20s is well under that and cheap.
            using var heartbeat = new CancellationTokenSource();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, heartbeat.Token);
            var heartbeatTask = Task.Run(async () =>
            {
                try
                {
                    while (!linked.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(20), linked.Token).ConfigureAwait(false);
                        channel.Writer.TryWrite(":hb\n\n");
                    }
                }
                catch (OperationCanceledException) { }
            }, linked.Token);

            await foreach (var line in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                await response.WriteAsync(line, ct).ConfigureAwait(false);
                await response.Body.FlushAsync(ct).ConfigureAwait(false);
            }

            heartbeat.Cancel();
            try { await heartbeatTask.ConfigureAwait(false); } catch { }
        }
        catch (OperationCanceledException) { /* client disconnect */ }
        finally
        {
            _clients.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }

    public void PublishConfigChanged(SimulationConfig cfg)
    {
        // Send the relevant fields only; the UI doesn't need the full record (and serialising
        // VehicleConfig defaults inflates the payload tenfold for a notification).
        var payload = JsonSerializer.Serialize(new
        {
            tickIntervalMs = cfg.TickInterval.TotalMilliseconds,
            timeScale = cfg.TimeScale,
            isPaused = cfg.IsPaused,
        }, JsonOpts);
        Broadcast("config-changed", payload);
    }

    public void PublishCityMoved(string cityId, double x, double y)
    {
        var payload = JsonSerializer.Serialize(new { cityId, x, y }, JsonOpts);
        Broadcast("city-moved", payload);
    }

    private void Broadcast(string eventName, string jsonPayload)
    {
        // SSE frame: event:<name>\n data:<payload>\n\n
        var line = $"event: {eventName}\ndata: {jsonPayload}\n\n";
        foreach (var ch in _clients.Values)
        {
            ch.Writer.TryWrite(line);
        }
    }
}
