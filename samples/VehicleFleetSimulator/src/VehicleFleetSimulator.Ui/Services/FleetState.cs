using System.Collections.Concurrent;
using VehicleFleetSimulator.Api.Grpc;

namespace VehicleFleetSimulator.Ui.Services;

/// <summary>
/// Latest-known state for every vehicle that has emitted telemetry on this connection. The
/// renderer reads <see cref="Snapshot"/> to map (from, to, progress) onto the static city layout
/// once per redraw.
/// </summary>
/// <remarks>
/// Telemetry arrives at ~1 Hz per vehicle from a single foreground task per stream, so the
/// thread-safety surface is "single writer, single reader" - we still use a
/// <see cref="ConcurrentDictionary{TKey, TValue}"/> because the WASM threading model can change
/// in the future and the cost is negligible relative to drawing.
/// </remarks>
public sealed class FleetState
{
    private readonly ConcurrentDictionary<string, VehiclePosition> _positions = new();
    private long _version;

    // Cut-off (UTC ticks) for accepting incoming telemetry. Set by Clear() to "now + quiet
    // window"; extended on every rejected message to "now + quiet window" again -- but
    // strictly bounded by SuppressMaxTicks measured from the most recent Clear(). The auto-
    // extend covers the buffered-fan-out drain after a high-load reset (which scales with
    // fleet size), and the hard cap ensures we don't conflate "stale buffered telemetry" with
    // "fresh telemetry from newly-spawned vehicles" -- the latter would otherwise keep the
    // gate shut indefinitely after a preset's reset+spawn sequence.
    private long _applySuppressedUntilTicks;
    private long _clearAtTicks;

    // 1 second of quiet covers a default 200 ms fan-out tick plus typical WASM JSON-decode
    // backpressure with comfortable margin. The 3-second hard cap is the upper bound on how
    // long we'll suppress telemetry after any Clear() -- well above the worst-case drain for
    // the 5,000-vehicle high-load preset and well under the human "click->see dots" budget.
    private const long SuppressQuietTicks = TimeSpan.TicksPerMillisecond * 1000;
    private const long SuppressMaxTicks = TimeSpan.TicksPerMillisecond * 3000;

    public IReadOnlyDictionary<string, VehiclePosition> Snapshot => _positions;

    public int Count => _positions.Count;

    /// <summary>
    /// Monotonic counter incremented on every <see cref="Apply"/> call. Lets the renderer skip
    /// JS interop when no telemetry has arrived since the previous frame -- without this guard,
    /// the C# render loop's fixed-cadence poll aliases against the grain tick cadence and sends
    /// duplicate packets, which the worker's exp-smoothing then converges to a halt against.
    /// </summary>
    public long Version => Interlocked.Read(ref _version);

    public void Apply(TelemetryMessage message)
    {
        // Reject in-flight messages that were buffered server-side before a Clear(). Every
        // rejection pushes the cut-off forward by another quiet window, but never past the
        // hard cap measured from Clear() -- otherwise legitimate telemetry from freshly-
        // spawned vehicles (which begins flowing within ~1 s of a preset's reset) would keep
        // re-extending the gate forever and no dots would ever appear.
        var now = DateTime.UtcNow.Ticks;
        if (now < Volatile.Read(ref _applySuppressedUntilTicks))
        {
            var hardCap = Volatile.Read(ref _clearAtTicks) + SuppressMaxTicks;
            var extended = Math.Min(now + SuppressQuietTicks, hardCap);
            Volatile.Write(ref _applySuppressedUntilTicks, extended);
            return;
        }

        // Parse the Guid once on the telemetry-arrival path. The render loop iterates this
        // dictionary every frame, every vehicle -- doing Guid.TryParse there means N string
        // parses per frame, which scales linearly with the fleet size and shows up as visible
        // stop/start on WASM (single-threaded; the render loop competes with the gRPC
        // `await foreach` consumer for the only thread). Parsing here runs once per arriving
        // message and caches the bytes inline on the position record.
        if (!Guid.TryParse(message.VehicleId, out var guid)) return;

        // The grain ships the configured tank capacity on every telemetry message, so the
        // gradient denominator is correct from the very first packet. We still guard against a
        // zero/missing capacity (older silos, default-constructed messages in tests) by
        // collapsing to 0 fuel rather than dividing by zero -- a flat-red dot is a clearer
        // "unknown" signal than a stuck-green one.
        var capacity = message.TankCapacityLitres;
        var frac = capacity > 0
            ? Math.Clamp(message.FuelLitres / capacity, 0.0, 1.0)
            : 0.0;

        var pos = new VehiclePosition(
            message.VehicleId,
            guid,
            message.FromCityId,
            message.ToCityId,
            message.SegmentLengthKm <= 0 ? 0 : Math.Clamp(message.SegmentProgressKm / message.SegmentLengthKm, 0, 1),
            message.Status,
            frac);
        _positions[message.VehicleId] = pos;
        Interlocked.Increment(ref _version);
    }

    public void Clear()
    {
        // Set the suppression cut-off BEFORE clearing the dictionary. Ordering matters: any
        // Apply() that has already passed the suppress check will either write into _positions
        // (immediately overwritten by Clear() below) or be on a thread that hasn't started yet
        // (which now sees the new cut-off and returns early). Either way we end up empty.
        var now = DateTime.UtcNow.Ticks;
        Volatile.Write(ref _clearAtTicks, now);
        Volatile.Write(ref _applySuppressedUntilTicks, now + SuppressQuietTicks);
        _positions.Clear();
        Interlocked.Increment(ref _version);
    }
}

public readonly record struct VehiclePosition(
    string VehicleId,
    Guid VehicleGuid,
    string FromCityId,
    string ToCityId,
    double Progress,
    VehicleStatus Status,
    double FuelFraction);
