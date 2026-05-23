// Azure throughput benchmark harness - workload producer.
//
// Streams synthetic VehicleTelemetryEvent samples as JSON lines over TCP to a single-silo
// lattice host. Deliberately minimal: no city-graph routing, no per-vehicle grain - just a
// flat fleet of vehicle IDs each emitting samples at a fixed tick rate, serialised inline
// and pushed through a buffered socket.
//
// Environment variables:
//   BENCH_VEHICLE_COUNT   number of synthetic vehicles (default 1000)
//   BENCH_TICK_HZ         per-vehicle samples per second (default 5)
//   BENCH_SILO_HOST       silo TCP host (default 127.0.0.1)
//   BENCH_SILO_PORT       silo TCP port (default 7000)
//   BENCH_DURATION_SEC    run duration in seconds; 0 = run forever (default 300)
//
// The producer reports its own outbound rate to stdout once per second so a wedged producer
// is distinguishable from a wedged silo when reading ACI logs.

using System.Buffers;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using VehicleFleetSimulator.Abstractions;

// Force autoflush on stdout/stderr. In a Linux container with stdout
// redirected, .NET's default `Console.Out` is a buffered StreamWriter
// and per-second progress lines sit in a 4-KiB buffer for tens of
// seconds before the container log driver sees them. See the matching
// note in Silo/Program.cs.
Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var vehicleCount = ReadInt("BENCH_VEHICLE_COUNT", 1000);
var tickHz       = ReadInt("BENCH_TICK_HZ", 5);
var siloHost     = Environment.GetEnvironmentVariable("BENCH_SILO_HOST") ?? "127.0.0.1";
var siloPort     = ReadInt("BENCH_SILO_PORT", 7000);
var duration     = ReadInt("BENCH_DURATION_SEC", 300);

Console.WriteLine($"[producer] vehicles={vehicleCount} tickHz={tickHz} silo={siloHost}:{siloPort} duration={duration}s");

// Generate stable vehicle IDs so restarts produce the same key distribution.
var vehicles = new Guid[vehicleCount];
Span<byte> idBytes = stackalloc byte[16];
for (var i = 0; i < vehicleCount; i++)
{
    BitConverter.TryWriteBytes(idBytes[..4], i);
    BitConverter.TryWriteBytes(idBytes.Slice(4, 4), 0xC0FFEE);
    BitConverter.TryWriteBytes(idBytes.Slice(8, 4), 0xDEADBEEF);
    BitConverter.TryWriteBytes(idBytes.Slice(12, 4), 0xCAFEBABE);
    vehicles[i] = new Guid(idBytes);
}

// Reconnect loop: the silo container may take a few seconds to start its listener.
using var client = new TcpClient { NoDelay = true };
for (var attempt = 1; ; attempt++)
{
    try
    {
        await client.ConnectAsync(siloHost, siloPort);
        break;
    }
    catch (SocketException ex) when (attempt < 60)
    {
        Console.WriteLine($"[producer] connect attempt {attempt} failed ({ex.SocketErrorCode}); retrying in 1s");
        await Task.Delay(1000);
    }
}
Console.WriteLine("[producer] connected");

await using var stream = client.GetStream();
var writer = new BufferedStream(stream, bufferSize: 64 * 1024);

var jsonOpts = new JsonSerializerOptions { IncludeFields = false, WriteIndented = false };
var newline = (byte)'\n';

var startedAt = Stopwatch.GetTimestamp();
var deadlineTicks = duration > 0
    ? Stopwatch.GetTimestamp() + (long)(duration * (double)Stopwatch.Frequency)
    : long.MaxValue;

var tickIntervalMs = Math.Max(1, 1000 / tickHz);
var nextTick = DateTimeOffset.UtcNow;

long totalSent = 0;
long sentSinceReport = 0;
var lastReport = Stopwatch.GetTimestamp();

var pool = ArrayPool<byte>.Shared;

while (Stopwatch.GetTimestamp() < deadlineTicks)
{
    var now = DateTimeOffset.UtcNow;
    if (now < nextTick)
    {
        var wait = (int)Math.Max(1, (nextTick - now).TotalMilliseconds);
        await Task.Delay(wait);
        continue;
    }
    nextTick = now + TimeSpan.FromMilliseconds(tickIntervalMs);

    for (var i = 0; i < vehicles.Length; i++)
    {
        var ev = new VehicleTelemetryEvent(
            VehicleId: vehicles[i],
            TimestampUtc: now,
            FromCityId: "A",
            ToCityId: "B",
            SegmentProgressKm: (i % 100) * 0.5,
            SegmentLengthKm: 100.0,
            SpeedKph: 60.0,
            FuelLitres: 40.0,
            Status: VehicleStatus.Driving,
            FuelCapacityLitres: 50.0);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(ev, jsonOpts);
        writer.Write(bytes, 0, bytes.Length);
        writer.WriteByte(newline);

        totalSent++;
        sentSinceReport++;
    }

    var sinceReport = Stopwatch.GetTimestamp() - lastReport;
    if (sinceReport >= Stopwatch.Frequency)
    {
        await writer.FlushAsync();
        var rate = sentSinceReport / (sinceReport / (double)Stopwatch.Frequency);
        var elapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
        Console.WriteLine($"[producer] t={elapsed,7:0.0}s sent={totalSent,12:N0} rate={rate,10:N0} msg/s");
        sentSinceReport = 0;
        lastReport = Stopwatch.GetTimestamp();
    }
}

await writer.FlushAsync();
var totalElapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
Console.WriteLine($"[producer] DONE total={totalSent:N0} elapsed={totalElapsed:0.0}s avg={totalSent / Math.Max(0.001, totalElapsed):N0} msg/s");

static int ReadInt(string name, int @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    return int.TryParse(raw, out var v) && v > 0 ? v : @default;
}
