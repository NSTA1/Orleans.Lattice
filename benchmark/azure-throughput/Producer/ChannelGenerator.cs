using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.AzureThroughput.Producer;

internal sealed class ChannelGenerator
{
    private const int ChunkSize = 1024;
    private readonly Guid[] vehicles;
    private readonly string[] keys;
    private readonly int parallelism;
    private readonly Channel<KeyValuePair<string, byte[]>[]> channel =
        Channel.CreateBounded<KeyValuePair<string, byte[]>[]>(new BoundedChannelOptions((1 << 16) / ChunkSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

    public ChannelGenerator(int vehicleCount, int parallelism)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(vehicleCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(parallelism);
        this.parallelism = Math.Min(vehicleCount, parallelism);
        vehicles = new Guid[vehicleCount];
        keys = new string[vehicleCount];
        Span<byte> idBytes = stackalloc byte[16];
        for (var i = 0; i < vehicleCount; i++)
        {
            BitConverter.TryWriteBytes(idBytes[..4], i);
            BitConverter.TryWriteBytes(idBytes.Slice(4, 4), 0xC0FFEE);
            BitConverter.TryWriteBytes(idBytes.Slice(8, 4), 0xDEADBEEF);
            BitConverter.TryWriteBytes(idBytes.Slice(12, 4), 0xCAFEBABE);
            vehicles[i] = new Guid(idBytes);
            keys[i] = vehicles[i].ToString("N");
        }
        Reader = new GeneratorChannelReader(channel.Reader);
    }

    public ChannelReader<KeyValuePair<string, byte[]>> Reader { get; }

    public async Task RunAsync(int tickHz, int duration, bool readOnly, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tickHz);
        ArgumentOutOfRangeException.ThrowIfNegative(duration);
        var stats = Enumerable.Range(0, parallelism).Select(_ => new GeneratorProgress()).ToArray();
        var started = Stopwatch.GetTimestamp();
        var deadline = duration > 0 ? started + (long)(duration * (double)Stopwatch.Frequency) : long.MaxValue;
        var interval = Math.Max(1, 1000 / tickHz) * Stopwatch.Frequency / 1000;
        var jsonOptions = new JsonSerializerOptions { IncludeFields = false, WriteIndented = false };
        Console.WriteLine($"[producer] generatorParallelism={parallelism} vehicles={keys.Length} tickHz={tickHz} readOnly={readOnly}");

        async Task GenerateAsync(int slice)
        {
            var begin = (int)((long)keys.Length * slice / parallelism);
            var end = (int)((long)keys.Length * (slice + 1) / parallelism);
            var next = started;
            var scheduled = started;
            while (!ct.IsCancellationRequested)
            {
                var now = Stopwatch.GetTimestamp();
                if (now >= deadline) break;
                if (now < next)
                {
                    await Task.Delay(TimeSpan.FromSeconds((next - now) / (double)Stopwatch.Frequency), ct).ConfigureAwait(false);
                    continue;
                }
                next = now + interval;
                stats[slice].BeginTick(scheduled, interval);
                scheduled += interval;
                var timestamp = DateTimeOffset.UtcNow;
                for (var offset = begin; offset < end; offset += ChunkSize)
                {
                    ct.ThrowIfCancellationRequested();
                    var chunk = new KeyValuePair<string, byte[]>[Math.Min(ChunkSize, end - offset)];
                    for (var j = 0; j < chunk.Length; j++)
                    {
                        var i = offset + j;
                        var bytes = readOnly ? Array.Empty<byte>() : JsonSerializer.SerializeToUtf8Bytes(
                            new VehicleTelemetryEvent(vehicles[i], timestamp, "A", "B",
                                (i % 100) * 0.5, 100.0, 60.0, 40.0, VehicleStatus.Driving, 50.0), jsonOptions);
                        chunk[j] = new(keys[i], bytes);
                    }
                    if (!channel.Writer.TryWrite(chunk))
                    {
                        stats[slice].BeginWait();
                        try { await channel.Writer.WriteAsync(chunk, ct).ConfigureAwait(false); }
                        finally { stats[slice].EndWait(); }
                    }
                    stats[slice].Sent(chunk.Length);
                }
            }
            stats[slice].Complete();
            ct.ThrowIfCancellationRequested();
        }

        var workers = Task.WhenAll(Enumerable.Range(0, parallelism)
            .Select(i => Task.Run(() => GenerateAsync(i), CancellationToken.None)));
        long lastSent = 0;
        long lastBlocked = 0;
        var lastReport = started;
        void Report(bool final)
        {
            var snapshots = stats.Select(s => s.Snapshot()).ToArray();
            var now = Stopwatch.GetTimestamp();
            var sent = snapshots.Sum(s => s.Sent);
            var blocked = snapshots.Sum(s => s.Blocked);
            var elapsed = (now - started) / (double)Stopwatch.Frequency;
            var rate = (sent - lastSent) * (double)Stopwatch.Frequency / Math.Max(1, now - lastReport);
            var fraction = final
                ? blocked / (double)Math.Max(1, (now - started) * parallelism)
                : (blocked - lastBlocked) / (double)Math.Max(1, (now - lastReport) * parallelism);
            var slipMs = snapshots.Max(s => s.Slip) * 1000.0 / Stopwatch.Frequency;
            Console.WriteLine(final
                ? FormattableString.Invariant($"[producer] DONE total={sent:N0} elapsed={elapsed:0.000}s avg={sent / Math.Max(0.001, elapsed):N0} msg/s genBlockedFrac={Math.Clamp(fraction, 0, 1):0.000} slipMaxMs={slipMs:0.0}")
                : FormattableString.Invariant($"[producer] t={elapsed:0.0}s sent={sent:N0} rate={rate:N0} msg/s genBlockedFrac={Math.Clamp(fraction, 0, 1):0.000} slipMaxMs={slipMs:0.0}"));
            lastSent = sent;
            lastBlocked = blocked;
            lastReport = now;
        }

        try
        {
            while (!workers.IsCompleted)
            {
                await Task.WhenAny(workers, Task.Delay(1000, CancellationToken.None)).ConfigureAwait(false);
                if (!workers.IsCompleted) Report(false);
            }
            await workers.ConfigureAwait(false);
            channel.Writer.TryComplete();
            Report(true);
        }
        catch (Exception ex)
        {
            channel.Writer.TryComplete(ex);
            throw;
        }
    }
}
