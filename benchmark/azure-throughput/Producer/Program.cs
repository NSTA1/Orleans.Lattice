// Azure throughput benchmark harness - workload producer.
//
// Streams synthetic VehicleTelemetryEvent samples as JSON lines over TCP to a single-silo
// lattice host. Deliberately minimal: no city-graph routing, no per-vehicle grain - just a
// flat fleet of vehicle IDs each emitting samples at a fixed tick rate, serialised inline
// and pushed through a buffered socket.
//
// Environment variables:
//   BENCH_PRODUCER_MODE  tcp (default) or orleans-client
//   BENCH_VEHICLE_COUNT  number of synthetic vehicles (default 1000)
//   BENCH_TICK_HZ        per-vehicle samples per second (default 5)
//   BENCH_SILO_HOST      silo TCP host (default 127.0.0.1)
//   BENCH_SILO_PORT      silo TCP port (default 7000)
//   BENCH_DURATION_SEC   run duration in seconds; 0 = run forever (default 300)
//
// The producer reports its own outbound rate to stdout once per second so a wedged producer
// is distinguishable from a wedged silo when reading the systemd-journald-captured logs.

using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.AzureThroughput.Engine;
using static VehicleFleetSimulator.AzureThroughput.Engine.BenchExceptionHelpers;

// Force autoflush on stdout/stderr. In a Linux container with stdout
// redirected, .NET's default `Console.Out` is a buffered StreamWriter
// and per-second progress lines sit in a 4-KiB buffer for tens of
// seconds before the container log driver sees them. See the matching
// note in Silo/Program.cs.
Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var producerMode = (Environment.GetEnvironmentVariable("BENCH_PRODUCER_MODE") ?? "tcp").Trim().ToLowerInvariant();
if (producerMode is not ("tcp" or "orleans-client"))
{
    Console.Error.WriteLine($"[producer] FATAL: BENCH_PRODUCER_MODE='{producerMode}' is invalid; expected 'tcp' or 'orleans-client'.");
    Environment.Exit(2);
    return;
}

if (producerMode == "orleans-client")
{
    await RunOrleansClientProducerAsync(args);
    return;
}

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

// U9o producer-side instrumentation. The U9m ladder run showed the
// producer offered only ~3.3k msg/s against a target of 25k msg/s, so
// the missing throughput is on the producer side of the wire, not in
// the silo. To disambiguate "CPU on the inner loop" from "TCP back-
// pressure on the flush" we keep three per-second aggregates:
//
//   innerLoopMs - sum of wall-clock spent in the per-vehicle for-loop
//                 (JSON serialize + buffered write).
//   flushMs     - sum of wall-clock spent in writer.FlushAsync.
//   tickSlipMs  - max of (actualTickEntry - scheduledTick), where
//                 scheduledTick advances strictly by tickIntervalMs
//                 regardless of how late each tick fires.
//
// scheduledTick advances independently of nextTick so the producer's
// existing self-resetting tick clock is preserved (no behavior change);
// the slippage is a measurement, not a feedback signal.
var scheduledTick = DateTimeOffset.UtcNow;
double innerLoopMsThisReport = 0.0;
double flushMsThisReport = 0.0;
double tickSlipMaxMsThisReport = 0.0;
long ticksThisReport = 0;

long totalSent = 0;
long sentSinceReport = 0;
var lastReport = Stopwatch.GetTimestamp();

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

    var slip = (now - scheduledTick).TotalMilliseconds;
    if (slip > tickSlipMaxMsThisReport) tickSlipMaxMsThisReport = slip;
    scheduledTick = scheduledTick.AddMilliseconds(tickIntervalMs);
    ticksThisReport++;

    var innerStart = Stopwatch.GetTimestamp();
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
    innerLoopMsThisReport += (Stopwatch.GetTimestamp() - innerStart) * 1000.0 / Stopwatch.Frequency;

    var sinceReport = Stopwatch.GetTimestamp() - lastReport;
    if (sinceReport >= Stopwatch.Frequency)
    {
        var flushStart = Stopwatch.GetTimestamp();
        await writer.FlushAsync();
        flushMsThisReport += (Stopwatch.GetTimestamp() - flushStart) * 1000.0 / Stopwatch.Frequency;

        var rate = sentSinceReport / (sinceReport / (double)Stopwatch.Frequency);
        var elapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
        var innerAvgMs = ticksThisReport > 0 ? innerLoopMsThisReport / ticksThisReport : 0.0;
        var flushAvgMs = ticksThisReport > 0 ? flushMsThisReport / ticksThisReport : 0.0;
        Console.WriteLine($"[producer] t={elapsed,7:0.0}s sent={totalSent,12:N0} rate={rate,10:N0} msg/s ticks={ticksThisReport,3} innerAvgMs={innerAvgMs,7:0.00} flushAvgMs={flushAvgMs,7:0.00} slipMaxMs={tickSlipMaxMsThisReport,8:0.0}");
        sentSinceReport = 0;
        ticksThisReport = 0;
        innerLoopMsThisReport = 0.0;
        flushMsThisReport = 0.0;
        tickSlipMaxMsThisReport = 0.0;
        lastReport = Stopwatch.GetTimestamp();
    }
}

await writer.FlushAsync();
var totalElapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
Console.WriteLine($"[producer] DONE total={totalSent:N0} elapsed={totalElapsed:0.0}s avg={totalSent / Math.Max(0.001, totalElapsed):N0} msg/s");

static async Task RunOrleansClientProducerAsync(string[] args)
{
    var vehicleCount = ReadInt("BENCH_VEHICLE_COUNT", 1000);
    var tickHz = ReadInt("BENCH_TICK_HZ", 5);
    var duration = ReadInt("BENCH_DURATION_SEC", 300);
    var treeId = Environment.GetEnvironmentVariable("BENCH_TREE_ID")
        ?? $"azure-throughput-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
    var tcpPort = ReadInt("BENCH_TCP_PORT", 7000);
    var batchSize = ReadInt("BENCH_BATCH_SIZE", 4096);
    var flushMs = ReadInt("BENCH_FLUSH_MS", 50);
    var flushConcurrency = ReadInt("BENCH_FLUSH_CONCURRENCY", 8);
    var walPartitions = ReadInt("BENCH_WAL_PARTITIONS", LatticeOptions.DefaultWalPartitions);
    var walMaxPending = ReadInt("BENCH_WAL_MAX_PENDING_BATCHES", LatticeOptions.DefaultWalMaxPendingBatches);
    var walExtraAccountUris = (Environment.GetEnvironmentVariable("BENCH_WAL_EXTRA_ACCOUNT_URIS") ?? string.Empty)
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    var walAccountsRequested = ReadInt("BENCH_WAL_ACCOUNTS", 1);
    var walAccounts = Math.Clamp(walAccountsRequested, 1, 1 + walExtraAccountUris.Length);
    var shardCountOverride = ReadIntAllowZero("BENCH_SHARD_COUNT", 0);
    var reportSec = ReadInt("BENCH_REPORT_SEC", 1);
    var responseTimeoutSec = ReadInt("BENCH_RESPONSE_TIMEOUT_SEC", 30);
    var workloadMode = BenchWorkloadMetadata.ParseWorkloadMode(Environment.GetEnvironmentVariable("BENCH_WORKLOAD_MODE"));
    var atomicBatchSize = ReadInt("BENCH_ATOMIC_BATCH_SIZE", 64);
    var preseedKeyCount = ReadIntAllowZero("BENCH_VEHICLE_COUNT", 0);
    var clusteringConn = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_CONNECTION_STRING");
    var clusteringTableServiceUri = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_TABLE_SERVICE_URI");
    var clusteringTable = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_TABLE") ?? "OrleansSiloInstances";

    if (string.IsNullOrWhiteSpace(clusteringConn) && string.IsNullOrWhiteSpace(clusteringTableServiceUri))
    {
        Console.Error.WriteLine("[producer] FATAL: orleans-client mode requires BENCH_CLUSTERING_CONNECTION_STRING or BENCH_CLUSTERING_TABLE_SERVICE_URI.");
        Environment.Exit(2);
        return;
    }

    var settings = new IngestSettings(
        treeId,
        tcpPort,
        batchSize,
        TimeSpan.FromMilliseconds(flushMs),
        TimeSpan.FromSeconds(reportSec),
        flushConcurrency,
        shardCountOverride,
        workloadMode,
        atomicBatchSize,
        preseedKeyCount,
        walMaxPending,
        responseTimeoutSec,
        walPartitions,
        walAccounts,
        "orleans-client");

    Console.WriteLine($"[producer] mode=orleans-client vehicles={vehicleCount} tickHz={tickHz} duration={duration}s");
    Console.WriteLine($"[producer] settings treeId={settings.TreeId} tcpPort={settings.TcpPort} batch={settings.BatchSize} flushMs={settings.FlushInterval.TotalMilliseconds:F0} flushConcurrency={settings.FlushConcurrency} walPartitions={settings.WalPartitions} walMaxPending={settings.WalMaxPendingBatches} shardCountOverride={settings.ShardCountOverride} responseTimeoutSec={settings.ResponseTimeoutSec} workloadMode={BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode)} atomicBatchSize={settings.AtomicBatchSize} preseedKeyCount={settings.PreseedKeyCount} walAccounts={settings.WalAccounts} walAccountsRequested={walAccountsRequested} walExtraAccounts={walExtraAccountUris.Length} clusteringTable={clusteringTable}");

    var builder = Host.CreateApplicationBuilder(args);
    builder.Logging.ClearProviders();
    builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
    builder.Logging.SetMinimumLevel(LogLevel.Warning);

    builder.UseOrleansClient(client =>
    {
        client.Configure<ClusterOptions>(o =>
        {
            o.ClusterId = "azure-throughput";
            o.ServiceId = "azure-throughput";
        });
        client.Configure<ClientMessagingOptions>(o =>
        {
            o.ResponseTimeout = TimeSpan.FromSeconds(responseTimeoutSec);
        });
        client.UseAzureStorageClustering(o =>
        {
            o.TableName = clusteringTable;
            o.TableServiceClient = !string.IsNullOrWhiteSpace(clusteringConn)
                ? new TableServiceClient(clusteringConn)
                : new TableServiceClient(new Uri(clusteringTableServiceUri!), new DefaultAzureCredential());
        });
    });

    using var host = builder.Build();
    await host.StartAsync().ConfigureAwait(false);
    try
    {
        var clusterClient = host.Services.GetRequiredService<IClusterClient>();
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("producer-engine");
        var lattice = clusterClient.GetGrain<ILattice>(settings.TreeId);
        var ct = lifetime.ApplicationStopping;

        if (settings.ShardCountOverride > 0)
        {
            await SubmitAndWaitForReshardAsync(lattice, settings.TreeId, settings.ShardCountOverride, ct).ConfigureAwait(false);
        }

        var channel = Channel.CreateBounded<KeyValuePair<string, byte[]>>(new BoundedChannelOptions(capacity: 1 << 16)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

        var engine = new BenchIngestEngine(
            clusterClient,
            settings,
            lifetime,
            new NoOpBenchSaturationGate(),
            logger);
        var drainTask = Task.Run(() => engine.DrainAsync(lattice, channel.Reader, ct), CancellationToken.None);

        await RunChannelGeneratorAsync(vehicleCount, tickHz, duration, channel.Writer, ct).ConfigureAwait(false);
        channel.Writer.TryComplete();
        await drainTask.ConfigureAwait(false);
    }
    finally
    {
        await host.StopAsync().ConfigureAwait(false);
    }
}

static async Task RunChannelGeneratorAsync(
    int vehicleCount,
    int tickHz,
    int duration,
    ChannelWriter<KeyValuePair<string, byte[]>> writer,
    CancellationToken ct)
{
    var vehicles = CreateVehicleIds(vehicleCount);
    var jsonOpts = new JsonSerializerOptions { IncludeFields = false, WriteIndented = false };

    var startedAt = Stopwatch.GetTimestamp();
    var deadlineTicks = duration > 0
        ? Stopwatch.GetTimestamp() + (long)(duration * (double)Stopwatch.Frequency)
        : long.MaxValue;

    var tickIntervalMs = Math.Max(1, 1000 / tickHz);
    var nextTick = DateTimeOffset.UtcNow;
    var scheduledTick = DateTimeOffset.UtcNow;
    double innerLoopMsThisReport = 0.0;
    double flushMsThisReport = 0.0;
    double tickSlipMaxMsThisReport = 0.0;
    long ticksThisReport = 0;

    long totalSent = 0;
    long sentSinceReport = 0;
    var lastReport = Stopwatch.GetTimestamp();

    while (Stopwatch.GetTimestamp() < deadlineTicks && !ct.IsCancellationRequested)
    {
        var now = DateTimeOffset.UtcNow;
        if (now < nextTick)
        {
            var wait = (int)Math.Max(1, (nextTick - now).TotalMilliseconds);
            await Task.Delay(wait, ct).ConfigureAwait(false);
            continue;
        }
        nextTick = now + TimeSpan.FromMilliseconds(tickIntervalMs);

        var slip = (now - scheduledTick).TotalMilliseconds;
        if (slip > tickSlipMaxMsThisReport) tickSlipMaxMsThisReport = slip;
        scheduledTick = scheduledTick.AddMilliseconds(tickIntervalMs);
        ticksThisReport++;

        var innerStart = Stopwatch.GetTimestamp();
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
            var key = ev.VehicleId.ToString("N");
            await writer.WriteAsync(new KeyValuePair<string, byte[]>(key, bytes), ct).ConfigureAwait(false);

            totalSent++;
            sentSinceReport++;
        }
        innerLoopMsThisReport += (Stopwatch.GetTimestamp() - innerStart) * 1000.0 / Stopwatch.Frequency;

        var sinceReport = Stopwatch.GetTimestamp() - lastReport;
        if (sinceReport >= Stopwatch.Frequency)
        {
            var rate = sentSinceReport / (sinceReport / (double)Stopwatch.Frequency);
            var elapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
            var innerAvgMs = ticksThisReport > 0 ? innerLoopMsThisReport / ticksThisReport : 0.0;
            var flushAvgMs = ticksThisReport > 0 ? flushMsThisReport / ticksThisReport : 0.0;
            Console.WriteLine($"[producer] t={elapsed,7:0.0}s sent={totalSent,12:N0} rate={rate,10:N0} msg/s ticks={ticksThisReport,3} innerAvgMs={innerAvgMs,7:0.00} flushAvgMs={flushAvgMs,7:0.00} slipMaxMs={tickSlipMaxMsThisReport,8:0.0}");
            sentSinceReport = 0;
            ticksThisReport = 0;
            innerLoopMsThisReport = 0.0;
            flushMsThisReport = 0.0;
            tickSlipMaxMsThisReport = 0.0;
            lastReport = Stopwatch.GetTimestamp();
        }
    }

    var totalElapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
    Console.WriteLine($"[producer] DONE total={totalSent:N0} elapsed={totalElapsed:0.0}s avg={totalSent / Math.Max(0.001, totalElapsed):N0} msg/s");
}

static Guid[] CreateVehicleIds(int vehicleCount)
{
    var vehicles = new Guid[vehicleCount];
    var idBytes = new byte[16];
    for (var i = 0; i < vehicleCount; i++)
    {
        BitConverter.TryWriteBytes(idBytes.AsSpan(0, 4), i);
        BitConverter.TryWriteBytes(idBytes.AsSpan(4, 4), 0xC0FFEE);
        BitConverter.TryWriteBytes(idBytes.AsSpan(8, 4), 0xDEADBEEF);
        BitConverter.TryWriteBytes(idBytes.AsSpan(12, 4), 0xCAFEBABE);
        vehicles[i] = new Guid(idBytes);
    }

    return vehicles;
}

static async Task SubmitAndWaitForReshardAsync(ILattice lattice, string treeId, int shardCount, CancellationToken ct)
{
    const int MaxReshardAttempts = 12;
    const int MaxReshardBackoffMs = 6000;
    var attempt = 0;
    var reshardSubmitted = false;
    Exception? lastReshardException = null;
    while (attempt < MaxReshardAttempts && !reshardSubmitted && !ct.IsCancellationRequested)
    {
        attempt++;
        try
        {
            Console.WriteLine($"[producer] reshard treeId={treeId} -> shardCount={shardCount} (submit attempt={attempt}/{MaxReshardAttempts})");
            await lattice.ReshardAsync(shardCount, ct).ConfigureAwait(false);
            reshardSubmitted = true;
        }
        catch (ArgumentOutOfRangeException ex)
        {
            Console.WriteLine($"[producer] reshard treeId={treeId} rejected: {ex.Message}");
            lastReshardException = ex;
            break;
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex) when (IsOrleansMessageRejection(ex)
            || WarmUpRetryClassifier.IsTransientPlacementConvergence(ex))
        {
            lastReshardException = ex;
            var backoffMs = Math.Min(100 * (1 << (attempt - 1)), MaxReshardBackoffMs);
            var kind = IsOrleansMessageRejection(ex) ? "REJECTED" : "PLACEMENT-CONVERGING";
            Console.WriteLine($"[producer] reshard treeId={treeId} attempt={attempt} {kind} ({ex.GetType().Name}: {Truncate(ex.Message, 160)}); backing off {backoffMs}ms before retry");
            await Task.Delay(TimeSpan.FromMilliseconds(backoffMs), ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[producer] reshard treeId={treeId} FAILED: {ex.GetType().Name}: {ex.Message}");
            lastReshardException = ex;
            break;
        }
    }

    if (!reshardSubmitted)
    {
        var detail = lastReshardException is null
            ? "no exception captured"
            : $"{lastReshardException.GetType().Name}: {Truncate(lastReshardException.Message, 240)}";
        var msg = $"[producer] ERROR reshard treeId={treeId} ABORTED after {attempt} attempt(s): {detail}. Tree remains at its previously-pinned shard count (likely the library default, NOT shardCount={shardCount}).";
        Console.WriteLine(msg);
        throw new InvalidOperationException(msg, lastReshardException);
    }

    var deadline = DateTime.UtcNow.AddSeconds(120);
    while (true)
    {
        bool complete;
        try
        {
            complete = await lattice.IsReshardCompleteAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex) when (IsOrleansMessageRejection(ex))
        {
            Console.WriteLine($"[producer] reshard treeId={treeId} IsReshardCompleteAsync rejected ({ex.GetType().Name}); retrying");
            complete = false;
        }
        if (complete)
        {
            Console.WriteLine($"[producer] reshard treeId={treeId} complete");
            break;
        }
        if (DateTime.UtcNow >= deadline)
        {
            Console.WriteLine($"[producer] reshard treeId={treeId} TIMEOUT - migration still in progress, continuing anyway");
            break;
        }
        Console.WriteLine($"[producer] reshard treeId={treeId} in progress, waiting...");
        await Task.Delay(TimeSpan.FromSeconds(2), ct).ConfigureAwait(false);
    }
}

static int ReadInt(string name, int @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    return int.TryParse(raw, out var v) && v > 0 ? v : @default;
}

static int ReadIntAllowZero(string name, int @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    return int.TryParse(raw, out var v) && v >= 0 ? v : @default;
}

