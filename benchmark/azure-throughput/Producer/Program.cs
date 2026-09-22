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
    var clientCount = Math.Clamp(ReadInt("BENCH_CLIENT_COUNT", 1), 1, 64);
    var atomicBatchSize = ReadInt("BENCH_ATOMIC_BATCH_SIZE", 64);
    var preseedKeyCount = ReadIntAllowZero("BENCH_VEHICLE_COUNT", 0);
    var clusteringConn = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_CONNECTION_STRING");
    var clusteringTableServiceUri = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_TABLE_SERVICE_URI");
    var clusteringTable = Environment.GetEnvironmentVariable("BENCH_CLUSTERING_TABLE") ?? "OrleansSiloInstances";
    // Must match the silo's BENCH_CLUSTER_ID for this cohort, or the client joins an
    // empty membership partition and never finds a gateway. See Silo/Program.cs.
    var clusterId = Environment.GetEnvironmentVariable("BENCH_CLUSTER_ID") ?? "azure-throughput";

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

    Console.WriteLine($"[producer] mode=orleans-client vehicles={vehicleCount} tickHz={tickHz} duration={duration}s clients={clientCount}");
    Console.WriteLine($"[producer] settings treeId={settings.TreeId} tcpPort={settings.TcpPort} batch={settings.BatchSize} flushMs={settings.FlushInterval.TotalMilliseconds:F0} flushConcurrency={settings.FlushConcurrency} walPartitions={settings.WalPartitions} walMaxPending={settings.WalMaxPendingBatches} shardCountOverride={settings.ShardCountOverride} responseTimeoutSec={settings.ResponseTimeoutSec} workloadMode={BenchWorkloadMetadata.FormatWorkloadMode(settings.WorkloadMode)} atomicBatchSize={settings.AtomicBatchSize} preseedKeyCount={settings.PreseedKeyCount} walAccounts={settings.WalAccounts} walAccountsRequested={walAccountsRequested} walExtraAccounts={walExtraAccountUris.Length} clusteringTable={clusteringTable}");

    // One IClusterClient reaches exactly one silo for this workload. Orleans
    // buckets client-to-grain traffic by TargetGrain hash to preserve per-grain
    // ordering, and a single-tree benchmark has a single grain id - so one
    // client pins to one gateway, and [StatelessWorker] LatticeGrain then
    // activates on that gateway's silo. Building several independent clients
    // is what spreads the front door: each has its own gateway bucket array
    // fed by a randomly-offset round-robin cursor. See BenchIngestEngine's
    // multi-handle DrainAsync for the full rationale.
    static IHost BuildClientHost(
        string[] hostArgs,
        int responseTimeoutSec,
        string? clusteringConn,
        string? clusteringTableServiceUri,
        string clusteringTable,
        string clusterId)
    {
        var builder = Host.CreateApplicationBuilder(hostArgs);
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.UseOrleansClient(client =>
        {
            client.Configure<ClusterOptions>(o =>
            {
                o.ClusterId = clusterId;
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

        return builder.Build();
    }

    var hosts = new List<IHost>(clientCount);
    for (var i = 0; i < clientCount; i++)
    {
        hosts.Add(BuildClientHost(args, responseTimeoutSec, clusteringConn, clusteringTableServiceUri, clusteringTable, clusterId));
    }

    try
    {
        await Task.WhenAll(hosts.Select(h => h.StartAsync())).ConfigureAwait(false);

        var host = hosts[0];
        var clusterClient = host.Services.GetRequiredService<IClusterClient>();
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("producer-engine");
        var lattices = hosts
            .Select(h => h.Services.GetRequiredService<IClusterClient>().GetGrain<ILattice>(settings.TreeId))
            .ToArray();
        var lattice = lattices[0];
        var ct = lifetime.ApplicationStopping;

        if (settings.ShardCountOverride > 0)
        {
            await SubmitAndWaitForReshardAsync(lattice, settings.TreeId, settings.ShardCountOverride, ct).ConfigureAwait(false);
        }

        // Warm the tree before the measured window opens. This is not an
        // optimisation - without it the Layer 3 cohort is bimodal, and the
        // failing mode produces no data at all.
        //
        // On the single-VM path the silo host calls lattice.WarmUpAsync during
        // startup for EVERY workload mode (see Silo/Program.cs: only the
        // read-mode content pre-seed is gated on the mode; shard-root
        // activation is unconditional). The Layer 3 silo returns early in
        // cluster mode and never reaches that call, so nothing warmed the tree
        // and the producer became the first caller to touch it.
        //
        // The engine then opens with FlushConcurrency concurrent SetManyAsync
        // calls of BatchSize keys each, every one of which fans out across all
        // 64 shard roots. Cold, those roots must activate, read their state,
        // and materialise leaves before any of them can answer. At N=1 that
        // fanout is in-process and absorbs the first wave; at N>=2 roughly half
        // of it becomes cross-silo RPC and the first wave can exceed the 180s
        // response timeout. Every in-flight flush then times out together,
        // nothing retires, and the cohort reports ops=0 for its whole run -
        // measured here as exactly that, with all 16 flushes parked in
        // set_many phase=fanout at p50 180,243 ms.
        //
        // That failure is bimodal rather than gradual (a cohort either flows at
        // full rate or returns literally nothing), which is why it presented as
        // flakiness: two N=2 cohorts wedged and one succeeded on identical
        // configuration and an identical image.
        //
        // Warming here also restores parity with Layer 2 rather than merely
        // avoiding a stall: Layer 2 measures a warm tree, so a Layer 3 number
        // that included cold-start activation would not be comparable with the
        // baseline the whole tier is anchored against.
        var warmSw = Stopwatch.StartNew();
        await WarmUpWithRetryAsync(lattice, settings.TreeId, ct).ConfigureAwait(false);
        warmSw.Stop();
        Console.WriteLine($"[producer] warmup treeId={settings.TreeId} complete elapsedMs={warmSw.Elapsed.TotalMilliseconds:F0}");

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
        var drainTask = Task.Run(() => engine.DrainAsync(lattices, channel.Reader, ct), CancellationToken.None);

        await RunChannelGeneratorAsync(vehicleCount, tickHz, duration, channel.Writer, ct).ConfigureAwait(false);
        channel.Writer.TryComplete();
        await drainTask.ConfigureAwait(false);
    }
    finally
    {
        foreach (var h in hosts)
        {
            try { await h.StopAsync().ConfigureAwait(false); } catch (Exception ex) { Console.Error.WriteLine($"[producer] client stop failed: {ex.Message}"); }
            h.Dispose();
        }
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

static async Task WarmUpWithRetryAsync(ILattice lattice, string treeId, CancellationToken ct)
{
    // Mirrors the silo-side warm-up retry loop, including its classification of
    // what counts as transient. A cold multi-silo cluster is exactly where
    // placement has not yet converged and an activation can be cancelled out
    // from under the caller, so a single-shot warm-up would fail for reasons
    // that resolve on their own a moment later. Saturation is retryable here
    // too: warming 64 shard roots at once is itself a burst.
    const int MaxWarmUpAttempts = 12;
    const int MaxWarmUpBackoffMs = 6000;
    const int MaxWarmUpSaturationBackoffMs = 20000;

    // An attempt cap alone is the wrong budget when a single attempt can cost
    // the whole client response timeout. A warm-up that keeps timing out burns
    // MaxWarmUpAttempts x ResponseTimeout - 36 minutes at the default 180 s -
    // before it gives up, and an unattended silo-count sweep multiplies that by
    // every cohort in the series. That is not a hang the operator can see: the
    // job just sits in Running with billable replicas up.
    //
    // So the real budget is wall-clock, and it is enforced with a linked token
    // rather than only as a loop condition. Checking it between attempts would
    // still let an attempt that started just inside the deadline run the full
    // response timeout past it; cancelling the call itself bounds the whole
    // helper. A healthy warm-up completes in around a second, so any budget in
    // the minutes is generous - it exists to cap the pathological case, not to
    // discipline the normal one.
    var budgetSec = Math.Clamp(ReadInt("BENCH_WARMUP_BUDGET_SEC", 480), 30, 3600);
    using var budgetCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    budgetCts.CancelAfter(TimeSpan.FromSeconds(budgetSec));
    var budgetToken = budgetCts.Token;
    var budgetSw = System.Diagnostics.Stopwatch.StartNew();

    var attempt = 0;
    Exception? lastException = null;
    while (attempt < MaxWarmUpAttempts && !budgetToken.IsCancellationRequested)
    {
        attempt++;
        try
        {
            Console.WriteLine($"[producer] warmup treeId={treeId} (attempt={attempt}/{MaxWarmUpAttempts}, {budgetSw.Elapsed.TotalSeconds:F0}s/{budgetSec}s budget)");
            await lattice.WarmUpAsync(budgetToken).ConfigureAwait(false);
            return;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch (OperationCanceledException) when (budgetToken.IsCancellationRequested) { break; }
        catch (Exception ex) when (!budgetToken.IsCancellationRequested
            && (IsOrleansMessageRejection(ex)
                || WarmUpRetryClassifier.IsTransientActivationCancellation(ex)
                || WarmUpRetryClassifier.IsTransientPlacementConvergence(ex)
                || WarmUpRetryClassifier.IsTransientSaturation(ex)
                || WarmUpRetryClassifier.IsTransientRequestTimeout(ex)))
        {
            lastException = ex;
            var isSaturation = WarmUpRetryClassifier.IsTransientSaturation(ex);
            var isTimeout = !isSaturation && WarmUpRetryClassifier.IsTransientRequestTimeout(ex);
            // A timed-out warm-up is still running on the silo, so the useful
            // thing to do is re-ask almost immediately and let the next attempt
            // inherit the progress the abandoned one is still making. Backing
            // off here would idle the client while the server works.
            var backoffMs = isSaturation
                ? Math.Min(2000 * attempt, MaxWarmUpSaturationBackoffMs)
                : isTimeout
                    ? 500
                    : Math.Min(100 * (1 << (attempt - 1)), MaxWarmUpBackoffMs);
            var kind = isSaturation ? " SATURATED" : isTimeout ? " TIMEOUT" : string.Empty;
            Console.WriteLine($"[producer] warmup treeId={treeId} transient{kind} ({ex.GetType().Name}); retrying in {backoffMs}ms");
            try
            {
                await Task.Delay(backoffMs, budgetToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (budgetToken.IsCancellationRequested) { break; }
        }
    }

    if (ct.IsCancellationRequested) { ct.ThrowIfCancellationRequested(); }

    // Fail loud. A cohort that proceeds on an unwarmed tree does not merely run
    // slower - it wedges and reports ops=0, which is far more expensive to
    // diagnose after the fact than an explicit warm-up failure here.
    var exhausted = budgetToken.IsCancellationRequested ? "budget" : "attempts";
    throw new InvalidOperationException(
        $"warm-up of tree '{treeId}' did not complete after {attempt} attempt(s) / {budgetSw.Elapsed.TotalSeconds:F0}s (exhausted {exhausted}; budget {budgetSec}s)",
        lastException);
}

static async Task SubmitAndWaitForReshardAsync(ILattice lattice, string treeId, int shardCount, CancellationToken ct)
{
    const int MaxReshardAttempts = 12;
    const int MaxReshardBackoffMs = 6000;
    // Saturation drains on a seconds timescale, so its ceiling is higher than
    // the placement-race one. 12 attempts x up to 20s is a ~2 minute budget,
    // which comfortably outlasts an observed cold-reshard queue drain while
    // still failing loudly rather than hanging.
    const int MaxReshardSaturationBackoffMs = 20000;
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
            || WarmUpRetryClassifier.IsTransientPlacementConvergence(ex)
            || WarmUpRetryClassifier.IsTransientSaturation(ex)
            || WarmUpRetryClassifier.IsTransientRequestTimeout(ex))
        {
            lastReshardException = ex;
            // Saturation needs a materially longer backoff than a placement
            // race. A cold reshard to S shards asks the cluster to admit S
            // shard-root activations at once, each needing a WAL replay
            // permit; the queue refuses above its ceiling and then drains
            // over seconds, not milliseconds. The exponential schedule below
            // starts at 100ms, which retries straight back into a queue that
            // has not moved and burns the whole 12-attempt budget in under a
            // second. Saturation therefore gets its own floor.
            //
            // A response timeout is retryable here for the same reason it is
            // during warm-up: on a cold tree the reshard is genuinely slow
            // rather than stuck - the grain is still executing and retiring
            // work items when the client's deadline fires. Without this arm
            // a timeout fell through to the generic handler below, which
            // breaks out of the loop, so reshard "ABORTED after 1 attempt(s)"
            // while 11 attempts of budget went unused and the cohort was
            // lost outright. It is treated as saturation for backoff
            // purposes because a slow reshard is a busy cluster, and
            // retrying into it after 100ms only adds load.
            var isSaturation = WarmUpRetryClassifier.IsTransientSaturation(ex)
                || WarmUpRetryClassifier.IsTransientRequestTimeout(ex);
            var backoffMs = isSaturation
                ? Math.Min(2000 * attempt, MaxReshardSaturationBackoffMs)
                : Math.Min(100 * (1 << (attempt - 1)), MaxReshardBackoffMs);
            var kind = WarmUpRetryClassifier.IsTransientRequestTimeout(ex)
                ? "TIMEOUT"
                : WarmUpRetryClassifier.IsTransientSaturation(ex)
                    ? "SATURATED"
                    : IsOrleansMessageRejection(ex) ? "REJECTED" : "PLACEMENT-CONVERGING";
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

