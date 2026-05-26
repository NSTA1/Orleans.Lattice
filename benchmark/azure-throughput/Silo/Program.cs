// Azure throughput benchmark harness - single-silo lattice host.
//
// Listens on a TCP port for newline-delimited JSON `VehicleTelemetryEvent` records, batches
// them, and writes each batch into a single lattice tree backed by the Azure Table WAL
// storage provider (managed identity to the configured storage account).
//
// Reports "Entries written per second" to stdout once per second so the ACI log is the
// canonical result surface.
//
// Environment variables:
//   BENCH_STORAGE_URI       https://{account}.table.core.windows.net  (required for managed identity)
//   BENCH_STORAGE_CONN      connection string fallback (optional, overrides BENCH_STORAGE_URI when set)
//   BENCH_WAL_TABLE         WAL table name (default OrleansLatticeWal)
//   BENCH_TREE_ID           lattice tree id. Default is "azure-throughput-{utc-yyyyMMdd-HHmmss}"
//                           which rotates per silo restart - the Azure Tables WAL keeps
//                           every run's rows under their own partition-key namespace
//                           (`_m_|{treeId}|{shardIndex}`), so a previous run's offsets
//                           never bias the next run's WalShardGrain activation cost.
//                           Set explicitly to pin a stable id (e.g. for cross-run replay
//                           testing) - that re-uses the existing rows.
//   BENCH_TCP_PORT          TCP listen port (default 7000)
//   BENCH_BATCH_SIZE        SetManyAsync batch size (default 4096 - sized so the 64-way
//                           shard/leaf/WAL fan-out still leaves ~64 entries per WAL
//                           partition transaction, giving phase-2 coalescing real
//                           work to do)
//   BENCH_FLUSH_MS          max flush latency in ms (default 50)
//   BENCH_FLUSH_CONCURRENCY max in-flight SetManyAsync calls (default 8 - matches the
//                           WalPartitions/WalMaxPendingBatches window so the pipelined
//                           phase-2 path always has a batch N+1 in flight to overlap
//                           with batch N's phase 2. Drop to 1 for diagnostic A/B runs
//                           that isolate leaf-mailbox queueing from per-leaf-turn
//                           Azure Tables RTT cost.)
//   BENCH_WAL_PARTITIONS    WAL partitions per tree (default 8 - matches flush concurrency so
//                           parallel SetManyAsync flushes fan out across distinct WAL grains
//                           and therefore distinct Azure Tables manifest partitions)
//   BENCH_WAL_MAX_PENDING_BATCHES
//                           Per-WalShardGrain pipeline depth (default 8). Library default is
//                           1 (single in-flight append per partition) for wire-compat; raising
//                           it lets each partition's flushes overlap against Azure Tables
//                           rather than strictly serialising.
//   BENCH_SHARD_COUNT       Override the tree's physical shard count at startup via
//                           ILattice.ReshardAsync. 0 = keep the library default (64).
//                           Notes: (a) ReshardAsync is grow-only against a populated
//                           tree (target must be > current shard count); (b) against a
//                           freshly-registered/empty tree any target works via the
//                           empty-tree fast-path and returns synchronously; (c) the
//                           harness polls IsReshardCompleteAsync before opening the TCP
//                           listener so writes never race a still-running migration.
//   BENCH_PIPELINE_PHASE2   Set to 0 to disable AzureTableWalStorageOptions.
//                           PipelinePhaseTwoCommits, which overlaps phase 2 of batch N
//                           with phase 0+1 of batch N+1 on the same shard. Halves the
//                           steady-state request-path latency under WalMaxPendingBatches=1
//                           and lets the PhaseTwoWorker's coalescing window actually
//                           collapse multiple commits into one Azure Tables transaction.
//                           Default 1 (on) for the bench; the library default remains
//                           off to preserve the existing wire-compat shape where every
//                           AppendBatchAsync awaits its own phase 2.
//   BENCH_WAL_PHASE2_COALESCING_WINDOW_MS
//                           AzureTableWalStorageOptions.PhaseTwoCoalescingWindow in ms
//                           (default 0 - drain-on-first-signal, matches the library
//                           default). Set to a small positive value (e.g. 5-10 ms,
//                           below the observed phase-2 commit duration p50) to let the
//                           per-shard PhaseTwoWorker wait briefly after the first arrival
//                           so additional commits coalesce into the same Azure Tables
//                           transaction. Probe lever for U9c.
//   BENCH_REPORT_SEC        stdout report interval in seconds (default 1)
//   BENCH_PHASEA_REPORT_SEC stdout cadence for the Phase A diagnostic
//                           reporter (default 10). Set to 0 to disable
//                           the reporter entirely. Emits one
//                           [phaseA] line per (instrument, tree, shard,
//                           phase, status) tuple per cadence tick,
//                           carrying p50/p90/p99/count/min/max over the
//                           preceding window. The ladder script
//                           (40-ladder.ps1) scrapes these lines to
//                           attribute caller-visible append latency to
//                           grain-side queueing vs storage-provider
//                           commit time.
//   BENCH_TOTAL_DURATION_SEC
//                           Server-side watchdog. After this many seconds the silo
//                           triggers a graceful host shutdown so the ACI container
//                           group transitions to Terminated even if the local deploy
//                           shell that orchestrated the run has died. 0 disables the
//                           watchdog. Default 600 (10 minutes) - well above the
//                           harness's nominal 120s run so a normal client-driven stop
//                           still wins the race, while a runaway run cannot burn paid
//                           Azure compute indefinitely.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Storage.AzureTable;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.AzureThroughput.Silo;

// Force autoflush on stdout/stderr. When the process is running inside
// a Linux container (ACI, Docker) and stdout is redirected, .NET's
// default `Console.Out` is a buffered StreamWriter that does NOT flush
// on every WriteLine. The buffer is ~4 KiB, so periodic single-line
// progress output (one line/sec from the throughput drainer) sits in
// the buffer for tens of seconds before the container log driver sees
// it - which looks exactly like a hung silo. Wrapping the existing
// stream in a new StreamWriter with AutoFlush=true is the canonical
// fix and is harmless on Windows/dev runs.
Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var storageUri  = Environment.GetEnvironmentVariable("BENCH_STORAGE_URI");
var storageConn = Environment.GetEnvironmentVariable("BENCH_STORAGE_CONN");
var walTable    = Environment.GetEnvironmentVariable("BENCH_WAL_TABLE") ?? "OrleansLatticeWal";
// Auto-rotate the tree id per silo restart so each run gets a fresh
// manifest-key namespace in the persisted WAL table. Lattice grain state
// is memory-backed and resets on restart anyway, but the WAL table is
// Azure-Tables-backed and keeps every previous run's offsets - cross-run
// activation cost would otherwise bias the first ~10s of each new
// benchmark. Operator can pin BENCH_TREE_ID explicitly to opt out.
var treeId      = Environment.GetEnvironmentVariable("BENCH_TREE_ID")
                  ?? $"azure-throughput-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
var tcpPort     = ReadInt("BENCH_TCP_PORT", 7000);
var batchSize   = ReadInt("BENCH_BATCH_SIZE", 4096);
var flushMs     = ReadInt("BENCH_FLUSH_MS", 50);
var flushConcurrency = ReadInt("BENCH_FLUSH_CONCURRENCY", 8);
var walPartitions = ReadInt("BENCH_WAL_PARTITIONS", 8);
var walMaxPending = ReadInt("BENCH_WAL_MAX_PENDING_BATCHES", 8);
var shardCountOverride = ReadIntAllowZero("BENCH_SHARD_COUNT", 0);
var pipelinePhase2 = ReadBool("BENCH_PIPELINE_PHASE2", true);
var eliminateCandidateRow = ReadBool("BENCH_WAL_ELIMINATE_CANDIDATE_ROW", false);
var phaseTwoCoalescingMs = ReadIntAllowZero("BENCH_WAL_PHASE2_COALESCING_WINDOW_MS", 0);
var reportSec   = ReadInt("BENCH_REPORT_SEC", 1);
var totalDurationSec = ReadIntAllowZero("BENCH_TOTAL_DURATION_SEC", 600);

if (string.IsNullOrWhiteSpace(storageUri) && string.IsNullOrWhiteSpace(storageConn))
{
    Console.Error.WriteLine("[silo] FATAL: set BENCH_STORAGE_URI (managed identity) or BENCH_STORAGE_CONN (connection string).");
    Environment.Exit(2);
    return;
}

Console.WriteLine($"[silo] treeId={treeId} walTable={walTable} tcpPort={tcpPort} batch={batchSize} flushMs={flushMs} flushConcurrency={flushConcurrency} walPartitions={walPartitions} walMaxPending={walMaxPending} shardCountOverride={shardCountOverride} pipelinePhase2={pipelinePhase2} eliminateCandidateRow={eliminateCandidateRow} phase2CoalescingMs={phaseTwoCoalescingMs} totalDurationSec={totalDurationSec}");
Console.WriteLine($"[silo] auth={(string.IsNullOrEmpty(storageConn) ? $"managed-identity {storageUri}" : "connection-string")}");

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
builder.Logging.SetMinimumLevel(LogLevel.Warning);
// Diagnostic verbosity for the surfaces most likely to reveal a WAL
// replay / activation fault. The bench's normal output is one progress
// line per second, so a handful of additional Information-level lines
// from these categories costs nothing and is the only way to capture
// an OnActivateAsync exception stack from inside the silo (Orleans
// wraps the original exception in "Unable to create local activation"
// at the rejection seam, and the underlying cause only appears as an
// Information / Warning line from the runtime's activation directory).
builder.Logging.AddFilter("Orleans.Lattice.Storage.AzureTable", LogLevel.Information);
builder.Logging.AddFilter("Orleans.Runtime.Catalog", LogLevel.Information);
builder.Logging.AddFilter("Orleans.Runtime.ActivationData", LogLevel.Information);
// Suppress two categories that emit a Warning per in-flight grain call
// during the post-FINAL drain window (the host is stopping, activations
// are being destroyed, the placement directory has been torn down). The
// underlying behaviour is expected shutdown back-pressure; suppressing
// these makes the bench log readable without hiding a real fault - any
// pre-shutdown forwarding/placement issue would still surface elsewhere
// in the log (e.g. as an exception from TcpIngestService's own handler).
builder.Logging.AddFilter("Orleans.Messaging", LogLevel.Error);
builder.Logging.AddFilter("Orleans.Runtime.Placement.PlacementService", LogLevel.Error);

builder.Services.AddHostedService<TcpIngestService>();
builder.Services.AddHostedService<VehicleFleetSimulator.AzureThroughput.Silo.PhaseADiagnosticReporter>();
builder.Services.AddSingleton(new IngestSettings(treeId, tcpPort, batchSize, TimeSpan.FromMilliseconds(flushMs), TimeSpan.FromSeconds(reportSec), flushConcurrency, shardCountOverride));

builder.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(o =>
    {
        o.ClusterId = "azure-throughput";
        o.ServiceId = "azure-throughput";
    });

    // In-memory single-silo clustering: no Azure Storage clustering table, no peer discovery.
    silo.UseLocalhostClustering();

    // Reminders: LatticeGrain.EnsureCompactionReminderAsync() registers a reminder on the
    // first write, so a reminder service must be wired even on a single-silo benchmark.
    // The in-memory reminder table is fine here - the harness is short-lived and the
    // compaction reminder is purely opportunistic.
    silo.UseInMemoryReminderService();

    silo.AddMemoryGrainStorageAsDefault();
    silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));

    // Fan WAL throughput across N independent per-shard WalShardGrain
    // activations - each one hits its own Azure Tables manifest
    // partition (`_m_|{treeId}|{shardIndex}`) and gets its own
    // PhaseTwoWorker, so the foreground SetManyAsync fan-out's flush
    // concurrency actually maps to N parallel Azure-side commits
    // instead of serialising behind a single WAL grain's turn.
    //
    // WalMaxPendingBatches also raises the per-WalShardGrain pipeline
    // depth from the library's wire-compat default of 1 so each
    // partition can have multiple appends in flight against Azure
    // Tables (offset assignment is still serialised under the grain
    // turn; only the AppendBatchAsync RPCs overlap).
    silo.ConfigureLattice(treeId, o =>
    {
        o.WalPartitions = walPartitions;
        o.WalMaxPendingBatches = walMaxPending;
    });

    silo.AddAzureTableWalStorage(o =>
    {
        if (!string.IsNullOrWhiteSpace(storageConn))
        {
            o.ConnectionString = storageConn;
        }
        else
        {
            o.ServiceUri = new Uri(storageUri!);
            o.TokenCredential = new DefaultAzureCredential();
        }
        o.TableName = walTable;
        o.PipelinePhaseTwoCommits = pipelinePhase2;
        // Opt-in to the phase-0 candidate-row elision optimisation. The library
        // default is off for wire-compat; enabling here lets the benchmark A/B
        // the hot-path candidate-row write against real Azure Tables.
        o.EliminateCandidateRowOnHotPath = eliminateCandidateRow;
        // U9c probe lever. Default 0 = drain-on-first-signal (library default).
        // A small positive window lets the per-shard PhaseTwoWorker wait briefly
        // after the first arrival so additional commits coalesce into the same
        // Azure Tables transaction; without this, provider.phase2.batch_size
        // stays pinned at 1.00 whenever per-partition arrival inter-spacing
        // exceeds the commit's own duration.
        o.PhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(phaseTwoCoalescingMs);
    });
});

var host = builder.Build();

// Server-side watchdog: if BENCH_TOTAL_DURATION_SEC > 0, schedule a graceful
// IHostApplicationLifetime.StopApplication() once that wall-clock window
// elapses. This is the only stop signal that survives a local deploy-shell
// crash; the ACI container group is configured with restartPolicy: Never,
// so a clean host exit transitions the group to Terminated and stops
// billing for the bench compute.
if (totalDurationSec > 0)
{
    var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
    _ = Task.Run(async () =>
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(totalDurationSec), lifetime.ApplicationStopping);
            Console.WriteLine($"[silo] watchdog: BENCH_TOTAL_DURATION_SEC={totalDurationSec}s elapsed; requesting graceful shutdown.");
            lifetime.StopApplication();
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown raced the watchdog - nothing to do.
        }
    });
}

await host.RunAsync();

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

static bool ReadBool(string name, bool @default)
{
    var raw = Environment.GetEnvironmentVariable(name);
    if (string.IsNullOrWhiteSpace(raw)) return @default;
    if (bool.TryParse(raw, out var b)) return b;
    // Accept 1/0 / yes/no shorthand for convenience in deployment scripts.
    return raw.Trim() switch
    {
        "1" => true,
        "0" => false,
        "yes" or "YES" or "Yes" or "y" or "Y" => true,
        "no" or "NO" or "No" or "n" or "N" => false,
        _ => @default,
    };
}

internal sealed record IngestSettings(string TreeId, int TcpPort, int BatchSize, TimeSpan FlushInterval, TimeSpan ReportInterval, int FlushConcurrency, int ShardCountOverride);

internal sealed class TcpIngestService(
    IGrainFactory grainFactory,
    IngestSettings settings,
    IHostApplicationLifetime lifetime,
    ILogger<TcpIngestService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Diagnostic: print what the HOSTED SERVICE actually received,
        // not just what Program.cs parsed at boot. If lat01 is running a
        // stale binary this line will surface it - the values here are
        // the ones that govern every flush dispatch, so they must match
        // the Program.cs "[silo]" startup line. The build stamp is the
        // assembly's BuiltAt UTC so we can prove which binary is running
        // even if the env-var defaults haven't changed across builds.
        var asm = typeof(TcpIngestService).Assembly;
        var asmLoc = asm.Location;
        var builtAtUtc = string.IsNullOrEmpty(asmLoc) ? "unknown" : File.GetLastWriteTimeUtc(asmLoc).ToString("yyyy-MM-ddTHH:mm:ssZ");
        Console.WriteLine($"[silo:ingest] settings.BatchSize={settings.BatchSize} settings.FlushConcurrency={settings.FlushConcurrency} settings.FlushInterval={settings.FlushInterval.TotalMilliseconds:F0}ms settings.ShardCountOverride={settings.ShardCountOverride} treeId={settings.TreeId} asm={Path.GetFileName(asmLoc)} builtAtUtc={builtAtUtc}");

        var lattice = grainFactory.GetGrain<ILattice>(settings.TreeId);

        // Optional one-shot reshard at silo startup.
        //
        // ShardCount is not a LatticeOptions field - it's pinned in the tree
        // registry at first-use - so the only way to push the bench above
        // the library default (64) is to call ReshardAsync once. A 0
        // override means "keep whatever is pinned".
        //
        // Important: ReshardAsync returns as soon as the coordinator has
        // accepted the request. For a non-empty tree the actual slot
        // migration then runs in the background driven by reminders. We
        // MUST poll IsReshardCompleteAsync before opening the TCP listener -
        // otherwise the benchmark's first writes race the migration and
        // get swallowed by StaleTreeRoutingException retries, masking the
        // very throughput number we're trying to measure.
        //
        // For a freshly-registered/empty tree (the typical bench start
        // state) ReshardAsync takes the empty-tree fast-path and returns
        // synchronously already complete, so the poll is a no-op.
        if (settings.ShardCountOverride > 0)
        {
            // The very first call into a freshly-activated LatticeGrain
            // races the Orleans client directory cache and routinely fails
            // with OrleansMessageRejectionException ("Unable to create
            // local activation" / "to invalid activation. Rejecting
            // now."). The directory recovers on its own within a few
            // hundred milliseconds, but a single un-retried call here
            // silently leaves the tree pinned at the library default
            // shard count (64) instead of the configured override - the
            // bench then measures the wrong configuration. Retry the
            // submit a few times on rejection and emit a loud, greppable
            // ERROR line if every attempt fails.
            const int MaxReshardAttempts = 4;
            var attempt = 0;
            var reshardSubmitted = false;
            Exception? lastReshardException = null;
            while (attempt < MaxReshardAttempts && !reshardSubmitted && !stoppingToken.IsCancellationRequested)
            {
                attempt++;
                try
                {
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} -> shardCount={settings.ShardCountOverride} (submit attempt={attempt}/{MaxReshardAttempts})");
                    await lattice.ReshardAsync(settings.ShardCountOverride, stoppingToken).ConfigureAwait(false);
                    reshardSubmitted = true;
                }
                catch (ArgumentOutOfRangeException ex)
                {
                    // Grow-only violation (target <= current shard count on a
                    // populated tree) or above the virtual-shard-space ceiling.
                    // Not retriable - log and continue with the existing pinned
                    // shard count.
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} rejected: {ex.Message}");
                    lastReshardException = ex;
                    break;
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex) when (IsOrleansMessageRejection(ex))
                {
                    lastReshardException = ex;
                    var backoffMs = 100 * (1 << (attempt - 1));
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} attempt={attempt} REJECTED ({ex.GetType().Name}: {Truncate(ex.Message, 160)}); backing off {backoffMs}ms before retry");
                    try
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(backoffMs), stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) { throw; }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} FAILED: {ex.GetType().Name}: {ex.Message}");
                    lastReshardException = ex;
                    break;
                }
            }

            if (!reshardSubmitted)
            {
                // Loud, greppable failure line. The harness must see this
                // and treat the run as misconfigured rather than silently
                // measuring the default shard count.
                var detail = lastReshardException is null
                    ? "no exception captured"
                    : $"{lastReshardException.GetType().Name}: {Truncate(lastReshardException.Message, 240)}";
                Console.WriteLine($"[silo] ERROR reshard treeId={settings.TreeId} ABORTED after {attempt} attempt(s): {detail}. Tree remains at its previously-pinned shard count (likely the library default, NOT shardCount={settings.ShardCountOverride}).");
            }
            else
            {
                // Bound the wait so a stuck reshard logs and continues
                // rather than wedging the silo permanently. 5 min is
                // generous for the bench's tree sizes; production callers
                // would size this against their data volume.
                var deadline = DateTime.UtcNow.AddMinutes(5);
                while (true)
                {
                    bool complete;
                    try
                    {
                        complete = await lattice.IsReshardCompleteAsync(stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex) when (IsOrleansMessageRejection(ex))
                    {
                        // Same directory-cache race can hit the very first
                        // IsReshardCompleteAsync. Treat as "not yet
                        // complete", wait, and try again on the next loop.
                        Console.WriteLine($"[silo] reshard treeId={settings.TreeId} IsReshardCompleteAsync rejected ({ex.GetType().Name}); retrying");
                        complete = false;
                    }
                    if (complete)
                    {
                        Console.WriteLine($"[silo] reshard treeId={settings.TreeId} complete");
                        break;
                    }
                    if (DateTime.UtcNow >= deadline)
                    {
                        Console.WriteLine($"[silo] reshard treeId={settings.TreeId} TIMEOUT - migration still in progress, continuing anyway");
                        break;
                    }
                    Console.WriteLine($"[silo] reshard treeId={settings.TreeId} in progress, waiting...");
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
                }
            }
        }

        // Drain channel: each connection writes into the same shared channel; a single drain
        // task batches and pushes into ILattice.SetManyAsync. One reader keeps the rate
        // reporter and the flush cadence trivially monotonic.
        var channel = Channel.CreateBounded<KeyValuePair<string, byte[]>>(new BoundedChannelOptions(capacity: 1 << 16)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

        var drainTask = Task.Run(() => DrainAsync(lattice, channel.Reader, stoppingToken), CancellationToken.None);

        var listener = new TcpListener(IPAddress.Any, settings.TcpPort);
        listener.Start();
        logger.LogInformation("[silo] tcp listener on :{Port}", settings.TcpPort);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await listener.AcceptTcpClientAsync(stoppingToken);
                }
                catch (OperationCanceledException) { break; }

                _ = Task.Run(() => HandleConnectionAsync(client, channel.Writer, stoppingToken), CancellationToken.None);
            }
        }
        finally
        {
            listener.Stop();
            channel.Writer.TryComplete();
            try { await drainTask; } catch { /* swallow on shutdown */ }
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, ChannelWriter<KeyValuePair<string, byte[]>> writer, CancellationToken ct)
    {
        var remote = client.Client.RemoteEndPoint?.ToString() ?? "?";
        Console.WriteLine($"[silo] accepted {remote}");
        var treeTag = new KeyValuePair<string, object?>("tree", settings.TreeId);
        try
        {
            using (client)
            await using (var stream = client.GetStream())
            using (var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 64 * 1024))
            {
                string? line;
                while ((line = await reader.ReadLineAsync(ct)) is not null)
                {
                    if (line.Length == 0) continue;

                    VehicleTelemetryEvent ev;
                    try
                    {
                        ev = JsonSerializer.Deserialize<VehicleTelemetryEvent>(line);
                    }
                    catch (JsonException)
                    {
                        continue;
                    }

                    var key = ev.VehicleId.ToString("N");
                    var value = Encoding.UTF8.GetBytes(line);

                    BenchMetrics.TcpReadLineBytes.Record(value.Length, treeTag);

                    // Time the ChannelWriter.WriteAsync separately so we
                    // can distinguish "TCP read loop is the bottleneck"
                    // (write completes immediately) from "drain is the
                    // bottleneck" (write blocks because the bounded
                    // channel is full). This is the U9o step-2 probe:
                    // it sits exactly between the TCP socket and the
                    // lattice flush dispatcher.
                    var startTs = Stopwatch.GetTimestamp();
                    await writer.WriteAsync(new KeyValuePair<string, byte[]>(key, value), ct);
                    var waitMs = Stopwatch.GetElapsedTime(startTs).TotalMilliseconds;
                    BenchMetrics.TcpReadChannelWriteWaitMs.Record(waitMs, treeTag);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException ex)
        {
            logger.LogWarning(ex, "[silo] connection {Remote} dropped", remote);
        }
        Console.WriteLine($"[silo] closed {remote}");
    }

    private async Task DrainAsync(ILattice lattice, ChannelReader<KeyValuePair<string, byte[]>> reader, CancellationToken ct)
    {
        // Concurrent flush model: the drain loop fills a working batch
        // and, when the batch is full or the flush deadline elapses,
        // hands the batch off to a background flush task and starts a
        // fresh batch immediately. A SemaphoreSlim caps the number of
        // in-flight SetManyAsync calls at `FlushConcurrency` so we
        // don't unboundedly queue against the silo / WAL. The previous
        // single-flusher shape serialised every batch behind one
        // outstanding SetManyAsync, which capped throughput at
        // (1 / per-call-latency) regardless of how cheap the per-key
        // work was. Lifting the in-flight cap exposes the WAL's
        // phase-2 worker to coalesce opportunities (up to 49 batches
        // per phase-2 transaction) and lets the leaf's batched
        // commit-log seam actually run in parallel against
        // independent shards / partitions.
        var startedAt = Stopwatch.GetTimestamp();
        long writtenTotal = 0;
        long writtenSinceReport = 0;
        long failedTotal = 0;
        long failedSinceReport = 0;
        long inFlight = 0;

        using var flushGate = new SemaphoreSlim(settings.FlushConcurrency, settings.FlushConcurrency);
        var flushTasks = new HashSet<Task>();
        var firstDispatchLogged = 0;

        // Local helper: awaits a flush slot, then schedules the
        // SetManyAsync call on the threadpool and returns. Returning
        // the Task lets the caller decide whether to await commit
        // ordering or just track it for the FINAL drain. Crucially,
        // we `await flushGate.WaitAsync` BEFORE scheduling the work
        // and BEFORE returning, so the drain loop naturally stalls
        // when `FlushConcurrency` flushes are already in flight. The
        // semaphore is released inside the running task once
        // SetManyAsync completes (or faults), so the next caller's
        // `WaitAsync` unblocks at the right moment.
        async Task<Task> DispatchFlushAsync(List<KeyValuePair<string, byte[]>> batchToFlush)
        {
            var gateWaitStart = Stopwatch.GetTimestamp();
            await flushGate.WaitAsync(ct).ConfigureAwait(false);
            var gateWaitMs = Stopwatch.GetElapsedTime(gateWaitStart).TotalMilliseconds;
            var treeTag = new KeyValuePair<string, object?>("tree", settings.TreeId);
            BenchMetrics.DrainFlushDispatchWaitMs.Record(gateWaitMs, treeTag);
            BenchMetrics.DrainFlushDispatchSize.Record(batchToFlush.Count, treeTag);
            Interlocked.Increment(ref inFlight);
            if (Interlocked.Exchange(ref firstDispatchLogged, 1) == 0)
            {
                // One-shot: prove what the very first SetManyAsync call
                // actually got. Configured batch size is the cap; the
                // first batch may be smaller if a flush deadline hit
                // before the batch filled, so we log both.
                Console.WriteLine($"[silo:ingest] first dispatch entries={batchToFlush.Count} (configured BatchSize={settings.BatchSize})");
            }
            return Task.Run(async () =>
            {
                try
                {
                    var committed = await FlushAsync(lattice, batchToFlush, ct).ConfigureAwait(false);
                    if (committed == ShutdownDiscarded)
                    {
                        // Neither accepted nor failed - shutdown back-pressure.
                    }
                    else
                    {
                        Interlocked.Add(ref writtenTotal, committed);
                        Interlocked.Add(ref writtenSinceReport, committed);
                        var failed = batchToFlush.Count - committed;
                        if (failed > 0)
                        {
                            Interlocked.Add(ref failedTotal, failed);
                            Interlocked.Add(ref failedSinceReport, failed);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Shutdown path - failures already accounted for.
                }
                finally
                {
                    Interlocked.Decrement(ref inFlight);
                    flushGate.Release();
                }
            }, CancellationToken.None);
        }

        // Reporter task: samples Interlocked counters on the report
        // cadence so a stalled drain loop (e.g. all flushers blocked
        // on the WAL) still produces a progress line. Cleanly exits
        // when `ct` is cancelled.
        var reporterCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var reporterTask = Task.Run(async () =>
        {
            var lastReport = Stopwatch.GetTimestamp();
            try
            {
                while (!reporterCts.IsCancellationRequested)
                {
                    await Task.Delay(settings.ReportInterval, reporterCts.Token).ConfigureAwait(false);
                    var now = Stopwatch.GetTimestamp();
                    var sinceLocal = now - lastReport;
                    var written = Interlocked.Exchange(ref writtenSinceReport, 0);
                    var failed = Interlocked.Exchange(ref failedSinceReport, 0);
                    var inFlightNow = Interlocked.Read(ref inFlight);
                    var totalNow = Interlocked.Read(ref writtenTotal);
                    var rate = written / Math.Max(0.001, sinceLocal / (double)Stopwatch.Frequency);
                    var elapsed = (now - startedAt) / (double)Stopwatch.Frequency;
                    var failedTag = failed > 0 ? $" failed={failed,8:N0}" : string.Empty;
                    Console.WriteLine($"[silo] t={elapsed,7:0.0}s written={totalNow,12:N0} Entries written per second={rate,10:N0} inFlight={inFlightNow,3}{failedTag}");
                    lastReport = now;
                }
            }
            catch (OperationCanceledException) { }
        }, CancellationToken.None);

        var batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
        var nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);

        // Track in-flight flush tasks so the FINAL line can wait for
        // them all to drain. Add/Remove happen on different threads
        // (drain loop vs flush-completion continuations) so every
        // mutation is under the set's own lock.
        void TrackFlush(Task task)
        {
            lock (flushTasks) { flushTasks.Add(task); }
            _ = task.ContinueWith(t => { lock (flushTasks) { flushTasks.Remove(t); } }, TaskScheduler.Default);
        }

        try
        {
            while (await reader.WaitToReadAsync(ct))
            {
                while (reader.TryRead(out var entry))
                {
                    batch.Add(entry);
                    if (batch.Count >= settings.BatchSize)
                    {
                        var ready = batch;
                        batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
                        // `DispatchFlushAsync` awaits the semaphore so the
                        // drain loop pauses here until a flush slot is
                        // free. That propagates backpressure all the way
                        // up: the channel fills, the TCP reader's
                        // `WriteAsync` blocks, and the producer slows
                        // to the silo's actual write rate. The previous
                        // shape created a Task per batch unconditionally
                        // and only awaited the gate inside the Task,
                        // which let the threadpool accumulate thousands
                        // of pending flush tasks while the silo plodded
                        // along at its own rate.
                        var flushTask = await DispatchFlushAsync(ready);
                        TrackFlush(flushTask);
                        nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);
                    }
                }

                if (batch.Count > 0 && Stopwatch.GetTimestamp() >= nextFlush)
                {
                    var ready = batch;
                    batch = new List<KeyValuePair<string, byte[]>>(settings.BatchSize);
                    var flushTask = await DispatchFlushAsync(ready);
                    TrackFlush(flushTask);
                    nextFlush = Stopwatch.GetTimestamp() + (long)(settings.FlushInterval.TotalSeconds * Stopwatch.Frequency);
                }
            }

            if (batch.Count > 0)
            {
                var ready = batch;
                batch = new List<KeyValuePair<string, byte[]>>(0);
                var flushTask = await DispatchFlushAsync(ready);
                TrackFlush(flushTask);
            }
        }
        catch (OperationCanceledException) { }

        // Drain in-flight flushes so the FINAL line reflects everything
        // that was accepted from the channel.
        Task[] outstanding;
        lock (flushTasks)
        {
            outstanding = new Task[flushTasks.Count];
            flushTasks.CopyTo(outstanding);
        }
        try { await Task.WhenAll(outstanding); } catch { /* per-task failures already accounted for */ }

        reporterCts.Cancel();
        try { await reporterTask; } catch { /* shutdown */ }
        reporterCts.Dispose();

        var totalElapsed = (Stopwatch.GetTimestamp() - startedAt) / (double)Stopwatch.Frequency;
        var writtenFinal = Interlocked.Read(ref writtenTotal);
        var failedFinal = Interlocked.Read(ref failedTotal);
        Console.WriteLine($"[silo] FINAL written={writtenFinal:N0} failed={failedFinal:N0} elapsed={totalElapsed:0.0}s Entries written per second (avg)={writtenFinal / Math.Max(0.001, totalElapsed):N0}");
    }

    // Sentinel returned by FlushAsync when a SetManyAsync was rejected
    // because the silo is draining at shutdown. The dispatcher treats it
    // as "neither accepted nor failed" - the in-flight batch raced the
    // drain after the producer closed the socket and is correctly not
    // counted in either the written or the failed total.
    private const int ShutdownDiscarded = -1;

    private async Task<int> FlushAsync(ILattice lattice, List<KeyValuePair<string, byte[]>> batch, CancellationToken ct)
    {
        var startTs = Stopwatch.GetTimestamp();
        try
        {
            await lattice.SetManyAsync(batch, ct);
            var elapsedMs = Stopwatch.GetElapsedTime(startTs).TotalMilliseconds;
            BenchMetrics.LatticeSetManyDurationMs.Record(
                elapsedMs,
                new KeyValuePair<string, object?>("tree", settings.TreeId));
            return batch.Count;
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex) when (lifetime.ApplicationStopping.IsCancellationRequested && IsShutdownRejection(ex))
        {
            // Expected: producer closed the socket, the silo emitted its
            // FINAL line, and the host is now draining grain activations.
            // Any in-flight SetManyAsync that races the drain gets an
            // OrleansMessageRejectionException ("Unable to create local
            // activation" / "silo is blocking application messages"). The
            // entries those batches carried were never accepted by the
            // lattice so they are correctly not in `written`; they should
            // also not be in `failed`, because they are not a real
            // ingestion failure - they are shutdown back-pressure. Return
            // the sentinel so the dispatcher skips both counters.
            return ShutdownDiscarded;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[silo] flush of {Count} failed", batch.Count);
            return 0;
        }
    }

    private static bool IsShutdownRejection(Exception ex)
    {
        // Match the two messages Orleans emits when an activation cannot
        // be created because the silo is draining. Type-name match keeps
        // the check resilient to Orleans internalising the type.
        var typeName = ex.GetType().FullName ?? string.Empty;
        if (!typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal))
        {
            return false;
        }
        var msg = ex.Message ?? string.Empty;
        return msg.Contains("Unable to create local activation", StringComparison.Ordinal)
            || msg.Contains("silo is blocking application messages", StringComparison.Ordinal)
            || msg.Contains("to invalid activation", StringComparison.Ordinal);
    }

    // Type-name match for any Orleans message-rejection exception. Used
    // by the startup reshard retry: a brand-new silo's first call to a
    // never-activated grain races the client directory cache and lands
    // here, but the directory recovers within a few hundred ms and a
    // retry succeeds. Keeping this separate from IsShutdownRejection
    // makes the retry safe to use before the silo is anywhere near
    // shutdown.
    private static bool IsOrleansMessageRejection(Exception ex)
    {
        var typeName = ex.GetType().FullName ?? string.Empty;
        return typeName.Contains("OrleansMessageRejectionException", StringComparison.Ordinal);
    }

    private static string Truncate(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return string.Empty;
        return s.Length <= max ? s : s.Substring(0, max) + "...";
    }
}
