using System.Diagnostics;
using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Samples.ClusterScaling.LoadDriver;
using Orleans.Serialization;

// ---------------------------------------------------------------------------
// ClusterScaling.LoadDriver - the bundled compute-axis load generator for the
// ClusterScaling ACA sample.
//
// PowerShell has no first-class gRPC client, so drive-load.ps1 wraps this small
// console. It connects to the deployed data-API gRPC surface over TLS (the ACA
// managed ingress), presents the admin Basic credential, and drives a sustained,
// high-cardinality write/read mix at a configurable offered rate and duration.
//
// The load is deliberately COMPUTE-axis, not storage-axis: it spreads a high op
// rate across many distinct trees and keys (activation + dispatch pressure) with
// a small fixed payload, so the scaling signal's compute-derived scaleValue
// climbs and ACA/KEDA scales replicas OUT. Bulk retained bytes would move the
// storage axis, which is advisory and never inflates replica count - so this
// driver keeps payloads tiny on purpose.
// ---------------------------------------------------------------------------

var options = LoadDriverOptions.Parse(args);
if (options is null)
{
    LoadDriverOptions.PrintUsage();
    return 2;
}

Console.WriteLine("== ClusterScaling load driver ==");
Console.WriteLine($"  target    : {options.Address}");
Console.WriteLine($"  user      : {options.Username}");
Console.WriteLine($"  rate      : {options.OfferedRatePerSecond:N0} ops/sec (offered)");
Console.WriteLine($"  duration  : {options.Duration.TotalSeconds:N0}s");
Console.WriteLine($"  trees     : {options.TreeCount}   keyspace: {options.KeySpace:N0}   read ratio: {options.ReadRatio:P0}");
Console.WriteLine($"  payload   : {options.PayloadBytes} bytes (small on purpose: this drives COMPUTE, not storage)");
Console.WriteLine();

// Orleans serialization drives the gRPC wire marshallers; the provider must have
// the data-API assembly's generated serializers available (AddSerializer scans
// the loaded assemblies, and referencing the wire records loads this one).
using var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

var channelOptions = new GrpcChannelOptions();

using var channel = GrpcChannel.ForAddress(options.Address, channelOptions);

// Present the admin Basic credential on every call. Basic-over-TLS is legitimate
// here because the ACA ingress terminates TLS: the header rides an encrypted
// channel end to end.
var basicHeader = "Basic " + Convert.ToBase64String(
    Encoding.UTF8.GetBytes($"{options.Username}:{options.Password}"));
var invoker = channel.CreateCallInvoker().Intercept(metadata =>
{
    metadata.Add("authorization", basicHeader);
    return metadata;
});
var client = LatticeDataApiGrpcClient.Create(invoker, serializerProvider);

// A small fixed payload reused across every write. Reusing one buffer keeps the
// driver from allocating per op and keeps the storage axis flat.
var payload = new byte[options.PayloadBytes];
Random.Shared.NextBytes(payload);

// --- Fail fast on an unauthenticated / wrong-password deployment -------------
try
{
    await client.SetAsync(
        new DataSetRequest { TreeId = TreeId(0, options), Key = "warmup", Value = payload });
}
catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
{
    Console.Error.WriteLine(
        "FATAL: the data API rejected the admin credential (PermissionDenied). " +
        "Check the -AdminPassword matches the one deploy.ps1 hashed into the ACA secret.");
    return 3;
}
catch (RpcException ex)
{
    Console.Error.WriteLine($"FATAL: could not reach the data API ({ex.StatusCode}): {ex.Status.Detail}");
    return 3;
}

Console.WriteLine("Warmup call authenticated. Driving load...\n");

// --- Metrics -----------------------------------------------------------------
long offered = 0;
long completed = 0;
long failed = 0;
var inFlight = new SemaphoreSlim(options.MaxInFlight);

using var driveCts = new CancellationTokenSource(options.Duration);
var driveToken = driveCts.Token;

// Progress reporter: prints an offered-load line every second so the operator
// sees continuous offered throughput alongside the replica-count timeline that
// drive-load.ps1 prints from `az`.
var reporter = Task.Run(async () =>
{
    var sw = Stopwatch.StartNew();
    var lastOffered = 0L;
    var lastElapsed = 0.0;
    try
    {
        while (!driveToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), driveToken).ConfigureAwait(false);
            var elapsed = sw.Elapsed.TotalSeconds;
            var offeredNow = Interlocked.Read(ref offered);
            var windowRate = (offeredNow - lastOffered) / Math.Max(0.001, elapsed - lastElapsed);
            Console.WriteLine(
                $"  t={elapsed,6:0.0}s  offered={offeredNow,10:N0}  offered/s={windowRate,9:N0}  " +
                $"completed={Interlocked.Read(ref completed),10:N0}  failed={Interlocked.Read(ref failed),7:N0}  " +
                $"inFlight={options.MaxInFlight - inFlight.CurrentCount,4}");
            lastOffered = offeredNow;
            lastElapsed = elapsed;
        }
    }
    catch (OperationCanceledException)
    {
        // duration elapsed
    }
});

// --- Pacing loop -------------------------------------------------------------
// Issue ops at the offered rate using a wall-clock schedule. Each op is started
// under a bounded in-flight semaphore so a lagging cluster (mid scale-out) does
// not let unbounded work pile up, while the offered counter still advances at
// the target cadence so the reported offered rate reflects intent, not the
// cluster's current capacity.
var interval = TimeSpan.FromSeconds(1.0 / options.OfferedRatePerSecond);
var startedAt = Stopwatch.StartNew();
long seq = 0;
var tasks = new List<Task>();

try
{
    while (!driveToken.IsCancellationRequested)
    {
        var due = interval * seq;
        var now = startedAt.Elapsed;
        if (due > now)
        {
            var wait = due - now;
            if (wait > TimeSpan.FromMilliseconds(1))
            {
                try
                {
                    await Task.Delay(wait, driveToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        var opIndex = seq;
        seq++;
        Interlocked.Increment(ref offered);

        await inFlight.WaitAsync(driveToken).ConfigureAwait(false);
        tasks.Add(IssueAsync(opIndex));

        // Periodically prune completed tasks so the list does not grow for the
        // whole run.
        if (tasks.Count >= 4096)
        {
            tasks.RemoveAll(t => t.IsCompleted);
        }
    }
}
catch (OperationCanceledException)
{
    // duration elapsed
}

await Task.WhenAll(tasks).ConfigureAwait(false);
await reporter.ConfigureAwait(false);

var totalElapsed = startedAt.Elapsed.TotalSeconds;
var offeredFinal = Interlocked.Read(ref offered);
var completedFinal = Interlocked.Read(ref completed);
var failedFinal = Interlocked.Read(ref failed);

Console.WriteLine();
Console.WriteLine("== FINAL ==");
Console.WriteLine($"  elapsed        : {totalElapsed:0.0}s");
Console.WriteLine($"  offered        : {offeredFinal:N0} ops ({offeredFinal / Math.Max(0.001, totalElapsed):N0}/s avg)");
Console.WriteLine($"  completed      : {completedFinal:N0} ops ({completedFinal / Math.Max(0.001, totalElapsed):N0}/s avg)");
Console.WriteLine($"  failed         : {failedFinal:N0} ops");
Console.WriteLine();
Console.WriteLine("Sustained offered load past the KEDA polling + cooldown + EWMA window means");
Console.WriteLine("ACA should have added replicas during the run. drive-load.ps1 prints the");
Console.WriteLine("replica-count timeline from `az` so you can see the scale-out and scale-in.");

return 0;

// --- op body ----------------------------------------------------------------
async Task IssueAsync(long opIndex)
{
    try
    {
        var treeId = TreeId(opIndex, options);
        // Spread across the keyspace so many distinct leaf grains activate.
        var key = "k-" + (opIndex % options.KeySpace).ToString("D9");
        var isRead = options.ReadRatio > 0 &&
            (opIndex % 100) < (long)Math.Round(options.ReadRatio * 100);

        if (isRead)
        {
            _ = await client.GetAsync(new DataGetRequest { TreeId = treeId, Key = key }).ConfigureAwait(false);
        }
        else
        {
            _ = await client.SetAsync(
                new DataSetRequest { TreeId = treeId, Key = key, Value = payload }).ConfigureAwait(false);
        }

        Interlocked.Increment(ref completed);
    }
    catch (RpcException)
    {
        Interlocked.Increment(ref failed);
    }
    catch (OperationCanceledException)
    {
        Interlocked.Increment(ref failed);
    }
    finally
    {
        inFlight.Release();
    }
}

static string TreeId(long opIndex, LoadDriverOptions options) =>
    "tree-" + (opIndex % options.TreeCount).ToString("D3");
