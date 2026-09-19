using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// Sidecar driver for the registry fan-in measurement rig.
/// <para>
/// It joins the silo's cluster as an ordinary Orleans client and addresses
/// <see cref="ILattice"/> and <see cref="ILatticeRegistry"/> directly. Going
/// through an MCP or HTTP surface instead would interpose that surface's own
/// queueing and retry behaviour between the driver and the seam under study, so
/// a measured stall could no longer be attributed to the registry.
/// </para>
/// <para>
/// It must run inside the silo container's network namespace
/// (<c>docker run --network container:&lt;id&gt;</c>), because the host binds
/// localhost clustering to the loopback interface INSIDE the container: Docker
/// port publishing maps to the container's ethernet interface, not its loopback,
/// so a published gateway port cannot reach a loopback-bound gateway at all.
/// </para>
/// </summary>
internal static class Program
{
    private static readonly JsonSerializerOptions ReportJson = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    private static async Task<int> Main(string[] args)
    {
        DriverOptions options;
        try
        {
            options = DriverOptions.Parse(args);
        }
        catch (ArgumentException ex)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine();
            Console.Error.WriteLine(DriverOptions.Usage);
            return 2;
        }

        if (options.Verb is "help" or "--help" or "-h")
        {
            Console.WriteLine(DriverOptions.Usage);
            return 0;
        }

        using var lifetime = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            lifetime.Cancel();
        };

        using var host = BuildHost(options);
        if (!await ConnectAsync(host, options, lifetime.Token).ConfigureAwait(false))
        {
            return 3;
        }

        var grains = host.Services.GetRequiredService<IGrainFactory>();
        var fleet = new TreeFleet(grains, options.Prefix);
        var census = new CallCensus();
        var notes = new List<string>();
        var startedAt = DateTimeOffset.UtcNow;
        var exitCode = 0;
        EstateCensus? estate = null;

        // The credential is scoped around the WHOLE workload rather than per
        // call. It rides Orleans' RequestContext, which is ambient and flows
        // into every continuation the workload spawns, so one scope covers the
        // parallel sweeps without threading anything through.
        using var credential = DriverCredential.Use(options.Subject, options.Scheme);

        try
        {
            switch (options.Verb)
            {
                case "create":
                    await fleet.CreateAsync(options.Trees, census, options.Parallelism, lifetime.Token).ConfigureAwait(false);
                    notes.Add($"created {options.Trees} trees with prefix '{options.Prefix}'");
                    break;

                case "populate":
                    var written = await fleet.PopulateAsync(
                        options.Trees,
                        options.LeavesPerTree,
                        options.KeysPerLeaf,
                        options.ValueBytes,
                        options.BatchSize,
                        census,
                        options.Parallelism,
                        lifetime.Token).ConfigureAwait(false);
                    var approxBytes = written * options.ValueBytes;
                    notes.Add(
                        $"wrote {written} entries of {options.ValueBytes}B across {options.Trees} trees " +
                        $"(target {options.LeavesPerTree} leaves/tree at {options.KeysPerLeaf} keys/leaf; " +
                        $"~{approxBytes / (1024d * 1024d):F1} MiB of values)");
                    estate = await fleet.CountAsync(options.Trees, options.KeysPerLeaf, census, options.Parallelism, lifetime.Token).ConfigureAwait(false);
                    break;

                case "census":
                    var counted = await fleet.CountAsync(options.Trees, options.KeysPerLeaf, census, options.Parallelism, lifetime.Token).ConfigureAwait(false);
                    estate = counted;
                    notes.Add(
                        $"{counted.Trees} trees hold {counted.TotalEntries} entries " +
                        $"(>= {counted.ImpliedLeaves} leaves at {counted.KeysPerLeaf} keys/leaf); " +
                        $"per-tree entries {counted.MinEntries}..{counted.MaxEntries}; " +
                        $"{counted.FailedTrees} counts did not return");
                    break;

                case "load":
                    notes.Add(await RunLoadAsync(grains, fleet, options, census, lifetime.Token).ConfigureAwait(false));
                    break;

                case "probe":
                    notes.Add(await RunProbeAsync(grains, fleet, options, census, lifetime.Token).ConfigureAwait(false));
                    break;

                case "teardown":
                    var residue = await fleet.TeardownAsync(options.Trees, census, options.Parallelism, lifetime.Token).ConfigureAwait(false);
                    notes.Add($"tore down {options.Trees} trees; {residue.Count} did not tear down cleanly");
                    notes.AddRange(residue);
                    break;

                case "list":
                    var registry = grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                    var ids = await registry.GetAllTreeIdsAsync().ConfigureAwait(false);
                    notes.Add($"registry holds {ids.Count} trees");
                    notes.AddRange(ids.OrderBy(id => id, StringComparer.Ordinal));
                    break;

                default:
                    Console.Error.WriteLine($"Unrecognised verb '{options.Verb}'.");
                    Console.Error.WriteLine();
                    Console.Error.WriteLine(DriverOptions.Usage);
                    return 2;
            }
        }
        catch (OperationCanceledException)
        {
            notes.Add("cancelled");
            exitCode = 4;
        }

        var report = DriverReport.From(options.Verb, startedAt, options.Trees, options.Prefix, census, notes, estate);
        Emit(report, options.OutputPath);
        await host.StopAsync(CancellationToken.None).ConfigureAwait(false);
        return exitCode;
    }

    private static IHost BuildHost(DriverOptions options) =>
        Host.CreateDefaultBuilder()
            .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Warning))
            .UseOrleansClient(client =>
            {
                client.UseLocalhostClustering(gatewayPort: options.GatewayPort);
                client.Configure<ClusterOptions>(cluster =>
                {
                    cluster.ClusterId = options.ClusterId;
                    cluster.ServiceId = options.ServiceId;
                });
                client.Configure<ClientMessagingOptions>(messaging =>
                    messaging.ResponseTimeout = options.ResponseTimeout);
            })
            .Build();

    /// <summary>
    /// Joins the cluster, retrying until <see cref="DriverOptions.ConnectTimeout"/>.
    /// A cold-started silo is not accepting client connections for some seconds
    /// after the container reports running, and a driver that gave up on the
    /// first refusal would simply not measure the window of interest.
    /// </summary>
    private static async Task<bool> ConnectAsync(IHost host, DriverOptions options, CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + options.ConnectTimeout;
        var attempt = 0;

        while (true)
        {
            attempt++;
            try
            {
                await host.StartAsync(cancellationToken).ConfigureAwait(false);
                Console.Error.WriteLine($"joined cluster after {attempt} attempt(s)");
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (DateTimeOffset.UtcNow >= deadline)
                {
                    Console.Error.WriteLine($"could not join cluster within {options.ConnectTimeout}: {ex.Message}");
                    return false;
                }

                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Drives synthetic reads and writes against the first
    /// <see cref="DriverOptions.LoadTrees"/> trees of the fleet, to create the
    /// 'heavily loaded other trees' condition.
    /// </summary>
    private static async Task<string> RunLoadAsync(
        IGrainFactory grains,
        TreeFleet fleet,
        DriverOptions options,
        CallCensus census,
        CancellationToken cancellationToken)
    {
        var targets = fleet.TreeIds(options.LoadTrees);
        var writeEvery = options.WriteFraction <= 0 ? int.MaxValue : (int)Math.Max(1, Math.Round(1 / options.WriteFraction));

        var issued = await RateDriver.RunAsync(
            options.Rate,
            options.Duration,
            options.MaxInFlight,
            sequence =>
            {
                var treeId = targets[(int)(sequence % targets.Count)];
                var lattice = grains.GetGrain<ILattice>(treeId);
                var key = $"load/{sequence % 512:D4}";

                return sequence % writeEvery == 0
                    ? census.MeasureAsync(
                        "ILattice.SetAsync",
                        () => lattice.SetAsync(key, TreeFleet.Payload(treeId, sequence), cancellationToken))
                    : census.MeasureAsync(
                        "ILattice.GetAsync",
                        () => lattice.GetAsync(key, cancellationToken));
            },
            cancellationToken).ConfigureAwait(false);

        return $"issued {issued} load operations across {targets.Count} of {options.Trees} trees " +
               $"at {options.Rate}/s for {options.Duration.TotalSeconds}s";
    }

    /// <summary>
    /// Drives registry reads in the member mix observed in the live storm
    /// (<c>ResolveAsync</c> and <c>GetEntryAsync</c> dominant, <c>GetShardMapAsync</c>
    /// a minority), so the client-side census is comparable with the server-side
    /// timeout population.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The storm mix is made up entirely of point reads, and every point read on
    /// <c>ILatticeRegistry</c> carries <c>[AlwaysInterleave]</c>. Replaying that
    /// mix alone therefore exercises only the interleaved half of the surface and
    /// cannot reach the members most likely to be the wall.
    /// </para>
    /// <para>
    /// <see cref="DriverOptions.EnumeratePercent"/> injects
    /// <c>GetAllTreeIdsAsync</c> into the mix. That member is the one read here
    /// which is not a point lookup but a multi-hop range traversal of the
    /// registry's own backing tree, and it is excluded from interleaving on
    /// correctness grounds rather than by oversight - so it holds the singleton's
    /// turn token for the whole traversal. Its cost grows with the number of
    /// entries traversed and with the depth of the activations it descends into,
    /// which is exactly the product this rig is trying to separate.
    /// </para>
    /// <para>
    /// It defaults to zero so the storm-replica mix stays a faithful replica.
    /// Raising it is a deliberate change of question, from "does the observed mix
    /// reproduce the storm" to "does the non-interleaved member saturate first".
    /// </para>
    /// </remarks>
    private static async Task<string> RunProbeAsync(
        IGrainFactory grains,
        TreeFleet fleet,
        DriverOptions options,
        CallCensus census,
        CancellationToken cancellationToken)
    {
        var targets = fleet.TreeIds(options.Trees);
        var enumerateShare = Math.Clamp(options.EnumeratePercent, 0, 100);

        var issued = await RateDriver.RunAsync(
            options.Rate,
            options.Duration,
            options.MaxInFlight,
            sequence =>
            {
                var registry = grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                var roll = (int)(sequence % 100);

                // The enumeration branch is taken BEFORE any target is resolved.
                // GetAllTreeIdsAsync addresses no particular tree, so requiring
                // one would make an enumeration-only probe (--trees 0) divide by
                // zero on an empty target list - and that is exactly the probe
                // shape needed to measure the range scan against a bare estate.
                if (roll < enumerateShare)
                {
                    return census.MeasureAsync(
                        "ILatticeRegistry.GetAllTreeIdsAsync",
                        () => registry.GetAllTreeIdsAsync());
                }

                if (targets.Count == 0)
                {
                    throw new InvalidOperationException(
                        "the point-read mix needs at least one target tree; pass --trees N, or --enumerate-pct 100 to probe the range scan alone.");
                }

                var treeId = targets[(int)(sequence % targets.Count)];

                // Re-roll the remainder across the storm mix so the point-read
                // proportions among themselves are preserved whatever share
                // enumeration takes. Scaling the original thresholds instead
                // would quietly change the mix being replayed as the share rose.
                var point = (roll - enumerateShare) * 100 / (100 - enumerateShare);

                return point switch
                {
                    < 48 => census.MeasureAsync(
                        "ILatticeRegistry.ResolveAsync",
                        () => registry.ResolveAsync(treeId)),
                    < 92 => census.MeasureAsync(
                        "ILatticeRegistry.GetEntryAsync",
                        () => registry.GetEntryAsync(treeId)),
                    _ => census.MeasureAsync(
                        "ILatticeRegistry.GetShardMapAsync",
                        () => registry.GetShardMapAsync(treeId)),
                };
            },
            cancellationToken).ConfigureAwait(false);

        var mix = enumerateShare > 0
            ? $"{enumerateShare}% enumeration, {100 - enumerateShare}% point reads"
            : "point reads only";

        return $"issued {issued} registry reads ({mix}) at {options.Rate}/s for {options.Duration.TotalSeconds}s";
    }

    private static void Emit(DriverReport report, string? outputPath)
    {
        var json = JsonSerializer.Serialize(report, ReportJson);
        Console.WriteLine(json);

        if (string.IsNullOrWhiteSpace(outputPath))
        {
            return;
        }

        var directory = Path.GetDirectoryName(Path.GetFullPath(outputPath));
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(outputPath, json);
        Console.Error.WriteLine($"report written to {outputPath}");
    }
}
