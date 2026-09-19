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

        try
        {
            switch (options.Verb)
            {
                case "create":
                    await fleet.CreateAsync(options.Trees, census, options.Parallelism, lifetime.Token).ConfigureAwait(false);
                    notes.Add($"created {options.Trees} trees with prefix '{options.Prefix}'");
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

        var report = DriverReport.From(options.Verb, startedAt, options.Trees, options.Prefix, census, notes);
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
    /// Drives registry point reads in the member mix observed in the live storm
    /// (<c>ResolveAsync</c> and <c>GetEntryAsync</c> dominant, <c>GetShardMapAsync</c>
    /// a minority), so the client-side census is comparable with the server-side
    /// timeout population.
    /// </summary>
    private static async Task<string> RunProbeAsync(
        IGrainFactory grains,
        TreeFleet fleet,
        DriverOptions options,
        CallCensus census,
        CancellationToken cancellationToken)
    {
        var targets = fleet.TreeIds(options.Trees);

        var issued = await RateDriver.RunAsync(
            options.Rate,
            options.Duration,
            options.MaxInFlight,
            sequence =>
            {
                var registry = grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                var treeId = targets[(int)(sequence % targets.Count)];

                return (sequence % 100) switch
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

        return $"issued {issued} registry point reads at {options.Rate}/s for {options.Duration.TotalSeconds}s";
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
