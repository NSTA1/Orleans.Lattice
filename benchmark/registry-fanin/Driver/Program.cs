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

                case "fanout":
                    notes.AddRange(await RunFanoutAsync(grains, fleet, options, census, lifetime.Token).ConfigureAwait(false));
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

    /// <summary>
    /// The saturating arm: releases <see cref="DriverOptions.FanoutWidth"/>
    /// distinct trees' option resolutions from a single barrier, repeatedly, so
    /// the offered fan-in is the wave width by construction.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why the other arms could not do this.</b> Silo-side fan-in is gated in
    /// <c>RegistryFanInGate</c>, which hangs off <c>LatticeOptionsResolver</c> -
    /// a silo singleton whose callers are all in-process. The <c>probe</c> arm
    /// addresses <c>ILatticeRegistry</c> as an Orleans client, so every one of
    /// its calls arrives past the gate and raising its rate raises registry-side
    /// width while gate occupancy stays at zero. That is the specific reason the
    /// original run could report a comfortable-looking width while the gate was
    /// never entered at all.
    /// </para>
    /// <para>
    /// <b>Why it forces a refresh.</b> <c>GetRoutingAsync(forceRefresh: true)</c>
    /// invalidates the activation's cached shard map and alias, so the next
    /// resolve goes through <c>LatticeOptionsResolver.ResolveAsync</c> and
    /// therefore through the gate. Without it a warm activation answers from its
    /// own cache and the wave never leaves the client's half of the system. Note
    /// the resolver's per-tree coalescer is deliberately not a cache - it retires
    /// a flight before publishing its result - so repeat waves keep reaching the
    /// registry rather than decaying to nothing.
    /// </para>
    /// <para>
    /// <b>Why one barrier and not a rate.</b> An open-loop pacer spreads arrivals
    /// across its tick, which is the same dispersal that defeats the birth arm at
    /// a 60-second scale. Releasing from a <see cref="TaskCompletionSource"/>
    /// makes simultaneity a property of the driver rather than a hoped-for
    /// coincidence, so the offered fan-in equals the wave width and can be
    /// stated rather than estimated.
    /// </para>
    /// </remarks>
    private static async Task<List<string>> RunFanoutAsync(
        IGrainFactory grains,
        TreeFleet fleet,
        DriverOptions options,
        CallCensus census,
        CancellationToken cancellationToken)
    {
        var targets = fleet.TreeIds(options.Trees);
        if (targets.Count == 0)
        {
            throw new InvalidOperationException("the fanout arm needs at least one target tree; pass --trees N.");
        }

        var width = Math.Max(1, options.FanoutWidth);
        var waves = Math.Max(1, options.FanoutWaves);
        var notes = new List<string>();
        var faults = 0;
        var issued = 0L;
        var waveElapsed = new List<double>(waves);

        for (var wave = 0; wave < waves; wave++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Every call in a wave awaits this one source, so they are released
            // together rather than in issue order. Continuations run
            // asynchronously so the release does not execute the wave inline on
            // the releasing thread, which would serialise the very thing the
            // barrier exists to parallelise.
            var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var wall = new List<Task>(width);

            for (var slot = 0; slot < width; slot++)
            {
                // Distinct ids within a wave, walked across the fleet between
                // waves. Distinctness is load-bearing: the resolver collapses
                // concurrent readers of ONE tree into a single flight, so a wave
                // of W calls on the same tree offers a fan-in of one however
                // wide it is.
                var treeId = targets[(wave * width + slot) % targets.Count];
                wall.Add(ResolveOnReleaseAsync(treeId));
            }

            var started = DateTimeOffset.UtcNow;
            barrier.SetResult();
            await Task.WhenAll(wall).ConfigureAwait(false);
            waveElapsed.Add((DateTimeOffset.UtcNow - started).TotalMilliseconds);

            if (options.FanoutGapMillis > 0)
            {
                await Task.Delay(options.FanoutGapMillis, cancellationToken).ConfigureAwait(false);
            }

            async Task ResolveOnReleaseAsync(string treeId)
            {
                await barrier.Task.ConfigureAwait(false);
                Interlocked.Increment(ref issued);

                try
                {
                    if (options.FanoutUngated)
                    {
                        // The control path. Identical wave, identical width,
                        // identical ids - but addressed as a client, so it
                        // reaches no silo-side gate. Any difference between the
                        // two runs is attributable to the gate rather than to
                        // the workload.
                        var registry = grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                        await census.MeasureAsync(
                            "ILatticeRegistry.GetEntryAsync",
                            () => registry.GetEntryAsync(treeId)).ConfigureAwait(false);
                        return;
                    }

                    var lattice = grains.GetGrain<ILattice>(treeId);
                    await census.MeasureAsync(
                        "ILattice.GetRoutingAsync",
                        () => lattice.GetRoutingAsync(true, cancellationToken).AsTask()).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    Interlocked.Increment(ref faults);
                }
            }
        }

        var path = options.FanoutUngated
            ? "UNGATED control (ILatticeRegistry direct from the client, reaching no silo-side gate)"
            : "gated (ILattice.GetRoutingAsync forceRefresh, through LatticeOptionsResolver)";

        notes.Add(
            $"fanout {path}: {waves} waves x {width} distinct trees = {issued} resolutions " +
            $"across a fleet of {targets.Count}, gap {options.FanoutGapMillis}ms, {faults} faulted");
        notes.Add(
            $"wave wall-clock ms: min {waveElapsed.Min():F1}, mean {waveElapsed.Average():F1}, max {waveElapsed.Max():F1}");

        // Stated in the report rather than left to the reader, because the whole
        // point of the arm is that a reading taken below this width is not a
        // weak measurement of the bound - it is no measurement of it.
        notes.Add(
            $"offered fan-in per wave is {width} distinct trees by construction (single-barrier release). " +
            "Compare it against the gate's permit count before reading any admission figure: below that " +
            "count the gate never queues, and the wait, width and batch-size instruments all report their " +
            "structural floor, which is indistinguishable by eye from a bound with headroom.");

        return notes;
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
