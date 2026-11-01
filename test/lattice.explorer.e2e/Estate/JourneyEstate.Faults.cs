using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Azure;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>One silo as the region's gateway sees it.</summary>
/// <param name="Id">The gateway's destination id (<c>silo-{index}</c>).</param>
/// <param name="Address">The silo's gRPC address.</param>
/// <param name="Health">The active health check's verdict: <c>Healthy</c>, <c>Unhealthy</c> or <c>Unknown</c>.</param>
/// <param name="Drained">Whether the harness has taken it out of rotation for new requests.</param>
public sealed record GatewayDestination(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("address")] string Address,
    [property: JsonPropertyName("health")] string Health,
    [property: JsonPropertyName("drained")] bool Drained = false)
{
    /// <summary>Whether the gateway routes to the silo.</summary>
    public bool Healthy => Health == "Healthy";
}

/// <summary>One grain activation and the silo hosting it.</summary>
/// <param name="GrainId">The grain id, in Orleans' <c>{type}/{key}</c> form.</param>
/// <param name="Silo">The index of the silo hosting it in its region.</param>
public sealed record JourneyActivation(string GrainId, int Silo)
{
    /// <summary>The grain type: the part of <see cref="GrainId"/> before its first <c>/</c>.</summary>
    public string GrainType => GrainId[..GrainId.IndexOf('/', StringComparison.Ordinal)];

    /// <summary>The grain key: the part of <see cref="GrainId"/> after its first <c>/</c>.</summary>
    public string Key => GrainId[(GrainId.IndexOf('/', StringComparison.Ordinal) + 1)..];
}

/// <summary>The process-level fault seams and diagnostics (issue #5359, scope item 9).</summary>
public sealed partial class JourneyEstate
{
    /// <summary>Kills a silo's process at once, as a crash would. The gateway takes it out of rotation within one probe interval.</summary>
    /// <param name="region">The region.</param>
    /// <param name="index">The silo's index.</param>
    public Task KillSiloAsync(string region, int index) => RegionOf(region).Silos[index].Process.KillAsync();

    /// <summary>Starts a killed silo again, with its configuration and ports, and waits until it serves.</summary>
    /// <param name="region">The region.</param>
    /// <param name="index">The silo's index.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public async Task RestartSiloAsync(string region, int index, CancellationToken cancellationToken = default)
    {
        var silo = RegionOf(region).Silos[index];
        await silo.Process.KillAsync();
        await silo.Process.StartAsync(_options.SiloStartTimeout, cancellationToken);
    }

    /// <summary>
    /// Restarts an Explorer head's process. Whether a browser signed in before the restart is
    /// still signed in after it depends on <see cref="JourneyEstateOptions.ExplorerKeyRing"/>:
    /// with the default (no persisted ring) the restart ends every session.
    /// </summary>
    /// <param name="region">The region.</param>
    /// <param name="index">The head's index.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public async Task RestartExplorerAsync(string region, int index = 0, CancellationToken cancellationToken = default)
    {
        var explorer = RegionOf(region).ExplorerProcesses[index];
        await explorer.KillAsync();
        await explorer.StartAsync(_options.HostStartTimeout, cancellationToken);
    }

    /// <summary>
    /// Arms the region's gateway to drop the next proxied call whose gRPC path contains
    /// <paramref name="match"/> (a method name such as <c>BulkLoadAppend</c>): the call is torn
    /// down mid-flight, as a lost connection would tear it down.
    /// </summary>
    /// <param name="match">A substring of the call's path.</param>
    /// <param name="region">The region.</param>
    public async Task FailNextGatewayCallAsync(string match, string region = West)
    {
        ArgumentException.ThrowIfNullOrEmpty(match);
        using var response = await _probe.PostAsync(new Uri(RegionOf(region).GatewayControl, $"control/fail-next?match={Uri.EscapeDataString(match)}"), null);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>The region's silos as its gateway's health checks see them.</summary>
    /// <param name="region">The region.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public Task<IReadOnlyList<GatewayDestination>> GatewayDestinationsAsync(string region = West, CancellationToken cancellationToken = default) =>
        DestinationsAsync(RegionOf(region).GatewayControl, cancellationToken);

    /// <summary>Waits until the region's gateway routes to exactly the running silos.</summary>
    /// <param name="region">The region.</param>
    /// <param name="timeout">How long to wait; defaults to five probe intervals.</param>
    /// <returns>How long it took.</returns>
    public async Task<TimeSpan> WaitForGatewayToMatchSilosAsync(string region = West, TimeSpan? timeout = null)
    {
        var limit = timeout ?? (_options.GatewayHealthInterval * 5);
        var started = System.Diagnostics.Stopwatch.StartNew();
        var silos = RegionOf(region).Silos;
        while (true)
        {
            var destinations = await GatewayDestinationsAsync(region);
            if (silos.All(silo => destinations.Single(d => d.Id == $"silo-{silo.Index}").Healthy == silo.IsRunning))
            {
                return started.Elapsed;
            }

            if (started.Elapsed > limit)
            {
                throw new TimeoutException(
                    $"The {region} gateway did not match its silos within {limit.TotalSeconds:0} seconds: "
                    + string.Join(", ", destinations.Select(d => $"{d.Id}={d.Health}")));
            }

            await Task.Delay(100);
        }
    }

    /// <summary>
    /// Forces a full, blocking, compacting collection in the journey host serving
    /// <paramref name="root"/> and reads what it holds afterwards (#5475): its live managed
    /// heap, the memory its collector keeps committed, and its working set. With
    /// <paramref name="settle"/> the collection is the aggressive mode, which also returns
    /// free memory to the operating system. Requires the fault and diagnostic seams.
    /// </summary>
    /// <param name="root">The host's root address: a silo's internal HTTP port, or an Explorer head.</param>
    /// <param name="settle">Whether to settle the host's memory rather than only collect it.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    internal async Task<JourneyMemoryReading> ReadMemoryAsync(Uri root, bool settle, CancellationToken cancellationToken = default)
    {
        using var response = await _probe.PostAsync(
            new Uri(root, JourneyMemoryProbe.Path.TrimStart('/') + (settle ? "?settle=true" : string.Empty)), content: null, cancellationToken);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<JourneyMemoryReading>(cancellationToken)
            ?? throw new InvalidOperationException($"{root} answered its memory probe with nothing.");
    }

    /// <summary>
    /// Which silo of <paramref name="region"/> hosts the activation of grain
    /// <paramref name="grainType"/> keyed <paramref name="key"/>, through the Orleans management
    /// grain, or <see langword="null"/> when it is not active.
    /// </summary>
    /// <param name="grainType">The Orleans grain type, as in a grain id's <c>{type}/{key}</c> form.</param>
    /// <param name="key">The grain key.</param>
    /// <param name="region">The region.</param>
    public async Task<int?> LocateActivationAsync(string grainType, string key, string region = West)
    {
        var regionHost = RegionOf(region);
        var silo = regionHost.Silos.FirstOrDefault(candidate => candidate.IsRunning)
            ?? throw new InvalidOperationException($"No {region} silo is running.");
        using var response = await _probe.GetAsync(new Uri(
            $"http://127.0.0.1:{silo.HttpPort}/e2e/activation?grainType={Uri.EscapeDataString(grainType)}&key={Uri.EscapeDataString(key)}"));
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        var located = await response.Content.ReadFromJsonAsync<LocatedActivation>()
            ?? throw new InvalidOperationException("The silo did not say where the activation is.");
        return regionHost.Silos.Single(candidate => candidate.SiloPort == located.Port).Index;
    }

    /// <summary>
    /// Every activation in <paramref name="region"/> whose grain key contains
    /// <paramref name="keyFragment"/> (a tree id, say), with the silo hosting it: how a journey
    /// finds the grains serving a tree without knowing the routing layer's grain types.
    /// </summary>
    /// <param name="keyFragment">A fragment of the grain key.</param>
    /// <param name="region">The region.</param>
    public async Task<IReadOnlyList<JourneyActivation>> FindActivationsAsync(string keyFragment, string region = West)
    {
        ArgumentException.ThrowIfNullOrEmpty(keyFragment);
        var regionHost = RegionOf(region);
        var silo = regionHost.Silos.FirstOrDefault(candidate => candidate.IsRunning)
            ?? throw new InvalidOperationException($"No {region} silo is running.");
        var found = await _probe.GetFromJsonAsync<FoundActivation[]>(new Uri(
            $"http://127.0.0.1:{silo.HttpPort}/e2e/activations?keyContains={Uri.EscapeDataString(keyFragment)}")) ?? [];
        return [.. found.Select(activation => new JourneyActivation(
            activation.GrainId,
            regionHost.Silos.Single(candidate => candidate.SiloPort == activation.Port).Index))];
    }

    /// <summary>
    /// Leaves an orphaned leaf in <paramref name="tree"/>: the persisted state a silo that
    /// crashed part way through a leaf split leaves, so the orphan audit finds it and the
    /// repair can remove it. The orphan shadows the second leaf of physical shard
    /// <paramref name="shard"/>, or - when no shard is named - of the first physical shard
    /// that has split and whose second leaf holds rows.
    /// </summary>
    /// <param name="tree">The tree; it must hold rows.</param>
    /// <param name="shard">The physical shard, or <see langword="null"/> for the first that holds rows.</param>
    /// <param name="region">The region.</param>
    /// <returns>The orphan's grain id.</returns>
    public async Task<string> InjectOrphanedLeafAsync(string tree, int? shard = null, string region = West)
    {
        RequireFaults();
        ArgumentException.ThrowIfNullOrEmpty(tree);
        IEnumerable<int> candidates;
        if (shard is { } named)
        {
            candidates = [named];
        }
        else
        {
            var admin = Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcClient.Create(Invoker(region: region), Serialization);
            candidates = (await admin.InspectShardMapAsync(tree)).PhysicalShardIndices;
        }

        var silo = RegionOf(region).Silos.First(candidate => candidate.IsRunning);
        var refusals = new List<string>();
        foreach (var index in candidates)
        {
            using var response = await _probe.PostAsync(new Uri(
                $"http://127.0.0.1:{silo.HttpPort}/e2e/orphan?tree={Uri.EscapeDataString(tree)}&shard={index}"), null);
            if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
            {
                refusals.Add($"shard {index}: {await response.Content.ReadAsStringAsync()}");
                continue;
            }

            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException(
                    $"Injecting an orphan into '{tree}' failed: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
            }

            return (await response.Content.ReadFromJsonAsync<InjectedOrphan>())?.Orphan
                ?? throw new InvalidOperationException("The silo did not name the orphan it injected.");
        }

        throw new InvalidOperationException(
            $"No physical shard of '{tree}' has a leaf holding rows to shadow: {string.Join("; ", refusals)}");
    }

    /// <summary>
    /// Holds the next compliance scan of <paramref name="tree"/> on every running silo of
    /// <paramref name="region"/>: the scan waits, Running in its first phase (Counting, total
    /// not yet known), until <paramref name="delay"/> from now before it reads a value. The
    /// hold is spent by the first scan of the tree on each silo, lapses at its deadline, ends
    /// at once when the scan is cancelled, and is withdrawn by
    /// <see cref="ClearComplianceScanDelayAsync"/>. Every silo is armed because the scan runs
    /// on whichever silo accepted its start.
    /// </summary>
    /// <param name="tree">
    /// The tree as the cluster scans it: a tenant-scoped tree in its effective form
    /// (<c>t/{tenant}/{tree}</c>).
    /// </param>
    /// <param name="delay">How long from now the hold lasts: more than 0 and at most 3 minutes.</param>
    /// <param name="region">The region.</param>
    public async Task DelayComplianceScanAsync(string tree, TimeSpan delay, string region = West)
    {
        RequireFaults();
        ArgumentException.ThrowIfNullOrEmpty(tree);
        foreach (var silo in RegionOf(region).Silos.Where(candidate => candidate.IsRunning))
        {
            using var response = await _probe.PostAsync(new Uri(
                $"http://127.0.0.1:{silo.HttpPort}/e2e/compliance-delay?tree={Uri.EscapeDataString(tree)}&seconds={delay.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)}"), null);
            if (!response.IsSuccessStatusCode)
            {
                throw new InvalidOperationException(
                    $"Holding '{tree}''s compliance scan on {region} silo {silo.Index} failed: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
            }
        }
    }

    /// <summary>Withdraws any hold <see cref="DelayComplianceScanAsync"/> armed on <paramref name="tree"/> on every running silo of <paramref name="region"/>.</summary>
    /// <param name="tree">The tree, as it was armed.</param>
    /// <param name="region">The region.</param>
    public async Task ClearComplianceScanDelayAsync(string tree, string region = West)
    {
        RequireFaults();
        ArgumentException.ThrowIfNullOrEmpty(tree);
        foreach (var silo in RegionOf(region).Silos.Where(candidate => candidate.IsRunning))
        {
            using var response = await _probe.DeleteAsync(new Uri(
                $"http://127.0.0.1:{silo.HttpPort}/e2e/compliance-delay?tree={Uri.EscapeDataString(tree)}"));
            response.EnsureSuccessStatusCode();
        }
    }

    /// <summary>
    /// Deletes one blob of a backup behind the backup catalogue's back, as a storage-side
    /// deletion would, so the catalogue holds a genuine orphan row.
    /// </summary>
    /// <param name="backupId">The backup id.</param>
    /// <param name="artifact">
    /// <c>manifest</c> (the default) for the backup's manifest, or an artifact id from
    /// <see cref="BackupBlobsAsync"/>.
    /// </param>
    /// <returns>The deleted blob's name.</returns>
    public async Task<string> DeleteBackupArtifactAsync(string backupId, string artifact = "manifest")
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(artifact);
        var name = artifact == "manifest" ? "manifests/" + backupId : "artifacts/" + artifact;
        try
        {
            await _storage.BackupBlobs().DeleteBlobAsync(name);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            throw new InvalidOperationException($"The backup sink holds no blob '{name}'.", ex);
        }

        return name;
    }

    /// <summary>Every blob name in the backup sink that mentions <paramref name="backupId"/>.</summary>
    /// <param name="backupId">The backup id, or <see langword="null"/> for every blob.</param>
    public async Task<IReadOnlyList<string>> BackupBlobsAsync(string? backupId = null)
    {
        var names = new List<string>();
        var container = _storage.BackupBlobs();
        if (!await container.ExistsAsync())
        {
            return names;
        }

        await foreach (var blob in container.GetBlobsAsync())
        {
            if (backupId is null || blob.Name.Contains(backupId, StringComparison.Ordinal))
            {
                names.Add(blob.Name);
            }
        }

        return names;
    }

    private void RequireFaults()
    {
        if (!_options.Faults)
        {
            throw new InvalidOperationException("This estate was started with its fault seams off (JourneyEstateOptions.Faults).");
        }
    }

    private async Task<IReadOnlyList<GatewayDestination>> DestinationsAsync(Uri control, CancellationToken cancellationToken, string cluster = "silos")
    {
        try
        {
            var state = await _probe.GetFromJsonAsync<GatewayState>(new Uri(control, "control/destinations?cluster=" + cluster), cancellationToken);
            return state?.Destinations ?? [];
        }
        catch (HttpRequestException)
        {
            return [];
        }
    }

    private sealed record GatewayState([property: JsonPropertyName("destinations")] GatewayDestination[] Destinations);

    private sealed record LocatedActivation([property: JsonPropertyName("port")] int Port);

    private sealed record FoundActivation(
        [property: JsonPropertyName("grainId")] string GrainId,
        [property: JsonPropertyName("port")] int Port);

    private sealed record InjectedOrphan([property: JsonPropertyName("orphan")] string Orphan);
}
