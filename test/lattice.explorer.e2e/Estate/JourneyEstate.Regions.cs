using System.Net.Http.Json;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>
/// The east region, the estate's Prometheus, the kiosk head, and the seams that act on them
/// (issue #5362): region isolation, telemetry outages, and switching a scaled-out console's
/// replica behind its gateway.
/// </summary>
public sealed partial class JourneyEstate
{
    /// <summary>
    /// The response header each replica of a scaled-out console names itself in, so a journey
    /// can tell which replica served a page (a harness diagnostic, not a product header).
    /// </summary>
    public const string ReplicaHeader = "X-Lattice-E2E-Replica";

    private Uri? _kioskUri;

    /// <summary>The east region; present when <see cref="JourneyEstateOptions.East"/> is on.</summary>
    public JourneyRegion EastRegion =>
        _regions.TryGetValue(East, out var east)
            ? east
            : throw new InvalidOperationException("This estate has no east region (JourneyEstateOptions.East).");

    /// <summary>
    /// The kiosk head's base address: a west Explorer head seeded with
    /// <see cref="JourneyEstateOptions.KioskUser"/>'s credential, so it opens signed in.
    /// </summary>
    public Uri KioskUri => _kioskUri ?? throw new InvalidOperationException("This estate has no kiosk head (JourneyEstateOptions.KioskUser).");

    /// <summary>The kiosk head's name, <c>{estate name}-kiosk</c> (<c>contoso-kiosk</c>).</summary>
    public string KioskName => $"{_options.Name}-kiosk";

    /// <summary>The estate's Prometheus, which every silo's telemetry facade queries.</summary>
    public Uri PrometheusUri => RequirePrometheus().Address;

    /// <summary>Whether the estate's Prometheus is running.</summary>
    public bool PrometheusIsRunning => RequirePrometheus().IsRunning;

    /// <summary>Stops the estate's Prometheus, as a telemetry outage would. Its data is kept.</summary>
    public Task StopPrometheusAsync() => RequirePrometheus().StopAsync();

    /// <summary>Starts the stopped Prometheus again and waits until it answers.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public Task StartPrometheusAsync(CancellationToken cancellationToken = default) => RequirePrometheus().StartAgainAsync(cancellationToken);

    /// <summary>
    /// Waits until Prometheus has scraped every silo at least twice, so every rate on a
    /// Telemetry board has the two samples it needs.
    /// </summary>
    /// <param name="timeout">How long to wait; ten scrape intervals by default.</param>
    public Task WaitForPrometheusScrapesAsync(TimeSpan? timeout = null) =>
        RequirePrometheus().WaitForScrapesAsync(timeout ?? (_options.PrometheusScrapeInterval * 10) + TimeSpan.FromSeconds(10));

    /// <summary>
    /// Isolates <paramref name="region"/>: kills every one of its silo processes at once, as a
    /// region outage would. Its gateway and Explorer heads keep running and report the outage.
    /// </summary>
    /// <param name="region">The region.</param>
    public async Task IsolateRegionAsync(string region)
    {
        foreach (var silo in RegionOf(region).Silos)
        {
            await silo.Process.KillAsync();
        }
    }

    /// <summary>
    /// Heals an isolated <paramref name="region"/>: starts each of its silos again, one at a
    /// time with its configuration and ports, and waits until its gateway routes to them.
    /// </summary>
    /// <param name="region">The region.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    public async Task HealRegionAsync(string region, CancellationToken cancellationToken = default)
    {
        foreach (var silo in RegionOf(region).Silos)
        {
            if (!silo.IsRunning)
            {
                await silo.Process.StartAsync(_options.SiloStartTimeout, cancellationToken);
            }
        }

        await WaitForGatewayToMatchSilosAsync(region, _options.HostStartTimeout);
    }

    /// <summary>The replicas of <paramref name="region"/>'s console as its gateway sees them (<c>explorer-{index}</c>).</summary>
    /// <param name="region">The region.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public Task<IReadOnlyList<GatewayDestination>> ConsoleDestinationsAsync(string region = East, CancellationToken cancellationToken = default) =>
        DestinationsAsync(RegionOf(region).GatewayControl, cancellationToken, "console");

    /// <summary>
    /// Takes console replica <paramref name="index"/> of <paramref name="region"/> out of the
    /// gateway's rotation for new requests, as an operator draining a replica does, or returns
    /// it. A browser held to it by the gateway's affinity cookie is switched to another
    /// replica on its next request; the replica's process keeps running.
    /// </summary>
    /// <param name="region">The region.</param>
    /// <param name="index">The replica's index.</param>
    /// <param name="drained">Whether it is drained; <see langword="false"/> returns it to rotation.</param>
    public async Task DrainConsoleReplicaAsync(string region, int index, bool drained = true)
    {
        var host = RegionOf(region);
        if (!host.ConsoleBehindGateway || index < 0 || index >= host.ExplorerProcesses.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(index), $"The {region} console has no replica {index} behind its gateway.");
        }

        using var response = await _probe.PostAsync(
            new Uri(host.GatewayControl, $"control/drain?cluster=console&destination=explorer-{index}&drained={drained.ToString().ToLowerInvariant()}"), null);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Forces the gateway to switch a browser off the console replica that has been serving
    /// it: drains <paramref name="from"/> and returns every other replica to rotation.
    /// </summary>
    /// <param name="from">The replica serving the browser now, as <see cref="ReplicaIndexOf"/> reads it.</param>
    /// <param name="region">The region.</param>
    public async Task ForceConsoleSwitchAsync(int from, string region = East)
    {
        for (var i = 0; i < RegionOf(region).ExplorerProcesses.Count; i++)
        {
            await DrainConsoleReplicaAsync(region, i, drained: i == from);
        }
    }

    /// <summary>The index of the console replica named in a <see cref="ReplicaHeader"/> value, or <see langword="null"/>.</summary>
    /// <param name="headerValue">The header's value, such as <c>east-explorer-1</c>.</param>
    public static int? ReplicaIndexOf(string? headerValue)
    {
        if (string.IsNullOrEmpty(headerValue))
        {
            return null;
        }

        var separator = headerValue.LastIndexOf('-');
        return separator >= 0 && int.TryParse(headerValue.AsSpan(separator + 1), out var index) ? index : null;
    }

    private async Task StartKioskAsync(string user, CancellationToken cancellationToken)
    {
        var west = WestRegion;
        var (_, kioskUri) = await StartExplorerHeadAsync(
            new ExplorerHead(KioskName, LoopbackPorts.Next(), "/", JourneyExplorerKeyRing.None, _gatewayPorts[West], west.GatewayPlaintextPort),
            cancellationToken,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                // A single-operator head: its environment carries one credential, and every
                // visitor is signed in as its owner (AllowEnvironmentCredentialSeed).
                ["Explorer__AllowEnvironmentCredentialSeed"] = "true",
                ["LATTICE_EXPLORER_USERNAME"] = user,
                ["LATTICE_EXPLORER_PASSWORD"] = PasswordOf(user),
            });
        _kioskUri = kioskUri;
    }

    private async Task WaitForConsoleReplicasAsync(string region, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(timeout);
        try
        {
            await WaitForAsync(async () =>
            {
                var replicas = await ConsoleDestinationsAsync(region, deadline.Token);
                return replicas.Count == RegionOf(region).ExplorerProcesses.Count && replicas.All(replica => replica.Healthy);
            }, deadline.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            var replicas = await ConsoleDestinationsAsync(region, CancellationToken.None);
            throw new TimeoutException(
                $"The {region} gateway did not find every console replica healthy within {timeout.TotalSeconds:0} seconds: "
                + string.Join(", ", replicas.Select(replica => $"{replica.Id}={replica.Health}")));
        }
    }

    private JourneyPrometheus RequirePrometheus() =>
        _prometheus ?? throw new InvalidOperationException("This estate runs no Prometheus (JourneyEstateOptions.Prometheus).");
}
