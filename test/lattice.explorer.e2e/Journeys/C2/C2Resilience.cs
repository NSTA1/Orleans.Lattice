using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// C2, resilience (#5390): infrastructure misbehaves in every way it can, and the console has
/// to tell the truth throughout. A west silo fails behind the gateway under priya's live Keys
/// tab; the silo holding olivia's compliance scan is killed mid-scan; the west Explorer
/// restarts without persisted keys under priya's tab; east is isolated and healed under the
/// Replication tree page; Prometheus stops under a Telemetry board; and the telemetry backup
/// schedule runs again after it all.
/// </summary>
/// <remarks>
/// <para>
/// Every fault is a harness action, as the external actor it models: killing and restarting
/// processes, isolating and healing a region, stopping and starting Prometheus, and seeding the
/// scratch tree the scan reads. Each is named in #5390. Every reading of the state is the UI's.
/// </para>
/// <para>
/// Each fault is undone in a <c>finally</c>, so a failed step still leaves the estate whole for
/// the chapters after it in a story run.
/// </para>
/// </remarks>
[JourneyChapter("C2", After = new[] { "A6", "A7", "A8" })]
public sealed partial class C2Resilience : JourneyChapter
{
    /// <summary>The west silo C2.1 fails behind the gateway.</summary>
    internal const int FailoverSilo = 1;

    private static readonly TimeSpan SiloRestart = TimeSpan.FromMinutes(2);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;

        await using (var priya = await estate.SignInAsAsync(Personas.Priya))
        {
            await new C2Step(1, "A silo fails behind the gateway under a live Keys tab").RunAsync(step => SiloFailoverAsync(estate, priya, step));
            await new C2Step(2, "The silo holding a compliance scan is lost mid-scan").RunAsync(step => OperationSiloLostAsync(estate, step));
            await new C2Step(3, "The west Explorer restarts without persisted keys").RunAsync(step => ExplorerRestartAsync(estate, priya, step));
        }

        await using var olivia = await estate.SignInAsAsync(Personas.Olivia);
        await new C2Step(4, "East is isolated, then healed").RunAsync(step => RegionOutageAsync(estate, olivia, step));
        var outagesOver = DateTimeOffset.UtcNow;
        await new C2Step(5, "Prometheus stops, then starts again").RunAsync(step => TelemetryOutageAsync(estate, olivia, step));
        await new C2Step(6, "The telemetry schedule runs again after the outages").RunAsync(step => ScheduleRecoversAsync(estate, olivia, step, outagesOver));
    }

    /// <summary>Restarts every stopped silo of <paramref name="region"/> and waits until its gateway routes to all of them.</summary>
    /// <param name="estate">The estate.</param>
    /// <param name="region">The region.</param>
    private static async Task RestoreSilosAsync(JourneyEstate estate, string region)
    {
        var silos = region == JourneyEstate.West ? estate.WestRegion.Silos : estate.EastRegion.Silos;
        foreach (var silo in silos.Where(silo => !silo.IsRunning))
        {
            await estate.RestartSiloAsync(region, silo.Index);
        }

        await estate.WaitForGatewayToMatchSilosAsync(region, SiloRestart);
    }
}
