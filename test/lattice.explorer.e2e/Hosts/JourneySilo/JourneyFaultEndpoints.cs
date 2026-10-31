using Orleans.Runtime;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// The journey host's diagnostic and fault endpoints, mapped on the silo's internal HTTP
/// port only and only when the fault seams are enabled. They are the harness's reach into
/// the running cluster for the things no product surface offers on purpose: where an
/// activation lives, and the state a crash mid-split leaves.
/// </summary>
internal static class JourneyFaultEndpoints
{
    /// <summary>The path prefix of every endpoint here.</summary>
    public const string Prefix = "/e2e";

    /// <summary>Maps the endpoints.</summary>
    /// <param name="app">The silo's web application.</param>
    public static void MapJourneyFaultEndpoints(this WebApplication app)
    {
        // Which silo hosts an activation, through the Orleans management grain, so a
        // journey can kill the silo running an operation and assert it did.
        app.MapGet(Prefix + "/activation", async (string grainType, string key, IGrainFactory grains) =>
        {
            var wanted = GrainId.Create(grainType, key);
            var management = grains.GetGrain<IManagementGrain>(0);
            var statistics = await management.GetDetailedGrainStatistics([grainType]);
            foreach (var statistic in statistics)
            {
                if (statistic.GrainId.Equals(wanted))
                {
                    return Results.Ok(new { silo = statistic.SiloAddress.Endpoint.ToString(), port = statistic.SiloAddress.Endpoint.Port });
                }
            }

            return Results.NotFound();
        });

        app.MapPost(Prefix + "/orphan", async (string tree, int? shard, IGrainFactory grains) =>
        {
            var orphan = await grains.GetGrain<IJourneyOrphanInjectorGrain>(tree).InjectAsync(shard ?? 0);
            return Results.Ok(new { orphan });
        });
    }
}
