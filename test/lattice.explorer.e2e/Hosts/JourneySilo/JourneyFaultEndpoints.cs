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
            var statistics = await management.GetDetailedGrainStatistics();
            foreach (var statistic in statistics)
            {
                if (statistic.GrainId.Equals(wanted))
                {
                    return Results.Ok(new { silo = statistic.SiloAddress.Endpoint.ToString(), port = statistic.SiloAddress.Endpoint.Port });
                }
            }

            return Results.NotFound();
        });

        // Every activation whose key contains a fragment, with its host: how a journey finds
        // the grains serving a tree without knowing the routing layer's grain types.
        app.MapGet(Prefix + "/activations", async (string keyContains, IGrainFactory grains) =>
        {
            var management = grains.GetGrain<IManagementGrain>(0);
            var statistics = await management.GetDetailedGrainStatistics();
            return Results.Ok(statistics
                .Where(statistic => statistic.GrainId.Key.ToString()!.Contains(keyContains, StringComparison.Ordinal))
                .Select(statistic => new { grainId = statistic.GrainId.ToString(), port = statistic.SiloAddress.Endpoint.Port })
                .OrderBy(activation => activation.grainId, StringComparer.Ordinal)
                .ToArray());
        });

        app.MapPost(Prefix + "/orphan", async (string tree, int? shard, IGrainFactory grains) =>
        {
            try
            {
                var orphan = await grains.GetGrain<IJourneyOrphanInjectorGrain>(tree).InjectAsync(shard ?? 0);
                return Results.Ok(new { orphan });
            }
            catch (InvalidOperationException ex)
            {
                // The shard has no leaf holding rows to split; the harness tries the next one.
                return Results.Conflict(new { reason = ex.Message });
            }
        });

        // A journey app source's publisher releasing its next version (A5, #5376), recorded
        // cluster-wide so every silo's copy of the source lists it from now on.
        app.MapPost(Prefix + "/apps/publish", async (string source, IGrainFactory grains) =>
        {
            if (source != JourneyAppSource.Samples)
            {
                return Results.NotFound(new { reason = $"Source '{source}' has no next version to publish." });
            }

            await grains.GetGrain<IJourneyCatalogueGrain>(source).PublishNextAsync();
            return Results.Ok(new { source });
        });
    }
}
