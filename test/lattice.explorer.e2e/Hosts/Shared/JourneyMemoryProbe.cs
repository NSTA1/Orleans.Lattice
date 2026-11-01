using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Orleans.Lattice.Explorer.EndToEnd.HostContract;

/// <summary>
/// What a journey host holds, read straight after a forced, blocking, compacting collection
/// of every generation (#5475). <see cref="LiveHeapBytes"/> is therefore the managed memory
/// something still references, not heap the collector has yet to sweep.
/// </summary>
/// <param name="LiveHeapBytes">The managed heap after the collection, in bytes.</param>
/// <param name="CommittedBytes">The memory the collector has committed after the collection, in bytes.</param>
/// <param name="WorkingSetBytes">The process's working set after the collection, in bytes.</param>
/// <param name="Collector">The collector's configuration (server or workstation, heap count, dynamic adaptation), as evidence.</param>
internal sealed record JourneyMemoryReading(long LiveHeapBytes, long CommittedBytes, long WorkingSetBytes, string Collector);

/// <summary>
/// The journey hosts' memory probe (#5475): <c>POST /e2e/memory</c> forces a full blocking
/// compacting collection and answers a <see cref="JourneyMemoryReading"/>. With
/// <c>?settle=true</c> the collection is the collector's aggressive mode, run on both sides of
/// a finalizer pass, so it also returns every free region it can to the operating system: the
/// settled reading a bounded-memory proof takes before and after the work it measures.
/// Mapped only when the fault and diagnostic seams are enabled (<c>LATTICE_E2E_FAULTS=1</c>).
/// </summary>
internal static class JourneyMemoryProbe
{
    /// <summary>The probe's path.</summary>
    public const string Path = "/e2e/memory";

    private static readonly string[] CollectorSettings = ["ServerGC", "ConcurrentGC", "HeapCount", "DynamicAdaptationMode", "ConserveMemory"];

    /// <summary>Maps the probe.</summary>
    /// <param name="endpoints">The host's endpoints.</param>
    public static void MapJourneyMemoryProbe(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapPost(Path, (bool? settle) =>
        {
            if (settle == true)
            {
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
                GC.WaitForPendingFinalizers();
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
            }
            else
            {
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            }

            return Results.Ok(new JourneyMemoryReading(
                GC.GetTotalMemory(forceFullCollection: false),
                GC.GetGCMemoryInfo().TotalCommittedBytes,
                Environment.WorkingSet,
                DescribeCollector()));
        }).AllowAnonymous();
    }

    private static string DescribeCollector()
    {
        var variables = GC.GetConfigurationVariables();
        return string.Join(", ", CollectorSettings
            .Where(variables.ContainsKey)
            .Select(name => $"{name}={variables[name]}"));
    }
}
