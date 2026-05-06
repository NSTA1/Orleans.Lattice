using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Observable snapshot of a site's chaos configuration plus its live
/// counters. Published by <see cref="ISiteRegistryGrain"/> to the gRPC
/// <c>WatchSites</c> feed and the Blazor chaos fly-out.
/// </summary>
[GenerateSerializer, Immutable]
public readonly record struct SiteState
{
    /// <summary>Site identity.</summary>
    [Id(0)] public ProcessSite Site { get; init; }

    /// <summary>Current chaos configuration.</summary>
    [Id(1)] public SiteConfig Config { get; init; }

    /// <summary>Number of facts currently held in the grain's pending queue.</summary>
    [Id(2)] public int PendingCount { get; init; }

    /// <summary>Total number of facts admitted (forwarded or held) by the grain since it activated.</summary>
    [Id(3)] public long AdmittedCount { get; init; }
}
