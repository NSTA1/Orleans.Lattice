using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Persistent state for <see cref="ProcessSiteGrain"/>. Holds both the
/// chaos configuration and the queue of facts currently held because of
/// it — so a process restart preserves both the operator's chaos choices
/// and any in-flight facts that were caught mid-pause.
/// </summary>
[GenerateSerializer]
public sealed record ProcessSiteState
{
    /// <summary>Current chaos configuration.</summary>
    [Id(0)] public SiteConfig Config { get; set; }

    /// <summary>Facts the grain has accepted but is holding because the site is paused.</summary>
    [Id(1)] public List<Fact> Pending { get; init; } = [];

    /// <summary>Total number of facts admitted (forwarded or held) since the part was first seeded.</summary>
    [Id(2)] public long AdmittedCount { get; set; }
}
