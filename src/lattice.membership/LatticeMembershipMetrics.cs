using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Membership;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/>
/// instruments for <c>Orleans.Lattice.Membership</c>. Every membership instrument
/// is published on a single <see cref="Meter"/> named <see cref="MeterName"/> so
/// an OpenTelemetry pipeline can subscribe once and receive every membership
/// metric. Mirrors the structure of <c>Orleans.Lattice.LatticeMetrics</c> and
/// <c>Orleans.Lattice.Auth.LatticeAuthMetrics</c>.
/// </summary>
/// <remarks>
/// The resolution-cache hit / miss counters are recorded by
/// <see cref="MembershipResolutionCache"/> at the point it serves a warm subject
/// or resolves a cold one. The cache owns the signal, so the counters live on
/// this membership-owned meter with no dependency on the authorization layer that
/// sits above membership in the package graph. Recording is guarded by each
/// instrument's <see cref="Instrument.Enabled"/> flag, so when no OpenTelemetry
/// listener is attached the cache does no measurement work: the meter is
/// zero-cost on the resolution hot path when nobody is listening.
/// </remarks>
public static class LatticeMembershipMetrics
{
    /// <summary>
    /// The root meter name for all <c>Orleans.Lattice.Membership</c> telemetry.
    /// Internal telemetry hooks and external subscribers must reference this
    /// constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice.membership";

    /// <summary>Canonical name of the <see cref="ResolutionCacheHits"/> counter.</summary>
    public const string ResolutionCacheHitsName = "orleans.lattice.membership.resolution_cache.hits";

    /// <summary>Canonical name of the <see cref="ResolutionCacheMisses"/> counter.</summary>
    public const string ResolutionCacheMissesName = "orleans.lattice.membership.resolution_cache.misses";

    /// <summary>Canonical name of the <see cref="DirectorySearchDuration"/> histogram.</summary>
    public const string DirectorySearchDurationName = "orleans.lattice.membership.directory.search.duration";

    /// <summary>Canonical name of the <see cref="DirectorySearchHits"/> counter.</summary>
    public const string DirectorySearchHitsName = "orleans.lattice.membership.directory.search.hits";

    /// <summary>Canonical name of the <see cref="DirectorySearchMisses"/> counter.</summary>
    public const string DirectorySearchMissesName = "orleans.lattice.membership.directory.search.misses";

    /// <summary>
    /// The meter that owns every membership instrument. Exposed publicly so
    /// integration tests and custom OpenTelemetry exporters can subscribe by
    /// reference rather than by name.
    /// </summary>
    public static readonly Meter Meter = new(MeterName);

    /// <summary>
    /// Counter of subject-resolution cache hits, recorded through
    /// <see cref="RecordResolutionCacheHit"/> whenever the per-silo resolution
    /// cache serves a warm subject without re-authenticating or touching the
    /// directory.
    /// </summary>
    public static readonly Counter<long> ResolutionCacheHits =
        Meter.CreateCounter<long>(ResolutionCacheHitsName, unit: "{lookup}",
            description: "Subject-resolution cache hits served warm from the per-silo membership resolution cache.");

    /// <summary>
    /// Counter of subject-resolution cache misses, recorded through
    /// <see cref="RecordResolutionCacheMiss"/> whenever the per-silo resolution
    /// cache has no live entry and resolves the subject afresh.
    /// </summary>
    public static readonly Counter<long> ResolutionCacheMisses =
        Meter.CreateCounter<long>(ResolutionCacheMissesName, unit: "{lookup}",
            description: "Subject-resolution cache misses that fell through to a fresh resolution.");

    /// <summary>
    /// Histogram of identity-directory search latency in milliseconds, recorded
    /// through <see cref="RecordDirectorySearch"/> around each provider-backed
    /// directory query the access-administration facade issues (for example the
    /// Explorer subject picker's typeahead). Times the provider call only, so it
    /// isolates directory cost from the facade's mapping and authorization work.
    /// </summary>
    public static readonly Histogram<double> DirectorySearchDuration =
        Meter.CreateHistogram<double>(DirectorySearchDurationName, unit: "ms",
            description: "Identity-directory search latency in milliseconds.");

    /// <summary>
    /// Counter of identity-directory searches that returned at least one matching
    /// principal, recorded through <see cref="RecordDirectorySearch"/>.
    /// </summary>
    public static readonly Counter<long> DirectorySearchHits =
        Meter.CreateCounter<long>(DirectorySearchHitsName, unit: "{search}",
            description: "Identity-directory searches that returned at least one matching principal.");

    /// <summary>
    /// Counter of identity-directory searches that returned no matching principal,
    /// recorded through <see cref="RecordDirectorySearch"/>.
    /// </summary>
    public static readonly Counter<long> DirectorySearchMisses =
        Meter.CreateCounter<long>(DirectorySearchMissesName, unit: "{search}",
            description: "Identity-directory searches that returned no matching principal.");

    /// <summary>
    /// Records a subject-resolution cache hit on <see cref="ResolutionCacheHits"/>.
    /// Cheap no-op when no listener is attached.
    /// </summary>
    public static void RecordResolutionCacheHit()
    {
        if (ResolutionCacheHits.Enabled)
        {
            ResolutionCacheHits.Add(1, LatticeTenantLabel.Platform);
        }
    }

    /// <summary>
    /// Records a subject-resolution cache miss on
    /// <see cref="ResolutionCacheMisses"/>. Cheap no-op when no listener is
    /// attached.
    /// </summary>
    public static void RecordResolutionCacheMiss()
    {
        if (ResolutionCacheMisses.Enabled)
        {
            ResolutionCacheMisses.Add(1, LatticeTenantLabel.Platform);
        }
    }

    /// <summary>
    /// Records one identity-directory search: its
    /// <paramref name="elapsedMilliseconds"/> latency on
    /// <see cref="DirectorySearchDuration"/>, plus a hit on
    /// <see cref="DirectorySearchHits"/> when <paramref name="matched"/> is
    /// <see langword="true"/> (the search returned at least one principal) or a
    /// miss on <see cref="DirectorySearchMisses"/> otherwise. Cheap no-op per
    /// instrument when no listener is attached.
    /// </summary>
    /// <param name="elapsedMilliseconds">The measured search latency in milliseconds.</param>
    /// <param name="matched"><see langword="true"/> when the search returned at least one principal.</param>
    public static void RecordDirectorySearch(double elapsedMilliseconds, bool matched)
    {
        if (DirectorySearchDuration.Enabled)
        {
            DirectorySearchDuration.Record(elapsedMilliseconds, LatticeTenantLabel.Platform);
        }

        if (matched)
        {
            if (DirectorySearchHits.Enabled)
            {
                DirectorySearchHits.Add(1, LatticeTenantLabel.Platform);
            }
        }
        else if (DirectorySearchMisses.Enabled)
        {
            DirectorySearchMisses.Add(1, LatticeTenantLabel.Platform);
        }
    }
}
