namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Options governing the per-tenant observability publisher: whether the
/// per-tenant usage, quota, burst, and overage observable gauges are published,
/// and how often the publisher samples the warm usage index and the durable
/// overage billing seam to refresh them.
/// </summary>
/// <remarks>
/// Publishing is on by default when the tenancy add-on is registered. The sample
/// cadence trades gauge freshness against the cost of the periodic overage-tree
/// scan the publisher performs off the hot path (the usage sample itself is a
/// warm in-memory read). Disabling publishing leaves the tenancy meter with no
/// series and removes the periodic scan.
/// </remarks>
public sealed class TenantObservabilityOptions
{
    /// <summary>The default publish cadence when none is configured.</summary>
    public static readonly TimeSpan DefaultPublishInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// <c>true</c> (the default) to publish the per-tenant observability gauges on
    /// the <c>orleans.lattice.tenancy</c> meter. <c>false</c> to leave the meter
    /// inert and skip the periodic sample entirely.
    /// </summary>
    public bool PublishGauges { get; set; } = true;

    /// <summary>
    /// The interval at which the publisher samples the warm usage index and the
    /// overage billing seam and republishes the per-tenant gauge snapshot.
    /// Defaults to <see cref="DefaultPublishInterval"/>; a non-positive value is
    /// treated as the default.
    /// </summary>
    public TimeSpan PublishInterval { get; set; } = DefaultPublishInterval;
}
