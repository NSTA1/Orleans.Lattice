namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Options for the per-silo tenant request-rate limiter and its budget
/// coordinator: how often each silo re-leases its share of every tenant's
/// cluster-wide rate, which apportionment strategy the coordinator uses, and the
/// reserve floor for demand-proportional leasing. Resolved through the standard
/// options system. Unlike <see cref="LatticeTenancyOptions"/>, this type is
/// <b>not</b> bound by the <c>AddLatticeTenancy(...)</c> /
/// <c>ConfigureLatticeTenancy(...)</c> delegate, which accepts only
/// <see cref="LatticeTenancyOptions"/>; configure it with
/// <c>services.Configure&lt;LatticeTenantRateLimiterOptions&gt;(...)</c>.
/// </summary>
/// <remarks>
/// These options never touch the per-op hot path; they govern only the
/// low-frequency coordinator (O(silos) at lease cadence). The limiter enforces
/// silo-local token buckets regardless of these settings, so a misconfiguration
/// affects only how the cluster rate is split, never whether enforcement is
/// lock-free.
/// </remarks>
public sealed class LatticeTenantRateLimiterOptions
{
    /// <summary>
    /// The default lease interval: how often the budget coordinator re-apportions
    /// each tenant's cluster rate across the live silos.
    /// </summary>
    /// <remarks>
    /// Sized against the cost of a cycle rather than the desired reaction time. A
    /// cycle reads the tenant registry, which is a whole-tree scan, so a cadence of
    /// a few seconds put a busy cluster at a 100% duty cycle - the next cycle
    /// started as soon as the previous one finished, and a slow registry never got
    /// a chance to drain. The configured rates themselves are cached with their own
    /// TTL (<see cref="RateSnapshotTtl"/>), so this interval governs only how often
    /// the buckets are re-apportioned from an already-resident snapshot.
    /// </remarks>
    public static readonly TimeSpan DefaultLeaseInterval = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The default bound on how long one lease cycle may run before it is
    /// cancelled, so a stalled registry read cannot occupy the loop indefinitely.
    /// </summary>
    public static readonly TimeSpan DefaultLeaseCycleTimeout = TimeSpan.FromSeconds(20);

    /// <summary>
    /// The default ceiling on the backed-off lease interval after consecutive cycle
    /// failures.
    /// </summary>
    public static readonly TimeSpan DefaultMaxLeaseBackoff = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The default lifetime of the cached tenant-rate snapshot, after which the next
    /// lease cycle re-reads the tenant registry.
    /// </summary>
    public static readonly TimeSpan DefaultRateSnapshotTtl = TimeSpan.FromMinutes(2);

    /// <summary>
    /// How often the budget coordinator re-leases each silo's share. Must be
    /// strictly positive; defaults to <see cref="DefaultLeaseInterval"/>. A longer
    /// interval lowers coordination cost but widens the transient overshoot bound
    /// (lease interval times cluster rate). A non-positive value falls back to the
    /// default.
    /// </summary>
    public TimeSpan LeaseInterval { get; set; } = DefaultLeaseInterval;

    /// <summary>
    /// The bound on a single lease cycle. A cycle that exceeds it is cancelled and
    /// retried on a later tick, so a stalled tenant-registry read can never occupy
    /// the loop for longer than one interval. Defaults to
    /// <see cref="DefaultLeaseCycleTimeout"/>; a non-positive value falls back to
    /// that default, and any value at or above <see cref="LeaseInterval"/> is
    /// clamped down to the interval so the duty cycle stays bounded.
    /// </summary>
    public TimeSpan LeaseCycleTimeout { get; set; } = DefaultLeaseCycleTimeout;

    /// <summary>
    /// The ceiling the lease interval backs off to after consecutive cycle
    /// failures. The effective interval doubles per consecutive failure and resets
    /// to <see cref="LeaseInterval"/> on the first success, so a persistently
    /// unhealthy registry is probed at a decaying rate instead of being hammered
    /// every tick. Defaults to <see cref="DefaultMaxLeaseBackoff"/>; a non-positive
    /// value falls back to that default, and a value below
    /// <see cref="LeaseInterval"/> disables backoff.
    /// </summary>
    public TimeSpan MaxLeaseBackoff { get; set; } = DefaultMaxLeaseBackoff;

    /// <summary>
    /// How long a read of the tenant registry's configured rates stays usable before
    /// the next lease cycle re-reads it. Configured rates change at administrative
    /// cadence, so caching them decouples the (frequent) re-apportionment of token
    /// buckets from the (expensive) whole-tree registry scan. Defaults to
    /// <see cref="DefaultRateSnapshotTtl"/>; a non-positive value falls back to that
    /// default.
    /// </summary>
    public TimeSpan RateSnapshotTtl { get; set; } = DefaultRateSnapshotTtl;

    /// <summary>
    /// The apportionment strategy. Defaults to
    /// <see cref="TenantRateApportionmentStrategy.Demand"/> (demand-proportional
    /// leasing), which degrades to static-even when no cluster-wide demand
    /// aggregate is available.
    /// </summary>
    public TenantRateApportionmentStrategy Apportionment { get; set; } = TenantRateApportionmentStrategy.Demand;

    /// <summary>
    /// The fraction of each tenant's cluster rate that demand-proportional leasing
    /// reserves and splits evenly across silos, guaranteeing an idle silo a
    /// non-zero floor so it can never be starved out of building demand. In
    /// <c>[0, 1]</c>; defaults to <c>0.2</c>. Ignored under static-even
    /// apportionment.
    /// </summary>
    public double DemandReserveFraction { get; set; } = 0.2;
}
