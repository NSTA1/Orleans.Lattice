using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Backs the <c>orleans.lattice.tenancy</c> per-tenant observable gauges. The
/// observability publisher calls <see cref="EnsureRegistered"/> once when it
/// starts to create the gauges (idempotent, process-wide), then calls
/// <see cref="Publish"/> on every sampling tick with a pre-built
/// <see cref="TenantObservabilityGaugeSnapshot"/>. Each gauge's measurement
/// callback returns the published snapshot's array for its instrument with a
/// single <see cref="Volatile"/> read, so the scrape path allocates nothing and
/// recomputes nothing.
/// </summary>
/// <remarks>
/// The gauges are created only when the tenancy add-on registers the publisher and
/// the publisher starts, so a cluster with tenancy disabled never creates this
/// meter and publishes no per-tenant series - the tenancy-off path is unchanged.
/// The publisher is the single writer; the snapshot reference is swapped
/// atomically, so a scrape observes a whole, self-consistent set of per-tenant
/// arrays with no lock on the scrape path.
/// </remarks>
internal static class TenantObservabilityGaugeRegistry
{
    private static readonly object Lock = new();
    private static bool _registered;
    private static TenantObservabilityGaugeSnapshot _snapshot = TenantObservabilityGaugeSnapshot.Empty;

    /// <summary>
    /// Creates the observable gauges on <see cref="LatticeTenantMetrics.Meter"/>
    /// exactly once. Safe to call from every publisher start.
    /// </summary>
    public static void EnsureRegistered()
    {
        if (Volatile.Read(ref _registered))
        {
            return;
        }

        lock (Lock)
        {
            if (_registered)
            {
                return;
            }

            var meter = LatticeTenantMetrics.Meter;

            meter.CreateObservableGauge(
                LatticeTenantMetrics.TenantsName,
                static () => LatticeTenantLabel.PlatformMeasurement(Volatile.Read(ref _snapshot).TenantCount),
                unit: "{tenant}",
                description: "Number of tenants in the warm per-tenant usage index (cluster aggregate, not tenant-scoped).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.UsageBytesName,
                static () => Volatile.Read(ref _snapshot).UsageBytes,
                unit: "By",
                description: "Per-tenant stored value bytes (global cross-cluster fold).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.UsageKeysName,
                static () => Volatile.Read(ref _snapshot).UsageKeys,
                unit: "{key}",
                description: "Per-tenant live key count (global cross-cluster fold).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.UsageMemoryBytesName,
                static () => Volatile.Read(ref _snapshot).UsageMemoryBytes,
                unit: "By",
                description: "Per-tenant resident memory bytes (global cross-cluster fold).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.UsageTreesName,
                static () => Volatile.Read(ref _snapshot).UsageTrees,
                unit: "{tree}",
                description: "Per-tenant owned tree count (global cross-cluster fold).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.QuotaBytesName,
                static () => Volatile.Read(ref _snapshot).QuotaBytes,
                unit: "By",
                description: "Per-tenant byte quota ceiling (bounded tenants only).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.QuotaKeysName,
                static () => Volatile.Read(ref _snapshot).QuotaKeys,
                unit: "{key}",
                description: "Per-tenant key quota ceiling (bounded tenants only).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.QuotaMemoryBytesName,
                static () => Volatile.Read(ref _snapshot).QuotaMemoryBytes,
                unit: "By",
                description: "Per-tenant memory quota ceiling (bounded tenants only).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.QuotaTreesName,
                static () => Volatile.Read(ref _snapshot).QuotaTrees,
                unit: "{tree}",
                description: "Per-tenant tree-count quota ceiling (bounded tenants only).");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.QuotaBurstPercentName,
                static () => Volatile.Read(ref _snapshot).QuotaBurstPercent,
                unit: "%",
                description: "Per-tenant transient burst headroom above the bounded ceilings, as a percentage.");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.OverageBytesName,
                static () => Volatile.Read(ref _snapshot).OverageBytes,
                unit: "By",
                description: "Per-tenant durable metered byte overage accrued by the overage meter.");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.OverageKeysName,
                static () => Volatile.Read(ref _snapshot).OverageKeys,
                unit: "{key}",
                description: "Per-tenant durable metered key overage accrued by the overage meter.");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.OverageMemoryBytesName,
                static () => Volatile.Read(ref _snapshot).OverageMemoryBytes,
                unit: "By",
                description: "Per-tenant durable metered memory overage accrued by the overage meter.");

            meter.CreateObservableGauge(
                LatticeTenantMetrics.OverageTreesName,
                static () => Volatile.Read(ref _snapshot).OverageTrees,
                unit: "{tree}",
                description: "Per-tenant durable metered tree-count overage accrued by the overage meter.");

            Volatile.Write(ref _registered, true);
        }
    }

    /// <summary>
    /// Publishes <paramref name="snapshot"/> as the latest per-tenant measurements
    /// the gauges observe. Called on the publisher's sampling timer, off the scrape
    /// path.
    /// </summary>
    /// <param name="snapshot">The pre-built per-tenant gauge snapshot. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="snapshot"/> is <c>null</c>.</exception>
    public static void Publish(TenantObservabilityGaugeSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        Volatile.Write(ref _snapshot, snapshot);
    }

    /// <summary>The latest published snapshot. Exposed for tests; the gauges read it directly.</summary>
    public static TenantObservabilityGaugeSnapshot Latest => Volatile.Read(ref _snapshot);
}
