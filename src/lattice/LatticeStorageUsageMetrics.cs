using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice;

/// <summary>
/// Process-wide sink that backs the observable storage-usage gauges on the
/// <see cref="LatticeMetrics.Meter"/>. The per-tree storage-usage aggregator
/// pushes the latest <see cref="TreeStorageUsageReport"/> for each tree here
/// (already coalesced behind <see cref="LatticeOptions.StorageUsageCacheTtl"/>),
/// and the five observable gauges - <c>storage.wal_bytes</c>,
/// <c>storage.snapshot_bytes</c>, <c>storage.leaf_state_bytes</c>,
/// <c>storage.total_bytes</c>, and the 0/1 <c>storage.policy.over_threshold</c>
/// gauge - read from that last-known snapshot when a listener observes the
/// meter. Registration is process-wide and idempotent; observation always
/// reflects the most recently constructed instance, matching the DI singleton
/// model used by <c>AddLattice</c>.
/// <para>
/// Gauges are observable so they cost nothing when no OpenTelemetry listener
/// is attached: the measurement callback only runs on scrape, and it reads a
/// concurrent dictionary rather than fanning out to grains. A tree contributes
/// a byte measurement only after the aggregator has pushed at least one report
/// for it, so the gauges report "no data" (no measurement) rather than a wrong
/// zero for a tree that has never been sampled - which is also how an
/// unsupported provider (<see cref="TreeStorageUsageReport.Partial"/>) surfaces.
/// The over-threshold gauge likewise contributes a measurement for a tree only
/// once a byte-pressure evaluation has observed it.
/// </para>
/// </summary>
public sealed class LatticeStorageUsageMetrics
{
    private static readonly object RegistrationLock = new();
    private static volatile LatticeStorageUsageMetrics? _current;
    private static bool _gaugesRegistered;

    private readonly ConcurrentDictionary<string, TreeStorageUsageReport> _reports = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _overThreshold = new(StringComparer.Ordinal);

    /// <summary>
    /// Initialises a new instance and ensures the observable storage-usage
    /// gauges declared on <see cref="LatticeMetrics"/> are registered on the
    /// shared meter. Gauge registration is process-wide and idempotent.
    /// </summary>
    public LatticeStorageUsageMetrics()
    {
        lock (RegistrationLock)
        {
            _current = this;
            if (!_gaugesRegistered)
            {
                RegisterGauges();
                _gaugesRegistered = true;
            }
        }
    }

    private static void RegisterGauges()
    {
        var meter = LatticeMetrics.Meter;

        meter.CreateObservableGauge(
            LatticeMetrics.StorageWalBytesName,
            static () => _current?.Observe(static r => r.WalRetainedBytes) ?? Array.Empty<Measurement<long>>(),
            unit: "By",
            description: "Retained WAL bytes for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StorageSnapshotBytesName,
            static () => _current?.Observe(static r => r.SnapshotBytes) ?? Array.Empty<Measurement<long>>(),
            unit: "By",
            description: "Snapshot blob bytes for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StorageLeafStateBytesName,
            static () => _current?.Observe(static r => r.LeafStateBytes) ?? Array.Empty<Measurement<long>>(),
            unit: "By",
            description: "Summed leaf/shard-root state bytes for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StorageTotalBytesName,
            static () => _current?.Observe(static r => r.TotalBytes) ?? Array.Empty<Measurement<long>>(),
            unit: "By",
            description: "Sum of the three storage surfaces for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StoragePolicyOverThresholdName,
            static () => _current?.ObserveOverThreshold() ?? Array.Empty<Measurement<long>>(),
            unit: "{tree}",
            description: "1 when the tree's retained WAL bytes currently breach the advisory ceiling, else 0.");
    }

    /// <summary>
    /// Publishes the latest storage-usage report for a tree so the byte
    /// gauges reflect it on the next scrape. Called by the per-tree aggregator
    /// after it assembles (or serves from cache) a report.
    /// </summary>
    public void Publish(TreeStorageUsageReport report)
    {
        ArgumentNullException.ThrowIfNull(report.TreeId);
        _reports[report.TreeId] = report;
    }

    /// <summary>
    /// Records whether the named tree's retained WAL bytes currently breach
    /// the advisory ceiling, driving the <c>storage.policy.over_threshold</c>
    /// 0/1 gauge. Pushed by the per-tree aggregator and by the WAL garbage
    /// collector after each byte-pressure evaluation so the gauge tracks the
    /// most recent observation regardless of which subsystem sampled it.
    /// </summary>
    public void PublishOverThreshold(string treeId, bool overThreshold)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        _overThreshold[treeId] = overThreshold;
    }

    private IEnumerable<Measurement<long>> Observe(Func<TreeStorageUsageReport, long> selector)
    {
        foreach (var kv in _reports)
        {
            var report = kv.Value;
            if (report.Partial)
            {
                // A partial report means at least one surface reported the
                // "unsupported" sentinel; do not publish a wrong byte count.
                continue;
            }
            yield return new Measurement<long>(
                selector(report),
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key));
        }
    }

    private IEnumerable<Measurement<long>> ObserveOverThreshold()
    {
        foreach (var kv in _overThreshold)
        {
            yield return new Measurement<long>(
                kv.Value ? 1L : 0L,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key));
        }
    }
}
