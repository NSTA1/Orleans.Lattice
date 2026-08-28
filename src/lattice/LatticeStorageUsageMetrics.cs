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
/// meter. Registration is process-wide and idempotent; observation unions
/// every live instance, so when more than one silo is co-hosted in a single
/// process (for example an in-process multi-silo test cluster) each silo's
/// DI singleton contributes its own published series rather than all but the
/// most-recently-constructed sink being silently dropped. In the normal
/// one-silo-per-process model there is a single instance and the union is
/// that instance, matching the DI singleton model used by <c>AddLattice</c>.
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
public sealed class LatticeStorageUsageMetrics : IDisposable
{
    private static readonly object RegistrationLock = new();
    private static readonly List<LatticeStorageUsageMetrics> Instances = new();
    private static bool _gaugesRegistered;

    private readonly ConcurrentDictionary<string, (TreeStorageUsageReport Report, DateTimeOffset PublishedAt)> _reports = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, (bool OverThreshold, DateTimeOffset PublishedAt)> _overThreshold = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;

    /// <summary>Default <see cref="StalenessHorizon"/> (60 seconds).</summary>
    public static readonly TimeSpan DefaultStalenessHorizon = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long a published per-tree series keeps contributing a measurement
    /// after the last <see cref="Publish"/> / <see cref="PublishOverThreshold"/>
    /// that touched it. A series not refreshed within this window stops being
    /// observed, so when a tree's storage-usage aggregator migrates to another
    /// silo the <i>old</i> silo's sink stops emitting that tree's now-stale
    /// value and a cross-silo <c>sum by (tree)</c> does not double-count it.
    /// Set by the background poller to a small multiple of its poll interval;
    /// defaults to <see cref="DefaultStalenessHorizon"/>. Must be positive.
    /// </summary>
    public TimeSpan StalenessHorizon { get; set; } = DefaultStalenessHorizon;

    /// <summary>
    /// Initialises a new instance and ensures the observable storage-usage
    /// gauges declared on <see cref="LatticeMetrics"/> are registered on the
    /// shared meter. Gauge registration is process-wide and idempotent.
    /// </summary>
    public LatticeStorageUsageMetrics() : this(null)
    {
    }

    /// <summary>
    /// Initialises a new instance with an explicit <paramref name="timeProvider"/>
    /// (used by tests to drive the staleness horizon deterministically) and
    /// ensures the observable storage-usage gauges are registered on the shared
    /// meter. Gauge registration is process-wide and idempotent.
    /// </summary>
    public LatticeStorageUsageMetrics(TimeProvider? timeProvider)
    {
        _time = timeProvider ?? TimeProvider.System;
        lock (RegistrationLock)
        {
            Instances.Add(this);
            if (!_gaugesRegistered)
            {
                RegisterGauges();
                _gaugesRegistered = true;
            }
        }
    }

    /// <summary>
    /// Removes this sink instance from the process-wide observation set so its
    /// published series stop contributing to the storage gauges. Called by the
    /// DI container when the owning silo shuts down; the gauges themselves stay
    /// registered on the shared meter (instruments cannot be unregistered) but
    /// simply observe no measurements from a disposed instance.
    /// </summary>
    public void Dispose()
    {
        lock (RegistrationLock)
        {
            Instances.Remove(this);
        }
    }

    private static void RegisterGauges()
    {
        var meter = LatticeMetrics.Meter;

        meter.CreateObservableGauge(
            LatticeMetrics.StorageWalBytesName,
            static () => ObserveAll(static r => r.WalRetainedBytes),
            unit: "By",
            description: "Retained WAL bytes for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StorageSnapshotBytesName,
            static () => ObserveAll(static r => r.SnapshotBytes),
            unit: "By",
            description: "Snapshot blob bytes for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StorageLeafStateBytesName,
            static () => ObserveAll(static r => r.LeafStateBytes),
            unit: "By",
            description: "Summed leaf/shard-root state bytes for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StorageTotalBytesName,
            static () => ObserveAll(static r => r.TotalBytes),
            unit: "By",
            description: "Sum of the three storage surfaces for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.StoragePolicyOverThresholdName,
            static () => ObserveAllOverThreshold(),
            unit: "1",
            description: "1 when the tree's retained WAL bytes currently breach the advisory ceiling, else 0.");
    }

    /// <summary>
    /// Unions the per-tree byte measurements of every live sink instance. A
    /// tree's aggregator is a single cluster-wide activation, so within one
    /// process a given tree is published through at most one instance and the
    /// union never double-counts it; co-hosted silos each contribute the trees
    /// they host.
    /// </summary>
    private static IEnumerable<Measurement<long>> ObserveAll(Func<TreeStorageUsageReport, long> selector)
    {
        LatticeStorageUsageMetrics[] snapshot;
        lock (RegistrationLock)
        {
            snapshot = Instances.ToArray();
        }
        foreach (var instance in snapshot)
        {
            foreach (var measurement in instance.Observe(selector))
            {
                yield return measurement;
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveAllOverThreshold()
    {
        LatticeStorageUsageMetrics[] snapshot;
        lock (RegistrationLock)
        {
            snapshot = Instances.ToArray();
        }
        foreach (var instance in snapshot)
        {
            foreach (var measurement in instance.ObserveOverThreshold())
            {
                yield return measurement;
            }
        }
    }

    /// <summary>
    /// Publishes the latest storage-usage report for a tree so the byte
    /// gauges reflect it on the next scrape. Called by the per-tree aggregator
    /// after it assembles (or serves from cache) a report. Stamps the publish
    /// time so a series the poller stops refreshing (because the aggregator
    /// migrated to another silo) expires from this silo's sink after
    /// <see cref="StalenessHorizon"/>. This is the deep-refresh path; the
    /// cluster-wide background poller no longer drives it (see
    /// <see cref="PublishWal"/> for the cheap WAL-only refresh path).
    /// </summary>
    public void Publish(TreeStorageUsageReport report)
    {
        ArgumentNullException.ThrowIfNull(report.TreeId);
        _reports[report.TreeId] = (report, _time.GetUtcNow());
    }

    /// <summary>
    /// Refreshes only the WAL-bytes series and the WAL-bytes
    /// publish-timestamp for a tree, without touching the snapshot,
    /// leaf-state, or total surfaces. Called by the cluster-wide
    /// background poller and the per-tree WAL-only aggregator so the
    /// byte-pressure path and the <c>storage.wal_bytes</c> gauge stay
    /// timely without paying the cost of a deep leaf/snapshot fan-out.
    /// Snapshot / leaf-state / total bytes continue to reflect the last
    /// deep publish (if any) until an explicit
    /// <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/> or
    /// <see cref="ILattice.GetStorageUsageAsync"/> caller drives a fresh
    /// deep report through <see cref="Publish"/>.
    /// </summary>
    public void PublishWal(TreeWalUsageReport report)
    {
        ArgumentNullException.ThrowIfNull(report.TreeId);
        if (report.Partial)
        {
            // A partial WAL surface is the "unsupported by provider"
            // sentinel; do not republish a wrong byte count and do not
            // refresh the timestamp (so the last deep value (if any)
            // ages out naturally rather than being pinned by polling).
            return;
        }
        var now = _time.GetUtcNow();
        _reports.AddOrUpdate(
            report.TreeId,
            _ => (new TreeStorageUsageReport
            {
                TreeId = report.TreeId,
                WalRetainedBytes = report.WalRetainedBytes,
                SnapshotBytes = 0,
                LeafStateBytes = 0,
                TotalBytes = report.WalRetainedBytes,
                Partial = false,
                SampledAt = report.SampledAt,
            }, now),
            (_, existing) =>
            {
                var prev = existing.Report;
                var merged = new TreeStorageUsageReport
                {
                    TreeId = prev.TreeId,
                    WalRetainedBytes = report.WalRetainedBytes,
                    SnapshotBytes = prev.SnapshotBytes,
                    LeafStateBytes = prev.LeafStateBytes,
                    TotalBytes = report.WalRetainedBytes + prev.SnapshotBytes + prev.LeafStateBytes,
                    Partial = prev.Partial,
                    SampledAt = report.SampledAt,
                };
                return (merged, now);
            });
    }

    /// <summary>
    /// Records whether the named tree's retained WAL bytes currently breach
    /// the advisory ceiling, driving the <c>storage.policy.over_threshold</c>
    /// 0/1 gauge. Pushed by the per-tree aggregator and by the WAL garbage
    /// collector after each byte-pressure evaluation so the gauge tracks the
    /// most recent observation regardless of which subsystem sampled it.
    /// Stamps the publish time so a stale series expires after
    /// <see cref="StalenessHorizon"/> once it stops being refreshed.
    /// </summary>
    public void PublishOverThreshold(string treeId, bool overThreshold)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        _overThreshold[treeId] = (overThreshold, _time.GetUtcNow());
    }

    private IEnumerable<Measurement<long>> Observe(Func<TreeStorageUsageReport, long> selector)
    {
        var cutoff = _time.GetUtcNow() - StalenessHorizon;
        foreach (var kv in _reports)
        {
            var (report, publishedAt) = kv.Value;
            if (publishedAt < cutoff)
            {
                // Series not refreshed within the horizon: the aggregator
                // for this tree has migrated away (or the host stopped
                // polling). Drop it so a migrated tree is reported by
                // exactly one silo and a cross-silo sum is not doubled.
                _reports.TryRemove(kv.Key, out _);
                continue;
            }

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
        var cutoff = _time.GetUtcNow() - StalenessHorizon;
        foreach (var kv in _overThreshold)
        {
            var (overThreshold, publishedAt) = kv.Value;
            if (publishedAt < cutoff)
            {
                _overThreshold.TryRemove(kv.Key, out _);
                continue;
            }
            yield return new Measurement<long>(
                overThreshold ? 1L : 0L,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key));
        }
    }
}
