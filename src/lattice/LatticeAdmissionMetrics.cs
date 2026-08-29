using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice;

/// <summary>
/// Process-wide sink that backs the observable per-tree admission-control gauges
/// on the <see cref="LatticeMetrics.Meter"/>. The per-tree storage-usage
/// aggregator pushes the latest <see cref="AdmissionUsageSample"/> for each tree
/// here (already coalesced behind <see cref="LatticeOptions.StorageUsageCacheTtl"/>),
/// and the four observable gauges - <c>admission.live_keys</c>,
/// <c>admission.estimated_bytes</c>, the 0/1 <c>admission.over_advisory</c>
/// gauge, and the per-dimension <c>admission.utilization</c> ratio - read from
/// that last-known snapshot when a listener observes the meter. Registration is
/// process-wide and idempotent; observation unions every live instance, so when
/// more than one silo is co-hosted in a single process (an in-process
/// multi-silo test cluster) each silo's DI singleton contributes its own
/// published series rather than all but the most-recently-constructed sink being
/// silently dropped.
/// <para>
/// Gauges are observable so they cost nothing when no OpenTelemetry listener is
/// attached: the measurement callback only runs on scrape, and it reads a
/// concurrent dictionary rather than fanning out to grains. A tree contributes a
/// measurement only after the aggregator has pushed at least one sample for it,
/// so the gauges report "no data" (no measurement) rather than a wrong zero for
/// a tree that has never been sampled. The <c>over_advisory</c> and
/// <c>utilization</c> gauges additionally contribute a measurement only for the
/// dimensions whose ceiling (advisory or enforcing) is configured, so a tree
/// that has set no ceiling emits neither.
/// </para>
/// <para>
/// This sink backs only the observable gauges. The write-path admission guard in
/// <c>LatticeGrain</c> does not read it; it reads the same aggregate directly
/// from the per-tree storage-usage aggregator so it works across silos even when
/// the aggregator activation is hosted on a different silo than the writer.
/// </para>
/// </summary>
internal sealed class LatticeAdmissionMetrics : IDisposable
{
    private static readonly object RegistrationLock = new();
    private static readonly List<LatticeAdmissionMetrics> Instances = new();
    private static bool _gaugesRegistered;

    private readonly ConcurrentDictionary<string, (AdmissionUsageSample Sample, DateTimeOffset PublishedAt)> _samples =
        new(StringComparer.Ordinal);
    private readonly TimeProvider _time;

    /// <summary>Default <see cref="StalenessHorizon"/> (60 seconds).</summary>
    public static readonly TimeSpan DefaultStalenessHorizon = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How long a published per-tree sample keeps contributing a measurement
    /// after the last <see cref="Publish"/> that touched it. A series not
    /// refreshed within this window stops being observed, so when a tree's
    /// aggregator migrates to another silo the old silo's sink stops emitting
    /// that tree's now-stale value and a cross-silo aggregation does not
    /// double-count it. Must be positive.
    /// </summary>
    public TimeSpan StalenessHorizon { get; set; } = DefaultStalenessHorizon;

    /// <summary>
    /// Initialises a new instance and ensures the observable admission gauges
    /// declared on <see cref="LatticeMetrics"/> are registered on the shared
    /// meter. Gauge registration is process-wide and idempotent.
    /// </summary>
    public LatticeAdmissionMetrics() : this(null)
    {
    }

    /// <summary>
    /// Initialises a new instance with an explicit <paramref name="timeProvider"/>
    /// (used by tests to drive the staleness horizon deterministically) and
    /// ensures the observable admission gauges are registered on the shared
    /// meter. Gauge registration is process-wide and idempotent.
    /// </summary>
    public LatticeAdmissionMetrics(TimeProvider? timeProvider)
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
    /// published series stop contributing to the admission gauges. Called by the
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
            LatticeMetrics.AdmissionLiveKeysName,
            static () => ObserveLong(static s => s.LiveKeys),
            unit: "{key}",
            description: "Current live (non-tombstone) key count for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.AdmissionEstimatedBytesName,
            static () => ObserveLong(static s => s.EstimatedBytes),
            unit: "By",
            description: "Current estimated retained bytes for the tree.");

        meter.CreateObservableGauge(
            LatticeMetrics.AdmissionOverAdvisoryName,
            static () => ObserveOverAdvisory(),
            unit: "1",
            description: "1 when the tree currently exceeds an advisory admission ceiling, else 0.");

        meter.CreateObservableGauge(
            LatticeMetrics.AdmissionUtilizationName,
            static () => ObserveUtilization(),
            unit: "1",
            description: "Current / ceiling admission utilisation ratio per dimension (keys, bytes).");
    }

    /// <summary>
    /// Publishes the latest admission sample for a tree so the admission gauges
    /// reflect it on the next scrape. Called by the per-tree aggregator after it
    /// assembles (or serves from cache) a report. Stamps the publish time so a
    /// series the aggregator stops refreshing (because it migrated to another
    /// silo) expires from this silo's sink after <see cref="StalenessHorizon"/>.
    /// </summary>
    public void Publish(AdmissionUsageSample sample)
    {
        ArgumentNullException.ThrowIfNull(sample.TreeId);
        _samples[sample.TreeId] = (sample, _time.GetUtcNow());
    }

    private static IEnumerable<Measurement<long>> ObserveLong(Func<AdmissionUsageSample, long> selector)
    {
        foreach (var instance in Snapshot())
        {
            foreach (var measurement in instance.Observe(selector))
            {
                yield return measurement;
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveOverAdvisory()
    {
        foreach (var instance in Snapshot())
        {
            foreach (var measurement in instance.ObserveOverAdvisoryLocal())
            {
                yield return measurement;
            }
        }
    }

    private static IEnumerable<Measurement<double>> ObserveUtilization()
    {
        foreach (var instance in Snapshot())
        {
            foreach (var measurement in instance.ObserveUtilizationLocal())
            {
                yield return measurement;
            }
        }
    }

    private static LatticeAdmissionMetrics[] Snapshot()
    {
        lock (RegistrationLock)
        {
            return Instances.ToArray();
        }
    }

    private IEnumerable<Measurement<long>> Observe(Func<AdmissionUsageSample, long> selector)
    {
        var cutoff = _time.GetUtcNow() - StalenessHorizon;
        foreach (var kv in _samples)
        {
            var (sample, publishedAt) = kv.Value;
            if (publishedAt < cutoff)
            {
                _samples.TryRemove(kv.Key, out _);
                continue;
            }
            yield return new Measurement<long>(
                selector(sample),
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    private IEnumerable<Measurement<long>> ObserveOverAdvisoryLocal()
    {
        var cutoff = _time.GetUtcNow() - StalenessHorizon;
        foreach (var kv in _samples)
        {
            var (sample, publishedAt) = kv.Value;
            if (publishedAt < cutoff)
            {
                _samples.TryRemove(kv.Key, out _);
                continue;
            }

            // Only emit for a tree that has set at least one advisory ceiling;
            // a tree with no advisory ceiling has nothing to be "over".
            if (sample.AdvisoryLiveKeys is null && sample.AdvisoryBytes is null)
            {
                continue;
            }

            var over =
                (sample.AdvisoryLiveKeys is { } advK && sample.LiveKeys >= advK) ||
                (sample.AdvisoryBytes is { } advB && sample.EstimatedBytes >= advB);

            yield return new Measurement<long>(
                over ? 1L : 0L,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    private IEnumerable<Measurement<double>> ObserveUtilizationLocal()
    {
        var cutoff = _time.GetUtcNow() - StalenessHorizon;
        foreach (var kv in _samples)
        {
            var (sample, publishedAt) = kv.Value;
            if (publishedAt < cutoff)
            {
                _samples.TryRemove(kv.Key, out _);
                continue;
            }

            var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, kv.Key);
            var tenantTag = LatticeTenantLabel.ForTree(kv.Key);

            // Prefer the enforcing cap as the denominator; fall back to the
            // advisory ceiling when only that is configured. Emit per dimension
            // only when a ceiling exists for it.
            var keysCeiling = sample.MaxLiveKeys ?? sample.AdvisoryLiveKeys;
            if (keysCeiling is { } kc && kc > 0)
            {
                yield return new Measurement<double>(
                    (double)sample.LiveKeys / kc,
                    treeTag,
                    LatticeMetrics.DimensionKeys,
                    tenantTag);
            }

            var bytesCeiling = sample.MaxEstimatedBytes ?? sample.AdvisoryBytes;
            if (bytesCeiling is { } bc && bc > 0)
            {
                yield return new Measurement<double>(
                    (double)sample.EstimatedBytes / bc,
                    treeTag,
                    LatticeMetrics.DimensionBytes,
                    tenantTag);
            }
        }
    }
}
