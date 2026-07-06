using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Backup;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the
/// <c>Orleans.Lattice.Backup</c> package, published on a single
/// <see cref="Meter"/> named <see cref="MeterName"/> so an OpenTelemetry pipeline
/// can subscribe once and receive every backup metric. Mirrors the core
/// <c>Orleans.Lattice</c> meter conventions (durations in milliseconds as
/// <c>double</c>).
/// </summary>
public static class BackupMetrics
{
    /// <summary>The root meter name for all Orleans.Lattice.Backup telemetry.</summary>
    public const string MeterName = "orleans.lattice.backup";

    /// <summary>Tag key for the participating-tree count of a cross-tree-consistent backup set.</summary>
    public const string TagTreeCount = "tree_count";

    /// <summary>
    /// The meter that owns every backup instrument. Exposed publicly so
    /// integration tests and custom exporters can subscribe by reference.
    /// </summary>
    public static readonly Meter Meter = new(MeterName);

    /// <summary>
    /// Counter incremented once per cross-tree-consistent backup-set fence
    /// selection (one per successful <see cref="ILatticeBackupCaptureService.CaptureSetAsync"/>
    /// with the cross-tree flag set over more than one tree). Tagged with
    /// <see cref="TagTreeCount"/>.
    /// </summary>
    public static readonly Counter<long> CrossTreeFenceSelections =
        Meter.CreateCounter<long>("orleans.lattice.backup.cross_tree_fence.selections", unit: "{fence}",
            description: "Cross-tree-consistent backup-set fences selected.");

    /// <summary>
    /// Counter incremented by the number of in-flight cross-tree sagas a fence
    /// waited to drain before capturing. Zero-increments are skipped.
    /// </summary>
    public static readonly Counter<long> CrossTreeFenceDrainedInFlight =
        Meter.CreateCounter<long>("orleans.lattice.backup.cross_tree_fence.drained_in_flight", unit: "{saga}",
            description: "In-flight cross-tree sagas a backup-set fence waited to drain.");

    /// <summary>
    /// Counter incremented once per additional fence attempt beyond the first,
    /// i.e. once each time a cross-tree saga registered during the capture window
    /// and forced the fence to retry.
    /// </summary>
    public static readonly Counter<long> CrossTreeFenceRetries =
        Meter.CreateCounter<long>("orleans.lattice.backup.cross_tree_fence.retries", unit: "{retry}",
            description: "Backup-set fence retries forced by a cross-tree saga registering during the capture window.");

    /// <summary>
    /// Histogram of the total wall-clock time, in milliseconds, a fence spent
    /// waiting for in-flight cross-tree sagas to drain.
    /// </summary>
    public static readonly Histogram<double> CrossTreeFenceDrainWaitMilliseconds =
        Meter.CreateHistogram<double>("orleans.lattice.backup.cross_tree_fence.drain_wait", unit: "ms",
            description: "Wall-clock time a backup-set fence waited for in-flight cross-tree sagas to drain.");
}
