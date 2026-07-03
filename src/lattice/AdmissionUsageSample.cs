namespace Orleans.Lattice;

/// <summary>
/// Immutable in-process snapshot of a single tree's admission-control aggregate,
/// pushed by the per-tree storage-usage aggregator to
/// <see cref="LatticeAdmissionMetrics"/> so the observable admission gauges can
/// be computed at scrape time from one record. Passed only in-process (aggregator
/// grain to the DI-singleton sink on the same silo), so it carries no Orleans
/// serialization attributes.
/// </summary>
internal readonly record struct AdmissionUsageSample
{
    /// <summary>Logical tree identifier.</summary>
    public string TreeId { get; init; }

    /// <summary>Current live (non-tombstone) key count for the tree.</summary>
    public long LiveKeys { get; init; }

    /// <summary>Current estimated retained bytes for the tree (aliases the storage total).</summary>
    public long EstimatedBytes { get; init; }

    /// <summary>The resolved enforcing live-key cap, or <see langword="null"/> when unbounded.</summary>
    public long? MaxLiveKeys { get; init; }

    /// <summary>The resolved enforcing estimated-byte cap, or <see langword="null"/> when unbounded.</summary>
    public long? MaxEstimatedBytes { get; init; }

    /// <summary>The resolved advisory live-key ceiling, or <see langword="null"/> when unset.</summary>
    public long? AdvisoryLiveKeys { get; init; }

    /// <summary>The resolved advisory estimated-byte ceiling, or <see langword="null"/> when unset.</summary>
    public long? AdvisoryBytes { get; init; }
}
