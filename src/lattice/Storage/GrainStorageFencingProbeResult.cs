namespace Orleans.Lattice;

/// <summary>
/// The result of one grain-storage fencing probe.
/// </summary>
/// <param name="Verdict">What the probe concluded.</param>
/// <param name="Reason">A short operator-facing explanation of the verdict.</param>
/// <param name="Fault">The exception that made the probe inconclusive, when there was one.</param>
internal sealed record GrainStorageFencingProbeResult(
    GrainStorageFencingVerdict Verdict,
    string Reason,
    Exception? Fault = null);
