namespace Orleans.Lattice;

/// <summary>What the grain-storage fencing probe concluded.</summary>
internal enum GrainStorageFencingVerdict
{
    /// <summary>The provider rejected a write carrying a stale ETag.</summary>
    Fenced = 0,

    /// <summary>The provider accepted a write carrying a stale ETag.</summary>
    Unfenced = 1,

    /// <summary>
    /// The probe could not reach a verdict: the provider faulted, timed out,
    /// is not registered, or kept losing races to concurrent probes.
    /// </summary>
    Inconclusive = 2,
}
