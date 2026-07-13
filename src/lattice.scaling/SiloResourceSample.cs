namespace Orleans.Lattice.Scaling;

/// <summary>
/// A single silo's raw resource sample, normalisation-ready and deliberately
/// Orleans-agnostic so the normalisation math (<see cref="ComputePressureMath"/>)
/// and its tests do not depend on Orleans runtime types. Populated from the
/// cluster management runtime statistics (CPU usage, memory used, and the
/// cgroup-honouring maximum-available memory) plus the silo's activation count.
/// </summary>
internal readonly record struct SiloResourceSample
{
    /// <summary>The silo's CPU usage as a percentage in the range 0..100.</summary>
    public double CpuUsagePercent { get; init; }

    /// <summary>Bytes of memory currently in use on the silo.</summary>
    public long MemoryUsedBytes { get; init; }

    /// <summary>
    /// The maximum memory in bytes available to the silo. This honours any
    /// cgroup / container memory cap (an ACA or AKS memory limit), so memory
    /// pressure is measured against the enforced ceiling rather than the raw
    /// machine total. Non-positive when the provider reports no ceiling.
    /// </summary>
    public long MaximumAvailableMemoryBytes { get; init; }

    /// <summary>The number of grain activations currently hosted on the silo.</summary>
    public int ActivationCount { get; init; }
}
