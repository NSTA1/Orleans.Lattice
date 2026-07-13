namespace Orleans.Lattice.Scaling;

/// <summary>
/// Pure, allocation-free normalisation helpers for the compute axis. Kept
/// separate from the collectors so the normalisation rules - including the
/// cgroup-honouring memory case and the WAL-saturation mapping - are unit-tested
/// directly without standing up a cluster.
/// </summary>
internal static class ComputePressureMath
{
    /// <summary>
    /// The compute-axis contribution of <see cref="WalSaturationState.Throttled"/>:
    /// the WAL dispatch pipeline is admitting after a wait, a partial (0.5)
    /// compute-bound pressure.
    /// </summary>
    internal const double ThrottledWalDispatch = 0.5;

    /// <summary>
    /// The compute-axis contribution of <see cref="WalSaturationState.Saturated"/>:
    /// the WAL dispatch pipeline is at its admission ceiling, full (1.0)
    /// compute-bound pressure.
    /// </summary>
    internal const double SaturatedWalDispatch = 1.0;

    /// <summary>
    /// Clamps <paramref name="value"/> into the inclusive range 0..1. Non-finite
    /// inputs (NaN, infinity) clamp to <c>0.0</c> so a bad sample cannot inject a
    /// spurious pressure spike.
    /// </summary>
    /// <param name="value">The value to clamp.</param>
    /// <returns>The clamped value in the range 0..1.</returns>
    internal static double Clamp01(double value)
    {
        if (double.IsNaN(value) || value < 0d)
        {
            return 0d;
        }

        return value > 1d ? 1d : value;
    }

    /// <summary>
    /// Normalises a single silo's resource sample to a 0..1 pressure: the worse of
    /// CPU utilisation (usage percent / 100) and memory utilisation (used bytes /
    /// cgroup-honouring maximum-available bytes). When the sample reports no
    /// memory ceiling (<see cref="SiloResourceSample.MaximumAvailableMemoryBytes"/>
    /// non-positive) the memory term is treated as zero so only CPU contributes.
    /// </summary>
    /// <param name="sample">The per-silo resource sample.</param>
    /// <returns>The silo's normalised resource pressure in the range 0..1.</returns>
    internal static double NormaliseResource(SiloResourceSample sample)
    {
        var cpu = Clamp01(sample.CpuUsagePercent / 100d);

        var memory = 0d;
        if (sample.MaximumAvailableMemoryBytes > 0 && sample.MemoryUsedBytes > 0)
        {
            memory = Clamp01((double)sample.MemoryUsedBytes / sample.MaximumAvailableMemoryBytes);
        }

        return Math.Max(cpu, memory);
    }

    /// <summary>
    /// Normalises a silo's activation count against the per-silo working-set
    /// target: <c>activationCount / target</c>, clamped to 0..1. A non-positive
    /// target disables the activation dimension (returns <c>0.0</c>).
    /// </summary>
    /// <param name="activationCount">The silo's current activation count.</param>
    /// <param name="workingSetTarget">The per-silo activation count at which the silo is considered saturated.</param>
    /// <returns>The silo's normalised activation pressure in the range 0..1.</returns>
    internal static double NormaliseActivation(int activationCount, int workingSetTarget)
    {
        if (workingSetTarget <= 0 || activationCount <= 0)
        {
            return 0d;
        }

        return Clamp01((double)activationCount / workingSetTarget);
    }

    /// <summary>
    /// Maps a WAL saturation state to its compute-axis dispatch pressure. Only the
    /// dispatch / compute-bound portion is represented here; the backend-bound
    /// storage portion is the storage axis's concern (#1187).
    /// </summary>
    /// <param name="state">The aggregate WAL saturation state.</param>
    /// <returns><c>0.0</c> for Healthy, <c>0.5</c> for Throttled, <c>1.0</c> for Saturated.</returns>
    internal static double MapWalDispatch(WalSaturationState state) => state switch
    {
        WalSaturationState.Saturated => SaturatedWalDispatch,
        WalSaturationState.Throttled => ThrottledWalDispatch,
        _ => 0d,
    };
}
