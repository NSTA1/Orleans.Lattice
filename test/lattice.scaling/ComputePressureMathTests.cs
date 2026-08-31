namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Unit coverage for <see cref="ComputePressureMath"/>: the 0..1 clamp, the
/// per-silo resource normalisation (including the cgroup-limited memory case),
/// the activation normalisation against the working-set target, and the
/// WAL-saturation to dispatch-pressure mapping.
/// </summary>
[TestFixture]
public sealed class ComputePressureMathTests
{
    [Test]
    public void Clamp01_passes_through_in_range_values()
    {
        Assert.That(ComputePressureMath.Clamp01(0.42), Is.EqualTo(0.42));
    }

    [Test]
    public void Clamp01_clamps_above_one_to_one()
    {
        Assert.That(ComputePressureMath.Clamp01(1.7), Is.EqualTo(1d));
    }

    [Test]
    public void Clamp01_clamps_negative_to_zero()
    {
        Assert.That(ComputePressureMath.Clamp01(-0.3), Is.Zero);
    }

    [Test]
    public void Clamp01_maps_nan_to_zero()
    {
        Assert.That(ComputePressureMath.Clamp01(double.NaN), Is.Zero);
    }

    [Test]
    public void Clamp01_maps_positive_infinity_to_zero()
    {
        // The documented contract is that non-finite inputs clamp to 0.0 (the
        // safe floor). A NaN-only guard let positive infinity slip through and
        // clamp to 1.0 - a spurious maximum-pressure spike that could trigger a
        // bogus scale-out.
        Assert.That(ComputePressureMath.Clamp01(double.PositiveInfinity), Is.Zero);
    }

    [Test]
    public void Clamp01_maps_negative_infinity_to_zero()
    {
        Assert.That(ComputePressureMath.Clamp01(double.NegativeInfinity), Is.Zero);
    }

    [Test]
    public void NormaliseResource_uses_cpu_when_cpu_is_the_worse_dimension()
    {
        var sample = new SiloResourceSample
        {
            CpuUsagePercent = 80,
            MemoryUsedBytes = 1,
            MaximumAvailableMemoryBytes = 100,
        };

        Assert.That(ComputePressureMath.NormaliseResource(sample), Is.EqualTo(0.8).Within(1e-9));
    }

    [Test]
    public void NormaliseResource_ignores_memory_when_no_usage_is_reported()
    {
        // A silo that reports a memory ceiling but no usage yet must contribute
        // only its CPU term rather than a divide-by-usage artefact.
        var sample = new SiloResourceSample
        {
            CpuUsagePercent = 30,
            MemoryUsedBytes = 0,
            MaximumAvailableMemoryBytes = 100,
        };

        Assert.That(ComputePressureMath.NormaliseResource(sample), Is.EqualTo(0.3).Within(1e-9));
    }

    [Test]
    public void NormaliseResource_uses_memory_when_memory_is_the_worse_dimension()
    {
        var sample = new SiloResourceSample
        {
            CpuUsagePercent = 10,
            MemoryUsedBytes = 90,
            MaximumAvailableMemoryBytes = 100,
        };

        Assert.That(ComputePressureMath.NormaliseResource(sample), Is.EqualTo(0.9).Within(1e-9));
    }

    [Test]
    public void NormaliseResource_measures_memory_against_cgroup_limit_not_machine_total()
    {
        // A 2 GiB cgroup cap on a 32 GiB machine, 1.8 GiB in use. Measured against
        // the cgroup cap this is 90% pressure; against the machine total it would
        // be a misleading ~5%. The maximum-available figure the provider reports
        // already honours the cgroup, so the normalisation must use it.
        const long twoGiB = 2L * 1024 * 1024 * 1024;
        var sample = new SiloResourceSample
        {
            CpuUsagePercent = 5,
            MemoryUsedBytes = (long)(1.8 * 1024 * 1024 * 1024),
            MaximumAvailableMemoryBytes = twoGiB,
        };

        var pressure = ComputePressureMath.NormaliseResource(sample);

        Assert.That(pressure, Is.EqualTo(0.9).Within(0.01));
    }

    [Test]
    public void NormaliseResource_ignores_memory_when_no_ceiling_reported()
    {
        var sample = new SiloResourceSample
        {
            CpuUsagePercent = 30,
            MemoryUsedBytes = 5000,
            MaximumAvailableMemoryBytes = 0,
        };

        Assert.That(ComputePressureMath.NormaliseResource(sample), Is.EqualTo(0.3).Within(1e-9));
    }

    [Test]
    public void NormaliseResource_clamps_over_capacity_memory_to_one()
    {
        var sample = new SiloResourceSample
        {
            CpuUsagePercent = 0,
            MemoryUsedBytes = 150,
            MaximumAvailableMemoryBytes = 100,
        };

        Assert.That(ComputePressureMath.NormaliseResource(sample), Is.EqualTo(1d));
    }

    [Test]
    public void NormaliseActivation_is_ratio_against_target()
    {
        Assert.That(ComputePressureMath.NormaliseActivation(2500, 10_000), Is.EqualTo(0.25).Within(1e-9));
    }

    [Test]
    public void NormaliseActivation_clamps_over_target_to_one()
    {
        Assert.That(ComputePressureMath.NormaliseActivation(20_000, 10_000), Is.EqualTo(1d));
    }

    [Test]
    public void NormaliseActivation_zero_when_target_non_positive()
    {
        Assert.That(ComputePressureMath.NormaliseActivation(5000, 0), Is.Zero);
    }

    [Test]
    public void MapWalDispatch_healthy_is_zero()
    {
        Assert.That(ComputePressureMath.MapWalDispatch(WalSaturationState.Healthy), Is.Zero);
    }

    [Test]
    public void MapWalDispatch_throttled_is_half()
    {
        Assert.That(ComputePressureMath.MapWalDispatch(WalSaturationState.Throttled), Is.EqualTo(0.5));
    }

    [Test]
    public void MapWalDispatch_saturated_is_one()
    {
        Assert.That(ComputePressureMath.MapWalDispatch(WalSaturationState.Saturated), Is.EqualTo(1d));
    }
}
