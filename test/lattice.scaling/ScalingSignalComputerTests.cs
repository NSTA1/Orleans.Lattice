using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="ScalingSignalComputer"/>: the replica-demand scalar
/// (dominant dimension times replica count), max-dimension selection and the
/// dominant-dimension reason, asymmetric smoothing (immediate scale-out,
/// EWMA-damped and window-gated scale-in), the scale-in suppressions (window not
/// elapsed, shard split in flight, non-healthy WAL), and the replica floor.
/// Deterministic: every tick is fed an explicit timestamp.
/// </summary>
[TestFixture]
public sealed class ScalingSignalComputerTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static IOptions<LatticeScalingSignalOptions> Options(Action<LatticeScalingSignalOptions>? configure = null)
    {
        var options = new LatticeScalingSignalOptions
        {
            EwmaHalfLife = TimeSpan.FromSeconds(10),
            ScaleInGateWindow = TimeSpan.FromSeconds(120),
        };
        configure?.Invoke(options);
        return Microsoft.Extensions.Options.Options.Create(options);
    }

    private static ComputePressure Compute(
        double activation = 0,
        double resource = 0,
        double walDispatch = 0,
        WalSaturationState wal = WalSaturationState.Healthy)
        => new()
        {
            Activation = activation,
            Resource = resource,
            WalDispatch = walDispatch,
            WalSaturation = wal,
        };

    [Test]
    public void Scalar_is_dominant_dimension_times_replica_count()
    {
        var computer = new ScalingSignalComputer(Options());

        var signal = computer.Compute(Compute(activation: 0.5), default, replicaCount: 4, splitInFlight: false, T0);

        Assert.Multiple(() =>
        {
            Assert.That(signal.RawScaleValue, Is.EqualTo(2.0).Within(1e-9));
            Assert.That(signal.ScaleValue, Is.EqualTo(2.0).Within(1e-9));
            Assert.That(signal.RecommendedReplicas, Is.EqualTo(2));
        });
    }

    [Test]
    public void Recommended_replicas_ceils_a_fractional_scalar()
    {
        var computer = new ScalingSignalComputer(Options());

        var signal = computer.Compute(Compute(activation: 0.6), default, replicaCount: 3, splitInFlight: false, T0);

        Assert.Multiple(() =>
        {
            Assert.That(signal.RawScaleValue, Is.EqualTo(1.8).Within(1e-9));
            Assert.That(signal.RecommendedReplicas, Is.EqualTo(2));
        });
    }

    [Test]
    public void Dominant_dimension_is_the_maximum_and_names_the_reason()
    {
        var computer = new ScalingSignalComputer(Options());

        var signal = computer.Compute(
            Compute(activation: 0.3, resource: 0.9, walDispatch: 0.2),
            default, replicaCount: 2, splitInFlight: false, T0);

        Assert.Multiple(() =>
        {
            Assert.That(signal.RawScaleValue, Is.EqualTo(1.8).Within(1e-9));
            Assert.That(signal.Reason, Does.Contain(ScalingSignalComputer.ResourceDimension));
            Assert.That(signal.Reason, Does.Contain("0.90"));
        });
    }

    [Test]
    public void Wal_dispatch_can_be_the_dominant_dimension()
    {
        var computer = new ScalingSignalComputer(Options());

        var signal = computer.Compute(
            Compute(activation: 0.1, resource: 0.2, walDispatch: 1.0, wal: WalSaturationState.Saturated),
            default, replicaCount: 3, splitInFlight: false, T0);

        Assert.Multiple(() =>
        {
            Assert.That(signal.Reason, Does.Contain(ScalingSignalComputer.WalDispatchDimension));
            Assert.That(signal.RawScaleValue, Is.EqualTo(3.0).Within(1e-9));
        });
    }

    [Test]
    public void Scale_out_reacts_immediately_without_ewma_lag()
    {
        var computer = new ScalingSignalComputer(Options());
        computer.Compute(Compute(activation: 0.5), default, 4, false, T0); // held = 2.0

        var signal = computer.Compute(Compute(activation: 1.0), default, 4, false, T0.AddSeconds(5));

        Assert.That(signal.ScaleValue, Is.EqualTo(4.0).Within(1e-9));
    }

    [Test]
    public void Scale_in_is_held_until_the_gate_window_elapses()
    {
        var computer = new ScalingSignalComputer(Options());
        computer.Compute(Compute(activation: 1.0), default, 4, false, T0); // held = 4.0

        // Demand collapses immediately, but the window has not elapsed.
        var early = computer.Compute(Compute(), default, 4, false, T0.AddSeconds(5));

        Assert.Multiple(() =>
        {
            Assert.That(early.RawScaleValue, Is.Zero);
            Assert.That(early.ScaleValue, Is.EqualTo(4.0).Within(1e-9));
            Assert.That(early.Reason, Does.Contain("scale-in held"));
        });
    }

    [Test]
    public void Scale_in_descends_once_eligibility_persists_through_the_window()
    {
        var computer = new ScalingSignalComputer(Options());
        computer.Compute(Compute(activation: 1.0), default, 4, false, T0); // held = 4.0
        computer.Compute(Compute(), default, 4, false, T0.AddSeconds(5));  // held (gated)

        // Continuously eligible since T0+5; at T0+130 the 120s window has elapsed.
        var relaxed = computer.Compute(Compute(), default, 4, false, T0.AddSeconds(130));

        Assert.That(relaxed.ScaleValue, Is.LessThan(0.1));
    }

    [Test]
    public void A_shard_split_in_flight_suppresses_scale_in()
    {
        var computer = new ScalingSignalComputer(Options());
        computer.Compute(Compute(activation: 1.0), default, 4, false, T0); // held = 4.0

        // Well past the window, all dims low, WAL healthy - but a split is in flight.
        var signal = computer.Compute(Compute(), default, 4, splitInFlight: true, T0.AddSeconds(300));

        Assert.That(signal.ScaleValue, Is.EqualTo(4.0).Within(1e-9));
    }

    [Test]
    public void A_non_healthy_wal_suppresses_scale_in()
    {
        var computer = new ScalingSignalComputer(Options());
        computer.Compute(Compute(activation: 1.0), default, 4, false, T0); // held = 4.0

        // Dimensions numerically low but WAL is Throttled - scale-in must stay gated.
        var signal = computer.Compute(
            Compute(wal: WalSaturationState.Throttled),
            default, 4, splitInFlight: false, T0.AddSeconds(300));

        Assert.That(signal.ScaleValue, Is.EqualTo(4.0).Within(1e-9));
    }

    [Test]
    public void Replica_floor_is_respected_for_scale_value_and_recommendation()
    {
        var computer = new ScalingSignalComputer(Options(o => o.MinReplicas = 3));

        var signal = computer.Compute(Compute(), default, replicaCount: 1, splitInFlight: false, T0);

        Assert.Multiple(() =>
        {
            Assert.That(signal.ScaleValue, Is.EqualTo(3d).Within(1e-9));
            Assert.That(signal.RecommendedReplicas, Is.EqualTo(3));
        });
    }

    [Test]
    public void Storage_axis_is_carried_through_without_inflating_the_scalar()
    {
        var computer = new ScalingSignalComputer(Options());
        var storage = new StoragePressure { OverThreshold = true, WalRetainedBytes = 1_000_000 };

        var signal = computer.Compute(Compute(activation: 0.5), storage, replicaCount: 2, splitInFlight: false, T0);

        Assert.Multiple(() =>
        {
            Assert.That(signal.Storage.OverThreshold, Is.True);
            Assert.That(signal.RawScaleValue, Is.EqualTo(1.0).Within(1e-9));
        });
    }

    [Test]
    public void Replica_count_is_floored_at_one()
    {
        var computer = new ScalingSignalComputer(Options());

        var signal = computer.Compute(Compute(activation: 0.5), default, replicaCount: 0, splitInFlight: false, T0);

        Assert.That(signal.RawScaleValue, Is.EqualTo(0.5).Within(1e-9));
    }
}
