using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the <c>orleans.lattice.scaling</c> meter: the stability of every
/// instrument-name constant, the flat-scalar projection
/// <see cref="ScalingGaugeSnapshot.FromSignal"/>, and that the registered
/// observable gauges report exactly the published snapshot's scalars.
/// </summary>
[TestFixture]
public sealed class LatticeScalingMetricsTests
{
    [Test]
    public void MeterName_is_the_canonical_scaling_meter()
    {
        Assert.That(LatticeScalingMetrics.MeterName, Is.EqualTo("orleans.lattice.scaling"));
        Assert.That(LatticeScalingMetrics.Meter.Name, Is.EqualTo("orleans.lattice.scaling"));
    }

    [Test]
    public void Instrument_name_constants_are_stable()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeScalingMetrics.ScaleValueName,
                Is.EqualTo("orleans.lattice.scaling.scale_value"));
            Assert.That(LatticeScalingMetrics.RawScaleValueName,
                Is.EqualTo("orleans.lattice.scaling.raw_scale_value"));
            Assert.That(LatticeScalingMetrics.ComputeActivationPressureName,
                Is.EqualTo("orleans.lattice.scaling.compute.activation_pressure"));
            Assert.That(LatticeScalingMetrics.ComputeResourcePressureName,
                Is.EqualTo("orleans.lattice.scaling.compute.resource_pressure"));
            Assert.That(LatticeScalingMetrics.ComputeWalDispatchPressureName,
                Is.EqualTo("orleans.lattice.scaling.compute.wal_dispatch_pressure"));
            Assert.That(LatticeScalingMetrics.ComputeReplicasName,
                Is.EqualTo("orleans.lattice.scaling.compute.replicas"));
            Assert.That(LatticeScalingMetrics.StorageAccountsOverThresholdName,
                Is.EqualTo("orleans.lattice.scaling.storage.accounts_over_threshold"));
            Assert.That(LatticeScalingMetrics.StorageRebalanceRecommendationsName,
                Is.EqualTo("orleans.lattice.scaling.storage.rebalance_recommendations"));
        });
    }

    [Test]
    public void Every_instrument_name_carries_the_meter_prefix()
    {
        foreach (var name in InstrumentNames())
        {
            Assert.That(name, Does.StartWith(LatticeScalingMetrics.MeterName + "."));
        }
    }

    [Test]
    public void FromSignal_projects_every_scalar_field()
    {
        var signal = new ScalingSignal
        {
            ScaleValue = 3.5d,
            RawScaleValue = 4.25d,
            RecommendedReplicas = 6,
            Compute = new ComputePressure
            {
                Activation = 0.4d,
                Resource = 0.7d,
                WalDispatch = 0.2d,
            },
            Storage = new StoragePressure
            {
                OverThreshold = true,
                Accounts = new[]
                {
                    new WalAccountPressure { ProviderKey = "a", OverThreshold = true },
                    new WalAccountPressure { ProviderKey = "b", OverThreshold = false },
                    new WalAccountPressure { ProviderKey = "c", OverThreshold = true },
                },
                Recommendation = new WalRebalanceRecommendation { Tree = "t", Partition = 0 },
            },
        };

        var snapshot = ScalingGaugeSnapshot.FromSignal(signal);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.ScaleValue, Is.EqualTo(3.5d));
            Assert.That(snapshot.RawScaleValue, Is.EqualTo(4.25d));
            Assert.That(snapshot.ActivationPressure, Is.EqualTo(0.4d));
            Assert.That(snapshot.ResourcePressure, Is.EqualTo(0.7d));
            Assert.That(snapshot.WalDispatchPressure, Is.EqualTo(0.2d));
            Assert.That(snapshot.RecommendedReplicas, Is.EqualTo(6L));
            Assert.That(snapshot.AccountsOverThreshold, Is.EqualTo(2L));
            Assert.That(snapshot.RebalanceRecommendations, Is.EqualTo(1L));
        });
    }

    [Test]
    public void FromSignal_reports_zero_counts_for_a_default_signal()
    {
        var snapshot = ScalingGaugeSnapshot.FromSignal(default);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.ScaleValue, Is.EqualTo(0d));
            Assert.That(snapshot.AccountsOverThreshold, Is.EqualTo(0L));
            Assert.That(snapshot.RebalanceRecommendations, Is.EqualTo(0L));
        });
    }

    [Test]
    public void FromSignal_reports_no_recommendation_when_none_present()
    {
        var signal = new ScalingSignal
        {
            Storage = new StoragePressure
            {
                Accounts = new[]
                {
                    new WalAccountPressure { ProviderKey = "a", OverThreshold = true },
                },
                Recommendation = null,
            },
        };

        var snapshot = ScalingGaugeSnapshot.FromSignal(signal);

        Assert.That(snapshot.AccountsOverThreshold, Is.EqualTo(1L));
        Assert.That(snapshot.RebalanceRecommendations, Is.EqualTo(0L));
    }

    [Test]
    public void Registered_gauges_report_the_published_snapshot()
    {
        var snapshot = new ScalingGaugeSnapshot
        {
            ScaleValue = 2.5d,
            RawScaleValue = 3.0d,
            ActivationPressure = 0.11d,
            ResourcePressure = 0.22d,
            WalDispatchPressure = 0.33d,
            RecommendedReplicas = 5L,
            AccountsOverThreshold = 2L,
            RebalanceRecommendations = 1L,
        };

        var observed = ObserveGauges(snapshot);

        Assert.Multiple(() =>
        {
            Assert.That(observed[LatticeScalingMetrics.ScaleValueName], Is.EqualTo(2.5d));
            Assert.That(observed[LatticeScalingMetrics.RawScaleValueName], Is.EqualTo(3.0d));
            Assert.That(observed[LatticeScalingMetrics.ComputeActivationPressureName], Is.EqualTo(0.11d));
            Assert.That(observed[LatticeScalingMetrics.ComputeResourcePressureName], Is.EqualTo(0.22d));
            Assert.That(observed[LatticeScalingMetrics.ComputeWalDispatchPressureName], Is.EqualTo(0.33d));
            Assert.That(observed[LatticeScalingMetrics.ComputeReplicasName], Is.EqualTo(5d));
            Assert.That(observed[LatticeScalingMetrics.StorageAccountsOverThresholdName], Is.EqualTo(2d));
            Assert.That(observed[LatticeScalingMetrics.StorageRebalanceRecommendationsName], Is.EqualTo(1d));
        });
    }

    [Test]
    public void Latest_reflects_the_most_recently_published_snapshot()
    {
        ScalingSignalGaugeRegistry.Publish(new ScalingGaugeSnapshot
        {
            ScaleValue = 9.5d,
            RecommendedReplicas = 8L,
        });

        var latest = ScalingSignalGaugeRegistry.Latest;

        Assert.That(latest.ScaleValue, Is.EqualTo(9.5d));
        Assert.That(latest.RecommendedReplicas, Is.EqualTo(8L));
    }

    private static IReadOnlyDictionary<string, double> ObserveGauges(ScalingGaugeSnapshot snapshot)
    {
        var values = new Dictionary<string, double>(StringComparer.Ordinal);

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, LatticeScalingMetrics.Meter))
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((instrument, value, _, _) => values[instrument.Name] = value);
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) => values[instrument.Name] = value);
        listener.Start();

        ScalingSignalGaugeRegistry.EnsureRegistered();
        ScalingSignalGaugeRegistry.Publish(snapshot);
        listener.RecordObservableInstruments();

        return values;
    }

    private static IEnumerable<string> InstrumentNames() => new[]
    {
        LatticeScalingMetrics.ScaleValueName,
        LatticeScalingMetrics.RawScaleValueName,
        LatticeScalingMetrics.ComputeActivationPressureName,
        LatticeScalingMetrics.ComputeResourcePressureName,
        LatticeScalingMetrics.ComputeWalDispatchPressureName,
        LatticeScalingMetrics.ComputeReplicasName,
        LatticeScalingMetrics.StorageAccountsOverThresholdName,
        LatticeScalingMetrics.StorageRebalanceRecommendationsName,
    };
}
