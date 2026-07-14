using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Pins the Orleans-serializer wire-shape contract for every
/// <c>Orleans.Lattice.Scaling</c> DTO alias
/// (<see cref="ComputePressure"/>, <see cref="StoragePressure"/>,
/// <see cref="WalAccountPressure"/>, <see cref="WalRebalanceRecommendation"/>,
/// <see cref="WalPressureClassification"/>, and <see cref="ScalingSignal"/>):
/// every slot must round-trip verbatim and a
/// default-constructed value must decode cleanly.
/// </summary>
[TestFixture]
public sealed class ScalingSignalRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void ComputePressure_round_trips_every_slot()
    {
        var value = new ComputePressure
        {
            Activation = 0.25,
            Resource = 0.5,
            WalDispatch = 0.75,
            WalSaturation = WalSaturationState.Saturated,
        };

        var decoded = RoundTrip(value);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Activation, Is.EqualTo(0.25));
            Assert.That(decoded.Resource, Is.EqualTo(0.5));
            Assert.That(decoded.WalDispatch, Is.EqualTo(0.75));
            Assert.That(decoded.WalSaturation, Is.EqualTo(WalSaturationState.Saturated));
            Assert.That(decoded, Is.EqualTo(value));
        });
    }

    [Test]
    public void ComputePressure_default_decodes_to_zeroed_values()
    {
        var decoded = RoundTrip(default(ComputePressure));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Activation, Is.Zero);
            Assert.That(decoded.Resource, Is.Zero);
            Assert.That(decoded.WalDispatch, Is.Zero);
            Assert.That(decoded.WalSaturation, Is.EqualTo(WalSaturationState.Healthy));
        });
    }

    [Test]
    public void WalAccountPressure_round_trips_every_slot()
    {
        var value = new WalAccountPressure
        {
            ProviderKey = "acct-a",
            WalRetainedBytes = 4096,
            Saturation = WalSaturationState.Throttled,
            Classification = WalPressureClassification.ThroughputBound,
            OverThreshold = true,
        };

        var decoded = RoundTrip(value);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ProviderKey, Is.EqualTo("acct-a"));
            Assert.That(decoded.WalRetainedBytes, Is.EqualTo(4096));
            Assert.That(decoded.Saturation, Is.EqualTo(WalSaturationState.Throttled));
            Assert.That(decoded.Classification, Is.EqualTo(WalPressureClassification.ThroughputBound));
            Assert.That(decoded.OverThreshold, Is.True);
            Assert.That(decoded, Is.EqualTo(value));
        });
    }

    [Test]
    public void WalRebalanceRecommendation_round_trips_every_slot()
    {
        var value = new WalRebalanceRecommendation
        {
            Tree = "tree-a",
            Partition = 3,
            CurrentProviderKey = "acct-a",
            TargetProviderKey = "acct-b",
            Rationale = "acct-a over threshold",
            HasHeadroom = true,
            Classification = WalPressureClassification.CapacityBound,
        };

        var decoded = RoundTrip(value);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Tree, Is.EqualTo("tree-a"));
            Assert.That(decoded.Partition, Is.EqualTo(3));
            Assert.That(decoded.CurrentProviderKey, Is.EqualTo("acct-a"));
            Assert.That(decoded.TargetProviderKey, Is.EqualTo("acct-b"));
            Assert.That(decoded.Rationale, Is.EqualTo("acct-a over threshold"));
            Assert.That(decoded.HasHeadroom, Is.True);
            Assert.That(decoded.Classification, Is.EqualTo(WalPressureClassification.CapacityBound));
            Assert.That(decoded, Is.EqualTo(value));
        });
    }

    [Test]
    public void WalPressureClassification_round_trips_each_member()
    {
        foreach (var member in Enum.GetValues<WalPressureClassification>())
        {
            Assert.That(RoundTrip(member), Is.EqualTo(member));
        }
    }

    [Test]
    public void StoragePressure_round_trips_every_slot()
    {
        var value = new StoragePressure
        {
            OverThreshold = true,
            WalRetainedBytes = 8192,
            Accounts = new[]
            {
                new WalAccountPressure { ProviderKey = "acct-a", WalRetainedBytes = 8192, Saturation = WalSaturationState.Saturated },
            },
            Recommendation = new WalRebalanceRecommendation
            {
                Tree = "tree-a",
                Partition = 1,
                CurrentProviderKey = "acct-a",
                TargetProviderKey = "acct-b",
                Rationale = "relieve acct-a",
            },
        };

        var decoded = RoundTrip(value);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.OverThreshold, Is.True);
            Assert.That(decoded.WalRetainedBytes, Is.EqualTo(8192));
            Assert.That(decoded.Accounts, Has.Count.EqualTo(1));
            Assert.That(decoded.Accounts[0].ProviderKey, Is.EqualTo("acct-a"));
            Assert.That(decoded.Recommendation, Is.Not.Null);
            Assert.That(decoded.Recommendation!.Value.TargetProviderKey, Is.EqualTo("acct-b"));
        });
    }

    [Test]
    public void StoragePressure_default_decodes_to_empty_non_null_accounts()
    {
        var decoded = RoundTrip(default(StoragePressure));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.OverThreshold, Is.False);
            Assert.That(decoded.WalRetainedBytes, Is.Zero);
            Assert.That(decoded.Accounts, Is.Not.Null);
            Assert.That(decoded.Accounts, Is.Empty);
            Assert.That(decoded.Recommendation, Is.Null);
        });
    }

    [Test]
    public void ScalingSignal_round_trips_every_slot()
    {
        var sampledAt = new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);
        var value = new ScalingSignal
        {
            ScaleValue = 2.5,
            RecommendedReplicas = 4,
            Compute = new ComputePressure { Activation = 0.9, Resource = 0.8, WalDispatch = 0.7, WalSaturation = WalSaturationState.Throttled },
            Storage = new StoragePressure { OverThreshold = true, WalRetainedBytes = 100 },
            Reason = "compute axis dominated",
            SampledAt = sampledAt,
            RawScaleValue = 3.2,
        };

        var decoded = RoundTrip(value);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ScaleValue, Is.EqualTo(2.5));
            Assert.That(decoded.RecommendedReplicas, Is.EqualTo(4));
            Assert.That(decoded.Compute.Activation, Is.EqualTo(0.9));
            Assert.That(decoded.Compute.WalSaturation, Is.EqualTo(WalSaturationState.Throttled));
            Assert.That(decoded.Storage.OverThreshold, Is.True);
            Assert.That(decoded.Storage.WalRetainedBytes, Is.EqualTo(100));
            Assert.That(decoded.Reason, Is.EqualTo("compute axis dominated"));
            Assert.That(decoded.SampledAt, Is.EqualTo(sampledAt));
            Assert.That(decoded.RawScaleValue, Is.EqualTo(3.2));
        });
    }

    [Test]
    public void ScalingSignal_default_decodes_to_zeroed_values()
    {
        var decoded = RoundTrip(default(ScalingSignal));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ScaleValue, Is.Zero);
            Assert.That(decoded.RecommendedReplicas, Is.Zero);
            Assert.That(decoded.Reason, Is.Null);
            Assert.That(decoded.Compute.WalSaturation, Is.EqualTo(WalSaturationState.Healthy));
            Assert.That(decoded.Storage.Accounts, Is.Empty);
        });
    }
}
