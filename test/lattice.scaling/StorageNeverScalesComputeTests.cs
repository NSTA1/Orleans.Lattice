using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// The storage-axis invariant (#1187): storage pressure is carried through the
/// <see cref="ScalingSignalComputer"/> for observability but must NEVER change the
/// compute scale value. These tests hold the compute pressure fixed and vary the
/// <see cref="StoragePressure"/> across the full spectrum (zeroed, over-threshold,
/// throughput-bound account with a recommendation, capacity-bound account),
/// asserting <see cref="ScalingSignal.ScaleValue"/> and
/// <see cref="ScalingSignal.RawScaleValue"/> are byte-identical regardless.
/// </summary>
[TestFixture]
public sealed class StorageNeverScalesComputeTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static IOptions<LatticeScalingSignalOptions> Options()
        => Microsoft.Extensions.Options.Options.Create(new LatticeScalingSignalOptions
        {
            EwmaHalfLife = TimeSpan.FromSeconds(10),
            ScaleInGateWindow = TimeSpan.FromSeconds(120),
        });

    private static ComputePressure FixedCompute() => new()
    {
        Activation = 0.6,
        Resource = 0.3,
        WalDispatch = 0.2,
        WalSaturation = WalSaturationState.Healthy,
    };

    private static IEnumerable<StoragePressure> StorageSpectrum()
    {
        yield return default;
        yield return new StoragePressure { OverThreshold = false, WalRetainedBytes = 123 };
        yield return new StoragePressure { OverThreshold = true, WalRetainedBytes = long.MaxValue };
        yield return new StoragePressure
        {
            OverThreshold = true,
            WalRetainedBytes = 9_000,
            Accounts = new[]
            {
                new WalAccountPressure
                {
                    ProviderKey = "acct-a",
                    WalRetainedBytes = 9_000,
                    Saturation = WalSaturationState.Saturated,
                    Classification = WalPressureClassification.ThroughputBound,
                    OverThreshold = true,
                },
            },
            Recommendation = new WalRebalanceRecommendation
            {
                Tree = "hot",
                Partition = 1,
                CurrentProviderKey = "acct-a",
                TargetProviderKey = "acct-b",
                HasHeadroom = true,
                Classification = WalPressureClassification.ThroughputBound,
                Rationale = "move it",
            },
        };
    }

    [Test]
    public void Storage_pressure_never_changes_the_scale_value()
    {
        var compute = FixedCompute();

        // Baseline: what a zeroed storage axis produces for the fixed compute.
        var baseline = new ScalingSignalComputer(Options())
            .Compute(compute, default, replicaCount: 5, splitInFlight: false, T0);

        foreach (var storage in StorageSpectrum())
        {
            // Fresh computer per case so EWMA/window state cannot leak between cases
            // and confound the comparison - the only varying input is `storage`.
            var signal = new ScalingSignalComputer(Options())
                .Compute(compute, storage, replicaCount: 5, splitInFlight: false, T0);

            Assert.Multiple(() =>
            {
                Assert.That(signal.RawScaleValue, Is.EqualTo(baseline.RawScaleValue),
                    "storage axis must not change RawScaleValue");
                Assert.That(signal.ScaleValue, Is.EqualTo(baseline.ScaleValue),
                    "storage axis must not change ScaleValue");
                Assert.That(signal.RecommendedReplicas, Is.EqualTo(baseline.RecommendedReplicas),
                    "storage axis must not change RecommendedReplicas");
                // The storage snapshot is still carried through verbatim.
                Assert.That(signal.Storage, Is.EqualTo(storage));
            });
        }
    }

    [Test]
    public void Storage_pressure_is_carried_through_unmodified()
    {
        var storage = new StoragePressure
        {
            OverThreshold = true,
            WalRetainedBytes = 4_242,
            Accounts = new[]
            {
                new WalAccountPressure { ProviderKey = "acct-a", WalRetainedBytes = 4_242, Classification = WalPressureClassification.CapacityBound, OverThreshold = true },
            },
        };

        var signal = new ScalingSignalComputer(Options())
            .Compute(FixedCompute(), storage, replicaCount: 3, splitInFlight: false, T0);

        Assert.Multiple(() =>
        {
            Assert.That(signal.Storage.OverThreshold, Is.True);
            Assert.That(signal.Storage.WalRetainedBytes, Is.EqualTo(4_242));
            Assert.That(signal.Storage.Accounts, Has.Count.EqualTo(1));
            Assert.That(signal.Storage.Accounts[0].Classification, Is.EqualTo(WalPressureClassification.CapacityBound));
        });
    }
}
