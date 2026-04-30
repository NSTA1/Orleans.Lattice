using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Vehicles;
using Xunit;

namespace VehicleFleetSimulator.Tests;

public class SpeedModelTests
{
    private static readonly VehicleConfig Config = VehicleConfig.Default;

    [Fact]
    public void IsNearCity_TrueWithinProximityOfStart()
    {
        // 10% of 100 km = 10 km approach window; 0.5 km is well inside it.
        Assert.True(SpeedModel.IsNearCity(segmentProgressKm: 0.5, segmentLengthKm: 100));
    }

    [Fact]
    public void IsNearCity_TrueWithinProximityOfEnd()
    {
        // 100 - 95 = 5 km from end, inside the 10 km window.
        Assert.True(SpeedModel.IsNearCity(segmentProgressKm: 95.0, segmentLengthKm: 100));
    }

    [Fact]
    public void IsNearCity_FalseInTheMiddle()
    {
        Assert.False(SpeedModel.IsNearCity(segmentProgressKm: 50, segmentLengthKm: 100));
    }

    [Fact]
    public void IsNearCity_ScalesWithSegmentLength()
    {
        // 5 km into a 20 km segment: window = 2 km, so 5 km from start is cruise.
        Assert.False(SpeedModel.IsNearCity(segmentProgressKm: 5, segmentLengthKm: 20));
        // 5 km into a 200 km segment: window = 20 km, so 5 km from start is near-city.
        Assert.True(SpeedModel.IsNearCity(segmentProgressKm: 5, segmentLengthKm: 200));
    }

    [Fact]
    public void SampleTarget_NearCity_StaysIn1To40Range()
    {
        var rng = new Random(1);
        for (int i = 0; i < 500; i++)
        {
            var s = SpeedModel.SampleTarget(segmentProgressKm: 1.0, segmentLengthKm: 100, Config, rng);
            Assert.InRange(s, 1.0, 40.0);
        }
    }

    [Fact]
    public void SampleTarget_Cruise_StaysIn50To140Range()
    {
        var rng = new Random(1);
        for (int i = 0; i < 500; i++)
        {
            var s = SpeedModel.SampleTarget(segmentProgressKm: 50, segmentLengthKm: 100, Config, rng);
            Assert.InRange(s, 50.0, 140.0);
        }
    }

    [Fact]
    public void Smooth_MovesPartiallyTowardTarget()
    {
        var smoothed = SpeedModel.Smooth(previousSpeed: 100, targetSpeed: 20, alpha: 0.2);
        // Move 20% of the way from 100 toward 20: 100 + 0.2 * (20 - 100) = 84
        Assert.Equal(84.0, smoothed, precision: 6);
    }

    [Fact]
    public void Smooth_NoStepWhenAlphaIsZero()
    {
        Assert.Equal(100.0, SpeedModel.Smooth(100, 20, 0), precision: 6);
    }

    [Fact]
    public void Smooth_FullJumpWhenAlphaIsOne()
    {
        Assert.Equal(20.0, SpeedModel.Smooth(100, 20, 1), precision: 6);
    }
}
