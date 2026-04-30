using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Vehicles;
using Xunit;

namespace VehicleFleetSimulator.Tests;

public class FuelModelTests
{
    private static readonly VehicleConfig Config = VehicleConfig.Default;

    [Fact]
    public void LitresPerKm_IsMinimisedAtOptimalSpeed()
    {
        var atOptimal = FuelModel.LitresPerKm(Config.OptimalSpeedKph, Config);
        var atLow = FuelModel.LitresPerKm(20, Config);
        var atHigh = FuelModel.LitresPerKm(140, Config);
        Assert.True(atOptimal < atLow);
        Assert.True(atOptimal < atHigh);
    }

    [Fact]
    public void LitresPerKm_IsSymmetricStyleUShape()
    {
        // Both extremes more expensive than optimal.
        var optimal = FuelModel.LitresPerKm(Config.OptimalSpeedKph, Config);
        for (double s = 1; s <= 140; s += 10)
        {
            var rate = FuelModel.LitresPerKm(s, Config);
            Assert.True(rate >= optimal - 1e-9, $"rate at {s} should be >= optimal");
        }
    }

    [Fact]
    public void FuelRequired_ScalesWithDistance()
    {
        var ten = FuelModel.FuelRequired(10, Config);
        var hundred = FuelModel.FuelRequired(100, Config);
        Assert.Equal(ten * 10, hundred, precision: 6);
    }
}
