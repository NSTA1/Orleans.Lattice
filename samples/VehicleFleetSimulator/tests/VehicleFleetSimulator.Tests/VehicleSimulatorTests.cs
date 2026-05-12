using System.Collections.Immutable;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Cities;
using VehicleFleetSimulator.Grains.Vehicles;
using Xunit;

namespace VehicleFleetSimulator.Tests;

public class VehicleSimulatorTests
{
    private static (CityGraph graph, RouteGenerator routes, Random rng) BuildHarness(int seed = 0)
    {
        var graph = TestGraph.BuildSimple();
        var rng = new Random(seed);
        var routes = new RouteGenerator(graph, rng);
        return (graph, routes, rng);
    }

    private static VehicleState NewVehicle(CityGraph graph, RouteGenerator routes, ImmutableArray<string>? explicitRoute = null, double? fuel = null, VehicleConfig? config = null)
    {
        var spec = new VehicleSpec(
            VehicleId: Guid.NewGuid(),
            Route: explicitRoute,
            Config: config,
            InitialFuelLitres: fuel);
        return VehicleSimulator.CreateInitialState(spec, graph, routes, DateTimeOffset.UnixEpoch);
    }

    [Fact]
    public void CreateInitialState_GeneratesRouteWhenNoneSupplied()
    {
        var (graph, routes, _) = BuildHarness(1);
        var state = NewVehicle(graph, routes);

        Assert.True(state.Route.Length >= 2);
        Assert.True(graph.IsRouteValid(state.Route));
        Assert.Equal(0, state.RouteIndex);
        Assert.Equal(0, state.SegmentProgressKm);
        Assert.Equal(VehicleStatus.Driving, state.Status);
    }

    [Fact]
    public void CreateInitialState_LocationIsAtFirstCityOfRoute()
    {
        var (graph, routes, _) = BuildHarness(2);
        var state = NewVehicle(graph, routes);
        Assert.Equal(state.Route[0], state.CurrentFromCityId);
        Assert.Equal(state.Route[1], state.CurrentToCityId);
    }

    [Fact]
    public void CreateInitialState_RejectsInvalidExplicitRoute()
    {
        var (graph, routes, _) = BuildHarness();
        var bad = ImmutableArray.Create("A", "E"); // not adjacent in TestGraph
        Assert.Throws<ArgumentException>(() => NewVehicle(graph, routes, explicitRoute: bad));
    }

    [Fact]
    public void Tick_AdvancesPositionWhenDriving()
    {
        var (graph, routes, rng) = BuildHarness(3);
        var state = NewVehicle(graph, routes, ImmutableArray.Create("A", "B", "C"));
        var t0 = state.LastUpdatedUtc;

        var result = VehicleSimulator.Tick(state, VehicleConfig.Default, graph, routes,
            elapsed: TimeSpan.FromSeconds(1), nowUtc: t0 + TimeSpan.FromSeconds(1), random: rng);

        Assert.True(result.State.SegmentProgressKm > 0);
        Assert.True(result.State.SpeedKph > 0);
        // Departed event should be present (we left A at progress=0).
        Assert.Contains(result.Events, e => e is DepartedCity);
    }

    [Fact]
    public void Tick_TriggersRefuellingWhenInsufficientFuel()
    {
        var (graph, routes, rng) = BuildHarness(4);
        var state = NewVehicle(graph, routes,
            explicitRoute: ImmutableArray.Create("A", "B"),
            fuel: 0.01); // way below required for 100 km

        var result = VehicleSimulator.Tick(state, VehicleConfig.Default, graph, routes,
            elapsed: TimeSpan.FromSeconds(1), nowUtc: state.LastUpdatedUtc + TimeSpan.FromSeconds(1), random: rng);

        Assert.Equal(VehicleStatus.Refuelling, result.State.Status);
        Assert.Contains(result.Events, e => e is RefuellingStarted);
        Assert.NotNull(result.State.RefuellingUntilUtc);
    }

    [Fact]
    public void Tick_CompletesRefuellingAfterDelayAndRefillsTank()
    {
        var (graph, routes, rng) = BuildHarness(5);
        var config = VehicleConfig.Default with { RefuelDelay = TimeSpan.FromSeconds(2) };
        var state = NewVehicle(graph, routes,
            explicitRoute: ImmutableArray.Create("A", "B"),
            fuel: 0.01,
            config: config);

        var t1 = state.LastUpdatedUtc + TimeSpan.FromSeconds(1);
        var afterStart = VehicleSimulator.Tick(state, config, graph, routes, TimeSpan.FromSeconds(1), t1, rng);
        Assert.Equal(VehicleStatus.Refuelling, afterStart.State.Status);

        var t2 = afterStart.State.RefuellingUntilUtc!.Value + TimeSpan.FromSeconds(1);
        var afterDone = VehicleSimulator.Tick(afterStart.State, config, graph, routes, t2 - t1, t2, rng);

        Assert.Equal(VehicleStatus.Driving, afterDone.State.Status);
        // Refuel filled the tank; some negligible burn during the remainder of the tick is fine.
        Assert.InRange(afterDone.State.FuelLitres, config.FuelCapacityLitres - 1.0, config.FuelCapacityLitres);
        Assert.Contains(afterDone.Events, e => e is RefuellingCompleted);
    }

    [Fact]
    public void Tick_AdvancesRouteIndexOnArrivalAtIntermediateCity()
    {
        var (graph, routes, rng) = BuildHarness(6);
        var state = NewVehicle(graph, routes, ImmutableArray.Create("A", "B", "C"));
        // Force progress beyond segment length.
        state = state with { SegmentProgressKm = 99.0, SpeedKph = 100.0 };

        var result = VehicleSimulator.Tick(state, VehicleConfig.Default, graph, routes,
            elapsed: TimeSpan.FromHours(0.1), // > 10 km at 100 kph -> definitely past 100 km mark
            nowUtc: state.LastUpdatedUtc + TimeSpan.FromMinutes(6), random: rng);

        Assert.Equal(1, result.State.RouteIndex);
        Assert.Equal(0, result.State.SegmentProgressKm);
        Assert.Contains(result.Events, e => e is ArrivedCity { CityId: "B" });
    }

    [Fact]
    public void Tick_RegeneratesRouteOnArrivalAtFinalCity_AndKeepsDriving()
    {
        var (graph, routes, rng) = BuildHarness(7);
        var state = NewVehicle(graph, routes, ImmutableArray.Create("A", "B"));
        var originalRoute = state.Route;
        state = state with { SegmentProgressKm = 99.0, SpeedKph = 100.0 };

        var result = VehicleSimulator.Tick(state, VehicleConfig.Default, graph, routes,
            elapsed: TimeSpan.FromHours(0.1),
            nowUtc: state.LastUpdatedUtc + TimeSpan.FromMinutes(6), random: rng);

        // Still driving - no RouteCompleted status, just an event.
        Assert.Equal(VehicleStatus.Driving, result.State.Status);
        Assert.Equal(0, result.State.RouteIndex);
        Assert.Equal(0, result.State.SegmentProgressKm);
        Assert.True(graph.IsRouteValid(result.State.Route));
        Assert.NotEqual(originalRoute, result.State.Route);
        Assert.Equal("B", result.State.Route[0]); // new route starts where we arrived
        Assert.Contains(result.Events, e => e is ArrivedCity { CityId: "B" });
        Assert.Contains(result.Events, e => e is RouteCompleted);
    }

    [Fact]
    public void Tick_KeepsSpeedWithinConfiguredBounds()
    {
        var (graph, routes, rng) = BuildHarness(8);
        var state = NewVehicle(graph, routes, ImmutableArray.Create("A", "B", "C", "D"));

        for (int i = 0; i < 200; i++)
        {
            var nextNow = state.LastUpdatedUtc + TimeSpan.FromSeconds(1);
            state = VehicleSimulator.Tick(state, VehicleConfig.Default, graph, routes,
                TimeSpan.FromSeconds(1), nextNow, rng).State;
            if (state.Status == VehicleStatus.Driving && state.SpeedKph > 0)
            {
                Assert.InRange(state.SpeedKph, VehicleConfig.Default.MinSpeedKph, VehicleConfig.Default.MaxSpeedKph);
            }
        }
    }
}
