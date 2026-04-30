using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Cities;

namespace VehicleFleetSimulator.Tests;

internal static class TestGraph
{
    /// <summary>A small connected graph used across tests.</summary>
    public static CityGraph BuildSimple()
    {
        var cities = new[]
        {
            new City("A", "Alpha"),
            new City("B", "Bravo"),
            new City("C", "Charlie"),
            new City("D", "Delta"),
            new City("E", "Echo"),
        };
        var edges = new[]
        {
            new RoadSegment("A", "B", 100),
            new RoadSegment("B", "C", 200),
            new RoadSegment("C", "D", 150),
            new RoadSegment("D", "E", 50),
            new RoadSegment("A", "C", 250),
            new RoadSegment("B", "D", 300),
        };
        return CityGraph.Build(cities, edges);
    }
}
