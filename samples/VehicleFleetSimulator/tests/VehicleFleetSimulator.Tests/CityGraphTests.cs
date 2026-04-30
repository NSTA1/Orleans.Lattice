using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Cities;
using Xunit;

namespace VehicleFleetSimulator.Tests;

public class CityGraphTests
{
    [Fact]
    public void Build_StoresCitiesAndBidirectionalEdges()
    {
        var graph = TestGraph.BuildSimple();

        Assert.True(graph.ContainsCity("A"));
        Assert.True(graph.TryGetDistance("A", "B", out var ab));
        Assert.True(graph.TryGetDistance("B", "A", out var ba));
        Assert.Equal(100, ab);
        Assert.Equal(100, ba);
    }

    [Fact]
    public void Build_RejectsDuplicateCityId()
    {
        var cities = new[] { new City("A", "Alpha"), new City("A", "Again") };
        Assert.Throws<InvalidOperationException>(() =>
            CityGraph.Build(cities, []));
    }

    [Fact]
    public void Build_RejectsEdgeReferencingUnknownCity()
    {
        var cities = new[] { new City("A", "Alpha") };
        var edges = new[] { new RoadSegment("A", "B", 10) };
        Assert.Throws<InvalidOperationException>(() => CityGraph.Build(cities, edges));
    }

    [Fact]
    public void Build_RejectsSelfLoop()
    {
        var cities = new[] { new City("A", "Alpha"), new City("B", "Bravo") };
        var edges = new[] { new RoadSegment("A", "A", 10) };
        Assert.Throws<InvalidOperationException>(() => CityGraph.Build(cities, edges));
    }

    [Fact]
    public void Build_RejectsNonPositiveDistance()
    {
        var cities = new[] { new City("A", "Alpha"), new City("B", "Bravo") };
        var edges = new[] { new RoadSegment("A", "B", 0) };
        Assert.Throws<InvalidOperationException>(() => CityGraph.Build(cities, edges));
    }

    [Fact]
    public void IsRouteValid_AcceptsConnectedRoutes()
    {
        var g = TestGraph.BuildSimple();
        Assert.True(g.IsRouteValid(["A", "B", "C", "D"]));
    }

    [Fact]
    public void IsRouteValid_RejectsConsecutiveDuplicates()
    {
        var g = TestGraph.BuildSimple();
        Assert.False(g.IsRouteValid(["A", "A", "B"]));
    }

    [Fact]
    public void IsRouteValid_RejectsDisconnectedHop()
    {
        var g = TestGraph.BuildSimple();
        Assert.False(g.IsRouteValid(["A", "E"]));
    }

    [Fact]
    public void IsRouteValid_RejectsTooShortRoute()
    {
        var g = TestGraph.BuildSimple();
        Assert.False(g.IsRouteValid(["A"]));
    }

    [Fact]
    public void GetUnreachableCities_FindsIslands()
    {
        var cities = new[] { new City("A", "A"), new City("B", "B"), new City("Z", "Z") };
        var edges = new[] { new RoadSegment("A", "B", 10) };
        var g = CityGraph.Build(cities, edges);

        var unreachable = g.GetUnreachableCities("A");
        Assert.Contains("Z", unreachable);
        Assert.DoesNotContain("B", unreachable);
    }
}
