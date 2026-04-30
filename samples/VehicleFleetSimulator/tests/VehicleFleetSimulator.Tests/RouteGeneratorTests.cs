using VehicleFleetSimulator.Grains.Cities;
using Xunit;

namespace VehicleFleetSimulator.Tests;

public class RouteGeneratorTests
{
    [Fact]
    public void Generate_RespectsLengthBounds()
    {
        var graph = TestGraph.BuildSimple();
        var rng = new Random(42);
        var gen = new RouteGenerator(graph, rng);

        for (int i = 0; i < 50; i++)
        {
            var route = gen.Generate(startCityId: "A", minLength: 3, maxLength: 7);
            Assert.InRange(route.Length, 3, 7);
        }
    }

    [Fact]
    public void Generate_CapsAtTen()
    {
        var graph = TestGraph.BuildSimple();
        var gen = new RouteGenerator(graph, new Random(1));
        for (int i = 0; i < 30; i++)
        {
            var route = gen.Generate(startCityId: "A", minLength: 2, maxLength: 50);
            Assert.True(route.Length <= RouteGenerator.MaxRouteLengthCap);
        }
    }

    [Fact]
    public void Generate_ProducesTopologicallyValidRoute()
    {
        var graph = TestGraph.BuildSimple();
        var gen = new RouteGenerator(graph, new Random(7));

        for (int i = 0; i < 100; i++)
        {
            var route = gen.Generate(startCityId: "A");
            Assert.True(graph.IsRouteValid(route));
        }
    }

    [Fact]
    public void Generate_AvoidsImmediateBackAndForth()
    {
        var graph = TestGraph.BuildSimple();
        var gen = new RouteGenerator(graph, new Random(99));

        for (int i = 0; i < 200; i++)
        {
            var route = gen.Generate(startCityId: "A", minLength: 4, maxLength: 10);
            for (int j = 2; j < route.Length; j++)
            {
                Assert.NotEqual(route[j - 2], route[j]);
            }
        }
    }

    [Fact]
    public void Generate_NoConsecutiveDuplicates()
    {
        var graph = TestGraph.BuildSimple();
        var gen = new RouteGenerator(graph, new Random(123));

        var route = gen.Generate(startCityId: "A", minLength: 5, maxLength: 10);
        for (int j = 1; j < route.Length; j++)
        {
            Assert.NotEqual(route[j - 1], route[j]);
        }
    }

    [Fact]
    public void Generate_StartsAtRequestedCity()
    {
        var graph = TestGraph.BuildSimple();
        var gen = new RouteGenerator(graph, new Random(5));
        var route = gen.Generate(startCityId: "C");
        Assert.Equal("C", route[0]);
    }

    [Fact]
    public void Generate_IsDeterministicWithSeededRandom()
    {
        var graph = TestGraph.BuildSimple();
        var first = new RouteGenerator(graph, new Random(2026)).Generate(startCityId: "A", minLength: 5, maxLength: 5);
        var second = new RouteGenerator(graph, new Random(2026)).Generate(startCityId: "A", minLength: 5, maxLength: 5);
        Assert.Equal(first.AsEnumerable(), second.AsEnumerable());
    }

    [Fact]
    public void Generate_RejectsUnknownStartCity()
    {
        var graph = TestGraph.BuildSimple();
        var gen = new RouteGenerator(graph, new Random(0));
        Assert.Throws<ArgumentException>(() => gen.Generate(startCityId: "ZZZ"));
    }
}
