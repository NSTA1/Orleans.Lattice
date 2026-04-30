using System.Collections.Immutable;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains.Cities;

/// <summary>Configuration shape for binding from <c>appsettings.json</c>.</summary>
public sealed class CityGraphOptions
{
    public List<CityEntry> Cities { get; set; } = [];
    public List<EdgeEntry> Edges { get; set; } = [];

    public sealed class CityEntry
    {
        public string Id { get; set; } = "";
        public string Name { get; set; } = "";
    }

    public sealed class EdgeEntry
    {
        public string From { get; set; } = "";
        public string To { get; set; } = "";
        public double DistanceKm { get; set; }
    }

    public CityGraph BuildGraph()
    {
        var cities = Cities.Select(c => new City(c.Id, string.IsNullOrWhiteSpace(c.Name) ? c.Id : c.Name));
        var edges = Edges.Select(e => new RoadSegment(e.From, e.To, e.DistanceKm));
        return CityGraph.Build(cities, edges);
    }
}
