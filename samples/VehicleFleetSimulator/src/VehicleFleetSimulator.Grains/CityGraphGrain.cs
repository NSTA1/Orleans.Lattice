using System.Collections.Immutable;
using Orleans.Runtime;
using VehicleFleetSimulator.Abstractions;
using VehicleFleetSimulator.Grains.Cities;

namespace VehicleFleetSimulator.Grains;

/// <summary>Stateless grain that exposes the silo-loaded <see cref="CityGraph"/> as a serialisable snapshot.</summary>
public sealed class CityGraphGrain : Grain, ICityGraphGrain
{
    private readonly ICityGraphProvider _provider;
    private readonly IPersistentState<CityGraphPersistentState> _persistent;

    public CityGraphGrain(
        ICityGraphProvider provider,
        [PersistentState("city-positions", "Default")] IPersistentState<CityGraphPersistentState> persistent)
    {
        _provider = provider;
        _persistent = persistent;
    }

    public Task<CityGraphSnapshot> GetGraph()
    {
        var graph = _provider.Graph;
        var cities = graph.Cities.ToImmutableArray();

        var seen = new HashSet<(string A, string B)>();
        var edges = ImmutableArray.CreateBuilder<RoadSegment>();
        foreach (var city in cities)
        {
            foreach (var neighbor in graph.GetNeighbors(city.Id))
            {
                var (a, b) = string.CompareOrdinal(city.Id, neighbor) <= 0
                    ? (city.Id, neighbor)
                    : (neighbor, city.Id);
                if (!seen.Add((a, b))) continue;
                if (graph.TryGetDistance(a, b, out var km))
                    edges.Add(new RoadSegment(a, b, km));
            }
        }

        // Drop any orphaned overrides (city no longer in the graph) on the way out so the client
        // never sees a position keyed off an id it can't render. We don't rewrite persistent state
        // here; that's a maintenance concern handled by ClearCityPositions and the silo's load
        // path, and read-only is the safest default for a getter.
        ImmutableDictionary<string, CityPosition>? overrides = null;
        if (_persistent.State.Positions.Count > 0)
        {
            var b = ImmutableDictionary.CreateBuilder<string, CityPosition>(StringComparer.Ordinal);
            foreach (var kv in _persistent.State.Positions)
                if (graph.ContainsCity(kv.Key))
                    b.Add(kv.Key, kv.Value);
            overrides = b.Count > 0 ? b.ToImmutable() : null;
        }

        return Task.FromResult(new CityGraphSnapshot(cities, edges.ToImmutable(), overrides));
    }

    public async Task<bool> SetCityPosition(string cityId, double x, double y)
    {
        if (string.IsNullOrEmpty(cityId)) return false;
        if (!_provider.Graph.ContainsCity(cityId)) return false;
        if (!double.IsFinite(x) || !double.IsFinite(y)) return false;

        _persistent.State.Positions[cityId] = new CityPosition(x, y);
        await _persistent.WriteStateAsync();
        return true;
    }

    public async Task ClearCityPositions()
    {
        if (_persistent.State.Positions.Count == 0) return;
        _persistent.State.Positions.Clear();
        await _persistent.WriteStateAsync();
    }
}
