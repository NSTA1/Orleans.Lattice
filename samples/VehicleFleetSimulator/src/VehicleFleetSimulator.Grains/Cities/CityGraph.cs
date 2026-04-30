using System.Collections.Immutable;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Grains.Cities;

/// <summary>
/// Immutable graph of cities and inter-city distances. Edges are stored bidirectionally;
/// supplying a directed edge A→B in <see cref="Build"/> implicitly enables travel both ways.
/// </summary>
public sealed class CityGraph
{
    private readonly ImmutableDictionary<string, City> _cities;
    private readonly ImmutableDictionary<string, ImmutableDictionary<string, double>> _adjacency;

    private CityGraph(
        ImmutableDictionary<string, City> cities,
        ImmutableDictionary<string, ImmutableDictionary<string, double>> adjacency)
    {
        _cities = cities;
        _adjacency = adjacency;
    }

    public IReadOnlyCollection<City> Cities => _cities.Values.ToArray();

    public bool ContainsCity(string cityId) => _cities.ContainsKey(cityId);

    public City GetCity(string cityId) => _cities[cityId];

    public IReadOnlyCollection<string> GetNeighbors(string cityId) =>
        _adjacency.TryGetValue(cityId, out var n)
            ? n.Keys.ToArray()
            : Array.Empty<string>();

    public bool TryGetDistance(string fromCityId, string toCityId, out double distanceKm)
    {
        if (_adjacency.TryGetValue(fromCityId, out var n) && n.TryGetValue(toCityId, out distanceKm))
            return true;
        distanceKm = 0;
        return false;
    }

    public bool IsRouteValid(IReadOnlyList<string> route)
    {
        if (route is null || route.Count < 2) return false;
        for (int i = 0; i < route.Count; i++)
        {
            if (!ContainsCity(route[i])) return false;
            if (i > 0)
            {
                if (route[i] == route[i - 1]) return false;
                if (!TryGetDistance(route[i - 1], route[i], out _)) return false;
            }
        }
        return true;
    }

    /// <summary>Returns the set of city ids not reachable from <paramref name="fromCityId"/>.</summary>
    public IReadOnlyCollection<string> GetUnreachableCities(string fromCityId)
    {
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        var stack = new Stack<string>();
        stack.Push(fromCityId);
        while (stack.Count > 0)
        {
            var c = stack.Pop();
            if (!reachable.Add(c)) continue;
            foreach (var n in GetNeighbors(c))
                if (!reachable.Contains(n)) stack.Push(n);
        }
        return _cities.Keys.Where(k => !reachable.Contains(k)).ToArray();
    }

    public static CityGraph Build(IEnumerable<City> cities, IEnumerable<RoadSegment> segments)
    {
        ArgumentNullException.ThrowIfNull(cities);
        ArgumentNullException.ThrowIfNull(segments);

        var cityBuilder = ImmutableDictionary.CreateBuilder<string, City>(StringComparer.Ordinal);
        foreach (var city in cities)
        {
            if (cityBuilder.ContainsKey(city.Id))
                throw new InvalidOperationException($"Duplicate city id '{city.Id}'.");
            cityBuilder.Add(city.Id, city);
        }

        var adj = new Dictionary<string, Dictionary<string, double>>(StringComparer.Ordinal);
        foreach (var seg in segments)
        {
            if (!cityBuilder.ContainsKey(seg.FromCityId))
                throw new InvalidOperationException($"Edge references unknown city '{seg.FromCityId}'.");
            if (!cityBuilder.ContainsKey(seg.ToCityId))
                throw new InvalidOperationException($"Edge references unknown city '{seg.ToCityId}'.");
            if (seg.FromCityId == seg.ToCityId)
                throw new InvalidOperationException($"Self-loop edge on city '{seg.FromCityId}' is not allowed.");
            if (seg.DistanceKm <= 0)
                throw new InvalidOperationException($"Edge {seg.FromCityId}->{seg.ToCityId} must have positive distance.");

            AddEdge(adj, seg.FromCityId, seg.ToCityId, seg.DistanceKm);
            AddEdge(adj, seg.ToCityId, seg.FromCityId, seg.DistanceKm);
        }

        var adjImmutable = adj.ToImmutableDictionary(
            kv => kv.Key,
            kv => kv.Value.ToImmutableDictionary(StringComparer.Ordinal),
            StringComparer.Ordinal);

        return new CityGraph(cityBuilder.ToImmutable(), adjImmutable);

        static void AddEdge(Dictionary<string, Dictionary<string, double>> map, string from, string to, double km)
        {
            if (!map.TryGetValue(from, out var inner))
            {
                inner = new(StringComparer.Ordinal);
                map[from] = inner;
            }
            if (inner.TryGetValue(to, out var existing) && Math.Abs(existing - km) > 1e-9)
                throw new InvalidOperationException($"Conflicting distances for edge {from}->{to}.");
            inner[to] = km;
        }
    }
}
