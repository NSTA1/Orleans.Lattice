using System.Collections.Immutable;

namespace VehicleFleetSimulator.Grains.Cities;

/// <summary>
/// Random-walk route generator. Produces routes of length [minLength, maxLength] (capped at 10)
/// that adhere to graph topology, contain no consecutive duplicates, and avoid immediate
/// back-and-forth (<c>A → B → A</c>).
/// </summary>
public sealed class RouteGenerator(CityGraph graph, Random random)
{
    public const int MaxRouteLengthCap = 10;

    private readonly CityGraph _graph = graph ?? throw new ArgumentNullException(nameof(graph));
    private readonly Random _random = random ?? throw new ArgumentNullException(nameof(random));

    public ImmutableArray<string> Generate(string? startCityId = null, int minLength = 2, int maxLength = 10)
    {
        if (minLength < 2) throw new ArgumentOutOfRangeException(nameof(minLength), "Must be >= 2.");
        if (maxLength < minLength) throw new ArgumentOutOfRangeException(nameof(maxLength), "Must be >= minLength.");
        if (maxLength > MaxRouteLengthCap) maxLength = MaxRouteLengthCap;

        var startId = startCityId ?? PickRandomCity()
            ?? throw new InvalidOperationException("City graph contains no cities.");
        if (!_graph.ContainsCity(startId))
            throw new ArgumentException($"Start city '{startId}' is not in the graph.", nameof(startCityId));

        var targetLength = _random.Next(minLength, maxLength + 1);
        var route = ImmutableArray.CreateBuilder<string>(targetLength);
        route.Add(startId);

        string? previous = null;
        var current = startId;

        while (route.Count < targetLength)
        {
            var candidates = _graph.GetNeighbors(current)
                .Where(n => n != previous)
                .ToArray();

            if (candidates.Length == 0)
            {
                if (route.Count >= minLength) break;

                // Dead-end before reaching minLength: allow the prior city as a last resort,
                // but still respect "no consecutive duplicates" by checking against current.
                candidates = _graph.GetNeighbors(current).Where(n => n != current).ToArray();
                if (candidates.Length == 0)
                    throw new InvalidOperationException(
                        $"City '{current}' has no neighbours; cannot satisfy minLength={minLength}.");
            }

            var next = candidates[_random.Next(candidates.Length)];
            route.Add(next);
            previous = current;
            current = next;
        }

        return route.ToImmutable();
    }

    private string? PickRandomCity()
    {
        var cities = _graph.Cities;
        if (cities.Count == 0) return null;
        var idx = _random.Next(cities.Count);
        return cities.ElementAt(idx).Id;
    }
}
