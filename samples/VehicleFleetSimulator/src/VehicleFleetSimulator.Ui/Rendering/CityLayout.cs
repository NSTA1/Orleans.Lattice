using VehicleFleetSimulator.Ui.Models;

namespace VehicleFleetSimulator.Ui.Rendering;

/// <summary>
/// Computes a stable 2-D position for every city in a graph. The layout is deterministic for a
/// given input (seeded by city id), and is computed exactly once per graph snapshot. After
/// <see cref="Compute"/> returns, no node ever moves — that is the contract the rendering loop
/// depends on.
/// </summary>
/// <remarks>
/// <para>
/// The simulator's city graph carries no geographic coordinates, so we synthesise them with a
/// few iterations of a Fruchterman-Reingold style force-directed layout, seeded by a hash of the
/// city id. Because the seed and parameters are fixed, every reload (and every connected client)
/// agrees on the same picture even though the algorithm is randomised.
/// </para>
/// <para>
/// Coordinates are returned in an arbitrary unit space; the renderer projects them into pixel
/// space with letterboxing, so the absolute scale doesn't matter.
/// </para>
/// </remarks>
public static class CityLayout
{
    public readonly record struct Node(string Id, double X, double Y);
    public readonly record struct Edge(string A, string B);
    public sealed record Result(IReadOnlyList<Node> Cities, IReadOnlyList<Edge> Edges,
        double MinX, double MinY, double MaxX, double MaxY,
        IReadOnlyDictionary<string, (double X, double Y)> ById);

    public static Result Compute(CityGraphDto graph)
    {
        var cities = graph.Cities;
        var n = cities.Count;
        if (n == 0)
        {
            return new Result([], [], 0, 0, 1, 1,
                new Dictionary<string, (double X, double Y)>());
        }

        // Deduplicate undirected edges so the spring force isn't applied twice when the API
        // surfaces both (a→b) and (b→a).
        var edgeSet = new HashSet<(string, string)>();
        var edges = new List<Edge>();
        foreach (var e in graph.Edges)
        {
            var (a, b) = string.CompareOrdinal(e.FromCityId, e.ToCityId) <= 0
                ? (e.FromCityId, e.ToCityId)
                : (e.ToCityId, e.FromCityId);
            if (edgeSet.Add((a, b)))
            {
                edges.Add(new Edge(a, b));
            }
        }

        // Seed positions from a stable hash of the city id so the algorithm is deterministic.
        var index = new Dictionary<string, int>(n);
        var x = new double[n];
        var y = new double[n];
        for (var i = 0; i < n; i++)
        {
            var id = cities[i].Id;
            index[id] = i;
            var h = StableHash(id);
            // Map two halves of the 64-bit hash into [-0.5, 0.5].
            x[i] = ((h & 0xFFFFFFFFu) / (double)uint.MaxValue) - 0.5;
            y[i] = (((h >> 32) & 0xFFFFFFFFu) / (double)uint.MaxValue) - 0.5;
        }

        // Edge index table for the spring pass.
        var edgePairs = new (int A, int B)[edges.Count];
        for (var i = 0; i < edges.Count; i++)
        {
            if (index.TryGetValue(edges[i].A, out var ia) &&
                index.TryGetValue(edges[i].B, out var ib))
            {
                edgePairs[i] = (ia, ib);
            }
            else
            {
                edgePairs[i] = (-1, -1);
            }
        }

        // Fruchterman-Reingold parameters. The constants are tuned for "looks reasonable
        // at any reasonable fleet size up to a few hundred cities" — beyond that the linear
        // O(n^2) repulsion pass becomes the bottleneck and we'd want a quadtree approximation.
        const int Iterations = 250;
        var area = 1.0;
        var k = Math.Sqrt(area / Math.Max(1, n));
        var temperature = 0.1;
        var cooling = temperature / Iterations;

        var dx = new double[n];
        var dy = new double[n];

        for (var iter = 0; iter < Iterations; iter++)
        {
            Array.Clear(dx);
            Array.Clear(dy);

            // Repulsive forces between every pair.
            for (var i = 0; i < n; i++)
            {
                for (var j = i + 1; j < n; j++)
                {
                    var ddx = x[i] - x[j];
                    var ddy = y[i] - y[j];
                    var dist2 = ddx * ddx + ddy * ddy;
                    if (dist2 < 1e-9) { ddx = 1e-4; ddy = 0; dist2 = 1e-8; }
                    var dist = Math.Sqrt(dist2);
                    var force = (k * k) / dist;
                    var fx = (ddx / dist) * force;
                    var fy = (ddy / dist) * force;
                    dx[i] += fx; dy[i] += fy;
                    dx[j] -= fx; dy[j] -= fy;
                }
            }

            // Attractive (spring) forces along edges.
            foreach (var (a, b) in edgePairs)
            {
                if (a < 0) continue;
                var ddx = x[a] - x[b];
                var ddy = y[a] - y[b];
                var dist = Math.Sqrt(ddx * ddx + ddy * ddy);
                if (dist < 1e-6) continue;
                var force = (dist * dist) / k;
                var fx = (ddx / dist) * force;
                var fy = (ddy / dist) * force;
                dx[a] -= fx; dy[a] -= fy;
                dx[b] += fx; dy[b] += fy;
            }

            // Apply, capped by the current temperature so the layout settles.
            for (var i = 0; i < n; i++)
            {
                var disp = Math.Sqrt(dx[i] * dx[i] + dy[i] * dy[i]);
                if (disp < 1e-9) continue;
                var capped = Math.Min(disp, temperature);
                x[i] += (dx[i] / disp) * capped;
                y[i] += (dy[i] / disp) * capped;
            }

            temperature -= cooling;
            if (temperature < 1e-4) temperature = 1e-4;
        }

        var nodes = new Node[n];
        var byId = new Dictionary<string, (double X, double Y)>(n);
        double minX = double.PositiveInfinity, minY = double.PositiveInfinity;
        double maxX = double.NegativeInfinity, maxY = double.NegativeInfinity;
        for (var i = 0; i < n; i++)
        {
            nodes[i] = new Node(cities[i].Id, x[i], y[i]);
            byId[cities[i].Id] = (x[i], y[i]);
            if (x[i] < minX) minX = x[i];
            if (y[i] < minY) minY = y[i];
            if (x[i] > maxX) maxX = x[i];
            if (y[i] > maxY) maxY = y[i];
        }

        if (minX == maxX) { minX -= 0.5; maxX += 0.5; }
        if (minY == maxY) { minY -= 0.5; maxY += 0.5; }

        return new Result(nodes, edges, minX, minY, maxX, maxY, byId);
    }

    /// <summary>FNV-1a 64-bit. Stable across runtimes; <see cref="string.GetHashCode()"/> isn't.</summary>
    private static ulong StableHash(string s)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var h = offset;
        foreach (var c in s)
        {
            h ^= c;
            h *= prime;
        }
        return h;
    }
}
