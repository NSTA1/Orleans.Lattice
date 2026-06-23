namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// A density-aware radial projection for a laid-out <see cref="TopologyGraph"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="RadialLayout"/> maps a node's in-order column to an angle and its
/// level to a fixed ring radius. Fixed ring spacing is fine for small trees but
/// collapses for high fan-out shapes: a tree whose leaf ring carries hundreds of
/// nodes packs them into an arc far too short for the node marker, so the ring
/// renders as a single overlapping smear instead of distinct nodes.
/// </para>
/// <para>
/// <see cref="RadialFrame"/> fixes that by sizing each level's ring so the most
/// closely spaced pair of nodes on it is separated by at least
/// <see cref="RadialLayout.MinNodeArc"/> of arc. Rings only ever grow relative to
/// the fixed baseline, so small trees keep their existing compact look while
/// dense rings expand enough for every node to render as a distinct marker. The
/// SVG viewBox fits the whole frame, so the larger radius simply zooms out.
/// </para>
/// </remarks>
public sealed class RadialFrame
{
    private readonly double[] _ringRadii;

    private RadialFrame(double[] ringRadii, int columnCount, double extent)
    {
        _ringRadii = ringRadii;
        ColumnCount = columnCount;
        Extent = extent;
    }

    /// <summary>The number of grid columns the layout spans (the angular divisor).</summary>
    public int ColumnCount { get; }

    /// <summary>
    /// The half-extent of the square canvas viewBox that frames the whole graph
    /// with the configured empty margin.
    /// </summary>
    public double Extent { get; }

    /// <summary>The ring radius assigned to each level (index = level).</summary>
    public IReadOnlyList<double> RingRadii => _ringRadii;

    /// <summary>The ring radius for a level, clamped into range.</summary>
    public double RingRadiusFor(int level)
    {
        if (_ringRadii.Length == 0)
        {
            return 0;
        }

        var index = Math.Clamp(level, 0, _ringRadii.Length - 1);
        return _ringRadii[index];
    }

    /// <summary>Projects an abstract grid position onto radial canvas coordinates.</summary>
    public RadialPoint Project(double column, int level)
    {
        var radius = RingRadiusFor(level);
        var angle = ColumnCount <= 0
            ? 0
            : ((column + 0.5) / ColumnCount * (2 * Math.PI)) - (Math.PI / 2);

        return new RadialPoint(radius * Math.Cos(angle), radius * Math.Sin(angle), radius, angle);
    }

    /// <summary>
    /// Builds a frame for <paramref name="graph"/>, sizing each level's ring so
    /// its nodes never overlap and keeping rings strictly ordered outward.
    /// </summary>
    public static RadialFrame Build(TopologyGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);

        var columnCount = Math.Max(graph.ColumnCount, 1);
        var levelCount = graph.LevelCount;

        if (graph.Nodes.Count == 0 || levelCount <= 0)
        {
            return new RadialFrame(Array.Empty<double>(), columnCount, RadialLayout.Extent(0, 0));
        }

        var rootCount = graph.Nodes.Count(n => n.Level == 0);

        // The smallest gap, in columns, between adjacent nodes on each level.
        var minColumnGap = MinColumnGapPerLevel(graph.Nodes, levelCount);

        var radii = new double[levelCount];
        var previous = double.NegativeInfinity;
        for (var level = 0; level < levelCount; level++)
        {
            var baseline = RadialLayout.RingRadius(level, rootCount);

            // Radius at which this level's tightest column gap spans at least the
            // minimum arc a node marker needs: arc = radius * gapAngle, where
            // gapAngle = (gap / columnCount) * 2*pi.
            var gap = minColumnGap[level];
            var density = gap > 0
                ? RadialLayout.MinNodeArc * columnCount / (gap * 2 * Math.PI)
                : 0;

            var radius = Math.Max(baseline, density);

            // Keep rings ordered outward with a minimum separation so parent edges
            // always point away from the centre.
            if (radius <= previous + RadialLayout.MinRingGap)
            {
                radius = previous + RadialLayout.MinRingGap;
            }

            radii[level] = radius;
            previous = radius;
        }

        var content = radii[^1] + RadialLayout.NodeRadius + RadialLayout.Padding;
        var extent = content / (1 - RadialLayout.MarginFraction);

        return new RadialFrame(radii, columnCount, extent);
    }

    private static double[] MinColumnGapPerLevel(IReadOnlyList<PositionedNode> nodes, int levelCount)
    {
        var columnsByLevel = new List<double>[levelCount];
        foreach (var node in nodes)
        {
            if (node.Level < 0 || node.Level >= levelCount)
            {
                continue;
            }

            (columnsByLevel[node.Level] ??= new List<double>()).Add(node.Column);
        }

        var gaps = new double[levelCount];
        for (var level = 0; level < levelCount; level++)
        {
            var columns = columnsByLevel[level];
            if (columns is null || columns.Count < 2)
            {
                // A single node (or none) on a level imposes no spacing pressure.
                gaps[level] = 0;
                continue;
            }

            columns.Sort();
            var min = double.PositiveInfinity;
            for (var i = 1; i < columns.Count; i++)
            {
                var delta = columns[i] - columns[i - 1];
                if (delta > 0 && delta < min)
                {
                    min = delta;
                }
            }

            gaps[level] = double.IsPositiveInfinity(min) ? 0 : min;
        }

        return gaps;
    }
}
