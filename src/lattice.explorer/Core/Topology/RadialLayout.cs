namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// Projects a topology graph's abstract grid positions (column, level) onto a
/// radial canvas: shard roots sit at the centre and successive levels radiate
/// outward on concentric rings, with each node's in-order column mapped to an
/// angle around the circle.
/// </summary>
/// <remarks>
/// The grid layout produced by <see cref="TopologyLayout"/> already centres a
/// parent over the span of its children (its column is the mean of its
/// children's columns), so mapping column to angle keeps a subtree within a
/// contiguous angular sector. Level maps to ring radius. When more than one
/// shard root is present the roots are placed on a small inner ring rather than
/// stacked at the exact centre.
/// </remarks>
public static class RadialLayout
{
    /// <summary>The radial distance, in canvas units, between successive levels.</summary>
    public const double LevelStep = 96;

    /// <summary>The radius of the inner ring used for the roots when more than one is present.</summary>
    public const double InnerRingRadius = 64;

    /// <summary>The rendered radius of a node marker, in canvas units.</summary>
    public const double NodeRadius = 11;

    /// <summary>
    /// The allowance, in canvas units, added beyond the outermost ring to cover a
    /// node's marker radius plus its expand/leaf-count label.
    /// </summary>
    public const double Padding = 16;

    /// <summary>
    /// The minimum fraction of the canvas left as empty margin around the graph
    /// when it is framed to fit, so the full topology shows with breathing room.
    /// </summary>
    public const double MarginFraction = 0.10;

    /// <summary>
    /// The minimum arc, in canvas units, a density-aware ring leaves between the
    /// centres of its two closest nodes so adjacent markers stay distinct rather
    /// than overlapping into a smear. A little over a node's diameter
    /// (<see cref="NodeRadius"/> x 2) to leave a visible gap.
    /// </summary>
    public const double MinNodeArc = (2 * NodeRadius) + 6;

    /// <summary>
    /// The minimum radial separation, in canvas units, kept between two
    /// successive density-aware rings so parent-to-child edges always point
    /// outward even when an inner ring is widened to fit its nodes.
    /// </summary>
    public const double MinRingGap = LevelStep;

    /// <summary>The ring radius for a given level and root count.</summary>
    /// <param name="level">The zero-based level (roots at level 0).</param>
    /// <param name="rootCount">The number of shard roots in the forest.</param>
    public static double RingRadius(int level, int rootCount)
    {
        var inner = rootCount > 1 ? InnerRingRadius : 0;
        return inner + Math.Max(0, level) * LevelStep;
    }

    /// <summary>Projects an abstract grid position onto radial canvas coordinates.</summary>
    /// <param name="column">The node's grid column (centre of its subtree).</param>
    /// <param name="level">The node's level in the visible forest.</param>
    /// <param name="columnCount">The total number of grid columns the layout spans.</param>
    /// <param name="rootCount">The number of shard roots in the forest.</param>
    public static RadialPoint Project(double column, int level, int columnCount, int rootCount)
    {
        // Map the in-order column to an angle, rotated so the first column starts
        // at the top of the circle.
        var angle = columnCount <= 0
            ? 0
            : ((column + 0.5) / columnCount * (2 * Math.PI)) - (Math.PI / 2);

        var radius = RingRadius(level, rootCount);
        return new RadialPoint(radius * Math.Cos(angle), radius * Math.Sin(angle), radius, angle);
    }

    /// <summary>
    /// The radius of the drawn content: the outermost ring plus a node's marker
    /// and label allowance. This is the half-size of the graph's bounding box.
    /// </summary>
    /// <param name="levelCount">The number of levels the layout spans.</param>
    /// <param name="rootCount">The number of shard roots in the forest.</param>
    public static double ContentRadius(int levelCount, int rootCount) =>
        RingRadius(Math.Max(0, levelCount - 1), rootCount) + NodeRadius + Padding;

    /// <summary>
    /// The half-extent of the square canvas viewBox that frames the whole graph
    /// with at least <see cref="MarginFraction"/> empty margin on the fitting
    /// axis, so a freshly loaded topology shows in full with breathing room.
    /// </summary>
    /// <param name="levelCount">The number of levels the layout spans.</param>
    /// <param name="rootCount">The number of shard roots in the forest.</param>
    public static double Extent(int levelCount, int rootCount) =>
        ContentRadius(levelCount, rootCount) / (1 - MarginFraction);
}
