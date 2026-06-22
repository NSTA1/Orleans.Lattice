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

    /// <summary>Extra canvas padding added around the outermost ring.</summary>
    public const double Padding = 28;

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
    /// The half-extent of the canvas: the distance from the centre to the edge of
    /// the bounding box that frames every ring, including the node radius and padding.
    /// </summary>
    /// <param name="levelCount">The number of levels the layout spans.</param>
    /// <param name="rootCount">The number of shard roots in the forest.</param>
    public static double Extent(int levelCount, int rootCount) =>
        RingRadius(Math.Max(0, levelCount - 1), rootCount) + NodeRadius + Padding;
}
