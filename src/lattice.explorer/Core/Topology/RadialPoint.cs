namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// A node's projected position on the radial topology canvas: its Cartesian
/// coordinates relative to the centre, plus the polar ring radius and angle it
/// was derived from.
/// </summary>
/// <param name="X">The horizontal offset from the canvas centre, in canvas units.</param>
/// <param name="Y">The vertical offset from the canvas centre, in canvas units.</param>
/// <param name="Radius">The ring radius the node sits on.</param>
/// <param name="AngleRadians">The angle around the circle, in radians.</param>
public readonly record struct RadialPoint(double X, double Y, double Radius, double AngleRadians);
