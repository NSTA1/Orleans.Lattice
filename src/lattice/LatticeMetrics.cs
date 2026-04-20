namespace Orleans.Lattice;

/// <summary>
/// Telemetry naming conventions for Orleans.Lattice instruments.
/// Standardises on the orleans.lattice.* prefix before bakes in the public instrument names.
/// </summary>
public static class LatticeMetrics
{
    /// <summary>
    /// The root meter / instrument / activity-source name for all Orleans.Lattice telemetry.
    /// All internal telemetry hooks must reference this constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice";
}
