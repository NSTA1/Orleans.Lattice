namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>The shape of an evaluated result, so a panel picks the right chart.</summary>
public enum ExplorerTelemetryResultKind
{
    /// <summary>No series matched.</summary>
    Empty = 0,

    /// <summary>One value per series at a single instant.</summary>
    Vector = 1,

    /// <summary>A series of points per series across a window.</summary>
    Matrix = 2,

    /// <summary>A single unlabelled value.</summary>
    Scalar = 3,
}
