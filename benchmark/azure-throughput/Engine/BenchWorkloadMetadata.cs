namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Shared kebab-case formatter for <see cref="BenchWorkloadMode"/>.
/// Lives on a static class (not as a top-level local function) so the
/// same mapping is reachable from both the program's startup section
/// AND from the <c>TcpIngestService</c> / <c>BenchWorkloadDispatcher</c>
/// class methods. Top-level local functions cannot be referenced from
/// non-top-level types (CS8801).
/// </summary>
public static class BenchWorkloadMetadata
{
    /// <summary>
    /// Renders <paramref name="mode"/> in the same kebab-case form
    /// <c>ParseWorkloadMode</c> accepts. Used for the silo's startup
    /// banner echo line, the per-call latency histogram's <c>mode</c>
    /// tag, and log-message context.
    /// </summary>
    public static string FormatWorkloadMode(BenchWorkloadMode mode) => mode switch
    {
        BenchWorkloadMode.SetMany => "set-many",
        BenchWorkloadMode.SetManyAtomic => "set-many-atomic",
        BenchWorkloadMode.SetManyAtomic2 => "set-many-atomic-2",
        BenchWorkloadMode.CrossTreeAtomic2 => "cross-tree-atomic-2",
        BenchWorkloadMode.CrossTreeAtomic64 => "cross-tree-atomic-64",
        BenchWorkloadMode.SetPoint => "set-point",
        BenchWorkloadMode.SetPointMv => "set-point-mv",
        BenchWorkloadMode.GetPoint => "get-point",
        BenchWorkloadMode.GetMany => "get-many",
        _ => mode.ToString(),
    };
}
