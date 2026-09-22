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
    /// Parses the <c>BENCH_WORKLOAD_MODE</c> env-var value, accepting
    /// case-insensitive kebab-case and its run-together spelling. Null,
    /// empty and unrecognised input fall back to
    /// <see cref="BenchWorkloadMode.SetMany"/>.
    /// </summary>
    /// <remarks>
    /// Deliberately shared rather than duplicated per host. Both the silo
    /// and the Orleans-client producer must resolve the same env-var to the
    /// same workload, because the harness labels a result cell from the
    /// value it *set*, not from the value the host *parsed*. A mode added to
    /// one copy and missed in the other would not fail: the host would
    /// silently fall through to <c>set-many</c> and report a plausible
    /// number under the wrong workload name, which is far more damaging than
    /// a crash because nothing in the output would look wrong.
    /// </remarks>
    public static BenchWorkloadMode ParseWorkloadMode(string? raw) =>
        string.IsNullOrWhiteSpace(raw) ? BenchWorkloadMode.SetMany : raw.Trim().ToLowerInvariant() switch
        {
            "set-many" or "setmany" => BenchWorkloadMode.SetMany,
            "set-many-atomic" or "setmanyatomic" => BenchWorkloadMode.SetManyAtomic,
            "set-many-atomic-2" or "setmanyatomic2" => BenchWorkloadMode.SetManyAtomic2,
            "cross-tree-atomic-2" or "crosstreeatomic2" => BenchWorkloadMode.CrossTreeAtomic2,
            "cross-tree-atomic-64" or "crosstreeatomic64" => BenchWorkloadMode.CrossTreeAtomic64,
            "set-point-mv" or "setpointmv" => BenchWorkloadMode.SetPointMv,
            "set-point" or "setpoint" or "set" => BenchWorkloadMode.SetPoint,
            "get-point" or "getpoint" or "get" => BenchWorkloadMode.GetPoint,
            "get-many" or "getmany" => BenchWorkloadMode.GetMany,
            _ => BenchWorkloadMode.SetMany,
        };

    /// <summary>
    /// Renders <paramref name="mode"/> in the same kebab-case form
    /// <see cref="ParseWorkloadMode"/> accepts. Used for the silo's startup
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
