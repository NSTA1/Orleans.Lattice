namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// One reading of the process's heap ceiling and the commitment measured against it.
/// </summary>
/// <remarks>
/// Every field is read from the runtime rather than configured, so the same type
/// reports a container memory limit and a developer machine's physical memory without
/// being told which it is on. See <see cref="RepoContextHeapCeilingMeter"/>.
/// </remarks>
/// <param name="LimitBytes">
/// Memory the garbage collector believes it may use: the GC hard limit, which in a
/// container defaults to 75% of the cgroup limit.
/// </param>
/// <param name="CommittedBytes">Memory committed as of the last collection.</param>
/// <param name="HighLoadThresholdBytes">
/// The commitment at which the collector treats memory as under pressure. Computed
/// against total physical or cgroup memory (90% by default), NOT against
/// <paramref name="LimitBytes"/>, so it may sit above the limit and be unreachable.
/// See issue #3133 and the remarks on <see cref="RepoContextHeapCeilingMeter"/>.
/// </param>
public readonly record struct RepoContextHeapCeiling(
    long LimitBytes,
    long CommittedBytes,
    long HighLoadThresholdBytes);
