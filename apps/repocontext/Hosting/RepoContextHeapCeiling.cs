namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// One reading of the process's heap ceiling and the commitment measured against it.
/// </summary>
/// <remarks>
/// Every field is read from the runtime rather than configured, so the same type
/// reports a container memory limit and a developer machine's physical memory without
/// being told which it is on. See <see cref="RepoContextHeapCeilingMeter"/>.
/// </remarks>
/// <param name="LimitBytes">Memory the garbage collector believes it may use.</param>
/// <param name="CommittedBytes">Memory committed as of the last collection.</param>
/// <param name="HighLoadThresholdBytes">
/// The commitment at which the collector treats memory as under pressure.
/// </param>
public readonly record struct RepoContextHeapCeiling(
    long LimitBytes,
    long CommittedBytes,
    long HighLoadThresholdBytes);
