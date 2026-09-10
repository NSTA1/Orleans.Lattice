namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The garbage-collector facts this process is actually running under, as the runtime
/// reports them rather than as a configuration file declares them.
/// </summary>
/// <remarks>
/// <para>
/// A record rather than a set of direct calls at the point of use, so the reporting and
/// hazard rules built on these facts are pure functions of a value a test can construct.
/// The alternative is a diagnostic that can only be exercised by reconfiguring the
/// collector of the test process itself, which is not something a unit test can do: the GC
/// reads its configuration once, at process start, so a fixture cannot reach the
/// Workstation-on-a-large-heap combination that this whole surface exists to name.
/// </para>
/// <para>
/// <b>The resolved figures are what matters, not the declared ones.</b> Both are reported,
/// but they are different claims and they come apart in ways that are the entire point of
/// issue #2596: a declared heap count is inert under Workstation GC, and a heap count
/// declared in decimal is read by the runtime as hexadecimal. Only the resolved figure
/// says what the process is doing.
/// </para>
/// </remarks>
/// <param name="IsServerGc">
/// Whether the process runs Server GC, from <see cref="System.Runtime.GCSettings.IsServerGC"/>.
/// </param>
/// <param name="ResolvedHeapCount">
/// The number of heaps the collector actually resolved, or <see langword="null"/> when the
/// runtime did not report one. Workstation GC resolves 1 by construction.
/// </param>
/// <param name="TotalAvailableMemoryBytes">
/// The memory ceiling the collector believes it is working against, from
/// <see cref="System.GCMemoryInfo.TotalAvailableMemoryBytes"/>. In a container this is the
/// cgroup limit, which is the figure that decides how large the heap can grow before a
/// blocking collection has to walk it.
/// </param>
/// <param name="TotalPauseDuration">
/// How long this process has been suspended for garbage collection since it started, from
/// <see cref="GC.GetTotalPauseDuration"/>.
/// </param>
public readonly record struct RepoContextGarbageCollectionFacts(
    bool IsServerGc,
    int? ResolvedHeapCount,
    long TotalAvailableMemoryBytes,
    TimeSpan TotalPauseDuration);
