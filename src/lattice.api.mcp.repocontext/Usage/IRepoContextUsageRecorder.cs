namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Records the usage figures of successfully answered repocontext calls and rolls them up into a
/// bounded-window aggregate for the read-only <c>repocontext_stats</c> tool. Recording is a pure
/// measurement side channel: it must never observe or alter the answer a tool returns, and must be
/// cheap enough to run on every answered call. The default implementation keeps an in-memory window
/// and emits the same figures as telemetry counters; the seam is injectable so a host or test can
/// substitute an alternative (mirroring how the session store is a thin injectable seam).
/// </summary>
internal interface IRepoContextUsageRecorder
{
    /// <summary>The bounded window the aggregate summarises over.</summary>
    TimeSpan Window { get; }

    /// <summary>
    /// Records the figures for one answered call. Passed by <see langword="in"/> to avoid copying the
    /// value on the recording hot path. Must not allocate per call beyond what the emission surface requires.
    /// </summary>
    /// <param name="usage">The figures to record.</param>
    void Record(in RepoContextCallUsage usage);

    /// <summary>
    /// Rolls up the figures recorded within the current <see cref="Window"/> into a single aggregate.
    /// </summary>
    /// <returns>The windowed aggregate.</returns>
    RepoContextUsageAggregate Summarize();
}
