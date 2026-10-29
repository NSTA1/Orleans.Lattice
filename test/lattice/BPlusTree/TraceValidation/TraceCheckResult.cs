namespace Orleans.Lattice.Tests.BPlusTree.TraceValidation;

/// <summary>
/// The outcome of one TLC run over a trace.
/// </summary>
/// <param name="ExitCode">TLC's process exit code.</param>
/// <param name="Output">TLC's combined standard output and error.</param>
/// <param name="DistinctStates">The distinct-state count TLC reported, or -1 when it reported none.</param>
/// <param name="Elapsed">Wall-clock time of the TLC process.</param>
/// <param name="TraceLength">The number of spec-level events in the trace.</param>
internal sealed record TraceCheckResult(int ExitCode, string Output, long DistinctStates, TimeSpan Elapsed, int TraceLength)
{
    /// <summary>
    /// Whether TLC accepted the trace. The behaviour is deterministic - every
    /// event pins its action's choices - so an accepted trace of n events
    /// explores exactly n + 1 distinct states. Requiring that count, and not
    /// only a clean exit, is what stops an initial predicate that matched
    /// nothing (zero states, also a clean exit) from passing as acceptance.
    /// </summary>
    public bool Accepted =>
        ExitCode == 0
        && Output.Contains(AtomicCommitTraceChecker.CleanBanner, StringComparison.Ordinal)
        && DistinctStates == TraceLength + 1;
}
