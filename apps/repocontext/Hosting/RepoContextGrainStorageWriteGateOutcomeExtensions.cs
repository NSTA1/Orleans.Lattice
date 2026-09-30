namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>Helpers for <see cref="RepoContextGrainStorageWriteGateOutcome"/>.</summary>
public static class RepoContextGrainStorageWriteGateOutcomeExtensions
{
    /// <summary>Whether the outcome took a permit that must be released.</summary>
    /// <param name="outcome">The outcome.</param>
    /// <returns><see langword="true"/> when a permit is held.</returns>
    public static bool HoldsPermit(this RepoContextGrainStorageWriteGateOutcome outcome)
        => outcome is RepoContextGrainStorageWriteGateOutcome.Immediate
            or RepoContextGrainStorageWriteGateOutcome.Queued;

    /// <summary>The metric tag value naming the outcome.</summary>
    /// <param name="outcome">The outcome.</param>
    /// <returns>The tag value.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="outcome"/> is not a declared outcome.</exception>
    public static string TagValue(this RepoContextGrainStorageWriteGateOutcome outcome) => outcome switch
    {
        RepoContextGrainStorageWriteGateOutcome.Unbounded => "unbounded",
        RepoContextGrainStorageWriteGateOutcome.Immediate => "immediate",
        RepoContextGrainStorageWriteGateOutcome.Queued => "queued",
        RepoContextGrainStorageWriteGateOutcome.TimedOut => "timed_out",
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null),
    };
}
