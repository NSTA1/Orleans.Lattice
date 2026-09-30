namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>How a writer was admitted by <see cref="RepoContextGrainStorageWriteGate"/>.</summary>
public enum RepoContextGrainStorageWriteGateOutcome
{
    /// <summary>The gate bounds nothing, so there was no admission to take.</summary>
    Unbounded = 0,

    /// <summary>A permit was free and was taken without waiting.</summary>
    Immediate = 1,

    /// <summary>The writer waited for a permit and was admitted.</summary>
    Queued = 2,

    /// <summary>
    /// The writer waited the whole timeout without being admitted and is proceeding
    /// ungated, exactly as it would have with no gate at all.
    /// </summary>
    TimedOut = 3,
}
