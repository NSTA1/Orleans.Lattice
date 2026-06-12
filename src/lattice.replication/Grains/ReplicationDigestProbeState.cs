namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for the anti-entropy digest-probe scheduler grain.
/// Tracks the last-completed probe-pass ticks so a silo restart resumes
/// the comparison pass on the configured cadence rather than firing
/// immediately on every reactivation.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationDigestProbeState)]
internal sealed class ReplicationDigestProbeState
{
    /// <summary>
    /// Wall-clock ticks (UtcNow) of the most-recent completed digest-probe
    /// comparison pass. Used by the phase pump to skip the pass until the
    /// jittered probe interval has elapsed. Default <c>0</c> fires the
    /// pass on the first phase tick after activation.
    /// </summary>
    [Id(0)]
    public long LastProbeTicks { get; set; }
}
