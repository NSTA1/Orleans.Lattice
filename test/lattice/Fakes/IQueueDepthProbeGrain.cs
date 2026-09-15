namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Probe grain used to create genuine non-reentrancy queueing on a single
/// activation, so a queue-depth observation channel can be tested against the
/// phenomenon it claims to describe rather than against an empty queue.
/// <para>
/// Deliberately left non-reentrant (the Orleans default), because a reentrant
/// grain would interleave the concurrent calls and never build a queue at all.
/// </para>
/// </summary>
public interface IQueueDepthProbeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Occupies the activation until the test releases it through
    /// <c>QueueDepthProbeGrain.Release</c>. Concurrent callers therefore queue
    /// behind the first, which is the state under observation.
    /// </summary>
    /// <returns>The number of calls that had entered the grain body when this one did.</returns>
    Task<int> BlockUntilReleasedAsync();
}
