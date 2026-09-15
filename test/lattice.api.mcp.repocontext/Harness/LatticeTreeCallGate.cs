namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Selector that suspends the first grain call matching a tree and method until
/// the test releases it, so a test can observe the store from outside while a
/// multi-step operation is provably mid-flight.
/// </summary>
/// <remarks>
/// This is the observation counterpart to <see cref="LatticeTreeFaultInjector"/>:
/// that one decides which calls fail, this one decides which call parks. Parking
/// is what makes an ordering defect measurable at all - an operation whose steps
/// are correct but sequenced wrongly is indistinguishable from a correct one once
/// it has finished, so the only sample that separates them is taken while it runs.
/// </remarks>
public sealed class LatticeTreeCallGate
{
    private readonly TaskCompletionSource reached =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly TaskCompletionSource released =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int claimed;

    /// <summary>Tree id whose calls are eligible to be parked.</summary>
    public required string TreeId { get; init; }

    /// <summary>Interface method name to park, for example DeleteRangeAsync.</summary>
    public required string Method { get; init; }

    /// <summary>
    /// Completes once a matching call has arrived and parked. Await this to know
    /// the operation under test is suspended at the chosen point rather than
    /// guessing with a delay.
    /// </summary>
    public Task Reached => reached.Task;

    /// <summary>Whether a call was ever parked, so a mis-aimed gate fails loudly.</summary>
    public bool WasReached => reached.Task.IsCompleted;

    /// <summary>Lets the parked call proceed. Safe to call more than once.</summary>
    public void Release() => released.TrySetResult();

    internal bool ShouldHold(string method, string? treeId)
    {
        if (!string.Equals(method, Method, StringComparison.Ordinal)
            || !string.Equals(treeId, TreeId, StringComparison.Ordinal))
        {
            return false;
        }

        // Only the first matching call parks. A tree facade call can fan out and
        // recur; parking every one would deadlock the release path.
        return Interlocked.Exchange(ref claimed, 1) == 0;
    }

    internal async Task HoldAsync()
    {
        reached.TrySetResult();
        await released.Task.ConfigureAwait(false);
    }
}
