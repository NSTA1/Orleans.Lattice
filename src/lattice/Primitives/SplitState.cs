namespace Orleans.Lattice.Primitives;

/// <summary>
/// Monotonically advancing lifecycle state for a B+ tree node split operation.
/// The lattice order is Unsplit &lt; SplitInProgress &lt; SplitComplete.
/// Merge is simply <c>max</c> — once a node reaches a higher state it can never go back.
/// </summary>
[GenerateSerializer]
public enum SplitState
{
    Unsplit = 0,
    SplitInProgress = 1,
    SplitComplete = 2
}

public static class SplitStateExtensions
{
    /// <summary>
    /// Lattice merge for <see cref="SplitState"/>: returns the maximum of two values.
    /// Commutative, associative, idempotent.
    /// </summary>
    public static SplitState Merge(this SplitState left, SplitState right) =>
        (SplitState)Math.Max((int)left, (int)right);

    /// <summary>
    /// Attempts to advance to the next state. Returns <see langword="false"/> if
    /// the target state is not strictly greater than the current state.
    /// </summary>
    public static bool TryAdvanceTo(this SplitState current, SplitState target, out SplitState result)
    {
        if (target > current)
        {
            result = target;
            return true;
        }

        result = current;
        return false;
    }
}
