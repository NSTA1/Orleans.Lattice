namespace Orleans.Lattice.Primitives;

internal static class SplitStateExtensions
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
