namespace Orleans.Lattice.Auth;

/// <summary>
/// Maps a <see cref="LatticeOperation"/> to its metric / audit tag string,
/// caching the single-flag values so the common case (a request that carries
/// exactly one operation flag) allocates no string. Composite masks - rare, and
/// only materialised when a listener is attached or auditing is enabled - fall
/// back to <see cref="Enum.ToString()"/>.
/// </summary>
internal static class LatticeOperationTag
{
    private static readonly string[] SingleFlagNames = BuildSingleFlagNames();

    /// <summary>Returns the tag string for <paramref name="operation"/>.</summary>
    public static string For(LatticeOperation operation)
    {
        var value = (int)operation;
        if (value > 0 && (value & (value - 1)) == 0)
        {
            // Exactly one bit set: index by bit position into the cached table.
            var index = System.Numerics.BitOperations.TrailingZeroCount((uint)value);
            if (index < SingleFlagNames.Length)
            {
                return SingleFlagNames[index];
            }
        }

        return operation == LatticeOperation.None ? "none" : operation.ToString();
    }

    private static string[] BuildSingleFlagNames()
    {
        // One entry per bit position 0..14 covering Read..TreeLifecycle.
        var names = new string[15];
        for (var bit = 0; bit < names.Length; bit++)
        {
            names[bit] = ((LatticeOperation)(1 << bit)).ToString();
        }

        return names;
    }
}
