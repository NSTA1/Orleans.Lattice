namespace Orleans.Lattice;

/// <summary>
/// Builds the <see cref="HashSet{T}"/> of <see cref="OrSetDot"/> that the
/// observed-remove primitives use on their large-both-sides reconciliation
/// branch, in the one shape that is actually fastest.
/// <para>
/// <see cref="HashSet{T}"/>'s collection constructor takes
/// <see cref="IEnumerable{T}"/>, so seeding it from a <see cref="List{T}"/>
/// boxes the list's struct enumerator and dispatches
/// <c>MoveNext</c> / <c>Current</c> through an interface on every element.
/// Presizing to the known count and filling through the struct enumerator
/// avoids both. Measured on the <c>ordedup</c> microbench suite
/// (<c>OrCrdtReconcileBenchmarks</c>), the collection constructor is roughly
/// 3x slower across dot histories of 16 - 256 and allocates 48 bytes more, so
/// the sibling primitives that reached for it were paying for the shorter
/// spelling.
/// </para>
/// </summary>
internal static class OrSetDotSet
{
    /// <summary>
    /// Creates a set containing every dot in <paramref name="dots"/>, presized
    /// to hold <paramref name="dots"/> plus <paramref name="extraCapacity"/>
    /// further additions so a caller that goes on to add does not rehash.
    /// </summary>
    /// <param name="dots">The dots to seed the set with.</param>
    /// <param name="extraCapacity">Additional capacity to reserve beyond <paramref name="dots"/>.</param>
    /// <returns>A presized set seeded with every dot in <paramref name="dots"/>.</returns>
    internal static HashSet<OrSetDot> Build(List<OrSetDot> dots, int extraCapacity = 0)
    {
        var set = new HashSet<OrSetDot>(dots.Count + extraCapacity);
        foreach (var dot in dots) set.Add(dot);
        return set;
    }
}
