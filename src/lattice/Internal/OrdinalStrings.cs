namespace Orleans.Lattice;

/// <summary>
/// Ordinal string helpers shared by the key-range and membership code paths.
/// Every comparison here is <see cref="StringComparison.Ordinal"/>, the ordering
/// the tree's keyspace is defined in.
/// </summary>
internal static class OrdinalStrings
{
    /// <summary>
    /// Ordinal maximum of two optional range bounds, treating
    /// <see langword="null"/> as "unbounded on this side": a
    /// <see langword="null"/> operand yields the other operand. Used to fold a
    /// scan's inclusive-start and exclusive-after bounds into the single lower
    /// bound a bounded read seeks to. Using an exclusive bound as an inclusive one
    /// is deliberately conservative: it can only widen the range by the boundary
    /// row, which the caller's own filter then skips.
    /// </summary>
    /// <param name="left">The first bound; may be <see langword="null"/>.</param>
    /// <param name="right">The second bound; may be <see langword="null"/>.</param>
    internal static string? MaxBound(string? left, string? right)
        => left is null ? right
            : right is null ? left
            : string.CompareOrdinal(left, right) >= 0 ? left : right;

    /// <summary>
    /// Ordinal minimum of two optional range bounds, treating
    /// <see langword="null"/> as "unbounded on this side": a
    /// <see langword="null"/> operand yields the other operand. Used to fold a
    /// scan's exclusive-end, exclusive-before and split-key bounds into the single
    /// upper bound a bounded read stops at.
    /// </summary>
    /// <param name="left">The first bound; may be <see langword="null"/>.</param>
    /// <param name="right">The second bound; may be <see langword="null"/>.</param>
    internal static string? MinBound(string? left, string? right)
        => left is null ? right
            : right is null ? left
            : string.CompareOrdinal(left, right) <= 0 ? left : right;

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="values"/> holds an
    /// element ordinally equal to <paramref name="value"/>. An indexed walk, so a
    /// short list is probed without allocating an enumerator.
    /// </summary>
    /// <param name="values">The list to search.</param>
    /// <param name="value">The value to look for.</param>
    internal static bool Contains(IReadOnlyList<string> values, string value)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (string.Equals(values[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
