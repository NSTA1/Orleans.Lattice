namespace Orleans.Lattice;

/// <summary>
/// Ordinal key-range helpers for prefix scans over the Lattice string keyspace.
/// </summary>
/// <remarks>
/// Every Lattice key is a <see cref="string"/> compared with
/// <see cref="StringComparison.Ordinal"/> (UTF-16 code-unit order), so a
/// "starts-with <c>prefix</c>" query is exactly the half-open range
/// <c>[prefix, PrefixUpperBound(prefix))</c>. Computing that exclusive upper
/// bound correctly is subtle at the <c>U+FFFF</c> boundary, so it is defined
/// once here and shared by every package that bounds a prefix scan rather than
/// re-derived per call site.
/// </remarks>
public static class LatticeKeyRange
{
    /// <summary>
    /// Computes the exclusive upper bound of the half-open range
    /// <c>[prefix, bound)</c> that covers exactly the keys which start with
    /// <paramref name="prefix"/> under <see cref="StringComparison.Ordinal"/>
    /// comparison: the smallest string that sorts strictly after every key
    /// beginning with <paramref name="prefix"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The bound is the prefix with its last code unit below
    /// <see cref="char.MaxValue"/> incremented and every trailing
    /// <see cref="char.MaxValue"/> (<c>U+FFFF</c>) code unit dropped. The final
    /// code unit is never advanced unconditionally: doing so would wrap a
    /// trailing <c>U+FFFF</c> to <c>U+0000</c> and yield a bound that sorts
    /// <em>below</em> the prefix, silently inverting the range so a scan
    /// captures nothing.
    /// </para>
    /// <para>
    /// Returns <see langword="null"/> when no finite upper bound exists - an
    /// empty <paramref name="prefix"/>, or one consisting solely of
    /// <c>U+FFFF</c> code units - meaning every ordinally-greater key shares the
    /// prefix and the range is unbounded above (a scan should run to the end of
    /// the keyspace). The scan primitives accept a <see langword="null"/>
    /// exclusive upper bound to mean exactly that.
    /// </para>
    /// <para>
    /// The bound is computed at the UTF-16 code-unit level, which is precisely
    /// the granularity of ordinal comparison, so the result is a correct
    /// comparison bound even for keys that contain surrogate pairs (it is a
    /// sort key, not necessarily a well-formed Unicode string).
    /// </para>
    /// </remarks>
    /// <param name="prefix">The inclusive key prefix. Must not be <see langword="null"/>.</param>
    /// <returns>
    /// The exclusive upper bound of the prefix range, or <see langword="null"/>
    /// when the range has no finite upper bound.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="prefix"/> is <see langword="null"/>.</exception>
    public static string? PrefixUpperBound(string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);

        // Scan back to the last code unit that can be incremented without
        // overflow. Everything from that unit onward becomes the bound: the
        // prefix truncated to that unit, with the unit itself advanced by one.
        // Trailing U+FFFF units are dropped by the truncation. A single
        // allocation (the result string) is filled in place via string.Create.
        for (var i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] != char.MaxValue)
            {
                return string.Create(i + 1, (prefix, i), static (span, state) =>
                {
                    var (source, last) = state;
                    source.AsSpan(0, last + 1).CopyTo(span);
                    span[last]++;
                });
            }
        }

        return null;
    }
}
