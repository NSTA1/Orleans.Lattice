namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Canonicalises a sequence of strings into an ordinal-sorted, de-duplicated
/// collection - the exact shape the cross-tree and view coordination barriers
/// stamp onto their wait/participant sets so an exact-match comparison is
/// order- and duplicate-insensitive.
/// <para>
/// The naive canonicalisation <c>source.Distinct(Ordinal).OrderBy(Ordinal)</c>
/// allocates, on every call, LINQ's ordering machinery: an
/// <c>OrderedEnumerable</c> wrapper, a materialised element buffer, a projected
/// key array, and an integer sort-index map - all in addition to the result
/// collection. These sites sit on warm cross-tree/view coordination paths that
/// re-canonicalise the set on each registration or terminal, so that machinery
/// is pure steady-state garbage.
/// </para>
/// <para>
/// This helper de-duplicates through a single <see cref="HashSet{T}"/> pass and
/// sorts the result in place, so only the result collection (and the transient
/// dedup set) is allocated. The output is byte-for-byte identical to the LINQ
/// form: a de-duplicated set rendered in a total ordinal order is deterministic,
/// independent of input order.
/// </para>
/// </summary>
internal static class CanonicalStringSet
{
    /// <summary>
    /// Returns <paramref name="source"/> as an ordinal-sorted, de-duplicated
    /// <see cref="List{T}"/>. Equivalent to
    /// <c>source.Distinct(StringComparer.Ordinal).OrderBy(v =&gt; v, StringComparer.Ordinal).ToList()</c>.
    /// </summary>
    public static List<string> SortedDistinct(IEnumerable<string> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        var list = new List<string>(new HashSet<string>(source, StringComparer.Ordinal));
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    /// <summary>
    /// Returns <paramref name="source"/> as an ordinal-sorted, de-duplicated
    /// <see cref="string"/> array. Equivalent to
    /// <c>source.Distinct(StringComparer.Ordinal).OrderBy(v =&gt; v, StringComparer.Ordinal).ToArray()</c>.
    /// </summary>
    public static string[] SortedDistinctArray(IEnumerable<string> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        var set = new HashSet<string>(source, StringComparer.Ordinal);
        var array = new string[set.Count];
        set.CopyTo(array);
        Array.Sort(array, StringComparer.Ordinal);
        return array;
    }
}
