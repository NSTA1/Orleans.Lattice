namespace Orleans.Lattice;

/// <summary>
/// The shared ordinal string ordering used by every deterministic key sort in
/// the library, held as a pre-constructed <see cref="Comparison{T}"/> rather
/// than reached through <see cref="StringComparer.Ordinal"/> at each call.
/// <para>
/// Passing a comparer to a sort looks allocation-free, because the comparer
/// itself is a cached singleton. It is not. Every <c>Sort</c> overload that
/// takes an <see cref="IComparer{T}"/> - <see cref="Array.Sort{T}(T[], IComparer{T})"/>,
/// its <c>(index, length)</c> form, <see cref="List{T}.Sort(IComparer{T})"/>,
/// and <c>MemoryExtensions.Sort</c> - funnels into
/// <c>ArraySortHelper&lt;T&gt;.Sort(Span&lt;T&gt;, IComparer&lt;T&gt;)</c>, whose body is
/// <c>IntrospectiveSort(keys, comparer.Compare)</c>. That argument is a
/// <b>method-group conversion</b>, so the runtime allocates a fresh
/// <see cref="Comparison{T}"/> delegate on every single call and never caches
/// it. Measured on net10.0 over 10 000 iterations with
/// <c>GC.GetAllocatedBytesForCurrentThread</c>, each such call costs a flat
/// <b>64 bytes</b>, independent of how many elements are being sorted.
/// </para>
/// <para>
/// The <see cref="Comparison{T}"/> overloads take the delegate the sort already
/// wanted and hand it straight to the same <c>IntrospectiveSort</c>, so a
/// delegate constructed once into a static field removes the per-call
/// allocation outright. The ordering is unchanged to the byte: the delegate
/// below is the method group of the very same
/// <see cref="StringComparer.Ordinal"/> singleton, and both overloads run the
/// identical introsort over it.
/// </para>
/// <para>
/// One asymmetry to watch when converting a call site: the
/// <see cref="Comparison{T}"/> family has no <c>(array, index, length)</c>
/// overload. Sort a sub-range as
/// <c>array.AsSpan(index, length).Sort(OrdinalStringOrder.Comparison)</c>.
/// </para>
/// </summary>
internal static class OrdinalStringOrder
{
    /// <summary>
    /// <see cref="StringComparer.Ordinal"/>'s comparison, constructed once.
    /// Substitutable for <c>StringComparer.Ordinal</c> at any sort call site
    /// without changing the resulting order.
    /// </summary>
    public static readonly Comparison<string> Comparison = StringComparer.Ordinal.Compare;
}
