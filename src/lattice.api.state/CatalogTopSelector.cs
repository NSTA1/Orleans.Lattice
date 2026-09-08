namespace Orleans.Lattice.Api.State;

/// <summary>
/// Selects the ordinally smallest <c>limit</c> ids out of a candidate stream
/// without ever buffering or sorting the whole stream.
/// </summary>
/// <remarks>
/// <para>
/// The catalog endpoints emit at most one page (<c>EffectivePageSize</c>
/// entries, plus one lookahead so the next-page token can be decided), yet the
/// straightforward shape buffers every surviving id into a list and sorts all of
/// it before taking the page. That is O(n) allocation and an O(n log n) sort to
/// serve O(page) results. This selector keeps a bounded max-heap of exactly the
/// ids seen so far that could still make the page, so the allocation is O(page)
/// and the scan is O(n log page) with no growth in n.
/// </para>
/// <para>
/// A max-heap - not a min-heap - is the right structure here: the value to evict
/// when a smaller candidate arrives is the largest one held, so the root must be
/// the maximum. The heap is only ordered at the end, by
/// <see cref="ToOrderedArray"/>, which sorts the retained ids ascending; a heap
/// is not itself sorted, so nothing may read the backing array before then.
/// </para>
/// <para>
/// This is only a valid substitute for the full sort when nothing downstream can
/// thin the candidate set further. A per-entry filter that runs after the
/// ordering (auth-backed visibility, a source-tree filter) can drop an arbitrary
/// number of the selected ids, so a page's worth of candidates would no longer
/// be enough to fill a page. Call sites must fall back to the full ordering
/// whenever such a filter is in play.
/// </para>
/// </remarks>
internal sealed class CatalogTopSelector
{
    private readonly string[] _heap;
    private int _count;

    /// <summary>
    /// Creates a selector that retains at most <paramref name="limit"/> ids.
    /// </summary>
    /// <param name="limit">
    /// The number of ids to retain. Callers pass the page size plus one, so the
    /// presence of a further id - and therefore the next-page token - is decided
    /// by the selection itself rather than by a second pass over the source.
    /// </param>
    internal CatalogTopSelector(int limit)
    {
        _heap = limit <= 0 ? [] : new string[limit];
    }

    /// <summary>The number of ids currently retained.</summary>
    internal int Count => _count;

    /// <summary>
    /// Offers one candidate. It is retained when the selector is not yet full,
    /// or when it sorts ordinally before the largest id currently retained;
    /// otherwise it is discarded and cannot affect the page.
    /// </summary>
    /// <param name="candidate">The candidate id.</param>
    internal void Offer(string candidate)
    {
        if (_heap.Length == 0)
        {
            return;
        }

        if (_count < _heap.Length)
        {
            var child = _count++;
            while (child > 0)
            {
                var parent = (child - 1) >> 1;
                if (string.CompareOrdinal(_heap[parent], candidate) >= 0)
                {
                    break;
                }

                _heap[child] = _heap[parent];
                child = parent;
            }

            _heap[child] = candidate;
            return;
        }

        // Full: the candidate only matters if it displaces the current maximum.
        if (string.CompareOrdinal(candidate, _heap[0]) >= 0)
        {
            return;
        }

        var parentIndex = 0;
        while (true)
        {
            var left = (parentIndex << 1) + 1;
            if (left >= _count)
            {
                break;
            }

            var right = left + 1;
            var largest = right < _count && string.CompareOrdinal(_heap[right], _heap[left]) > 0
                ? right
                : left;

            if (string.CompareOrdinal(_heap[largest], candidate) <= 0)
            {
                break;
            }

            _heap[parentIndex] = _heap[largest];
            parentIndex = largest;
        }

        _heap[parentIndex] = candidate;
    }

    /// <summary>
    /// Returns the retained ids in ascending ordinal order - exactly the prefix a
    /// full sort of the whole candidate set would have yielded.
    /// </summary>
    /// <returns>The retained ids, ascending.</returns>
    internal string[] ToOrderedArray()
    {
        if (_count == 0)
        {
            return [];
        }

        // The backing array is handed out directly when it is exactly full, so a
        // full page costs no copy; a short selection is trimmed to its width.
        var ordered = _count == _heap.Length ? _heap : _heap[.._count];
        Array.Sort(ordered, StringComparer.Ordinal);
        return ordered;
    }
}
