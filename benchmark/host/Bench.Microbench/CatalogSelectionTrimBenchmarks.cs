using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The two catalog-ordering trims in the state API: bounded top-P page
/// selection, and the last remaining LINQ <c>OrderBy</c> on
/// <c>LatticeStateQuery.ListCoveredTreesAsync</c>.
/// <para>
/// <b>Judge this suite on Allocated.</b> Every lane is sub-microsecond to a few
/// microseconds and the benchmark host is shared, so Mean moves between rounds
/// for reasons unrelated to the change; Allocated reproduces bit-for-bit. Both
/// trims are allocation trims first: they remove objects that had to exist only
/// because of how the page was assembled, never because of what it computes.
/// </para>
/// <para>
/// <b>Shell fidelity.</b> Every arm in a lane runs the identical surrounding
/// shell over the identical pre-built input, with all construction hoisted into
/// <see cref="Setup"/>. The only difference inside a measured region is the body
/// under test, and each lane walks the resulting page to the same page size so
/// no arm is credited for work it merely deferred. Lane 1's shipped arm drives
/// the real shipped <see cref="CatalogTopSelector"/> rather than a copy of it,
/// and <see cref="Selection_Contrast_BoundedSortedSet"/> measures the rejected
/// alternative instead of asserting its rejection.
/// </para>
/// <para>
/// Nothing here starts a silo. Run it via
/// <c>BENCH_MICROBENCH_SUITE=stateorder</c> (or <c>--suite stateorder</c>); see
/// <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CatalogSelectionTrimBenchmarks
{
    /// <summary>Default catalog page size the state API pages at.</summary>
    private const int PageSize = 100;

    /// <summary>Catalog width every lane filters and orders.</summary>
    [Params(512, 4096)]
    public int CatalogSize { get; set; }

    private List<string> _catalogIds = [];
    private IReadOnlyCollection<string> _coveredTrees = [];
    private string? _pageToken;

    /// <summary>
    /// Builds the catalog both lanes read. The covered-tree lane is handed the
    /// same ids through an <c>IReadOnlyCollection&lt;string&gt;</c> reference,
    /// because that is the static type the shipped code enumerates and its
    /// interface enumeration is part of the shape under test.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _catalogIds = new List<string>(CatalogSize);
        for (var i = 0; i < CatalogSize; i++)
        {
            _catalogIds.Add(string.Create(CultureInfo.InvariantCulture, $"catalog-tree-{i:D5}"));
        }

        _coveredTrees = _catalogIds;

        // Page from the middle, the steady state of a paging walk: the token
        // filter drops a prefix and the rest survives to be ordered.
        _pageToken = _catalogIds[CatalogSize / 2];
    }

    // ==================================================================
    // Lane 1: bounded top-P page selection
    // ==================================================================

    /// <summary>
    /// Baseline: buffer every surviving id into a presized list and sort all of
    /// it, then take one page. This is the shape the previous trim shipped, so
    /// the lane measures the increment this change adds rather than re-crediting
    /// the LINQ removal that preceded it. The sort is O(n log n) and the buffer
    /// is O(n), to serve at most <see cref="PageSize"/> results.
    /// </summary>
    [Benchmark(Description = "Catalog page: buffer all + full sort (baseline)")]
    public int Selection_Baseline_BufferAndSort()
    {
        var pageToken = _pageToken;
        var buffered = new List<string>(_catalogIds.Count);
        for (var i = 0; i < _catalogIds.Count; i++)
        {
            var candidate = _catalogIds[i];
            if (pageToken is not null && string.CompareOrdinal(candidate, pageToken) <= 0)
            {
                continue;
            }

            buffered.Add(candidate);
        }

        buffered.Sort(StringComparer.Ordinal);
        return WalkPage(buffered);
    }

    /// <summary>
    /// Shipped: the real <see cref="CatalogTopSelector"/>, a bounded max-heap
    /// holding at most <c>pageSize + 1</c> ids. Allocation is O(page) and does
    /// not grow with the catalog; the scan is O(n log page). The lookahead entry
    /// is what lets the page decide its own next-page token without a second
    /// pass, so the emitted page and token are identical to the baseline's.
    /// </summary>
    [Benchmark(Description = "Catalog page: bounded top-P selector (shipped)")]
    public int Selection_Shipped_BoundedSelector()
    {
        var pageToken = _pageToken;
        var selector = new CatalogTopSelector(Math.Min(PageSize + 1, _catalogIds.Count));
        for (var i = 0; i < _catalogIds.Count; i++)
        {
            var candidate = _catalogIds[i];
            if (pageToken is not null && string.CompareOrdinal(candidate, pageToken) <= 0)
            {
                continue;
            }

            selector.Offer(candidate);
        }

        return WalkPage(selector.ToOrderedArray());
    }

    /// <summary>
    /// Contrast: the obvious bounded alternative, a <c>SortedSet</c> capped at
    /// <c>pageSize + 1</c> by evicting its maximum. It is rejected because it
    /// allocates a red-black node per retained id and per eviction, and because
    /// a set silently collapses duplicate ids where the page contract is a
    /// sequence - but the cost is measured here rather than asserted.
    /// </summary>
    [Benchmark(Description = "Catalog page: bounded SortedSet (contrast)")]
    public int Selection_Contrast_BoundedSortedSet()
    {
        var pageToken = _pageToken;
        var limit = Math.Min(PageSize + 1, _catalogIds.Count);
        var retained = new SortedSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < _catalogIds.Count; i++)
        {
            var candidate = _catalogIds[i];
            if (pageToken is not null && string.CompareOrdinal(candidate, pageToken) <= 0)
            {
                continue;
            }

            retained.Add(candidate);
            if (retained.Count > limit)
            {
                retained.Remove(retained.Max!);
            }
        }

        var taken = 0;
        foreach (var id in retained)
        {
            if (++taken == PageSize)
            {
                break;
            }
        }

        return taken;
    }

    // ==================================================================
    // Lane 2: covered-tree ordering
    // ==================================================================

    /// <summary>
    /// Baseline: the prior <c>ListCoveredTreesAsync</c> body - a LINQ
    /// <c>OrderBy</c> over the covered set with the page-token filter applied
    /// inside the walk. Enumerating the ordered sequence buffers the whole
    /// covered set, materialises a parallel key array and an index map, and
    /// allocates the key-selector delegate and the <c>OrderedEnumerable</c>, all
    /// to yield one page.
    /// </summary>
    [Benchmark(Description = "Covered trees: LINQ OrderBy (baseline)")]
    public int Covered_Baseline_LinqOrderBy()
    {
        var pageToken = _pageToken;
        var ordered = _coveredTrees.OrderBy(id => id, StringComparer.Ordinal);

        var taken = 0;
        foreach (var treeId in ordered)
        {
            if (pageToken is not null && string.CompareOrdinal(treeId, pageToken) <= 0)
            {
                continue;
            }

            if (++taken == PageSize)
            {
                break;
            }
        }

        return taken;
    }

    /// <summary>
    /// Shipped: one page-token pass into the bounded selector, then the page.
    /// This is both trims composed, which is how the endpoint actually ships.
    /// </summary>
    [Benchmark(Description = "Covered trees: single pass + bounded selector (shipped)")]
    public int Covered_Shipped_SinglePassBounded()
    {
        var pageToken = _pageToken;
        var selector = new CatalogTopSelector(Math.Min(PageSize + 1, _coveredTrees.Count));
        foreach (var candidate in _coveredTrees)
        {
            if (pageToken is not null && string.CompareOrdinal(candidate, pageToken) <= 0)
            {
                continue;
            }

            selector.Offer(candidate);
        }

        return WalkPage(selector.ToOrderedArray());
    }

    /// <summary>
    /// Contrast: the LINQ removal alone, without the bounded selection - a
    /// single-pass filter into a presized list followed by a full in-place sort.
    /// It splits this lane's win into its two halves, so neither trim can be
    /// credited with the other's saving.
    /// </summary>
    [Benchmark(Description = "Covered trees: single pass + full sort (contrast)")]
    public int Covered_Contrast_SinglePassFullSort()
    {
        var pageToken = _pageToken;
        var buffered = new List<string>(_coveredTrees.Count);
        foreach (var candidate in _coveredTrees)
        {
            if (pageToken is not null && string.CompareOrdinal(candidate, pageToken) <= 0)
            {
                continue;
            }

            buffered.Add(candidate);
        }

        buffered.Sort(StringComparer.Ordinal);
        return WalkPage(buffered);
    }

    /// <summary>
    /// The page walk every arm shares, so no arm is credited for deferring work
    /// the others performed. It mirrors the shipped indexed walk, including its
    /// page-size stop.
    /// </summary>
    /// <param name="ordered">The ordered candidate ids.</param>
    /// <returns>The number of ids the page emitted.</returns>
    private static int WalkPage(IReadOnlyList<string> ordered)
    {
        var taken = 0;
        for (var i = 0; i < ordered.Count; i++)
        {
            if (++taken == PageSize)
            {
                break;
            }
        }

        return taken;
    }
}
