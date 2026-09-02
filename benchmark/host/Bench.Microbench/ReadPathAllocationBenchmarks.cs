using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state read-path allocation trims so their
/// per-operation byte deltas are measurable in the clear. As with the sibling
/// <see cref="QueryProjectionAllocationBenchmarks"/> and
/// <see cref="ViewMaintainerAllocationBenchmarks"/> suites, the full end-to-end
/// cluster benchmarks route each read through Orleans serialization,
/// persistence, and task machinery and allocate on the order of tens of
/// kilobytes per op, so a sub-kilobyte trim sits below their run-to-run noise
/// floor. Each benchmark below reproduces exactly one optimized code shape
/// against its prior shape with no cluster in the loop, so the <c>Allocated</c>
/// column is deterministic and the baseline-vs-optimized delta is precisely the
/// heap the production change removes.
/// <para>
/// The pairs mirror the production edits verbatim:
/// (1) <c>LatticeStateQuery.ListViewsAsync</c> - the authenticated branch built
/// a <c>visible</c> list already presized to the candidate count, then copied it
/// into a throwaway array via <c>visible.ToArray()</c>; returning the list as an
/// <c>IReadOnlyList</c> and iterating it index-based (so reading through the
/// interface does not box <c>List&lt;T&gt;</c>'s struct enumerator) removes the
/// copy;
/// (2) <c>BPlusLeafGrain.GetLiveEntriesAsync</c> - the prior form grew the result
/// dictionary from empty while folding every live cached row, reallocating its
/// bucket and entry arrays; presizing it to the cached row count (the fold's
/// upper bound), exactly as the sibling <c>GetLiveRawEntriesAsync</c> already
/// did, removes that regrowth;
/// (3) <c>SnapshotLeafGrain.GetKeysAsync</c> / <c>GetEntriesAsync</c> /
/// <c>GetRawEntriesAsync</c> - the prior form grew the result list from empty
/// while scanning the folder's ordered rows; presizing it to
/// <c>Math.Min(limit, entryCount)</c> (a tight upper bound on the emitted count)
/// removes the list's regrowth.
/// </para>
/// <para>
/// The shapes are reproduced against stand-in record types (the production value
/// types are Orleans-serializable records whose own size is identical between
/// each pair's lanes, so the reported delta is exactly the collection overhead
/// the edit removes, not the payload). Run it via
/// <c>BENCH_MICROBENCH_SUITE=readpathtrims</c> (or <c>--suite readpathtrims</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast
/// to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence
/// intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReadPathAllocationBenchmarks
{
    /// <summary>A minimal stand-in for the internal <c>ViewListing</c> record.</summary>
    private sealed record Listing(string ViewName, string SourceTreeId);

    /// <summary>A minimal stand-in for a folder row value.</summary>
    private readonly record struct Row(byte[] Value, bool IsTombstone);

    // ---- (1) the ordered candidate set every authenticated view is visible over ----
    private Listing[] _candidates = null!;

    // ---- (2) the cached rows a live-entries read folds ----
    private KeyValuePair<string, Row>[] _cacheRows = null!;

    // ---- (3) the folder rows a snapshot read scans, plus the page limit ----
    private KeyValuePair<string, Row>[] _folderRows = null!;
    private int _limit;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // A moderate view roster where every view's source tree is readable, so
        // the authenticated visibility filter keeps them all (worst case for the
        // copied array).
        const int viewCount = 256;
        _candidates = new Listing[viewCount];
        for (var i = 0; i < viewCount; i++)
        {
            var name = "view-" + i.ToString("D5", CultureInfo.InvariantCulture);
            _candidates[i] = new Listing(name, "tree-" + (i % 8).ToString("D2", CultureInfo.InvariantCulture));
        }

        // A full leaf's worth of live cached rows (no tombstones => every row is
        // emitted, the fold's upper bound).
        const int rowCount = 512;
        _cacheRows = new KeyValuePair<string, Row>[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var key = "key-" + i.ToString("D5", CultureInfo.InvariantCulture);
            _cacheRows[i] = new KeyValuePair<string, Row>(key, new Row(new byte[8], IsTombstone: false));
        }

        // A snapshot folder read whose page limit exceeds the live row count, so
        // Math.Min(limit, count) == count and every row is emitted.
        _folderRows = _cacheRows;
        _limit = int.MaxValue;
    }

    // ------------------------------------------------------------------
    // (1) ListViewsAsync visible projection
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the presized visible list is copied into a throwaway array via
    /// <c>ToArray()</c>, then iterated with a foreach over the array.
    /// </summary>
    [Benchmark(Baseline = true, Description = "ListViews: visible.ToArray() copy (baseline)")]
    public int ListViews_ToArray()
    {
        var visible = new List<Listing>(_candidates.Length);
        foreach (var candidate in _candidates)
        {
            visible.Add(candidate);
        }

        var ordered = visible.ToArray();
        var seen = 0;
        foreach (var registration in ordered)
        {
            if (registration.ViewName.Length > 0)
            {
                seen++;
            }
        }

        return seen;
    }

    /// <summary>
    /// Optimized: the visible list is handed back as an <c>IReadOnlyList</c> with
    /// no array copy, and iterated index-based so reading through the interface
    /// does not box the list's struct enumerator.
    /// </summary>
    [Benchmark(Description = "ListViews: return list, index iterate (optimized)")]
    public int ListViews_NoCopy()
    {
        var visible = new List<Listing>(_candidates.Length);
        foreach (var candidate in _candidates)
        {
            visible.Add(candidate);
        }

        IReadOnlyList<Listing> ordered = visible;
        var seen = 0;
        for (var i = 0; i < ordered.Count; i++)
        {
            var registration = ordered[i];
            if (registration.ViewName.Length > 0)
            {
                seen++;
            }
        }

        return seen;
    }

    // ------------------------------------------------------------------
    // (2) GetLiveEntriesAsync result dictionary
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the live-entries dictionary grows from empty as the cached-row
    /// fold populates it, reallocating its bucket and entry arrays.
    /// </summary>
    [Benchmark(Description = "GetLiveEntries: dict grow from empty (baseline)")]
    public int GetLiveEntries_GrowFromEmpty()
    {
        var result = new Dictionary<string, byte[]>();
        foreach (var (key, row) in _cacheRows)
        {
            if (row.IsTombstone)
            {
                continue;
            }

            result[key] = row.Value;
        }

        return result.Count;
    }

    /// <summary>
    /// Optimized: presizing the dictionary to the cached row count (the fold's
    /// upper bound) removes the grow-from-empty rehash churn; the fold body is
    /// identical.
    /// </summary>
    [Benchmark(Description = "GetLiveEntries: dict presized (optimized)")]
    public int GetLiveEntries_Presized()
    {
        var result = new Dictionary<string, byte[]>(_cacheRows.Length);
        foreach (var (key, row) in _cacheRows)
        {
            if (row.IsTombstone)
            {
                continue;
            }

            result[key] = row.Value;
        }

        return result.Count;
    }

    // ------------------------------------------------------------------
    // (3) SnapshotLeafGrain read result list
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the snapshot read list grows from empty as the folder scan
    /// populates it, reallocating its backing array.
    /// </summary>
    [Benchmark(Description = "SnapshotRead: list grow from empty (baseline)")]
    public int SnapshotRead_GrowFromEmpty()
    {
        var result = new List<KeyValuePair<string, byte[]>>();
        foreach (var (key, row) in _folderRows)
        {
            if (row.IsTombstone)
            {
                continue;
            }

            result.Add(new KeyValuePair<string, byte[]>(key, row.Value));
            if (result.Count >= _limit)
            {
                break;
            }
        }

        return result.Count;
    }

    /// <summary>
    /// Optimized: presizing the list to <c>Math.Min(limit, count)</c> (a tight
    /// upper bound on the emitted count) removes the regrowth; the scan body is
    /// identical.
    /// </summary>
    [Benchmark(Description = "SnapshotRead: list presized (optimized)")]
    public int SnapshotRead_Presized()
    {
        var result = new List<KeyValuePair<string, byte[]>>(Math.Min(_limit, _folderRows.Length));
        foreach (var (key, row) in _folderRows)
        {
            if (row.IsTombstone)
            {
                continue;
            }

            result.Add(new KeyValuePair<string, byte[]>(key, row.Value));
            if (result.Count >= _limit)
            {
                break;
            }
        }

        return result.Count;
    }
}
