using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three trims this suite was added for, so each one's
/// per-operation delta is measurable in the clear.
/// <para>
/// (1) <c>LatticeTagIndexContext.TryParseRow</c> - every membership scan in the
/// tag index walks one row per index entry, and the parser cut the row key
/// <c>{tag}\0{treeId}\0{key}</c> into <b>three</b> substrings before any call
/// site had a chance to say which of them it wanted. All four call sites discard
/// at least one: the tag enumeration keeps a tag only when the tag actually
/// changes, the reconcile scan drops every row belonging to another tree (or
/// outside the requested key range) before it looks at the key, the covered-tree
/// self-scan wants the tree id alone, and the per-key tag lookup keeps only the
/// trailing tag of a row whose key segment matched. Locating the separators and
/// comparing on the row's own spans lets each site cut exactly what it keeps.
/// </para>
/// <para>
/// (2)/(3) <c>AggregationApplier</c> - the accumulator materialise pass awaited
/// one store read <b>per shard slot</b>, and the post-flip empty-slot cleanup
/// awaited one read per candidate key, where the two sibling materialisers
/// (<c>MaterialiseInverseAsync</c>, <c>MaterialiseFoldAsync</c>) already gather
/// every slot in a single batched <c>GetManyAsync</c>. The real win there is
/// round trips, which an in-process microbenchmark cannot charge; what the lanes
/// below <b>do</b> charge is the per-await async machinery and the per-call
/// <c>Task</c> the serial shape pays on top, which is a strict lower bound on
/// the saving rather than the whole of it. Read those two lanes as "even with
/// the round trip priced at zero, batching is not worse"; the PR body carries
/// the round-trip count separately, asserted by a test rather than measured
/// here.
/// </para>
/// <para>
/// Every lane in a pair reproduces its partner's surrounding shell exactly - the
/// same inputs, the same accumulation, the same return type - so the only thing
/// that differs is the body under test and the reported <c>Allocated</c> delta is
/// precisely the heap the production change removes. The tag-index shells mirror
/// the production code verbatim; the baseline arm cannot call the shipped method
/// because the shipped method is the optimised one.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=tagrowtrims</c> (or
/// <c>--suite tagrowtrims</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>
/// for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class TagRowParseAndShardGatherBenchmarks
{
    private const char Sep = '\0';

    /// <summary>The tree the reconcile and covered-tree lanes scan for.</summary>
    private const string TreeId = "orders-tree";

    /// <summary>A second tree whose rows the reconcile lane must reject.</summary>
    private const string OtherTreeId = "orders-tree-archive";

    private string[] _rows = null!;
    private string[] _keyRows = null!;
    private string _keyRowHead = null!;
    private string _probeKey = null!;
    private InMemoryShardStore _store = null!;
    private AsyncShardStore _asyncStore = null!;
    private List<string> _slotKeys = null!;

    /// <summary>
    /// Builds one scan's worth of membership rows. The mix is deliberate: rows
    /// are ordered by tag (as a real ordered scan delivers them) so a run of rows
    /// shares one tag, and two thirds of them belong to a tree the reconcile lane
    /// rejects, which is the shape that makes the elision worth having. The two
    /// tree ids are prefix-nested so a length-blind span compare would be caught
    /// by the lane rather than flattered by it.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        const int tagCount = 16;
        const int keysPerTag = 16;

        var rows = new List<string>(tagCount * keysPerTag);
        for (var t = 0; t < tagCount; t++)
        {
            var tag = "tag-" + t.ToString("D2", CultureInfo.InvariantCulture);
            for (var k = 0; k < keysPerTag; k++)
            {
                var tree = k % 3 == 0 ? TreeId : OtherTreeId;
                var key = "key-" + (t * keysPerTag + k).ToString("D4", CultureInfo.InvariantCulture);
                rows.Add(tag + Sep + tree + Sep + key);
            }
        }

        _rows = rows.ToArray();

        // Key-major rows: `\0k\0{treeId}\0{fullKey}\0{tag}`. The lookup scans the
        // whole per-tree band and keeps only the rows whose key segment matches,
        // so most rows are rejected on the key comparison.
        _keyRowHead = Sep + "k" + Sep + TreeId + Sep;
        _probeKey = "key-0007";
        var keyRows = new List<string>(tagCount * keysPerTag);
        for (var k = 0; k < keysPerTag; k++)
        {
            var key = "key-" + k.ToString("D4", CultureInfo.InvariantCulture);
            for (var t = 0; t < tagCount; t++)
            {
                keyRows.Add(_keyRowHead + key + Sep + "tag-" + t.ToString("D2", CultureInfo.InvariantCulture));
            }
        }

        _keyRows = keyRows.ToArray();

        // A group's accumulator shards at a sharded fanout. Half the slots hold a
        // row; the rest are absent, which is the realistic steady state and is
        // what makes the batched read's "omits absent keys" behaviour matter.
        const int fanout = 32;
        _store = new InMemoryShardStore();
        _asyncStore = new AsyncShardStore(_store);
        _slotKeys = new List<string>(fanout);
        for (var slot = 0; slot < fanout; slot++)
        {
            var slotKey = Sep + "a" + "grp" + Sep + slot.ToString(CultureInfo.InvariantCulture);
            _slotKeys.Add(slotKey);
            if (slot % 2 == 0)
            {
                _store.Seed(slotKey, BitConverter.GetBytes((long)slot));
            }
        }
    }

    // ------------------------------------------------------------------
    // (1a) Tag enumeration: one row per index entry, tag kept only on change
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the row is cut into three substrings and the tag is compared as
    /// a string, so every scanned row allocates a tag, a tree id, and a key even
    /// though the key is discarded outright and the tag is redundant for every
    /// row after the first of its run.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Tag enumeration: 3 substrings per row (baseline)")]
    public int EnumerateTags_Substrings()
    {
        string? currentTag = null;
        var emitted = false;
        var acc = 0;
        foreach (var rowKey in _rows)
        {
            if (!TryParseRow_Baseline(rowKey, out var tag, out var treeId, out _))
            {
                continue;
            }
            if (!string.Equals(tag, currentTag, StringComparison.Ordinal))
            {
                currentTag = tag;
                emitted = false;
            }
            if (emitted)
            {
                continue;
            }
            if (string.Equals(treeId, TreeId, StringComparison.Ordinal))
            {
                emitted = true;
                acc += tag.Length;
            }
        }

        return acc;
    }

    /// <summary>
    /// Optimised: the separators are located once and the tag and tree
    /// comparisons run on the row's own spans, so a run of rows sharing one tag
    /// costs a single substring for the whole run and the key is never cut.
    /// </summary>
    [Benchmark(Description = "Tag enumeration: span-located segments (optimised)")]
    public int EnumerateTags_Spans()
    {
        string? currentTag = null;
        var emitted = false;
        var acc = 0;
        foreach (var rowKey in _rows)
        {
            if (!TryLocateRow(rowKey, out var s))
            {
                continue;
            }
            if (!RowTagEquals(rowKey, s, currentTag))
            {
                currentTag = rowKey[..s.TagLength];
                emitted = false;
            }
            if (emitted)
            {
                continue;
            }
            if (RowTreeEquals(rowKey, s, TreeId))
            {
                emitted = true;
                acc += currentTag!.Length;
            }
        }

        return acc;
    }

    // ------------------------------------------------------------------
    // (1b) Reconcile scan: reject on tree and key range before materialising
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: three substrings are cut before the tree filter and the range
    /// bounds get a chance to reject the row, so a row belonging to another tree
    /// pays for a tag, a tree id, and a key that are all thrown away.
    /// </summary>
    [Benchmark(Description = "Reconcile scan: 3 substrings per row (baseline)")]
    public int Reconcile_Substrings()
    {
        var acc = 0;
        foreach (var rowKey in _rows)
        {
            if (!TryParseRow_Baseline(rowKey, out var tag, out var rowTree, out var key))
            {
                continue;
            }
            if (!string.Equals(rowTree, TreeId, StringComparison.Ordinal))
            {
                continue;
            }
            if (string.CompareOrdinal(key, "key-0000") < 0)
            {
                continue;
            }
            if (string.CompareOrdinal(key, "key-9999") >= 0)
            {
                continue;
            }
            acc += tag.Length + rowTree.Length + key.Length;
        }

        return acc;
    }

    /// <summary>
    /// Contrast (rejected halfway variant): elide the tag but still cut the tree
    /// id and key up front. It removes a third of the heap and no more, because
    /// the tree id it materialises is exactly the one the very next line proves
    /// equal to a string the caller already holds. Kept as a lane so the
    /// difference between "trim the obvious one" and "let each site cut what it
    /// keeps" is visible rather than asserted.
    /// </summary>
    [Benchmark(Description = "Reconcile scan: elide tag only (contrast)")]
    public int Reconcile_ElideTagOnly()
    {
        var acc = 0;
        foreach (var rowKey in _rows)
        {
            if (!TryLocateRow(rowKey, out var s))
            {
                continue;
            }
            var rowTree = rowKey.Substring(s.TreeStart, s.TreeLength);
            var key = rowKey[s.KeyStart..];
            if (!string.Equals(rowTree, TreeId, StringComparison.Ordinal))
            {
                continue;
            }
            if (string.CompareOrdinal(key, "key-0000") < 0)
            {
                continue;
            }
            if (string.CompareOrdinal(key, "key-9999") >= 0)
            {
                continue;
            }
            acc += s.TagLength + rowTree.Length + key.Length;
        }

        return acc;
    }

    /// <summary>
    /// Optimised: the tree filter and both range bounds are answered on the row's
    /// own spans, so only a row that survives every filter cuts its key, and the
    /// tree id it would have cut is provably the caller's own string.
    /// </summary>
    [Benchmark(Description = "Reconcile scan: span-located segments (optimised)")]
    public int Reconcile_Spans()
    {
        var acc = 0;
        foreach (var rowKey in _rows)
        {
            if (!TryLocateRow(rowKey, out var s))
            {
                continue;
            }
            if (!RowTreeEquals(rowKey, s, TreeId))
            {
                continue;
            }
            if (CompareRowKeyTo(rowKey, s, "key-0000") < 0)
            {
                continue;
            }
            if (CompareRowKeyTo(rowKey, s, "key-9999") >= 0)
            {
                continue;
            }
            var key = rowKey[s.KeyStart..];
            acc += s.TagLength + TreeId.Length + key.Length;
        }

        return acc;
    }

    // ------------------------------------------------------------------
    // (1c) Per-key tag lookup: reject on the key segment before cutting
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the row body, its full-key segment, and its trailing tag are all
    /// cut before the key comparison can reject the row.
    /// </summary>
    [Benchmark(Description = "Key-row tag lookup: 3 substrings per row (baseline)")]
    public int KeyRowTags_Substrings()
    {
        var headLen = _keyRowHead.Length;
        var acc = 0;
        foreach (var rowKey in _keyRows)
        {
            var body = rowKey[headLen..];
            var lastSep = body.LastIndexOf(Sep);
            if (lastSep < 0)
            {
                continue;
            }
            var fullKey = body[..lastSep];
            if (!string.Equals(fullKey, _probeKey, StringComparison.Ordinal))
            {
                continue;
            }
            var tag = body[(lastSep + 1)..];
            if (tag.Length > 0)
            {
                acc += tag.Length;
            }
        }

        return acc;
    }

    /// <summary>
    /// Optimised: the key comparison runs on the row's own span, so only a
    /// matching row materialises its tag and nothing else is ever cut.
    /// </summary>
    [Benchmark(Description = "Key-row tag lookup: span match (optimised)")]
    public int KeyRowTags_Spans()
    {
        var headLen = _keyRowHead.Length;
        var acc = 0;
        foreach (var rowKey in _keyRows)
        {
            var tag = TryMatchKeyRowTag(rowKey, headLen, _probeKey);
            if (tag is not null)
            {
                acc += tag.Length;
            }
        }

        return acc;
    }

    // ------------------------------------------------------------------
    // (2) Accumulator shard gather: one read per slot vs one batched read
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: one awaited store read per shard slot. Even with the round trip
    /// priced at zero (the store here is an in-memory dictionary), the serial
    /// shape still pays one <c>Task</c> and one async resumption per slot.
    /// </summary>
    [Benchmark(Description = "Accumulator gather: one read per slot (baseline)")]
    public async Task<long> ShardGather_PerSlot()
    {
        long total = 0;
        for (var i = 0; i < _slotKeys.Count; i++)
        {
            var bytes = await _store.GetAsync(_slotKeys[i], CancellationToken.None);
            if (bytes is not null && bytes.Length == sizeof(long))
            {
                total += BitConverter.ToInt64(bytes);
            }
        }

        return total;
    }

    /// <summary>
    /// Optimised: one batched read for the whole group, matching the two sibling
    /// materialisers. Summation is order-independent, so the arbitrary iteration
    /// order of the returned map is immaterial.
    /// </summary>
    [Benchmark(Description = "Accumulator gather: one batched read (optimised)")]
    public async Task<long> ShardGather_Batched()
    {
        long total = 0;
        var shards = await _store.GetManyAsync(_slotKeys, CancellationToken.None);
        foreach (var (_, bytes) in shards)
        {
            if (bytes is not null && bytes.Length == sizeof(long))
            {
                total += BitConverter.ToInt64(bytes);
            }
        }

        return total;
    }

    /// <summary>
    /// Baseline, against a store whose reads complete <b>asynchronously</b>. The
    /// production seam is an Orleans grain call, which never completes
    /// synchronously, so the pair above understates the serial shape by letting
    /// every one of its awaits take the completed-task fast path. Yielding once
    /// per read is still far cheaper than a real round trip, but it charges the
    /// resumption the serial form genuinely pays per slot.
    /// </summary>
    [Benchmark(Description = "Accumulator gather, async store: one read per slot (baseline)")]
    public async Task<long> ShardGatherAsync_PerSlot()
    {
        long total = 0;
        for (var i = 0; i < _slotKeys.Count; i++)
        {
            var bytes = await _asyncStore.GetAsync(_slotKeys[i], CancellationToken.None);
            if (bytes is not null && bytes.Length == sizeof(long))
            {
                total += BitConverter.ToInt64(bytes);
            }
        }

        return total;
    }

    /// <summary>
    /// Optimised, against the same asynchronous store: one resumption for the
    /// whole group instead of one per slot.
    /// </summary>
    [Benchmark(Description = "Accumulator gather, async store: one batched read (optimised)")]
    public async Task<long> ShardGatherAsync_Batched()
    {
        long total = 0;
        var shards = await _asyncStore.GetManyAsync(_slotKeys, CancellationToken.None);
        foreach (var (_, bytes) in shards)
        {
            if (bytes is not null && bytes.Length == sizeof(long))
            {
                total += BitConverter.ToInt64(bytes);
            }
        }

        return total;
    }

    // ------------------------------------------------------------------
    // Shells (mirroring production verbatim)
    // ------------------------------------------------------------------

    /// <summary>The pre-change parser, reproduced exactly as it shipped.</summary>
    private static bool TryParseRow_Baseline(string rowKey, out string tag, out string treeId, out string key)
    {
        tag = treeId = key = string.Empty;
        var first = rowKey.IndexOf(Sep);
        if (first < 0) return false;
        var second = rowKey.IndexOf(Sep, first + 1);
        if (second < 0) return false;
        tag = rowKey[..first];
        treeId = rowKey.Substring(first + 1, second - first - 1);
        key = rowKey[(second + 1)..];
        return tag.Length > 0;
    }

    private readonly record struct RowSegments(int TagLength, int TreeStart, int TreeLength, int KeyStart);

    private static bool TryLocateRow(string rowKey, out RowSegments segments)
    {
        segments = default;
        var first = rowKey.IndexOf(Sep);
        if (first < 0) return false;
        var second = rowKey.IndexOf(Sep, first + 1);
        if (second < 0) return false;
        if (first == 0) return false;
        segments = new RowSegments(first, first + 1, second - first - 1, second + 1);
        return true;
    }

    private static bool RowTagEquals(string rowKey, in RowSegments segments, string? other) =>
        other is not null
        && other.Length == segments.TagLength
        && rowKey.AsSpan(0, segments.TagLength).SequenceEqual(other.AsSpan());

    private static bool RowTreeEquals(string rowKey, in RowSegments segments, string other) =>
        other.Length == segments.TreeLength
        && rowKey.AsSpan(segments.TreeStart, segments.TreeLength).SequenceEqual(other.AsSpan());

    private static int CompareRowKeyTo(string rowKey, in RowSegments segments, string other) =>
        rowKey.AsSpan(segments.KeyStart).CompareTo(other.AsSpan(), StringComparison.Ordinal);

    private static string? TryMatchKeyRowTag(string rowKey, int headLen, string key)
    {
        var body = rowKey.AsSpan(headLen);
        var lastSep = body.LastIndexOf(Sep);
        if (lastSep < 0)
        {
            return null;
        }
        if (!body[..lastSep].SequenceEqual(key.AsSpan()))
        {
            return null;
        }
        var tag = body[(lastSep + 1)..];
        return tag.Length > 0 ? new string(tag) : null;
    }

    /// <summary>
    /// The narrowest stand-in for the aggregation view store: the two read shapes
    /// the gather lanes contrast, over a plain dictionary. Both return completed
    /// tasks, so the lanes charge the async machinery and nothing else - a real
    /// store's round trip would only widen the gap.
    /// </summary>
    private sealed class InMemoryShardStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);

        public void Seed(string key, byte[] value) => _map[key] = value;

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken)
            => Task.FromResult(_map.TryGetValue(key, out var v) ? v : null);

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken)
        {
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_map.TryGetValue(key, out var v))
                {
                    result[key] = v;
                }
            }

            return Task.FromResult(result);
        }
    }

    /// <summary>
    /// The same store with genuinely asynchronous completions. An Orleans grain
    /// call never completes synchronously, so this is the shape the production
    /// seam actually has; a single yield per read is still orders of magnitude
    /// cheaper than a real round trip, which makes these lanes a floor on the
    /// saving rather than an estimate of it.
    /// </summary>
    private sealed class AsyncShardStore(InMemoryShardStore inner)
    {
        public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetAsync(key, cancellationToken);
        }

        public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetManyAsync(keys, cancellationToken);
        }
    }
}
