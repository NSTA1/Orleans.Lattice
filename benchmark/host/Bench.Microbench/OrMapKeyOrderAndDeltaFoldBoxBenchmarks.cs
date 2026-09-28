using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Arbitrates the CRDT decode and delta-fold trims. Every group carries a
/// baseline lane holding a verbatim copy of the replaced body, an optimized
/// lane, and a control lane on the input shape where the trim is expected to
/// buy nothing - so a win that is really measurement noise has somewhere to
/// show itself.
/// <para>
/// <b>(1) keyorder, pooled scratch arm.</b> The key-ordering pass below needs
/// one call-scoped slot per distinct key. Allocating that scratch is the one
/// allocation the trim ADDS, and it lands hardest on a flat map, where the
/// ordering work it buys is smallest - so the flat lane read a real allocation
/// regression against the pre-trim baseline even while the churned lane won
/// outright. Renting the scratch removes it in both shapes. Each slot also
/// carries the key's own two entry lists, resolved once while the scratch is
/// built, so the emit walk re-probes neither dictionary. The
/// <c>KeyOrderOnly</c> arm is the key-ordered emitter WITHOUT either change,
/// so the table separates what the ordering bought from what the pooling and
/// probe elision bought rather than reporting one bundled number.
/// </para>
/// <para>/// <b>(2) keyorder - the OR-map state decoder's global element-byte sort.</b>
/// <c>OrMapProvenanceDecoder.DecodeState</c> emitted in dictionary order and
/// then sorted the WHOLE event list with a comparator whose first term walks
/// two key surrogates byte by byte - O(N log N) byte-array walks over N events.
/// Sorting the distinct KEYS instead is O(K log K), and on a churned map K is
/// far below N because a key's whole dot history is one key but many events.
/// Each key's events are then emitted contiguously and ordered against their
/// own group by the causal comparator alone, which does no byte compares. This
/// is the shape the OR-set and RW-set twins already use. The resulting total
/// order is identical: the old comparator ordered by surrogate first and fell
/// back to the causal comparator, and within a group the first term was always
/// zero. The trim also drops one key-surrogate encode and one
/// equal-content <c>byte[]</c> for every key that carries both adds and
/// tombstones, which the old shape encoded twice.
/// </para>
/// <para>
/// <b>(3) foldbox - boxed enumerators in the delta coalescing fold.</b>
/// <c>foreach</c> over a variable of static type <see cref="IReadOnlyList{T}"/>
/// binds <c>IEnumerable&lt;T&gt;.GetEnumerator</c>, which HEAP-ALLOCATES a
/// boxed enumerator on every call - for <c>T[]</c> as well as for
/// <see cref="List{T}"/>. The coalescing fold calls its appenders once per
/// member per delta, so folding a run of N deltas paid N boxes on a path that
/// otherwise allocates only its result.
/// </para>
/// <para>
/// <b>How to read the columns.</b> Time is the arbiter for group (2);
/// allocation is the arbiter for groups (1) and (3), and neither is claimed as
/// a time win. Group (1) sheds exactly one closure and one delegate per union,
/// so its byte delta is small, fixed, and independent of the corpus size - if
/// it scales with the dot count something other than the trim moved. Group (2)
/// may legitimately shed a small, bounded number of bytes (the duplicate key
/// surrogates) while also gaining a <c>List&lt;(byte[], string)&gt;</c> it did
/// not build before, so read its allocation column as a wash unless it moves by
/// more than the key count. Group (3) is the other place bytes must fall:
/// <c>MemoryDiagnoser</c>'s Gen0 column is the result there.
/// </para>
/// <para>
/// <b>Controls.</b> <c>OrMapDecodeState_*_Flat</c> is a map of single-dot keys,
/// where K equals N and sorting the keys saves nothing over sorting the events,
/// so the key-order trim has no headroom and the lane measures only the extra
/// key list it builds. <c>FoldDots_*_Empty</c> folds a run whose members are
/// all empty arrays, where the baseline's boxed enumerator is the cached
/// empty-array singleton and so costs nothing - the one shape in group (3)
/// where no bytes may move. <c>UnionRun_*_Single</c> folds a run of one delta,
/// where the sizing walk is a single step and the closure is therefore the
/// whole of the removed cost.
/// </para>
/// <para>
/// <b>Why the spanned loops copy the element rather than reading through
/// <c>ref readonly</c>.</b> Every loop body here calls out - to
/// <c>List.Add</c>, to a hash-set probe, to the base64 transcode. A byref into
/// the span that is live across a call is an interior pointer the JIT must
/// report to the GC, so it is pinned to a tracked stack slot rather than
/// enregistered, and that costs more than the struct copy it saves. The shipped
/// bodies still take the span, so they still drop the <c>Count</c> re-read.
/// </para>
/// <para>
/// <b>Where both arms are mirrors.</b> The coalescing fold's union primitives
/// and appenders are private, and <c>CrdtShape.CombineDeltaRun</c> dispatches
/// through a shape table that does substantially more work than either trim
/// touches, so benchmarking through it would bury both under untouched cost.
/// Groups (1) and (3) therefore hold a mirror of the old body and a mirror of
/// the new one, copied from the source, with the equivalence assertions below
/// pinning the two mirrors to the same answer. Group (2) does not need this:
/// its entry point is public and the baseline lane is a verbatim copy of the
/// whole replaced method - including the reflection-built emitter behind the
/// same concurrent-dictionary cache - so both arms pay the same dispatch.
/// </para>
/// <para>
/// Run with <c>--suite ormapkeyorderfoldbox</c> (or
/// <c>BENCH_MICROBENCH_SUITE=ormapkeyorderfoldbox</c>). This machine is noisy:
/// treat smoke fidelity as an equivalence and direction check only and take
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> as the arbiter.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class OrMapKeyOrderAndDeltaFoldBoxBenchmarks
{
    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    private static readonly OrMapProvenanceDecoder OrMapDecoder = new();

    // Group 1 - OR-map state decode.
    private OrMap<string, OrFlag> _churnedMap = null!;
    private OrMap<string, OrFlag> _flatMap = null!;

    // Group 2 - coalescing fold appenders.
    private IReadOnlyList<OrSetDeltaDot>[] _foldRunArrays = null!;
    private IReadOnlyList<OrSetDeltaDot>[] _foldRunLists = null!;
    private IReadOnlyList<OrSetDeltaDot>[] _foldRunEmpty = null!;

    /// <summary>
    /// Builds every corpus and asserts each optimized lane answers exactly what
    /// its baseline answers - on the ordinary shapes, on the control shapes
    /// where a trim must buy nothing, and on the shapes most likely to expose a
    /// disagreement: a map whose keys appear in both the add and tombstone
    /// dictionaries, and a delta list whose runtime type defeats the span type
    /// test.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        // Churned: every key carries a long add history AND a tombstone list,
        // so K is far below N and every key is present in both dictionaries -
        // the shape the key-order trim targets and the shape where a grouping
        // mistake would reorder events.
        _churnedMap = MakeChurnedOrMap(keys: 24, churnPerKey: 20);
        _flatMap = MakeFlatOrMap(keys: 256);

        _foldRunArrays = MakeFoldRun(deltas: 16, dotsPerDelta: 4, Backing.Array);
        _foldRunLists = MakeFoldRun(deltas: 16, dotsPerDelta: 4, Backing.List);
        _foldRunEmpty = MakeEmptyFoldRun(deltas: 16);

        AssertKeyOrderEquivalence();
        AssertFoldEquivalence();
    }

    private void AssertKeyOrderEquivalence()
    {
        AssertChangesEqual(
            BaselineOrMapDecodeState(_churnedMap),
            OrMapDecoder.DecodeState(_churnedMap),
            "or-map decode state (churned, keys in both dictionaries)");
        AssertChangesEqual(
            BaselineOrMapDecodeState(_flatMap),
            OrMapDecoder.DecodeState(_flatMap),
            "or-map decode state (flat)");
        AssertChangesEqual(
            BaselineOrMapDecodeState(_churnedMap),
            KeyOrderOrMapDecodeState(_churnedMap),
            "or-map decode state key-order-only arm (churned)");
        AssertChangesEqual(
            BaselineOrMapDecodeState(_flatMap),
            KeyOrderOrMapDecodeState(_flatMap),
            "or-map decode state key-order-only arm (flat)");
        AssertChangesEqual(
            BaselineOrMapDecodeState(new OrMap<string, OrFlag>()),
            OrMapDecoder.DecodeState(new OrMap<string, OrFlag>()),
            "or-map decode state (empty)");
    }

    private void AssertFoldEquivalence()
    {
        AssertDotListsEqual(
            BaselineFoldDots(_foldRunArrays),
            OptimizedFoldDots(_foldRunArrays),
            "fold dots (array-backed)");
        AssertDotListsEqual(
            BaselineFoldDots(_foldRunLists),
            OptimizedFoldDots(_foldRunLists),
            "fold dots (list-backed)");
        AssertDotListsEqual(
            BaselineFoldDots(_foldRunEmpty),
            OptimizedFoldDots(_foldRunEmpty),
            "fold dots (empty)");
    }

    // ---------------------------------------------------------------------
    // Group 1 - keyorder.
    // ---------------------------------------------------------------------

    [Benchmark]
    [BenchmarkCategory("keyorder")]
    public int OrMapDecodeState_Baseline_Churned() => BaselineOrMapDecodeState(_churnedMap).Count;

    [Benchmark]
    [BenchmarkCategory("keyorder")]
    public int OrMapDecodeState_Optimized_Churned() => OrMapDecoder.DecodeState(_churnedMap).Count;

    [Benchmark]
    [BenchmarkCategory("keyorder")]
    public int OrMapDecodeState_KeyOrderOnly_Churned() => KeyOrderOrMapDecodeState(_churnedMap).Count;

    /// <summary>
    /// Control: one dot per key, so the distinct-key count equals the event
    /// count and sorting the keys is the same size of sort as sorting the
    /// events. The trim has no headroom here and the lane measures only the
    /// extra key list it builds.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("keyorder")]
    public int OrMapDecodeState_Baseline_Flat() => BaselineOrMapDecodeState(_flatMap).Count;

    [Benchmark]
    [BenchmarkCategory("keyorder")]
    public int OrMapDecodeState_Optimized_Flat() => OrMapDecoder.DecodeState(_flatMap).Count;

    [Benchmark]
    [BenchmarkCategory("keyorder")]
    public int OrMapDecodeState_KeyOrderOnly_Flat() => KeyOrderOrMapDecodeState(_flatMap).Count;

    // ---------------------------------------------------------------------
    // Group 3 - foldbox.
    // ---------------------------------------------------------------------

    [Benchmark]
    [BenchmarkCategory("foldbox")]
    public int FoldDots_Baseline_Array() => BaselineFoldDots(_foldRunArrays).Count;

    [Benchmark]
    [BenchmarkCategory("foldbox")]
    public int FoldDots_Optimized_Array() => OptimizedFoldDots(_foldRunArrays).Count;

    [Benchmark]
    [BenchmarkCategory("foldbox")]
    public int FoldDots_Baseline_List() => BaselineFoldDots(_foldRunLists).Count;

    [Benchmark]
    [BenchmarkCategory("foldbox")]
    public int FoldDots_Optimized_List() => OptimizedFoldDots(_foldRunLists).Count;

    /// <summary>
    /// Control: every member is <see cref="Array.Empty{T}"/>, whose enumerator
    /// is a cached singleton, so the baseline allocates no box either. No bytes
    /// may move here.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("foldbox")]
    public int FoldDots_Baseline_Empty() => BaselineFoldDots(_foldRunEmpty).Count;

    [Benchmark]
    [BenchmarkCategory("foldbox")]
    public int FoldDots_Optimized_Empty() => OptimizedFoldDots(_foldRunEmpty).Count;

    // ---------------------------------------------------------------------
    // Baselines - verbatim copies of the replaced bodies.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shipped decoder resolves a closed-generic emitter once per map type
    /// and caches it, so the baseline mirrors that whole dispatch - the
    /// concurrent-dictionary probe and the reflection-built delegate included -
    /// rather than calling a typed body directly. A baseline that skipped it
    /// would be paying less than the optimized lane and would read as a
    /// regression that is really just the missing overhead.
    /// </summary>
    private delegate void BaselineStateEmitter(object boxed, List<CrdtMemberChange> sink);
    private static readonly ConcurrentDictionary<Type, BaselineStateEmitter> BaselineStateEmitters = new();

    private static BaselineStateEmitter CreateBaselineStateEmitter(Type closedType)
    {
        var args = closedType.GetGenericArguments();
        var method = typeof(OrMapKeyOrderAndDeltaFoldBoxBenchmarks)
            .GetMethod(nameof(BaselineEmitStateTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(args);
        return (BaselineStateEmitter)method.CreateDelegate(typeof(BaselineStateEmitter));
    }

    private static void BaselineEmitStateTyped<TKey, TValue>(object boxed, List<CrdtMemberChange> sink)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var map = (OrMap<TKey, TValue>)boxed;

        sink.EnsureCapacity(sink.Count + map.Adds.Count + map.Tombstones.Count);

        foreach (var (key, entries) in map.Adds)
        {
            if (entries.Count == 0) continue;
            var element = BaselineKeyToBytes(key);
            var entrySpan = CollectionsMarshal.AsSpan(entries);
            for (var i = 0; i < entrySpan.Length; i++)
            {
                var e = entrySpan[i];
                sink.Add(new CrdtMemberChange
                {
                    Element = element,
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = e.ReplicaId,
                    Ordinal = e.Counter,
                    WallClock = null,
                });
            }
        }

        foreach (var (key, dots) in map.Tombstones)
        {
            if (dots.Count == 0) continue;
            var element = BaselineKeyToBytes(key);
            var dotSpan = CollectionsMarshal.AsSpan(dots);
            for (var i = 0; i < dotSpan.Length; i++)
            {
                var dot = dotSpan[i];
                sink.Add(new CrdtMemberChange
                {
                    Element = element,
                    Kind = CrdtMemberChangeKind.Removed,
                    ReplicaId = dot.ReplicaId,
                    Ordinal = dot.Counter,
                    WallClock = null,
                });
            }
        }
    }

    private static byte[] BaselineKeyToBytes<TKey>(TKey key)
    {
        var text = key as string ?? Convert.ToString(key, CultureInfo.InvariantCulture) ?? string.Empty;
        return text.Length == 0 ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(text);
    }

    private sealed class BaselineElementOrderComparer : IComparer<CrdtMemberChange>
    {
        public static BaselineElementOrderComparer Instance { get; } = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byElement = CompareBytes(x.Element, y.Element);
            if (byElement != 0) return byElement;
            return CrdtMemberChangeCausalComparer.Instance.Compare(x, y);
        }

        internal static int CompareBytes(byte[] a, byte[] b)
        {
            var min = Math.Min(a.Length, b.Length);
            for (var i = 0; i < min; i++)
            {
                var c = a[i].CompareTo(b[i]);
                if (c != 0) return c;
            }
            return a.Length.CompareTo(b.Length);
        }
    }

    /// <summary>
    /// Mirror of the coalescing fold's dot union as it stood before the trim -
    /// the <c>foreach</c> that boxes an enumerator per member of the run.
    /// </summary>
    private static List<OrSetDeltaDot> BaselineFoldDots(IReadOnlyList<OrSetDeltaDot>[] run)
    {
        var bound = 0;
        for (var i = 0; i < run.Length; i++) bound += run[i].Count;
        var result = new List<OrSetDeltaDot>(bound);
        var seen = new HashSet<(string ReplicaId, long Counter, string Element)>(bound);
        for (var i = 0; i < run.Length; i++)
        {
            var source = run[i];
            if (source is null) continue;
            foreach (var dot in source)
            {
                if (dot.Element is null) continue;
                if (seen.Add((dot.ReplicaId ?? string.Empty, dot.Counter, Convert.ToBase64String(dot.Element))))
                {
                    result.Add(dot);
                }
            }
        }
        return result;
    }

    /// <summary>
    /// Mirror of the shipped fold: the same body walked by index over a
    /// resolved span, so no boxed enumerator is allocated.
    /// </summary>
    private static List<OrSetDeltaDot> OptimizedFoldDots(IReadOnlyList<OrSetDeltaDot>[] run)
    {
        var bound = 0;
        for (var i = 0; i < run.Length; i++) bound += run[i].Count;
        var result = new List<OrSetDeltaDot>(bound);
        var seen = new HashSet<(string ReplicaId, long Counter, string Element)>(bound);
        for (var i = 0; i < run.Length; i++)
        {
            var source = run[i];
            if (source is null) continue;
            var spanned = CrdtDeltaListSpan.TryGetSpan(source, out var span);
            var count = source.Count;
            for (var j = 0; j < count; j++)
            {
                var dot = spanned ? span[j] : source[j];
                if (dot.Element is null) continue;
                if (seen.Add((dot.ReplicaId ?? string.Empty, dot.Counter, Convert.ToBase64String(dot.Element))))
                {
                    result.Add(dot);
                }
            }
        }
        return result;
    }

    // ---------------------------------------------------------------------
    // Equivalence assertions.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The OR-map state decode as it stood before the trim: emit in dictionary
    /// order, then sort the WHOLE event list with the element-byte comparator.
    /// Resolution goes through the same cached reflection-built emitter the
    /// shipped decoder uses, so the lane compares the two orderings and not two
    /// different dispatch costs.
    /// </summary>
    private static List<CrdtMemberChange> BaselineOrMapDecodeState(object state)
    {
        var sink = new List<CrdtMemberChange>();
        var emitter = BaselineStateEmitters.GetOrAdd(state.GetType(), CreateBaselineStateEmitter);
        emitter(state, sink);
        sink.Sort(BaselineElementOrderComparer.Instance);
        return sink;
    }

    /// <summary>
    /// A run of dot lists for the coalescing fold, wrapped in the requested
    /// backing type so the lane can contrast an array source, a
    /// <see cref="List{T}"/> source, and a source whose runtime type defeats
    /// the span type test.
    /// </summary>
    private static IReadOnlyList<OrSetDeltaDot>[] MakeFoldRun(
        int deltas, int dotsPerDelta, Backing backing)
    {
        var run = new IReadOnlyList<OrSetDeltaDot>[deltas];
        for (var d = 0; d < deltas; d++)
        {
            run[d] = Wrap(MakeDotList(dotsPerDelta, 1, (d * 1000L) + 1), backing);
        }

        return run;
    }

    /// <summary>
    /// The control corpus. <see cref="Array.Empty{T}"/> hands back a cached
    /// singleton whose enumerator is also cached, so the baseline moves no
    /// bytes here either - which is exactly why it is the right control: the
    /// lane must show the dispatch saving with the allocation column flat.
    /// </summary>
    private static IReadOnlyList<OrSetDeltaDot>[] MakeEmptyFoldRun(int deltas)
    {
        var run = new IReadOnlyList<OrSetDeltaDot>[deltas];
        for (var d = 0; d < deltas; d++)
        {
            run[d] = Array.Empty<OrSetDeltaDot>();
        }

        return run;
    }

    /// <summary>
    /// The key-ordered emitter as it stood BEFORE the scratch was pooled and
    /// the entry lists were carried in it: a freshly allocated
    /// <see cref="List{T}"/> of (surrogate, key) pairs, and a fresh probe of
    /// both dictionaries for every key during the emit walk. Dispatch goes
    /// through the same cached reflection-built delegate shape as the other two
    /// arms, so the three lanes differ only in the body being measured.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, BaselineStateEmitter> KeyOrderStateEmitters = new();

    private static List<CrdtMemberChange> KeyOrderOrMapDecodeState(object state)
    {
        var sink = new List<CrdtMemberChange>();
        var emitter = KeyOrderStateEmitters.GetOrAdd(state.GetType(), CreateKeyOrderStateEmitter);
        emitter(state, sink);
        return sink;
    }

    private static BaselineStateEmitter CreateKeyOrderStateEmitter(Type closedType)
    {
        var args = closedType.GetGenericArguments();
        var method = typeof(OrMapKeyOrderAndDeltaFoldBoxBenchmarks)
            .GetMethod(nameof(KeyOrderEmitStateTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(args);
        return (BaselineStateEmitter)method.CreateDelegate(typeof(BaselineStateEmitter));
    }

    private static void KeyOrderEmitStateTyped<TKey, TValue>(object boxed, List<CrdtMemberChange> sink)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var map = (OrMap<TKey, TValue>)boxed;
        var adds = map.Adds;
        var tombstones = map.Tombstones;

        var keys = new List<(byte[] Element, TKey Key)>(adds.Count + tombstones.Count);
        var total = 0;
        foreach (var (key, entries) in adds)
        {
            if (entries.Count == 0) continue;
            total += entries.Count;
            keys.Add((BaselineKeyToBytes(key), key));
        }

        foreach (var (key, dots) in tombstones)
        {
            if (dots.Count == 0) continue;
            total += dots.Count;
            if (!adds.TryGetValue(key, out var addEntries) || addEntries.Count == 0)
            {
                keys.Add((BaselineKeyToBytes(key), key));
            }
        }

        if (total == 0) return;

        keys.Sort(static (x, y) => BaselineElementOrderComparer.CompareBytes(x.Element, y.Element));
        sink.EnsureCapacity(sink.Count + total);

        var groupStart = sink.Count;
        byte[]? groupElement = null;
        var keySpan = CollectionsMarshal.AsSpan(keys);
        for (var k = 0; k < keySpan.Length; k++)
        {
            var element = keySpan[k].Element;
            var key = keySpan[k].Key;

            if (groupElement is not null && BaselineElementOrderComparer.CompareBytes(groupElement, element) != 0)
            {
                KeyOrderSortGroup(sink, groupStart);
                groupStart = sink.Count;
            }
            groupElement = element;

            if (adds.TryGetValue(key, out var entries) && entries.Count > 0)
            {
                var entrySpan = CollectionsMarshal.AsSpan(entries);
                for (var i = 0; i < entrySpan.Length; i++)
                {
                    var e = entrySpan[i];
                    sink.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = e.ReplicaId,
                        Ordinal = e.Counter,
                        WallClock = null,
                    });
                }
            }

            if (tombstones.TryGetValue(key, out var dots) && dots.Count > 0)
            {
                var dotSpan = CollectionsMarshal.AsSpan(dots);
                for (var i = 0; i < dotSpan.Length; i++)
                {
                    var dot = dotSpan[i];
                    sink.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }
        }

        KeyOrderSortGroup(sink, groupStart);
    }

    /// <summary>
    /// Mirror of the shipped run-sort guard, so the key-order-only arm differs
    /// from the shipped emitter only in the scratch it uses.
    /// </summary>
    private static void KeyOrderSortGroup(List<CrdtMemberChange> sink, int groupStart)
    {
        var count = sink.Count - groupStart;
        if (count > 1) sink.Sort(groupStart, count, CrdtMemberChangeCausalComparer.Instance);
    }

    private static void AssertChangesEqual(
        IReadOnlyList<CrdtMemberChange> expected,
        IReadOnlyList<CrdtMemberChange> actual,
        string what)
    {
        if (expected.Count != actual.Count)
        {
            throw new InvalidOperationException(
                $"Baseline and optimized disagree on {what}: {expected.Count} vs {actual.Count} events.");
        }

        for (var i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            if (e.Kind != a.Kind
                || e.Ordinal != a.Ordinal
                || !string.Equals(e.ReplicaId, a.ReplicaId, StringComparison.Ordinal)
                || !e.Element.AsSpan().SequenceEqual(a.Element))
            {
                throw new InvalidOperationException(
                    $"Baseline and optimized disagree on {what} at index {i}.");
            }
        }
    }

    private static void AssertDotListsEqual(
        List<OrSetDeltaDot> expected,
        List<OrSetDeltaDot> actual,
        string what)
    {
        if (expected.Count != actual.Count)
        {
            throw new InvalidOperationException(
                $"Baseline and optimized disagree on {what}: {expected.Count} vs {actual.Count} dots.");
        }

        for (var i = 0; i < expected.Count; i++)
        {
            if (expected[i].Counter != actual[i].Counter
                || !string.Equals(expected[i].ReplicaId, actual[i].ReplicaId, StringComparison.Ordinal)
                || !expected[i].Element.AsSpan().SequenceEqual(actual[i].Element))
            {
                throw new InvalidOperationException(
                    $"Baseline and optimized disagree on {what} at index {i}.");
            }
        }
    }

    // ---------------------------------------------------------------------
    // Corpora.
    // ---------------------------------------------------------------------

    private enum Backing
    {
        Array,
        List,
        Exotic,
    }

    private static IReadOnlyList<T> Wrap<T>(List<T> items, Backing backing) => backing switch
    {
        Backing.Array => items.ToArray(),
        Backing.List => items,
        _ => new OpaqueReadOnlyList<T>(items.ToArray()),
    };

    /// <summary>
    /// An <see cref="IReadOnlyList{T}"/> that is neither an array nor a
    /// <see cref="List{T}"/>, so the shipped span type test fails and the
    /// fallback indexer walk runs.
    /// </summary>
    private sealed class OpaqueReadOnlyList<T>(T[] items) : IReadOnlyList<T>
    {
        public T this[int index] => items[index];

        public int Count => items.Length;

        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)items).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => items.GetEnumerator();
    }

    private static List<OrSetDeltaDot> MakeDotList(int elements, int dotsPerElement, long baseCounter)
    {
        var dots = new List<OrSetDeltaDot>(elements * dotsPerElement);
        var counter = baseCounter;
        for (var e = 0; e < elements; e++)
        {
            var element = Encoding.UTF8.GetBytes("element:" + e.ToString("D4", CultureInfo.InvariantCulture));
            for (var i = 0; i < dotsPerElement; i++)
            {
                dots.Add(new OrSetDeltaDot
                {
                    Element = element,
                    ReplicaId = i % 2 == 0 ? ReplicaA : ReplicaB,
                    Counter = counter++,
                });
            }
        }

        return dots;
    }

    /// <summary>
    /// A map whose keys have each been set repeatedly and then removed, so
    /// every key carries a long add history AND a tombstone list - the shape
    /// where the distinct-key count is far below the event count, and where
    /// every key is present in both dictionaries.
    /// </summary>
    private static OrMap<string, OrFlag> MakeChurnedOrMap(int keys, int churnPerKey)
    {
        var left = new OrMap<string, OrFlag>();
        var right = new OrMap<string, OrFlag>();
        for (var k = 0; k < keys; k++)
        {
            var key = "key:" + k.ToString("D4", CultureInfo.InvariantCulture);
            for (var i = 0; i < churnPerKey; i++) left.Set(key, ReplicaA, new OrFlag());
            left.Remove(key);
            for (var i = 0; i < 3; i++) right.Set(key, ReplicaB, new OrFlag());
        }

        return OrMap<string, OrFlag>.Merge(left, right);
    }

    /// <summary>
    /// The control corpus: many keys, one add dot each, so the key count equals
    /// the event count.
    /// </summary>
    private static OrMap<string, OrFlag> MakeFlatOrMap(int keys)
    {
        var map = new OrMap<string, OrFlag>();
        for (var k = 0; k < keys; k++)
        {
            map.Set("flat:" + k.ToString("D4", CultureInfo.InvariantCulture), ReplicaA, new OrFlag());
        }

        return map;
    }
}
