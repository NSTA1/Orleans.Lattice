using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates one mechanism - a <b>pooled scratch key window</b> - harvested at
/// three seams that each built a transient key collection purely to sort it and
/// then threw it away. The collection never escapes its call, so it is scratch
/// by construction and can be rented rather than allocated.
/// <para>
/// (1) <b>The CRDT set <c>DecodeState</c> union window.</b>
/// <c>OrSetProvenanceDecoder.DecodeState</c> and its RW-set twin need their
/// elements emitted in ordinal key order, and got there by unioning the add and
/// tombstone (respectively remove) key sets into a fresh
/// <c>List&lt;string&gt;</c>, sorting it, and then <b>re-probing the adds map
/// once per key</b> in the emit loop for a value the union pass had already
/// held in hand. Collecting <see cref="KeyValuePair{TKey,TValue}"/> into a
/// <b>pooled</b> window instead keeps the key beside its add-dot list, so the
/// emit walk reads the list straight off the pair and the per-element string
/// hash, bucket walk and confirming ordinal compare disappear with the list
/// allocation.
/// </para>
/// <para>
/// (2) <b>The CRDT set <c>DecodeCurrentValue</c> add window.</b> The same shape
/// without the union: the live-member projection copied <c>adds.Keys</c> into a
/// list, sorted it, and then re-probed <c>adds[key]</c> per element. The window
/// here carries a non-nullable list, because every key in it came from the adds
/// map.
/// </para>
/// <para>
/// (3) <b>The atomic-commit key fingerprints.</b>
/// <c>AtomicWriteGrain.ComputeKeyFingerprint</c> allocated a
/// <c>string[]</c> per prepared batch, and
/// <c>LatticeCrossTreeTxGrain.ComputeFingerprint</c> allocated one <b>per
/// participant</b> - so a saga spanning N trees paid N transient arrays. Both
/// windows feed an <c>IncrementalHash</c> and are dead on return. The
/// cross-tree site additionally hoists a <b>single</b> rental, sized to the
/// widest participant, out of the loop.
/// </para>
/// <para>
/// <b>What this adds over the existing suites (rule 19).</b> Group (2)'s
/// isolated shape - sorted keys plus a dictionary re-probe, against a sorted
/// pair window - is already measured by
/// <see cref="CrdtProvenanceDecodeWalkTrimBenchmarks"/> group (1) on
/// <c>VersionVectorProvenanceDecoder</c>, so this suite deliberately does
/// <b>not</b> re-measure it in isolation and carries the in-situ pair instead,
/// which is what says whether the shape is worth anything where the OR-set and
/// RW-set decoders actually spend their time. Group (1) adds the union-dedup
/// variant of that shape, which no suite has measured, and group (3) is a site
/// nothing has benchmarked at all.
/// </para>
/// <para>
/// <b>Baseline fidelity (rule 4).</b> Every <c>*Baseline</c> lane is a verbatim
/// copy of the body it replaces, loop form included, taken from the commit this
/// branch forked from. Where the replaced body called a helper this change did
/// <b>not</b> touch - <c>ContainsExact</c>, <c>IsTombstoned</c>,
/// <c>SingleReplica</c>, <c>HasLiveRemove</c>, <c>AppendLengthPrefixed</c>,
/// <c>ComputeKeyFingerprintCore</c> - the baseline calls the <b>shipped</b>
/// helper rather than a copy of it, so the pair differs only in the key window.
/// <c>OrdinalStringOrder.Comparison</c> is likewise reached through
/// <c>InternalsVisibleTo</c> rather than re-spelled, so the sort order is
/// provably the same one. <c>[GlobalSetup]</c> asserts every pair agrees before
/// a single timing is taken (rule 2), so a baseline that drifted fails the run
/// rather than reporting a win.
/// </para>
/// <para>
/// <b>How to read it (rule 3).</b> This is primarily an <b>allocation</b> trim:
/// every lane pair should show the optimised side dropping the window's bytes,
/// and that saving <b>scales with <see cref="Width"/></b> rather than being
/// flat (rule 20). Time is a secondary and smaller effect, real only where the
/// re-probe is also removed - groups (1) and (2) - and expected to be close to
/// flat in group (3), where only the allocation goes. Read the <c>Isolated</c>
/// pairs for the size of the trim and the in-situ decode pairs for what it is
/// worth where it runs, because a <c>DecodeState</c> arm is dominated by the
/// per-element <c>Convert.FromBase64String</c> and <c>CrdtMemberChange</c>
/// construction this change does not touch (rules 6 and 26).
/// </para>
/// <para>
/// <b>Controls (rule 5).</b> <c>Width</c> includes <c>4</c>, below which a
/// rental's fixed cost is not obviously repaid by a four-slot array; a lane
/// pair that flips sign between <c>4</c> and <c>512</c> is below the
/// measurement floor and must not be claimed (rule 25). The cross-tree group
/// additionally carries a <c>SingleParticipant</c> control, which is the case
/// where hoisting the rental out of the loop can save nothing because the loop
/// runs once.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=scratchkeywindowtrims</c> (or
/// <c>--suite scratchkeywindowtrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ScratchKeyWindowTrimBenchmarks
{
    /// <summary>
    /// Mirrors the private <c>OrSetProvenanceDecoder.DotIndexThreshold</c>, so
    /// the churned fixtures below cross it and exercise the same branch the
    /// shipped decoder takes.
    /// </summary>
    private const int DotIndexThreshold = 8;

    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    /// <summary>
    /// Elements in the decoded set, or key-value entries in the fingerprinted
    /// batch. The window's allocation saving is one array of this length, so it
    /// must scale; <c>4</c> is the narrow control where a rental's fixed cost
    /// is largest relative to the array it replaces.
    /// </summary>
    [Params(4, 64, 512)]
    public int Width { get; set; }

    private OrSet _orSet = null!;
    private RwSet _rwSet = null!;

    private List<KeyValuePair<string, byte[]>> _batch = null!;
    private List<CrossTreeParticipant> _participants = null!;
    private List<CrossTreeParticipant> _singleParticipant = null!;

    /// <summary>Builds the fixtures each lane drives, and asserts every pair agrees.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _orSet = BuildOrSet(Width);
        _rwSet = BuildRwSet(Width);
        _batch = BuildBatch(Width);
        _participants = BuildParticipants(Width, participantCount: 4);
        _singleParticipant = BuildParticipants(Width, participantCount: 1);

        // (1) State decode.
        AssertChanges(
            BaselineOrSetDecodeState(_orSet),
            OrSetProvenanceDecoder.Instance.DecodeState(_orSet),
            "OrSet DecodeState");
        AssertChanges(
            BaselineRwSetDecodeState(_rwSet),
            RwSetProvenanceDecoder.Instance.DecodeState(_rwSet),
            "RwSet DecodeState");
        AssertChanges(
            BaselineOrSetDecodeState(new OrSet()),
            OrSetProvenanceDecoder.Instance.DecodeState(new OrSet()),
            "OrSet DecodeState (empty)");
        AssertChanges(
            BaselineRwSetDecodeState(new RwSet()),
            RwSetProvenanceDecoder.Instance.DecodeState(new RwSet()),
            "RwSet DecodeState (empty)");

        // (2) Current-value decode.
        AssertValues(
            BaselineOrSetDecodeCurrentValue(_orSet),
            OrSetProvenanceDecoder.Instance.DecodeCurrentValue(_orSet),
            "OrSet DecodeCurrentValue");
        AssertValues(
            BaselineRwSetDecodeCurrentValue(_rwSet),
            RwSetProvenanceDecoder.Instance.DecodeCurrentValue(_rwSet),
            "RwSet DecodeCurrentValue");
        AssertValues(
            BaselineOrSetDecodeCurrentValue(new OrSet()),
            OrSetProvenanceDecoder.Instance.DecodeCurrentValue(new OrSet()),
            "OrSet DecodeCurrentValue (empty)");
        AssertValues(
            BaselineRwSetDecodeCurrentValue(new RwSet()),
            RwSetProvenanceDecoder.Instance.DecodeCurrentValue(new RwSet()),
            "RwSet DecodeCurrentValue (empty)");

        // (1) isolated: the union window alone, which is the hunk itself.
        if (BaselineOrSetUnionWindow(_orSet) != OptimizedOrSetUnionWindow(_orSet))
            throw new InvalidOperationException("OrSet union window checksum disagrees with its baseline.");
        if (BaselineRwSetUnionWindow(_rwSet) != OptimizedRwSetUnionWindow(_rwSet))
            throw new InvalidOperationException("RwSet union window checksum disagrees with its baseline.");

        // (3) Fingerprints.
        AssertDigest(
            BaselineComputeKeyFingerprint(_batch),
            AtomicWriteGrain.ComputeKeyFingerprint(_batch),
            "AtomicWriteGrain.ComputeKeyFingerprint");
        AssertDigest(
            BaselineComputeKeyFingerprint([]),
            AtomicWriteGrain.ComputeKeyFingerprint([]),
            "AtomicWriteGrain.ComputeKeyFingerprint (empty)");
        AssertDigest(
            BaselineCrossTreeFingerprint(_participants),
            LatticeCrossTreeTxGrain.ComputeFingerprint(_participants),
            "LatticeCrossTreeTxGrain.ComputeFingerprint");
        AssertDigest(
            BaselineCrossTreeFingerprint(_singleParticipant),
            LatticeCrossTreeTxGrain.ComputeFingerprint(_singleParticipant),
            "LatticeCrossTreeTxGrain.ComputeFingerprint (single participant)");
        AssertDigest(
            BaselineCrossTreeFingerprint([]),
            LatticeCrossTreeTxGrain.ComputeFingerprint([]),
            "LatticeCrossTreeTxGrain.ComputeFingerprint (no participants)");
    }

    // =====================================================================
    // (1) DecodeState union key window.
    // =====================================================================

    /// <summary>
    /// The union window alone - build, dedup and sort - on the pre-trim
    /// <c>List&lt;string&gt;</c> shape, with the emit loop stripped. This is the
    /// hunk in isolation (rule 6), because the in-situ arm below is dominated by
    /// base64 decode and event construction this change does not touch.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("statewindow")]
    public int OrSetUnionWindow_Baseline() => BaselineOrSetUnionWindow(_orSet);

    /// <summary>The shipped pooled window over the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("statewindow")]
    public int OrSetUnionWindow_Optimized() => OptimizedOrSetUnionWindow(_orSet);

    /// <summary>RW-set twin of <see cref="OrSetUnionWindow_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("statewindow")]
    public int RwSetUnionWindow_Baseline() => BaselineRwSetUnionWindow(_rwSet);

    /// <summary>The shipped pooled window over the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("statewindow")]
    public int RwSetUnionWindow_Optimized() => OptimizedRwSetUnionWindow(_rwSet);

    /// <summary>In-situ: the whole state decode on the pre-trim key window.</summary>
    [Benchmark]
    [BenchmarkCategory("statedecode")]
    public int OrSetDecodeState_Baseline() => BaselineOrSetDecodeState(_orSet).Count;

    /// <summary>In-situ: the shipped state decode of the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("statedecode")]
    public int OrSetDecodeState_Optimized() => OrSetProvenanceDecoder.Instance.DecodeState(_orSet).Count;

    /// <summary>RW-set twin of <see cref="OrSetDecodeState_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("statedecode")]
    public int RwSetDecodeState_Baseline() => BaselineRwSetDecodeState(_rwSet).Count;

    /// <summary>In-situ: the shipped state decode of the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("statedecode")]
    public int RwSetDecodeState_Optimized() => RwSetProvenanceDecoder.Instance.DecodeState(_rwSet).Count;

    // =====================================================================
    // (2) DecodeCurrentValue add key window.
    // =====================================================================

    /// <summary>
    /// In-situ: the live-member projection on the pre-trim key list plus
    /// per-element <c>adds[key]</c> re-probe. Carried in situ only, because the
    /// isolated shape is already measured by
    /// <see cref="CrdtProvenanceDecodeWalkTrimBenchmarks"/> (rule 19).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("valuedecode")]
    public int OrSetDecodeCurrentValue_Baseline() => BaselineOrSetDecodeCurrentValue(_orSet).Count;

    /// <summary>The shipped projection of the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("valuedecode")]
    public int OrSetDecodeCurrentValue_Optimized()
        => OrSetProvenanceDecoder.Instance.DecodeCurrentValue(_orSet).Count;

    /// <summary>RW-set twin of <see cref="OrSetDecodeCurrentValue_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("valuedecode")]
    public int RwSetDecodeCurrentValue_Baseline() => BaselineRwSetDecodeCurrentValue(_rwSet).Count;

    /// <summary>The shipped projection of the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("valuedecode")]
    public int RwSetDecodeCurrentValue_Optimized()
        => RwSetProvenanceDecoder.Instance.DecodeCurrentValue(_rwSet).Count;

    // =====================================================================
    // (3) Atomic-commit key fingerprints.
    // =====================================================================

    /// <summary>The batch fingerprint on the pre-trim allocated key array.</summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int BatchFingerprint_Baseline() => BaselineComputeKeyFingerprint(_batch).Length;

    /// <summary>The shipped batch fingerprint over the same entries.</summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int BatchFingerprint_Optimized() => AtomicWriteGrain.ComputeKeyFingerprint(_batch).Length;

    /// <summary>
    /// The cross-tree saga fingerprint on the pre-trim shape, which allocated a
    /// key array <b>per participant</b>.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int CrossTreeFingerprint_Baseline() => BaselineCrossTreeFingerprint(_participants).Length;

    /// <summary>The shipped fingerprint, whose single rental spans the loop.</summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int CrossTreeFingerprint_Optimized()
        => LatticeCrossTreeTxGrain.ComputeFingerprint(_participants).Length;

    /// <summary>
    /// Control: one participant, so the loop runs once and hoisting the rental
    /// out of it can save nothing beyond the single array.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int CrossTreeFingerprint_Baseline_SingleParticipant()
        => BaselineCrossTreeFingerprint(_singleParticipant).Length;

    /// <summary>Optimized counterpart to the single-participant control.</summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int CrossTreeFingerprint_Optimized_SingleParticipant()
        => LatticeCrossTreeTxGrain.ComputeFingerprint(_singleParticipant).Length;

    // =====================================================================
    // Isolated window stages.
    // =====================================================================

    /// <summary>
    /// Verbatim copy of the union-window prelude of
    /// <c>OrSetProvenanceDecoder.DecodeState</c> as it stood before the pooled
    /// window, returning a checksum so the sorted order is observable and the
    /// work cannot be elided.
    /// </summary>
    private static int BaselineOrSetUnionWindow(OrSet set)
    {
        var adds = set.Adds;
        var tombstones = set.Tombstones;

        var keys = new List<string>(adds.Count + tombstones.Count);
        var total = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }
        foreach (var (key, dots) in tombstones)
        {
            total += dots.Count;
            if (!adds.ContainsKey(key)) keys.Add(key);
        }
        if (total == 0) return 0;

        keys.Sort(OrdinalStringOrder.Comparison);

        // Stands in for the emit loop's per-key work: the baseline re-probes the
        // adds map once per key for a list the fill pass already held.
        var checksum = 0;
        foreach (var key in keys)
        {
            checksum = (checksum * 31) + key.Length;
            if (adds.TryGetValue(key, out var addDots)) checksum += addDots.Count;
        }
        return checksum;
    }

    /// <summary>The shipped pooled-window form of the same stage.</summary>
    private static int OptimizedOrSetUnionWindow(OrSet set)
    {
        var adds = set.Adds;
        var tombstones = set.Tombstones;

        var keyCount = adds.Count + tombstones.Count;
        if (keyCount == 0) return 0;

        var window = ArrayPool<string>.Shared.Rent(keyCount);
        var written = 0;
        try
        {
            var total = 0;
            foreach (var (key, dots) in adds)
            {
                window[written++] = key;
                total += dots.Count;
            }
            foreach (var (key, dots) in tombstones)
            {
                total += dots.Count;
                if (!adds.ContainsKey(key))
                {
                    window[written++] = key;
                }
            }
            if (total == 0) return 0;

            var keys = window.AsSpan(0, written);
            keys.Sort(OrdinalStringOrder.Comparison);

            var checksum = 0;
            foreach (var key in keys)
            {
                checksum = (checksum * 31) + key.Length;
                if (adds.TryGetValue(key, out var addDots)) checksum += addDots.Count;
            }
            return checksum;
        }
        finally
        {
            Array.Clear(window, 0, written);
            ArrayPool<string>.Shared.Return(window);
        }
    }

    /// <summary>RW-set twin of <see cref="BaselineOrSetUnionWindow"/>.</summary>
    private static int BaselineRwSetUnionWindow(RwSet set)
    {
        var adds = set.Adds;
        var removes = set.Removes;

        var keys = new List<string>(adds.Count + removes.Count);
        var total = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }
        foreach (var (key, dots) in removes)
        {
            total += dots.Count;
            if (!adds.ContainsKey(key)) keys.Add(key);
        }
        if (total == 0) return 0;

        keys.Sort(OrdinalStringOrder.Comparison);

        var checksum = 0;
        foreach (var key in keys)
        {
            checksum = (checksum * 31) + key.Length;
            if (adds.TryGetValue(key, out var addDots)) checksum += addDots.Count;
        }
        return checksum;
    }

    /// <summary>The shipped pooled-window form of the same stage.</summary>
    private static int OptimizedRwSetUnionWindow(RwSet set)
    {
        var adds = set.Adds;
        var removes = set.Removes;

        var keyCount = adds.Count + removes.Count;
        if (keyCount == 0) return 0;

        var window = ArrayPool<string>.Shared.Rent(keyCount);
        var written = 0;
        try
        {
            var total = 0;
            foreach (var (key, dots) in adds)
            {
                window[written++] = key;
                total += dots.Count;
            }
            foreach (var (key, dots) in removes)
            {
                total += dots.Count;
                if (!adds.ContainsKey(key))
                {
                    window[written++] = key;
                }
            }
            if (total == 0) return 0;

            var keys = window.AsSpan(0, written);
            keys.Sort(OrdinalStringOrder.Comparison);

            var checksum = 0;
            foreach (var key in keys)
            {
                checksum = (checksum * 31) + key.Length;
                if (adds.TryGetValue(key, out var addDots)) checksum += addDots.Count;
            }
            return checksum;
        }
        finally
        {
            Array.Clear(window, 0, written);
            ArrayPool<string>.Shared.Return(window);
        }
    }

    // =====================================================================
    // Verbatim pre-trim decode bodies.
    // =====================================================================

    /// <summary>
    /// Verbatim copy of <c>OrSetProvenanceDecoder.DecodeState</c> as it stood
    /// before the pooled key window. It calls the shipped <c>ContainsExact</c>,
    /// so the pair differs only in how the keys are collected and walked.
    /// </summary>
    private static List<CrdtMemberChange> BaselineOrSetDecodeState(OrSet set)
    {
        var adds = set.Adds;
        var tombstones = set.Tombstones;

        var keys = new List<string>(adds.Count + tombstones.Count);
        var total = 0;
        var tombstoneDots = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }
        foreach (var (key, dots) in tombstones)
        {
            total += dots.Count;
            tombstoneDots += dots.Count;
            if (!adds.ContainsKey(key)) keys.Add(key);
        }
        if (total == 0) return [];

        keys.Sort(OrdinalStringOrder.Comparison);

        var result = new List<CrdtMemberChange>(total);
        var widened = false;
        foreach (var key in keys)
        {
            var element = Convert.FromBase64String(key);
            var start = result.Count;

            if (adds.TryGetValue(key, out var addDots))
            {
                var addSpan = CollectionsMarshal.AsSpan(addDots);
                for (var i = 0; i < addSpan.Length; i++)
                {
                    var dot = addSpan[i];
                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            if (tombstones.TryGetValue(key, out var tombDots))
            {
                var tombSpan = CollectionsMarshal.AsSpan(tombDots);
                for (var i = 0; i < tombSpan.Length; i++)
                {
                    var dot = tombSpan[i];
                    if (addDots is not null && !OrSetProvenanceDecoder.ContainsExact(addDots, in dot))
                    {
                        if (!widened)
                        {
                            widened = true;
                            result.Capacity = total + tombstoneDots;
                        }

                        result.Add(new CrdtMemberChange
                        {
                            Element = element,
                            Kind = CrdtMemberChangeKind.Added,
                            ReplicaId = dot.ReplicaId,
                            Ordinal = dot.Counter,
                            WallClock = null,
                        });
                    }

                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            CollectionsMarshal.AsSpan(result).Slice(start, result.Count - start).Sort(OrSetProvenanceDecoder.CausalOrderComparer.Comparison);
        }

        return result;
    }

    /// <summary>
    /// Verbatim copy of <c>RwSetProvenanceDecoder.DecodeState</c> as it stood
    /// before the pooled key window.
    /// </summary>
    private static List<CrdtMemberChange> BaselineRwSetDecodeState(RwSet set)
    {
        var adds = set.Adds;
        var removes = set.Removes;

        var keys = new List<string>(adds.Count + removes.Count);
        var total = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }
        foreach (var (key, dots) in removes)
        {
            total += dots.Count;
            if (!adds.ContainsKey(key)) keys.Add(key);
        }
        if (total == 0) return [];

        keys.Sort(OrdinalStringOrder.Comparison);

        var result = new List<CrdtMemberChange>(total);
        foreach (var key in keys)
        {
            var element = Convert.FromBase64String(key);
            var start = result.Count;

            if (adds.TryGetValue(key, out var addDots))
            {
                var addSpan = CollectionsMarshal.AsSpan(addDots);
                for (var i = 0; i < addSpan.Length; i++)
                {
                    var dot = addSpan[i];
                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            if (removes.TryGetValue(key, out var removeDots))
            {
                var removeSpan = CollectionsMarshal.AsSpan(removeDots);
                for (var i = 0; i < removeSpan.Length; i++)
                {
                    var dot = removeSpan[i];
                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            CollectionsMarshal.AsSpan(result).Slice(start, result.Count - start).Sort(RwSetProvenanceDecoder.CausalOrderComparer.Comparison);
        }
        return result;
    }

    /// <summary>
    /// Verbatim copy of <c>OrSetProvenanceDecoder.DecodeCurrentValue</c> as it
    /// stood before the pooled key window. It calls the shipped
    /// <c>SingleReplica</c> and <c>IsTombstoned</c>.
    /// </summary>
    private static List<CrdtMemberValue> BaselineOrSetDecodeCurrentValue(OrSet set)
    {
        var adds = set.Adds;
        if (adds.Count == 0) return [];

        var keys = new List<string>(adds.Count);
        foreach (var key in adds.Keys) keys.Add(key);
        keys.Sort(OrdinalStringOrder.Comparison);

        var tombstones = set.Tombstones;
        var result = new List<CrdtMemberValue>(keys.Count);
        foreach (var key in keys)
        {
            var addDots = adds[key];
            tombstones.TryGetValue(key, out var tomb);

            string? sharedReplica = null;
            var coverCounter = long.MinValue;
            if (tomb is not null && tomb.Count > DotIndexThreshold && addDots.Count > 1)
            {
                sharedReplica = OrSetProvenanceDecoder.SingleReplica(tomb);
                if (sharedReplica is not null)
                {
                    var tombSpan = CollectionsMarshal.AsSpan(tomb);
                    for (var i = 0; i < tombSpan.Length; i++)
                    {
                        var counter = tombSpan[i].Counter;
                        if (counter > coverCounter) coverCounter = counter;
                    }
                }
            }

            var hasLive = false;
            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            var addSpan = CollectionsMarshal.AsSpan(addDots);
            for (var i = 0; i < addSpan.Length; i++)
            {
                var dot = addSpan[i];
                var tombstoned = sharedReplica is not null
                    ? dot.Counter <= coverCounter
                        && string.Equals(dot.ReplicaId, sharedReplica, StringComparison.Ordinal)
                    : OrSetProvenanceDecoder.IsTombstoned(tomb, dot);
                if (tombstoned) continue;
                if (!hasLive
                    || dot.Counter > bestCounter
                    || (dot.Counter == bestCounter && string.CompareOrdinal(dot.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = dot.ReplicaId;
                    bestCounter = dot.Counter;
                }
            }

            if (!hasLive) continue;
            result.Add(new CrdtMemberValue
            {
                Element = Convert.FromBase64String(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        return result;
    }

    /// <summary>
    /// Verbatim copy of <c>RwSetProvenanceDecoder.DecodeCurrentValue</c> as it
    /// stood before the pooled key window. It calls the shipped
    /// <c>HasLiveRemove</c>.
    /// </summary>
    private static List<CrdtMemberValue> BaselineRwSetDecodeCurrentValue(RwSet set)
    {
        var adds = set.Adds;
        if (adds.Count == 0) return [];

        var keys = new List<string>(adds.Count);
        foreach (var key in adds.Keys) keys.Add(key);
        keys.Sort(OrdinalStringOrder.Comparison);

        var result = new List<CrdtMemberValue>(keys.Count);
        foreach (var key in keys)
        {
            var addDots = adds[key];
            if (addDots.Count == 0) continue;

            if (RwSetProvenanceDecoder.HasLiveRemove(set, key)) continue;

            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            var hasLive = false;
            var addSpan = CollectionsMarshal.AsSpan(addDots);
            for (var i = 0; i < addSpan.Length; i++)
            {
                ref readonly var dot = ref addSpan[i];
                if (!hasLive
                    || dot.Counter > bestCounter
                    || (dot.Counter == bestCounter && string.CompareOrdinal(dot.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = dot.ReplicaId;
                    bestCounter = dot.Counter;
                }
            }

            if (!hasLive) continue;
            result.Add(new CrdtMemberValue
            {
                Element = Convert.FromBase64String(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        return result;
    }

    // =====================================================================
    // Verbatim pre-trim fingerprint bodies.
    // =====================================================================

    /// <summary>
    /// Verbatim copy of <c>AtomicWriteGrain.ComputeKeyFingerprint</c> as it
    /// stood before the pooled key window. It calls the shipped hashing core,
    /// so the pair differs only in where the sorted key set lives.
    /// </summary>
    private static byte[] BaselineComputeKeyFingerprint(List<KeyValuePair<string, byte[]>> entries)
    {
        var sortedKeys = new string[entries.Count];
        for (int i = 0; i < entries.Count; i++) sortedKeys[i] = entries[i].Key;
        Array.Sort(sortedKeys, OrdinalStringOrder.Comparison);

        return AtomicWriteGrain.ComputeKeyFingerprintCore(sortedKeys);
    }

    /// <summary>
    /// Verbatim copy of <c>LatticeCrossTreeTxGrain.ComputeFingerprint</c> as it
    /// stood before the hoisted rental - a fresh key array per participant. It
    /// appends through the shipped <c>AppendLengthPrefixed</c>.
    /// </summary>
    private static byte[] BaselineCrossTreeFingerprint(List<CrossTreeParticipant> participants)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> lenPrefix = stackalloc byte[4];
        foreach (var p in participants)
        {
            LatticeCrossTreeTxGrain.AppendLengthPrefixed(hash, p.TreeId, lenPrefix);
            var keys = new string[p.Entries.Count];
            for (var i = 0; i < p.Entries.Count; i++) keys[i] = p.Entries[i].Key;
            Array.Sort(keys, OrdinalStringOrder.Comparison);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(lenPrefix, keys.Length);
            hash.AppendData(lenPrefix);
            foreach (var key in keys)
            {
                LatticeCrossTreeTxGrain.AppendLengthPrefixed(hash, key, lenPrefix);
            }
        }
        return hash.GetHashAndReset();
    }

    // =====================================================================
    // Fixtures.
    // =====================================================================

    /// <summary>
    /// A churned OR-set: every element carries several add dots and a tombstone
    /// list longer than <see cref="DotIndexThreshold"/>, and a tail of elements
    /// appears only in tombstones so the union dedup has real work to do.
    /// </summary>
    private static OrSet BuildOrSet(int width)
    {
        var set = new OrSet();
        long counter = 0;
        for (var e = 0; e < width; e++)
        {
            var element = Encoding.UTF8.GetBytes($"element-{e:D4}");

            // Churn past the dot-index threshold, then tombstone the lot, so the
            // element carries a long tombstone list.
            for (var i = 0; i < DotIndexThreshold + 2; i++) set.Add(element, ReplicaB, ++counter);
            set.Remove(element);

            // Re-add so the element is live again and the value projection has
            // something to emit.
            for (var i = 0; i < 3; i++) set.Add(element, ReplicaA, ++counter);
        }

        // Pure-remove tail: present in tombstones only, which is the branch the
        // union dedup exists for.
        for (var e = 0; e < Math.Max(1, width / 4); e++)
        {
            var element = Encoding.UTF8.GetBytes($"gone-{e:D4}");
            set.Add(element, ReplicaA, ++counter);
            set.Remove(element);
        }

        return set;
    }

    /// <summary>RW-set twin of <see cref="BuildOrSet"/>.</summary>
    private static RwSet BuildRwSet(int width)
    {
        var set = new RwSet();
        long counter = 0;
        for (var e = 0; e < width; e++)
        {
            var element = Encoding.UTF8.GetBytes($"element-{e:D4}");
            for (var i = 0; i < DotIndexThreshold + 2; i++)
            {
                set.Add(element, ReplicaB, ++counter);
                set.Remove(element, ReplicaB, counter);
            }
            for (var i = 0; i < 3; i++) set.Add(element, ReplicaA, ++counter);
        }

        for (var e = 0; e < Math.Max(1, width / 4); e++)
        {
            var element = Encoding.UTF8.GetBytes($"gone-{e:D4}");
            set.Remove(element, ReplicaA, ++counter);
        }

        return set;
    }

    /// <summary>
    /// An atomic write batch of <paramref name="width"/> entries, keyed in
    /// reverse so the sort is not already-ordered.
    /// </summary>
    private static List<KeyValuePair<string, byte[]>> BuildBatch(int width)
    {
        var batch = new List<KeyValuePair<string, byte[]>>(width);
        for (var i = width - 1; i >= 0; i--)
        {
            batch.Add(new KeyValuePair<string, byte[]>($"tenant/customer/{i:D6}", new byte[24]));
        }
        return batch;
    }

    /// <summary>
    /// A cross-tree saga over <paramref name="participantCount"/> trees, each
    /// addressing <paramref name="width"/> keys.
    /// </summary>
    private static List<CrossTreeParticipant> BuildParticipants(int width, int participantCount)
    {
        var participants = new List<CrossTreeParticipant>(participantCount);
        for (var p = 0; p < participantCount; p++)
        {
            participants.Add(new CrossTreeParticipant
            {
                TreeId = $"tree-{p:D2}",
                Entries = BuildBatch(width),
            });
        }
        return participants;
    }

    // =====================================================================
    // Equivalence assertions (rule 2).
    // =====================================================================

    private static void AssertChanges(
        IReadOnlyList<CrdtMemberChange> baseline,
        IReadOnlyList<CrdtMemberChange> optimized,
        string label)
    {
        if (baseline.Count != optimized.Count)
            throw new InvalidOperationException($"{label}: count {optimized.Count} != baseline {baseline.Count}.");
        for (var i = 0; i < baseline.Count; i++)
        {
            var a = baseline[i];
            var b = optimized[i];
            if (a.Kind != b.Kind
                || a.Ordinal != b.Ordinal
                || !string.Equals(a.ReplicaId, b.ReplicaId, StringComparison.Ordinal)
                || !a.Element.AsSpan().SequenceEqual(b.Element))
            {
                throw new InvalidOperationException($"{label}: element {i} differs from its baseline.");
            }
        }
    }

    private static void AssertValues(
        IReadOnlyList<CrdtMemberValue> baseline,
        IReadOnlyList<CrdtMemberValue> optimized,
        string label)
    {
        if (baseline.Count != optimized.Count)
            throw new InvalidOperationException($"{label}: count {optimized.Count} != baseline {baseline.Count}.");
        for (var i = 0; i < baseline.Count; i++)
        {
            var a = baseline[i];
            var b = optimized[i];
            if (a.Ordinal != b.Ordinal
                || !string.Equals(a.ReplicaId, b.ReplicaId, StringComparison.Ordinal)
                || !a.Element.AsSpan().SequenceEqual(b.Element))
            {
                throw new InvalidOperationException($"{label}: member {i} differs from its baseline.");
            }
        }
    }

    private static void AssertDigest(byte[] baseline, byte[] optimized, string label)
    {
        if (!baseline.AsSpan().SequenceEqual(optimized))
            throw new InvalidOperationException($"{label}: digest differs from its baseline.");
    }
}
