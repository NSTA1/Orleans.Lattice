using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three fresh-dictionary direct-iteration trims made to
/// <c>Orleans.Lattice.Views.AggregationApplier</c> so their per-operation byte
/// delta is measurable in the clear. The aggregation applier reduces every view
/// contribution against freshly-materialised dictionaries - the per-contribution
/// accumulator slot map, and the per-materialise inverse/fold-inverse shard maps
/// decoded from the store. Each of these is a <b>fresh</b> dictionary, so a
/// <c>.Keys</c> / <c>.Values</c> access on it allocates a throwaway
/// <c>KeyCollection</c> / <c>ValueCollection</c> wrapper on first touch (a
/// long-lived dictionary caches the wrapper in its <c>_keys</c>/<c>_values</c>
/// field and pays nothing after the first access, but these maps live for a
/// single call). Walking the dictionary through its struct enumerator instead
/// removes that wrapper allocation while visiting exactly the same entries.
/// <para>
/// The pairs mirror the production edits verbatim, completing the direct-iteration
/// pass that <c>WorstKey</c>/<c>LargestSourceKey</c> already started:
/// (1) <c>ContributeNumericAsync</c> - the opportunistic cleanup loop walked
/// <c>slots.Keys</c> over the fresh per-contribution accumulator map;
/// (2) <c>MaterialiseInverseAsync</c> - the shard-gather walked
/// <c>shards.Values</c> over the fresh GetMany result and, nested inside,
/// <c>DecodeInverse(bytes).Values</c> over each fresh per-shard decode, so it
/// dropped <b>one wrapper for the outer map plus one per shard</b>;
/// (3) <c>MaterialiseFoldAsync</c> - the shard-gather walked <c>shards.Values</c>
/// over the fresh GetMany result.
/// </para>
/// <para>
/// Each lane builds the fresh dictionaries inside the benchmark (as production
/// does per call) so the wrapper allocation is charged, and both lanes in a pair
/// build them identically - the only difference is <c>.Keys</c>/<c>.Values</c>
/// versus the struct enumerator - so the measured <c>Allocated</c> delta is
/// precisely the collection-wrapper heap the production change removes. The maps
/// are keyed <see cref="StringComparer.Ordinal"/> exactly as the applier's are.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggiter</c> (or <c>--suite aggiter</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast
/// to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationDictIterationBenchmarks
{
    // A minimal stand-in for the applier's AccumulatorRow / MemberEntry value
    // types. Only its presence as the dictionary's TValue matters; the wrapper
    // allocation the trims remove is independent of the value type.
    private readonly record struct Entry(long Count, double Numeric);

    // ---- (1) numeric-contribution cleanup: a fresh 1-2 entry slots map ----
    private string[] _slotKeys = null!;

    // ---- (2)/(3) materialise shard-gather: fresh outer + per-shard maps ----
    private string[] _shardKeys = null!;
    private string[][] _memberKeys = null!;

    /// <summary>Builds the key inputs the fresh maps are rebuilt from per op.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // A same-group overwrite touches two accumulator slots (old + new); a
        // fresh-group contribution touches one. Two is the steady-state upper
        // bound and the shape the cleanup loop walks.
        _slotKeys = new[] { "grp-a\0s0", "grp-b\0s0" };

        // A group's inverse/fold-inverse rows are sharded by _fanout; a handful
        // of shards is the common case. Each shard decodes to a small member map.
        const int shardCount = 8;
        const int membersPerShard = 8;
        _shardKeys = new string[shardCount];
        _memberKeys = new string[shardCount][];
        for (var s = 0; s < shardCount; s++)
        {
            _shardKeys[s] = "inv\0grp\0" + s.ToString("D2", CultureInfo.InvariantCulture);
            var members = new string[membersPerShard];
            for (var m = 0; m < membersPerShard; m++)
            {
                members[m] = "src-" + (s * membersPerShard + m).ToString("D4", CultureInfo.InvariantCulture);
            }

            _memberKeys[s] = members;
        }

        // A realistic group key and the source key whose shard slot a numeric
        // contribution derives.
        _groupKey = "region\u0000emea\u0000tier\u0000gold";
        _sourceKey = "tenant-0042/orders/8f3c1a9e-7b21-4d55-9f60-2ac1d8e40b77";

        AssertShardKeyEquivalence();
    }

    /// <summary>
    /// The shard-key pair must build the same key as the concatenation it was
    /// measured against for every slot in the fanout, the slot-derivation pair
    /// must agree with the shipped hash, and the slot-accumulation pair must
    /// produce the same entry and cleanup counts on both the re-group and the
    /// same-group shapes - otherwise the lanes are timing two different answers.
    /// </summary>
    private void AssertShardKeyEquivalence()
    {
        for (var slot = 0; slot <= Fanout; slot++)
        {
            if (!string.Equals(
                    BaselineAccumulatorKey(_groupKey, slot),
                    RejectedAccumulatorKey(_groupKey, slot),
                    StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Shard-key lanes build different keys for slot {slot}; the comparison would be void.");
            }
        }

        if (Regroup_SlotTwice() != Regroup_SlotOnce())
        {
            throw new InvalidOperationException(
                "Slot-derivation lanes build different keys; the comparison would be void.");
        }

        if (Contribute_Regroup_Dictionary() != Contribute_Regroup_Locals()
            || Contribute_SameGroup_Dictionary() != Contribute_SameGroup_Locals())
        {
            throw new InvalidOperationException(
                "Slot-accumulation lanes produce different batches; the comparison would be void.");
        }

        // The same-group shape must genuinely fold, or the control proves nothing.
        if (Contribute_SameGroup_Locals() >= Contribute_Regroup_Locals())
        {
            throw new InvalidOperationException(
                "The same-group control did not fold onto one slot; it is not a control.");
        }
    }

    // ------------------------------------------------------------------
    // (1) Numeric-contribution cleanup loop
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>foreach (var key in slots.Keys)</c> over the fresh
    /// per-contribution accumulator map allocates a throwaway
    /// <c>KeyCollection</c> on first access.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Numeric cleanup: slots.Keys (baseline)")]
    public int NumericCleanup_Keys()
    {
        var slots = BuildSlots();
        var acc = 0;
        foreach (var key in slots.Keys)
        {
            acc += key.Length;
        }

        return acc;
    }

    /// <summary>
    /// Optimized: iterating the fresh map directly uses only the struct
    /// enumerator, allocating no <c>KeyCollection</c>.
    /// </summary>
    [Benchmark(Description = "Numeric cleanup: direct (optimized)")]
    public int NumericCleanup_Direct()
    {
        var slots = BuildSlots();
        var acc = 0;
        foreach (var (key, _) in slots)
        {
            acc += key.Length;
        }

        return acc;
    }

    private Dictionary<string, Entry> BuildSlots()
    {
        var slots = new Dictionary<string, Entry>(StringComparer.Ordinal);
        for (var i = 0; i < _slotKeys.Length; i++)
        {
            slots[_slotKeys[i]] = new Entry(i + 1, i);
        }

        return slots;
    }

    // ------------------------------------------------------------------
    // (2) Inverse-materialise shard gather (nested .Values)
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>foreach (var v in shards.Values)</c> over the fresh GetMany
    /// result plus <c>foreach (var e in DecodeInverse(bytes).Values)</c> over each
    /// fresh per-shard decode allocates one <c>ValueCollection</c> for the outer
    /// map and one for every shard.
    /// </summary>
    [Benchmark(Description = "Inverse materialise: nested .Values (baseline)")]
    public double InverseMaterialise_Values()
    {
        var shards = BuildShards();
        var extreme = double.NegativeInfinity;
        foreach (var inner in shards.Values)
        {
            foreach (var entry in inner.Values)
            {
                extreme = Math.Max(extreme, entry.Numeric);
            }
        }

        return extreme;
    }

    /// <summary>
    /// Optimized: walking both fresh maps through their struct enumerators
    /// allocates neither <c>ValueCollection</c>.
    /// </summary>
    [Benchmark(Description = "Inverse materialise: nested direct (optimized)")]
    public double InverseMaterialise_Direct()
    {
        var shards = BuildShards();
        var extreme = double.NegativeInfinity;
        foreach (var (_, inner) in shards)
        {
            foreach (var (_, entry) in inner)
            {
                extreme = Math.Max(extreme, entry.Numeric);
            }
        }

        return extreme;
    }

    // ------------------------------------------------------------------
    // (3) Fold-materialise shard gather (outer .Values)
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>foreach (var v in shards.Values)</c> over the fresh GetMany
    /// result allocates a throwaway <c>ValueCollection</c> on first access; the
    /// inner map is already walked directly.
    /// </summary>
    [Benchmark(Description = "Fold materialise: shards.Values (baseline)")]
    public int FoldMaterialise_Values()
    {
        var shards = BuildShards();
        var acc = 0;
        foreach (var inner in shards.Values)
        {
            foreach (var (sourceKey, _) in inner)
            {
                acc += sourceKey.Length;
            }
        }

        return acc;
    }

    /// <summary>
    /// Optimized: walking the fresh outer map through its struct enumerator
    /// allocates no <c>ValueCollection</c>.
    /// </summary>
    [Benchmark(Description = "Fold materialise: direct (optimized)")]
    public int FoldMaterialise_Direct()
    {
        var shards = BuildShards();
        var acc = 0;
        foreach (var (_, inner) in shards)
        {
            foreach (var (sourceKey, _) in inner)
            {
                acc += sourceKey.Length;
            }
        }

        return acc;
    }

    private Dictionary<string, Dictionary<string, Entry>> BuildShards()
    {
        var shards = new Dictionary<string, Dictionary<string, Entry>>(_shardKeys.Length, StringComparer.Ordinal);
        for (var s = 0; s < _shardKeys.Length; s++)
        {
            var members = _memberKeys[s];
            var inner = new Dictionary<string, Entry>(members.Length, StringComparer.Ordinal);
            for (var m = 0; m < members.Length; m++)
            {
                inner[members[m]] = new Entry(1, m);
            }

            shards[_shardKeys[s]] = inner;
        }

        return shards;
    }

    // ------------------------------------------------------------------
    // (4) Shard row-key construction - REJECTED MECHANISM, kept as evidence
    // ------------------------------------------------------------------
    //
    // Every accumulator / inverse / fold-inverse row key is
    // "\0{family}{groupKey}\0{slot}", built by a four-part string concatenation.
    // The hypothesis was that this allocates TWICE per key - once for
    // slot.ToString(), once for the joined result - and that formatting the
    // digits into a stack buffer and joining the pieces as spans would remove
    // the first.
    //
    // It does not, and these lanes are kept so the mechanism is not re-tried.
    // The measured Allocated column is IDENTICAL on both sides, because .NET
    // serves int.ToString() for a small non-negative value out of a cached
    // string table rather than allocating - and a slot index is bounded by the
    // view's fanout, so it is always in that range. The span form therefore
    // buys no allocation at all and costs a TryFormat plus the slower
    // four-span Concat path, measuring about 30% SLOWER per key on both the
    // fanout lane and the single-key control.
    //
    // The shipped builder is the plain concatenation, i.e. the "baseline" side.

    /// <summary>Slots a group's rows are sharded across, mirroring a view's fanout.</summary>
    private const int Fanout = 16;

    private const string AccumulatorPrefix = "\u0000a";
    private const string ReservedPrefix = "\u0000";
    private const int MaxSlotDigits = 11;

    private string _groupKey = null!;

    /// <summary>
    /// The shipped key builder: a plain four-part concatenation whose slot
    /// digits come from the runtime's small-number string cache.
    /// </summary>
    [Benchmark(Description = "Shard key: concat + slot.ToString() (shipped)")]
    public int ShardKey_Concat()
    {
        var acc = 0;
        for (var slot = 0; slot < Fanout; slot++)
        {
            acc += BaselineAccumulatorKey(_groupKey, slot).Length;
        }

        return acc;
    }

    /// <summary>
    /// Rejected: the digits formatted on the stack and the pieces joined as
    /// spans. Allocates exactly as much and runs measurably slower.
    /// </summary>
    [Benchmark(Description = "Shard key: stack-formatted span concat (rejected)")]
    public int ShardKey_SpanConcat()
    {
        var acc = 0;
        for (var slot = 0; slot < Fanout; slot++)
        {
            acc += RejectedAccumulatorKey(_groupKey, slot).Length;
        }

        return acc;
    }

    /// <summary>
    /// Control: a single key rather than a whole fanout, confirming the
    /// regression is per key rather than an artefact of the loop.
    /// </summary>
    [Benchmark(Description = "Control - single shard key: concat (shipped)")]
    public int ShardKeySingle_Concat() => BaselineAccumulatorKey(_groupKey, 0).Length;

    /// <summary>Control: a single key, rejected side.</summary>
    [Benchmark(Description = "Control - single shard key: span concat (rejected)")]
    public int ShardKeySingle_SpanConcat() => RejectedAccumulatorKey(_groupKey, 0).Length;

    /// <summary>Verbatim copy of the shipped shard key builder.</summary>
    private static string BaselineAccumulatorKey(string groupKey, int slot)
        => "\u0000a" + groupKey + "\u0000" + slot.ToString();

    /// <summary>The rejected span-concatenating builder, kept as evidence.</summary>
    private static string RejectedAccumulatorKey(string groupKey, int slot)
    {
        Span<char> digits = stackalloc char[MaxSlotDigits];
        slot.TryFormat(digits, out var digitCount, provider: CultureInfo.InvariantCulture);
        return string.Concat(
            AccumulatorPrefix.AsSpan(), groupKey.AsSpan(), ReservedPrefix.AsSpan(), digits[..digitCount]);
    }

    // ------------------------------------------------------------------
    // (5) Repeated slot derivation on a numeric contribution
    // ------------------------------------------------------------------
    //
    // A numeric contribution that re-groups its source key touches two
    // accumulator rows - the group it left and the group it joined - and the
    // shard slot is the same for both, being a pure function of the source key
    // and the fanout. Deriving it once per accumulator key transcoded the key
    // to UTF-8 and hashed it twice per contribution; it is now derived once.

    private string _sourceKey = null!;

    /// <summary>Baseline: the slot derived once per accumulator key.</summary>
    [Benchmark(Description = "Re-group contribution: slot derived twice (baseline)")]
    public int Regroup_SlotTwice()
    {
        var oldKey = BaselineAccumulatorKey("grp-old", BaselineSlot(_sourceKey, Fanout));
        var newKey = BaselineAccumulatorKey("grp-new", BaselineSlot(_sourceKey, Fanout));
        return oldKey.Length + newKey.Length;
    }

    /// <summary>Optimized: the slot derived once and reused for both keys.</summary>
    [Benchmark(Description = "Re-group contribution: slot derived once (optimized)")]
    public int Regroup_SlotOnce()
    {
        var slot = BaselineSlot(_sourceKey, Fanout);
        var oldKey = BaselineAccumulatorKey("grp-old", slot);
        var newKey = BaselineAccumulatorKey("grp-new", slot);
        return oldKey.Length + newKey.Length;
    }

    /// <summary>
    /// Verbatim copy of the applier's shard hash, so the lanes above charge the
    /// same transcode-and-hash the production path does.
    /// </summary>
    private static int BaselineSlot(string sourceKey, int fanout)
    {
        if (fanout <= 1)
        {
            return 0;
        }

        var maxByteCount = Encoding.UTF8.GetMaxByteCount(sourceKey.Length);
        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = Encoding.UTF8.GetBytes(sourceKey, buffer);
            var hash = XxHash32.HashToUInt32(buffer[..written]);
            return (int)(hash % (uint)fanout);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    // ------------------------------------------------------------------
    // (6) Numeric-contribution slot accumulation
    // ------------------------------------------------------------------
    //
    // ContributeNumericAsync accumulates every accumulator slot the flip
    // touches before writing them as one atomic batch. A contribution touches
    // at most TWO keys - the group the source key left and the group it joined
    // - and a same-group overwrite collapses them onto one. That upper bound is
    // structural, not incidental: the slot is a pure function of the source key
    // and the fanout, so no third key can appear.
    //
    // The accumulation was nonetheless a Dictionary<string, AccumulatorRow>,
    // and the cleanup pass that follows then built a second List by walking
    // that dictionary back out to recover the very keys it had just put in. Per
    // numeric contribution that is the dictionary's bucket array, its entry
    // array, the cleanup List and the List's backing array - for a map that can
    // never hold more than two entries, on the hottest path an aggregation view
    // has.
    //
    // Two locals and one ordinal comparison carry the same de-duplication and
    // the same retract-then-add fold. The lanes below report Allocated, which
    // is the point: the removed allocation is deterministic and does not depend
    // on the machine the suite runs on.
    //
    // Both sides run the re-group shape (two distinct groups, so two distinct
    // keys) and the same-group shape (one key, folded), because the dictionary
    // earned its keep only in the second and the replacement must match it
    // there too. Each lane consumes the built entries and cleanup keys so the
    // JIT cannot delete the work.

    private const string OldGroup = "grp-old";
    private const string NewGroup = "grp-new";

    /// <summary>Mirrors the applier's accumulator row.</summary>
    private readonly record struct BenchAccumulatorRow(long Count, double Sum);

    /// <summary>Baseline: a dictionary accumulates the slots, then is walked back out.</summary>
    [Benchmark(Description = "Numeric contribution (re-group): dictionary slots (baseline)")]
    public int Contribute_Regroup_Dictionary() => BaselineAccumulate(OldGroup, NewGroup);

    /// <summary>Optimized: two locals carry the at-most-two slots.</summary>
    [Benchmark(Description = "Numeric contribution (re-group): two locals (optimized)")]
    public int Contribute_Regroup_Locals() => OptimisedAccumulate(OldGroup, NewGroup);

    /// <summary>Control: a same-group overwrite, where both sides fold onto one key.</summary>
    [Benchmark(Description = "Control - same-group overwrite: dictionary slots (baseline)")]
    public int Contribute_SameGroup_Dictionary() => BaselineAccumulate(NewGroup, NewGroup);

    /// <summary>Control: a same-group overwrite, optimized side.</summary>
    [Benchmark(Description = "Control - same-group overwrite: two locals (optimized)")]
    public int Contribute_SameGroup_Locals() => OptimisedAccumulate(NewGroup, NewGroup);

    /// <summary>
    /// Verbatim copy of the accumulation as it stood: a dictionary for the
    /// slots, a list built from it for the atomic batch, and a second list
    /// built by walking the dictionary again for the cleanup probes.
    /// </summary>
    private int BaselineAccumulate(string oldGroup, string newGroup)
    {
        var slot = BaselineSlot(_sourceKey, Fanout);
        var slots = new Dictionary<string, BenchAccumulatorRow>(StringComparer.Ordinal);

        var oldKey = BaselineAccumulatorKey(oldGroup, slot);
        slots[oldKey] = new BenchAccumulatorRow(2, 20d);

        var newKey = BaselineAccumulatorKey(newGroup, slot);
        var baseRow = slots.TryGetValue(newKey, out var pending) ? pending : new BenchAccumulatorRow(5, 50d);
        slots[newKey] = new BenchAccumulatorRow(baseRow.Count + 1, baseRow.Sum + 7d);

        var entries = new List<KeyValuePair<string, long>>(slots.Count + 1);
        foreach (var (key, row) in slots)
        {
            entries.Add(new KeyValuePair<string, long>(key, row.Count));
        }

        entries.Add(new KeyValuePair<string, long>(_sourceKey, 1));

        var cleanupKeys = new List<string>(slots.Count);
        foreach (var (key, _) in slots)
        {
            cleanupKeys.Add(key);
        }

        return entries.Count + cleanupKeys.Count;
    }

    /// <summary>
    /// Verbatim copy of the shipped accumulation: two locals, one ordinal
    /// comparison for the same-group fold, and the two lists built straight
    /// from the keys already in hand.
    /// </summary>
    private int OptimisedAccumulate(string oldGroup, string newGroup)
    {
        var slot = BaselineSlot(_sourceKey, Fanout);

        var oldKey = BaselineAccumulatorKey(oldGroup, slot);
        var oldRow = new BenchAccumulatorRow(2, 20d);

        var newKey = BaselineAccumulatorKey(newGroup, slot);
        var sameSlot = string.Equals(oldKey, newKey, StringComparison.Ordinal);
        var baseRow = sameSlot ? oldRow : new BenchAccumulatorRow(5, 50d);
        var newRow = new BenchAccumulatorRow(baseRow.Count + 1, baseRow.Sum + 7d);

        var entries = new List<KeyValuePair<string, long>>(sameSlot ? 2 : 3);
        if (!sameSlot)
        {
            entries.Add(new KeyValuePair<string, long>(oldKey, oldRow.Count));
        }

        entries.Add(new KeyValuePair<string, long>(newKey, newRow.Count));
        entries.Add(new KeyValuePair<string, long>(_sourceKey, 1));

        var cleanupKeys = sameSlot ? [newKey] : new List<string> { oldKey, newKey };

        return entries.Count + cleanupKeys.Count;
    }
}
