using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three observed-remove dot-reconciliation allocation trims made
/// to the <c>Orleans.Lattice</c> CRDT primitives so their per-call byte deltas
/// are measurable in the clear, with no Orleans cluster in the loop. Each pair
/// below reproduces exactly one production method's prior shape against its
/// optimized shape over identical inputs, so the <c>Allocated</c> column is
/// deterministic and the baseline-vs-optimized delta is precisely the heap the
/// production change removes.
/// <para>
/// All three read/dedup sites shared the same gap: when an element/key had accumulated a
/// long observed-remove (tombstone) history, the reconciliation built a
/// <c>HashSet&lt;OrSetDot&gt;</c> sized to that history on <em>every</em> call,
/// even though the <em>live</em> side (the element's surviving add dots, or a
/// key's live entries) is overwhelmingly 1-2 in practice. Building the set is
/// wasted when the live side is tiny: a linear membership scan of the few live
/// dots against the tombstone list is <c>O(liveCount * tombCount)</c> - the same
/// asymptotic as building the tomb-sized set when <c>liveCount</c> is a small
/// constant - but allocates nothing. The optimized shape diverts to the linear
/// scan whenever <em>either</em> side is small (the prior guard checked only the
/// tombstone side), so a heavily-tombstoned key with a single live dot no longer
/// allocates on the read (<c>IsEmpty</c> / <c>Count</c> / <c>Elements</c> /
/// <c>Contains</c>) and remove hot paths.
/// </para>
/// <para>
/// The pairs mirror the production edits verbatim:/// (1) <see cref="OrSet"/>'s private <c>LiveDotCount</c> - the per-key live-dot
/// tally behind every OR-Set read;
/// (2) <see cref="OrSet"/>'s <c>Remove</c> tombstone-dedup reconciliation;
/// (3) <see cref="OrMap{TKey, TValue}"/>'s private <c>LiveEntryCount</c> - the
/// per-key live-entry tally behind every OR-Map read;
/// (4) the flag-family disable/enable dedup, which scans a tombstone list it is
/// simultaneously appending to - the one shape in the family where the
/// small-side guard could regress rather than help;
/// (5) <see cref="BoundedRegister"/>'s <c>Clone</c>, measuring the cost of
/// restoring the <c>ICrdt.Clone</c> deep-copy contract rather than an
/// optimization.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=ordedup</c> (or <c>--suite ordedup</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is
/// fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence
/// intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class OrCrdtReconcileBenchmarks
{
    // OrSet's DotLinearScanThreshold (4): below this many dots on the small
    // side a linear scan beats allocating a HashSet.
    private const int OrSetLinearThreshold = 4;

    // OrMap's LinearDedupThreshold (16): the equivalent crossover for OR-Map
    // entry reconciliation.
    private const int OrMapLinearThreshold = 16;

    // A single live add dot (the dominant real case) against a long tombstone
    // history. Kept at 2 live dots to sit clearly inside both thresholds.
    private const int LiveDots = 2;

    private List<OrSetDot> _liveDots = null!;
    private List<OrSetDot> _tombstones = null!;
    private List<OrMapEntry<GCounter>> _liveEntries = null!;
    private BoundedRegister _register = null!;
    private byte[] _compareLeft = null!;
    private byte[] _compareRight = null!;

    /// <summary>Builds the (few live, many tombstoned) inputs shared by all three pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _liveDots = new List<OrSetDot>(LiveDots);
        for (var i = 0; i < LiveDots; i++)
        {
            _liveDots.Add(new OrSetDot { ReplicaId = "live", Counter = i });
        }

        _liveEntries = new List<OrMapEntry<GCounter>>(LiveDots);
        for (var i = 0; i < LiveDots; i++)
        {
            _liveEntries.Add(new OrMapEntry<GCounter>("live", i, new GCounter()));
        }
    }

    // ------------------------------------------------------------------
    // (1) OrSet.LiveDotCount - per-key live-dot tally behind every OR-Set read
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior <c>LiveDotCount</c> shape - once the tombstone list
    /// crosses the linear threshold it builds a <c>HashSet&lt;OrSetDot&gt;</c>
    /// sized to the whole tombstone history on every call, regardless of how
    /// few live dots are being counted.
    /// </summary>
    [Benchmark(Description = "OrSet live-dot count: HashSet (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetLiveDotCount_Baseline(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveDotCount_Baseline(_liveDots, _tombstones);
    }

    /// <summary>
    /// Optimized: the shipped <c>LiveDotCount</c> shape - it diverts to the
    /// allocation-free linear scan whenever the live-dot list is small, so the
    /// few-live/many-tombstoned read no longer allocates.
    /// </summary>
    [Benchmark(Description = "OrSet live-dot count: small-side linear (optimized)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetLiveDotCount_Optimized(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveDotCount_Optimized(_liveDots, _tombstones);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveDotCount_Baseline(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return dots.Count;
        if (tomb.Count <= OrSetLinearThreshold)
        {
            var live = 0;
            foreach (var d in dots)
            {
                if (!tomb.Contains(d)) live++;
            }
            return live;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        var n = 0;
        foreach (var d in dots)
        {
            if (!tombSet.Contains(d)) n++;
        }
        return n;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveDotCount_Optimized(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return dots.Count;
        if (tomb.Count <= OrSetLinearThreshold || dots.Count <= OrSetLinearThreshold)
        {
            var live = 0;
            foreach (var d in dots)
            {
                if (!tomb.Contains(d)) live++;
            }
            return live;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        var n = 0;
        foreach (var d in dots)
        {
            if (!tombSet.Contains(d)) n++;
        }
        return n;
    }

    // ------------------------------------------------------------------
    // (2) OrSet.Remove - tombstone-dedup reconciliation on the remove path
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior <c>Remove</c> tombstone-dedup shape - once the
    /// tombstone list is large it seeds a <c>HashSet&lt;OrSetDot&gt;</c> from
    /// the whole history to dedup the few observed add dots being tombstoned.
    /// </summary>
    [Benchmark(Description = "OrSet remove dedup: HashSet (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetRemoveDedup_Baseline(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return RemoveDedup_Baseline(_liveDots, _tombstones);
    }

    /// <summary>
    /// Optimized: the shipped <c>Remove</c> shape - it diverts to the
    /// allocation-free linear dedup whenever the observed add-dot list is
    /// small, the common case for an element removed after a handful of adds.
    /// </summary>
    [Benchmark(Description = "OrSet remove dedup: small-side linear (optimized)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetRemoveDedup_Optimized(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return RemoveDedup_Optimized(_liveDots, _tombstones);
    }

    // Non-mutating mirror of the Remove membership decision: returns the number
    // of observed dots that are not already tombstoned (the production method
    // then appends exactly those to the tombstone list - an O(1)-amortized add
    // identical in both shapes, excluded here so the sole per-lane difference
    // is the HashSet allocation the baseline pays).
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int RemoveDedup_Baseline(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        var added = 0;
        if (tomb.Count <= OrSetLinearThreshold)
        {
            foreach (var dot in dots)
            {
                if (!tomb.Contains(dot)) added++;
            }
            return added;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        foreach (var dot in dots)
        {
            if (tombSet.Add(dot)) added++;
        }
        return added;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int RemoveDedup_Optimized(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        var added = 0;
        if (tomb.Count <= OrSetLinearThreshold || dots.Count <= OrSetLinearThreshold)
        {
            foreach (var dot in dots)
            {
                if (!tomb.Contains(dot)) added++;
            }
            return added;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        foreach (var dot in dots)
        {
            if (tombSet.Add(dot)) added++;
        }
        return added;
    }

    // ------------------------------------------------------------------
    // (3) OrMap.LiveEntryCount - per-key live-entry tally behind every OR-Map read
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior <c>LiveEntryCount</c> shape - once the tombstone
    /// list crosses the dedup threshold it builds a
    /// <c>HashSet&lt;OrSetDot&gt;</c> sized to the whole tombstone history on
    /// every call, regardless of how few live entries are being counted.
    /// </summary>
    [Benchmark(Description = "OrMap live-entry count: HashSet (baseline)")]
    [Arguments(32)]
    [Arguments(128)]
    [Arguments(512)]
    public int OrMapLiveEntryCount_Baseline(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveEntryCount_Baseline(_liveEntries, _tombstones);
    }

    /// <summary>
    /// Optimized: the shipped <c>LiveEntryCount</c> shape - it diverts to the
    /// allocation-free linear scan whenever the live-entry list is small, so a
    /// heavily-tombstoned key with a single live value no longer allocates on
    /// the read path.
    /// </summary>
    [Benchmark(Description = "OrMap live-entry count: small-side linear (optimized)")]
    [Arguments(32)]
    [Arguments(128)]
    [Arguments(512)]
    public int OrMapLiveEntryCount_Optimized(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveEntryCount_Optimized(_liveEntries, _tombstones);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveEntryCount_Baseline(List<OrMapEntry<GCounter>> entries, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return entries.Count;
        if (tomb.Count <= OrMapLinearThreshold)
        {
            var n = 0;
            foreach (var e in entries)
            {
                var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
                if (!ListContainsDot(tomb, dot)) n++;
            }
            return n;
        }
        var tombSet = new HashSet<OrSetDot>(tomb.Count);
        foreach (var d in tomb) tombSet.Add(d);
        var live = 0;
        foreach (var e in entries)
        {
            if (!tombSet.Contains(new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter })) live++;
        }
        return live;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveEntryCount_Optimized(List<OrMapEntry<GCounter>> entries, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return entries.Count;
        if (tomb.Count <= OrMapLinearThreshold || entries.Count <= OrMapLinearThreshold)
        {
            var n = 0;
            foreach (var e in entries)
            {
                var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
                if (!ListContainsDot(tomb, dot)) n++;
            }
            return n;
        }
        var tombSet = new HashSet<OrSetDot>(tomb.Count);
        foreach (var d in tomb) tombSet.Add(d);
        var live = 0;
        foreach (var e in entries)
        {
            if (!tombSet.Contains(new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter })) live++;
        }
        return live;
    }

    // ------------------------------------------------------------------
    // (4) OrFlag.Disable / RwFlag.Enable - dedup against a *growing* tombstone
    //     list. Distinct from pair (2): the production flag paths append to the
    //     very list they are scanning, so the linear lane's scan target grows
    //     inside the loop. That is the one shape in this family where the
    //     small-side guard could plausibly regress rather than help, so it is
    //     measured rather than assumed.
    //
    //     The other four P1 sites (RwSet.LiveDotCount, RwSet.AddObservedTombstones,
    //     OrFlag.LiveEnableCount, RwFlag.LiveDisableCount) reduce to kernels
    //     byte-identical to pairs (1) and (2) above and are deliberately not
    //     re-measured here - duplicating an identical body would manufacture a
    //     number, not produce evidence.
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior flag shape - the guard consults only the tombstone
    /// side, so a flag with a long disable history seeds a
    /// <c>HashSet&lt;OrSetDot&gt;</c> from that whole history to dedup the
    /// handful of enable dots being tombstoned.
    /// <para>
    /// Both lanes first take an identical private copy of the tombstone list
    /// (the production method mutates it in place, so a shared list could not be
    /// reused across BDN invocations). That copy is a constant offset present in
    /// both the <c>Mean</c> and <c>Allocated</c> columns on both sides, so the
    /// lane-to-lane delta remains exactly the <c>HashSet</c> the baseline pays.
    /// </para>
    /// </summary>
    [Benchmark(Description = "Flag disable dedup: HashSet (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int FlagDisableDedup_Baseline(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return FlagDisableDedup_Baseline(_liveDots, new List<OrSetDot>(_tombstones));
    }

    /// <summary>
    /// Optimized: the small-side guard replayed onto the flag path - when the
    /// enable-dot list is small (the dominant case: a flag is enabled once or
    /// twice between disables) the dedup runs as an allocation-free linear scan
    /// against the growing tombstone list.
    /// </summary>
    [Benchmark(Description = "Flag disable dedup: small-side linear (optimized)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int FlagDisableDedup_Optimized(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return FlagDisableDedup_Optimized(_liveDots, new List<OrSetDot>(_tombstones));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int FlagDisableDedup_Baseline(List<OrSetDot> enables, List<OrSetDot> tomb)
    {
        var added = 0;
        if (tomb.Count <= OrSetLinearThreshold)
        {
            foreach (var dot in enables)
            {
                if (!tomb.Contains(dot)) { tomb.Add(dot); added++; }
            }
            return added;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        foreach (var dot in enables)
        {
            if (tombSet.Add(dot)) { tomb.Add(dot); added++; }
        }
        return added;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int FlagDisableDedup_Optimized(List<OrSetDot> enables, List<OrSetDot> tomb)
    {
        var added = 0;
        if (tomb.Count <= OrSetLinearThreshold || enables.Count <= OrSetLinearThreshold)
        {
            foreach (var dot in enables)
            {
                if (!tomb.Contains(dot)) { tomb.Add(dot); added++; }
            }
            return added;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        foreach (var dot in enables)
        {
            if (tombSet.Add(dot)) { tomb.Add(dot); added++; }
        }
        return added;
    }

    // ------------------------------------------------------------------
    // (5) BoundedRegister.Clone - the cost of restoring the ICrdt.Clone deep-copy
    //     contract. This pair is a *cost measurement*, not an optimization: the
    //     "shallow" lane is the prior (incorrect) shape that shared the caller's
    //     byte arrays, and the "deep" lane is what now ships. The delta is the
    //     price paid for isolation, reported rather than hidden.
    // ------------------------------------------------------------------

    /// <summary>
    /// Prior shape: <c>Clone</c> copied the value and order-key array
    /// <em>references</em>, so a register handed out of
    /// <c>OrMap&lt;string, BoundedRegister&gt;.Get</c> aliased the map's durable
    /// state. Zero allocation, but it does not honour <c>ICrdt.Clone</c>.
    /// </summary>
    [Benchmark(Description = "BoundedRegister clone: shared arrays (prior)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int BoundedRegisterClone_Shallow(int valueBytes)
    {
        EnsureRegister(valueBytes);
        return CloneShallow(_register).Value!.Length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static BoundedRegister CloneShallow(BoundedRegister source) => new()
    {
        Value = source.Value,
        OrderKey = source.OrderKey,
        HasValue = source.HasValue,
        IsMin = source.IsMin,
    };

    // Lazily (re)builds the register whose value/order-key arrays are the
    // requested width. The order key is held at a fixed 16 bytes (the shape a
    // sequence-stamped key actually has) so the swept axis is the payload.
    private void EnsureRegister(int valueBytes)
    {
        if (_register is not null && _register.Value is { } current && current.Length == valueBytes) return;
        var register = new BoundedRegister();
        register.Set(new byte[valueBytes], new byte[16]);
        _register = register;
    }

    /// <summary>
    /// Shipped shape, variant A: the two byte arrays are copied with
    /// <see cref="Array.Clone"/>, so the clone is fully independent of its
    /// source and <c>ICrdt.Clone</c> holds.
    /// </summary>
    [Benchmark(Description = "BoundedRegister clone: Array.Clone (deep)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int BoundedRegisterClone_ArrayClone(int valueBytes)
    {
        EnsureRegister(valueBytes);
        return CloneViaArrayClone(_register).Value!.Length;
    }

    /// <summary>
    /// Shipped shape, variant B: the same deep copy expressed as a span copy.
    /// <see cref="Array.Clone"/> goes through the non-generic
    /// <see cref="Array"/> path and returns <see cref="object"/>, so it pays a
    /// type check and a castclass the span form does not; this lane exists to
    /// find out whether that is measurable at CRDT payload sizes.
    /// </summary>
    [Benchmark(Description = "BoundedRegister clone: span copy (deep)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int BoundedRegisterClone_SpanCopy(int valueBytes)
    {
        EnsureRegister(valueBytes);
        return CloneViaSpanCopy(_register).Value!.Length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static BoundedRegister CloneViaArrayClone(BoundedRegister source) => new()
    {
        Value = source.Value is null ? null : (byte[])source.Value.Clone(),
        OrderKey = source.OrderKey is null ? null : (byte[])source.OrderKey.Clone(),
        HasValue = source.HasValue,
        IsMin = source.IsMin,
    };

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static BoundedRegister CloneViaSpanCopy(BoundedRegister source) => new()
    {
        Value = source.Value?.AsSpan().ToArray(),
        OrderKey = source.OrderKey?.AsSpan().ToArray(),
        HasValue = source.HasValue,
        IsMin = source.IsMin,
    };

    // ------------------------------------------------------------------
    // (6) OrSetDot.Equals - the record struct declares ReplicaId [Id(0)] before
    //     Counter [Id(1)], so the synthesized equality compares the string
    //     first. Dots authored by the same replica share a ReplicaId, so the
    //     string compare almost never discriminates and the cheap long compare
    //     that would is done second. This pair measures whether reversing the
    //     order is worth touching dot identity for.
    // ------------------------------------------------------------------

    /// <summary>Baseline: string-first comparison, the synthesized order.</summary>
    [Benchmark(Description = "OrSetDot equality: string first (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int DotEquality_StringFirst(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return DotEquality_StringFirst(_tombstones, _liveDots[0]);
    }

    /// <summary>Candidate: long-first comparison.</summary>
    [Benchmark(Description = "OrSetDot equality: counter first (candidate)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int DotEquality_CounterFirst(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return DotEquality_CounterFirst(_tombstones, _liveDots[0]);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int DotEquality_StringFirst(List<OrSetDot> dots, OrSetDot probe)
    {
        var hits = 0;
        for (var i = 0; i < dots.Count; i++)
        {
            var d = dots[i];
            if (string.Equals(d.ReplicaId, probe.ReplicaId, System.StringComparison.Ordinal) && d.Counter == probe.Counter) hits++;
        }
        return hits;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int DotEquality_CounterFirst(List<OrSetDot> dots, OrSetDot probe)
    {
        var hits = 0;
        for (var i = 0; i < dots.Count; i++)
        {
            var d = dots[i];
            if (d.Counter == probe.Counter && string.Equals(d.ReplicaId, probe.ReplicaId, System.StringComparison.Ordinal)) hits++;
        }
        return hits;
    }

    // ------------------------------------------------------------------
    // (7) HashSet construction - presize-then-loop vs the collection ctor's
    //     bulk fill, on the large-both-sides branch that survives finding 1.
    // ------------------------------------------------------------------

    /// <summary>Baseline: capacity ctor followed by a per-item Add loop.</summary>
    [Benchmark(Description = "HashSet build: presize then loop (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int HashSetBuild_PresizeLoop(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return PresizeLoop(_tombstones);
    }

    /// <summary>Candidate: the collection ctor, which bulk-fills.</summary>
    [Benchmark(Description = "HashSet build: collection ctor (candidate)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int HashSetBuild_CollectionCtor(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return CollectionCtor(_tombstones);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int PresizeLoop(List<OrSetDot> tomb)
    {
        var set = new HashSet<OrSetDot>(tomb.Count);
        foreach (var d in tomb) set.Add(d);
        return set.Count;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int CollectionCtor(List<OrSetDot> tomb) => new HashSet<OrSetDot>(tomb).Count;

    // ------------------------------------------------------------------
    // (8) Byte-sequence comparison - the CRDT convergence tie-breaker in
    //     Rga.CompareBytes and MvRegister.CompareValueBytes is a scalar loop,
    //     while BoundedRegister already uses the vectorized span compare. Only
    //     the sign of the result is load-bearing, so the two are interchangeable.
    // ------------------------------------------------------------------

    /// <summary>Baseline: the scalar byte-at-a-time comparison loop.</summary>
    [Benchmark(Description = "Byte compare: scalar loop (baseline)")]
    [Arguments(16)]
    [Arguments(256)]
    [Arguments(1024)]
    public int ByteCompare_Scalar(int valueBytes)
    {
        EnsureComparePair(valueBytes);
        return ScalarCompare(_compareLeft, _compareRight);
    }

    /// <summary>Candidate: <c>SequenceCompareTo</c>, which vectorizes.</summary>
    [Benchmark(Description = "Byte compare: SequenceCompareTo (candidate)")]
    [Arguments(16)]
    [Arguments(256)]
    [Arguments(1024)]
    public int ByteCompare_Vectorized(int valueBytes)
    {
        EnsureComparePair(valueBytes);
        return ((ReadOnlySpan<byte>)_compareLeft).SequenceCompareTo(_compareRight);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int ScalarCompare(byte[] a, byte[] b)
    {
        var n = a.Length < b.Length ? a.Length : b.Length;
        for (var i = 0; i < n; i++)
        {
            if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
        }
        return a.Length.CompareTo(b.Length);
    }

    // Two equal-length buffers that differ only in the final byte: the
    // worst case for the scalar loop and the case the vector path is meant
    // to win, without degenerating into a length-only comparison.
    private void EnsureComparePair(int valueBytes)
    {
        if (_compareLeft is not null && _compareLeft.Length == valueBytes) return;
        _compareLeft = new byte[valueBytes];
        _compareRight = new byte[valueBytes];
        _compareRight[valueBytes - 1] = 1;
    }

    private static bool ListContainsDot(List<OrSetDot> list, OrSetDot dot)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (list[i].Counter == dot.Counter && string.Equals(list[i].ReplicaId, dot.ReplicaId, System.StringComparison.Ordinal))
            {
                return true;
            }
        }
        return false;
    }

    // Lazily (re)builds the shared tombstone list to the requested length. The
    // tombstone dots use a distinct replica id so none of them match a live
    // dot/entry - every live dot survives, isolating the reconciliation cost
    // rather than the count result.
    private void EnsureTombstones(int tombstoneCount)
    {
        if (_tombstones is not null && _tombstones.Count == tombstoneCount) return;
        var tomb = new List<OrSetDot>(tombstoneCount);
        for (var i = 0; i < tombstoneCount; i++)
        {
            tomb.Add(new OrSetDot { ReplicaId = "tomb-" + i.ToString("D6", CultureInfo.InvariantCulture), Counter = i });
        }
        _tombstones = tomb;
    }
}
