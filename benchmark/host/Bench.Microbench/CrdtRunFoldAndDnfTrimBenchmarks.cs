using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the <b>complexity</b> change to the pre-ship CRDT coalescer: folding
/// a key's run of same-key deltas.
/// <para>
/// <b>This is a complexity claim, not a constant-factor one, so read it as a
/// curve.</b> <c>CrdtShape.CombineDeltas</c> is a pairwise running left fold, so
/// union number <c>k</c> re-walks and re-materialises everything unions
/// <c>1..k-1</c> accumulated: both the element walk and the intermediate
/// collections grow with the square of the run length. The lane is parameterised
/// on that run length so the report shows the baseline's Allocated bending
/// upward while the linear arm stays proportional - a shape that is robust to
/// host timing noise in a way a single-width delta is not.
/// </para>
/// <para>
/// <b>Shell fidelity is exact here.</b> Neither arm copies a body: both call the
/// <b>real shipped</b> <see cref="CrdtShape"/> closures through
/// <c>InternalsVisibleTo</c>, over the identical pre-built run, and differ only
/// in which fold the identical surrounding loop drives.
/// </para>
/// <para>
/// Judge it on <b>Allocated</b>. The allocated column reproduces bit-for-bit
/// across rounds; Mean on a shared developer host does not. Nothing here starts
/// a silo, so the suite is cheap enough to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtrunfolds</c> (or
/// <c>--suite crdtrunfolds</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtDeltaRunFoldBenchmarks
{
    /// <summary>Elements each delta in the run contributes.</summary>
    private const int ElementsPerDelta = 8;

    /// <summary>Distinct elements the run draws from, so sources genuinely overlap.</summary>
    private const int ElementPoolSize = 4096;

    private const int ElementBytes = 24;
    private const int ReplicaCount = 4;

    private static readonly CrdtShape OrSetShape = CrdtShape.ForOrSet();
    private static readonly CrdtShape GSetShape = CrdtShape.ForGSet();

    /// <summary>The run length the lane sweeps. The curve is the evidence.</summary>
    [Params(4, 16, 64, 256)]
    public int RunLength { get; set; }

    private object[] _orsetRun = [];
    private object[] _gsetRun = [];

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(20260227);

        var pool = new byte[ElementPoolSize][];
        for (var i = 0; i < ElementPoolSize; i++)
        {
            var element = new byte[ElementBytes];
            rng.NextBytes(element);
            pool[i] = element;
        }

        _orsetRun = new object[RunLength];
        _gsetRun = new object[RunLength];
        for (var d = 0; d < RunLength; d++)
        {
            var adds = new OrSetDeltaDot[ElementsPerDelta];
            var gsetAdds = new List<byte[]>(ElementsPerDelta);
            for (var e = 0; e < ElementsPerDelta; e++)
            {
                var element = pool[((d * ElementsPerDelta) + e) % ElementPoolSize];
                adds[e] = new OrSetDeltaDot
                {
                    Element = element,
                    ReplicaId = "replica-" + (e % ReplicaCount).ToString(),
                    Counter = d + 1,
                };
                gsetAdds.Add(element);
            }

            _orsetRun[d] = new OrSetDelta { Adds = adds, Removes = [] };
            _gsetRun[d] = new GSetDelta { Adds = gsetAdds };
        }

        // Fail loudly rather than publishing a report where the arms are not
        // computing the same thing.
        AssertAgree("OrSet", OrSet_Baseline_PairwiseFold(), OrSet_Optimized_LinearRun());
        AssertAgree("GSet", GSet_Baseline_PairwiseFold(), GSet_Optimized_LinearRun());
    }

    private static void AssertAgree(string lane, int baseline, int optimized)
    {
        if (baseline != optimized)
        {
            throw new InvalidOperationException(
                $"lane {lane} arms disagree: baseline={baseline} optimized={optimized}");
        }
    }

    /// <summary>
    /// The shape before the change: a pairwise running left fold, so the whole
    /// accumulated union is re-walked and re-materialised on every step.
    /// </summary>
    [Benchmark]
    public int OrSet_Baseline_PairwiseFold()
    {
        var combined = _orsetRun[0];
        for (var i = 1; i < _orsetRun.Length; i++)
        {
            combined = OrSetShape.CombineDeltas!(combined, _orsetRun[i]);
        }

        return ((OrSetDelta)combined).Adds.Count;
    }

    /// <summary>
    /// The shipped shape: one accumulator, sized from the run's own summed
    /// width, with every source unioned into it exactly once.
    /// </summary>
    [Benchmark]
    public int OrSet_Optimized_LinearRun()
        => ((OrSetDelta)OrSetShape.CombineDeltaRun!(_orsetRun)).Adds.Count;

    /// <summary>The same fold on the simplest shape, where the element walk dominates.</summary>
    [Benchmark]
    public int GSet_Baseline_PairwiseFold()
    {
        var combined = _gsetRun[0];
        for (var i = 1; i < _gsetRun.Length; i++)
        {
            combined = GSetShape.CombineDeltas!(combined, _gsetRun[i]);
        }

        return ((GSetDelta)combined).Adds.Count;
    }

    /// <inheritdoc cref="OrSet_Optimized_LinearRun"/>
    [Benchmark]
    public int GSet_Optimized_LinearRun()
        => ((GSetDelta)GSetShape.CombineDeltaRun!(_gsetRun)).Adds.Count;
}

/// <summary>
/// Isolates the two <b>allocation</b> changes that ride alongside the run fold:
/// deferring typed-delta deserialisation in pass 1 of the pre-ship CRDT
/// coalescer, and the single-conjunction fast path in the grain-index predicate
/// lowering.
/// <para>
/// <b>Coalesce lanes.</b> Pass 1 previously deserialised the typed delta of
/// <b>every</b> coalescable entry, but pass 2 only consumes a combined delta for
/// a key that actually repeats. On a wide keyspace - where most keys appear once
/// per drain - that built and discarded a whole delta graph per entry. The first
/// occurrence now holds the raw bytes and is deserialised only when a second
/// occurrence arrives. The three arms are laid out so the report attributes the
/// win: baseline to contrast isolates the dictionary probe fold, and contrast to
/// optimized isolates the deferral. Every arm calls the <b>real shipped</b>
/// <c>CrdtShape.ForPnCounter()</c> deserialiser and run fold, so only the
/// bookkeeping differs.
/// </para>
/// <para>
/// <b>DNF lanes.</b> Lowering a predicate to disjunctive normal form allocated a
/// nested singleton pair (an outer list, an inner list, and both backing arrays)
/// for every atom and then folded all but one of them away. A pure
/// <c>&amp;&amp;</c> chain - the shape that dominates in practice - now collects
/// its atoms straight into one conjunction. The baseline reproduces the
/// pre-change lowering, since the shipped one no longer exists, so it is a
/// copied shell; the optimized arm mirrors that shell exactly and differs only
/// in the fast path under test.
/// </para>
/// <para>
/// Judge these on <b>Allocated</b>: both are allocation changes, and the
/// allocated column is what reproduces across rounds on a shared host.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CoalesceDeferAndDnfTrimBenchmarks
{
    private const int ReplicaCount = 4;

    /// <summary>Entries in the simulated drain buffer.</summary>
    private const int DrainWidth = 512;

    /// <summary>
    /// Distinct keys behind those entries. A wide keyspace is the ordinary case:
    /// most keys are written once per drain, and only a few are hot enough to
    /// repeat, so most entries never need a typed delta at all.
    /// </summary>
    private const int DrainKeys = 448;

    /// <summary>Atoms in the pure '&amp;&amp;' chain the DNF lanes lower.</summary>
    private const int ChainAtoms = 6;

    private static readonly CrdtShape PnShape = CrdtShape.ForPnCounter();

    private string[] _drainKeys = [];
    private byte[][] _drainDeltas = [];
    private Expression _chain = Expression.Constant(true);

    [GlobalSetup]
    public void Setup()
    {
        _drainKeys = new string[DrainWidth];
        _drainDeltas = new byte[DrainWidth][];
        for (var i = 0; i < DrainWidth; i++)
        {
            _drainKeys[i] = "key-" + (i % DrainKeys).ToString();
            _drainDeltas[i] = PnShape.SerializeDelta!(new PnCounterDelta
            {
                Increments = new Dictionary<string, long>(StringComparer.Ordinal)
                {
                    ["replica-" + (i % ReplicaCount).ToString()] = i + 1,
                },
                Decrements = new Dictionary<string, long>(StringComparer.Ordinal),
            });
        }

        var parameter = Expression.Parameter(typeof(int), "s");
        Expression chain = Expression.LessThan(parameter, Expression.Constant(0));
        for (var i = 1; i < ChainAtoms; i++)
        {
            chain = Expression.AndAlso(
                chain,
                Expression.LessThan(parameter, Expression.Constant(i)));
        }

        _chain = chain;

        AssertAgree(
            "CoalescePass1",
            CoalescePass1_Baseline_DeserializeEveryEntry(),
            CoalescePass1_Contrast_ProbeFoldOnly(),
            CoalescePass1_Optimized_DeferUntilRepeat());
        AssertAgree(
            "Dnf",
            Dnf_Baseline_NestedSingletonPerAtom(),
            Dnf_Optimized_CollectConjunction());
    }

    private static void AssertAgree(string lane, params int[] results)
    {
        foreach (var result in results)
        {
            if (result != results[0])
            {
                throw new InvalidOperationException(
                    $"lane {lane} arms disagree: [{string.Join(", ", results)}]");
            }
        }
    }

    // ---------------------------------------------------------------------
    // Pass 1 of the coalescer over a wide keyspace.
    // ---------------------------------------------------------------------

    /// <summary>Per-key coalesce state, in the shape pass 1 keeps it.</summary>
    private struct BaselineState
    {
        public List<object>? Run;
        public int FoldCount;
    }

    private struct OptimizedState
    {
        public List<object>? Run;
        public byte[]? FirstDelta;
        public int FoldCount;
    }

    /// <summary>
    /// The shape before the change: deserialise the typed delta of every
    /// coalescable entry, with both probes on the state map kept separate.
    /// </summary>
    [Benchmark]
    public int CoalescePass1_Baseline_DeserializeEveryEntry()
    {
        var states = new Dictionary<string, BaselineState>(StringComparer.Ordinal);
        for (var i = 0; i < _drainKeys.Length; i++)
        {
            var delta = PnShape.DeserializeDelta(_drainDeltas[i]);
            if (states.TryGetValue(_drainKeys[i], out var state))
            {
                state.Run!.Add(delta);
                state.FoldCount++;
                states[_drainKeys[i]] = state;
            }
            else
            {
                states[_drainKeys[i]] = new BaselineState { Run = [delta], FoldCount = 1 };
            }
        }

        return FoldRepeats(states);
    }

    /// <summary>
    /// The cheap half only: fold the two probes into one and still deserialise
    /// every entry. Isolates the probe fold from the deferral, so the report can
    /// show which of the two actually earns the change.
    /// </summary>
    [Benchmark]
    public int CoalescePass1_Contrast_ProbeFoldOnly()
    {
        var states = new Dictionary<string, BaselineState>(StringComparer.Ordinal);
        for (var i = 0; i < _drainKeys.Length; i++)
        {
            var delta = PnShape.DeserializeDelta(_drainDeltas[i]);
            ref var state = ref CollectionsMarshal
                .GetValueRefOrAddDefault(states, _drainKeys[i], out var existed);
            if (!existed)
            {
                state.Run = [delta];
                state.FoldCount = 1;
                continue;
            }

            state.Run!.Add(delta);
            state.FoldCount++;
        }

        return FoldRepeats(states);
    }

    /// <summary>
    /// The shipped shape: hold the first occurrence's raw bytes and deserialise
    /// only once a second occurrence for that key arrives, folding the two
    /// dictionary probes into one while the value slot is being written anyway.
    /// </summary>
    [Benchmark]
    public int CoalescePass1_Optimized_DeferUntilRepeat()
    {
        var states = new Dictionary<string, OptimizedState>(StringComparer.Ordinal);
        for (var i = 0; i < _drainKeys.Length; i++)
        {
            ref var state = ref CollectionsMarshal
                .GetValueRefOrAddDefault(states, _drainKeys[i], out var existed);
            if (!existed)
            {
                state.FirstDelta = _drainDeltas[i];
                state.FoldCount = 1;
                continue;
            }

            if (state.Run is null)
            {
                state.Run = [PnShape.DeserializeDelta(state.FirstDelta!)];
                state.FirstDelta = null;
            }

            state.Run.Add(PnShape.DeserializeDelta(_drainDeltas[i]));
            state.FoldCount++;
        }

        var folded = 0;
        foreach (var state in states.Values)
        {
            if (state.FoldCount >= 2)
            {
                _ = PnShape.CombineDeltaRun!(state.Run!);
                folded++;
            }
        }

        return folded;
    }

    private static int FoldRepeats(Dictionary<string, BaselineState> states)
    {
        var folded = 0;
        foreach (var state in states.Values)
        {
            if (state.FoldCount >= 2)
            {
                _ = PnShape.CombineDeltaRun!(state.Run!);
                folded++;
            }
        }

        return folded;
    }

    // ---------------------------------------------------------------------
    // Lowering a pure '&&' chain to disjunctive normal form.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: every atom becomes a nested singleton pair,
    /// and the distribute fold then collapses all but one of them away.
    /// </summary>
    [Benchmark]
    public int Dnf_Baseline_NestedSingletonPerAtom()
    {
        var lowered = BaselineLower(_chain, negated: false);
        return lowered.Count + lowered[0].Count;
    }

    /// <summary>
    /// The shipped shape: a pure conjunction collects its atoms straight into
    /// one list, so the whole predicate costs one list pair rather than one pair
    /// per atom. Falls back to the general lowering the moment a real
    /// disjunction is reached.
    /// </summary>
    [Benchmark]
    public int Dnf_Optimized_CollectConjunction()
    {
        var single = new List<Expression>();
        if (OptimizedCollect(_chain, negated: false, single))
        {
            return 1 + single.Count;
        }

        var lowered = BaselineLower(_chain, negated: false);
        return lowered.Count + lowered[0].Count;
    }

    private static List<List<Expression>> BaselineLower(Expression expression, bool negated)
    {
        if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Not)
            return BaselineLower(unary.Operand, !negated);

        if (expression is BinaryExpression binary
            && binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
        {
            bool conjunction = (binary.NodeType == ExpressionType.AndAlso) != negated;
            var left = BaselineLower(binary.Left, negated);
            var right = BaselineLower(binary.Right, negated);
            return conjunction ? Distribute(left, right) : Concatenate(left, right);
        }

        return [[expression]];
    }

    private static List<List<Expression>> Distribute(
        List<List<Expression>> left,
        List<List<Expression>> right)
    {
        if (right.Count == 1)
        {
            var appended = right[0];
            for (var i = 0; i < left.Count; i++)
            {
                left[i].AddRange(appended);
            }

            return left;
        }

        if (left.Count == 1)
        {
            var prepended = left[0];
            for (var i = 0; i < right.Count; i++)
            {
                right[i].InsertRange(0, prepended);
            }

            return right;
        }

        var combined = new List<List<Expression>>(left.Count * right.Count);
        for (var i = 0; i < left.Count; i++)
        {
            for (var j = 0; j < right.Count; j++)
            {
                var merged = new List<Expression>(left[i].Count + right[j].Count);
                merged.AddRange(left[i]);
                merged.AddRange(right[j]);
                combined.Add(merged);
            }
        }

        return combined;
    }

    private static List<List<Expression>> Concatenate(
        List<List<Expression>> left,
        List<List<Expression>> right)
    {
        left.AddRange(right);
        return left;
    }

    private static bool OptimizedCollect(Expression expression, bool negated, List<Expression> atoms)
    {
        if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Not)
            return OptimizedCollect(unary.Operand, !negated, atoms);

        if (expression is BinaryExpression binary
            && binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
        {
            if ((binary.NodeType == ExpressionType.AndAlso) == negated)
                return false;

            return OptimizedCollect(binary.Left, negated, atoms)
                && OptimizedCollect(binary.Right, negated, atoms);
        }

        atoms.Add(expression);
        return true;
    }
}
