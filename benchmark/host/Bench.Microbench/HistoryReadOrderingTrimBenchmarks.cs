using System;
using System.Text;
using System.Text.Json;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three per-row costs the read path pays, so their time and byte
/// deltas are measurable in the clear rather than buried under a silo, a grain
/// call, and a transport.
/// <para>
/// (1) <b>Predicate validation ordering.</b> Every row a predicate-filtered
/// scan touches is folded by <c>LatticePredicateEvaluator.Matches</c>. On its
/// zero-allocation fast path that call ran a full forward
/// <c>Utf8JsonReader</c> token scan of the whole value purely to reproduce the
/// "must be well-formed JSON, else false" gate, and only then evaluated the
/// tree. The two gates are not independent: a malformed payload and a predicate
/// that folds to false answer the same <c>false</c>, so the validation scan is
/// observable only on a row the tree admits. Deferring it behind the evaluation
/// preserves the contract exactly - an admitted row is still validated before
/// it is returned, and malformed input still throws and is caught as a
/// non-match - while a selective scan stops paying a whole-document scan for
/// every row it rejects. The lanes sweep a selective predicate and, as a
/// control, one that admits every row and must therefore show no win.
/// </para>
/// <para>
/// (2) <b>Fast-path eligibility hoisting.</b> Choosing between the two
/// evaluators means walking the predicate tree to check that every member path
/// is a single top-level property name. That walk is a pure function of the
/// predicate, so it is loop-invariant for a scan - yet it ran once per row,
/// rediscovering an answer that cannot change. The leaf read paths now resolve
/// it once per call and pass it down. These lanes fold identical rows through
/// the per-row and hoisted forms.
/// </para>
/// <para>
/// (3) <b>Entry-history per-revision delta wrapper.</b> An entry-history page
/// decodes each retained CRDT revision separately, and each one handed its
/// single delta to the decoder as a freshly minted one-element array. The
/// decoders are pure functions over the list they are handed - every decoder in
/// the family iterates it and retains nothing, and the interface now says so -
/// so one page-scoped buffer carries the whole page.
/// </para>
/// <para>
/// Read group (3) for <b>bytes</b> and groups (1) and (2) for <b>time</b>: the
/// first two remove scanning and dispatch rather than heap traffic, so their
/// byte columns are expected to be identical and a claimed byte win there would
/// be noise. Each baseline lane is asserted in <see cref="Setup"/> to produce
/// exactly the answer its shipped counterpart produces - a lane that answers
/// differently is measuring different work, and the comparison would be void.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=historyreadtrims</c> (or
/// <c>--suite historyreadtrims</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class HistoryReadOrderingTrimBenchmarks
{
    // One leaf page's rows carrying a small JSON document, the shape a
    // predicate-filtered range scan folds row by row.
    private const int PredicateRowCount = 256;

    // One history page's worth of revisions - the page size the state query
    // clamps a history read to by default.
    private const int RevisionCount = 64;

    private const string ReplicaA = "replica-a";

    private byte[][] _rows = null!;
    private LatticePredicateNode _selectivePredicate;
    private LatticePredicateNode _admitAllPredicate;
    private bool _selectiveEligible;
    private bool _admitAllEligible;

    private CrdtProvenanceDelta[] _revisionDeltas = null!;

    /// <summary>Builds the rows, predicates and revisions the lanes fold.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // A predicate-filtered scan's per-row payload: a small JSON object with
        // the member the predicate reads plus a few unread siblings, which is
        // what makes a whole-document validation scan cost more than the member
        // resolution the evaluation actually needs.
        _rows = new byte[PredicateRowCount][];
        for (var i = 0; i < PredicateRowCount; i++)
        {
            _rows[i] = Encoding.UTF8.GetBytes(
                $"{{\"rank\":{i},\"name\":\"row-{i:D6}\",\"tag\":\"alpha\",\"active\":true,\"score\":{i * 3}}}");
        }

        // Selective: admits 1 row in 256, the shape a range scan with a real
        // filter walks. The control admits every row, so the two lanes bracket
        // the selectivity range rather than reporting one point on it.
        _selectivePredicate = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("rank"),
            LatticePredicateNode.Const(LatticeConstant.Integer(PredicateRowCount - 1)));

        _admitAllPredicate = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("rank"),
            LatticePredicateNode.Const(LatticeConstant.Integer(0)));

        _selectiveEligible = LatticePredicateEvaluator.IsFastPathEligible(_selectivePredicate);
        _admitAllEligible = LatticePredicateEvaluator.IsFastPathEligible(_admitAllPredicate);
        if (!_selectiveEligible || !_admitAllEligible)
        {
            throw new InvalidOperationException(
                "Both benchmark predicates must be fast-path eligible; otherwise the lanes measure the JsonDocument path.");
        }

        _revisionDeltas = new CrdtProvenanceDelta[RevisionCount];
        for (var i = 0; i < RevisionCount; i++)
        {
            _revisionDeltas[i] = new CrdtProvenanceDelta(MakeRevisionDelta(i), HybridLogicalClock.Zero);
        }

        // A baseline lane is only honest evidence if it answers exactly what the
        // shipped lane answers. Assert that here rather than trusting the
        // reproduction by eye.
        AssertSameCount(
            CountValidateFirst(_selectivePredicate),
            CountPerRowEligibility(_selectivePredicate),
            "selective predicate, validation ordering");
        AssertSameCount(
            CountPerRowEligibility(_selectivePredicate),
            CountHoistedEligibility(_selectivePredicate, _selectiveEligible),
            "selective predicate, eligibility hoist");
        AssertSameCount(
            CountValidateFirst(_admitAllPredicate),
            CountHoistedEligibility(_admitAllPredicate, _admitAllEligible),
            "admit-all predicate");

        // The ordering swap has to agree on the inputs the validation pass
        // exists for. A payload that is not well-formed JSON answers false
        // either way, including one whose malformed region sits past the point
        // the tree short-circuits at - which is exactly the case that forces the
        // shipped path to validate before admitting a row.
        AssertRejects(Encoding.UTF8.GetBytes("{\"rank\":9999,"), "truncated document");
        AssertRejects(Encoding.UTF8.GetBytes("{\"rank\":9999} trailing"), "trailing garbage");
        AssertRejects(Encoding.UTF8.GetBytes("{\"rank\":9999,\"name\":}"), "malformed past the short-circuit");

        var baselineRevisions = HistoryRevisionsBaseline();
        var shippedRevisions = HistoryRevisionsShipped();
        if (baselineRevisions != shippedRevisions)
        {
            throw new InvalidOperationException(
                $"[history revision decode] baseline emitted {baselineRevisions} events, shipped emitted {shippedRevisions}.");
        }
    }

    // ========================================================================
    // (1) predicate validation ordering
    // ========================================================================

    /// <summary>
    /// The prior shape on a <b>selective</b> predicate: validate the whole
    /// document, then evaluate. Composed from the real production
    /// <c>Validate</c> and <c>EvaluateBoolean</c> members behind the real
    /// production eligibility check, so the baseline is the prior method body
    /// rather than a paraphrase of it.
    /// </summary>
    [Benchmark]
    public int PredicateOrderSelective_Baseline_ValidateThenEvaluate()
        => CountValidateFirst(_selectivePredicate);

    /// <summary>
    /// The shipped ordering on the same predicate, through the <b>real
    /// production</b> <c>Matches</c>: evaluate first, validate only the rows
    /// the tree admits. Eligibility is still resolved per row in both lanes, so
    /// this pair isolates the ordering and nothing else.
    /// </summary>
    [Benchmark]
    public int PredicateOrderSelective_Optimized_EvaluateThenValidate()
        => CountPerRowEligibility(_selectivePredicate);

    /// <summary>
    /// The control: a predicate that admits every row, where the shipped
    /// ordering validates exactly as often as the prior one did and the two
    /// lanes must agree. A lane that "wins" here is measuring noise.
    /// </summary>
    [Benchmark]
    public int PredicateOrderAdmitAll_Baseline_ValidateThenEvaluate()
        => CountValidateFirst(_admitAllPredicate);

    /// <summary>The shipped ordering on the admit-all control.</summary>
    [Benchmark]
    public int PredicateOrderAdmitAll_Optimized_EvaluateThenValidate()
        => CountPerRowEligibility(_admitAllPredicate);

    // ========================================================================
    // (2) fast-path eligibility hoisting
    // ========================================================================

    /// <summary>
    /// The prior shape: the parameterless <c>Matches</c>, which re-walks the
    /// whole predicate tree on every row to rediscover a loop-invariant answer.
    /// </summary>
    [Benchmark]
    public int PredicateEligibility_Baseline_PerRow()
        => CountPerRowEligibility(_selectivePredicate);

    /// <summary>
    /// The shipped shape: eligibility resolved once for the call and passed to
    /// the overload, as the leaf read paths now do.
    /// </summary>
    [Benchmark]
    public int PredicateEligibility_Optimized_Hoisted()
        => CountHoistedEligibility(_selectivePredicate, _selectiveEligible);

    // ========================================================================
    // (3) entry-history per-revision delta wrapper
    // ========================================================================

    /// <summary>
    /// The prior shape: a page of revisions, each minting a fresh one-element
    /// <c>CrdtProvenanceDelta[]</c> to hand the <b>real production</b> decoder.
    /// </summary>
    [Benchmark]
    public int HistoryRevisionDecode_Baseline_ArrayPerRevision() => HistoryRevisionsBaseline();

    /// <summary>
    /// The shipped shape: the same page through the same decoder, with one
    /// page-scoped buffer carrying every revision.
    /// </summary>
    [Benchmark]
    public int HistoryRevisionDecode_Optimized_PageScopedBuffer() => HistoryRevisionsShipped();

    // ========================================================================
    // lane bodies
    // ========================================================================

    private int CountValidateFirst(in LatticePredicateNode predicate)
    {
        var admitted = 0;
        for (var i = 0; i < _rows.Length; i++)
        {
            if (MatchesValidateFirst(_rows[i], predicate)) admitted++;
        }

        return admitted;
    }

    private int CountPerRowEligibility(in LatticePredicateNode predicate)
    {
        var admitted = 0;
        for (var i = 0; i < _rows.Length; i++)
        {
            if (LatticePredicateEvaluator.Matches(_rows[i], predicate)) admitted++;
        }

        return admitted;
    }

    private int CountHoistedEligibility(in LatticePredicateNode predicate, bool eligible)
    {
        var admitted = 0;
        for (var i = 0; i < _rows.Length; i++)
        {
            if (LatticePredicateEvaluator.Matches(_rows[i], predicate, eligible)) admitted++;
        }

        return admitted;
    }

    // The prior fast-path body, verbatim: the per-row eligibility walk, then a
    // full forward token scan of the value to reproduce the well-formedness
    // gate, then the fold.
    private static bool MatchesValidateFirst(byte[] value, in LatticePredicateNode predicate)
    {
        if (value.Length == 0) return false;

        if (LatticePredicateEvaluator.IsFastPathEligible(predicate))
        {
            try
            {
                ReadOnlySpan<byte> json = value;
                LatticePredicateEvaluator.Validate(json);
                return LatticePredicateEvaluator.EvaluateBoolean(predicate, json, 0);
            }
            catch (JsonException)
            {
                return false;
            }
        }

        throw new InvalidOperationException(
            "The benchmark predicates are fast-path eligible by construction; the slow path is not measured here.");
    }

    private int HistoryRevisionsBaseline()
    {
        var total = 0;
        for (var i = 0; i < _revisionDeltas.Length; i++)
        {
            total += OrMapProvenanceDecoder.Instance.DecodeDeltas(new[] { _revisionDeltas[i] }).Count;
        }

        return total;
    }

    private int HistoryRevisionsShipped()
    {
        var total = 0;
        var buffer = new CrdtProvenanceDelta[1];
        for (var i = 0; i < _revisionDeltas.Length; i++)
        {
            buffer[0] = _revisionDeltas[i];
            total += OrMapProvenanceDecoder.Instance.DecodeDeltas(buffer).Count;
        }

        return total;
    }

    // ========================================================================
    // shared helpers
    // ========================================================================

    private static OrMapDelta<string, GCounter> MakeRevisionDelta(int index)
    {
        // The steady-state shape a single mutation ships: one dot-tagged key
        // snapshot, plus the tombstone for the value it superseded.
        return new OrMapDelta<string, GCounter>
        {
            Adds = new[]
            {
                new OrMapDeltaEntry<string, GCounter>
                {
                    Key = $"key-{index:D4}",
                    ReplicaId = ReplicaA,
                    Counter = index + 1,
                    Value = new GCounter(),
                },
            },
            Tombstones = new[]
            {
                new OrMapDeltaTombstone<string>
                {
                    Key = $"key-{index:D4}",
                    ReplicaId = ReplicaA,
                    Counter = index,
                },
            },
        };
    }

    private static void AssertSameCount(int baseline, int shipped, string lane)
    {
        if (baseline != shipped)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline admitted {baseline} rows, shipped admitted {shipped}.");
        }
    }

    private void AssertRejects(byte[] malformed, string lane)
    {
        if (MatchesValidateFirst(malformed, _admitAllPredicate)
            || LatticePredicateEvaluator.Matches(malformed, _admitAllPredicate)
            || LatticePredicateEvaluator.Matches(malformed, _admitAllPredicate, _admitAllEligible))
        {
            throw new InvalidOperationException($"[{lane}] a malformed payload was admitted.");
        }
    }
}
