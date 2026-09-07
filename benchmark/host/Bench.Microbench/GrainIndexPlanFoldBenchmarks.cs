using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.GrainIndex;
using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three output-identical folds made to the <b>grain-index
/// plan-and-execute path</b>: reading a comparison's constant side without
/// invoking the expression compiler, distributing <c>&amp;&amp;</c> over
/// <c>||</c> in place when one side is a single conjunction, and de-duplicating
/// a union's grains through a span probe over the raw entry key.
/// <para>
/// What makes the first fold worth taking is that a query plan is <b>not
/// cached</b>: <c>GrainIndex.Where(...)</c> builds a fresh plan on every call.
/// A bound captured from the surrounding method - which is what real predicates
/// are made of - reaches the tree as a member read over a display-class
/// constant rather than as a bare constant, so the naive path ran the full
/// expression compiler, emitted IL, and reflect-invoked a delegate once per
/// comparison of every query.
/// </para>
/// <para>
/// Judge the constant-fold and union lanes on <b>Allocated</b> and the
/// distribute lane on both: allocation reproduces bit-for-bit across rounds and
/// Mean on a shared developer host does not. The constant-fold lane is the one
/// exception worth reading for time as well, because the effect there is orders
/// of magnitude rather than a fraction and survives any plausible host noise.
/// Nothing here starts a silo, so the suite is cheap enough to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// <para>
/// <b>Shell fidelity.</b> Every baseline arm reproduces its optimized arm's
/// surrounding shell exactly and differs only in the body under test; both arms
/// of a lane are driven from the <b>same</b> pre-built fixture, so neither pays
/// for setup the other skips. The constant-fold lane needs no copy on the
/// optimized side at all: it calls the <b>real shipped</b>
/// <c>ExpressionConstantReader</c> through <c>InternalsVisibleTo</c>, and it
/// additionally ships a <c>_Production</c> arm that plans a real captured-bound
/// lambda end to end through the real shipped
/// <see cref="GrainIndexQueryPlanner"/>, which is what pins the isolated reader
/// figure to what a whole query actually pays.
/// </para>
/// <para>
/// The three edits under test:
/// (1) the constant side of every comparison was evaluated by
/// <c>Expression.Lambda(expr).Compile().DynamicInvoke()</c>, at <b>both</b>
/// seams a predicate passes through - the core
/// <c>LatticePredicateTranslator</c>, which every server-side push-down in the
/// platform goes through, and the grain-index planner's own comparison routing.
/// Both now share one <c>ExpressionConstantReader</c> that folds a constant, or
/// a field or property chain rooted in one, by direct reflection, falling back
/// to compiling only for shapes outside that closed set;
/// (2) converting a predicate to disjunctive normal form distributed
/// <c>&amp;&amp;</c> over <c>||</c> by building a fresh jagged
/// <c>List&lt;List&lt;QueryAtom&gt;&gt;</c> cross product. When either side is a
/// single conjunction - which every <c>&amp;&amp;</c> chain and every
/// <c>&amp;&amp;</c> against a union is - it now appends into the existing lists
/// in place, so an N-atom chain allocates one conjunction rather than N;
/// (3) a union de-duplicated its grains through a <c>HashSet&lt;string&gt;</c>
/// that had to be handed a grain-key <c>string</c>, so every scanned entry paid
/// for a substring purely to probe it. It now probes through the set's
/// <c>ReadOnlySpan&lt;char&gt;</c> alternate lookup over the tree's own key
/// string, materialising a string only on the first sighting of a grain, and
/// scans keys rather than materialised matches.
/// </para>
/// <para>
/// <b>Contrast arm.</b> <see cref="ConstantFold_Contrast_BareConstantOnly"/>
/// takes the obvious cheap half of edit (1) - fold a bare
/// <see cref="ConstantExpression"/> and compile everything else - so the report
/// can show that the member-chain walk is what actually earns the change, since
/// a captured bound is never a bare constant.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=grainindexplanfolds</c> (or
/// <c>--suite grainindexplanfolds</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class GrainIndexPlanFoldBenchmarks
{
    /// <summary>
    /// Comparisons in the predicate whose constant side has to be evaluated. A
    /// three-property conjunction with a range on one of them is four.
    /// </summary>
    private const int Comparisons = 4;

    /// <summary>Atoms in the <c>&amp;&amp;</c> chain the distribute lane folds.</summary>
    private const int ChainAtoms = 6;

    /// <summary>Entries a union's branches yield in total.</summary>
    private const int UnionWidth = 8192;

    /// <summary>
    /// Distinct grains behind those entries. A union over correlated properties
    /// reports most grains from more than one branch, which is the whole reason
    /// the de-duplication set exists.
    /// </summary>
    private const int UnionGrains = 3072;

    private static readonly int StaticBound = 18;

    private Expression[] _constantSides = [];
    private Expression<Func<BenchFoldState, bool>> _capturedPredicate = _ => true;
    private GrainIndexQueryProperty[] _properties = [];
    private string[] _propertyNames = [];

    private string[] _unionKeys = [];

    private Expression<Func<BenchFoldState, bool>> _scanPredicate = _ => true;
    private ParameterExpression _parameterTarget = Expression.Parameter(typeof(BenchFoldState), "s");
    private Expression[] _parameterOperands = [];

    [GlobalSetup]
    public void Setup()
    {
        // The constant sides a real captured-bound predicate presents: a bare
        // literal, a captured local (a display-class field read), a field two
        // levels down a captured object, and a property read. Only the first is
        // a ConstantExpression; the rest are member chains rooted in one, which
        // is exactly the shape the naive path compiled.
        int captured = 18;
        var bound = new Bound { Age = 30, Country = "GB" };

        Expression<Func<BenchFoldState, bool>> literal = s => s.Age >= 21;
        Expression<Func<BenchFoldState, bool>> local = s => s.Age >= captured;
        Expression<Func<BenchFoldState, bool>> nested = s => s.Country == bound.Country;
        Expression<Func<BenchFoldState, bool>> statik = s => s.Age >= StaticBound;

        _constantSides =
        [
            ((BinaryExpression)literal.Body).Right,
            ((BinaryExpression)local.Body).Right,
            ((BinaryExpression)nested.Body).Right,
            ((BinaryExpression)statik.Body).Right,
        ];

        _capturedPredicate = s => s.Age >= captured && s.Country == bound.Country && s.Status == 2;

        _properties =
        [
            new GrainIndexQueryProperty(0, "Age", typeof(int)),
            new GrainIndexQueryProperty(1, "Country", typeof(string)),
            new GrainIndexQueryProperty(2, "Score", typeof(double)),
            new GrainIndexQueryProperty(3, "Status", typeof(int)),
        ];
        _propertyNames = ["Age", "Country", "Score", "Status"];

        // The union lane's fixture: the raw entry keys a union's branches hand
        // back, in the order they arrive. Grains repeat across branches, so the
        // set sees both first sightings and repeats in a realistic proportion.
        var random = new Random(20260301);
        _unionKeys = new string[UnionWidth];
        for (var i = 0; i < UnionWidth; i++)
        {
            int grain = i < UnionGrains ? i : random.Next(UnionGrains);
            _unionKeys[i] = GrainIndexKeyEncoder.ComposeKey(
                (i & 1) == 0 ? "Country" : "Age",
                GrainIndexKeyEncoder.EncodeValue(grain % 97),
                GrainKey(grain));
        }

        // The parameter-scan lane's fixture: the operand pairs the planner
        // actually interrogates while routing a four-atom predicate. Each atom
        // contributes both sides - the parameter-rooted member chain the planner
        // must recognise, and the captured constant side it must reject - which
        // is precisely the two calls per atom the router makes.
        _scanPredicate = s => s.Age >= captured
            && s.Status == 2
#pragma warning disable CA1310 // The planner supports only the ordinal single-argument overload.
            && s.Country.StartsWith(bound.Country)
#pragma warning restore CA1310
            && s.Score < 99.5;

        _parameterTarget = _scanPredicate.Parameters[0];

        var operands = new List<Expression>();
        CollectOperands(_scanPredicate.Body, operands);
        _parameterOperands = [.. operands];
    }

    /// <summary>
    /// Walks the conjunction the same way the planner's router does, recording
    /// the operand pairs it would ask about.
    /// </summary>
    private static void CollectOperands(Expression expression, List<Expression> operands)
    {
        if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.OrElse } chain)
        {
            CollectOperands(chain.Left, operands);
            CollectOperands(chain.Right, operands);
            return;
        }

        switch (expression)
        {
            case BinaryExpression comparison:
                operands.Add(comparison.Left);
                operands.Add(comparison.Right);
                break;
            case MethodCallExpression { Object: not null } call:
                operands.Add(call.Object);
                for (var i = 0; i < call.Arguments.Count; i++)
                {
                    operands.Add(call.Arguments[i]);
                }

                break;
            default:
                operands.Add(expression);
                break;
        }
    }

    // ---------------------------------------------------------------------
    // Lane 1: evaluating the constant side of a comparison.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: build a lambda over the constant side, run
    /// the expression compiler over it to emit IL, and reflect-invoke the
    /// resulting delegate once - per comparison, per query.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int ConstantFold_Baseline_CompileAndInvoke()
    {
        var total = 0;
        for (var i = 0; i < Comparisons; i++)
        {
            object? value = Expression.Lambda(_constantSides[i]).Compile().DynamicInvoke();
            total += value is null ? 0 : 1;
        }

        return total;
    }

    /// <summary>
    /// The shipped shape: the <b>real</b> <c>ExpressionConstantReader</c>, which
    /// folds a constant or a field or property chain rooted in one by direct
    /// reflection and compiles only what falls outside that closed set. It is
    /// reached through <c>InternalsVisibleTo</c>, so this arm runs production
    /// code rather than a copy of it.
    /// </summary>
    [Benchmark]
    public int ConstantFold_Optimized_ReflectChain()
    {
        var total = 0;
        for (var i = 0; i < Comparisons; i++)
        {
            object? value = ExpressionConstantReader.Read(_constantSides[i]);
            total += value is null ? 0 : 1;
        }

        return total;
    }

    /// <summary>
    /// The rejected cheaper alternative: fold only a bare
    /// <see cref="ConstantExpression"/>. It is the change everyone reaches for
    /// first, and it does nothing for a captured bound, which is never a bare
    /// constant.
    /// </summary>
    [Benchmark]
    public int ConstantFold_Contrast_BareConstantOnly()
    {
        var total = 0;
        for (var i = 0; i < Comparisons; i++)
        {
            var expression = _constantSides[i];
            object? value = expression is ConstantExpression constant
                ? constant.Value
                : Expression.Lambda(expression).Compile().DynamicInvoke();

            total += value is null ? 0 : 1;
        }

        return total;
    }

    /// <summary>
    /// The real shipped planner, end to end over a real captured-bound lambda -
    /// the predicate an application actually writes. This is what pins the
    /// copied fold shell above to production.
    /// </summary>
    [Benchmark]
    public int ConstantFold_Production_RealPlanner()
    {
        var plan = GrainIndexQueryPlanner.Build(_capturedPredicate, "Bench", _properties, _propertyNames);
        return plan.Disjuncts.Length;
    }

    // ---------------------------------------------------------------------
    // Lane 2: distributing AND over OR when building disjunctive normal form.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: every <c>&amp;&amp;</c> level built a fresh
    /// jagged cross product, so an N-atom chain allocated N conjunction lists
    /// and copied the accumulated atoms into each of them.
    /// </summary>
    [Benchmark]
    public int Distribute_Baseline_CrossProduct()
    {
        var accumulated = Leaf(0);
        for (var i = 1; i < ChainAtoms; i++)
        {
            accumulated = BaselineDistribute(accumulated, Leaf(i));
        }

        // The union case: one more level, against a two-branch disjunction.
        accumulated = BaselineDistribute(accumulated, Union(ChainAtoms));

        return accumulated.Count + accumulated[0].Count;
    }

    /// <summary>
    /// The shipped shape: when either side is a single conjunction, append into
    /// the lists already built rather than allocating a cross product. The
    /// general product still runs when both sides are genuinely unions.
    /// </summary>
    [Benchmark]
    public int Distribute_Optimized_InPlace()
    {
        var accumulated = Leaf(0);
        for (var i = 1; i < ChainAtoms; i++)
        {
            accumulated = OptimizedDistribute(accumulated, Leaf(i));
        }

        accumulated = OptimizedDistribute(accumulated, Union(ChainAtoms));

        return accumulated.Count + accumulated[0].Count;
    }

    // ---------------------------------------------------------------------
    // Lane 3: de-duplicating a union's grains.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: materialise a grain-key substring for every
    /// entry the union yields, purely so a <c>HashSet&lt;string&gt;</c> can be
    /// probed with it, and carry each one in a match.
    /// </summary>
    [Benchmark]
    public int UnionDedup_Baseline_SubstringPerEntry()
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var reported = 0;

        for (var i = 0; i < _unionKeys.Length; i++)
        {
            if (!TryReadGrainKey(_unionKeys[i], out ReadOnlySpan<char> span))
                continue;

            string grainKey = new(span);
            if (!seen.Add(grainKey))
                continue;

            _ = new GrainIndexMatch(grainKey, "union", []);
            reported++;
        }

        return reported;
    }

    /// <summary>
    /// The shipped shape: probe through the set's span alternate lookup over the
    /// tree's own key string, so a string is materialised only on a grain's
    /// first sighting, and hand that same instance back.
    /// </summary>
    [Benchmark]
    public int UnionDedup_Optimized_SpanProbe()
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var lookup = seen.GetAlternateLookup<ReadOnlySpan<char>>();
        var reported = 0;

        for (var i = 0; i < _unionKeys.Length; i++)
        {
            if (!TryReadGrainKey(_unionKeys[i], out ReadOnlySpan<char> span))
                continue;

            if (!lookup.Add(span))
                continue;

            lookup.TryGetValue(span, out string? grainKey);
            _ = new GrainIndexMatch(grainKey!, "union", []);
            reported++;
        }

        return reported;
    }

    // ---------------------------------------------------------------------
    // Lane 4: answering "does this operand mention the lambda parameter?".
    // ---------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: allocate a fresh
    /// <see cref="ExpressionVisitor"/> per question and walk the whole operand
    /// sub-tree even once the answer is known. The planner asks this up to twice
    /// per binary atom while routing a predicate, so a four-atom conjunction
    /// pays for eight visitors per query.
    /// </summary>
    [Benchmark]
    public int ParameterScan_Baseline_VisitorPerCall()
    {
        var found = 0;
        for (var i = 0; i < _parameterOperands.Length; i++)
        {
            if (BaselineParameterFinder.Contains(_parameterOperands[i], _parameterTarget))
                found++;
        }

        return found;
    }

    /// <summary>
    /// The shipped shape: a static structural walk over the node kinds a
    /// routable predicate is built from, returning the moment the parameter is
    /// seen and allocating nothing. Unrecognised node kinds fall back to the
    /// visitor, so no operand answers <c>false</c> that previously answered
    /// <c>true</c>.
    /// </summary>
    [Benchmark]
    public int ParameterScan_Optimized_StaticWalk()
    {
        var found = 0;
        for (var i = 0; i < _parameterOperands.Length; i++)
        {
            if (StaticContainsParameter(_parameterOperands[i], _parameterTarget))
                found++;
        }

        return found;
    }

    /// <summary>
    /// The cheaper alternative that was considered and rejected: keep the
    /// visitor but short-circuit the common case where the operand is the
    /// parameter itself or a member chain rooted directly in it. It removes the
    /// allocation only for the operands that were already cheapest, and leaves
    /// the constant side - the operand that costs the most to walk - unchanged.
    /// </summary>
    [Benchmark]
    public int ParameterScan_Contrast_MemberChainShortcut()
    {
        var found = 0;
        for (var i = 0; i < _parameterOperands.Length; i++)
        {
            var expression = _parameterOperands[i];
            while (expression is MemberExpression { Expression: not null } member)
            {
                expression = member.Expression;
            }

            if (ReferenceEquals(expression, _parameterTarget))
            {
                found++;
                continue;
            }

            if (BaselineParameterFinder.Contains(_parameterOperands[i], _parameterTarget))
                found++;
        }

        return found;
    }

    /// <summary>
    /// The real shipped planner over a wider predicate than the constant-fold
    /// lane's - four atoms, one of them a string method call - so the parameter
    /// scan is exercised at production call depth. This is what pins the copied
    /// walk above to the code that actually ships.
    /// </summary>
    [Benchmark]
    public int ParameterScan_Production_RealPlanner()
    {
        var plan = GrainIndexQueryPlanner.Build(_scanPredicate, "Bench", _properties, _propertyNames);
        return plan.Disjuncts.Length;
    }

    // ---------------------------------------------------------------------
    // Reproduced shells and fixtures.
    // ---------------------------------------------------------------------

    /// <summary>
    /// A verbatim copy of the planner's prior <c>ParameterFinder</c>, kept so the
    /// baseline arm measures the shape that actually shipped before the change.
    /// </summary>
    private sealed class BaselineParameterFinder : ExpressionVisitor
    {
        private readonly ParameterExpression _target;
        private bool _found;

        private BaselineParameterFinder(ParameterExpression target) => _target = target;

        internal static bool Contains(Expression expression, ParameterExpression target)
        {
            var finder = new BaselineParameterFinder(target);
            finder.Visit(expression);
            return finder._found;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _target)
            {
                _found = true;
            }

            return base.VisitParameter(node);
        }
    }

    /// <summary>
    /// A copy of the shipped static walk. The production copy is
    /// <c>private</c> to the planner, so the arm above reproduces it rather than
    /// calling it; the <c>ParameterScan_Production_RealPlanner</c> arm pins the
    /// two together end to end.
    /// </summary>
    private static bool StaticContainsParameter(Expression? expression, ParameterExpression target)
    {
        switch (expression)
        {
            case null:
                return false;
            case ParameterExpression parameterExpression:
                return ReferenceEquals(parameterExpression, target);
            case ConstantExpression:
                return false;
            case UnaryExpression unary:
                return StaticContainsParameter(unary.Operand, target);
            case MemberExpression member:
                return StaticContainsParameter(member.Expression, target);
            case TypeBinaryExpression typeBinary:
                return StaticContainsParameter(typeBinary.Expression, target);
            case BinaryExpression binary:
                return StaticContainsParameter(binary.Left, target)
                    || StaticContainsParameter(binary.Right, target)
                    || StaticContainsParameter(binary.Conversion, target);
            case ConditionalExpression conditional:
                return StaticContainsParameter(conditional.Test, target)
                    || StaticContainsParameter(conditional.IfTrue, target)
                    || StaticContainsParameter(conditional.IfFalse, target);
            case MethodCallExpression call:
                return StaticContainsParameter(call.Object, target)
                    || StaticContainsAnyParameter(call.Arguments, target);
            case InvocationExpression invocation:
                return StaticContainsParameter(invocation.Expression, target)
                    || StaticContainsAnyParameter(invocation.Arguments, target);
            case NewExpression construction:
                return StaticContainsAnyParameter(construction.Arguments, target);
            case NewArrayExpression newArray:
                return StaticContainsAnyParameter(newArray.Expressions, target);
            case IndexExpression index:
                return StaticContainsParameter(index.Object, target)
                    || StaticContainsAnyParameter(index.Arguments, target);
            case LambdaExpression lambda:
                return StaticContainsAnyParameter(lambda.Parameters, target)
                    || StaticContainsParameter(lambda.Body, target);
            default:
                return BaselineParameterFinder.Contains(expression, target);
        }
    }

    private static bool StaticContainsAnyParameter<TExpression>(
        ReadOnlyCollection<TExpression> expressions,
        ParameterExpression target)
        where TExpression : Expression
    {
        for (var i = 0; i < expressions.Count; i++)
        {
            if (StaticContainsParameter(expressions[i], target))
                return true;
        }

        return false;
    }

    private static List<List<int>> Leaf(int atom) => [[atom]];

    private static List<List<int>> Union(int atom) => [[atom], [atom + 1]];

    private static List<List<int>> BaselineDistribute(List<List<int>> left, List<List<int>> right)
    {
        var product = new List<List<int>>(left.Count * right.Count);
        for (var i = 0; i < left.Count; i++)
        {
            for (var j = 0; j < right.Count; j++)
            {
                var combined = new List<int>(left[i].Count + right[j].Count);
                combined.AddRange(left[i]);
                combined.AddRange(right[j]);
                product.Add(combined);
            }
        }

        return product;
    }

    private static List<List<int>> OptimizedDistribute(List<List<int>> left, List<List<int>> right)
    {
        if (right.Count == 1)
        {
            var only = right[0];
            for (var i = 0; i < left.Count; i++)
            {
                left[i].AddRange(only);
            }

            return left;
        }

        if (left.Count == 1)
        {
            var only = left[0];
            for (var j = 0; j < right.Count; j++)
            {
                right[j].InsertRange(0, only);
            }

            return right;
        }

        return BaselineDistribute(left, right);
    }

    private static bool TryReadGrainKey(string key, out ReadOnlySpan<char> grainKey)
    {
        int first = key.IndexOf(GrainIndexKeyEncoder.Separator);
        if (first < 0)
        {
            grainKey = default;
            return false;
        }

        int second = key.IndexOf(GrainIndexKeyEncoder.Separator, first + 1);
        if (second < 0)
        {
            grainKey = default;
            return false;
        }

        grainKey = key.AsSpan(second + 1);
        return true;
    }

    private static string GrainKey(int ordinal) =>
        string.Create(
            null,
            stackalloc char[32],
            $"subject-{ordinal:D8}");

    private sealed class Bound
    {
        internal int Age { get; init; }

        internal string Country { get; init; } = string.Empty;
    }

    /// <summary>The state shape the constant-fold lane plans against.</summary>
    public sealed class BenchFoldState
    {
        /// <summary>An order-preserving integer property.</summary>
        public int Age { get; set; }

        /// <summary>A string property, which routes as an exact range.</summary>
        public string Country { get; set; } = string.Empty;

        /// <summary>A floating-point property, which keeps its residual predicate.</summary>
        public double Score { get; set; }

        /// <summary>A second integer property, so the conjunction spans three.</summary>
        public int Status { get; set; }
    }
}
