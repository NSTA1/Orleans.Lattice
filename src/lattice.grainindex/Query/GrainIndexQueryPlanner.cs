using System.Linq.Expressions;

namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// Lowers a user predicate into a <see cref="GrainIndexQueryPlan"/>: a union of
/// conjunctions, each a set of per-property key-range scans.
/// <para>
/// The planner exists because of one property of the entry encoding: an index
/// entry carries exactly <b>one</b> projected property, plus metadata fields no
/// lambda can name. A predicate naming a second property therefore matches no
/// entry at all, so a conjunction across two properties cannot be pushed down as
/// one predicate - it has to become two scans whose grain keys are intersected.
/// The planner is what turns the user's single lambda into that shape.
/// </para>
/// <para>
/// Every expression is validated through
/// <see cref="LatticePredicateTranslator"/>, so an unsupported construct fails
/// with the core dialect's own <see cref="NotSupportedException"/> and users see
/// one predicate language across Lattice rather than two.
/// </para>
/// </summary>
internal static class GrainIndexQueryPlanner
{
    /// <summary>
    /// The ceiling on conjunctions produced while distributing <c>&amp;&amp;</c>
    /// over <c>||</c>. A deeply alternating predicate expands combinatorially,
    /// and a plan with thousands of scans is never what the caller wanted.
    /// </summary>
    internal const int MaxConjunctions = 64;

    /// <summary>
    /// Builds the plan for <paramref name="predicate"/> against
    /// <paramref name="properties"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">The predicate uses a construct the index cannot route.</exception>
    /// <exception cref="GrainIndexPropertyNotIndexedException">The predicate names an unprojected property.</exception>
    internal static GrainIndexQueryPlan Build<TState>(
        Expression<Func<TState, bool>> predicate,
        string indexName,
        GrainIndexQueryProperty[] properties,
        IReadOnlyList<string> propertyNames)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var parameter = predicate.Parameters[0];
        var normalized = ToDisjunctiveNormalForm(predicate.Body, negated: false);

        List<GrainIndexConjunction>? disjuncts = null;
        for (var i = 0; i < normalized.Count; i++)
        {
            var conjunction = BuildConjunction<TState>(
                normalized[i],
                parameter,
                indexName,
                properties,
                propertyNames);

            if (conjunction is null)
                continue;

            disjuncts ??= new List<GrainIndexConjunction>(normalized.Count);
            disjuncts.Add(conjunction);
        }

        return disjuncts is null ? GrainIndexQueryPlan.Empty : new GrainIndexQueryPlan(disjuncts.ToArray());
    }

    private static List<List<QueryAtom>> ToDisjunctiveNormalForm(Expression expression, bool negated)
    {
        expression = Unwrap(expression);

        switch (expression)
        {
            case UnaryExpression unary when unary.NodeType == ExpressionType.Not:
                return ToDisjunctiveNormalForm(unary.Operand, !negated);

            case BinaryExpression binary when binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse:
            {
                // De Morgan: negating an And yields an Or of the negated operands
                // and vice versa, so one flag drives both the combinator and the
                // polarity handed down to each side.
                bool conjunction = (binary.NodeType == ExpressionType.AndAlso) != negated;
                var left = ToDisjunctiveNormalForm(binary.Left, negated);
                var right = ToDisjunctiveNormalForm(binary.Right, negated);
                return conjunction ? Distribute(left, right) : Concatenate(left, right);
            }

            default:
                return [[new QueryAtom(expression, negated)]];
        }
    }

    private static List<List<QueryAtom>> Distribute(List<List<QueryAtom>> left, List<List<QueryAtom>> right)
    {
        long product = (long)left.Count * right.Count;
        if (product > MaxConjunctions)
            throw TooComplex();

        var combined = new List<List<QueryAtom>>((int)product);
        for (var i = 0; i < left.Count; i++)
        {
            for (var j = 0; j < right.Count; j++)
            {
                var merged = new List<QueryAtom>(left[i].Count + right[j].Count);
                merged.AddRange(left[i]);
                merged.AddRange(right[j]);
                combined.Add(merged);
            }
        }

        return combined;
    }

    private static List<List<QueryAtom>> Concatenate(List<List<QueryAtom>> left, List<List<QueryAtom>> right)
    {
        if (left.Count + right.Count > MaxConjunctions)
            throw TooComplex();

        left.AddRange(right);
        return left;
    }

    private static GrainIndexConjunction? BuildConjunction<TState>(
        List<QueryAtom> atoms,
        ParameterExpression parameter,
        string indexName,
        GrainIndexQueryProperty[] properties,
        IReadOnlyList<string> propertyNames)
    {
        // One accumulator slot per projected property. An index projects a
        // handful of properties, so a flat array indexed by declaration ordinal
        // beats any dictionary here.
        var ranges = new GrainIndexKeyRange[properties.Length][];
        var residuals = new LatticePredicateNode?[properties.Length];
        var pointLookups = new bool[properties.Length];
        var touched = 0;

        for (var i = 0; i < atoms.Count; i++)
        {
            var analysis = Analyse<TState>(atoms[i], parameter, indexName, properties, propertyNames);

            if (analysis.Property is null)
            {
                // A parameter-free atom folds away: false kills the whole
                // conjunction, true contributes nothing.
                if (!analysis.ConstantValue)
                    return null;

                continue;
            }

            int ordinal = analysis.Property.Ordinal;
            if (ranges[ordinal] is null)
            {
                touched++;
                ranges[ordinal] = analysis.Ranges;
            }
            else
            {
                ranges[ordinal] = GrainIndexRangeSet.Intersect(ranges[ordinal]!, analysis.Ranges);
            }

            if (ranges[ordinal]!.Length == 0)
                return null;

            residuals[ordinal] = Combine(residuals[ordinal], analysis.Residual);
            pointLookups[ordinal] |= analysis.PointLookup;
        }

        if (touched == 0)
        {
            // Nothing narrowed anything: every grain contributes exactly one
            // entry per property, so scanning the first property's whole range
            // enumerates the indexed grains once each.
            return new GrainIndexConjunction(
                [new GrainIndexScanClause(properties[0], properties[0].FullRange, null, 3)]);
        }

        var clauses = new GrainIndexScanClause[touched];
        var next = 0;
        for (var ordinal = 0; ordinal < properties.Length; ordinal++)
        {
            var clauseRanges = ranges[ordinal];
            if (clauseRanges is null)
                continue;

            clauses[next++] = new GrainIndexScanClause(
                properties[ordinal],
                clauseRanges,
                residuals[ordinal],
                Selectivity(properties[ordinal], clauseRanges, pointLookups[ordinal]));
        }

        SortBySelectivity(clauses);
        return new GrainIndexConjunction(clauses);
    }

    private static QueryAtomPlan Analyse<TState>(
        in QueryAtom atom,
        ParameterExpression parameter,
        string indexName,
        GrainIndexQueryProperty[] properties,
        IReadOnlyList<string> propertyNames)
    {
        if (atom.Expression.Type != typeof(bool))
            throw Unsupported($"expression '{atom.Expression}' in boolean position");

        // Translating through the core lowers the atom and validates it against
        // the one predicate dialect Lattice supports, so an unsupported construct
        // fails here with the core's own message.
        var body = atom.Negated ? Expression.Not(atom.Expression) : atom.Expression;
        var node = LatticePredicateTranslator.Translate(
            Expression.Lambda<Func<TState, bool>>(body, parameter));

        var paths = new List<string>(1);
        CollectMemberPaths(node, paths);

        if (paths.Count == 0)
            return QueryAtomPlan.Constant(EvaluateBoolean(atom));

        if (paths.Count > 1)
        {
            throw Unsupported(
                $"a clause over more than one projected property ({string.Join(" and ", paths)}). "
                + "An index entry carries exactly one property, so a clause spanning two of them "
                + "matches no entry. Combine the properties with '&&' at the top level instead, "
                + "which the planner routes as one scan per property.");
        }

        string path = paths[0];
        if (path.IndexOf('.') >= 0)
        {
            throw Unsupported(
                $"nested member access '{path}'. An index projects top-level state properties, so "
                + "a predicate must compare one of those directly.");
        }

        var property = Find(properties, path)
            ?? throw new GrainIndexPropertyNotIndexedException(indexName, path, propertyNames);

        var (ranges, exact, pointLookup) = Route(atom.Expression, parameter, property);

        if (atom.Negated)
        {
            // Negating a relational comparison is not the same as flipping its
            // operator once nulls or NaN are in play (a null operand makes both
            // 'x < c' and 'x >= c' false), so negation is taken as the complement
            // of an exact range set and falls back to the whole property range
            // otherwise.
            ranges = exact
                ? GrainIndexRangeSet.Complement(ranges, property.RangeStartInclusive, property.RangeEndExclusive)
                : property.FullRange;
            pointLookup = false;
        }

        if (exact)
            return new QueryAtomPlan(property, ranges, null, pointLookup);

        if (property.IsTemporal)
        {
            throw Unsupported(
                $"clause '{atom.Expression}' over date/time property '{property.Name}'. A date is "
                + "stored in the entry payload in round-trip form but captured from a lambda "
                + "through ToString(), so the two never compare equal and the clause can only be "
                + "served from the key range. Use a direct comparison (==, !=, <, <=, >, >=) "
                + "against a date constant.");
        }

        return new QueryAtomPlan(property, ranges, node, pointLookup);
    }

    private static (GrainIndexKeyRange[] Ranges, bool Exact, bool PointLookup) Route(
        Expression expression,
        ParameterExpression parameter,
        GrainIndexQueryProperty property)
    {
        switch (Unwrap(expression))
        {
            case BinaryExpression binary when TryMapComparison(binary.NodeType, out var op):
            {
                bool leftReferences = ReferencesParameter(binary.Left, parameter);
                bool rightReferences = ReferencesParameter(binary.Right, parameter);
                if (leftReferences && rightReferences)
                {
                    throw Unsupported(
                        $"comparison '{binary}' between two state members. An index entry stores a "
                        + "projected value against a constant bound, so one side must be a "
                        + "constant.");
                }

                var constantSide = leftReferences ? binary.Right : binary.Left;
                if (!leftReferences)
                {
                    op = Mirror(op);
                }

                object? constant = EvaluateConstant(constantSide);
                bool derived = GrainIndexRangeBuilder.TryBuild(property, op, constant, out var ranges, out bool exact);
                return derived
                    ? (ranges, exact, op == LatticeComparisonOperator.Equal)
                    : (property.FullRange, false, false);
            }

            case MethodCallExpression call
                when call.Method.DeclaringType == typeof(string)
                    && call.Object is not null
                    && call.Arguments.Count >= 1
                    && ReferencesParameter(call.Object, parameter)
                    && !ReferencesParameter(call.Arguments[0], parameter):
            {
                object? argument = EvaluateConstant(call.Arguments[0]);

                // A prefix match narrows to a contiguous range, and an equality
                // to a single slot. Both keep the predicate: the range prunes,
                // the server-side evaluator remains the authority on the dialect.
                if (string.Equals(call.Method.Name, nameof(string.StartsWith), StringComparison.Ordinal)
                    && argument is string prefix
                    && GrainIndexRangeBuilder.TryBuildPrefix(property, prefix, out var prefixRanges))
                {
                    return (prefixRanges, false, false);
                }

                if (string.Equals(call.Method.Name, nameof(string.Equals), StringComparison.Ordinal)
                    && GrainIndexRangeBuilder.TryBuild(
                        property,
                        LatticeComparisonOperator.Equal,
                        argument,
                        out var equalRanges,
                        out _))
                {
                    return (equalRanges, false, true);
                }

                return (property.FullRange, false, false);
            }

            case MemberExpression member when member.Type == typeof(bool):
            {
                // A bare boolean member in predicate position is an equality
                // against true.
                bool derived = GrainIndexRangeBuilder.TryBuild(
                    property,
                    LatticeComparisonOperator.Equal,
                    true,
                    out var ranges,
                    out bool exact);
                return derived ? (ranges, exact, true) : (property.FullRange, false, false);
            }

            default:
                return (property.FullRange, false, false);
        }
    }

    private static int Selectivity(GrainIndexQueryProperty property, GrainIndexKeyRange[] ranges, bool pointLookup)
    {
        if (pointLookup)
            return 0;

        bool startNarrowed = !string.Equals(
            ranges[0].StartInclusive,
            property.RangeStartInclusive,
            StringComparison.Ordinal);
        bool endNarrowed = !string.Equals(
            ranges[^1].EndExclusive,
            property.RangeEndExclusive,
            StringComparison.Ordinal);

        if (startNarrowed && endNarrowed)
            return 1;

        return startNarrowed || endNarrowed ? 2 : 3;
    }

    private static void SortBySelectivity(GrainIndexScanClause[] clauses)
    {
        // Insertion sort: a conjunction spans a handful of properties at most,
        // and this keeps the order deterministic (ties break on declaration
        // ordinal, which the array is already in).
        for (var i = 1; i < clauses.Length; i++)
        {
            var current = clauses[i];
            int j = i - 1;
            while (j >= 0 && clauses[j].Selectivity > current.Selectivity)
            {
                clauses[j + 1] = clauses[j];
                j--;
            }

            clauses[j + 1] = current;
        }
    }

    private static GrainIndexQueryProperty? Find(GrainIndexQueryProperty[] properties, string name)
    {
        for (var i = 0; i < properties.Length; i++)
        {
            if (string.Equals(properties[i].Name, name, StringComparison.Ordinal))
                return properties[i];
        }

        // The server-side evaluator resolves a member name case-insensitively,
        // so the planner accepts the same spellings it would.
        for (var i = 0; i < properties.Length; i++)
        {
            if (string.Equals(properties[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return properties[i];
        }

        return null;
    }

    private static void CollectMemberPaths(in LatticePredicateNode node, List<string> paths)
    {
        if (node.Kind == LatticePredicateNodeKind.Member)
        {
            string path = node.MemberPath ?? string.Empty;
            for (var i = 0; i < paths.Count; i++)
            {
                if (string.Equals(paths[i], path, StringComparison.OrdinalIgnoreCase))
                    return;
            }

            paths.Add(path);
            return;
        }

        var children = node.Children;
        if (children is null)
            return;

        for (var i = 0; i < children.Length; i++)
        {
            CollectMemberPaths(children[i], paths);
        }
    }

    private static LatticePredicateNode? Combine(LatticePredicateNode? left, LatticePredicateNode? right)
    {
        if (left is null)
            return right;

        return right is null
            ? left
            : LatticePredicateNode.Bool(LatticeBooleanOperator.And, left.Value, right.Value);
    }

    private static bool EvaluateBoolean(in QueryAtom atom)
    {
        var value = (bool)Expression.Lambda(atom.Expression).Compile().DynamicInvoke()!;
        return atom.Negated ? !value : value;
    }

    private static object? EvaluateConstant(Expression expression)
    {
        expression = Unwrap(expression);
        return expression is ConstantExpression constant
            ? constant.Value
            : Expression.Lambda(expression).Compile().DynamicInvoke();
    }

    private static bool TryMapComparison(ExpressionType nodeType, out LatticeComparisonOperator op)
    {
        switch (nodeType)
        {
            case ExpressionType.Equal: op = LatticeComparisonOperator.Equal; return true;
            case ExpressionType.NotEqual: op = LatticeComparisonOperator.NotEqual; return true;
            case ExpressionType.LessThan: op = LatticeComparisonOperator.LessThan; return true;
            case ExpressionType.LessThanOrEqual: op = LatticeComparisonOperator.LessThanOrEqual; return true;
            case ExpressionType.GreaterThan: op = LatticeComparisonOperator.GreaterThan; return true;
            case ExpressionType.GreaterThanOrEqual: op = LatticeComparisonOperator.GreaterThanOrEqual; return true;
            default: op = LatticeComparisonOperator.Equal; return false;
        }
    }

    private static LatticeComparisonOperator Mirror(LatticeComparisonOperator op) => op switch
    {
        LatticeComparisonOperator.LessThan => LatticeComparisonOperator.GreaterThan,
        LatticeComparisonOperator.LessThanOrEqual => LatticeComparisonOperator.GreaterThanOrEqual,
        LatticeComparisonOperator.GreaterThan => LatticeComparisonOperator.LessThan,
        LatticeComparisonOperator.GreaterThanOrEqual => LatticeComparisonOperator.LessThanOrEqual,
        _ => op,
    };

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static bool ReferencesParameter(Expression expression, ParameterExpression parameter) =>
        ParameterFinder.Contains(expression, parameter);

    private static NotSupportedException Unsupported(string what) =>
        new($"Unsupported grain-index query construct: {what}");

    private static NotSupportedException TooComplex() =>
        new($"Unsupported grain-index query construct: the predicate expands to more than "
            + $"{MaxConjunctions} disjunctions once '&&' is distributed over '||'. Split it into "
            + "separate queries, or restructure it so the top level is a simple union.");

    private readonly record struct QueryAtom(Expression Expression, bool Negated);

    private readonly record struct QueryAtomPlan(
        GrainIndexQueryProperty? Property,
        GrainIndexKeyRange[] Ranges,
        LatticePredicateNode? Residual,
        bool PointLookup)
    {
        internal bool ConstantValue { get; private init; }

        internal static QueryAtomPlan Constant(bool value) =>
            new(null, GrainIndexRangeSet.Empty, null, false) { ConstantValue = value };
    }

    private sealed class ParameterFinder : ExpressionVisitor
    {
        private readonly ParameterExpression _target;
        private bool _found;

        private ParameterFinder(ParameterExpression target) => _target = target;

        internal static bool Contains(Expression expression, ParameterExpression target)
        {
            var finder = new ParameterFinder(target);
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
}
