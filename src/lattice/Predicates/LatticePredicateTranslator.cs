using System.Linq.Expressions;
using System.Reflection;

namespace Orleans.Lattice;

/// <summary>
/// Lowers a client-side <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c> into the
/// allowlisted, serializable <see cref="LatticePredicateNode"/> IR for
/// server-side evaluation. Constructs outside the allowlist throw
/// <see cref="NotSupportedException"/> <b>at translation time on the client</b>,
/// naming the offending construct - the server never sees an unsupported node.
/// <para>
/// Allowlist: parameter member access (property path resolved by name),
/// constants (including captured locals), the comparison operators
/// <c>== != &lt; &lt;= &gt; &gt;=</c>, the boolean operators <c>&amp;&amp; || !</c>,
/// and the string methods <c>StartsWith</c>, <c>EndsWith</c>, <c>Contains</c>,
/// and <c>Equals</c>.
/// </para>
/// </summary>
public static class LatticePredicateTranslator
{
    /// <summary>
    /// Translates <paramref name="predicate"/> into the predicate IR.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">The expression contains a construct outside the allowlist.</exception>
    public static LatticePredicateNode Translate<T>(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        var parameter = predicate.Parameters[0];
        return TranslateBoolean(predicate.Body, parameter);
    }

    private static LatticePredicateNode TranslateBoolean(Expression expression, ParameterExpression parameter)
    {
        expression = Unwrap(expression);

        switch (expression)
        {
            case BinaryExpression binary:
                switch (binary.NodeType)
                {
                    case ExpressionType.AndAlso:
                        return LatticePredicateNode.Bool(
                            LatticeBooleanOperator.And,
                            TranslateBoolean(binary.Left, parameter),
                            TranslateBoolean(binary.Right, parameter));
                    case ExpressionType.OrElse:
                        return LatticePredicateNode.Bool(
                            LatticeBooleanOperator.Or,
                            TranslateBoolean(binary.Left, parameter),
                            TranslateBoolean(binary.Right, parameter));
                    case ExpressionType.Equal:
                    case ExpressionType.NotEqual:
                    case ExpressionType.LessThan:
                    case ExpressionType.LessThanOrEqual:
                    case ExpressionType.GreaterThan:
                    case ExpressionType.GreaterThanOrEqual:
                        return TranslateComparison(binary, parameter);
                    default:
                        throw Unsupported($"binary operator '{binary.NodeType}'");
                }

            case UnaryExpression unary when unary.NodeType == ExpressionType.Not:
                return LatticePredicateNode.Bool(
                    LatticeBooleanOperator.Not,
                    TranslateBoolean(unary.Operand, parameter));

            case MethodCallExpression call when call.Type == typeof(bool):
                return TranslateStringMethod(call, parameter);

            // A bare boolean member or constant used directly as the predicate
            // body (e.g. u => u.IsActive). The evaluator resolves a Member /
            // Constant node to its truthiness in boolean position.
            case MemberExpression member when member.Type == typeof(bool) && ReferencesParameter(member, parameter):
                return TranslateMemberAccess(member, parameter);

            case ConstantExpression { Value: bool } constant:
                return LatticePredicateNode.Const(LatticeConstant.Bool((bool)constant.Value!));

            default:
                throw Unsupported($"expression '{expression}' in boolean position");
        }
    }

    private static LatticePredicateNode TranslateComparison(BinaryExpression binary, ParameterExpression parameter)
    {
        var op = binary.NodeType switch
        {
            ExpressionType.Equal => LatticeComparisonOperator.Equal,
            ExpressionType.NotEqual => LatticeComparisonOperator.NotEqual,
            ExpressionType.LessThan => LatticeComparisonOperator.LessThan,
            ExpressionType.LessThanOrEqual => LatticeComparisonOperator.LessThanOrEqual,
            ExpressionType.GreaterThan => LatticeComparisonOperator.GreaterThan,
            ExpressionType.GreaterThanOrEqual => LatticeComparisonOperator.GreaterThanOrEqual,
            _ => throw Unsupported($"comparison operator '{binary.NodeType}'"),
        };

        var left = TranslateOperand(binary.Left, parameter);
        var right = TranslateOperand(binary.Right, parameter);
        return LatticePredicateNode.Compare(op, left, right);
    }

    private static LatticePredicateNode TranslateStringMethod(MethodCallExpression call, ParameterExpression parameter)
    {
        if (call.Method.DeclaringType != typeof(string))
            throw Unsupported($"method '{call.Method.DeclaringType?.Name}.{call.Method.Name}'");

        var method = call.Method.Name switch
        {
            nameof(string.StartsWith) => LatticeStringMethod.StartsWith,
            nameof(string.EndsWith) => LatticeStringMethod.EndsWith,
            nameof(string.Contains) => LatticeStringMethod.Contains,
            nameof(string.Equals) => LatticeStringMethod.Equals,
            _ => throw Unsupported($"string method '{call.Method.Name}'"),
        };

        Expression targetExpression;
        Expression argumentExpression;
        if (call.Object is not null)
        {
            // Instance form: target.Method(argument). A second argument is a
            // StringComparison / ignoreCase / CultureInfo modifier the
            // evaluator cannot honour - it compares strings ordinally - so
            // accepting the overload would silently push down a match with
            // different semantics than the compiled lambda (e.g.
            // Equals(x, StringComparison.OrdinalIgnoreCase) would run as an
            // ordinal, case-sensitive comparison). Reject it instead.
            targetExpression = call.Object;
            if (call.Arguments.Count < 1)
                throw Unsupported($"string method '{call.Method.Name}' with no argument");
            if (call.Arguments.Count > 1)
                throw Unsupported($"string method '{call.Method.Name}' with a comparison or culture argument (only the ordinal single-argument overload is supported)");
            argumentExpression = call.Arguments[0];
        }
        else
        {
            // Static form: string.Equals(a, b). A third argument is a
            // StringComparison the ordinal evaluator likewise cannot honour.
            if (call.Arguments.Count < 2)
                throw Unsupported($"static string method '{call.Method.Name}'");
            if (call.Arguments.Count > 2)
                throw Unsupported($"static string method '{call.Method.Name}' with a comparison argument (only the ordinal two-argument overload is supported)");
            targetExpression = call.Arguments[0];
            argumentExpression = call.Arguments[1];
        }

        var target = TranslateOperand(targetExpression, parameter);
        var argument = TranslateOperand(argumentExpression, parameter);
        return LatticePredicateNode.StringCall(method, target, argument);
    }

    private static LatticePredicateNode TranslateOperand(Expression expression, ParameterExpression parameter)
    {
        expression = Unwrap(expression);

        if (ReferencesParameter(expression, parameter))
        {
            if (expression is MemberExpression member)
                return TranslateMemberAccess(member, parameter);
            throw Unsupported($"expression '{expression}' (only parameter member access is supported on the value side)");
        }

        // No reference to the parameter: a constant or captured local. Evaluate
        // it once at translation time and capture the literal.
        return LatticePredicateNode.Const(CaptureConstant(expression));
    }

    private static LatticePredicateNode TranslateMemberAccess(MemberExpression member, ParameterExpression parameter)
    {
        var segments = new List<string>();
        Expression? current = member;
        while (current is MemberExpression m)
        {
            if (m.Member is not PropertyInfo and not FieldInfo)
                throw Unsupported($"member '{m.Member.Name}'");
            segments.Add(m.Member.Name);
            current = m.Expression;
        }

        if (current != parameter)
            throw Unsupported($"member access '{member}' (must be rooted at the lambda parameter)");

        segments.Reverse();
        return LatticePredicateNode.Member(string.Join('.', segments));
    }

    private static LatticeConstant CaptureConstant(Expression expression)
    {
        object? value;
        if (expression is ConstantExpression constant)
            value = constant.Value;
        else
            value = Expression.Lambda(expression).Compile().DynamicInvoke();

        return value switch
        {
            null => LatticeConstant.Null(),
            bool b => LatticeConstant.Bool(b),
            string s => LatticeConstant.Text(s),
            char c => LatticeConstant.Text(c.ToString()),
            sbyte or byte or short or ushort or int or uint or long => LatticeConstant.Integer(Convert.ToInt64(value)),
            ulong ul => LatticeConstant.Integer(unchecked((long)ul)),
            float or double => LatticeConstant.Real(Convert.ToDouble(value)),
            decimal d => LatticeConstant.Real((double)d),
            Enum e => LatticeConstant.Integer(Convert.ToInt64(e)),
            _ => LatticeConstant.Text(value.ToString() ?? string.Empty),
        };
    }

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            expression = unary.Operand;
        return expression;
    }

    private static bool ReferencesParameter(Expression expression, ParameterExpression parameter) =>
        ParameterFinder.Contains(expression, parameter);

    private static NotSupportedException Unsupported(string what) =>
        new($"Unsupported predicate construct: {what}. Server-side predicate push-down supports parameter member access, constants, the comparison operators == != < <= > >=, the boolean operators && || !, and the string methods StartsWith/EndsWith/Contains/Equals.");

    private sealed class ParameterFinder : ExpressionVisitor
    {
        private readonly ParameterExpression _target;
        private bool _found;

        private ParameterFinder(ParameterExpression target) => _target = target;

        public static bool Contains(Expression expression, ParameterExpression target)
        {
            var finder = new ParameterFinder(target);
            finder.Visit(expression);
            return finder._found;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _target)
                _found = true;
            return base.VisitParameter(node);
        }
    }
}
