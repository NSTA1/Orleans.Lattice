using System.Linq.Expressions;
using System.Reflection;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Lowers a client-side <c>Expression&lt;Func&lt;TOld, TNew&gt;&gt;</c> (or
/// <c>Expression&lt;Func&lt;T, T&gt;&gt;</c>) into the allowlisted, serializable
/// <see cref="LatticeValueTransform"/> IR for server-side evaluation. Constructs
/// outside the allowlist throw <see cref="NotSupportedException"/> <b>at
/// translation time on the client</b>, naming the offending construct - the
/// server never sees an unsupported node. This is the transform-side sibling of
/// <see cref="LatticePredicateTranslator"/>.
/// <para>
/// Allowlist: the projection body must be a member-initialization
/// (<c>new TNew { A = ..., B = ... }</c>, optionally with record/anonymous
/// constructor members). Each assigned member becomes a
/// <see cref="LatticeValueTransformKind.SetMember"/> over a copy of the input
/// document, so unassigned members survive (additive evolution). A member value
/// may be a parameter member access (a top-level property), a constant
/// (including a captured local), a null-coalescing expression (<c>a ?? b</c>),
/// a string concatenation (<c>a + b</c> or <c>string.Concat(...)</c>), or a
/// ternary conditional (<c>test ? a : b</c>) whose test lowers through
/// <see cref="LatticePredicateTranslator"/>.
/// </para>
/// </summary>
public static class LatticeValueTransformTranslator
{
    /// <summary>
    /// Translates a same-type <paramref name="transform"/> into the transform IR.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="transform"/> is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">The expression contains a construct outside the allowlist.</exception>
    public static LatticeValueTransform Translate<T>(Expression<Func<T, T>> transform) =>
        Translate<T, T>(transform);

    /// <summary>
    /// Translates <paramref name="transform"/> into the transform IR.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="transform"/> is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">The expression contains a construct outside the allowlist.</exception>
    public static LatticeValueTransform Translate<TOld, TNew>(Expression<Func<TOld, TNew>> transform)
    {
        ArgumentNullException.ThrowIfNull(transform);
        var parameter = transform.Parameters[0];
        var operations = new List<LatticeValueTransform>();
        TranslateProjection<TOld>(Unwrap(transform.Body), parameter, operations);
        return LatticeValueTransform.Passthrough(operations.ToArray());
    }

    private static void TranslateProjection<TOld>(
        Expression body,
        ParameterExpression parameter,
        List<LatticeValueTransform> operations)
    {
        switch (body)
        {
            case MemberInitExpression init:
                TranslateNew<TOld>(init.NewExpression, parameter, operations);
                foreach (var binding in init.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                        throw Unsupported($"member binding '{binding.BindingType}' on '{binding.Member.Name}'");
                    operations.Add(LatticeValueTransform.SetMember(
                        assignment.Member.Name,
                        TranslateValue<TOld>(assignment.Expression, parameter)));
                }

                break;

            case NewExpression newExpression:
                TranslateNew<TOld>(newExpression, parameter, operations);
                break;

            default:
                throw Unsupported(
                    $"projection body '{body}' (the body must be a member-initialization, e.g. new TNew {{ A = ..., B = ... }})");
        }
    }

    private static void TranslateNew<TOld>(
        NewExpression newExpression,
        ParameterExpression parameter,
        List<LatticeValueTransform> operations)
    {
        var members = newExpression.Members;
        if (members is null)
        {
            // A parameterless constructor carries no member mapping and simply
            // starts the copy - nothing to emit. A constructor with positional
            // arguments but no member metadata cannot be mapped to member names.
            if (newExpression.Arguments.Count > 0)
                throw Unsupported($"constructor '{newExpression.Constructor?.DeclaringType?.Name}(...)' without member mapping");
            return;
        }

        for (int i = 0; i < members.Count; i++)
        {
            operations.Add(LatticeValueTransform.SetMember(
                members[i].Name,
                TranslateValue<TOld>(newExpression.Arguments[i], parameter)));
        }
    }

    private static LatticeValueTransform TranslateValue<TOld>(Expression expression, ParameterExpression parameter)
    {
        expression = Unwrap(expression);

        switch (expression)
        {
            case ConditionalExpression conditional:
                return LatticeValueTransform.Conditional(
                    TranslateCondition<TOld>(conditional.Test, parameter),
                    TranslateValue<TOld>(conditional.IfTrue, parameter),
                    TranslateValue<TOld>(conditional.IfFalse, parameter));

            case BinaryExpression { NodeType: ExpressionType.Coalesce } coalesce:
                return LatticeValueTransform.Compute(
                    LatticeComputeOperator.Coalesce,
                    TranslateValue<TOld>(coalesce.Left, parameter),
                    TranslateValue<TOld>(coalesce.Right, parameter));

            case BinaryExpression { NodeType: ExpressionType.Add } add when add.Type == typeof(string):
                var operands = new List<LatticeValueTransform>();
                FlattenConcat<TOld>(add, parameter, operands);
                return LatticeValueTransform.Compute(LatticeComputeOperator.Concat, operands.ToArray());

            case MethodCallExpression call
                when call.Method.DeclaringType == typeof(string) && call.Method.Name == nameof(string.Concat):
                return LatticeValueTransform.Compute(
                    LatticeComputeOperator.Concat,
                    call.Arguments.Select(a => TranslateValue<TOld>(a, parameter)).ToArray());

            case MemberExpression member when ReferencesParameter(member, parameter):
                return LatticeValueTransform.Member(TranslateMemberAccess(member, parameter));

            default:
                if (ReferencesParameter(expression, parameter))
                    throw Unsupported($"value expression '{expression}' (only a top-level parameter member access is supported on the value side)");
                return LatticeValueTransform.Const(CaptureConstant(expression));
        }
    }

    private static void FlattenConcat<TOld>(
        Expression expression,
        ParameterExpression parameter,
        List<LatticeValueTransform> operands)
    {
        expression = Unwrap(expression);
        if (expression is BinaryExpression { NodeType: ExpressionType.Add } add && add.Type == typeof(string))
        {
            FlattenConcat<TOld>(add.Left, parameter, operands);
            FlattenConcat<TOld>(add.Right, parameter, operands);
            return;
        }

        operands.Add(TranslateValue<TOld>(expression, parameter));
    }

    private static LatticePredicateNode TranslateCondition<TOld>(Expression test, ParameterExpression parameter) =>
        LatticePredicateTranslator.Translate(Expression.Lambda<Func<TOld, bool>>(test, parameter));

    private static string TranslateMemberAccess(MemberExpression member, ParameterExpression parameter)
    {
        if (member.Member is not PropertyInfo and not FieldInfo)
            throw Unsupported($"member '{member.Member.Name}'");

        if (member.Expression != parameter)
            throw Unsupported($"member access '{member}' (v1 supports only a single top-level property rooted at the lambda parameter)");

        return member.Member.Name;
    }

    private static LatticeConstant CaptureConstant(Expression expression)
    {
        object? value = expression is ConstantExpression constant
            ? constant.Value
            : Expression.Lambda(expression).Compile().DynamicInvoke();

        return value switch
        {
            null => LatticeConstant.Null(),
            bool b => LatticeConstant.Bool(b),
            string s => LatticeConstant.Text(s),
            char c => LatticeConstant.Text(c.ToString()),
            sbyte or byte or short or ushort or int or uint or long => LatticeConstant.Integer(Convert.ToInt64(value)),
            ulong ul => ul <= long.MaxValue ? LatticeConstant.Integer((long)ul) : LatticeConstant.Real(ul),
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
        new($"Unsupported value-transform construct: {what}. Client-side value-transform lowering supports a member-initialization body whose members are parameter member access, constants, null-coalescing (??), string concatenation (+ / string.Concat), and ternary conditionals (?:).");

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
