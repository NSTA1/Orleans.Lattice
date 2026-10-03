using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace Orleans.Lattice;

/// <summary>
/// Structural queries over LINQ expression trees shared by the expression
/// translators (server-side predicate push-down, client-side value-transform
/// lowering, and the grain-index query planner).
/// </summary>
internal static class ExpressionTreeShapes
{
    /// <summary>
    /// Strips any chain of <see cref="ExpressionType.Convert"/> /
    /// <see cref="ExpressionType.ConvertChecked"/> nodes the compiler inserts
    /// around an operand (boxing, nullable lifting, enum-to-underlying), returning
    /// the innermost operand.
    /// </summary>
    /// <param name="expression">The expression to unwrap.</param>
    internal static Expression StripConversions(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    /// <summary>
    /// Allocation-free, early-exit answer to "does this sub-expression mention
    /// the lambda parameter?". The translators ask this repeatedly while routing a
    /// predicate, and an <see cref="ExpressionVisitor"/> allocates a fresh visitor
    /// per call and always walks the whole sub-tree, even once the answer is known.
    /// <para>
    /// The switch enumerates the node shapes a translatable predicate is built
    /// from. Anything else - a member/list initialiser, a block, a switch - falls
    /// back to a visitor walk, so an unrecognised node kind degrades to the
    /// exhaustive behaviour rather than silently answering <c>false</c>.
    /// </para>
    /// </summary>
    /// <param name="expression">The sub-expression to search; may be <see langword="null"/>.</param>
    /// <param name="parameter">The lambda parameter to look for.</param>
    internal static bool ReferencesParameter(Expression? expression, ParameterExpression parameter)
    {
        switch (expression)
        {
            case null:
                return false;
            case ParameterExpression parameterExpression:
                return ReferenceEquals(parameterExpression, parameter);
            case ConstantExpression:
                return false;
            case UnaryExpression unary:
                return ReferencesParameter(unary.Operand, parameter);
            case MemberExpression member:
                return ReferencesParameter(member.Expression, parameter);
            case TypeBinaryExpression typeBinary:
                return ReferencesParameter(typeBinary.Expression, parameter);
            case BinaryExpression binary:
                return ReferencesParameter(binary.Left, parameter)
                    || ReferencesParameter(binary.Right, parameter)
                    || ReferencesParameter(binary.Conversion, parameter);
            case ConditionalExpression conditional:
                return ReferencesParameter(conditional.Test, parameter)
                    || ReferencesParameter(conditional.IfTrue, parameter)
                    || ReferencesParameter(conditional.IfFalse, parameter);
            case MethodCallExpression call:
                return ReferencesParameter(call.Object, parameter)
                    || ReferencesAnyParameter(call.Arguments, parameter);
            case InvocationExpression invocation:
                return ReferencesParameter(invocation.Expression, parameter)
                    || ReferencesAnyParameter(invocation.Arguments, parameter);
            case NewExpression construction:
                return ReferencesAnyParameter(construction.Arguments, parameter);
            case NewArrayExpression newArray:
                return ReferencesAnyParameter(newArray.Expressions, parameter);
            case IndexExpression index:
                return ReferencesParameter(index.Object, parameter)
                    || ReferencesAnyParameter(index.Arguments, parameter);
            case LambdaExpression lambda:
                return ReferencesAnyParameter(lambda.Parameters, parameter)
                    || ReferencesParameter(lambda.Body, parameter);
            default:
                return ParameterFinder.Contains(expression, parameter);
        }
    }

    private static bool ReferencesAnyParameter<TExpression>(
        ReadOnlyCollection<TExpression> expressions,
        ParameterExpression parameter)
        where TExpression : Expression
    {
        for (var i = 0; i < expressions.Count; i++)
        {
            if (ReferencesParameter(expressions[i], parameter))
                return true;
        }

        return false;
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
