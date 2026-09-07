using System.Linq.Expressions;
using System.Reflection;

namespace Orleans.Lattice;

/// <summary>
/// Reads the value behind a parameter-free expression - the constant side of a
/// predicate comparison - without invoking the expression compiler where the
/// shape allows it.
/// <para>
/// The shape that matters is <c>state =&gt; state.Age &gt;= minimum</c> over a
/// bound captured from the surrounding method, which is what real predicates
/// are made of. The C# compiler hoists such a local onto a display class, so
/// the expression tree carries it as a <b>member read over a constant</b>, not
/// as a <see cref="ConstantExpression"/>. The naive path for that was to build
/// a lambda around it, run the whole expression compiler to emit IL, and
/// reflect-invoke the resulting delegate once - orders of magnitude more
/// expensive than the field read it stands for, and paid once per comparison of
/// every predicate, because neither the core translator nor the grain-index
/// planner caches its output.
/// </para>
/// <para>
/// A constant, or a field or property chain rooted in one, is therefore read
/// directly by reflection. Every other shape - a method call, an arithmetic
/// expression, a <c>new</c> - still falls back to compiling, so no predicate
/// loses the ability to name a bound this reader cannot fold.
/// </para>
/// </summary>
internal static class ExpressionConstantReader
{
    /// <summary>
    /// Reads <paramref name="expression"/>, folding it directly when its shape
    /// allows and compiling it otherwise.
    /// </summary>
    internal static object? Read(Expression expression) =>
        TryRead(expression, out object? value)
            ? value
            : Expression.Lambda(expression).Compile().DynamicInvoke();

    /// <summary>
    /// Attempts to read <paramref name="expression"/> without invoking the
    /// expression compiler. Reports <c>false</c> for every shape outside the
    /// closed set of a constant and the field or property chains rooted in one,
    /// leaving the caller to compile.
    /// </summary>
    internal static bool TryRead(Expression expression, out object? value)
    {
        switch (Unwrap(expression))
        {
            case ConstantExpression constant:
                value = constant.Value;
                return true;

            case MemberExpression member:
            {
                object? instance = null;
                if (member.Expression is not null && !TryRead(member.Expression, out instance))
                    break;

                // A static member has no instance. An instance member read off a
                // null target would throw, where the compiled delegate throws the
                // same NullReferenceException wrapped by the invoke, so that case
                // is left to the fallback rather than reproduced here.
                switch (member.Member)
                {
                    case FieldInfo field when field.IsStatic || instance is not null:
                        value = field.GetValue(instance);
                        return true;

                    case PropertyInfo property
                        when property.GetIndexParameters().Length == 0
                            && property.GetGetMethod(nonPublic: true) is { } getter
                            && (getter.IsStatic || instance is not null):
                        value = property.GetValue(instance);
                        return true;
                }

                break;
            }
        }

        value = null;
        return false;
    }

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            expression = unary.Operand;

        return expression;
    }
}
