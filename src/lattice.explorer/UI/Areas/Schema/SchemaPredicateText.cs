using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A predicate written as a compact expression, such as
/// <c>total &gt;= 0 and (status == "open" or status == "shipped")</c>, for a
/// rule the builder shows but cannot edit as a card. Text only, never markup.
/// </summary>
internal static class SchemaPredicateText
{
    /// <summary>The deepest nesting written out; anything deeper is elided.</summary>
    public const int MaximumDepth = 32;

    /// <summary>Writes <paramref name="node"/> as an expression.</summary>
    /// <param name="node">The predicate.</param>
    /// <returns>The expression.</returns>
    public static string Expression(LatticePredicateNode node)
    {
        var builder = new StringBuilder();
        Write(builder, node, 0, topLevel: true);
        return builder.ToString();
    }

    private static void Write(StringBuilder builder, LatticePredicateNode node, int depth, bool topLevel = false)
    {
        if (depth > MaximumDepth)
        {
            builder.Append("...");
            return;
        }

        var children = node.Children ?? [];
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Member:
                builder.Append(string.IsNullOrEmpty(node.MemberPath) ? "?" : node.MemberPath);
                break;

            case LatticePredicateNodeKind.Self:
                builder.Append("it");
                break;

            case LatticePredicateNodeKind.Constant:
                builder.Append(Constant(node.Constant));
                break;

            case LatticePredicateNodeKind.Compare when children.Length == 2:
                Write(builder, children[0], depth + 1);
                builder.Append(' ').Append(Operator(node.ComparisonOperator)).Append(' ');
                Write(builder, children[1], depth + 1);
                break;

            case LatticePredicateNodeKind.StringMethod when children.Length == 2:
                Write(builder, children[0], depth + 1);
                builder.Append(node.StringMethod switch
                {
                    LatticeStringMethod.StartsWith => " starts with ",
                    LatticeStringMethod.EndsWith => " ends with ",
                    LatticeStringMethod.Contains => " contains ",
                    _ => " equals ",
                });
                Write(builder, children[1], depth + 1);
                break;

            case LatticePredicateNodeKind.Boolean when node.BooleanOperator == LatticeBooleanOperator.Not && children.Length == 1:
                builder.Append("not (");
                Write(builder, children[0], depth + 1, topLevel: true);
                builder.Append(')');
                break;

            case LatticePredicateNodeKind.Boolean when children.Length > 0:
                if (!topLevel)
                {
                    builder.Append('(');
                }

                var joiner = node.BooleanOperator == LatticeBooleanOperator.Or ? " or " : " and ";
                for (var index = 0; index < children.Length; index++)
                {
                    if (index > 0)
                    {
                        builder.Append(joiner);
                    }

                    Write(builder, children[index], depth + 1);
                }

                if (!topLevel)
                {
                    builder.Append(')');
                }

                break;

            case LatticePredicateNodeKind.TypeOf:
                builder.Append(Subject(node.MemberPath)).Append(" is ").Append(node.ValueKind switch
                {
                    LatticeValueKind.Present => "present",
                    LatticeValueKind.Null => "null",
                    LatticeValueKind.Boolean => "true or false",
                    LatticeValueKind.Number => "a number",
                    LatticeValueKind.Integer => "a whole number",
                    LatticeValueKind.String => "text",
                    LatticeValueKind.Object => "an object",
                    LatticeValueKind.Array => "a list",
                    _ => "of an unknown kind",
                });
                break;

            case LatticePredicateNodeKind.Length:
                builder.Append("length of ").Append(Subject(node.MemberPath));
                break;

            case LatticePredicateNodeKind.Every when children.Length == 1:
                builder.Append("every item of ").Append(Subject(node.MemberPath)).Append(": (");
                Write(builder, children[0], depth + 1, topLevel: true);
                builder.Append(')');
                break;

            default:
                builder.Append("(unreadable)");
                break;
        }
    }

    private static string Subject(string? path) => string.IsNullOrEmpty(path) ? "it" : path;

    private static string Operator(LatticeComparisonOperator op) => op switch
    {
        LatticeComparisonOperator.Equal => "==",
        LatticeComparisonOperator.NotEqual => "!=",
        LatticeComparisonOperator.LessThan => "<",
        LatticeComparisonOperator.LessThanOrEqual => "<=",
        LatticeComparisonOperator.GreaterThan => ">",
        LatticeComparisonOperator.GreaterThanOrEqual => ">=",
        _ => "?",
    };

    private static string Constant(LatticeConstant constant) => constant.Kind switch
    {
        LatticeConstantKind.Null => "null",
        LatticeConstantKind.Boolean => constant.BooleanValue ? "true" : "false",
        LatticeConstantKind.Int64 => constant.Int64Value.ToString(CultureInfo.InvariantCulture),
        LatticeConstantKind.Double => constant.DoubleValue.ToString("R", CultureInfo.InvariantCulture),
        LatticeConstantKind.String => Quote(constant.StringValue ?? string.Empty),
        _ => "?",
    };

    /// <summary>
    /// A text constant in quotes, with every character that would change how it reads
    /// escaped: the quote and the backslash itself, so an escape is never ambiguous, and
    /// control and line-separator characters, so the expression stays on one line.
    /// </summary>
    private static string Quote(string text)
    {
        var builder = new StringBuilder(text.Length + 2).Append('"');
        foreach (var ch in text)
        {
            switch (ch)
            {
                case '"':
                    builder.Append("\\\"");
                    break;
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                default:
                    if (char.IsControl(ch) || ch is '\u2028' or '\u2029')
                    {
                        builder.Append("\\u").Append(((int)ch).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        builder.Append(ch);
                    }

                    break;
            }
        }

        return builder.Append('"').ToString();
    }
}
