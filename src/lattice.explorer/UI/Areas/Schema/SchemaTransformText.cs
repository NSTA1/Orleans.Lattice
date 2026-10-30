using System.Text;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A declarative migration transform written as plain steps, such as
/// <c>Renames qty to quantity</c>, so the difference one migration hop makes to a
/// value's shape can be read without the JSON it was registered as. Text only,
/// never markup.
/// </summary>
internal static class SchemaTransformText
{
    /// <summary>The deepest nesting written out; anything deeper is elided.</summary>
    public const int MaximumDepth = 16;

    /// <summary>The most steps listed for one transform; the rest are counted.</summary>
    public const int MaximumSteps = 32;

    /// <summary>The steps <paramref name="transform"/> takes, in the order it takes them.</summary>
    /// <param name="transform">The transform.</param>
    /// <returns>One sentence per step; one sentence when it copies the value unchanged.</returns>
    public static IReadOnlyList<string> Steps(LatticeValueTransform transform)
    {
        var operations = transform.Kind == LatticeValueTransformKind.Passthrough
            ? transform.Children ?? []
            : [transform];
        if (operations.Length == 0)
        {
            return ["Copies the value unchanged."];
        }

        var shown = Math.Min(operations.Length, MaximumSteps);
        var steps = new List<string>(shown + 1);
        for (var index = 0; index < shown; index++)
        {
            steps.Add(Step(operations[index]));
        }

        if (operations.Length > shown)
        {
            steps.Add($"And {SchemaFormat.Count(operations.Length - shown, "more step")}.");
        }

        return steps;
    }

    private static string Step(LatticeValueTransform operation)
    {
        var children = operation.Children ?? [];
        return operation.Kind switch
        {
            LatticeValueTransformKind.RenameMember => $"Renames {Path(operation.MemberPath)} to {Path(operation.ToPath)}.",
            LatticeValueTransformKind.DropMember => $"Removes {Path(operation.MemberPath)}.",
            LatticeValueTransformKind.SetMember when children.Length == 1 =>
                $"Sets {Path(operation.MemberPath)} to {Value(children[0])}.",
            LatticeValueTransformKind.Passthrough when children.Length == 0 => "Copies the value unchanged.",
            LatticeValueTransformKind.Passthrough => $"Applies {SchemaFormat.Count(children.Length, "nested step")}.",
            _ => "Takes a step this page cannot describe.",
        };
    }

    private static string Value(LatticeValueTransform expression)
    {
        var builder = new StringBuilder();
        Write(builder, expression, 0);
        return builder.ToString();
    }

    private static void Write(StringBuilder builder, LatticeValueTransform expression, int depth)
    {
        if (depth > MaximumDepth)
        {
            builder.Append("...");
            return;
        }

        var children = expression.Children ?? [];
        switch (expression.Kind)
        {
            case LatticeValueTransformKind.Member:
                builder.Append("the value of ").Append(Path(expression.MemberPath));
                break;

            case LatticeValueTransformKind.Constant:
                builder.Append(SchemaPredicateText.Constant(expression.Constant));
                break;

            case LatticeValueTransformKind.Conditional when children.Length == 2:
                Write(builder, children[0], depth + 1);
                builder.Append(" when ").Append(SchemaPredicateText.Expression(expression.Condition)).Append(", otherwise ");
                Write(builder, children[1], depth + 1);
                break;

            case LatticeValueTransformKind.Compute when children.Length > 0:
                builder.Append(expression.ComputeOperator switch
                {
                    LatticeComputeOperator.Concat => "the text of ",
                    LatticeComputeOperator.Coalesce => "the first present of ",
                    _ => "a computation over ",
                });
                for (var index = 0; index < children.Length; index++)
                {
                    if (index > 0)
                    {
                        builder.Append(index == children.Length - 1 ? " and " : ", ");
                    }

                    Write(builder, children[index], depth + 1);
                }

                if (expression.ComputeOperator == LatticeComputeOperator.Concat)
                {
                    builder.Append(" joined");
                }

                break;

            default:
                builder.Append("a value this page cannot describe");
                break;
        }
    }

    private static string Path(string? path) => string.IsNullOrEmpty(path) ? "the value" : path;
}
