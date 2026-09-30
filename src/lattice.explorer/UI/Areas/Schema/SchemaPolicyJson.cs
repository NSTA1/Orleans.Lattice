using System.Text;
using System.Text.Json;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// A policy written out exactly, as indented JSON: every rule's kind and the
/// fields that kind uses, and every predicate node in full. The builder's
/// advanced view shows it read-only so an expert can see precisely what will be
/// saved. Text only, never markup.
/// </summary>
internal static class SchemaPolicyJson
{
    private static readonly JsonWriterOptions Options = new() { Indented = true };

    /// <summary>Writes <paramref name="policy"/>.</summary>
    /// <param name="policy">The policy.</param>
    /// <returns>The JSON text.</returns>
    public static string Write(LatticeSchemaPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, Options))
        {
            writer.WriteStartObject();
            writer.WriteBoolean("strictIngest", policy.StrictIngest);
            writer.WriteStartArray("rules");
            foreach (var rule in policy.Rules)
            {
                WriteRule(writer, rule);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static void WriteRule(Utf8JsonWriter writer, LatticeSchemaRule rule)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", rule.Kind.ToString());
        switch (rule.Kind)
        {
            case LatticeSchemaRuleKind.Regex:
                writer.WriteString("pattern", rule.RegexPattern);
                if (rule.MemberPath is { } member)
                {
                    writer.WriteString("member", member);
                }

                break;

            case LatticeSchemaRuleKind.Encoding:
                writer.WriteString("encoding", rule.EncodingKind.ToString());
                if (rule.MaxByteLength is { } bytes)
                {
                    writer.WriteNumber("maxByteLength", bytes);
                }

                break;

            case LatticeSchemaRuleKind.Structured when rule.Predicate is { } predicate:
                writer.WritePropertyName("predicate");
                WriteNode(writer, predicate, 0);
                break;
        }

        if (rule.Description is { } description)
        {
            writer.WriteString("description", description);
        }

        writer.WriteEndObject();
    }

    private static void WriteNode(Utf8JsonWriter writer, LatticePredicateNode node, int depth)
    {
        writer.WriteStartObject();
        writer.WriteString("kind", node.Kind.ToString());
        if (depth > SchemaPredicateText.MaximumDepth)
        {
            writer.WriteString("elided", "nested too deeply to show");
            writer.WriteEndObject();
            return;
        }

        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Member:
            case LatticePredicateNodeKind.Length:
            case LatticePredicateNodeKind.Every:
                if (node.MemberPath is { } path)
                {
                    writer.WriteString("member", path);
                }

                break;

            case LatticePredicateNodeKind.TypeOf:
                if (node.MemberPath is { } typed)
                {
                    writer.WriteString("member", typed);
                }

                writer.WriteString("valueKind", node.ValueKind.ToString());
                break;

            case LatticePredicateNodeKind.Constant:
                WriteConstant(writer, node.Constant);
                break;

            case LatticePredicateNodeKind.Compare:
                writer.WriteString("operator", node.ComparisonOperator.ToString());
                break;

            case LatticePredicateNodeKind.Boolean:
                writer.WriteString("operator", node.BooleanOperator.ToString());
                break;

            case LatticePredicateNodeKind.StringMethod:
                writer.WriteString("method", node.StringMethod.ToString());
                break;
        }

        if (node.Children is { Length: > 0 } children)
        {
            writer.WriteStartArray("children");
            foreach (var child in children)
            {
                WriteNode(writer, child, depth + 1);
            }

            writer.WriteEndArray();
        }

        writer.WriteEndObject();
    }

    private static void WriteConstant(Utf8JsonWriter writer, LatticeConstant constant)
    {
        switch (constant.Kind)
        {
            case LatticeConstantKind.Null:
                writer.WriteNull("value");
                break;
            case LatticeConstantKind.Boolean:
                writer.WriteBoolean("value", constant.BooleanValue);
                break;
            case LatticeConstantKind.Int64:
                writer.WriteNumber("value", constant.Int64Value);
                break;
            case LatticeConstantKind.Double:
                writer.WriteNumber("value", constant.DoubleValue);
                break;
            default:
                writer.WriteString("value", constant.StringValue);
                break;
        }
    }
}
