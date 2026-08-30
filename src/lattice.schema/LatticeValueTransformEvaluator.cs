using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Server-side evaluator for the <see cref="LatticeValueTransform"/> IR. Parses a
/// value's <c>byte[]</c> payload as a UTF-8 JSON document once, applies the
/// transform against a single mutable document model, and serializes the result
/// once. It is the transform-side sibling of the internal predicate evaluator.
/// <para>
/// The contract is deterministic and total per value: member reads always read
/// from the <i>input</i> document (never the partially-rewritten output), so the
/// result is independent of operation order among reads. Unlike the strictly
/// subtractive predicate evaluator, this evaluator <b>throws</b> a clear
/// <see cref="InvalidOperationException"/> on a null, empty, or malformed payload
/// rather than silently producing a corrupt value - a consumer driving a shadow
/// build is expected to abort on that exception.
/// </para>
/// </summary>
internal static class LatticeValueTransformEvaluator
{
    /// <summary>
    /// Maximum transform-tree nesting depth the evaluator will fold. The IR is a
    /// serializable, client-supplied tree that may be persisted and replayed, so
    /// an adversarial or corrupt payload could otherwise nest deeply enough to
    /// exhaust the call stack. The guard converts that into a catchable
    /// <see cref="InvalidOperationException"/> far below the real stack limit.
    /// Legitimate translator output is only a handful of levels deep.
    /// </summary>
    internal const int MaxDepth = 128;

    /// <summary>
    /// Evaluates <paramref name="transform"/> against the JSON document in
    /// <paramref name="value"/> and returns the resulting UTF-8 JSON document.
    /// </summary>
    public static byte[] Evaluate(byte[]? value, in LatticeValueTransform transform)
    {
        if (value is null || value.Length == 0)
        {
            throw new InvalidOperationException(
                "The value is null or empty; a value transform requires a UTF-8 JSON document.");
        }

        JsonNode? input;
        try
        {
            input = JsonNode.Parse(value);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                "The value is not a well-formed UTF-8 JSON document; the value transform cannot be applied.", ex);
        }

        var output = ApplyDocument(transform, value, input, 0);
        return Serialize(output);
    }

    private static JsonNode? ApplyDocument(in LatticeValueTransform transform, byte[] inputBytes, JsonNode? input, int depth)
    {
        ThrowIfTooDeep(depth);

        if (transform.Kind != LatticeValueTransformKind.Passthrough)
        {
            throw new InvalidOperationException(
                $"The root of a value transform must be a '{nameof(LatticeValueTransformKind.Passthrough)}' node, but was '{transform.Kind}'.");
        }

        var output = input?.DeepClone();
        var operations = transform.Children;
        if (operations is null || operations.Length == 0)
            return output;

        if (output is not JsonObject document)
        {
            throw new InvalidOperationException(
                "The value transform declares member operations, but the input document is not a JSON object.");
        }

        foreach (var operation in operations)
            ApplyOperation(operation, inputBytes, input, document, depth + 1);

        return document;
    }

    private static void ApplyOperation(in LatticeValueTransform operation, byte[] inputBytes, JsonNode? input, JsonObject document, int depth)
    {
        ThrowIfTooDeep(depth);

        switch (operation.Kind)
        {
            case LatticeValueTransformKind.SetMember:
            {
                var path = RequirePath(operation.MemberPath, operation.Kind);
                var children = operation.Children;
                if (children is null || children.Length != 1)
                    throw new InvalidOperationException("A SetMember operation must carry exactly one value-expression child.");
                document[path] = EvaluateValue(children[0], inputBytes, input, depth + 1);
                break;
            }

            case LatticeValueTransformKind.DropMember:
            {
                var path = RequirePath(operation.MemberPath, operation.Kind);
                document.Remove(path);
                break;
            }

            case LatticeValueTransformKind.RenameMember:
            {
                var from = RequirePath(operation.MemberPath, operation.Kind);
                var to = RequirePath(operation.ToPath, operation.Kind);
                if (document.TryGetPropertyValue(from, out var node))
                {
                    var clone = node?.DeepClone();
                    document.Remove(from);
                    document[to] = clone;
                }

                break;
            }

            default:
                throw new InvalidOperationException(
                    $"'{operation.Kind}' is not a document operation; only SetMember, DropMember, and RenameMember may appear in a Passthrough pipeline.");
        }
    }

    private static JsonNode? EvaluateValue(in LatticeValueTransform expression, byte[] inputBytes, JsonNode? input, int depth)
    {
        ThrowIfTooDeep(depth);

        switch (expression.Kind)
        {
            case LatticeValueTransformKind.Member:
            {
                var path = RequirePath(expression.MemberPath, expression.Kind);
                if (input is JsonObject obj && obj.TryGetPropertyValue(path, out var node) && node is not null)
                    return node.DeepClone();
                return null;
            }

            case LatticeValueTransformKind.Constant:
                return FromConstant(expression.Constant);

            case LatticeValueTransformKind.Conditional:
            {
                var children = expression.Children;
                if (children is null || children.Length != 2)
                    throw new InvalidOperationException("A Conditional expression must carry exactly two branches (then, else).");
                // The condition evaluates against the original input bytes via the
                // shared predicate evaluator. This re-parses the input document per
                // conditional (not per member), which is acceptable: conditionals
                // are rare relative to member reads, and reusing the public helper
                // keeps the boolean semantics identical to push-down rather than
                // reimplementing predicate evaluation over the parsed JsonNode.
                bool matched = LatticePredicateEvaluation.Matches(inputBytes, expression.Condition);
                return EvaluateValue(matched ? children[0] : children[1], inputBytes, input, depth + 1);
            }

            case LatticeValueTransformKind.Compute:
                return EvaluateCompute(expression, inputBytes, input, depth + 1);

            default:
                throw new InvalidOperationException(
                    $"'{expression.Kind}' is not a value expression; only Member, Constant, Conditional, and Compute may appear in value position.");
        }
    }

    private static JsonNode? EvaluateCompute(in LatticeValueTransform expression, byte[] inputBytes, JsonNode? input, int depth)
    {
        var operands = expression.Children;
        if (operands is null || operands.Length == 0)
            throw new InvalidOperationException("A Compute expression must carry at least one operand.");

        switch (expression.ComputeOperator)
        {
            case LatticeComputeOperator.Coalesce:
                foreach (var operand in operands)
                {
                    var candidate = EvaluateValue(operand, inputBytes, input, depth + 1);
                    if (candidate is not null)
                        return candidate;
                }

                return null;

            case LatticeComputeOperator.Concat:
            {
                var builder = new StringBuilder();
                foreach (var operand in operands)
                    builder.Append(RenderScalar(EvaluateValue(operand, inputBytes, input, depth + 1)));
                return JsonValue.Create(builder.ToString());
            }

            default:
                throw new InvalidOperationException($"Unknown compute operator '{expression.ComputeOperator}'.");
        }
    }

    private static string RenderScalar(JsonNode? node)
    {
        if (node is null)
            return string.Empty;
        if (node is JsonValue value && value.TryGetValue<string>(out var text))
            return text;
        return node.ToJsonString();
    }

    private static JsonNode? FromConstant(in LatticeConstant constant) => constant.Kind switch
    {
        LatticeConstantKind.Null => null,
        LatticeConstantKind.Boolean => JsonValue.Create(constant.BooleanValue),
        LatticeConstantKind.String => constant.StringValue is null ? null : JsonValue.Create(constant.StringValue),
        LatticeConstantKind.Int64 => JsonValue.Create(constant.Int64Value),
        LatticeConstantKind.Double => JsonValue.Create(constant.DoubleValue),
        // Fail closed, exactly as the unknown-operator and non-value-expression
        // arms above do. An unrecognised kind is reachable on a mixed-version
        // cluster (the enum is wire format and is persisted in
        // SchemaRemediationState.Transform), and projecting it as JSON null is
        // indistinguishable from an explicit Null constant - so a transform this
        // node cannot map would silently overwrite the member's existing value
        // across an entire remediation pass while reporting success.
        _ => throw new InvalidOperationException($"Unknown constant kind '{constant.Kind}'."),
    };

    private static byte[] Serialize(JsonNode? output)
    {
        if (output is null)
            return "null"u8.ToArray();
        // SerializeToUtf8Bytes writes UTF-8 directly, avoiding the intermediate
        // UTF-16 string an output.ToJsonString() + Encoding.UTF8.GetBytes would
        // allocate on this per-value path.
        return JsonSerializer.SerializeToUtf8Bytes(output);
    }

    private static string RequirePath(string? path, LatticeValueTransformKind kind)
    {
        if (string.IsNullOrEmpty(path))
            throw new InvalidOperationException($"A '{kind}' node requires a non-empty member path.");
        return path;
    }

    private static void ThrowIfTooDeep(int depth)
    {
        if (depth > MaxDepth)
        {
            throw new InvalidOperationException(
                $"Value-transform nesting depth exceeds the maximum of {MaxDepth}. The transform is rejected to protect the server from stack exhaustion.");
        }
    }
}
