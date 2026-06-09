using System.Text.Json;

namespace Orleans.Lattice;

/// <summary>
/// Server-side evaluator for the predicate IR. Parses a value's
/// <c>byte[]</c> payload as a JSON document and folds a
/// <see cref="LatticePredicateNode"/> tree to a boolean, resolving member
/// paths by name (ordinal, case-insensitive) against the document.
/// <para>
/// This is the single-call leaf-scan seam each predicate-aware operation uses
/// to decide whether a candidate row's value matches. It never sees the value
/// type or the serializer: the contract is purely "the bytes are a UTF-8 JSON
/// document". A value that does not parse as JSON, or whose member path is
/// absent in a way that cannot satisfy the predicate, evaluates to
/// <c>false</c> - the predicate is strictly subtractive.
/// </para>
/// </summary>
internal static class LatticePredicateEvaluator
{
    /// <summary>
    /// Evaluates <paramref name="predicate"/> against <paramref name="value"/>.
    /// Returns <c>false</c> for a null/empty payload or one that does not parse
    /// as a JSON document.
    /// </summary>
    public static bool Matches(byte[]? value, in LatticePredicateNode predicate)
    {
        if (value is null || value.Length == 0)
            return false;

        try
        {
            using var document = JsonDocument.Parse(value);
            return EvaluateBoolean(predicate, document.RootElement);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool EvaluateBoolean(in LatticePredicateNode node, JsonElement root)
    {
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Boolean:
                return EvaluateBooleanOperator(node, root);

            case LatticePredicateNodeKind.Compare:
                return EvaluateComparison(node, root);

            case LatticePredicateNodeKind.StringMethod:
                return EvaluateStringMethod(node, root);

            case LatticePredicateNodeKind.Member:
            case LatticePredicateNodeKind.Constant:
                // A bare member / constant in boolean position is truthy iff it
                // resolves to the boolean value true.
                var operand = ResolveOperand(node, root);
                return operand.Kind == OperandKind.Boolean && operand.Boolean;

            default:
                return false;
        }
    }

    private static bool EvaluateBooleanOperator(in LatticePredicateNode node, JsonElement root)
    {
        var children = node.Children;
        if (children is null || children.Length == 0)
            return false;

        switch (node.BooleanOperator)
        {
            case LatticeBooleanOperator.And:
                foreach (var child in children)
                    if (!EvaluateBoolean(child, root))
                        return false;
                return true;

            case LatticeBooleanOperator.Or:
                foreach (var child in children)
                    if (EvaluateBoolean(child, root))
                        return true;
                return false;

            case LatticeBooleanOperator.Not:
                return !EvaluateBoolean(children[0], root);

            default:
                return false;
        }
    }

    private static bool EvaluateComparison(in LatticePredicateNode node, JsonElement root)
    {
        var children = node.Children;
        if (children is null || children.Length != 2)
            return false;

        var left = ResolveOperand(children[0], root);
        var right = ResolveOperand(children[1], root);
        return Compare(node.ComparisonOperator, left, right);
    }

    private static bool EvaluateStringMethod(in LatticePredicateNode node, JsonElement root)
    {
        var children = node.Children;
        if (children is null || children.Length != 2)
            return false;

        var target = ResolveOperand(children[0], root);
        var argument = ResolveOperand(children[1], root);
        if (target.Kind != OperandKind.String || argument.Kind != OperandKind.String)
            return false;

        var s = target.String!;
        var arg = argument.String!;
        return node.StringMethod switch
        {
            LatticeStringMethod.StartsWith => s.StartsWith(arg, StringComparison.Ordinal),
            LatticeStringMethod.EndsWith => s.EndsWith(arg, StringComparison.Ordinal),
            LatticeStringMethod.Contains => s.Contains(arg, StringComparison.Ordinal),
            LatticeStringMethod.Equals => string.Equals(s, arg, StringComparison.Ordinal),
            _ => false,
        };
    }

    private static bool Compare(LatticeComparisonOperator op, in Operand left, in Operand right)
    {
        // Equality / inequality have null-aware semantics: a Missing or Null
        // operand is equal to a Null operand and to another Missing operand.
        if (op is LatticeComparisonOperator.Equal or LatticeComparisonOperator.NotEqual)
        {
            bool equal = AreEqual(left, right);
            return op == LatticeComparisonOperator.Equal ? equal : !equal;
        }

        // Ordering requires both operands present and of a comparable, matching
        // category. Anything else is false (never matches).
        int? order = CompareOrder(left, right);
        if (order is null)
            return false;

        return op switch
        {
            LatticeComparisonOperator.LessThan => order < 0,
            LatticeComparisonOperator.LessThanOrEqual => order <= 0,
            LatticeComparisonOperator.GreaterThan => order > 0,
            LatticeComparisonOperator.GreaterThanOrEqual => order >= 0,
            _ => false,
        };
    }

    private static bool AreEqual(in Operand left, in Operand right)
    {
        bool leftNullish = left.Kind is OperandKind.Null or OperandKind.Missing;
        bool rightNullish = right.Kind is OperandKind.Null or OperandKind.Missing;
        if (leftNullish || rightNullish)
            return leftNullish && rightNullish;

        if (left.Kind == OperandKind.Number && right.Kind == OperandKind.Number)
            return left.Number.Equals(right.Number);

        if (left.Kind == OperandKind.String && right.Kind == OperandKind.String)
            return string.Equals(left.String, right.String, StringComparison.Ordinal);

        if (left.Kind == OperandKind.Boolean && right.Kind == OperandKind.Boolean)
            return left.Boolean == right.Boolean;

        return false;
    }

    private static int? CompareOrder(in Operand left, in Operand right)
    {
        if (left.Kind == OperandKind.Number && right.Kind == OperandKind.Number)
            return left.Number.CompareTo(right.Number);

        if (left.Kind == OperandKind.String && right.Kind == OperandKind.String)
            return string.CompareOrdinal(left.String, right.String);

        return null;
    }

    private static Operand ResolveOperand(in LatticePredicateNode node, JsonElement root)
    {
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Constant:
                return FromConstant(node.Constant);

            case LatticePredicateNodeKind.Member:
                return ResolveMember(node.MemberPath, root);

            default:
                // Nested boolean / comparison / string-method in operand
                // position resolves to its boolean result.
                return Operand.OfBoolean(EvaluateBoolean(node, root));
        }
    }

    private static Operand ResolveMember(string? memberPath, JsonElement root)
    {
        if (string.IsNullOrEmpty(memberPath))
            return Operand.Missing();

        var element = root;
        int start = 0;
        while (start <= memberPath.Length)
        {
            int dot = memberPath.IndexOf('.', start);
            var segment = dot < 0 ? memberPath.AsSpan(start) : memberPath.AsSpan(start, dot - start);

            if (element.ValueKind != JsonValueKind.Object || !TryGetProperty(element, segment, out element))
                return Operand.Missing();

            if (dot < 0)
                break;
            start = dot + 1;
        }

        return FromJson(element);
    }

    private static bool TryGetProperty(JsonElement obj, ReadOnlySpan<char> name, out JsonElement value)
    {
        foreach (var property in obj.EnumerateObject())
        {
            if (name.Equals(property.Name.AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static Operand FromJson(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null => Operand.Null(),
        JsonValueKind.True => Operand.OfBoolean(true),
        JsonValueKind.False => Operand.OfBoolean(false),
        JsonValueKind.String => Operand.OfString(element.GetString() ?? string.Empty),
        JsonValueKind.Number => Operand.OfNumber(element.GetDouble()),
        // Objects and arrays are not comparable in the allowlist.
        _ => Operand.Missing(),
    };

    private static Operand FromConstant(in LatticeConstant constant) => constant.Kind switch
    {
        LatticeConstantKind.Null => Operand.Null(),
        LatticeConstantKind.Boolean => Operand.OfBoolean(constant.BooleanValue),
        LatticeConstantKind.String => constant.StringValue is null ? Operand.Null() : Operand.OfString(constant.StringValue),
        LatticeConstantKind.Int64 => Operand.OfNumber(constant.Int64Value),
        LatticeConstantKind.Double => Operand.OfNumber(constant.DoubleValue),
        _ => Operand.Missing(),
    };

    private enum OperandKind : byte { Missing, Null, Boolean, String, Number }

    private readonly struct Operand
    {
        public OperandKind Kind { get; private init; }
        public bool Boolean { get; private init; }
        public string? String { get; private init; }
        public double Number { get; private init; }

        public static Operand Missing() => new() { Kind = OperandKind.Missing };
        public static Operand Null() => new() { Kind = OperandKind.Null };
        public static Operand OfBoolean(bool value) => new() { Kind = OperandKind.Boolean, Boolean = value };
        public static Operand OfString(string value) => new() { Kind = OperandKind.String, String = value };
        public static Operand OfNumber(double value) => new() { Kind = OperandKind.Number, Number = value };
    }
}
