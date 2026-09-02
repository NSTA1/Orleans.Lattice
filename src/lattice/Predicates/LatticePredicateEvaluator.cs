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
    /// Maximum predicate-tree nesting depth the evaluator will fold. The IR is
    /// a serializable, client-supplied tree that arrives on grain parameters,
    /// the ambient request context, and durable cursor / atomic-write state, so
    /// an adversarial or corrupt payload could otherwise nest deeply enough to
    /// exhaust the call stack and crash the silo with an uncatchable
    /// <see cref="StackOverflowException"/>. The guard converts that into a
    /// catchable <see cref="InvalidOperationException"/> raised far below the
    /// real stack limit. Legitimate translator output is only a handful of
    /// levels deep, well under this ceiling.
    /// </summary>
    internal const int MaxDepth = 128;

    private static void ThrowIfTooDeep(int depth)
    {
        if (depth > MaxDepth)
        {
            throw new InvalidOperationException(
                $"Predicate nesting depth exceeds the maximum of {MaxDepth}. The predicate is " +
                "rejected to protect the server from stack exhaustion.");
        }
    }

    /// <summary>
    /// Evaluates <paramref name="predicate"/> against <paramref name="value"/>.
    /// Returns <c>false</c> for a null/empty payload or one that does not parse
    /// as a JSON document.
    /// </summary>
    public static bool Matches(byte[]? value, in LatticePredicateNode predicate)
    {
        if (value is null || value.Length == 0)
            return false;

        // Fast path: when the predicate references at least one member and
        // every member path is a single top-level property name (no nested
        // 'a.b' paths), fold the tree with a forward-only Utf8JsonReader
        // instead of materializing a JsonDocument. The reader is a ref struct,
        // so the common numeric / boolean predicate resolves with zero heap
        // allocation - the JsonDocument object and its metadata database are
        // never built. A single zero-alloc validation pass reproduces the
        // exact "must be well-formed JSON, else false" contract even when the
        // tree short-circuits before resolving any member; malformed input
        // throws JsonException from the reader exactly as JsonDocument.Parse
        // would, and is caught here as a non-match.
        if (IsFastPathEligible(predicate))
        {
            try
            {
                ReadOnlySpan<byte> json = value;
                Validate(json);
                return EvaluateBoolean(predicate, json, 0);
            }
            catch (JsonException)
            {
                return false;
            }
        }

        // Slow path: nested ('a.b') member paths - or a constant-only predicate
        // whose validity gate is the parse itself - fold against a JsonDocument,
        // whose random-access object model resolves dotted paths without
        // re-scanning the buffer per segment.
        try
        {
            using var document = JsonDocument.Parse(value);
            return EvaluateBoolean(predicate, document.RootElement, 0);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// True when the predicate references at least one member and every member
    /// path is a single, non-empty top-level property name. Nested ('a.b')
    /// paths and member-free predicates fall to the JsonDocument slow path so
    /// dotted resolution and the parse-as-validation gate are preserved exactly.
    /// </summary>
    private static bool IsFastPathEligible(in LatticePredicateNode node)
    {
        int memberCount = 0;
        return CollectFastPathMembers(node, ref memberCount) && memberCount > 0;
    }

    private static bool CollectFastPathMembers(in LatticePredicateNode node, ref int memberCount)
    {
        if (node.Kind == LatticePredicateNodeKind.Member)
        {
            var path = node.MemberPath;
            if (string.IsNullOrEmpty(path) || path.IndexOf('.') >= 0)
                return false;
            memberCount++;
            return true;
        }

        var children = node.Children;
        if (children is not null)
        {
            foreach (var child in children)
                if (!CollectFastPathMembers(child, ref memberCount))
                    return false;
        }

        return true;
    }

    /// <summary>
    /// Reads every token of <paramref name="json"/> to force the same
    /// well-formedness check <see cref="JsonDocument.Parse(System.ReadOnlyMemory{byte}, JsonDocumentOptions)"/>
    /// performs - malformed input throws <see cref="JsonException"/> - without
    /// allocating a document. Decoupling the validity gate from evaluation
    /// keeps the contract exact even when the tree short-circuits before
    /// resolving (and thereby reading past) the malformed region.
    /// </summary>
    private static void Validate(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);
        while (reader.Read())
        {
        }
    }

    private static bool EvaluateBoolean(in LatticePredicateNode node, JsonElement root, int depth)
    {
        ThrowIfTooDeep(depth);
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Boolean:
                return EvaluateBooleanOperator(node, root, depth);

            case LatticePredicateNodeKind.Compare:
                return EvaluateComparison(node, root, depth);

            case LatticePredicateNodeKind.StringMethod:
                return EvaluateStringMethod(node, root, depth);

            case LatticePredicateNodeKind.Member:
            case LatticePredicateNodeKind.Constant:
                // A bare member / constant in boolean position is truthy iff it
                // resolves to the boolean value true.
                var operand = ResolveOperand(node, root, depth);
                return operand.Kind == OperandKind.Boolean && operand.Boolean;

            default:
                return false;
        }
    }

    private static bool EvaluateBooleanOperator(in LatticePredicateNode node, JsonElement root, int depth)
    {
        var children = node.Children;
        if (children is null || children.Length == 0)
            return false;

        switch (node.BooleanOperator)
        {
            case LatticeBooleanOperator.And:
                foreach (var child in children)
                    if (!EvaluateBoolean(child, root, depth + 1))
                        return false;
                return true;

            case LatticeBooleanOperator.Or:
                foreach (var child in children)
                    if (EvaluateBoolean(child, root, depth + 1))
                        return true;
                return false;

            case LatticeBooleanOperator.Not:
                return !EvaluateBoolean(children[0], root, depth + 1);

            default:
                return false;
        }
    }

    private static bool EvaluateComparison(in LatticePredicateNode node, JsonElement root, int depth)
    {
        var children = node.Children;
        if (children is null || children.Length != 2)
            return false;

        var left = ResolveOperand(children[0], root, depth + 1);
        var right = ResolveOperand(children[1], root, depth + 1);
        return Compare(node.ComparisonOperator, left, right);
    }

    private static bool EvaluateStringMethod(in LatticePredicateNode node, JsonElement root, int depth)
    {
        var children = node.Children;
        if (children is null || children.Length != 2)
            return false;

        var target = ResolveOperand(children[0], root, depth + 1);
        var argument = ResolveOperand(children[1], root, depth + 1);
        return ApplyStringMethod(node.StringMethod, target, argument);
    }

    private static bool ApplyStringMethod(LatticeStringMethod method, in Operand target, in Operand argument)
    {
        if (target.Kind != OperandKind.String || argument.Kind != OperandKind.String)
            return false;

        var s = target.String!;
        var arg = argument.String!;
        return method switch
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
        {
            // Compare two integral operands exactly as Int64 so 64-bit
            // identifiers above 2^53 (e.g. snowflake IDs) are distinguished.
            // Both a JSON integer and an Int64 constant would otherwise funnel
            // through double and collapse adjacent longs to the same value. A
            // non-integral operand on either side falls back to a double
            // comparison.
            if (left.IsInteger && right.IsInteger)
                return left.Integer == right.Integer;
            return left.AsDouble().Equals(right.AsDouble());
        }

        if (left.Kind == OperandKind.String && right.Kind == OperandKind.String)
            return string.Equals(left.String, right.String, StringComparison.Ordinal);

        if (left.Kind == OperandKind.Boolean && right.Kind == OperandKind.Boolean)
            return left.Boolean == right.Boolean;

        return false;
    }

    private static int? CompareOrder(in Operand left, in Operand right)
    {
        if (left.Kind == OperandKind.Number && right.Kind == OperandKind.Number)
        {
            // Order two integral operands exactly as Int64 (see AreEqual); a
            // non-integral operand on either side falls back to double order.
            if (left.IsInteger && right.IsInteger)
                return left.Integer.CompareTo(right.Integer);
            return left.AsDouble().CompareTo(right.AsDouble());
        }

        if (left.Kind == OperandKind.String && right.Kind == OperandKind.String)
            return string.CompareOrdinal(left.String, right.String);

        return null;
    }

    private static Operand ResolveOperand(in LatticePredicateNode node, JsonElement root, int depth)
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
                return Operand.OfBoolean(EvaluateBoolean(node, root, depth));
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
        // Fast path: ordinal lookup straight off the parsed metadata - no
        // per-property name string is materialized. This is the common case
        // because the predicate member path is the CLR property name and the
        // default JSON serialization preserves that casing.
        if (obj.TryGetProperty(name, out value))
            return true;

        // Fallback: case-insensitive match, reached only on an ordinal miss
        // (e.g. the serializer applied a camelCase naming policy). Only here
        // do we pay the per-property name allocation, and only until a match.
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
        JsonValueKind.Number => element.TryGetInt64(out long i) ? Operand.OfInteger(i) : Operand.OfNumber(element.GetDouble()),
        // Objects and arrays are not comparable in the allowlist.
        _ => Operand.Missing(),
    };

    // ===== Utf8JsonReader fast path (top-level-member predicates) =====
    // Structural twins of the JsonElement folds above that thread the raw UTF-8
    // buffer instead of a parsed document. They share the source-agnostic leaf
    // logic (Compare / AreEqual / ApplyStringMethod / FromConstant / Operand);
    // only member resolution differs - each Member node is resolved by a fresh
    // forward scan of the (already validated) buffer.

    private static bool EvaluateBoolean(in LatticePredicateNode node, ReadOnlySpan<byte> json, int depth)
    {
        ThrowIfTooDeep(depth);
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Boolean:
                return EvaluateBooleanOperator(node, json, depth);

            case LatticePredicateNodeKind.Compare:
                return EvaluateComparison(node, json, depth);

            case LatticePredicateNodeKind.StringMethod:
                return EvaluateStringMethod(node, json, depth);

            case LatticePredicateNodeKind.Member:
            case LatticePredicateNodeKind.Constant:
                var operand = ResolveOperand(node, json, depth);
                return operand.Kind == OperandKind.Boolean && operand.Boolean;

            default:
                return false;
        }
    }

    private static bool EvaluateBooleanOperator(in LatticePredicateNode node, ReadOnlySpan<byte> json, int depth)
    {
        var children = node.Children;
        if (children is null || children.Length == 0)
            return false;

        switch (node.BooleanOperator)
        {
            case LatticeBooleanOperator.And:
                foreach (var child in children)
                    if (!EvaluateBoolean(child, json, depth + 1))
                        return false;
                return true;

            case LatticeBooleanOperator.Or:
                foreach (var child in children)
                    if (EvaluateBoolean(child, json, depth + 1))
                        return true;
                return false;

            case LatticeBooleanOperator.Not:
                return !EvaluateBoolean(children[0], json, depth + 1);

            default:
                return false;
        }
    }

    private static bool EvaluateComparison(in LatticePredicateNode node, ReadOnlySpan<byte> json, int depth)
    {
        var children = node.Children;
        if (children is null || children.Length != 2)
            return false;

        var left = ResolveOperand(children[0], json, depth + 1);
        var right = ResolveOperand(children[1], json, depth + 1);
        return Compare(node.ComparisonOperator, left, right);
    }

    private static bool EvaluateStringMethod(in LatticePredicateNode node, ReadOnlySpan<byte> json, int depth)
    {
        var children = node.Children;
        if (children is null || children.Length != 2)
            return false;

        var target = ResolveOperand(children[0], json, depth + 1);
        var argument = ResolveOperand(children[1], json, depth + 1);
        return ApplyStringMethod(node.StringMethod, target, argument);
    }

    private static Operand ResolveOperand(in LatticePredicateNode node, ReadOnlySpan<byte> json, int depth)
    {
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Constant:
                return FromConstant(node.Constant);

            case LatticePredicateNodeKind.Member:
                return ResolveMember(node.MemberPath, json);

            default:
                return Operand.OfBoolean(EvaluateBoolean(node, json, depth));
        }
    }

    private static Operand ResolveMember(string? memberPath, ReadOnlySpan<byte> json)
    {
        // Eligibility guarantees a non-empty, dot-free path; stay defensive.
        if (string.IsNullOrEmpty(memberPath))
            return Operand.Missing();

        // Pass 1: ordinal property match - zero-alloc (the reader compares the
        // member name against the UTF-8 name bytes directly) and short-circuits
        // at the first hit. This is the common case: the member path is the CLR
        // property name and default serialization preserves its casing. Mirrors
        // the slow path's JsonElement.TryGetProperty(span) ordinal lookup.
        var ordinal = new Utf8JsonReader(json);
        if (!ordinal.Read() || ordinal.TokenType != JsonTokenType.StartObject)
            return Operand.Missing();

        while (ordinal.Read() && ordinal.TokenType == JsonTokenType.PropertyName)
        {
            bool match = ordinal.ValueTextEquals(memberPath);
            ordinal.Read();
            if (match)
                return OperandFromValue(ref ordinal);
            ordinal.Skip();
        }

        // Pass 2: case-insensitive fallback, reached only when no property
        // ordinally matches (e.g. a camelCase naming policy). Mirrors the slow
        // path's enumerate-then-compare fallback: it pays the per-name string
        // only here and returns the first case-insensitive match in document
        // order.
        var insensitive = new Utf8JsonReader(json);
        insensitive.Read();
        while (insensitive.Read() && insensitive.TokenType == JsonTokenType.PropertyName)
        {
            string name = insensitive.GetString()!;
            insensitive.Read();
            if (string.Equals(name, memberPath, StringComparison.OrdinalIgnoreCase))
                return OperandFromValue(ref insensitive);
            insensitive.Skip();
        }

        return Operand.Missing();
    }

    private static Operand OperandFromValue(ref Utf8JsonReader reader) => reader.TokenType switch
    {
        JsonTokenType.Null => Operand.Null(),
        JsonTokenType.True => Operand.OfBoolean(true),
        JsonTokenType.False => Operand.OfBoolean(false),
        JsonTokenType.String => Operand.OfString(reader.GetString() ?? string.Empty),
        JsonTokenType.Number => reader.TryGetInt64(out long i) ? Operand.OfInteger(i) : Operand.OfNumber(reader.GetDouble()),
        // Objects and arrays are not comparable in the allowlist.
        _ => Operand.Missing(),
    };

    private static Operand FromConstant(in LatticeConstant constant) => constant.Kind switch
    {
        LatticeConstantKind.Null => Operand.Null(),
        LatticeConstantKind.Boolean => Operand.OfBoolean(constant.BooleanValue),
        LatticeConstantKind.String => constant.StringValue is null ? Operand.Null() : Operand.OfString(constant.StringValue),
        LatticeConstantKind.Int64 => Operand.OfInteger(constant.Int64Value),
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

        /// <summary>The exact Int64 value when <see cref="IsInteger"/> is true.</summary>
        public long Integer { get; private init; }

        /// <summary>
        /// True when this numeric operand carries an exact integral value in
        /// <see cref="Integer"/>. Integral operands compare exactly as Int64
        /// rather than through <see cref="Number"/>, so 64-bit identifiers
        /// above 2^53 are not collapsed by double rounding.
        /// </summary>
        public bool IsInteger { get; private init; }

        public static Operand Missing() => new() { Kind = OperandKind.Missing };
        public static Operand Null() => new() { Kind = OperandKind.Null };
        public static Operand OfBoolean(bool value) => new() { Kind = OperandKind.Boolean, Boolean = value };
        public static Operand OfString(string value) => new() { Kind = OperandKind.String, String = value };
        public static Operand OfNumber(double value) => new() { Kind = OperandKind.Number, Number = value };
        public static Operand OfInteger(long value) => new() { Kind = OperandKind.Number, Number = value, Integer = value, IsInteger = true };

        /// <summary>The double view of a numeric operand, for mixed integer/real comparison.</summary>
        public double AsDouble() => IsInteger ? Integer : Number;
    }
}
