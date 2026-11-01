using System.Globalization;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Compiles rule-builder cards to the policy model. Formats and patterns become
/// pattern rules; the whole-value checks become encoding rules; every other card
/// becomes a structured predicate, written in the forms an older cluster already
/// evaluates wherever one exists (a presence, text, number or true-or-false check
/// uses comparisons and string methods), and in the structural forms (type,
/// length, every item) only where nothing older can say it.
/// </summary>
/// <remarks>
/// The output shapes are exact and fixed: <see cref="SchemaCardDecompiler"/>
/// recognises each of them, so a card survives a round trip through a saved policy.
/// </remarks>
internal static class SchemaCardCompiler
{
    /// <summary>Whether a card of <paramref name="kind"/> compiles to a predicate, so it can sit in an "any of" group or inside "every item".</summary>
    /// <param name="kind">The card kind.</param>
    /// <returns><see langword="true"/> for a predicate card.</returns>
    public static bool IsPredicate(SchemaCardKind kind) => kind is not (
        SchemaCardKind.Format or SchemaCardKind.Pattern or SchemaCardKind.Encoding or SchemaCardKind.MaxSize or SchemaCardKind.Custom);

    /// <summary>Whether a card of <paramref name="kind"/> may be marked optional (a missing member passes).</summary>
    /// <param name="kind">The card kind.</param>
    /// <returns><see langword="true"/> when it may.</returns>
    public static bool CanBeOptional(SchemaCardKind kind) =>
        IsPredicate(kind) && kind is not (SchemaCardKind.Required or SchemaCardKind.AnyOf);

    /// <summary>Whether a card of <paramref name="kind"/> always judges the whole value, never a member.</summary>
    /// <param name="kind">The card kind.</param>
    /// <returns><see langword="true"/> for the encoding and size cards.</returns>
    public static bool IsWholeValueOnly(SchemaCardKind kind) => kind is SchemaCardKind.Encoding or SchemaCardKind.MaxSize;

    /// <summary>Compiles <paramref name="card"/> to one policy rule.</summary>
    /// <param name="card">The card.</param>
    /// <param name="rule">The rule, when it compiles.</param>
    /// <param name="error">Why it does not, as a sentence.</param>
    /// <returns><see langword="true"/> when the card compiles.</returns>
    public static bool TryCompile(SchemaRuleCard card, out LatticeSchemaRule rule, out string? error)
    {
        ArgumentNullException.ThrowIfNull(card);
        rule = default;
        var description = string.IsNullOrWhiteSpace(card.Description) ? null : card.Description.Trim();
        if (!TryCheckPath(card, out error))
        {
            return false;
        }

        switch (card.Kind)
        {
            case SchemaCardKind.Custom:
                if (card.Original is not { } original)
                {
                    error = "This rule has nothing to keep.";
                    return false;
                }

                rule = original;
                return true;

            case SchemaCardKind.Encoding:
                rule = card.Encoding == LatticeSchemaEncodingKind.Utf8 ? LatticeSchemaRule.Utf8(description) : LatticeSchemaRule.Json(description);
                return true;

            case SchemaCardKind.MaxSize:
                if (!TryWhole(card.MaxBytes, "the largest size, in bytes,", out var bytes, out error))
                {
                    return false;
                }

                if (bytes > int.MaxValue)
                {
                    error = $"The largest size can be at most {int.MaxValue:N0} bytes.";
                    return false;
                }

                rule = LatticeSchemaRule.MaxLength((int)bytes, description);
                return true;

            case SchemaCardKind.Format:
                rule = LatticeSchemaRule.Regex(SchemaFormatPatterns.PatternOf(card.Format), MemberOf(card), description);
                return true;

            case SchemaCardKind.Pattern:
                if (!SchemaPatterns.TryCompile(card.Pattern, out _, out error))
                {
                    return false;
                }

                rule = LatticeSchemaRule.Regex(card.Pattern, MemberOf(card), description);
                return true;

            default:
                if (!TryCompilePredicate(card, out var predicate, out error))
                {
                    return false;
                }

                rule = LatticeSchemaRule.Structured(predicate, description);
                return true;
        }
    }

    /// <summary>Compiles a predicate card to its predicate, including its optional wrapper.</summary>
    /// <param name="card">The card.</param>
    /// <param name="predicate">The predicate, when it compiles.</param>
    /// <param name="error">Why it does not, as a sentence.</param>
    /// <returns><see langword="true"/> when the card compiles.</returns>
    public static bool TryCompilePredicate(SchemaRuleCard card, out LatticePredicateNode predicate, out string? error)
    {
        ArgumentNullException.ThrowIfNull(card);
        predicate = default;
        if (!IsPredicate(card.Kind))
        {
            error = card.Kind switch
            {
                SchemaCardKind.Format or SchemaCardKind.Pattern => "A format or pattern checks one member on its own, so it cannot be combined with \"any of\" or \"every item\".",
                SchemaCardKind.Encoding or SchemaCardKind.MaxSize => "A whole-value check cannot be combined with \"any of\" or \"every item\".",
                _ => "A custom rule cannot be combined with other cards.",
            };
            return false;
        }

        if (!TryCheckPath(card, out error) || !TryCompileCore(card, out var core, out error))
        {
            return false;
        }

        predicate = card.Optional && CanBeOptional(card.Kind)
            ? LatticePredicateNode.Bool(LatticeBooleanOperator.Or, IsMissing(card.Path), core)
            : core;
        return true;
    }

    /// <summary>The operand naming a card's subject: its member, or the current document when the path is empty.</summary>
    /// <param name="path">The path.</param>
    /// <returns>The operand.</returns>
    internal static LatticePredicateNode Operand(string path) =>
        path.Length == 0 ? LatticePredicateNode.Self() : LatticePredicateNode.Member(path);

    /// <summary>The optional wrapper's first branch: the subject is missing or null.</summary>
    /// <param name="path">The path.</param>
    /// <returns>The predicate.</returns>
    internal static LatticePredicateNode IsMissing(string path) =>
        LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, Operand(path), Null);

    /// <summary>The JSON null constant.</summary>
    internal static LatticePredicateNode Null => LatticePredicateNode.Const(LatticeConstant.Null());

    /// <summary>An integer constant.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The constant node.</returns>
    internal static LatticePredicateNode Integer(long value) => LatticePredicateNode.Const(LatticeConstant.Integer(value));

    /// <summary>Parses a number as typed: an integer when it has no fraction, otherwise a real.</summary>
    /// <param name="text">The text.</param>
    /// <param name="constant">The constant.</param>
    /// <returns><see langword="true"/> when it parses.</returns>
    internal static bool TryNumber(string text, out LatticeConstant constant)
    {
        var trimmed = text.Trim().Replace(",", string.Empty, StringComparison.Ordinal);
        if (long.TryParse(trimmed, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var whole))
        {
            constant = LatticeConstant.Integer(whole);
            return true;
        }

        if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var real) && double.IsFinite(real))
        {
            constant = LatticeConstant.Real(real);
            return true;
        }

        constant = default;
        return false;
    }

    private static bool TryCompileCore(SchemaRuleCard card, out LatticePredicateNode node, out string? error)
    {
        node = default;
        error = null;
        var path = card.Path;
        var subject = Operand(path);
        var structuralPath = path.Length == 0 ? null : path;
        switch (card.Kind)
        {
            case SchemaCardKind.Required:
                node = card.Structural
                    ? LatticePredicateNode.TypeOf(structuralPath, LatticeValueKind.Present)
                    : LatticePredicateNode.Compare(LatticeComparisonOperator.NotEqual, subject, Null);
                return true;

            case SchemaCardKind.Type:
                node = card.ValueType switch
                {
                    SchemaValueType.Text => LatticePredicateNode.StringCall(LatticeStringMethod.StartsWith, subject, LatticePredicateNode.Const(LatticeConstant.Text(string.Empty))),
                    SchemaValueType.Number => LatticePredicateNode.Bool(
                        LatticeBooleanOperator.Or,
                        LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, subject, Integer(0)),
                        LatticePredicateNode.Compare(LatticeComparisonOperator.LessThan, subject, Integer(0))),
                    SchemaValueType.Boolean => LatticePredicateNode.Bool(
                        LatticeBooleanOperator.Or,
                        LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, subject, LatticePredicateNode.Const(LatticeConstant.Bool(true))),
                        LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, subject, LatticePredicateNode.Const(LatticeConstant.Bool(false)))),
                    SchemaValueType.Object => LatticePredicateNode.TypeOf(structuralPath, LatticeValueKind.Object),
                    _ => LatticePredicateNode.TypeOf(structuralPath, LatticeValueKind.Array),
                };
                return true;

            case SchemaCardKind.OneOf:
                return TryOneOf(card, subject, out node, out error);

            case SchemaCardKind.NumberRange:
                return TryNumberRange(card, subject, structuralPath, out node, out error);

            case SchemaCardKind.TextLength:
                return TryLengthRange(card, structuralPath, LatticeValueKind.String, "characters", out node, out error);

            case SchemaCardKind.ListLength:
                return TryLengthRange(card, structuralPath, LatticeValueKind.Array, "items", out node, out error);

            case SchemaCardKind.TextMatch:
                if (card.MatchText.Length == 0)
                {
                    error = "Enter the text to look for.";
                    return false;
                }

                var method = card.Match switch
                {
                    SchemaTextMatch.StartsWith => LatticeStringMethod.StartsWith,
                    SchemaTextMatch.EndsWith => LatticeStringMethod.EndsWith,
                    _ => LatticeStringMethod.Contains,
                };
                node = LatticePredicateNode.StringCall(method, subject, LatticePredicateNode.Const(LatticeConstant.Text(card.MatchText)));
                return true;

            case SchemaCardKind.EveryItem:
                if (card.Item is not { } item)
                {
                    error = "Say what every item must be.";
                    return false;
                }

                if (!TryCompilePredicate(item, out var itemPredicate, out var itemError))
                {
                    error = "Every item: " + itemError;
                    return false;
                }

                node = LatticePredicateNode.Every(structuralPath, itemPredicate);
                return true;

            case SchemaCardKind.AnyOf:
                if (card.Alternatives.Count < 2)
                {
                    error = "An \"any of\" group needs at least two alternatives.";
                    return false;
                }

                var alternatives = new LatticePredicateNode[card.Alternatives.Count];
                for (var index = 0; index < alternatives.Length; index++)
                {
                    if (!TryCompilePredicate(card.Alternatives[index], out alternatives[index], out var alternativeError))
                    {
                        error = $"Alternative {index + 1}: {alternativeError}";
                        return false;
                    }
                }

                node = LatticePredicateNode.Bool(LatticeBooleanOperator.Or, alternatives);
                return true;

            default:
                error = "This card cannot be written as a predicate.";
                return false;
        }
    }

    private static bool TryOneOf(SchemaRuleCard card, LatticePredicateNode subject, out LatticePredicateNode node, out string? error)
    {
        node = default;
        error = null;
        var values = card.Values.Where(value => value.Length > 0).Distinct(StringComparer.Ordinal).ToArray();
        if (values.Length == 0)
        {
            error = "Add at least one allowed value.";
            return false;
        }

        var options = new LatticePredicateNode[values.Length];
        for (var index = 0; index < values.Length; index++)
        {
            LatticeConstant constant;
            if (card.ValuesAreNumbers)
            {
                if (!TryNumber(values[index], out constant))
                {
                    error = $"\"{values[index]}\" is not a number. Compare the values as text instead, or remove it.";
                    return false;
                }
            }
            else
            {
                constant = LatticeConstant.Text(values[index]);
            }

            options[index] = LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, subject, LatticePredicateNode.Const(constant));
        }

        node = options.Length == 1 ? options[0] : LatticePredicateNode.Bool(LatticeBooleanOperator.Or, options);
        return true;
    }

    private static bool TryNumberRange(SchemaRuleCard card, LatticePredicateNode subject, string? structuralPath, out LatticePredicateNode node, out string? error)
    {
        node = default;
        error = null;
        LatticeConstant? minimum = null;
        LatticeConstant? maximum = null;
        if (card.Minimum.Trim().Length > 0)
        {
            if (!TryNumber(card.Minimum, out var parsed))
            {
                error = "Enter the smallest allowed number, such as 0 or -2.5.";
                return false;
            }

            minimum = parsed;
        }

        if (card.Maximum.Trim().Length > 0)
        {
            if (!TryNumber(card.Maximum, out var parsed))
            {
                error = "Enter the largest allowed number, such as 10000.";
                return false;
            }

            maximum = parsed;
        }

        if (minimum is null && maximum is null && !card.IntegerOnly)
        {
            error = "Enter a smallest number, a largest number or both, or require a whole number.";
            return false;
        }

        if (minimum is { } low && maximum is { } high
            && (low.Kind == LatticeConstantKind.Int64 && high.Kind == LatticeConstantKind.Int64
                ? low.Int64Value > high.Int64Value
                : AsDouble(low) > AsDouble(high)))
        {
            error = "The smallest number is larger than the largest.";
            return false;
        }

        var parts = new List<LatticePredicateNode>(3);
        if (card.IntegerOnly)
        {
            parts.Add(LatticePredicateNode.TypeOf(structuralPath, LatticeValueKind.Integer));
        }

        if (minimum is { } min)
        {
            parts.Add(LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, subject, LatticePredicateNode.Const(min)));
        }

        if (maximum is { } max)
        {
            parts.Add(LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, subject, LatticePredicateNode.Const(max)));
        }

        node = parts.Count == 1 ? parts[0] : LatticePredicateNode.Bool(LatticeBooleanOperator.And, [.. parts]);
        return true;
    }

    private static bool TryLengthRange(SchemaRuleCard card, string? structuralPath, LatticeValueKind kind, string unit, out LatticePredicateNode node, out string? error)
    {
        node = default;
        long? minimum = null;
        long? maximum = null;
        if (card.Minimum.Trim().Length > 0)
        {
            if (!TryWhole(card.Minimum, $"the fewest {unit}", out var parsed, out error))
            {
                return false;
            }

            minimum = parsed;
        }

        if (card.Maximum.Trim().Length > 0)
        {
            if (!TryWhole(card.Maximum, $"the most {unit}", out var parsed, out error))
            {
                return false;
            }

            maximum = parsed;
        }

        if (minimum is null && maximum is null)
        {
            error = $"Enter the fewest {unit}, the most {unit} or both.";
            return false;
        }

        if (minimum > maximum)
        {
            error = $"The fewest {unit} is more than the most.";
            return false;
        }

        error = null;
        var parts = new List<LatticePredicateNode>(3) { LatticePredicateNode.TypeOf(structuralPath, kind) };
        if (minimum is { } min)
        {
            parts.Add(LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.LengthOf(structuralPath), Integer(min)));
        }

        if (maximum is { } max)
        {
            parts.Add(LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, LatticePredicateNode.LengthOf(structuralPath), Integer(max)));
        }

        node = LatticePredicateNode.Bool(LatticeBooleanOperator.And, [.. parts]);
        return true;
    }

    private static bool TryWhole(string text, string what, out long value, out string? error)
    {
        error = null;
        if (!long.TryParse(text.Trim().Replace(",", string.Empty, StringComparison.Ordinal), NumberStyles.None, CultureInfo.InvariantCulture, out value))
        {
            error = $"Enter {what} as a whole number.";
            return false;
        }

        return true;
    }

    private static bool TryCheckPath(SchemaRuleCard card, out string? error)
    {
        error = null;
        if (IsWholeValueOnly(card.Kind) || card.Kind is SchemaCardKind.Custom or SchemaCardKind.AnyOf)
        {
            return true;
        }

        var path = card.Path;
        if (path.Length > 0 && (path.StartsWith('.') || path.EndsWith('.') || path.Contains("..", StringComparison.Ordinal) || path.Any(char.IsWhiteSpace)))
        {
            error = "A member path is member names joined by dots, such as order.total, with no spaces.";
            return false;
        }

        return true;
    }

    private static string? MemberOf(SchemaRuleCard card) => card.Path.Length == 0 ? null : card.Path;

    private static double AsDouble(LatticeConstant constant) =>
        constant.Kind == LatticeConstantKind.Int64 ? constant.Int64Value : constant.DoubleValue;
}
