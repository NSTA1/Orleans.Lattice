using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// Reads a policy rule back as a rule-builder card: the inverse of
/// <see cref="SchemaCardCompiler"/>, matching exactly the shapes it writes. A
/// rule that matches none of them - a hand-written predicate, say - becomes a
/// <see cref="SchemaCardKind.Custom"/> card holding the rule unchanged, so
/// opening a policy in the builder never loses or rewrites anything.
/// </summary>
internal static class SchemaCardDecompiler
{
    /// <summary>Reads every rule of <paramref name="rules"/> as a card.</summary>
    /// <param name="rules">The rules.</param>
    /// <returns>The cards, one per rule, in order.</returns>
    public static List<SchemaRuleCard> Decompile(IEnumerable<LatticeSchemaRule> rules)
    {
        ArgumentNullException.ThrowIfNull(rules);
        return [.. rules.Select(Decompile)];
    }

    /// <summary>Reads <paramref name="rule"/> as a card.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The card; <see cref="SchemaCardKind.Custom"/> when no card writes this rule.</returns>
    public static SchemaRuleCard Decompile(LatticeSchemaRule rule)
    {
        var card = TryRead(rule) ?? new SchemaRuleCard { Kind = SchemaCardKind.Custom, Original = rule };
        card.Description = rule.Description ?? string.Empty;

        // A card that would compile to anything other than this very rule is not
        // a faithful reading, so it is kept as custom rather than silently changed.
        if (card.Kind != SchemaCardKind.Custom
            && !(SchemaCardCompiler.TryCompile(card, out var recompiled, out _) && recompiled.Equals(Normalise(rule))))
        {
            return new SchemaRuleCard { Kind = SchemaCardKind.Custom, Original = rule, Description = card.Description };
        }

        return card;
    }

    private static LatticeSchemaRule Normalise(LatticeSchemaRule rule) =>
        rule with { Description = string.IsNullOrWhiteSpace(rule.Description) ? null : rule.Description.Trim() };

    private static SchemaRuleCard? TryRead(LatticeSchemaRule rule)
    {
        switch (rule.Kind)
        {
            case LatticeSchemaRuleKind.Encoding:
                return rule.EncodingKind switch
                {
                    LatticeSchemaEncodingKind.Utf8 or LatticeSchemaEncodingKind.Json => new SchemaRuleCard { Kind = SchemaCardKind.Encoding, Encoding = rule.EncodingKind },
                    LatticeSchemaEncodingKind.MaxByteLength when rule.MaxByteLength is { } bytes => new SchemaRuleCard
                    {
                        Kind = SchemaCardKind.MaxSize,
                        MaxBytes = bytes.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    },
                    _ => null,
                };

            case LatticeSchemaRuleKind.Regex when rule.RegexPattern is { } pattern:
                var path = rule.MemberPath ?? string.Empty;
                return SchemaFormatPatterns.TryRecognise(pattern, out var format)
                    ? new SchemaRuleCard { Kind = SchemaCardKind.Format, Path = path, Format = format }
                    : new SchemaRuleCard { Kind = SchemaCardKind.Pattern, Path = path, Pattern = pattern };

            case LatticeSchemaRuleKind.Structured when rule.Predicate is { } predicate:
                return ReadPredicate(predicate);

            default:
                return null;
        }
    }

    /// <summary>Reads a predicate as a predicate card, or <see langword="null"/> when no card writes it.</summary>
    /// <param name="node">The predicate.</param>
    /// <returns>The card.</returns>
    internal static SchemaRuleCard? ReadPredicate(LatticePredicateNode node)
    {
        // Optional wrapper: (subject == null) || core, where core names the same subject.
        if (IsOr(node, out var children) && children.Length == 2 && TryMissing(children[0], out var missingPath))
        {
            if (ReadCore(children[1]) is { } inner && SchemaCardCompiler.CanBeOptional(inner.Kind) && inner.Path == missingPath)
            {
                inner.Optional = true;
                return inner;
            }
        }

        if (ReadCore(node) is { } card)
        {
            return card;
        }

        // "Any of": two or more alternatives, each a card.
        if (IsOr(node, out var alternatives) && alternatives.Length >= 2)
        {
            var group = new SchemaRuleCard { Kind = SchemaCardKind.AnyOf };
            foreach (var alternative in alternatives)
            {
                if (ReadPredicate(alternative) is not { } option || option.Kind == SchemaCardKind.AnyOf)
                {
                    return null;
                }

                group.Alternatives.Add(option);
            }

            return group;
        }

        return null;
    }

    private static SchemaRuleCard? ReadCore(LatticePredicateNode node)
    {
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Compare:
                return ReadComparison(node);

            case LatticePredicateNodeKind.StringMethod when node.Children is [var target, { Kind: LatticePredicateNodeKind.Constant } argument]
                && TrySubject(target, out var subject)
                && argument.Constant is { Kind: LatticeConstantKind.String, StringValue: { } text }:
                return node.StringMethod switch
                {
                    LatticeStringMethod.StartsWith when text.Length == 0 => new SchemaRuleCard { Kind = SchemaCardKind.Type, Path = subject, ValueType = SchemaValueType.Text },
                    LatticeStringMethod.StartsWith => TextMatch(subject, SchemaTextMatch.StartsWith, text),
                    LatticeStringMethod.EndsWith => TextMatch(subject, SchemaTextMatch.EndsWith, text),
                    LatticeStringMethod.Contains => TextMatch(subject, SchemaTextMatch.Contains, text),
                    _ => null,
                };

            case LatticePredicateNodeKind.TypeOf:
                var typePath = node.MemberPath ?? string.Empty;
                return node.ValueKind switch
                {
                    LatticeValueKind.Present => new SchemaRuleCard { Kind = SchemaCardKind.Required, Path = typePath, Structural = true },
                    LatticeValueKind.Object => new SchemaRuleCard { Kind = SchemaCardKind.Type, Path = typePath, ValueType = SchemaValueType.Object },
                    LatticeValueKind.Array => new SchemaRuleCard { Kind = SchemaCardKind.Type, Path = typePath, ValueType = SchemaValueType.List },
                    LatticeValueKind.Integer => new SchemaRuleCard { Kind = SchemaCardKind.NumberRange, Path = typePath, IntegerOnly = true },
                    _ => null,
                };

            case LatticePredicateNodeKind.Every when node.Children is [var body]:
                return ReadPredicate(body) is { } item
                    ? new SchemaRuleCard { Kind = SchemaCardKind.EveryItem, Path = node.MemberPath ?? string.Empty, Item = item }
                    : null;

            case LatticePredicateNodeKind.Boolean when node.BooleanOperator == LatticeBooleanOperator.And && node.Children is { Length: >= 2 } parts:
                return ReadRange(parts);

            case LatticePredicateNodeKind.Boolean when node.BooleanOperator == LatticeBooleanOperator.Or && node.Children is { Length: >= 2 } options:
                return ReadTypeOrOneOf(options);

            default:
                return null;
        }
    }

    private static SchemaRuleCard? ReadComparison(LatticePredicateNode node)
    {
        if (node.Children is not [var left, { Kind: LatticePredicateNodeKind.Constant } right] || !TrySubject(left, out var path))
        {
            return null;
        }

        var constant = right.Constant;
        switch (node.ComparisonOperator)
        {
            case LatticeComparisonOperator.NotEqual when constant.Kind == LatticeConstantKind.Null:
                return new SchemaRuleCard { Kind = SchemaCardKind.Required, Path = path };

            case LatticeComparisonOperator.Equal when constant.Kind is LatticeConstantKind.String or LatticeConstantKind.Int64 or LatticeConstantKind.Double:
                return new SchemaRuleCard
                {
                    Kind = SchemaCardKind.OneOf,
                    Path = path,
                    Values = [Text(constant)],
                    ValuesAreNumbers = constant.Kind != LatticeConstantKind.String,
                };

            case LatticeComparisonOperator.GreaterThanOrEqual when IsNumber(constant):
                return new SchemaRuleCard { Kind = SchemaCardKind.NumberRange, Path = path, Minimum = Text(constant) };

            case LatticeComparisonOperator.LessThanOrEqual when IsNumber(constant):
                return new SchemaRuleCard { Kind = SchemaCardKind.NumberRange, Path = path, Maximum = Text(constant) };

            default:
                return null;
        }
    }

    private static SchemaRuleCard? ReadRange(LatticePredicateNode[] parts)
    {
        // Text or list length: TypeOf(kind) && length >= a [&& length <= b].
        if (parts[0] is { Kind: LatticePredicateNodeKind.TypeOf } type && type.ValueKind is LatticeValueKind.String or LatticeValueKind.Array)
        {
            var card = new SchemaRuleCard
            {
                Kind = type.ValueKind == LatticeValueKind.String ? SchemaCardKind.TextLength : SchemaCardKind.ListLength,
                Path = type.MemberPath ?? string.Empty,
            };
            foreach (var part in parts.Skip(1))
            {
                if (part is not { Kind: LatticePredicateNodeKind.Compare, Children: [{ Kind: LatticePredicateNodeKind.Length } length, { Kind: LatticePredicateNodeKind.Constant } bound] }
                    || (length.MemberPath ?? string.Empty) != card.Path
                    || bound.Constant.Kind != LatticeConstantKind.Int64)
                {
                    return null;
                }

                var text = Text(bound.Constant);
                if (part.ComparisonOperator == LatticeComparisonOperator.GreaterThanOrEqual && card.Minimum.Length == 0 && card.Maximum.Length == 0)
                {
                    card.Minimum = text;
                }
                else if (part.ComparisonOperator == LatticeComparisonOperator.LessThanOrEqual && card.Maximum.Length == 0)
                {
                    card.Maximum = text;
                }
                else
                {
                    return null;
                }
            }

            return card;
        }

        // Number range: [TypeOf(integer)] && [subject >= a] && [subject <= b].
        var range = new SchemaRuleCard { Kind = SchemaCardKind.NumberRange };
        string? path = null;
        foreach (var part in parts)
        {
            if (ReadCore(part) is not { Kind: SchemaCardKind.NumberRange } bit)
            {
                return null;
            }

            if (path is not null && bit.Path != path)
            {
                return null;
            }

            path = bit.Path;
            if (bit.IntegerOnly && !range.IntegerOnly && range.Minimum.Length == 0 && range.Maximum.Length == 0)
            {
                range.IntegerOnly = true;
            }
            else if (bit.Minimum.Length > 0 && range.Minimum.Length == 0 && range.Maximum.Length == 0)
            {
                range.Minimum = bit.Minimum;
            }
            else if (bit.Maximum.Length > 0 && range.Maximum.Length == 0)
            {
                range.Maximum = bit.Maximum;
            }
            else
            {
                return null;
            }
        }

        range.Path = path ?? string.Empty;
        return range;
    }

    private static SchemaRuleCard? ReadTypeOrOneOf(LatticePredicateNode[] options)
    {
        if (options.Length == 2
            && options[0] is { Kind: LatticePredicateNodeKind.Compare, Children: [var a, { Kind: LatticePredicateNodeKind.Constant } ca] }
            && options[1] is { Kind: LatticePredicateNodeKind.Compare, Children: [var b, { Kind: LatticePredicateNodeKind.Constant } cb] }
            && TrySubject(a, out var pathA) && TrySubject(b, out var pathB) && pathA == pathB)
        {
            if (options[0].ComparisonOperator == LatticeComparisonOperator.GreaterThanOrEqual
                && options[1].ComparisonOperator == LatticeComparisonOperator.LessThan
                && ca.Constant == LatticeConstant.Integer(0) && cb.Constant == LatticeConstant.Integer(0))
            {
                return new SchemaRuleCard { Kind = SchemaCardKind.Type, Path = pathA, ValueType = SchemaValueType.Number };
            }

            if (options[0].ComparisonOperator == LatticeComparisonOperator.Equal
                && options[1].ComparisonOperator == LatticeComparisonOperator.Equal
                && ca.Constant == LatticeConstant.Bool(true) && cb.Constant == LatticeConstant.Bool(false))
            {
                return new SchemaRuleCard { Kind = SchemaCardKind.Type, Path = pathA, ValueType = SchemaValueType.Boolean };
            }
        }

        // One of: subject == c1 || subject == c2 ..., every constant text or every constant a number.
        var card = new SchemaRuleCard { Kind = SchemaCardKind.OneOf };
        string? path = null;
        bool? numbers = null;
        foreach (var option in options)
        {
            if (ReadComparison(option) is not { Kind: SchemaCardKind.OneOf } value
                || (path is not null && value.Path != path)
                || (numbers is { } known && known != value.ValuesAreNumbers))
            {
                return null;
            }

            path = value.Path;
            numbers = value.ValuesAreNumbers;
            card.Values.Add(value.Values[0]);
        }

        card.Path = path ?? string.Empty;
        card.ValuesAreNumbers = numbers ?? false;
        return card;
    }

    private static SchemaRuleCard TextMatch(string path, SchemaTextMatch match, string text) =>
        new() { Kind = SchemaCardKind.TextMatch, Path = path, Match = match, MatchText = text };

    private static bool TrySubject(LatticePredicateNode node, out string path)
    {
        switch (node.Kind)
        {
            case LatticePredicateNodeKind.Self:
                path = string.Empty;
                return true;

            case LatticePredicateNodeKind.Member when !string.IsNullOrEmpty(node.MemberPath):
                path = node.MemberPath;
                return true;

            default:
                path = string.Empty;
                return false;
        }
    }

    private static bool TryMissing(LatticePredicateNode node, out string path)
    {
        path = string.Empty;
        return node is { Kind: LatticePredicateNodeKind.Compare, ComparisonOperator: LatticeComparisonOperator.Equal, Children: [var subject, { Kind: LatticePredicateNodeKind.Constant } constant] }
            && constant.Constant.Kind == LatticeConstantKind.Null
            && TrySubject(subject, out path);
    }

    private static bool IsOr(LatticePredicateNode node, out LatticePredicateNode[] children)
    {
        children = node.Children ?? [];
        return node.Kind == LatticePredicateNodeKind.Boolean && node.BooleanOperator == LatticeBooleanOperator.Or;
    }

    private static bool IsNumber(LatticeConstant constant) => constant.Kind is LatticeConstantKind.Int64 or LatticeConstantKind.Double;

    private static string Text(LatticeConstant constant) => constant.Kind switch
    {
        LatticeConstantKind.Int64 => constant.Int64Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        LatticeConstantKind.Double => constant.DoubleValue.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
        _ => constant.StringValue ?? string.Empty,
    };
}
