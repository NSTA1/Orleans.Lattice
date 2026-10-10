using System.Globalization;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>Writes cards as sentences.</summary>
internal static class SchemaCardText
{
    /// <summary>The sentence for <paramref name="card"/>.</summary>
    /// <param name="card">The card.</param>
    /// <param name="insideItem">Whether the card is the item card of an every-item card, so an empty path means "each item".</param>
    /// <returns>The sentence.</returns>
    public static SchemaCardSentence Sentence(SchemaRuleCard card, bool insideItem = false)
    {
        ArgumentNullException.ThrowIfNull(card);
        var subject = card.Path.Length == 0 ? null : card.Path;
        var lead = subject is null ? (insideItem ? "Each item" : "The value") : string.Empty;
        var optional = card.Optional && SchemaCardCompiler.CanBeOptional(card.Kind) ? ", when present" : string.Empty;

        switch (card.Kind)
        {
            case SchemaCardKind.EveryItem:
                return new SchemaCardSentence(
                    subject,
                    subject is null ? (insideItem ? "Each item" : "The value") : string.Empty,
                    "must be a list in which" + optional + ":",
                    card.Item is { } item ? [Sentence(item, insideItem: true)] : []);

            case SchemaCardKind.AnyOf:
                return new SchemaCardSentence(null, "At least one of these must hold:", string.Empty,
                    [.. card.Alternatives.Select(alternative => Sentence(alternative, insideItem))]);

            case SchemaCardKind.Custom:
                return new SchemaCardSentence(null, "A custom rule:", card.Original is { } original ? Custom(original) : "nothing", []);

            case SchemaCardKind.Encoding:
            case SchemaCardKind.MaxSize:
                return new SchemaCardSentence(null, "The value", Predicate(card), []);

            default:
                return new SchemaCardSentence(subject, lead, Predicate(card) + optional, []);
        }
    }

    /// <summary>The sentence for <paramref name="card"/> as plain text.</summary>
    /// <param name="card">The card.</param>
    /// <returns>The text.</returns>
    public static string Plain(SchemaRuleCard card) => Sentence(card).Plain();

    /// <summary>What a card requires, after its subject: "must be ...".</summary>
    /// <param name="card">The card.</param>
    /// <returns>The words.</returns>
    public static string Predicate(SchemaRuleCard card)
    {
        ArgumentNullException.ThrowIfNull(card);
        return card.Kind switch
        {
            SchemaCardKind.Required => card.Structural ? "must be present, as any value" : "must be present as text, a number or true or false",
            SchemaCardKind.Type => "must be " + TypePhrase(card.ValueType),
            SchemaCardKind.OneOf => OneOf(card),
            SchemaCardKind.NumberRange => NumberRange(card),
            SchemaCardKind.TextLength => "must be text of " + Range(card, "character"),
            SchemaCardKind.ListLength => "must be a list of " + Range(card, "item"),
            SchemaCardKind.Format => "must be " + SchemaFormatPatterns.PhraseOf(card.Format),
            SchemaCardKind.TextMatch => card.Match switch
            {
                SchemaTextMatch.StartsWith => "must start with " + Quote(card.MatchText),
                SchemaTextMatch.EndsWith => "must end with " + Quote(card.MatchText),
                _ => "must contain " + Quote(card.MatchText),
            },
            SchemaCardKind.Pattern => "must match the pattern " + card.Pattern,
            SchemaCardKind.Encoding => card.Encoding == LatticeSchemaEncodingKind.Utf8 ? "must be well-formed UTF-8" : "must be one JSON document",
            SchemaCardKind.MaxSize => "must be at most " + Number(card.MaxBytes) + " " + Plural(card.MaxBytes.Trim(), "byte"),
            _ => "must satisfy a custom rule",
        };
    }

    /// <summary>A type in words, as the complement of "must be".</summary>
    /// <param name="type">The type.</param>
    /// <returns>The words.</returns>
    public static string TypePhrase(SchemaValueType type) => type switch
    {
        SchemaValueType.Text => "text",
        SchemaValueType.Number => "a number",
        SchemaValueType.Boolean => "true or false",
        SchemaValueType.Object => "an object",
        _ => "a list",
    };

    /// <summary>A number as typed, grouped for reading, such as "10,000"; the text unchanged when it is not a number.</summary>
    /// <param name="text">The number as typed.</param>
    /// <returns>The text.</returns>
    public static string Number(string text)
    {
        var trimmed = text.Trim();
        return SchemaCardCompiler.TryNumber(trimmed, out var constant)
            ? constant.Kind == LatticeConstantKind.Int64
                ? constant.Int64Value.ToString("N0", CultureInfo.InvariantCulture)
                : constant.DoubleValue.ToString("#,0.###############", CultureInfo.InvariantCulture)
            : trimmed;
    }

    /// <summary>A predicate as an expression, for a rule no card can show.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The expression.</returns>
    public static string Custom(LatticeSchemaRule rule) => rule.Kind switch
    {
        LatticeSchemaRuleKind.Structured when rule.Predicate is { } predicate => SchemaPredicateText.Expression(predicate),
        _ => SchemaFormat.RuleDetail(rule with { Description = null }),
    };

    private static string OneOf(SchemaRuleCard card)
    {
        var values = card.Values.Where(value => value.Length > 0).Select(value => card.ValuesAreNumbers ? Number(value) : Quote(value)).ToArray();
        return values.Length switch
        {
            0 => "must be one of a set of values",
            1 => "must be " + values[0],
            _ => "must be one of " + string.Join(", ", values[..^1]) + " or " + values[^1],
        };
    }

    private static string NumberRange(SchemaRuleCard card)
    {
        var noun = card.IntegerOnly ? "a whole number" : "a number";
        var minimum = card.Minimum.Trim();
        var maximum = card.Maximum.Trim();
        return (minimum.Length, maximum.Length) switch
        {
            ( > 0, > 0) => $"must be {noun} between {Number(minimum)} and {Number(maximum)}",
            ( > 0, 0) => $"must be {noun} of at least {Number(minimum)}",
            (0, > 0) => $"must be {noun} of at most {Number(maximum)}",
            _ => "must be " + noun,
        };
    }

    private static string Range(SchemaRuleCard card, string unit)
    {
        var minimum = card.Minimum.Trim();
        var maximum = card.Maximum.Trim();
        return (minimum.Length, maximum.Length) switch
        {
            ( > 0, > 0) => $"{Number(minimum)} to {Number(maximum)} {unit}s",
            ( > 0, 0) => $"at least {Number(minimum)} {Plural(minimum, unit)}",
            (0, > 0) => $"at most {Number(maximum)} {Plural(maximum, unit)}",
            _ => $"any number of {unit}s",
        };
    }

    // The count as written decides nothing: "01" and "1.0" are both one.
    private static string Plural(string count, string unit) => Number(count) == "1" ? unit : unit + "s";

    private static string Quote(string text) => "\"" + text + "\"";
}
