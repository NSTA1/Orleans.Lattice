using System.Globalization;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The live example under each card of the gallery, taken from what the sample
/// shows for the chosen member ("seen 9.99 to 1,210", "all look like an email
/// address") and falling back to a plain illustration when nothing was seen.
/// Also seeds a new card from the member, so a chosen card starts from the data.
/// </summary>
internal static class SchemaCardExamples
{
    /// <summary>The kinds the gallery offers, in order.</summary>
    public static IReadOnlyList<SchemaCardKind> Gallery { get; } =
    [
        SchemaCardKind.Required,
        SchemaCardKind.Type,
        SchemaCardKind.OneOf,
        SchemaCardKind.NumberRange,
        SchemaCardKind.TextLength,
        SchemaCardKind.Format,
        SchemaCardKind.TextMatch,
        SchemaCardKind.ListLength,
        SchemaCardKind.EveryItem,
        SchemaCardKind.Pattern,
        SchemaCardKind.Encoding,
        SchemaCardKind.MaxSize,
    ];

    /// <summary>A card kind's title in the gallery.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The title.</returns>
    public static string Title(SchemaCardKind kind) => kind switch
    {
        SchemaCardKind.Required => "Required",
        SchemaCardKind.Type => "Type",
        SchemaCardKind.OneOf => "One of a set",
        SchemaCardKind.NumberRange => "Number range",
        SchemaCardKind.TextLength => "Text length",
        SchemaCardKind.Format => "Common format",
        SchemaCardKind.TextMatch => "Starts, ends or contains",
        SchemaCardKind.ListLength => "List length",
        SchemaCardKind.EveryItem => "Every item",
        SchemaCardKind.Pattern => "Custom pattern",
        SchemaCardKind.Encoding => "Well-formed value",
        SchemaCardKind.MaxSize => "Largest size",
        SchemaCardKind.AnyOf => "Any of",
        _ => "Custom rule",
    };

    /// <summary>What a card kind means, in one line.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The line.</returns>
    public static string Meaning(SchemaCardKind kind) => kind switch
    {
        SchemaCardKind.Required => "It must be there and not null.",
        SchemaCardKind.Type => "Text, a number, true or false, an object or a list.",
        SchemaCardKind.OneOf => "Only these values are allowed.",
        SchemaCardKind.NumberRange => "A smallest and largest number, and whole numbers only if you like.",
        SchemaCardKind.TextLength => "The fewest and most characters.",
        SchemaCardKind.Format => "Email, URL, UUID, ISO date and time, IP address, slug and more.",
        SchemaCardKind.TextMatch => "A prefix, a suffix or some text anywhere.",
        SchemaCardKind.ListLength => "The fewest and most items.",
        SchemaCardKind.EveryItem => "Each item of a list must satisfy another card.",
        SchemaCardKind.Pattern => "A regular expression, with a live tester.",
        SchemaCardKind.Encoding => "The whole value is well-formed UTF-8, or one JSON document.",
        SchemaCardKind.MaxSize => "The whole value is at most a number of bytes.",
        _ => string.Empty,
    };

    /// <summary>Whether a card of <paramref name="kind"/> can be chosen for <paramref name="node"/>, and why not when it cannot.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="wholeValue">Whether the subject is the whole value (or the item) rather than a member.</param>
    /// <param name="predicateOnly">Whether only predicate cards may be chosen (inside "every item" or "any of").</param>
    /// <param name="reason">Why it cannot, as a sentence.</param>
    /// <returns><see langword="true"/> when it can.</returns>
    public static bool IsAvailable(SchemaCardKind kind, bool wholeValue, bool predicateOnly, out string? reason)
    {
        reason = null;
        if (predicateOnly && !SchemaCardCompiler.IsPredicate(kind))
        {
            reason = SchemaCardCompiler.IsWholeValueOnly(kind)
                ? "Only for the whole value."
                : "Checks a member on its own; not available here.";
            return false;
        }

        if (!wholeValue && SchemaCardCompiler.IsWholeValueOnly(kind))
        {
            reason = "Only for the whole value.";
            return false;
        }

        return true;
    }

    /// <summary>The live example for a card kind on a member.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="node">What the sample showed for the member, or <see langword="null"/> when nothing is known.</param>
    /// <returns>The example, one line.</returns>
    public static string Example(SchemaCardKind kind, SchemaShapeNode? node)
    {
        var seen = node is { Unseen: false } ? node : null;
        switch (kind)
        {
            case SchemaCardKind.Required when seen is not null:
                var present = seen.Seen - seen.Nulls;
                return $"Set in {present:N0} of {Documents(seen):N0} sampled values.";

            case SchemaCardKind.Type when seen?.Dominant is { } dominant:
                return "Seen as " + string.Join(", ", seen.Types.OrderByDescending(pair => pair.Value).Select(pair => $"{SchemaCardText.TypePhrase(pair.Key)} ({pair.Value:N0})")) + ".";

            case SchemaCardKind.OneOf when seen is not null && seen.Distinct.Count > 0:
                return (seen.ManyValues || seen.Distinct.Count > 4 ? "Many values, such as " : "Seen: ") + string.Join(", ", seen.Distinct.Take(4).Select(Clip)) + ".";

            case SchemaCardKind.NumberRange when seen?.Numbers is { } numbers:
                return $"Seen {SchemaShape.Show(numbers.Min)} to {SchemaShape.Show(numbers.Max)}{(seen.AllWhole ? ", all whole" : string.Empty)}.";

            case SchemaCardKind.TextLength when seen?.TextLengths is { } lengths:
                return $"Seen {lengths.Min:N0} to {lengths.Max:N0} characters.";

            case SchemaCardKind.Format when seen is not null && SchemaFormatPatterns.Matching(seen.Texts) is [var first, ..]:
                return $"Every value seen is {SchemaFormatPatterns.PhraseOf(first)}.";

            case SchemaCardKind.TextMatch when seen is not null && CommonPrefix(seen.Texts) is { Length: > 0 } prefix:
                return $"Every value seen starts with \"{prefix}\".";

            case SchemaCardKind.ListLength when seen?.ItemCounts is { } counts:
                return $"Seen {counts.Min:N0} to {counts.Max:N0} items.";

            case SchemaCardKind.EveryItem when seen?.Items is { Dominant: { } itemType }:
                return $"Items seen are mostly {SchemaCardText.TypePhrase(itemType)}.";

            case SchemaCardKind.Pattern when seen?.Texts is [var sample, ..]:
                return $"Test it against \"{Clip(sample)}\" and the rest of the sample.";
        }

        var illustration = kind switch
        {
            SchemaCardKind.Required => "id must be present.",
            SchemaCardKind.Type => "total must be a number.",
            SchemaCardKind.OneOf => "status must be one of \"open\", \"shipped\" or \"cancelled\".",
            SchemaCardKind.NumberRange => "total must be a number between 0 and 10,000.",
            SchemaCardKind.TextLength => "name must be text of 1 to 80 characters.",
            SchemaCardKind.Format => "email must be an email address.",
            SchemaCardKind.TextMatch => "sku must start with \"SKU-\".",
            SchemaCardKind.ListLength => "lines must be a list of 1 to 50 items.",
            SchemaCardKind.EveryItem => "Every item of lines: qty must be a whole number of at least 1.",
            SchemaCardKind.Pattern => "code must match ^[A-Z]{3}-[0-9]{4}$.",
            SchemaCardKind.Encoding => "The value must be one JSON document.",
            SchemaCardKind.MaxSize => "The value must be at most 65,536 bytes.",
            _ => string.Empty,
        };

        return illustration.Length == 0 ? illustration : "For example: " + illustration;
    }

    /// <summary>A new card of <paramref name="kind"/>, seeded from what the sample showed for the member.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="node">What the sample showed, or <see langword="null"/>.</param>
    /// <returns>The card, its path not yet set.</returns>
    public static SchemaRuleCard Seed(SchemaCardKind kind, SchemaShapeNode? node)
    {
        var card = new SchemaRuleCard { Kind = kind };
        var seen = node is { Unseen: false } ? node : null;
        switch (kind)
        {
            case SchemaCardKind.Required:
                card.Structural = seen?.Dominant is SchemaValueType.Object or SchemaValueType.List;
                break;

            case SchemaCardKind.Type:
                card.ValueType = seen?.Dominant ?? SchemaValueType.Text;
                break;

            case SchemaCardKind.OneOf:
                card.ValuesAreNumbers = seen?.Dominant == SchemaValueType.Number;
                if (seen is { ManyValues: false })
                {
                    card.Values = [.. seen.Distinct.Where(value => value is not ("true" or "false"))];
                }

                break;

            case SchemaCardKind.NumberRange when seen?.Numbers is { } numbers:
                card.Minimum = Raw(numbers.Min);
                card.Maximum = Raw(numbers.Max);
                card.IntegerOnly = seen.AllWhole;
                break;

            case SchemaCardKind.TextLength when seen?.TextLengths is { } lengths:
                card.Minimum = lengths.Min.ToString(CultureInfo.InvariantCulture);
                card.Maximum = lengths.Max.ToString(CultureInfo.InvariantCulture);
                break;

            case SchemaCardKind.ListLength when seen?.ItemCounts is { } counts:
                card.Minimum = counts.Min.ToString(CultureInfo.InvariantCulture);
                card.Maximum = counts.Max.ToString(CultureInfo.InvariantCulture);
                break;

            case SchemaCardKind.Format:
                card.Format = seen is not null && SchemaFormatPatterns.Matching(seen.Texts) is [var format, ..] ? format : SchemaTextFormat.Email;
                break;

            case SchemaCardKind.TextMatch:
                card.MatchText = seen is not null ? CommonPrefix(seen.Texts) : string.Empty;
                break;

            case SchemaCardKind.EveryItem:
                var itemKind = seen?.Items?.Dominant is SchemaValueType.Number ? SchemaCardKind.NumberRange : SchemaCardKind.Type;
                card.Item = Seed(itemKind, seen?.Items);
                break;

            case SchemaCardKind.Encoding:
                card.Encoding = Orleans.Lattice.Schema.LatticeSchemaEncodingKind.Json;
                break;
        }

        return card;
    }

    private static int Documents(SchemaShapeNode node)
    {
        var root = node;
        while (root.Parent is { } parent)
        {
            root = parent;
        }

        return Math.Max(root.Seen, node.Seen);
    }

    /// <summary>The longest prefix every text shares, when there are at least two texts.</summary>
    /// <param name="texts">The texts.</param>
    /// <returns>The prefix; empty when there is none.</returns>
    internal static string CommonPrefix(IReadOnlyList<string> texts)
    {
        if (texts.Count < 2)
        {
            return string.Empty;
        }

        var prefix = texts[0];
        foreach (var text in texts.Skip(1))
        {
            var length = 0;
            while (length < prefix.Length && length < text.Length && prefix[length] == text[length])
            {
                length++;
            }

            prefix = prefix[..length];
        }

        return prefix.Length > 40 ? prefix[..40] : prefix;
    }

    private static string Raw(double value) => value.ToString("R", CultureInfo.InvariantCulture);

    private static string Clip(string text) => text.Length > 32 ? text[..32] + "..." : text;
}
