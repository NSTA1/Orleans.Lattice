namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The regular expressions the format cards compile to, with their plain names
/// and examples. Every pattern is anchored and uses only constructs the
/// cluster's linear-time (<c>RegexOptions.NonBacktracking</c>) engine accepts:
/// no back-references and no look-arounds. A policy's pattern equal to one of
/// these reads back as that format card.
/// </summary>
internal static class SchemaFormatPatterns
{
    private const string Octet = "(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])";

    private const string DatePart = "[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])";

    private const string TimePart = "([01][0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9](\\.[0-9]+)?)?";

    private const string Hex = "[0-9A-Fa-f]{1,4}";

    private static readonly Format[] Formats =
    [
        new(SchemaTextFormat.Email, "Email address", "an email address",
            "^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}$", "dana@example.com", "dana@example"),
        new(SchemaTextFormat.Url, "URL", "an http or https URL",
            "^https?://[^\\s/?#]+([/?#][^\\s]*)?$", "https://example.com/orders?id=7", "example.com/orders"),
        new(SchemaTextFormat.Uuid, "UUID", "a UUID",
            "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$", "3f2504e0-4f89-11d3-9a0c-0305e82c3301", "3f2504e04f8911d3"),
        new(SchemaTextFormat.Date, "ISO date", "an ISO date",
            "^" + DatePart + "$", "2026-09-29", "29/09/2026"),
        new(SchemaTextFormat.DateTime, "ISO date and time", "an ISO date and time with an offset",
            "^" + DatePart + "T" + TimePart + "(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$", "2026-09-29T14:02:11Z", "2026-09-29 14:02"),
        new(SchemaTextFormat.Time, "ISO time", "an ISO time of day",
            "^" + TimePart + "$", "14:02:11", "2pm"),
        new(SchemaTextFormat.Ipv4, "IPv4 address", "an IPv4 address",
            "^(" + Octet + "\\.){3}" + Octet + "$", "192.0.2.10", "192.0.2.300"),
        new(SchemaTextFormat.Ipv6, "IPv6 address", "an IPv6 address",
            "^((" + Hex + ":){7}" + Hex
            + "|(" + Hex + ":){1,7}:"
            + "|(" + Hex + ":){1,6}:" + Hex
            + "|(" + Hex + ":){1,5}(:" + Hex + "){1,2}"
            + "|(" + Hex + ":){1,4}(:" + Hex + "){1,3}"
            + "|(" + Hex + ":){1,3}(:" + Hex + "){1,4}"
            + "|(" + Hex + ":){1,2}(:" + Hex + "){1,5}"
            + "|" + Hex + ":(:" + Hex + "){1,6}"
            + "|:((:" + Hex + "){1,7}|:))$", "2001:db8::ff00:42:8329", "2001:db8:::1"),
        new(SchemaTextFormat.Slug, "Slug", "a slug",
            "^[a-z0-9]+(-[a-z0-9]+)*$", "spring-sale-2026", "Spring Sale"),
        new(SchemaTextFormat.CountryCode, "Country code", "a two-letter country code",
            "^[A-Z]{2}$", "GB", "gbr"),
        new(SchemaTextFormat.CurrencyCode, "Currency code", "a three-letter currency code",
            "^[A-Z]{3}$", "EUR", "euro"),
        new(SchemaTextFormat.HexColour, "Hex colour", "a hex colour",
            "^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$", "#1a2b3c", "1a2b3c"),
        new(SchemaTextFormat.SemanticVersion, "Semantic version", "a semantic version",
            "^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\\+[0-9A-Za-z.-]+)?$", "2.1.0", "v2.1"),
        new(SchemaTextFormat.Phone, "Phone number", "an E.164 phone number",
            "^\\+[1-9][0-9]{1,14}$", "+441632960961", "01632 960961"),
    ];

    /// <summary>Every format, in the order the builder lists them.</summary>
    public static IReadOnlyList<SchemaTextFormat> All { get; } = [.. Formats.Select(format => format.Kind)];

    /// <summary>The anchored pattern <paramref name="format"/> compiles to.</summary>
    /// <param name="format">The format.</param>
    /// <returns>The regular expression.</returns>
    public static string PatternOf(SchemaTextFormat format) => Find(format).Pattern;

    /// <summary>The format's name, such as "Email address".</summary>
    /// <param name="format">The format.</param>
    /// <returns>The name.</returns>
    public static string NameOf(SchemaTextFormat format) => Find(format).Name;

    /// <summary>The format as the complement of "must be", such as "an email address".</summary>
    /// <param name="format">The format.</param>
    /// <returns>The phrase.</returns>
    public static string PhraseOf(SchemaTextFormat format) => Find(format).Phrase;

    /// <summary>A value the format accepts.</summary>
    /// <param name="format">The format.</param>
    /// <returns>The example.</returns>
    public static string PassingExampleOf(SchemaTextFormat format) => Find(format).Passing;

    /// <summary>A value the format refuses.</summary>
    /// <param name="format">The format.</param>
    /// <returns>The example.</returns>
    public static string FailingExampleOf(SchemaTextFormat format) => Find(format).Failing;

    /// <summary>The format whose pattern is exactly <paramref name="pattern"/>, if any.</summary>
    /// <param name="pattern">A policy's pattern.</param>
    /// <param name="format">The format, when one matches.</param>
    /// <returns><see langword="true"/> when <paramref name="pattern"/> is a format's pattern.</returns>
    public static bool TryRecognise(string? pattern, out SchemaTextFormat format)
    {
        foreach (var candidate in Formats)
        {
            if (string.Equals(candidate.Pattern, pattern, StringComparison.Ordinal))
            {
                format = candidate.Kind;
                return true;
            }
        }

        format = default;
        return false;
    }

    /// <summary>The formats every one of <paramref name="texts"/> matches, best first.</summary>
    /// <param name="texts">Observed text values; none means no suggestion.</param>
    /// <returns>The matching formats.</returns>
    public static IReadOnlyList<SchemaTextFormat> Matching(IReadOnlyCollection<string> texts)
    {
        ArgumentNullException.ThrowIfNull(texts);
        if (texts.Count == 0)
        {
            return [];
        }

        var matching = new List<SchemaTextFormat>();
        foreach (var candidate in Formats)
        {
            if (texts.All(candidate.Regex.IsMatch))
            {
                matching.Add(candidate.Kind);
            }
        }

        return matching;
    }

    /// <summary>Whether <paramref name="text"/> is in <paramref name="format"/>, judged as the cluster judges it.</summary>
    /// <param name="format">The format.</param>
    /// <param name="text">The text.</param>
    /// <returns><see langword="true"/> when it matches.</returns>
    public static bool IsMatch(SchemaTextFormat format, string text) => Find(format).Regex.IsMatch(text);

    private static Format Find(SchemaTextFormat format) =>
        Array.Find(Formats, candidate => candidate.Kind == format) ?? throw new ArgumentOutOfRangeException(nameof(format));

    private sealed class Format(SchemaTextFormat kind, string name, string phrase, string pattern, string passing, string failing)
    {
        private System.Text.RegularExpressions.Regex? _regex;

        public SchemaTextFormat Kind => kind;

        public string Name => name;

        public string Phrase => phrase;

        public string Pattern => pattern;

        public string Passing => passing;

        public string Failing => failing;

        public System.Text.RegularExpressions.Regex Regex => _regex ??= SchemaPatterns.Compile(pattern);
    }
}
