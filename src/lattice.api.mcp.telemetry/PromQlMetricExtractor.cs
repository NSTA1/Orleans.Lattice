namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// A conservative extractor of the metric names a PromQL expression references in
/// metric-name position, used to gate a query in the deny-all metric-access
/// posture. It scans the expression for identifiers that sit where a metric
/// selector may appear and reports the distinct set.
/// </summary>
/// <remarks>
/// <para>
/// The extraction is deliberately conservative rather than a full PromQL parser.
/// It recognises an identifier (<c>[a-zA-Z_:][a-zA-Z0-9_:]*</c>) as a metric name
/// only when it is <b>not</b> immediately followed by <c>(</c> (a function or
/// aggregation call), <b>not</b> a PromQL keyword or aggregation operator,
/// <b>not</b> inside a <c>{...}</c> label matcher, and <b>not</b> inside a quoted
/// string or a numeric / duration literal such as <c>5m</c>.
/// </para>
/// <para>
/// Two known conservatism gaps follow from this: a selector that names its metric
/// only through the reserved <c>__name__</c> label (for example
/// <c>{__name__="up"}</c>) yields no extracted name, and an aggregation whose
/// operator token happens to collide with a real metric name is skipped. Callers
/// use the extractor solely to reject an obviously non-whitelisted metric in
/// deny-all mode; it is not a security boundary against a crafted expression.
/// </para>
/// </remarks>
internal static class PromQlMetricExtractor
{
    private static readonly HashSet<string> ReservedWords = new(StringComparer.Ordinal)
    {
        // Set / binary / vector-matching keywords.
        "and", "or", "unless", "by", "without", "on", "ignoring",
        "group_left", "group_right", "offset", "bool", "inf", "nan",
        "start", "end", "atan2",
        // Aggregation operators (all are call-like but may be written with a
        // modifier before the '(' so they are excluded by name as well).
        "sum", "min", "max", "avg", "group", "stddev", "stdvar", "count",
        "count_values", "bottomk", "topk", "quantile", "limitk", "limit_ratio",
    };

    private static readonly HashSet<string> GroupingKeywords = new(StringComparer.Ordinal)
    {
        "by", "without", "on", "ignoring", "group_left", "group_right",
    };

    /// <summary>
    /// Extracts the distinct metric names <paramref name="query"/> references in
    /// metric-name position, in first-seen order.
    /// </summary>
    /// <param name="query">The PromQL expression to scan.</param>
    /// <returns>
    /// The distinct metric names found, or an empty list when the expression names
    /// none in an extractable position.
    /// </returns>
    public static IReadOnlyList<string> Extract(string query)
    {
        ArgumentNullException.ThrowIfNull(query);

        List<string>? names = null;
        HashSet<string>? seen = null;
        var braceDepth = 0;
        var i = 0;
        var length = query.Length;

        while (i < length)
        {
            var c = query[i];

            if (c == '"' || c == '\'' || c == '`')
            {
                i = SkipString(query, i);
                continue;
            }

            if (c == '{')
            {
                braceDepth++;
                i++;
                continue;
            }

            if (c == '}')
            {
                if (braceDepth > 0)
                {
                    braceDepth--;
                }

                i++;
                continue;
            }

            if (IsIdentifierStart(c))
            {
                var start = i;
                while (i < length && IsIdentifierPart(query[i]))
                {
                    i++;
                }

                if (braceDepth != 0)
                {
                    continue;
                }

                var identifier = query.Substring(start, i - start);
                if (GroupingKeywords.Contains(identifier))
                {
                    // A grouping modifier (by/without/on/ignoring/group_left/
                    // group_right) may be followed by a parenthesised label list
                    // whose identifiers are label names, not metrics. Skip it. This
                    // is checked before the function-call test because the label
                    // list opens with '(' just as a call does.
                    var listStart = SkipWhitespaceIndex(query, i);
                    if (listStart < length && query[listStart] == '(')
                    {
                        i = SkipBalancedParens(query, listStart);
                    }

                    continue;
                }

                if (NextNonWhitespace(query, i) == '(')
                {
                    continue;
                }

                if (ReservedWords.Contains(identifier))
                {
                    continue;
                }

                seen ??= new HashSet<string>(StringComparer.Ordinal);
                if (seen.Add(identifier))
                {
                    (names ??= []).Add(identifier);
                }

                continue;
            }

            if (char.IsAsciiDigit(c))
            {
                // Consume a numeric or duration literal (for example 5m, 1.5h) so
                // its trailing unit letters are not mistaken for a metric name.
                while (i < length && (IsIdentifierPart(query[i]) || query[i] == '.'))
                {
                    i++;
                }

                continue;
            }

            i++;
        }

        return names is null ? [] : names;
    }

    private static int SkipString(string text, int openIndex)
    {
        var quote = text[openIndex];
        var i = openIndex + 1;
        while (i < text.Length)
        {
            var c = text[i];
            if (c == '\\' && quote != '`')
            {
                i += 2;
                continue;
            }

            if (c == quote)
            {
                return i + 1;
            }

            i++;
        }

        return i;
    }

    private static char NextNonWhitespace(string text, int index)
    {
        for (var i = index; i < text.Length; i++)
        {
            if (!char.IsWhiteSpace(text[i]))
            {
                return text[i];
            }
        }

        return '\0';
    }

    private static int SkipWhitespaceIndex(string text, int index)
    {
        var i = index;
        while (i < text.Length && char.IsWhiteSpace(text[i]))
        {
            i++;
        }

        return i;
    }

    private static int SkipBalancedParens(string text, int openIndex)
    {
        var depth = 0;
        for (var i = openIndex; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
                if (depth == 0)
                {
                    return i + 1;
                }
            }
        }

        return text.Length;
    }

    private static bool IsIdentifierStart(char c)
        => char.IsAsciiLetter(c) || c == '_' || c == ':';

    private static bool IsIdentifierPart(char c)
        => char.IsAsciiLetterOrDigit(c) || c == '_' || c == ':';
}
