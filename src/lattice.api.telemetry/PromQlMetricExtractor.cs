namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// A conservative extractor of the metric names a PromQL expression references,
/// used to gate a query in the deny-all metric-access posture. It scans the
/// expression for identifiers that sit where a metric selector may appear, and for
/// the reserved <c>__name__</c> label matcher inside a <c>{...}</c> label set, and
/// reports the distinct set of names together with whether an unresolvable
/// <c>__name__</c> matcher was seen.
/// </summary>
/// <remarks>
/// <para>
/// The extraction is deliberately conservative rather than a full PromQL parser.
/// It recognises an identifier (<c>[a-zA-Z_:][a-zA-Z0-9_:]*</c>) as a metric name
/// only when it is <b>not</b> immediately followed by <c>(</c> (a function or
/// aggregation call), <b>not</b> a PromQL keyword or aggregation operator,
/// <b>not</b> inside a quoted string or a numeric / duration literal such as
/// <c>5m</c>, and <b>not</b> inside a <c>{...}</c> label matcher unless it is the
/// reserved <c>__name__</c> label.
/// </para>
/// <para>
/// The reserved <c>__name__</c> label designates a metric by name from inside a
/// label matcher (for example <c>{__name__="up"}</c>). An exact
/// <c>__name__="up"</c> matcher contributes its value as a referenced name so the
/// deny-all gate can admit or reject it like any name-position identifier. A regex
/// <c>__name__=~"..."</c> matcher or a negative <c>__name__!="..."</c> /
/// <c>__name__!~"..."</c> matcher cannot be reduced to a fixed set of names, so it
/// sets <see cref="PromQlMetricReferences.HasUnresolvableNameMatcher"/> and the
/// deny-all gate fails closed. This closes the allow-list bypass where a caller
/// named a denied series only through <c>__name__</c>.
/// </para>
/// </remarks>
public static class PromQlMetricExtractor
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
    /// metric-name position or through an exact <c>__name__</c> label matcher, in
    /// first-seen order, and reports whether it carries an unresolvable
    /// <c>__name__</c> matcher.
    /// </summary>
    /// <param name="query">The PromQL expression to scan.</param>
    /// <returns>
    /// The referenced metric names and the unresolvable-matcher flag. The name list
    /// is empty when the expression names none in an extractable position.
    /// </returns>
    public static PromQlMetricReferences ExtractReferences(string query)
    {
        ArgumentNullException.ThrowIfNull(query);

        List<string>? names = null;
        HashSet<string>? seen = null;
        var hasUnresolvableNameMatcher = false;
        var braceDepth = 0;
        var i = 0;
        var length = query.Length;

        // Track whether a top-level '{...}' label selector is constrained. A
        // selector is safe only when it is either anchored to a metric name in
        // name position (for example up{job="api"}) or carries an exact
        // __name__="..." matcher; a bare, unanchored label selector such as the
        // right-hand side of `up or {job="api"}` selects series across every
        // metric name and must fail the deny-all gate closed even though the
        // expression also names an admitted metric.
        var metricNamePrecedes = false;
        var hasUnconstrainedSelector = false;
        var selectorAnchored = false;
        var selectorSawExactName = false;

        while (i < length)
        {
            var c = query[i];

            if (c == '"' || c == '\'' || c == '`')
            {
                i = SkipString(query, i);
                metricNamePrecedes = false;
                continue;
            }

            if (c == '{')
            {
                if (braceDepth == 0)
                {
                    selectorAnchored = metricNamePrecedes;
                    selectorSawExactName = false;
                }

                braceDepth++;
                metricNamePrecedes = false;
                i++;
                continue;
            }

            if (c == '}')
            {
                if (braceDepth > 0)
                {
                    braceDepth--;
                    if (braceDepth == 0 && !selectorAnchored && !selectorSawExactName)
                    {
                        // A top-level label selector that is neither anchored to a
                        // metric name nor pinned by an exact __name__ matcher is
                        // unconstrained; the deny-all gate must reject it.
                        hasUnconstrainedSelector = true;
                    }
                }

                metricNamePrecedes = false;
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
                    // Inside a label matcher only the reserved __name__ label names a
                    // metric; every other identifier is a label name, not a metric.
                    // Compare the span so a plain label name allocates no substring.
                    if (query.AsSpan(start, i - start).SequenceEqual("__name__"))
                    {
                        var before = names?.Count ?? 0;
                        i = ReadNameMatcher(query, i, ref names, ref seen, ref hasUnresolvableNameMatcher);
                        if ((names?.Count ?? 0) > before)
                        {
                            // An exact __name__="..." matcher pins the selector to a
                            // named metric, so it is constrained.
                            selectorSawExactName = true;
                        }
                    }

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

                    metricNamePrecedes = false;
                    continue;
                }

                if (NextNonWhitespace(query, i) == '(')
                {
                    metricNamePrecedes = false;
                    continue;
                }

                if (ReservedWords.Contains(identifier))
                {
                    metricNamePrecedes = false;
                    continue;
                }

                AddName(identifier, ref names, ref seen);
                metricNamePrecedes = true;
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

                metricNamePrecedes = false;
                continue;
            }

            if (char.IsWhiteSpace(c))
            {
                // Whitespace does not break the adjacency between a metric name and
                // a following label selector, so leave metricNamePrecedes intact.
                i++;
                continue;
            }

            metricNamePrecedes = false;
            i++;
        }

        // An unterminated top-level '{' (a malformed selector) is treated as
        // unconstrained unless it was anchored or pinned by an exact __name__,
        // so the deny-all gate fails closed on it.
        if (braceDepth > 0 && !selectorAnchored && !selectorSawExactName)
        {
            hasUnconstrainedSelector = true;
        }

        return new PromQlMetricReferences
        {
            Names = names is null ? [] : names,
            HasUnresolvableNameMatcher = hasUnresolvableNameMatcher,
            HasUnconstrainedSelector = hasUnconstrainedSelector,
        };
    }

    /// <summary>
    /// Reads a <c>__name__</c> label matcher whose label token ends at
    /// <paramref name="afterLabel"/>. An exact <c>=</c> matcher contributes its
    /// quoted value as a referenced name; a regex <c>=~</c> matcher or a negative
    /// <c>!=</c> / <c>!~</c> matcher, or any malformed form, sets the unresolvable
    /// flag so the deny-all gate fails closed.
    /// </summary>
    /// <returns>The index just past the matcher's value (or operator when no value follows).</returns>
    private static int ReadNameMatcher(
        string query,
        int afterLabel,
        ref List<string>? names,
        ref HashSet<string>? seen,
        ref bool hasUnresolvableNameMatcher)
    {
        var length = query.Length;
        var i = SkipWhitespaceIndex(query, afterLabel);
        if (i >= length)
        {
            hasUnresolvableNameMatcher = true;
            return i;
        }

        var op = query[i];
        if (op == '=')
        {
            i++;
            if (i < length && query[i] == '~')
            {
                // =~ regex matcher: cannot be reduced to a fixed set of names.
                hasUnresolvableNameMatcher = true;
                return i + 1;
            }

            i = SkipWhitespaceIndex(query, i);
            if (i < length && IsQuote(query[i]))
            {
                var end = ReadStringValue(query, i, out var value);
                if (value is null)
                {
                    // Unterminated string literal: fail closed.
                    hasUnresolvableNameMatcher = true;
                }
                else
                {
                    AddName(value, ref names, ref seen);
                }

                return end;
            }

            // '=' not followed by a quoted value: malformed, fail closed.
            hasUnresolvableNameMatcher = true;
            return i;
        }

        if (op == '!' && i + 1 < length && (query[i + 1] == '=' || query[i + 1] == '~'))
        {
            // != or !~ negative matcher: does not constrain to allow-listed names.
            hasUnresolvableNameMatcher = true;
            return i + 2;
        }

        // No recognised matcher operator after __name__: fail closed.
        hasUnresolvableNameMatcher = true;
        return i;
    }

    private static void AddName(string name, ref List<string>? names, ref HashSet<string>? seen)
    {
        seen ??= new HashSet<string>(StringComparer.Ordinal);
        if (seen.Add(name))
        {
            (names ??= []).Add(name);
        }
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

    private static int ReadStringValue(string text, int quoteIndex, out string? value)
    {
        var quote = text[quoteIndex];
        var start = quoteIndex + 1;
        var i = start;
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
                value = text.Substring(start, i - start);
                return i + 1;
            }

            i++;
        }

        value = null;
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

    private static bool IsQuote(char c)
        => c == '"' || c == '\'' || c == '`';
}
