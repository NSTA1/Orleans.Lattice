using System.Text;

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
/// aggregation call), <b>not</b> inside a quoted string or a numeric / duration
/// literal such as <c>5m</c>, and <b>not</b> inside a <c>{...}</c> label matcher
/// unless it is the reserved <c>__name__</c> label.
/// </para>
/// <para>
/// A PromQL keyword is excluded only where Prometheus itself reads it as a keyword.
/// Prometheus's grammar also accepts the aggregation operators, the set operators,
/// and the <c>by</c>, <c>without</c>, <c>offset</c>, <c>start</c>, and <c>end</c>
/// keywords as a bare metric name, so <c>up or min</c> selects the metric named
/// <c>min</c>. An aggregation operator is therefore a keyword only when a call or a
/// <c>by</c> / <c>without</c> clause follows it, <c>start</c> and <c>end</c> only as
/// the <c>@ start()</c> / <c>@ end()</c> calls, and a set operator, <c>offset</c>,
/// <c>by</c>, or <c>without</c> only when it follows an operand; anywhere else each
/// is reported as a referenced name, so the deny-all gate cannot be walked past by
/// naming a metric that shares a keyword's spelling.
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
/// <para>
/// A <c>#</c> comment runs to the end of the line and is discarded exactly as
/// Prometheus's own lexer discards it, before any string or brace state is
/// entered. A quote inside a comment therefore opens no string literal, so a
/// comment can never be used to hide an uncommented metric selector on a later
/// line from the deny-all gate.
/// </para>
/// </remarks>
public static class PromQlMetricExtractor
{
    // Aggregation operators. Each is call-like - written `sum(...)`, or with a
    // grouping modifier before the '(' as `sum by (job) (...)` - and anywhere else
    // Prometheus's grammar reads the keyword as a bare metric name
    // (generated_parser.y, metric_identifier), so `up or min` selects the metric
    // named min.
    private static readonly HashSet<string> AggregationOperators = new(StringComparer.Ordinal)
    {
        "sum", "min", "max", "avg", "group", "stddev", "stdvar", "count",
        "count_values", "bottomk", "topk", "quantile", "limitk", "limit_ratio",
    };

    // Keywords that are an operator or modifier after an operand and a bare
    // metric name where an operand is expected: `a or b` is a set operation, but
    // `a or or` selects the metric named or.
    private static readonly HashSet<string> OperandPositionMetricKeywords = new(StringComparer.Ordinal)
    {
        "and", "or", "unless", "offset", "by", "without",
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

        // Whether the scanner stands where Prometheus expects an operand (the start
        // of the expression, or after an operator, '(' or ','), as opposed to just
        // after one. It decides whether a keyword that doubles as a metric name is
        // the metric (`up or offset`) or the operator / modifier (`up offset 5m`).
        var expectOperand = true;

        while (i < length)
        {
            var c = query[i];

            if (c == '#')
            {
                // A PromQL comment runs to the end of the line. It must be skipped
                // before the quote arm below, because Prometheus strips comments
                // before parsing: an unbalanced quote inside a comment opens no
                // string literal there. Treating it as one would let the scanner
                // swallow the real, uncommented expression on the following lines
                // (`up or #"` then `secret_metric #"`), hiding a denied metric name
                // from the deny-all gate while the backend still evaluates it.
                // Prometheus's lexer discards a comment as it does whitespace, so
                // like the whitespace arm below this leaves metricNamePrecedes
                // intact: `up #c` then `{job="api"}` selects the metric up, and its
                // anchor name was itself allow-list checked when it was added.
                i = SkipLineComment(query, i);
                continue;
            }

            if (c == '"' || c == '\'' || c == '`')
            {
                i = SkipString(query, i);
                metricNamePrecedes = false;
                expectOperand = false;
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

                    if (braceDepth == 0)
                    {
                        expectOperand = false;
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
                    var aggregationGrouping = identifier is "by" or "without";
                    var listStart = SkipWhitespaceIndex(query, i);
                    if (listStart < length && query[listStart] == '(')
                    {
                        i = SkipBalancedParens(query, listStart);

                        // A by/without list closes an aggregation's grouping clause;
                        // an on/ignoring/group_left/group_right list precedes the
                        // right-hand operand of a binary operation.
                        expectOperand = !aggregationGrouping;
                        metricNamePrecedes = false;
                        continue;
                    }

                    if (expectOperand && aggregationGrouping)
                    {
                        AddName(identifier, ref names, ref seen);
                        metricNamePrecedes = true;
                        expectOperand = false;
                        continue;
                    }

                    if (!aggregationGrouping)
                    {
                        // group_left / group_right written without a label list.
                        expectOperand = true;
                    }

                    metricNamePrecedes = false;
                    continue;
                }

                if (NextNonWhitespace(query, i) == '(')
                {
                    // A function or aggregation call; the '(' arm below puts the
                    // scanner in operand position for its arguments.
                    metricNamePrecedes = false;
                    continue;
                }

                if (AggregationOperators.Contains(identifier)
                    && IsFollowedByAggregationGrouping(query, i))
                {
                    // `sum by (job) (...)`: the grouping clause is read next.
                    metricNamePrecedes = false;
                    expectOperand = false;
                    continue;
                }

                if (OperandPositionMetricKeywords.Contains(identifier) && !expectOperand)
                {
                    // A set operator or offset modifier after an operand. Either way
                    // an operand (or the modifier's duration) follows.
                    metricNamePrecedes = false;
                    expectOperand = true;
                    continue;
                }

                if (identifier is "bool" or "atan2")
                {
                    // A comparison modifier or binary operator, never a metric name.
                    metricNamePrecedes = false;
                    expectOperand = true;
                    continue;
                }

                if (identifier is "inf" or "nan")
                {
                    // A numeric literal.
                    metricNamePrecedes = false;
                    expectOperand = false;
                    continue;
                }

                // Everything else is a metric selector, including an aggregation
                // operator or a start / end preprocessor keyword written without a
                // call, and an operator keyword standing where an operand belongs.
                AddName(identifier, ref names, ref seen);
                metricNamePrecedes = true;
                expectOperand = false;
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
                expectOperand = false;
                continue;
            }

            if (char.IsWhiteSpace(c))
            {
                // Whitespace does not break the adjacency between a metric name and
                // a following label selector, so leave metricNamePrecedes intact.
                i++;
                continue;
            }

            // A closing bracket ends an operand; an opening bracket, a comma, or an
            // operator character leaves the scanner expecting one.
            metricNamePrecedes = false;
            expectOperand = c is not (')' or ']');
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

    /// <summary>
    /// Skips a PromQL <c>#</c> comment, which runs to the end of the line. Returns
    /// the index of the line terminator (so the main loop's whitespace arm consumes
    /// it) or the end of the expression when the comment is unterminated.
    /// </summary>
    private static int SkipLineComment(string text, int hashIndex)
    {
        var i = hashIndex + 1;
        while (i < text.Length && text[i] != '\n' && text[i] != '\r')
        {
            i++;
        }

        return i;
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

        // Backtick strings are raw in PromQL: no escape processing, so the value is
        // the literal span between the backticks verbatim.
        if (quote == '`')
        {
            while (i < text.Length)
            {
                if (text[i] == '`')
                {
                    value = text.Substring(start, i - start);
                    return i + 1;
                }

                i++;
            }

            value = null;
            return i;
        }

        // Double- and single-quoted strings process escape sequences. A metric name
        // is compared literally against the deny-all allow-list, so the value must
        // be the unescaped name: {__name__="a\"b"} designates the metric a"b, not
        // a\"b, and returning the raw span with the backslash left in wrongly denies
        // a legitimately allow-listed name. Only the unambiguous backslash and
        // matching-quote escapes - whose expansion is byte-identical to Prometheus -
        // are unescaped here. Any other escape (\n, \x41, \u00e9, octal, ...) leaves
        // the value unresolved so the deny-all gate fails closed rather than risk
        // resolving to a name that diverges from Prometheus's own interpretation.
        StringBuilder? builder = null;
        var segmentStart = start;
        while (i < text.Length)
        {
            var c = text[i];
            if (c == '\\')
            {
                if (i + 1 >= text.Length)
                {
                    // Trailing backslash: the literal is unterminated.
                    value = null;
                    return text.Length;
                }

                var escaped = text[i + 1];
                if (escaped != '\\' && escaped != quote)
                {
                    // An escape this extractor does not model unambiguously. Skip to
                    // the end of the literal and leave the value unresolved.
                    value = null;
                    return SkipString(text, quoteIndex);
                }

                builder ??= new StringBuilder(text.Length - start);
                builder.Append(text, segmentStart, i - segmentStart);
                builder.Append(escaped);
                i += 2;
                segmentStart = i;
                continue;
            }

            if (c == quote)
            {
                if (builder is null)
                {
                    value = text.Substring(start, i - start);
                }
                else
                {
                    builder.Append(text, segmentStart, i - segmentStart);
                    value = builder.ToString();
                }

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

    /// <summary>
    /// <see langword="true"/> when the identifier that follows
    /// <paramref name="index"/> (after any whitespace) is exactly <c>by</c> or
    /// <c>without</c> - the grouping clause an aggregation operator may carry before
    /// its argument list.
    /// </summary>
    private static bool IsFollowedByAggregationGrouping(string text, int index)
    {
        var start = SkipWhitespaceIndex(text, index);
        var end = start;
        while (end < text.Length && IsIdentifierPart(text[end]))
        {
            end++;
        }

        var word = text.AsSpan(start, end - start);
        return word.SequenceEqual("by") || word.SequenceEqual("without");
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
