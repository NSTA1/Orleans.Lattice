using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Translates .NET metrics identifiers into the Prometheus text exposition
/// format (version 0.0.4), and formats sample values. Pure and static so the
/// naming contract is testable without a live meter.
/// </summary>
/// <remarks>
/// Kept separate from <see cref="RepoContextMetricsCollector"/> because the
/// name mapping is the part a scrape configuration depends on: an instrument
/// rename that changes the exposed series name is a breaking change for a
/// dashboard, so the mapping is pinned by its own tests.
/// </remarks>
public static class RepoContextPrometheusExposition
{
    /// <summary>
    /// The content type a Prometheus scraper expects, including the format
    /// version and charset.
    /// </summary>
    public const string ContentType = "text/plain; version=0.0.4; charset=utf-8";

    /// <summary>The suffix Prometheus convention appends to a cumulative counter.</summary>
    public const string CounterSuffix = "_total";

    /// <summary>
    /// Converts a .NET instrument name (dotted, for example
    /// <c>repocontext.ann.sweep</c>) into a legal Prometheus metric name
    /// (<c>repocontext_ann_sweep</c>), appending <see cref="CounterSuffix"/> for a
    /// counter that does not already carry it.
    /// </summary>
    /// <param name="instrumentName">The .NET instrument name.</param>
    /// <param name="kind">The Prometheus family the instrument renders as.</param>
    /// <returns>The exposed metric name.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="instrumentName"/> is null.</exception>
    public static string MetricName(string instrumentName, RepoContextMetricKind kind)
    {
        ArgumentNullException.ThrowIfNull(instrumentName);

        var sanitized = SanitizeMetricName(instrumentName);
        if (kind != RepoContextMetricKind.Counter
            || sanitized.EndsWith(CounterSuffix, StringComparison.Ordinal))
        {
            return sanitized;
        }

        return sanitized + CounterSuffix;
    }

    /// <summary>
    /// Replaces every character outside <c>[a-zA-Z0-9_:]</c> with an underscore and
    /// prefixes an underscore when the result would otherwise start with a digit,
    /// so the name matches Prometheus' metric-name grammar.
    /// </summary>
    /// <param name="value">The raw name.</param>
    /// <returns>A legal Prometheus metric name.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    public static string SanitizeMetricName(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.Length == 0)
        {
            return "_";
        }

        var builder = new StringBuilder(value.Length + 1);
        if (char.IsAsciiDigit(value[0]))
        {
            builder.Append('_');
        }

        foreach (var c in value)
        {
            builder.Append(char.IsAsciiLetterOrDigit(c) || c == '_' || c == ':' ? c : '_');
        }

        return builder.ToString();
    }

    /// <summary>
    /// Replaces every character outside <c>[a-zA-Z0-9_]</c> with an underscore and
    /// prefixes an underscore when the result would otherwise start with a digit,
    /// so the name matches Prometheus' label-name grammar. Note a label name may
    /// not contain a colon, unlike a metric name.
    /// </summary>
    /// <param name="value">The raw tag key.</param>
    /// <returns>A legal Prometheus label name.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    public static string SanitizeLabelName(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.Length == 0)
        {
            return "_";
        }

        var builder = new StringBuilder(value.Length + 1);
        if (char.IsAsciiDigit(value[0]))
        {
            builder.Append('_');
        }

        foreach (var c in value)
        {
            builder.Append(char.IsAsciiLetterOrDigit(c) || c == '_' ? c : '_');
        }

        return builder.ToString();
    }

    /// <summary>
    /// Escapes a label value for the text exposition format: backslash, double
    /// quote, and line feed are the three characters the grammar reserves.
    /// </summary>
    /// <param name="value">The raw label value.</param>
    /// <returns>The escaped value, without surrounding quotes.</returns>
    public static string EscapeLabelValue(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        if (value.AsSpan().IndexOfAny('\\', '"', '\n') < 0)
        {
            return value;
        }

        var builder = new StringBuilder(value.Length + 8);
        foreach (var c in value)
        {
            switch (c)
            {
                case '\\': builder.Append("\\\\"); break;
                case '"': builder.Append("\\\""); break;
                case '\n': builder.Append("\\n"); break;
                default: builder.Append(c); break;
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// Escapes a <c># HELP</c> comment body, where backslash and line feed are
    /// reserved but the double quote is not.
    /// </summary>
    /// <param name="value">The raw help text.</param>
    /// <returns>The escaped help text.</returns>
    public static string EscapeHelp(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        if (value.AsSpan().IndexOfAny('\\', '\n') < 0)
        {
            return value;
        }

        var builder = new StringBuilder(value.Length + 8);
        foreach (var c in value)
        {
            switch (c)
            {
                case '\\': builder.Append("\\\\"); break;
                case '\n': builder.Append("\\n"); break;
                default: builder.Append(c); break;
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// Formats a sample value the way the exposition grammar requires: culture
    /// invariant, round-trippable, with the three non-finite values spelled the way
    /// Prometheus expects rather than the way .NET spells them.
    /// </summary>
    /// <param name="value">The sample value.</param>
    /// <returns>The formatted value.</returns>
    public static string FormatValue(double value)
    {
        if (double.IsNaN(value))
        {
            return "NaN";
        }

        if (double.IsPositiveInfinity(value))
        {
            return "+Inf";
        }

        if (double.IsNegativeInfinity(value))
        {
            return "-Inf";
        }

        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    /// <summary>Renders the <c># TYPE</c> keyword for a metric family.</summary>
    /// <param name="kind">The metric family.</param>
    /// <returns>The exposition keyword.</returns>
    public static string TypeKeyword(RepoContextMetricKind kind) => kind switch
    {
        RepoContextMetricKind.Counter => "counter",
        RepoContextMetricKind.Summary => "summary",
        _ => "gauge",
    };
}
