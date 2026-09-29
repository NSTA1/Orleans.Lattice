using System.Diagnostics.Metrics;
using System.Text;

namespace Orleans.Lattice.Testing.Metrics;

/// <summary>
/// The Prometheus family type an instrument is exported as, which decides the type
/// suffixes its series carry.
/// </summary>
public enum PrometheusFamilyType
{
    /// <summary>A monotonic sum (<c>Counter&lt;T&gt;</c>, <c>ObservableCounter&lt;T&gt;</c>): one series ending <c>_total</c>.</summary>
    Counter,

    /// <summary>A gauge or non-monotonic sum (<c>UpDownCounter&lt;T&gt;</c>, <c>ObservableGauge&lt;T&gt;</c>, <c>ObservableUpDownCounter&lt;T&gt;</c>): one series named for the family.</summary>
    Gauge,

    /// <summary>A <c>Histogram&lt;T&gt;</c>: <c>_bucket</c>, <c>_count</c> and <c>_sum</c> series.</summary>
    Histogram,
}

/// <summary>
/// The exact series names <c>.AddPrometheusExporter()</c> emits for an instrument,
/// derived from its name, declared unit and kind.
/// </summary>
/// <remarks>
/// <para>
/// This models <c>OpenTelemetry.Exporter.Prometheus</c> as pinned by
/// <c>OpenTelemetry.Exporter.Prometheus.AspNetCore</c> 1.15.3-beta.1 - the
/// <c>PrometheusMetric</c> constructor and its <c>GetUnit</c> / <c>MapUnit</c> /
/// <c>MapPerUnit</c> tables - rule for rule:
/// </para>
/// <list type="number">
/// <item>The name is sanitized: every run of characters other than letters, digits
/// and <c>:</c> becomes one <c>_</c>, and a leading digit is replaced by <c>_</c>.</item>
/// <item>The unit loses its <c>{annotation}</c> segments, a <c>a/b</c> unit becomes
/// <c>a_per_b</c> (with <c>b</c> mapped to its singular word), and otherwise the
/// abbreviation is mapped to its word (<c>ms</c> to <c>milliseconds</c>, <c>s</c> to
/// <c>seconds</c>, <c>By</c> to <c>bytes</c>, <c>%</c> to <c>percent</c>, <c>1</c> to
/// nothing, an unknown unit to itself), then sanitized and trimmed of <c>_</c>.</item>
/// <item>The unit word is appended as <c>_word</c> unless the sanitized name already
/// ends with the word. The check is a plain suffix match, so a name ending in the
/// abbreviation (<c>_ms</c>) still gains the word, and an empty word never appends.</item>
/// <item>A counter gains <c>_total</c> unless the name already ends with it; a
/// histogram's series gain <c>_bucket</c>, <c>_count</c> and <c>_sum</c>.</item>
/// </list>
/// <para>
/// It deliberately does <b>not</b> model the repository-context container's own
/// exposition, which appends no unit word and renders every histogram as a
/// bucketless summary. The bundled dashboards target this exporter, so a gate built
/// on this model rejects a panel written in the container's spelling.
/// </para>
/// </remarks>
public static class PrometheusExporterNaming
{
    /// <summary>The Prometheus family type an instrument declared by <paramref name="kind"/> is exported as.</summary>
    /// <param name="kind">The declaring factory.</param>
    /// <returns>The family type.</returns>
    public static PrometheusFamilyType FamilyTypeOf(DeclaredInstrumentKind kind) => kind switch
    {
        DeclaredInstrumentKind.Counter or DeclaredInstrumentKind.ObservableCounter => PrometheusFamilyType.Counter,
        DeclaredInstrumentKind.Histogram => PrometheusFamilyType.Histogram,
        DeclaredInstrumentKind.UpDownCounter
            or DeclaredInstrumentKind.ObservableGauge
            or DeclaredInstrumentKind.ObservableUpDownCounter => PrometheusFamilyType.Gauge,
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown instrument kind."),
    };

    /// <summary>The Prometheus family type a live instrument is exported as.</summary>
    /// <param name="instrument">The live instrument.</param>
    /// <returns>The family type.</returns>
    public static PrometheusFamilyType FamilyTypeOf(Instrument instrument)
    {
        ArgumentNullException.ThrowIfNull(instrument);

        var definition = instrument.GetType().IsGenericType
            ? instrument.GetType().GetGenericTypeDefinition()
            : instrument.GetType();

        if (definition == typeof(Counter<>) || definition == typeof(ObservableCounter<>))
        {
            return PrometheusFamilyType.Counter;
        }

        if (definition == typeof(Histogram<>))
        {
            return PrometheusFamilyType.Histogram;
        }

        if (definition == typeof(UpDownCounter<>)
            || definition == typeof(ObservableGauge<>)
            || definition == typeof(ObservableUpDownCounter<>)
            || definition == typeof(Gauge<>))
        {
            return PrometheusFamilyType.Gauge;
        }

        throw new ArgumentException($"Instrument type '{instrument.GetType()}' has no modelled Prometheus family type.", nameof(instrument));
    }

    /// <summary>
    /// The word the exporter appends for a declared unit, or the empty string when it
    /// appends none.
    /// </summary>
    /// <param name="unit">The declared unit; <see langword="null"/> or empty means none.</param>
    /// <returns>The sanitized unit word, without a leading underscore.</returns>
    public static string UnitWord(string? unit)
    {
        if (string.IsNullOrEmpty(unit))
        {
            return string.Empty;
        }

        var stripped = RemoveAnnotations(unit);
        var slash = stripped.IndexOf('/', StringComparison.Ordinal);
        var word = slash >= 0 && slash < stripped.Length - 1
            ? MapUnit(stripped[..slash]) + "_per_" + MapPerUnit(stripped[(slash + 1)..])
            : MapUnit(stripped);

        return Sanitize(word, allowLeadingDigit: true).Trim('_');
    }

    /// <summary>
    /// The family name the exporter writes on the <c># TYPE</c> line: the sanitized
    /// name plus the unit word, plus <c>_total</c> for a counter.
    /// </summary>
    /// <param name="instrumentName">The instrument's dotted name.</param>
    /// <param name="unit">The declared unit; <see langword="null"/> or empty means none.</param>
    /// <param name="type">The family type.</param>
    /// <returns>The family name.</returns>
    public static string FamilyName(string instrumentName, string? unit, PrometheusFamilyType type)
    {
        ArgumentNullException.ThrowIfNull(instrumentName);

        var name = Sanitize(instrumentName, allowLeadingDigit: false);
        var word = UnitWord(unit);
        if (!name.EndsWith(word, StringComparison.Ordinal))
        {
            name += "_" + word;
        }

        if (type == PrometheusFamilyType.Counter && !name.EndsWith("_total", StringComparison.Ordinal))
        {
            name += "_total";
        }

        return name;
    }

    /// <summary>
    /// Every series name a PromQL query can select for the instrument: the family
    /// name itself for a counter or gauge, and its <c>_bucket</c>, <c>_count</c> and
    /// <c>_sum</c> series for a histogram (whose bare family name selects nothing).
    /// </summary>
    /// <param name="instrumentName">The instrument's dotted name.</param>
    /// <param name="unit">The declared unit; <see langword="null"/> or empty means none.</param>
    /// <param name="type">The family type.</param>
    /// <returns>The series names, in a stable order.</returns>
    public static IReadOnlyList<string> SeriesNames(string instrumentName, string? unit, PrometheusFamilyType type)
    {
        var family = FamilyName(instrumentName, unit, type);
        return type == PrometheusFamilyType.Histogram
            ? [family + "_bucket", family + "_count", family + "_sum"]
            : [family];
    }

    private static string Sanitize(string value, bool allowLeadingDigit)
    {
        var sb = new StringBuilder(value.Length);
        var lastUnderscore = false;

        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];

            if (i == 0 && !allowLeadingDigit && char.IsNumber(c))
            {
                sb.Append('_');
                lastUnderscore = true;
                continue;
            }

            if (!char.IsLetterOrDigit(c) && c != ':')
            {
                if (!lastUnderscore)
                {
                    sb.Append('_');
                    lastUnderscore = true;
                }

                continue;
            }

            sb.Append(c);
            lastUnderscore = false;
        }

        return sb.ToString();
    }

    private static string RemoveAnnotations(string unit)
    {
        var sb = new StringBuilder(unit.Length);
        var open = -1;
        var lastWrite = 0;

        for (var i = 0; i < unit.Length; i++)
        {
            if (unit[i] == '{' && open < 0)
            {
                open = i;
            }
            else if (unit[i] == '}' && open >= 0)
            {
                sb.Append(unit, lastWrite, open - lastWrite);
                open = -1;
                lastWrite = i + 1;
            }
        }

        if (lastWrite == 0)
        {
            return unit;
        }

        sb.Append(unit, lastWrite, unit.Length - lastWrite);
        return sb.ToString();
    }

    private static string MapUnit(string unit) => unit switch
    {
        "d" => "days",
        "h" => "hours",
        "min" => "minutes",
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        "By" or "B" => "bytes",
        "KiBy" => "kibibytes",
        "MiBy" => "mebibytes",
        "GiBy" => "gibibytes",
        "TiBy" => "tibibytes",
        "KBy" or "KB" => "kilobytes",
        "MBy" or "MB" => "megabytes",
        "GBy" or "GB" => "gigabytes",
        "TBy" or "TB" => "terabytes",
        "m" => "meters",
        "V" => "volts",
        "A" => "amperes",
        "J" => "joules",
        "W" => "watts",
        "g" => "grams",
        "Cel" => "celsius",
        "Hz" => "hertz",
        "1" => string.Empty,
        "%" => "percent",
        "$" => "dollars",
        _ => unit,
    };

    private static string MapPerUnit(string perUnit) => perUnit switch
    {
        "s" => "second",
        "m" => "minute",
        "h" => "hour",
        "d" => "day",
        "w" => "week",
        "mo" => "month",
        "y" => "year",
        _ => perUnit,
    };
}
