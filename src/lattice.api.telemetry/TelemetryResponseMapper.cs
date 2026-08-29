using System.Globalization;
using System.Text.Json;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Projects the metrics backend's query envelope into the transport-neutral
/// contract shapes, so every telemetry binding surfaces one series model rather
/// than each interpreting the backend's JSON for itself.
/// </summary>
/// <remarks>
/// <para>
/// The backend's free-form <c>resultType</c> string is mapped onto the closed
/// <see cref="TelemetryResultKind"/> enum; an unrecognised shape maps to
/// <see cref="TelemetryResultKind.Empty"/> with no series rather than being
/// forwarded verbatim, so a binding never has to interpret a backend string.
/// </para>
/// <para>
/// Samples are carried as <see cref="double"/>, exactly as the backend evaluates
/// them, and the special forms a backend can return (<see cref="double.NaN"/> and
/// the infinities) are preserved rather than coerced, so a gap reaches the client
/// as itself. A sample whose value is not parseable is skipped rather than
/// substituted with a plausible number.
/// </para>
/// <para>
/// Every list is sized from the JSON array it is built from, so the mapping
/// allocates one array per series and one per label set and never grows a list by
/// doubling.
/// </para>
/// </remarks>
internal static class TelemetryResponseMapper
{
    /// <summary>
    /// The initial label-list capacity. A Lattice series carries a handful of
    /// dimensions (tree, tenant, and one or two operation-specific tags), so sizing
    /// to that up front avoids the doubling reallocations a default-capacity list
    /// would perform once per series in a many-series matrix.
    /// </summary>
    private const int TypicalLabelCount = 8;

    /// <summary>
    /// The inclusive bounds <see cref="DateTimeOffset.FromUnixTimeMilliseconds"/>
    /// accepts. Compared as <see cref="double"/> before the conversion, because an
    /// unchecked cast of an out-of-range double to <see cref="long"/> yields an
    /// unspecified value rather than saturating.
    /// </summary>
    private const double MinRepresentableUnixMilliseconds = -62135596800000d;

    /// <inheritdoc cref="MinRepresentableUnixMilliseconds"/>
    private const double MaxRepresentableUnixMilliseconds = 253402300799999d;

    private static readonly IReadOnlyList<TelemetryTimeSeries> NoSeries = [];

    /// <summary>
    /// Maps the backend <paramref name="data"/> payload into the result kind and
    /// the series it carries.
    /// </summary>
    /// <param name="data">The backend's <c>data</c> element.</param>
    /// <returns>The mapped result shape and series.</returns>
    public static (TelemetryResultKind Kind, IReadOnlyList<TelemetryTimeSeries> Series) Map(JsonElement data)
    {
        if (data.ValueKind != JsonValueKind.Object
            || !data.TryGetProperty("resultType", out var resultTypeElement)
            || resultTypeElement.ValueKind != JsonValueKind.String
            || !data.TryGetProperty("result", out var result))
        {
            return (TelemetryResultKind.Empty, NoSeries);
        }

        return resultTypeElement.GetString() switch
        {
            "vector" => (TelemetryResultKind.Vector, MapVector(result)),
            "matrix" => (TelemetryResultKind.Matrix, MapMatrix(result)),
            "scalar" => (TelemetryResultKind.Scalar, MapScalar(result)),
            _ => (TelemetryResultKind.Empty, NoSeries),
        };
    }

    private static IReadOnlyList<TelemetryTimeSeries> MapVector(JsonElement result)
    {
        if (result.ValueKind != JsonValueKind.Array)
        {
            return NoSeries;
        }

        var series = new List<TelemetryTimeSeries>(result.GetArrayLength());
        foreach (var item in result.EnumerateArray())
        {
            TelemetryDataPoint[] points = [];
            if (item.TryGetProperty("value", out var value) && TryReadSample(value, out var sample))
            {
                points = [sample];
            }

            series.Add(new TelemetryTimeSeries { Labels = ReadLabels(item), Points = points });
        }

        return series;
    }

    private static IReadOnlyList<TelemetryTimeSeries> MapMatrix(JsonElement result)
    {
        if (result.ValueKind != JsonValueKind.Array)
        {
            return NoSeries;
        }

        var series = new List<TelemetryTimeSeries>(result.GetArrayLength());
        foreach (var item in result.EnumerateArray())
        {
            IReadOnlyList<TelemetryDataPoint> points = [];
            if (item.TryGetProperty("values", out var values) && values.ValueKind == JsonValueKind.Array)
            {
                var mapped = new List<TelemetryDataPoint>(values.GetArrayLength());
                foreach (var pair in values.EnumerateArray())
                {
                    if (TryReadSample(pair, out var sample))
                    {
                        mapped.Add(sample);
                    }
                }

                points = mapped;
            }

            series.Add(new TelemetryTimeSeries { Labels = ReadLabels(item), Points = points });
        }

        return series;
    }

    private static IReadOnlyList<TelemetryTimeSeries> MapScalar(JsonElement result) =>
        TryReadSample(result, out var sample)
            ? [new TelemetryTimeSeries { Labels = [], Points = [sample] }]
            : NoSeries;

    private static IReadOnlyList<TelemetryLabel> ReadLabels(JsonElement item)
    {
        if (!item.TryGetProperty("metric", out var metric) || metric.ValueKind != JsonValueKind.Object)
        {
            return [];
        }

        var labels = new List<TelemetryLabel>(TypicalLabelCount);
        foreach (var property in metric.EnumerateObject())
        {
            var value = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString() ?? string.Empty
                : property.Value.GetRawText();
            labels.Add(new TelemetryLabel(property.Name, value));
        }

        return labels;
    }

    /// <summary>
    /// Reads one <c>[timestamp, "value"]</c> pair. A Prometheus timestamp is
    /// seconds since the epoch as a JSON number, and the value is a decimal string.
    /// </summary>
    private static bool TryReadSample(JsonElement pair, out TelemetryDataPoint sample)
    {
        sample = default;
        if (pair.ValueKind != JsonValueKind.Array || pair.GetArrayLength() < 2)
        {
            return false;
        }

        var timestampElement = pair[0];
        if (timestampElement.ValueKind != JsonValueKind.Number
            || !timestampElement.TryGetDouble(out var epochSeconds)
            || !double.IsFinite(epochSeconds))
        {
            return false;
        }

        // Range-check before converting. A backend that emits milliseconds where the
        // protocol specifies seconds - or any other out-of-range instant - would
        // otherwise throw out of the mapper, past the fault handling that turns a
        // bad payload into a TelemetryBackendException. An unrepresentable instant is
        // an unreadable sample, so it is skipped like any other.
        var milliseconds = Math.Round(epochSeconds * 1000d);
        if (milliseconds < MinRepresentableUnixMilliseconds
            || milliseconds > MaxRepresentableUnixMilliseconds)
        {
            return false;
        }

        if (!TryReadValue(pair[1], out var value))
        {
            return false;
        }

        sample = new TelemetryDataPoint(DateTimeOffset.FromUnixTimeMilliseconds((long)milliseconds), value);
        return true;
    }

    private static bool TryReadValue(JsonElement element, out double value)
    {
        if (element.ValueKind == JsonValueKind.Number)
        {
            return element.TryGetDouble(out value);
        }

        if (element.ValueKind != JsonValueKind.String)
        {
            value = 0d;
            return false;
        }

        var text = element.GetString();
        if (text is null)
        {
            value = 0d;
            return false;
        }

        // A backend renders the special forms as bare tokens, which the invariant
        // parser accepts, so a gap or an overflow is carried through as itself.
        return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
            || TryReadSpecial(text, out value);
    }

    private static bool TryReadSpecial(string text, out double value)
    {
        switch (text)
        {
            case "NaN":
                value = double.NaN;
                return true;
            case "+Inf":
            case "Inf":
                value = double.PositiveInfinity;
                return true;
            case "-Inf":
                value = double.NegativeInfinity;
                return true;
            default:
                value = 0d;
                return false;
        }
    }
}
