namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// One (timestamp, value) sample of a returned time series. A
/// <see langword="readonly"/> record struct, so a range response of thousands of
/// points costs one array rather than one object per point.
/// </summary>
/// <remarks>
/// The value is a <see cref="double"/> because a Prometheus-compatible backend
/// evaluates in 64-bit floating point, so the projection is lossless. The special
/// forms a backend can return - <see cref="double.NaN"/>,
/// <see cref="double.PositiveInfinity"/>, and
/// <see cref="double.NegativeInfinity"/> - are representable and are carried
/// through rather than coerced, so a gap or an overflow reaches the client as
/// itself.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryDataPoint)]
[Immutable]
public readonly record struct TelemetryDataPoint
{
    /// <summary>Initializes a sample.</summary>
    /// <param name="timestamp">The instant the sample was evaluated at.</param>
    /// <param name="value">The sample value.</param>
    public TelemetryDataPoint(DateTimeOffset timestamp, double value)
    {
        Timestamp = timestamp;
        Value = value;
    }

    /// <summary>The instant this sample was evaluated at.</summary>
    [Id(0)] public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The sample value. May be <see cref="double.NaN"/> or an infinity when the
    /// backend evaluated to one.
    /// </summary>
    [Id(1)] public double Value { get; init; }

    /// <summary>
    /// <see langword="true"/> when the value is finite, so a client can skip
    /// plotting a gap or an overflow without inspecting the raw value itself.
    /// </summary>
    public bool IsFinite => double.IsFinite(Value);
}
