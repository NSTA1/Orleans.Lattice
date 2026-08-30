namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// One label on a returned time series: a name and its value, for example
/// <c>tree</c> / <c>t/acme/orders</c> or <c>tenant</c> / <c>acme</c>. Labels are
/// carried as an ordered list of value-typed pairs rather than a dictionary, so a
/// response of many series costs one array per series instead of a hash table, and
/// the backend's label order is preserved for a stable legend.
/// </summary>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryLabel)]
[Immutable]
public readonly record struct TelemetryLabel
{
    /// <summary>Initializes a label pair.</summary>
    /// <param name="name">The label name, for example <c>tree</c>.</param>
    /// <param name="value">The label value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> or <paramref name="value"/> is <see langword="null"/>.</exception>
    public TelemetryLabel(string name, string value)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(value);

        Name = name;
        Value = value;
    }

    /// <summary>
    /// The label name. <see langword="null"/> only on a default-constructed value,
    /// which the facade never produces.
    /// </summary>
    [Id(0)] public string Name { get; init; }

    /// <summary>
    /// The label value. <see langword="null"/> only on a default-constructed value,
    /// which the facade never produces.
    /// </summary>
    [Id(1)] public string Value { get; init; }
}
