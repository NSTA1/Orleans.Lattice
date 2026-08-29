namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// A reference from a curated named query to one metric instrument it reads,
/// carrying the instrument's own declared unit and measurement semantic. It is
/// the anti-drift link: a query's title and unit can be checked against the
/// instruments that actually feed it instead of being trusted, so a panel cannot
/// silently claim a record rate over an instrument that counts operations.
/// </summary>
/// <remarks>
/// This is a value-typed reference (a <see langword="readonly"/> record struct),
/// so a catalogue listing many queries with several instruments each allocates
/// only the backing arrays, not one object per reference. The strings are
/// interned catalogue text authored server-side, never caller input.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryInstrumentReference)]
[Immutable]
public readonly record struct TelemetryInstrumentReference
{
    /// <summary>
    /// Initializes a reference to <paramref name="name"/> declaring the unit and
    /// measurement semantic the instrument truly records.
    /// </summary>
    /// <param name="name">The fully qualified instrument name, for example <c>orleans.lattice.shard.writes</c>.</param>
    /// <param name="meter">The meter the instrument belongs to, for example <c>orleans.lattice</c>.</param>
    /// <param name="unit">The instrument's declared unit, for example <c>{op}</c>, <c>ms</c>, or <c>By</c>.</param>
    /// <param name="semantic">What the instrument actually measures.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/>, <paramref name="meter"/>, or <paramref name="unit"/> is <see langword="null"/>.</exception>
    public TelemetryInstrumentReference(
        string name,
        string meter,
        string unit,
        TelemetryMeasurementSemantic semantic)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(meter);
        ArgumentNullException.ThrowIfNull(unit);

        Name = name;
        Meter = meter;
        Unit = unit;
        Semantic = semantic;
    }

    /// <summary>
    /// The fully qualified instrument name as emitted by the meter, for example
    /// <c>orleans.lattice.shard.writes</c>. <see langword="null"/> only on a
    /// default-constructed value, which the facade never produces.
    /// </summary>
    [Id(0)] public string Name { get; init; }

    /// <summary>
    /// The meter that owns the instrument, for example <c>orleans.lattice</c> or
    /// <c>orleans.lattice.tenancy</c>. Lets a client group a query's sources by
    /// the package that emits them.
    /// </summary>
    [Id(1)] public string Meter { get; init; }

    /// <summary>
    /// The instrument's own declared unit (<c>{op}</c>, <c>ms</c>, <c>By</c>,
    /// <c>1</c>, ...). It is the instrument's unit, not the query's: a rate query
    /// over a <c>{op}</c> counter reports <c>{op}/s</c> while this stays
    /// <c>{op}</c>.
    /// </summary>
    [Id(2)] public string Unit { get; init; }

    /// <summary>
    /// What this instrument actually measures. Declared per instrument because a
    /// derived query may combine instruments whose semantics differ.
    /// </summary>
    [Id(3)] public TelemetryMeasurementSemantic Semantic { get; init; }
}
