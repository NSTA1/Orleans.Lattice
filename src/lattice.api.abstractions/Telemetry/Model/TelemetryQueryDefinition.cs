namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The server-side authoring shape of one curated named query: its client-facing
/// <see cref="Descriptor"/> plus the query-language template that actually
/// evaluates it. It is the only place a query expression exists in this contract,
/// and it travels from a catalogue author to the facade, never from a caller.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the split.</b> Keeping the template off <see cref="TelemetryQueryDescriptor"/>
/// makes the "no caller-supplied query text" rule structural: the type a client
/// receives has nowhere to put an expression, and the type that carries one is
/// never accepted as request input by any operation on
/// <see cref="ILatticeTelemetry"/>. A future operator-only authoring surface can
/// consume this type without widening the read path.
/// </para>
/// <para>
/// <b>Templates are not caller-composed.</b> A template may contain placeholders
/// the facade substitutes from the bounded parameters the descriptor declares. The
/// tenant-scoping predicate is <em>not</em> among them: the facade derives the
/// effective tenant from the authenticated caller and injects the scope itself, so
/// a template cannot opt out of tenant isolation, and an author cannot weaken it
/// by omission.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryDefinition)]
[Immutable]
public sealed record TelemetryQueryDefinition
{
    /// <summary>The client-facing catalogue entry this definition backs.</summary>
    [Id(0)] public required TelemetryQueryDescriptor Descriptor { get; init; }

    /// <summary>
    /// The server-authored query template evaluated against the metrics backend,
    /// with placeholders for the bounded parameters the descriptor declares. Never
    /// read from a caller and never returned to one.
    /// </summary>
    [Id(1)] public required string QueryTemplate { get; init; }

    /// <summary>
    /// The id of the query this definition backs, taken from its
    /// <see cref="Descriptor"/>.
    /// </summary>
    public string QueryId => Descriptor.QueryId;
}
