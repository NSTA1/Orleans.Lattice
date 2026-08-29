namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The shape of a query result, projected from the metrics backend's own result
/// type into a closed, transport-neutral enum so a binding never has to interpret
/// a free-form backend string.
/// </summary>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryResultKind)]
public enum TelemetryResultKind
{
    /// <summary>
    /// The query evaluated but matched nothing, so no series were returned. The
    /// zero value, so an unpopulated response reads as empty rather than as a
    /// shape it does not have.
    /// </summary>
    Empty = 0,

    /// <summary>
    /// An instant vector: one series per matching label set, each carrying exactly
    /// one sample. Produced by a <see cref="TelemetryQueryKind.Instant"/> query.
    /// </summary>
    Vector = 1,

    /// <summary>
    /// A range matrix: one series per matching label set, each carrying one sample
    /// per resolution step. Produced by a <see cref="TelemetryQueryKind.Range"/>
    /// query.
    /// </summary>
    Matrix = 2,

    /// <summary>
    /// A single scalar value, modelled as one label-free series carrying one
    /// sample.
    /// </summary>
    Scalar = 3,
}
