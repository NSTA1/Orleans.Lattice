namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The evaluation window of a telemetry query: a start instant, an end instant,
/// and a resolution step. It is the only temporal input a caller supplies, and
/// every field is checked against the catalogue entry's
/// <see cref="TelemetryQueryBounds"/> before the query is evaluated.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="TelemetryQueryKind.Instant"/> query uses only <see cref="EndUtc"/>
/// as its evaluation instant and ignores <see cref="StartUtc"/> and
/// <see cref="Step"/>; a <see cref="TelemetryQueryKind.Range"/> query uses all
/// three. Both are carried in one value type so a request and its echoed,
/// post-clamp response range share one shape.
/// </para>
/// <para>
/// Every computed member is a pure function of the three fields and takes no
/// ambient clock, so a caller (and a test) gets a deterministic answer. The type
/// is a <see langword="readonly"/> record struct, so carrying a window costs no
/// heap allocation.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryTimeRange)]
[Immutable]
public readonly record struct TelemetryTimeRange
{
    /// <summary>
    /// The inclusive start of the window. Ignored by an
    /// <see cref="TelemetryQueryKind.Instant"/> query.
    /// </summary>
    [Id(0)] public DateTimeOffset StartUtc { get; init; }

    /// <summary>
    /// The inclusive end of the window, and the evaluation instant of an
    /// <see cref="TelemetryQueryKind.Instant"/> query.
    /// </summary>
    [Id(1)] public DateTimeOffset EndUtc { get; init; }

    /// <summary>
    /// The resolution step between samples. <see cref="TimeSpan.Zero"/> means
    /// "unspecified", in which case the catalogue entry's
    /// <see cref="TelemetryQueryBounds.EffectiveStep(TimeSpan)"/> supplies the
    /// default. Ignored by an <see cref="TelemetryQueryKind.Instant"/> query.
    /// </summary>
    [Id(2)] public TimeSpan Step { get; init; }

    /// <summary>
    /// Creates an instant window evaluated at <paramref name="instant"/>: start
    /// and end both set to it, with no step.
    /// </summary>
    /// <param name="instant">The evaluation instant.</param>
    /// <returns>An instant window.</returns>
    public static TelemetryTimeRange At(DateTimeOffset instant) =>
        new() { StartUtc = instant, EndUtc = instant, Step = TimeSpan.Zero };

    /// <summary>
    /// Creates a range window from <paramref name="startUtc"/> to
    /// <paramref name="endUtc"/> sampled every <paramref name="step"/>. No
    /// validation is performed here; the catalogue entry's bounds decide whether
    /// the window is acceptable.
    /// </summary>
    /// <param name="startUtc">The inclusive start of the window.</param>
    /// <param name="endUtc">The inclusive end of the window.</param>
    /// <param name="step">The resolution step, or <see cref="TimeSpan.Zero"/> to accept the entry's default.</param>
    /// <returns>A range window.</returns>
    public static TelemetryTimeRange Between(DateTimeOffset startUtc, DateTimeOffset endUtc, TimeSpan step) =>
        new() { StartUtc = startUtc, EndUtc = endUtc, Step = step };

    /// <summary>
    /// The window's length. Negative when the window descends, which
    /// <see cref="TelemetryQueryBounds.Validate(TelemetryTimeRange, DateTimeOffset)"/>
    /// rejects as <see cref="TelemetryBoundsViolation.RangeNotAscending"/>.
    /// </summary>
    public TimeSpan Duration => EndUtc - StartUtc;

    /// <summary>
    /// <see langword="true"/> when the window has zero length, so it names a
    /// single instant.
    /// </summary>
    public bool IsInstant => EndUtc == StartUtc;

    /// <summary>
    /// <see langword="true"/> when the window ends at or after it starts, so it is
    /// a well-formed window.
    /// </summary>
    public bool IsAscending => EndUtc >= StartUtc;

    /// <summary>
    /// The number of sample points the window yields at its current
    /// <see cref="Step"/>: <c>0</c> for a descending window, <c>1</c> when no step
    /// is set (or the window is an instant), and
    /// <c>floor(Duration / Step) + 1</c> otherwise. Used to enforce
    /// <see cref="TelemetryQueryBounds.MaxPoints"/> without evaluating the query.
    /// </summary>
    public long PointCount
    {
        get
        {
            if (EndUtc < StartUtc)
            {
                return 0;
            }

            if (Step <= TimeSpan.Zero)
            {
                return 1;
            }

            return ((EndUtc - StartUtc).Ticks / Step.Ticks) + 1;
        }
    }

    /// <summary>
    /// Returns this window with its <see cref="Step"/> replaced by
    /// <paramref name="step"/>, leaving the endpoints untouched. Used by a facade
    /// to echo the step it actually evaluated after clamping.
    /// </summary>
    /// <param name="step">The step to apply.</param>
    /// <returns>A window with the supplied step.</returns>
    public TelemetryTimeRange WithStep(TimeSpan step) => this with { Step = step };
}
