using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// One quota dimension projected for display: its label, which of the four
/// reading states it is in, and the figures already rendered to text.
/// <para>
/// The projection happens once when a reading loads, not once per render, so a
/// quota surface that re-renders on every gate change and every busy transition
/// allocates no strings on the render path. A
/// <see langword="readonly"/> <see langword="record"/> <see langword="struct"/>,
/// so the five dimensions of a reading are one array rather than five objects.
/// </para>
/// <para>
/// <b>The state is the point.</b> Build a row through
/// <see cref="From(ExplorerTenantQuotaDimensionKind, ExplorerTenantQuotaDimension)"/>
/// and render <see cref="UsageText"/> and <see cref="LimitText"/> as they come:
/// they already say "unlimited" and "not measured" where those are the truth, so
/// nothing downstream has to remember not to substitute a zero.
/// </para>
/// </summary>
public readonly record struct TenantQuotaRow
{
    /// <summary>The dimension this row reports.</summary>
    public required ExplorerTenantQuotaDimensionKind Kind { get; init; }

    /// <summary>The dimension's display label.</summary>
    public required string Label { get; init; }

    /// <summary>Which of the four reading states the dimension is in.</summary>
    public required TenantQuotaReadingState State { get; init; }

    /// <summary>
    /// The consumption, already formatted, or
    /// <see cref="TenantQuotaFormat.NotMeasuredText"/> when the reading carried
    /// none. Never <c>0</c> standing in for an absent sample.
    /// </summary>
    public required string UsageText { get; init; }

    /// <summary>
    /// The steady-state ceiling, already formatted, or
    /// <see cref="TenantQuotaFormat.UnlimitedText"/> when the dimension is
    /// unbounded. Never <c>0</c> standing in for an absent ceiling.
    /// </summary>
    public required string LimitText { get; init; }

    /// <summary>
    /// The burst-adjusted admission ceiling - what admission actually enforces at
    /// the moment of the reading - already formatted, or
    /// <see cref="TenantQuotaFormat.UnlimitedText"/> when unbounded.
    /// </summary>
    public required string BurstLimitText { get; init; }

    /// <summary>
    /// The live overage, already formatted, or an empty string when the
    /// dimension is not both bounded and measured, where an overage carries no
    /// meaning.
    /// </summary>
    public required string OverageText { get; init; }

    /// <summary>
    /// The accrued, billable overage, already formatted, or an empty string when
    /// it carries no meaning.
    /// </summary>
    public required string MeteredOverageText { get; init; }

    /// <summary>
    /// Consumption as a fraction of the ceiling, or <see langword="null"/>
    /// whenever no honest fraction exists - that is, in every state but
    /// <see cref="TenantQuotaReadingState.Bounded"/>.
    /// </summary>
    public double? Utilization { get; init; }

    /// <summary>
    /// Whether consumption currently exceeds the ceiling. Always
    /// <see langword="false"/> unless the dimension is both bounded and
    /// measured, because neither absence can establish a breach.
    /// </summary>
    public bool IsOverLimit { get; init; }

    /// <summary>
    /// Whether a consumption bar may be drawn at all. Only a bounded, measured
    /// dimension has a real numerator and denominator; every other state renders
    /// its words instead, because a bar there would assert a fraction nobody
    /// measured.
    /// </summary>
    public bool ShowsBar => State == TenantQuotaReadingState.Bounded;

    /// <summary>
    /// The bar's width as a whole percentage, clamped to <c>0</c>-<c>100</c> so a
    /// breach fills the track rather than overflowing it. Meaningful only when
    /// <see cref="ShowsBar"/>; <c>0</c> otherwise.
    /// </summary>
    public int BarPercent => Utilization is { } fraction
        ? (int)Math.Clamp(Math.Round(fraction * 100d), 0d, 100d)
        : 0;

    /// <summary>
    /// Projects <paramref name="dimension"/> for display, resolving its reading
    /// state and formatting every figure exactly once.
    /// </summary>
    /// <param name="kind">The dimension being projected.</param>
    /// <param name="dimension">The reading to project.</param>
    /// <returns>The display row.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a defined dimension.</exception>
    public static TenantQuotaRow From(
        ExplorerTenantQuotaDimensionKind kind,
        ExplorerTenantQuotaDimension dimension)
    {
        var label = TenantQuotaFormat.Label(kind);
        var bounded = dimension.IsBounded;
        var measured = dimension.IsMeasured;

        // The four combinations stay four. Ordering matters only for readability:
        // the two single-absence states are distinguishable either way, but the
        // double absence must not fall through into one of them.
        var state = (bounded, measured) switch
        {
            (true, true) => TenantQuotaReadingState.Bounded,
            (false, true) => TenantQuotaReadingState.Unlimited,
            (true, false) => TenantQuotaReadingState.NotMeasured,
            _ => TenantQuotaReadingState.Unknown,
        };

        var overageMeaningful = bounded && measured;
        return new TenantQuotaRow
        {
            Kind = kind,
            Label = label,
            State = state,
            UsageText = dimension.Usage is { } usage
                ? TenantQuotaFormat.Value(kind, usage)
                : TenantQuotaFormat.NotMeasuredText,
            LimitText = dimension.Limit is { } limit
                ? TenantQuotaFormat.Value(kind, limit)
                : TenantQuotaFormat.UnlimitedText,
            BurstLimitText = dimension.BurstLimit is { } burst
                ? TenantQuotaFormat.Value(kind, burst)
                : TenantQuotaFormat.UnlimitedText,
            OverageText = overageMeaningful && dimension.Overage > 0
                ? TenantQuotaFormat.Value(kind, dimension.Overage)
                : string.Empty,
            MeteredOverageText = overageMeaningful && dimension.MeteredOverage > 0
                ? TenantQuotaFormat.Value(kind, dimension.MeteredOverage)
                : string.Empty,
            Utilization = state == TenantQuotaReadingState.Bounded ? dimension.Utilization : null,
            IsOverLimit = dimension.IsOverLimit,
        };
    }
}
