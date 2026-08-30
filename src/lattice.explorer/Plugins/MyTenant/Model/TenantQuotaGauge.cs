using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// One quota dimension, projected for display: the dimension it reports, the
/// reading behind it, and the presentation that reading actually admits.
/// <para>
/// This is the type that keeps the plugin honest. It never coalesces a missing
/// ceiling or a missing usage sample to zero: <see cref="Presentation"/> reports
/// which of the four genuine cases the dimension is in, and
/// <see cref="BarPercent"/> is meaningful only for
/// <see cref="TenantQuotaPresentation.Bar"/>. A caller that renders a bar
/// unconditionally would show an unlimited tenant as full and an unmeasured rate
/// limit as unused; both are lies the reading itself does not tell.
/// </para>
/// </summary>
/// <remarks>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/> wrapping the reading rather than copying its fields,
/// so a refresh of the whole set costs one array write per dimension and a
/// render costs none.
/// </remarks>
/// <param name="Kind">The dimension this gauge reports.</param>
/// <param name="Reading">The dimension's reading, exactly as the cluster gave it.</param>
public readonly record struct TenantQuotaGauge(
    ExplorerTenantQuotaDimensionKind Kind,
    ExplorerTenantQuotaDimension Reading)
{
    /// <summary>
    /// Whether the dimension has a ceiling at all. <see langword="false"/> means
    /// unbounded, which is not the same as a ceiling of <c>0</c>.
    /// </summary>
    public bool IsBounded => Reading.IsBounded;

    /// <summary>
    /// Whether the reading carries a consumption figure for this dimension.
    /// <see langword="false"/> means not measured, which is not the same as a
    /// measured zero.
    /// </summary>
    public bool IsMeasured => Reading.IsMeasured;

    /// <summary>The ceiling, or <see langword="null"/> when unbounded.</summary>
    public long? Limit => Reading.Limit;

    /// <summary>The consumption, or <see langword="null"/> when not measured.</summary>
    public long? Usage => Reading.Usage;

    /// <summary>
    /// The burst ceiling, or <see langword="null"/> when unbounded. Stays
    /// <see langword="null"/> rather than collapsing to <c>0</c>, for the same
    /// reason <see cref="Limit"/> does.
    /// </summary>
    public long? BurstLimit => Reading.BurstLimit;

    /// <summary>
    /// The live overage: how far consumption currently exceeds the ceiling.
    /// Carries no meaning on a dimension that is not both bounded and measured,
    /// which <see cref="HasOverage"/> tests.
    /// </summary>
    public long Overage => Reading.Overage;

    /// <summary>
    /// The accrued overage the cluster has metered. Carries no meaning on a
    /// dimension that is not both bounded and measured.
    /// </summary>
    public long MeteredOverage => Reading.MeteredOverage;

    /// <summary>How this dimension must be presented.</summary>
    public TenantQuotaPresentation Presentation => (IsBounded, IsMeasured) switch
    {
        (true, true) => TenantQuotaPresentation.Bar,
        (false, true) => TenantQuotaPresentation.UnboundedWithUsage,
        (true, false) => TenantQuotaPresentation.UnmeasuredWithLimit,
        _ => TenantQuotaPresentation.Unknown,
    };

    /// <summary>
    /// Whether a proportional bar may be drawn. Only a dimension that is both
    /// bounded and measured admits one.
    /// </summary>
    public bool HasBar => Presentation == TenantQuotaPresentation.Bar;

    /// <summary>
    /// The bar's fill, as a whole percentage clamped to <c>[0, 100]</c>, or
    /// <c>0</c> when no bar may be drawn - in which case the caller must render
    /// no bar at all rather than an empty one.
    /// </summary>
    /// <remarks>
    /// A ceiling of exactly <c>0</c> reads as <c>100</c> whenever any usage
    /// exists, because every byte against a cap of nothing is already overage.
    /// That is the reading's own arithmetic, not a rendering convenience.
    /// </remarks>
    public int BarPercent
    {
        get
        {
            if (Reading.Utilization is not { } utilization)
            {
                return 0;
            }

            var scaled = (int)Math.Round(utilization * 100d, MidpointRounding.AwayFromZero);
            return Math.Clamp(scaled, 0, 100);
        }
    }

    /// <summary>
    /// Whether consumption exceeds the ceiling. <see langword="false"/> on any
    /// dimension that is not both bounded and measured, so an unmeasured or
    /// unbounded dimension is never reported as breached.
    /// </summary>
    public bool IsOverLimit => Reading.IsOverLimit;

    /// <summary>
    /// Whether an overage figure is meaningful here: the dimension is bounded
    /// and measured, and the cluster reported a non-zero live or accrued
    /// overage.
    /// </summary>
    public bool HasOverage => HasBar && (Overage > 0 || MeteredOverage > 0);

    /// <summary>
    /// The remaining burst allowance above the ceiling, or
    /// <see langword="null"/> when the dimension is unbounded, unmeasured, or
    /// carries no burst ceiling. Never negative: once consumption is past the
    /// burst ceiling the headroom is gone, which is <c>0</c>, not a debt.
    /// </summary>
    public long? BurstHeadroom =>
        BurstLimit is { } burst && Usage is { } usage
            ? Math.Max(0, burst - usage)
            : null;
}
