namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// One tenant quota dimension as the Explorer presents it: the consumption, the
/// steady-state ceiling, the burst-adjusted admission ceiling, and the live and
/// accrued overage.
/// <para>
/// It keeps <b>two distinctions a naive <c>long</c> model would flatten</b>, and
/// a renderer must branch on them rather than substitute a zero for either:
/// </para>
/// <list type="number">
///   <item>
///     <description>
///     <b>Unbounded is not a ceiling of zero.</b> <see cref="Limit"/> is
///     <see langword="null"/> when the dimension has no ceiling at all;
///     <c>0</c> is a real cap permitting nothing, under which all usage is
///     already overage. Collapsing the first into the second renders an
///     unlimited tenant as a full bar. Branch on <see cref="IsBounded"/>.
///     <see cref="BurstLimit"/> stays <see langword="null"/> for the same
///     reason.
///     </description>
///   </item>
///   <item>
///     <description>
///     <b>Unmeasured is not a measured zero.</b> <see cref="Usage"/> is
///     <see langword="null"/> when the reading carries no consumption figure -
///     the operation-rate dimension reports its ceiling with no usage sample,
///     because the sampler measures stored bytes, live keys, resident memory,
///     and owned trees but no operation rate. Substituting <c>0</c> renders a
///     permanently empty bar that reads as "you are using none of your rate
///     limit" when the truth is "we are not measuring this". Branch on
///     <see cref="IsMeasured"/>; <see cref="Overage"/> and
///     <see cref="MeteredOverage"/> carry no meaning on an unmeasured
///     dimension.
///     </description>
///   </item>
/// </list>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so a quota panel that polls this figure reads it
/// without allocating one object per dimension per poll.
/// </para>
/// </summary>
public readonly record struct ExplorerTenantQuotaDimension
{
    /// <summary>
    /// The tenant's consumption of this dimension, or <see langword="null"/>
    /// when the reading carries no consumption figure. Never substitute
    /// <c>0</c> for <see langword="null"/>; see <see cref="IsMeasured"/>.
    /// </summary>
    public long? Usage { get; init; }

    /// <summary>
    /// The steady-state ceiling, or <see langword="null"/> when the dimension
    /// is unbounded. Never substitute <c>0</c> for <see langword="null"/>; see
    /// <see cref="IsBounded"/>.
    /// </summary>
    public long? Limit { get; init; }

    /// <summary>
    /// The burst-adjusted admission ceiling - what admission actually enforces
    /// at the moment of the reading - or <see langword="null"/> when the
    /// dimension is unbounded.
    /// </summary>
    public long? BurstLimit { get; init; }

    /// <summary>
    /// The live overage: how far consumption currently exceeds the ceiling.
    /// Meaningless unless the dimension is both bounded and measured.
    /// </summary>
    public long Overage { get; init; }

    /// <summary>
    /// The accrued, billable overage recorded for this dimension. Meaningless
    /// unless the dimension is both bounded and measured.
    /// </summary>
    public long MeteredOverage { get; init; }

    /// <summary>
    /// The dimension with no ceiling and no consumption figure, which is also
    /// <see langword="default"/>. The honest default: nothing is asserted about
    /// either the ceiling or the consumption.
    /// </summary>
    public static ExplorerTenantQuotaDimension Unbounded => default;

    /// <summary>
    /// <see langword="true"/> when the dimension carries a ceiling. A bounded
    /// dimension with a <see cref="Limit"/> of <c>0</c> permits nothing and is
    /// deliberately distinct from an unbounded one.
    /// </summary>
    public bool IsBounded => Limit is not null;

    /// <summary>
    /// <see langword="true"/> when the reading carries a consumption figure. An
    /// unmeasured dimension is deliberately distinct from one measured at
    /// <c>0</c>.
    /// </summary>
    public bool IsMeasured => Usage is not null;

    /// <summary>
    /// <see langword="true"/> when consumption currently exceeds the ceiling.
    /// Always <see langword="false"/> on a dimension that is not both bounded
    /// and measured, because neither case can establish a breach.
    /// </summary>
    public bool IsOverLimit => IsBounded && IsMeasured && Usage > Limit;

    /// <summary>
    /// Consumption as a fraction of the ceiling, or <see langword="null"/> when
    /// no fraction can honestly be computed - that is, whenever the dimension
    /// is unbounded or unmeasured. A renderer must show "unlimited" or "not
    /// measured" in those cases rather than an empty bar.
    /// <para>
    /// The value is not clamped, so a breach reports greater than <c>1</c> and
    /// a bar can render the overage. A ceiling of <c>0</c> is the one case with
    /// no meaningful ratio: it reports <c>0</c> when nothing is consumed and
    /// <c>1</c> otherwise (fully consumed, with <see cref="Overage"/> carrying
    /// the real excess), because dividing by it would be undefined.
    /// </para>
    /// </summary>
    public double? Utilization
    {
        get
        {
            if (Usage is not { } usage || Limit is not { } limit)
            {
                return null;
            }

            if (limit == 0)
            {
                return usage == 0 ? 0d : 1d;
            }

            return (double)usage / limit;
        }
    }
}
