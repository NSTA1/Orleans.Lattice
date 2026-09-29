using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// One quota dimension of a usage report, projected for display without ever
/// coalescing an absence to zero: an unbounded dimension is not a ceiling of
/// zero, and an unmeasured one is not a measured zero. Only a dimension that is
/// both bounded and measured has a <see cref="Percent"/>, so nothing draws a bar
/// that the reading does not support.
/// </summary>
/// <param name="Dimension">The dimension reported.</param>
/// <param name="Reading">The reading, exactly as the cluster gave it.</param>
internal readonly record struct TenancyQuotaGauge(TenancyQuotaDimension Dimension, TenantQuotaDimensionUsage Reading)
{
    /// <summary>Whether the dimension has a ceiling.</summary>
    public bool IsBounded => Reading.IsBounded;

    /// <summary>Whether the reading carries a consumption figure.</summary>
    public bool IsMeasured => Reading.IsMeasured;

    /// <summary>
    /// Consumption as a whole percentage of the ceiling, which may exceed 100, or
    /// <see langword="null"/> unless the dimension is bounded and measured. A
    /// ceiling of zero reads as 100 once anything is used, and 0 otherwise.
    /// </summary>
    public int? Percent
    {
        get
        {
            if (Reading.Usage is not { } usage || Reading.Limit is not { } limit)
            {
                return null;
            }

            if (limit <= 0)
            {
                return usage > 0 ? 100 : 0;
            }

            return (int)Math.Min(int.MaxValue, Math.Round(usage * 100d / limit, MidpointRounding.AwayFromZero));
        }
    }

    /// <summary>The bar's fill, <see cref="Percent"/> clamped to <c>[0, 100]</c>, or <see langword="null"/> for no bar.</summary>
    public int? BarPercent => Percent is { } percent ? Math.Clamp(percent, 0, 100) : null;

    /// <summary>Whether consumption exceeds the ceiling; never true unless bounded and measured.</summary>
    public bool IsOverLimit => Reading.Usage is { } usage && Reading.Limit is { } limit && usage > limit;

    /// <summary>The consumption, in the dimension's unit, or "Not measured".</summary>
    public string UsageText => Reading.Usage is { } usage ? TenancyFormat.Figure(Dimension, usage) : "Not measured";

    /// <summary>The ceiling, in the dimension's unit, or "Unbounded".</summary>
    public string LimitText => Reading.Limit is { } limit ? TenancyFormat.Figure(Dimension, limit) : "Unbounded";

    /// <summary>The burst ceiling, in the dimension's unit, or "None".</summary>
    public string BurstText => Reading.BurstLimit is { } burst ? TenancyFormat.Figure(Dimension, burst) : "None";

    /// <summary>The use column: a percentage, or what stands in for one.</summary>
    public string UseText => (IsBounded, IsMeasured) switch
    {
        (true, true) => IsOverLimit
            ? TenancyFormat.Percent(Percent!.Value) + ", over by " + TenancyFormat.Figure(Dimension, Reading.Usage!.Value - Reading.Limit!.Value)
            : TenancyFormat.Percent(Percent!.Value),
        (false, true) => "No ceiling",
        (true, false) => "Not measured",
        _ => "No ceiling, not measured",
    };

    /// <summary>The five gauges of <paramref name="report"/>, in the area's order.</summary>
    /// <param name="report">The usage report.</param>
    public static IReadOnlyList<TenancyQuotaGauge> All(TenantQuotaUsageReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return
        [
            new(TenancyQuotaDimension.Bytes, report.Bytes),
            new(TenancyQuotaDimension.Keys, report.Keys),
            new(TenancyQuotaDimension.MemoryBytes, report.MemoryBytes),
            new(TenancyQuotaDimension.TreeCount, report.TreeCount),
            new(TenancyQuotaDimension.OpsPerSecond, report.OpsPerSecond),
        ];
    }
}
