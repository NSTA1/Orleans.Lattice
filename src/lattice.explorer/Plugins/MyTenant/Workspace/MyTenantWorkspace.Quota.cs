using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;

/// <summary>
/// The Quota surface: consumption against each ceiling, qualified by the scope
/// the figures were enforced under.
/// </summary>
/// <remarks>
/// <para>
/// The projection keeps the two distinctions the reading itself keeps. A
/// <see langword="null"/> ceiling means <em>unbounded</em>, not a ceiling of
/// zero; a <see langword="null"/> usage means <em>not measured</em>, not a
/// measured zero. Neither is ever coalesced, because the seam's
/// <c>Utilization</c> is deliberately <see langword="null"/> in both cases and a
/// renderer that treated that as zero would draw an empty bar for an unlimited
/// quota and for an unmeasured rate limit alike - a bar that lies in two
/// different directions.
/// </para>
/// <para>
/// The gauges are projected into one array that is allocated once and refilled
/// in place, so re-reading the quota costs no allocation on the polling path.
/// </para>
/// </remarks>
public sealed partial class MyTenantWorkspace
{
    private readonly TenantQuotaGauge[] _gauges =
        new TenantQuotaGauge[ExplorerTenantQuotaUsage.Dimensions.Count];

    private bool _quotaLoaded;

    /// <summary>
    /// The tenant's consumption reading, or <see langword="null"/> before it is
    /// read.
    /// </summary>
    public ExplorerTenantQuotaUsage? Usage { get; private set; }

    /// <summary>
    /// One gauge per dimension, in the reading's own declared order. Empty until
    /// a reading arrives, so the surface never renders five gauges of invented
    /// zeros.
    /// </summary>
    public IReadOnlyList<TenantQuotaGauge> Gauges =>
        Usage is null ? Array.Empty<TenantQuotaGauge>() : _gauges;

    /// <summary>
    /// The scope the figures were enforced under, for the badge beside them.
    /// <see langword="null"/> before a reading arrives.
    /// </summary>
    public ExplorerTenantQuotaEnforcement? EnforcementScope => Usage?.EnforcementScope;

    /// <summary>
    /// The sentence qualifying the whole reading: whether the figures are a
    /// converged cross-cluster total or one cluster's local view, or whether no
    /// usage has been compiled at all.
    /// <para>
    /// A per-cluster reading genuinely is not a global total, so it is never
    /// presented as one.
    /// </para>
    /// </summary>
    public string? Caption => Usage is { } usage ? TenantQuotaLabels.Caption(usage) : null;

    /// <summary>
    /// Whether the reading carries usage figures at all. When false the ceilings
    /// below are still authoritative, and every dimension reports its
    /// consumption as not measured rather than as zero.
    /// </summary>
    public bool HasUsageReading => Usage?.HasUsage == true;

    /// <summary>
    /// The burst allowance, as a percentage above each ceiling, that the tenant
    /// is permitted to exceed transiently.
    /// </summary>
    public int BurstPercent => Usage?.BurstPercent ?? 0;

    /// <summary>
    /// Whether any bounded, measured dimension is currently over its ceiling, so
    /// the surface can lead with that rather than making the caller find it.
    /// </summary>
    public bool HasBreach
    {
        get
        {
            if (Usage is null)
            {
                return false;
            }

            for (var i = 0; i < _gauges.Length; i++)
            {
                if (_gauges[i].IsOverLimit)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>Re-reads the tenant's consumption against its ceilings.</summary>
    public Task RefreshQuotaAsync() => LoadQuotaAsync(force: true);

    private async Task LoadQuotaAsync(bool force)
    {
        if ((!force && _quotaLoaded) || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        _quotaLoaded = true;

        var usage = await _domain.Tenants.GetQuotaUsageAsync(TenantId).ConfigureAwait(false);
        if (usage.IsSuccess && usage.Value is { } reading)
        {
            Project(reading);
            return;
        }

        Usage = null;
        LastNotice = MyTenantNotice.For(usage);
    }

    /// <summary>
    /// Refills the gauge array from one reading.
    /// </summary>
    /// <remarks>
    /// Every figure comes from this single reading and none is combined with a
    /// separately-read descriptor, because the control API guarantees the
    /// reading is internally coherent: pairing a just-lowered ceiling with
    /// not-yet-resampled usage would render a breach that admission is not
    /// actually enforcing.
    /// </remarks>
    private void Project(ExplorerTenantQuotaUsage reading)
    {
        Usage = reading;

        var dimensions = ExplorerTenantQuotaUsage.Dimensions;
        for (var i = 0; i < _gauges.Length && i < dimensions.Count; i++)
        {
            var kind = dimensions[i];
            _gauges[i] = new TenantQuotaGauge(kind, reading[kind]);
        }
    }
}
