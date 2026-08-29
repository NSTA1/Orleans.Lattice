using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The display vocabulary for a quota reading: the label and unit of each
/// dimension, the caption that qualifies the whole reading with the scope it was
/// enforced under, and the text each non-bar presentation renders.
/// <para>
/// Every member returns an interned literal or a cached string, so the quota
/// surface can call them per dimension per render without allocating.
/// </para>
/// </summary>
public static class TenantQuotaLabels
{
    /// <summary>
    /// The caption for a reading whose figures are a converged cross-cluster
    /// total, so the number genuinely is the tenant's whole consumption.
    /// </summary>
    public const string GlobalConvergedCaption =
        "Converged total across every cluster. These figures are the tenant's whole consumption.";

    /// <summary>
    /// The caption for a reading whose figures are one cluster's local view.
    /// <para>
    /// It says so plainly, because a per-cluster reading is genuinely not a
    /// global total and presenting it as one would understate consumption on a
    /// multi-cluster tenant.
    /// </para>
    /// </summary>
    public const string PerClusterCaption =
        "This cluster's local view only. These figures are not a global total: another cluster's "
        + "consumption for this tenant is not included, and the ceiling is enforced per cluster.";

    /// <summary>
    /// The caption for a reading that carries authoritative ceilings but no
    /// usage figures at all, because no warm reading has been compiled yet.
    /// Ceilings are still real; the consumption is simply unknown.
    /// </summary>
    public const string NoUsageReadingCaption =
        "No usage reading has been compiled for this tenant yet. The ceilings below are "
        + "authoritative; consumption is not measured rather than zero.";

    /// <summary>What an unbounded dimension renders instead of a bar.</summary>
    public const string UnboundedText = "No limit";

    /// <summary>What an unmeasured dimension renders instead of a figure.</summary>
    public const string UnmeasuredText = "Not measured";

    /// <summary>What a dimension with neither a ceiling nor a sample renders.</summary>
    public const string UnknownText = "No limit, not measured";

    /// <summary>
    /// The human-readable label of <paramref name="kind"/>.
    /// </summary>
    /// <param name="kind">The dimension to label.</param>
    /// <returns>The dimension's display label.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="kind"/> is not a declared dimension.
    /// </exception>
    public static string Label(ExplorerTenantQuotaDimensionKind kind) => kind switch
    {
        ExplorerTenantQuotaDimensionKind.Bytes => "Stored bytes",
        ExplorerTenantQuotaDimensionKind.Keys => "Live keys",
        ExplorerTenantQuotaDimensionKind.MemoryBytes => "Resident memory",
        ExplorerTenantQuotaDimensionKind.TreeCount => "Owned trees",
        ExplorerTenantQuotaDimensionKind.OpsPerSecond => "Operations per second",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown quota dimension."),
    };

    /// <summary>
    /// The unit <paramref name="kind"/> is counted in, or an empty string for a
    /// dimension whose figures are bare counts.
    /// </summary>
    /// <param name="kind">The dimension to describe.</param>
    /// <returns>The dimension's unit.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="kind"/> is not a declared dimension.
    /// </exception>
    public static string Unit(ExplorerTenantQuotaDimensionKind kind) => kind switch
    {
        ExplorerTenantQuotaDimensionKind.Bytes => "bytes",
        ExplorerTenantQuotaDimensionKind.Keys => "keys",
        ExplorerTenantQuotaDimensionKind.MemoryBytes => "bytes",
        ExplorerTenantQuotaDimensionKind.TreeCount => "trees",
        ExplorerTenantQuotaDimensionKind.OpsPerSecond => "ops/s",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown quota dimension."),
    };

    /// <summary>
    /// The short label naming the scope the figures were enforced under, for the
    /// badge beside them.
    /// </summary>
    /// <param name="scope">The enforcement scope the reading reported.</param>
    /// <returns>The scope's display label.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="scope"/> is not a declared scope.
    /// </exception>
    public static string EnforcementLabel(ExplorerTenantQuotaEnforcement scope) => scope switch
    {
        ExplorerTenantQuotaEnforcement.GlobalConverged => "Global (converged)",
        ExplorerTenantQuotaEnforcement.PerCluster => "Per cluster",
        _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, "Unknown enforcement scope."),
    };

    /// <summary>
    /// The sentence qualifying a whole reading: what the figures actually are.
    /// A reading with no usage at all is captioned as such, whatever its scope,
    /// because "not measured" is the more important qualification.
    /// </summary>
    /// <param name="usage">The reading to caption. Must not be <see langword="null"/>.</param>
    /// <returns>The caption for the reading.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="usage"/> is <see langword="null"/>.</exception>
    public static string Caption(ExplorerTenantQuotaUsage usage)
    {
        ArgumentNullException.ThrowIfNull(usage);
        return usage.HasUsage ? Caption(usage.EnforcementScope) : NoUsageReadingCaption;
    }

    /// <summary>
    /// The sentence qualifying figures read under <paramref name="scope"/>.
    /// </summary>
    /// <param name="scope">The enforcement scope the reading reported.</param>
    /// <returns>The caption for that scope.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="scope"/> is not a declared scope.
    /// </exception>
    public static string Caption(ExplorerTenantQuotaEnforcement scope) => scope switch
    {
        ExplorerTenantQuotaEnforcement.GlobalConverged => GlobalConvergedCaption,
        ExplorerTenantQuotaEnforcement.PerCluster => PerClusterCaption,
        _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, "Unknown enforcement scope."),
    };

    /// <summary>
    /// The text a gauge renders in place of a bar, or <see langword="null"/>
    /// when the gauge admits a real bar and the caller should draw one.
    /// </summary>
    /// <param name="gauge">The gauge to describe.</param>
    /// <returns>The replacement text, or <see langword="null"/> for a real bar.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="gauge"/> carries an unknown presentation.
    /// </exception>
    public static string? WithoutBarText(in TenantQuotaGauge gauge) => gauge.Presentation switch
    {
        TenantQuotaPresentation.Bar => null,
        TenantQuotaPresentation.UnboundedWithUsage => UnboundedText,
        TenantQuotaPresentation.UnmeasuredWithLimit => UnmeasuredText,
        TenantQuotaPresentation.Unknown => UnknownText,
        _ => throw new ArgumentOutOfRangeException(nameof(gauge), gauge.Presentation, "Unknown presentation."),
    };
}
