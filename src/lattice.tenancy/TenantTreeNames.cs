namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Well-known names for the reserved, dogfooded <c>ILattice</c> trees that back
/// the tenant registry. These live in the <c>sys-</c> system-data namespace, so
/// every read and write addresses them under system-origin (both to skip the
/// access gate and to satisfy the reserved-prefix write guard). The
/// <c>sys-tenant-</c> naming convention reserves them from collision with
/// application trees while keeping tenant definitions introspectable through the
/// standard read / scan / change-feed surface.
/// </summary>
internal static class TenantTreeNames
{
    /// <summary>The shared prefix identifying every tenant-registry-owned tree.</summary>
    internal const string TreePrefix = "sys-tenant-";

    /// <summary>
    /// Tree holding one <see cref="TenantRecord"/> per tenant, keyed by the
    /// tenant id text.
    /// </summary>
    internal const string RegistryTree = "sys-tenant-registry";

    /// <summary>Durable per-key history view name for <see cref="RegistryTree"/>.</summary>
    internal const string RegistryHistoryView = "sys-tenant-registry-history";

    /// <summary>
    /// Tree holding one <see cref="TenantUsageRecord"/> per tenant, keyed by the
    /// tenant id text. Backs the aggregate per-tenant usage accounting and quota
    /// enforcement layer; each cluster publishes only its own usage slot into the
    /// per-tenant record, and the global fold sums the slots. It is not part of
    /// <see cref="AllTrees"/>: the registry initializer's history-retention and
    /// history-view bootstrap is scoped to the definition registry, and the usage
    /// tree carries transient converged aggregates that need no durable history.
    /// </summary>
    internal const string UsageTree = "sys-tenant-usage";

    /// <summary>
    /// Tree holding one <see cref="TenantOverageRecord"/> per tenant, keyed by the
    /// tenant id text. Backs the first-class, billing-ready per-tenant overage
    /// meter; each cluster advances only its own grow-only counter component in the
    /// per-tenant record, and the global fold sums the components. Like
    /// <see cref="UsageTree"/> it is not part of <see cref="AllTrees"/>: it carries
    /// converged grow-only aggregates that need no durable per-key history.
    /// </summary>
    internal const string OverageTree = "sys-tenant-overage";

    /// <summary>Enumerates the backing tree names.</summary>
    internal static IReadOnlyList<string> AllTrees { get; } = new[] { RegistryTree };
}
