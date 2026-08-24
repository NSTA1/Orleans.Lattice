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

    /// <summary>Enumerates the backing tree names.</summary>
    internal static IReadOnlyList<string> AllTrees { get; } = new[] { RegistryTree };
}
