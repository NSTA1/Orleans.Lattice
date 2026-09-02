using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// A directly driven <see cref="IExplorerAccessibleTenantSource"/>, so a test
/// states exactly which tenants the caller can reach rather than standing up an
/// administrative surface to answer it. Counts its calls so a test can prove the
/// list is read once per refresh and not once per render.
/// </summary>
internal sealed class FakeAccessibleTenantSource : IExplorerAccessibleTenantSource
{
    /// <summary>Creates a source reporting <paramref name="tenantIds"/> as reachable.</summary>
    /// <param name="tenantIds">The reachable tenant ids, best-first.</param>
    public FakeAccessibleTenantSource(params string[] tenantIds) => Tenants = Map(tenantIds);

    /// <summary>The tenants reported as reachable. Settable so a test can revoke one.</summary>
    public IReadOnlyList<ExplorerTenantId> Tenants { get; set; }

    /// <summary>How many times the list has been asked for.</summary>
    public int Calls { get; private set; }

    /// <summary>Replaces the reachable set, as a revoked grant or a deleted tenant would.</summary>
    /// <param name="tenantIds">The new reachable tenant ids.</param>
    public void Reachable(params string[] tenantIds) => Tenants = Map(tenantIds);

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<ExplorerTenantId>> GetAccessibleTenantsAsync(
        CancellationToken cancellationToken = default)
    {
        Calls++;
        return new ValueTask<IReadOnlyList<ExplorerTenantId>>(Tenants);
    }

    private static IReadOnlyList<ExplorerTenantId> Map(string[] tenantIds)
    {
        if (tenantIds.Length == 0)
        {
            return Array.Empty<ExplorerTenantId>();
        }

        var mapped = new ExplorerTenantId[tenantIds.Length];
        for (var i = 0; i < tenantIds.Length; i++)
        {
            mapped[i] = new ExplorerTenantId(tenantIds[i]);
        }

        return mapped;
    }
}
