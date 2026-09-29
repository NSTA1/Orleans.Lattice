using System.Runtime.InteropServices;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// The Explorer's <see cref="IExplorerAccessibleTenantSource"/>: the tenants the
/// cluster says this caller can reach, read from the same catalogue the tenant
/// directory lists, so the address root's tenant selector, the <c>t/</c>
/// completions and the directory can never offer different lists.
/// </summary>
/// <remarks>
/// <para>
/// The established tenant leads, so the first entry is the one to fall back to.
/// A suspended tenant is not offered, since scoping to one would show a surface
/// of refusals, but the established tenant is kept whatever its state.
/// </para>
/// <para>
/// Fail-closed on every unhappy path: a refused, failed or empty read reports
/// exactly what Core's own default would, the established tenant alone or
/// nothing, and never a tenant the cluster did not name.
/// </para>
/// </remarks>
/// <param name="catalog">The circuit's tenancy catalogue.</param>
/// <param name="context">The circuit's tenant context, or <see langword="null"/> when tenancy is not registered.</param>
internal sealed class TenancyAccessibleTenantSource(TenancyCatalog catalog, IExplorerTenantContext? context) : IExplorerAccessibleTenantSource
{
    private readonly TenancyCatalog _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    private ExplorerTenantId[] _settled = [];

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<ExplorerTenantId>> GetAccessibleTenantsAsync(CancellationToken cancellationToken = default)
    {
        var established = context?.ActiveTenant;
        var reachable = new List<ExplorerTenantId>();
        if (established is { } active)
        {
            reachable.Add(active);
        }

        try
        {
            foreach (var tenant in await _catalog.GetTenantsAsync(cancellationToken).ConfigureAwait(true))
            {
                if (tenant.Status != TenantLifecycleStatus.Active || string.IsNullOrEmpty(tenant.TenantId))
                {
                    continue;
                }

                var candidate = new ExplorerTenantId(tenant.TenantId);
                if (candidate != established)
                {
                    reachable.Add(candidate);
                }
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // Exactly Core's own fail-closed answer: where the caller already is.
        }

        // The settled array is handed out again while the answer is unchanged, so a
        // steady state costs the consumers no new list.
        if (!_settled.AsSpan().SequenceEqual(CollectionsMarshal.AsSpan(reachable)))
        {
            _settled = [.. reachable];
        }

        return _settled;
    }
}
