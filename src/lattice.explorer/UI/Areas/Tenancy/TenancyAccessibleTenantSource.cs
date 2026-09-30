using System.Runtime.InteropServices;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

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
/// The established tenant is only ever one established for the identity asking
/// now. The circuit's tenant context still holds the previous identity's tenant
/// while a new sign-in establishes its own, so the tenant the context held when
/// the identity changed is not led with, kept on a failed read, or added when the
/// cluster does not name it, until the context is written again for the new
/// identity: a new identity inherits nothing from the previous one.
/// </para>
/// <para>
/// A proven platform operator is also offered the reserved default tenant, which
/// the cluster never lists (it lists only the tenants a caller administers), so
/// an operator can always get back to it. Every other caller's list is exactly
/// what the cluster named.
/// </para>
/// <para>
/// Fail-closed on every unhappy path: a refused, failed or empty read reports
/// exactly what Core's own default would, the established tenant alone or
/// nothing (plus the default tenant for a proven operator), and never a tenant
/// the cluster did not name.
/// </para>
/// </remarks>
/// <param name="catalog">The circuit's tenancy catalogue.</param>
/// <param name="context">The circuit's tenant context, or <see langword="null"/> when tenancy is not registered.</param>
internal sealed class TenancyAccessibleTenantSource(TenancyCatalog catalog, IExplorerTenantContext? context) : IExplorerAccessibleTenantSource
{
    private readonly TenancyCatalog _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    private ExplorerTenantId[] _settled = [];
    private ShellCallerKey? _identity;
    private ExplorerTenantId? _inherited;
    private bool _inheriting;

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<ExplorerTenantId>> GetAccessibleTenantsAsync(CancellationToken cancellationToken = default)
    {
        var established = EstablishedForThisIdentity();
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

        // The cluster lists only the tenants a caller administers, which never
        // includes the reserved default tenant, so without this an operator who
        // administers any tenant could never reach it. Only proven operator
        // standing adds it: a non-operator's list is exactly what the cluster said.
        if (!reachable.Contains(ExplorerTenantId.Default)
            && await _catalog.IsOperatorAsync(cancellationToken).ConfigureAwait(true))
        {
            reachable.Insert(DefaultTenantPosition(reachable, established is not null), ExplorerTenantId.Default);
        }

        // The settled array is handed out again while the answer is unchanged, so a
        // steady state costs the consumers no new list.
        if (!_settled.AsSpan().SequenceEqual(CollectionsMarshal.AsSpan(reachable)))
        {
            _settled = [.. reachable];
        }

        return _settled;
    }

    /// <summary>
    /// The context's tenant, unless it is still the one the context held when the
    /// identity (sign-in, scheme, user and endpoint) changed since this source last
    /// answered: that one was established for the previous identity, not for the
    /// caller asking now. The circuit's first answer trusts the context.
    /// </summary>
    private ExplorerTenantId? EstablishedForThisIdentity()
    {
        var current = context?.ActiveTenant;
        var identity = _catalog.Caller.Current with { Tenant = null, Generation = 0 };
        if (_identity is not { } previous)
        {
            _identity = identity;
        }
        else if (previous != identity)
        {
            _identity = identity;
            _inherited = current;
            _inheriting = current is not null;
        }
        else if (_inheriting && current != _inherited)
        {
            // Written since the identity changed: established for this identity.
            _inheriting = false;
        }

        return _inheriting ? null : current;
    }

    /// <summary>
    /// Where the default tenant goes in the list: after the established tenant,
    /// which always leads, in id order among the rest.
    /// </summary>
    private static int DefaultTenantPosition(List<ExplorerTenantId> reachable, bool establishedLeads)
    {
        var position = establishedLeads ? 1 : 0;
        while (position < reachable.Count
            && string.CompareOrdinal(reachable[position].Value, ExplorerTenantId.Default.Value) < 0)
        {
            position++;
        }

        return position;
    }
}
