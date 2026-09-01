using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// The real <see cref="IExplorerAccessibleTenantSource"/>: the tenants the
/// cluster says this caller can reach, read through the tenancy seam the tenant
/// administration area already lists from.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is the one-source-of-truth seam.</b> The shell's tenant picker offers
/// exactly this list and the identity resolver validates a remembered tenant
/// against exactly this list, so registering it is what stops the picker and the
/// tenant administration area from being two disconnected views of the same
/// question. Without it the navigation core falls back to
/// <c>ActiveTenantOnlyAccessibleTenantSource</c>, which reports only the tenant
/// already in force - correct, fail-closed, and useless for switching.
/// </para>
/// <para>
/// <b>Fail-closed on every unhappy path.</b> A refused, failed, or empty read
/// degrades to exactly what the navigation core's own default would have
/// reported: the established tenant, or nothing when none is established. It
/// never guesses, and it never reports a tenant the cluster did not name.
/// </para>
/// <para>
/// <b>Suspended tenants are not offered.</b> A suspended tenant's data plane
/// refuses operations, so scoping the Explorer to one would produce a surface of
/// refusals. The caller's own established tenant is kept regardless of its state,
/// because a list that omitted where the caller already is would claim they
/// cannot reach it.
/// </para>
/// </remarks>
/// <param name="tenants">The tenancy operations surface the list is read from.</param>
/// <param name="context">
/// The per-circuit tenant context, read for the established tenant that orders
/// and backstops the list.
/// </param>
internal sealed class TenantsAccessibleTenantSource(
    ITenantAdminService tenants,
    IExplorerTenantContext context) : IExplorerAccessibleTenantSource
{
    private readonly ITenantAdminService _tenants =
        tenants ?? throw new ArgumentNullException(nameof(tenants));

    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    // The last answer, returned again by reference whenever the cluster's answer
    // has not changed. The consumers ask on the resolve path and on every
    // tenant-control refresh, so a steady state costs no allocation - but the
    // read itself is not skipped, because a tenant created a moment ago must
    // appear in the picker without a reconnect.
    private ExplorerTenantId[] _cached = Array.Empty<ExplorerTenantId>();

    /// <inheritdoc />
    /// <remarks>
    /// One <see cref="List{T}"/> per call is the whole cost. The contract puts
    /// this on the resolve path and on a tenant-control refresh - never per
    /// render - and consumers cache the answer for a render pass, so the read is
    /// not skipped in exchange for a stale picker; the settled array is reused by
    /// reference whenever the cluster's answer has not moved.
    /// </remarks>
    public async ValueTask<IReadOnlyList<ExplorerTenantId>> GetAccessibleTenantsAsync(
        CancellationToken cancellationToken = default)
    {
        var active = _context.ActiveTenant;

        var listed = await _tenants.ListAccessibleTenantsAsync(cancellationToken).ConfigureAwait(false);
        if (!listed.IsSuccess || listed.Value is not { Count: > 0 } summaries)
        {
            // Exactly the navigation core's own fail-closed answer: the caller
            // can reach where they already are, and nothing that was not named.
            var established = new List<ExplorerTenantId>(1);
            if (active is { } current)
            {
                established.Add(current);
            }

            return Settle(established);
        }

        var reachable = new List<ExplorerTenantId>(summaries.Count);

        // The established tenant leads, so the first entry is the one a consumer
        // falls back to when it knows nothing better.
        if (active is { } scoped)
        {
            reachable.Add(scoped);
        }

        for (var i = 0; i < summaries.Count; i++)
        {
            var summary = summaries[i];
            if (summary.TenantId is not { Length: > 0 } tenantId
                || summary.Status != ExplorerTenantLifecycle.Active)
            {
                continue;
            }

            var candidate = new ExplorerTenantId(tenantId);
            if (candidate != active)
            {
                reachable.Add(candidate);
            }
        }

        return Settle(reachable);
    }

    /// <summary>
    /// Returns the previous array when the answer is unchanged, and otherwise
    /// adopts <paramref name="reachable"/> as the new one.
    /// </summary>
    private ExplorerTenantId[] Settle(List<ExplorerTenantId> reachable)
    {
        if (Matches(reachable))
        {
            return _cached;
        }

        _cached = reachable.ToArray();
        return _cached;
    }

    private bool Matches(List<ExplorerTenantId> reachable)
    {
        if (_cached.Length != reachable.Count)
        {
            return false;
        }

        for (var i = 0; i < _cached.Length; i++)
        {
            if (_cached[i] != reachable[i])
            {
                return false;
            }
        }

        return true;
    }
}
