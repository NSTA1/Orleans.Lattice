using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ITenantAccessDataPurge"/>. Composes the two engine-side
/// purges: the policy store's tenant-tier rule purge
/// (<see cref="ITenantPolicyRuleStore.PurgeTenantRulesAsync"/>) and the
/// membership directory's tenant purge
/// (<see cref="ITenantScopedMembershipStore.PurgeTenantAsync"/>). Each is
/// idempotent and resumable on its own, so the composition is too: a crash
/// between the two leaves the rules gone and the groups in place, and the next
/// run removes no rules and finishes the groups.
/// </summary>
/// <remarks>
/// <para>
/// <b>Order.</b> Rules go first. A crash after the rule purge leaves groups that
/// no tenant rule grants anything to, which is strictly less access than before;
/// the opposite order could leave rules naming a group id that a later
/// recreation would silently inherit.
/// </para>
/// <para>
/// <b>Fail closed.</b> The stores are resolved when the purge runs, not when the
/// registry is constructed, so a host that replaced the membership directory or
/// the policy store keeps a working registry but cannot delete a tenant: the
/// resolution throws <see cref="InvalidOperationException"/> and the record stays
/// in place, rather than the delete silently orphaning access data the purge
/// cannot reach.
/// </para>
/// </remarks>
/// <param name="purgeRules">Removes the tenant's tenant-tier rules and returns how many it removed. Must not be <c>null</c>.</param>
/// <param name="purgeMembership">Removes the tenant's groups and their edges and returns what it removed. Must not be <c>null</c>.</param>
internal sealed class TenantAccessDataPurge(
    Func<TenantId, CancellationToken, Task<int>> purgeRules,
    Func<TenantId, CancellationToken, Task<TenantMembershipPurgeResult>> purgeMembership) : ITenantAccessDataPurge
{
    /// <summary>The tenant-tier rule purge step.</summary>
    internal Func<TenantId, CancellationToken, Task<int>> PurgeRules { get; } =
        purgeRules ?? throw new ArgumentNullException(nameof(purgeRules));

    /// <summary>The tenant group and edge purge step.</summary>
    internal Func<TenantId, CancellationToken, Task<TenantMembershipPurgeResult>> PurgeMembership { get; } =
        purgeMembership ?? throw new ArgumentNullException(nameof(purgeMembership));

    /// <summary>
    /// Creates the purge over the stores registered in <paramref name="services"/>:
    /// the auth add-on's <see cref="ITenantPolicyRuleStore"/> and the membership
    /// add-on's <see cref="ITenantScopedMembershipStore"/>, both resolved on each
    /// purge.
    /// </summary>
    /// <param name="services">The silo service provider. Must not be <c>null</c>.</param>
    /// <returns>The purge.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    internal static TenantAccessDataPurge FromServices(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return new TenantAccessDataPurge(
            (tenant, ct) => services.GetRequiredService<ITenantPolicyRuleStore>().PurgeTenantRulesAsync(tenant, ct),
            (tenant, ct) => services.GetTenantScopedMembershipStore().PurgeTenantAsync(tenant, ct));
    }

    /// <inheritdoc />
    public async Task<TenantAccessPurgeResult> PurgeAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        if (tenant.Value is null)
        {
            throw new ArgumentException(
                "The uninitialised 'no tenant' value cannot address a tenant's access data.",
                nameof(tenant));
        }

        // The reserved default tenant can own no tenant group (t/default/... is
        // refused) and no tenant-tier rule (tenant:default:... is refused), and both
        // engine purges refuse it, so there is nothing to remove.
        if (tenant.IsDefault)
        {
            return default;
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Infrastructure acting for an already-authorized delete: both engine purges
        // enter system origin themselves, and entering it here as well keeps the
        // whole step under it whichever store implementation runs.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var rulesRemoved = await PurgeRules(tenant, cancellationToken).ConfigureAwait(false);
            var membership = await PurgeMembership(tenant, cancellationToken).ConfigureAwait(false);
            return new TenantAccessPurgeResult(rulesRemoved, membership.GroupsRemoved, membership.EdgesRemoved);
        }
    }
}
