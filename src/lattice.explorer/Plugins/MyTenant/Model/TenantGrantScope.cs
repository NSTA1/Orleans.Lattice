using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The plugin's tenant-isolation guard: the single place that decides, from the
/// caller's <em>active</em> tenant and a grant's own parties and state, which
/// half of the two-step agreement the caller may drive.
/// <para>
/// This is what makes "an admin of tenant A can never read or mutate tenant B
/// through this plugin" a property of the code rather than of the markup. Every
/// grant action in the plugin routes through <see cref="Authorize"/> before a
/// call leaves the process, so an admin of A cannot approve a grant offered to
/// B, and cannot offer a grant from B, whatever a view hands it.
/// </para>
/// <para>
/// It is advisory in the sense the epic's D6 means: the cluster re-enforces every
/// transition and remains the enforcement point. What this guard removes is the
/// possibility of the plugin <em>asking</em> for something the caller has no
/// standing to ask for.
/// </para>
/// </summary>
/// <remarks>
/// Every member is a pure function of its arguments with ordinal comparison and
/// no allocation, so a view may call it per grant per render.
/// </remarks>
public static class TenantGrantScope
{
    /// <summary>
    /// The refusal message for an action on a grant neither of whose parties is
    /// the active tenant.
    /// </summary>
    public const string UnrelatedGrantMessage =
        "That grant is between two other tenants. This surface only administers your own tenant's "
        + "side of a cross-tenant agreement.";

    /// <summary>
    /// The refusal message for an attempt to drive the grantee's half of the
    /// agreement from the granting tenant.
    /// </summary>
    public const string NotGranteeMessage =
        "Only the tenant a grant was offered to can approve or reject it. Your tenant offered this "
        + "grant, so it stays pending until the other tenant accepts it.";

    /// <summary>
    /// The refusal message for offering a grant on some other tenant's data.
    /// </summary>
    public const string NotGranterMessage =
        "A grant can only be offered on your own tenant's data.";

    /// <summary>
    /// The refusal message for a transition the grant's current state does not
    /// admit.
    /// </summary>
    public const string WrongStateMessage =
        "That grant is not in a state this action can be applied from.";

    /// <summary>
    /// Which side of <paramref name="grant"/> the tenant
    /// <paramref name="activeTenantId"/> is on.
    /// </summary>
    /// <param name="activeTenantId">
    /// The caller's active tenant id, or <see langword="null"/> when none is
    /// established - in which case the grant is
    /// <see cref="TenantGrantDirection.Unrelated"/>, because a caller with no
    /// tenant has no side.
    /// </param>
    /// <param name="grant">The grant to place.</param>
    /// <returns>The direction the grant runs relative to the active tenant.</returns>
    public static TenantGrantDirection Direction(string? activeTenantId, in ExplorerTenantGrant grant)
    {
        if (string.IsNullOrEmpty(activeTenantId))
        {
            return TenantGrantDirection.Unrelated;
        }

        // Granter is checked first so a self-grant - a tenant naming itself on
        // both sides - resolves deterministically to outbound rather than
        // offering its own approval.
        if (string.Equals(grant.GranterTenantId, activeTenantId, StringComparison.Ordinal))
        {
            return TenantGrantDirection.Outbound;
        }

        return string.Equals(grant.GranteeTenantId, activeTenantId, StringComparison.Ordinal)
            ? TenantGrantDirection.Inbound
            : TenantGrantDirection.Unrelated;
    }

    /// <summary>
    /// The transitions <paramref name="activeTenantId"/> may drive on
    /// <paramref name="grant"/>.
    /// </summary>
    /// <param name="activeTenantId">The caller's active tenant id, or <see langword="null"/>.</param>
    /// <param name="grant">The grant to classify.</param>
    /// <returns>
    /// The permitted transitions, or <see cref="TenantGrantActions.None"/> when
    /// the active tenant has no standing on this grant or the grant's state
    /// admits nothing.
    /// </returns>
    public static TenantGrantActions Available(string? activeTenantId, in ExplorerTenantGrant grant)
    {
        var direction = Direction(activeTenantId, grant);
        if (direction == TenantGrantDirection.Unrelated)
        {
            return TenantGrantActions.None;
        }

        return grant.State switch
        {
            // Only the grantee closes the offer, either way. A granting tenant
            // approving its own offer would make the second step of a two-step
            // agreement a formality.
            ExplorerTenantGrantState.Pending when direction == TenantGrantDirection.Inbound =>
                TenantGrantActions.Approve | TenantGrantActions.Reject,

            // Either party may withdraw a live grant, so neither is trapped.
            ExplorerTenantGrantState.Active => TenantGrantActions.Revoke,

            _ => TenantGrantActions.None,
        };
    }

    /// <summary>
    /// Whether <paramref name="activeTenantId"/> may drive
    /// <paramref name="action"/> on <paramref name="grant"/>.
    /// </summary>
    /// <param name="activeTenantId">The caller's active tenant id, or <see langword="null"/>.</param>
    /// <param name="grant">The grant to act on.</param>
    /// <param name="action">
    /// The single transition being attempted. A combination of flags is refused,
    /// because an action is one transition.
    /// </param>
    /// <returns><see langword="true"/> when the action is permitted.</returns>
    public static bool Allows(
        string? activeTenantId,
        in ExplorerTenantGrant grant,
        TenantGrantActions action) =>
        action is TenantGrantActions.Approve
            or TenantGrantActions.Reject
            or TenantGrantActions.Revoke
        && Available(activeTenantId, grant).HasFlag(action);

    /// <summary>
    /// Authorizes <paramref name="action"/> on <paramref name="grant"/>, naming
    /// the specific reason when it is refused so the surface can say what is
    /// actually wrong.
    /// </summary>
    /// <param name="activeTenantId">The caller's active tenant id, or <see langword="null"/>.</param>
    /// <param name="grant">The grant to act on.</param>
    /// <param name="action">The single transition being attempted.</param>
    /// <param name="refusal">
    /// The refusal message when the action is not permitted, and
    /// <see langword="null"/> when it is.
    /// </param>
    /// <returns><see langword="true"/> when the action is permitted.</returns>
    public static bool Authorize(
        string? activeTenantId,
        in ExplorerTenantGrant grant,
        TenantGrantActions action,
        out string? refusal)
    {
        if (Allows(activeTenantId, grant, action))
        {
            refusal = null;
            return true;
        }

        refusal = Direction(activeTenantId, grant) switch
        {
            TenantGrantDirection.Unrelated => UnrelatedGrantMessage,
            TenantGrantDirection.Outbound when action is TenantGrantActions.Approve
                or TenantGrantActions.Reject => NotGranteeMessage,
            _ => WrongStateMessage,
        };

        return false;
    }

    /// <summary>
    /// Whether <paramref name="activeTenantId"/> may offer a grant on
    /// <paramref name="granterTenantId"/>'s data. Only a tenant's own data may
    /// be offered.
    /// </summary>
    /// <param name="activeTenantId">The caller's active tenant id, or <see langword="null"/>.</param>
    /// <param name="granterTenantId">The tenant whose data would be shared.</param>
    /// <returns><see langword="true"/> when the offer is permitted.</returns>
    public static bool AllowsOffer(string? activeTenantId, string? granterTenantId) =>
        !string.IsNullOrEmpty(activeTenantId)
        && string.Equals(granterTenantId, activeTenantId, StringComparison.Ordinal);
}
