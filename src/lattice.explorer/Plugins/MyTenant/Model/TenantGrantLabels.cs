using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// The display vocabulary for a cross-tenant grant: the name of each lifecycle
/// state, the sentence saying what that state does and does not authorize, and
/// the modifier class the badge renders with.
/// <para>
/// Grant state is load-bearing, so it gets a first-class vocabulary rather than
/// an inline <c>ToString</c>: a pending grant authorizes nothing and must never
/// be presented as live access.
/// </para>
/// </summary>
/// <remarks>
/// Every member returns an interned literal, so a grant list may call them per
/// row per render without allocating.
/// </remarks>
public static class TenantGrantLabels
{
    /// <summary>The name of <paramref name="state"/>.</summary>
    /// <param name="state">The grant state to name.</param>
    /// <returns>The state's display label.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="state"/> is not a declared state.
    /// </exception>
    public static string Label(ExplorerTenantGrantState state) => state switch
    {
        ExplorerTenantGrantState.Active => "Active",
        ExplorerTenantGrantState.Pending => "Pending",
        ExplorerTenantGrantState.Rejected => "Rejected",
        ExplorerTenantGrantState.Revoked => "Revoked",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unknown grant state."),
    };

    /// <summary>
    /// What <paramref name="state"/> authorizes, said plainly. Only the active
    /// state authorizes anything, and every other sentence says so.
    /// </summary>
    /// <param name="state">The grant state to describe.</param>
    /// <returns>The state's description.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="state"/> is not a declared state.
    /// </exception>
    public static string Description(ExplorerTenantGrantState state) => state switch
    {
        ExplorerTenantGrantState.Active => "Live: this grant authorizes access now.",
        ExplorerTenantGrantState.Pending =>
            "Offered but not accepted. This grant authorizes nothing until the receiving tenant approves it.",
        ExplorerTenantGrantState.Rejected =>
            "Declined by the receiving tenant. Closed, and authorizes nothing.",
        ExplorerTenantGrantState.Revoked =>
            "Withdrawn. Closed, and authorizes nothing.",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unknown grant state."),
    };

    /// <summary>
    /// The badge's modifier class for <paramref name="state"/>, so only a live
    /// grant reads as live.
    /// </summary>
    /// <param name="state">The grant state to classify.</param>
    /// <returns>The badge class.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="state"/> is not a declared state.
    /// </exception>
    public static string BadgeClass(ExplorerTenantGrantState state) => state switch
    {
        ExplorerTenantGrantState.Active => "lxm-grant-state is-active",
        ExplorerTenantGrantState.Pending => "lxm-grant-state is-pending",
        ExplorerTenantGrantState.Rejected => "lxm-grant-state is-closed",
        ExplorerTenantGrantState.Revoked => "lxm-grant-state is-closed",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unknown grant state."),
    };

    /// <summary>
    /// The operations <paramref name="operations"/> names, for display.
    /// </summary>
    /// <param name="operations">The operations the grant covers.</param>
    /// <returns>The operations' display label.</returns>
    public static string Operations(ExplorerTenantGrantAccess operations) => operations switch
    {
        ExplorerTenantGrantAccess.None => "None",
        ExplorerTenantGrantAccess.Read => "Read",
        ExplorerTenantGrantAccess.Write => "Write",
        ExplorerTenantGrantAccess.ReadWrite => "Read and write",
        _ => "Read and write",
    };

    /// <summary>
    /// The operations <paramref name="operations"/> names, in lower case, for
    /// use mid-sentence. Returns an interned literal, so a grant list may call
    /// it per row per render without allocating - unlike lower-casing the label
    /// at the call site, which allocates a string each time.
    /// </summary>
    /// <param name="operations">The operations the grant covers.</param>
    /// <returns>The operations' mid-sentence label.</returns>
    public static string OperationsInSentence(ExplorerTenantGrantAccess operations) => operations switch
    {
        ExplorerTenantGrantAccess.None => "no",
        ExplorerTenantGrantAccess.Read => "read",
        ExplorerTenantGrantAccess.Write => "write",
        ExplorerTenantGrantAccess.ReadWrite => "read and write",
        _ => "read and write",
    };

    /// <summary>
    /// The side of the agreement <paramref name="direction"/> names, for the
    /// counterparty column's label.
    /// </summary>
    /// <param name="direction">The direction to label.</param>
    /// <returns>The direction's display label.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="direction"/> is not a declared direction.
    /// </exception>
    public static string Label(TenantGrantDirection direction) => direction switch
    {
        TenantGrantDirection.Outbound => "Offered to",
        TenantGrantDirection.Inbound => "Offered by",
        TenantGrantDirection.Unrelated => "Between other tenants",
        _ => throw new ArgumentOutOfRangeException(nameof(direction), direction, "Unknown direction."),
    };
}
