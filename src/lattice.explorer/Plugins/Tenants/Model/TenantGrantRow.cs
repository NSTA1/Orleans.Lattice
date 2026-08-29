using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// One cross-tenant grant projected for display, with its lifecycle state made
/// unmistakable.
/// <para>
/// <b>Only an active grant authorizes anything.</b> The two-step agreement means
/// a grant exists from the moment it is offered, so a row that showed only its
/// scope and operations would present a pending offer as live access - the exact
/// mistake the two-step design exists to prevent. Every row therefore carries
/// three separate statements of the same fact: a <see cref="StateLabel"/>, an
/// <see cref="AuthorityText"/> that says in words what the grant does and does
/// not currently permit, and <see cref="Authorizes"/> for anything that gates on
/// it. There is no way to render this row without saying which state it is in.
/// </para>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, and every text field is a compile-time constant
/// chosen by a switch, so a grant list allocates one array and no strings.
/// </para>
/// </summary>
public readonly record struct TenantGrantRow
{
    /// <summary>The grant as the seam reported it, carrying its authoritative state.</summary>
    public required ExplorerTenantGrant Grant { get; init; }

    /// <summary>Which side of the agreement the tenant in view is on.</summary>
    public required TenantGrantDirection Direction { get; init; }

    /// <summary>The grant's stable id.</summary>
    public string GrantId => Grant.GrantId;

    /// <summary>The tenant that offered a scope of its own data.</summary>
    public string GranterTenantId => Grant.GranterTenantId;

    /// <summary>The tenant the grant was offered to.</summary>
    public string GranteeTenantId => Grant.GranteeTenantId;

    /// <summary>The scope of the granting tenant's data the grant covers.</summary>
    public string Scope => Grant.Scope;

    /// <summary>The grant's lifecycle state.</summary>
    public ExplorerTenantGrantState State => Grant.State;

    /// <summary>
    /// Whether the grant currently authorizes the operations it names. Gate every
    /// "this tenant can reach that data" affordance on this, never on the grant's
    /// presence.
    /// </summary>
    public bool Authorizes => Grant.AuthorizesAccess;

    /// <summary>The grant's lifecycle state as an unambiguous display label.</summary>
    public string StateLabel => State switch
    {
        ExplorerTenantGrantState.Active => "Active",
        ExplorerTenantGrantState.Pending => "Pending approval",
        ExplorerTenantGrantState.Rejected => "Rejected",
        _ => "Revoked",
    };

    /// <summary>
    /// The state's modifier class, so an active grant is visually distinct from
    /// one that authorizes nothing rather than differing only in wording.
    /// </summary>
    public string StateClass => State switch
    {
        ExplorerTenantGrantState.Active => "is-active",
        ExplorerTenantGrantState.Pending => "is-pending",
        _ => "is-closed",
    };

    /// <summary>
    /// What the grant does and does not currently permit, in words. Rendered
    /// beside the state label so an operator reads the consequence and not just
    /// the state name.
    /// <para>
    /// Every branch is a compile-time literal rather than a concatenation, so a
    /// grant list costs no string per row per render.
    /// </para>
    /// </summary>
    public string AuthorityText => State switch
    {
        ExplorerTenantGrantState.Active => Grant.Operations switch
        {
            ExplorerTenantGrantAccess.ReadWrite => "Authorizes read and write now.",
            ExplorerTenantGrantAccess.Read => "Authorizes read now.",
            ExplorerTenantGrantAccess.Write => "Authorizes write now.",
            _ => "Authorizes no operations at all, though it is active.",
        },
        ExplorerTenantGrantState.Pending =>
            "Authorizes nothing yet. Offered, and awaiting the grantee tenant's approval.",
        ExplorerTenantGrantState.Rejected => "Authorizes nothing. The grantee declined the offer.",
        _ => "Authorizes nothing. The grant was withdrawn.",
    };

    /// <summary>The operations the grant names, whether or not it currently authorizes them.</summary>
    public string OperationsText => Grant.Operations switch
    {
        ExplorerTenantGrantAccess.ReadWrite => "read and write",
        ExplorerTenantGrantAccess.Read => "read",
        ExplorerTenantGrantAccess.Write => "write",
        _ => "no operations",
    };

    /// <summary>
    /// Whether the grantee may still approve or reject. Only a pending grant can
    /// be answered.
    /// </summary>
    public bool CanAnswer => State == ExplorerTenantGrantState.Pending;

    /// <summary>
    /// Whether the grant may still be withdrawn. Only an active grant can be
    /// revoked; a pending one is declined and a closed one has nowhere to go.
    /// </summary>
    public bool CanRevoke => State == ExplorerTenantGrantState.Active;

    /// <summary>
    /// Whether the grant has reached a terminal state, so no transition remains.
    /// </summary>
    public bool IsClosed => Grant.IsClosed;

    /// <summary>Projects <paramref name="grant"/> for display on <paramref name="direction"/>'s side.</summary>
    /// <param name="grant">The grant to project.</param>
    /// <param name="direction">Which side of the agreement the tenant in view is on.</param>
    /// <returns>The display row.</returns>
    public static TenantGrantRow From(ExplorerTenantGrant grant, TenantGrantDirection direction) =>
        new() { Grant = grant, Direction = direction };
}
