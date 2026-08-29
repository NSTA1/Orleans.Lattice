using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.MyTenant.Workspace;

/// <summary>
/// The Sharing surface: this tenant's own side of the two-step cross-tenant
/// grant agreement.
/// <para>
/// Outbound, the tenant offers a scope of its own data to another tenant and may
/// withdraw a live grant. Inbound, it receives offers and decides them - and
/// that inbound approval is the step that makes a grant authorize anything at
/// all, so it is the surface's primary call to action.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// This is the only surface that names a second tenant, so it is the one where
/// tenant isolation has to be enforced rather than assumed. Every transition
/// routes through <see cref="TenantGrantScope"/> against
/// <see cref="TenantId"/> before a call leaves the process: an admin of tenant A
/// can neither approve a grant offered to B nor offer a grant from B, and a
/// grant between two other tenants offers no action at all.
/// </para>
/// <para>
/// Grant state travels with every row and is rendered explicitly. A pending
/// grant authorizes nothing, and the surface never lets one read as live access.
/// </para>
/// </remarks>
public sealed partial class MyTenantWorkspace
{
    private static readonly IReadOnlyList<TenantGrantRow> NoGrants = Array.Empty<TenantGrantRow>();

    /// <summary>The refusal shown when the offer form carries no grantee tenant.</summary>
    public const string EmptyGranteeRefusal = "Enter the tenant to offer the grant to.";

    /// <summary>The refusal shown when the offer form carries no scope.</summary>
    public const string EmptyScopeRefusal = "Enter the scope the grant covers.";

    /// <summary>The refusal shown when the offer form names this tenant as the grantee.</summary>
    public const string SelfGrantRefusal =
        "A tenant does not need a grant to reach its own data. Name a different tenant.";

    /// <summary>The refusal shown when the offer form selects no operations.</summary>
    public const string NoOperationsRefusal =
        "Select at least one operation the grant will authorize once it is active.";

    private TenantGrantRow[] _inbound = [];
    private TenantGrantRow[] _outbound = [];
    private bool _grantsLoaded;

    /// <summary>
    /// Grants offered <em>to</em> this tenant: the inbox. Approving one here is
    /// what makes it live.
    /// </summary>
    public IReadOnlyList<TenantGrantRow> Inbound => _inbound;

    /// <summary>Grants this tenant has offered to others.</summary>
    public IReadOnlyList<TenantGrantRow> Outbound => _outbound;

    /// <summary>
    /// How many inbound offers are waiting on this tenant's decision. The
    /// surface leads with this count, because an unanswered offer authorizes
    /// nothing and is easy to miss.
    /// </summary>
    public int PendingInboundCount { get; private set; }

    /// <summary>Whether any inbound offer is waiting on a decision.</summary>
    public bool HasPendingInbound => PendingInboundCount > 0;

    /// <summary>The tenant the offer form would grant access to.</summary>
    public string OfferGranteeTenantId { get; set; } = string.Empty;

    /// <summary>The scope the offer form would cover.</summary>
    public string OfferScope { get; set; } = string.Empty;

    /// <summary>The operations the offer form would authorize once the grant is active.</summary>
    public ExplorerTenantGrantAccess OfferOperations { get; set; } = ExplorerTenantGrantAccess.Read;

    /// <summary>
    /// Offers a grant on <em>this</em> tenant's data to another tenant, creating
    /// it pending. It authorizes nothing until the other tenant approves it,
    /// which is the whole point of the two-step agreement.
    /// </summary>
    public async Task OfferGrantAsync()
    {
        if (!Allowed || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        var grantee = OfferGranteeTenantId.Trim();
        if (grantee.Length == 0)
        {
            Refuse(TenantOperationStatus.InvalidRequest, EmptyGranteeRefusal);
            return;
        }

        var scope = OfferScope.Trim();
        if (scope.Length == 0)
        {
            Refuse(TenantOperationStatus.InvalidRequest, EmptyScopeRefusal);
            return;
        }

        if (OfferOperations == ExplorerTenantGrantAccess.None)
        {
            Refuse(TenantOperationStatus.InvalidRequest, NoOperationsRefusal);
            return;
        }

        var granter = TenantId;
        if (string.Equals(grantee, granter, StringComparison.Ordinal))
        {
            Refuse(TenantOperationStatus.InvalidRequest, SelfGrantRefusal);
            return;
        }

        // The isolation invariant on the outbound side: a grant may only be
        // offered on the active tenant's own data. The granter is this tenant by
        // construction here, and the guard proves it rather than trusting the
        // construction.
        if (!TenantGrantScope.AllowsOffer(TenantId, granter))
        {
            Refuse(TenantOperationStatus.Denied, TenantGrantScope.NotGranterMessage);
            return;
        }

        var operations = OfferOperations;
        var succeeded = await RunAsync(
            () => _domain.Tenants.OfferGrantAsync(granter, grantee, scope, operations))
            .ConfigureAwait(false);

        if (succeeded)
        {
            OfferGranteeTenantId = string.Empty;
            OfferScope = string.Empty;
            await ReloadGrantsAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Approves an inbound offer, which is the step that makes it authorize.
    /// Refused client-side unless this tenant is the grantee and the grant is
    /// pending.
    /// </summary>
    /// <param name="row">The grant row to approve.</param>
    public Task ApproveGrantAsync(TenantGrantRow row) =>
        TransitionAsync(row, TenantGrantActions.Approve);

    /// <summary>
    /// Declines an inbound offer, closing it terminally. Refused client-side
    /// unless this tenant is the grantee and the grant is pending.
    /// </summary>
    /// <param name="row">The grant row to reject.</param>
    public Task RejectGrantAsync(TenantGrantRow row) =>
        TransitionAsync(row, TenantGrantActions.Reject);

    /// <summary>
    /// Withdraws a live grant, closing it terminally. Permitted to either party,
    /// so neither side is trapped in the agreement, but only while the grant is
    /// active.
    /// </summary>
    /// <param name="row">The grant row to revoke.</param>
    public Task RevokeGrantAsync(TenantGrantRow row) =>
        TransitionAsync(row, TenantGrantActions.Revoke);

    /// <summary>Re-reads this tenant's grants in both directions.</summary>
    public Task ReloadGrantsAsync() => LoadGrantsAsync(force: true);

    private async Task TransitionAsync(TenantGrantRow row, TenantGrantActions action)
    {
        if (!Allowed || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        // The isolation invariant. Nothing below this line runs for a grant the
        // active tenant has no standing on, so an admin of A cannot approve a
        // grant offered to B, and cannot revoke one between two other tenants,
        // whatever row a view hands over.
        if (!TenantGrantScope.Authorize(TenantId, row.Grant, action, out var refusal))
        {
            Refuse(
                TenantOperationStatus.Denied,
                refusal ?? TenantGrantScope.UnrelatedGrantMessage,
                action == TenantGrantActions.Revoke ? MyTenantNotice.GrantTransitionGuidance : null);
            return;
        }

        var granter = row.Grant.GranterTenantId;
        var grantee = row.Grant.GranteeTenantId;
        var scope = row.Grant.Scope;

        var succeeded = await RunAsync(() => action switch
        {
            TenantGrantActions.Approve => _domain.Tenants.ApproveGrantAsync(granter, grantee, scope),
            TenantGrantActions.Reject => _domain.Tenants.RejectGrantAsync(granter, grantee, scope),
            _ => _domain.Tenants.RevokeGrantAsync(granter, grantee, scope),
        }).ConfigureAwait(false);

        if (succeeded)
        {
            await ReloadGrantsAsync().ConfigureAwait(false);
        }
    }

    private async Task LoadGrantsAsync(bool force)
    {
        if ((!force && _grantsLoaded) || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        _grantsLoaded = true;

        var grants = await _domain.Tenants.ListGrantsAsync(TenantId).ConfigureAwait(false);
        if (grants.IsSuccess && grants.Value is { } report)
        {
            Project(report);
            return;
        }

        ClearGrants();
        LastNotice = MyTenantNotice.For(grants);
    }

    /// <summary>
    /// Projects the cluster's two grant lists into rows, resolving each one's
    /// direction and permitted transitions against the active tenant.
    /// </summary>
    /// <remarks>
    /// The cluster's own lists are used as the direction hint only in that they
    /// are read separately; the direction on each row is still derived from the
    /// grant's own parties, so a row can never claim a side the grant does not
    /// support.
    /// </remarks>
    private void Project(ExplorerTenantGrants report)
    {
        _outbound = Project(report.Issued, _outbound);
        _inbound = Project(report.Received, _inbound);

        var pending = 0;
        for (var i = 0; i < _inbound.Length; i++)
        {
            if (_inbound[i].NeedsDecision)
            {
                pending++;
            }
        }

        PendingInboundCount = pending;
    }

    private TenantGrantRow[] Project(IReadOnlyList<ExplorerTenantGrant> grants, TenantGrantRow[] buffer)
    {
        if (grants.Count == 0)
        {
            return [];
        }

        // Sized exactly, and reused whenever a refresh returns the same number of
        // grants, so a steady-state reload writes rows rather than allocating a
        // list per poll.
        var rows = buffer.Length == grants.Count ? buffer : new TenantGrantRow[grants.Count];
        for (var i = 0; i < grants.Count; i++)
        {
            rows[i] = TenantGrantRow.For(TenantId, grants[i]);
        }

        return rows;
    }

    private void ClearGrants()
    {
        _inbound = [];
        _outbound = [];
        PendingInboundCount = 0;
    }
}
