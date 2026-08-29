using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants.Workspace;

/// <summary>
/// The tenant-access surface: the subjects holding tenant-admin authority over
/// the selected tenant, and its cross-tenant grants in both directions and every
/// lifecycle state.
/// <para>
/// A cross-tenant grant is a <b>two-step agreement</b>, so a grant exists from
/// the moment it is offered and authorizes nothing until the grantee approves.
/// Every row therefore carries its state explicitly through
/// <see cref="TenantGrantRow"/>, and every transition is checked against the
/// state the surface already holds before it is sent - so an operator is told
/// "that grant is pending, not active" here rather than receiving a precondition
/// failure the wire has stripped of its reason.
/// </para>
/// <para>
/// The same is done for the last admin subject: a tenant must always keep one,
/// and the surface knows how many there are, so it refuses locally with the rule
/// rather than round-tripping for a generic refusal.
/// </para>
/// </summary>
public sealed partial class TenantsWorkspace
{
    /// <summary>The message shown when the access surface has no tenant selected.</summary>
    public const string AccessNeedsTenantMessage =
        "Select a tenant to administer its admin subjects and cross-tenant grants.";

    /// <summary>The message shown when the subject to add is blank.</summary>
    public const string SubjectRequiredMessage = "Enter the id of the subject to grant admin authority to.";

    /// <summary>The message shown when the offer form is incomplete.</summary>
    public const string OfferIncompleteMessage =
        "A cross-tenant grant needs the other tenant's id and the scope it covers.";

    /// <summary>The message shown when an offer names no operations.</summary>
    public const string OfferNeedsOperationsMessage =
        "A grant that authorizes no operations would do nothing. Choose read, write, or both.";

    private static readonly IReadOnlyList<string> NoSubjects = Array.Empty<string>();

    private static readonly IReadOnlyList<TenantGrantRow> NoGrants = Array.Empty<TenantGrantRow>();

    private readonly List<TenantGrantRow> _issuedGrants = [];
    private readonly List<TenantGrantRow> _receivedGrants = [];

    private bool _accessLoaded;

    /// <summary>
    /// The subjects holding tenant-admin authority over the selected tenant, in
    /// ordinal order.
    /// </summary>
    public IReadOnlyList<string> AdminSubjects { get; private set; } = NoSubjects;

    /// <summary>The subject id to grant admin authority to, as typed into the surface.</summary>
    public string AddAdminSubjectId { get; set; } = string.Empty;

    /// <summary>
    /// Grants the selected tenant offered over its own data, in every state.
    /// </summary>
    public IReadOnlyList<TenantGrantRow> IssuedGrants => _accessLoaded ? _issuedGrants : NoGrants;

    /// <summary>
    /// Grants other tenants offered to the selected tenant, in every state. The
    /// pending entries are its approval inbox.
    /// </summary>
    public IReadOnlyList<TenantGrantRow> ReceivedGrants => _accessLoaded ? _receivedGrants : NoGrants;

    /// <summary>
    /// Whether the operator is offering a grant <em>from</em> the selected tenant
    /// (its data, offered outward) rather than <em>to</em> it. An operator acts on
    /// either tenant's behalf, so both directions can be offered from here.
    /// </summary>
    public bool OfferFromSelectedTenant { get; set; } = true;

    /// <summary>The counterparty tenant id for the offer form.</summary>
    public string OfferCounterpartyTenantId { get; set; } = string.Empty;

    /// <summary>The scope of the granting tenant's data the offer covers.</summary>
    public string OfferScope { get; set; } = string.Empty;

    /// <summary>The operations the offered grant will authorize once active.</summary>
    public ExplorerTenantGrantAccess OfferOperations { get; set; } = ExplorerTenantGrantAccess.Read;

    /// <summary>
    /// The number of received grants awaiting the selected tenant's answer, so
    /// the surface can flag a non-empty inbox without the operator scanning for
    /// it.
    /// </summary>
    public int PendingReceivedCount
    {
        get
        {
            var pending = 0;
            for (var i = 0; i < _receivedGrants.Count; i++)
            {
                if (_receivedGrants[i].CanAnswer)
                {
                    pending++;
                }
            }

            return pending;
        }
    }

    /// <summary>
    /// Whether removing an admin subject is possible at all. A tenant must keep
    /// at least one, so the last one's removal control renders disabled with the
    /// rule beside it rather than offering an action the cluster will refuse.
    /// </summary>
    public bool CanRemoveAdminSubject => AdminSubjects.Count > 1;

    /// <summary>Re-reads the selected tenant's admin subjects and cross-tenant grants.</summary>
    public async Task RefreshAccessAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            await LoadAccessAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Grants <see cref="AddAdminSubjectId"/> tenant-admin authority over the
    /// selected tenant. Additive, so it runs directly.
    /// </summary>
    public async Task AddAdminSubjectAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            Report(TenantOperationStatus.InvalidRequest, AccessNeedsTenantMessage);
            RaiseChanged();
            return;
        }

        var subjectId = AddAdminSubjectId.Trim();
        if (subjectId.Length == 0)
        {
            Report(TenantOperationStatus.InvalidRequest, SubjectRequiredMessage);
            RaiseChanged();
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            var added = await _domain.Tenants.AddAdminSubjectAsync(tenantId, subjectId).ConfigureAwait(false);
            if (!added.IsSuccess)
            {
                Report(added.Status, TenantRefusal.DescribeAdminChange(added));
                return;
            }

            AddAdminSubjectId = string.Empty;
            AdminSubjects = added.Value?.Subjects ?? NoSubjects;
            Report(
                TenantOperationStatus.Succeeded,
                added.Value is { Changed: true }
                    ? "Granted " + subjectId + " admin authority over " + tenantId + "."
                    : subjectId + " already held admin authority over " + tenantId + "; nothing changed.");
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Asks to revoke <paramref name="subjectId"/>'s admin authority, holding the
    /// request for an explicit confirmation. Removing the tenant's last admin
    /// subject is refused here rather than sent, because the surface already
    /// knows how many there are and the cluster's refusal would arrive stripped
    /// of its reason.
    /// </summary>
    /// <param name="subjectId">The subject whose authority to revoke.</param>
    public void RequestRemoveAdminSubject(string subjectId)
    {
        ArgumentNullException.ThrowIfNull(subjectId);

        if (!Allowed || Busy)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            Report(TenantOperationStatus.InvalidRequest, AccessNeedsTenantMessage);
            RaiseChanged();
            return;
        }

        if (!CanRemoveAdminSubject)
        {
            Report(
                TenantOperationStatus.LastAdminSubject,
                "Cannot remove " + subjectId + ": it is the only admin subject on " + tenantId + ". "
                    + TenantRefusal.LastAdminSubjectRule);
            RaiseChanged();
            return;
        }

        Confirmation = new TenantConfirmation
        {
            Kind = TenantConfirmationKind.RemoveAdminSubject,
            TenantId = tenantId,
            Target = subjectId,
            Title = "Revoke admin authority?",
            Body = "This removes " + subjectId + "'s tenant-admin authority over " + tenantId
                + ". They will no longer be able to administer the tenant or see it on the "
                + "self-service surface.",
            ConfirmLabel = "Revoke authority",
        };

        ClearResult();
        RaiseChanged();
    }

    /// <summary>
    /// Offers a cross-tenant grant from the offer form, creating it
    /// <b>pending</b>. It authorizes nothing until the grantee approves.
    /// </summary>
    public async Task OfferGrantAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            Report(TenantOperationStatus.InvalidRequest, AccessNeedsTenantMessage);
            RaiseChanged();
            return;
        }

        var counterparty = OfferCounterpartyTenantId.Trim();
        var scope = OfferScope.Trim();
        if (counterparty.Length == 0 || scope.Length == 0)
        {
            Report(TenantOperationStatus.InvalidRequest, OfferIncompleteMessage);
            RaiseChanged();
            return;
        }

        if (OfferOperations == ExplorerTenantGrantAccess.None)
        {
            Report(TenantOperationStatus.InvalidRequest, OfferNeedsOperationsMessage);
            RaiseChanged();
            return;
        }

        var granter = OfferFromSelectedTenant ? tenantId : counterparty;
        var grantee = OfferFromSelectedTenant ? counterparty : tenantId;

        ClearResult();
        BeginBusy();
        try
        {
            var offered = await _domain.Tenants
                .OfferGrantAsync(granter, grantee, scope, OfferOperations)
                .ConfigureAwait(false);

            if (!offered.IsSuccess)
            {
                Report(offered.Status, TenantRefusal.DescribeGrantTransition(offered));
                return;
            }

            OfferCounterpartyTenantId = string.Empty;
            OfferScope = string.Empty;
            Report(TenantOperationStatus.Succeeded, DescribeTransition(offered.Value, "offered"));
            await LoadAccessAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Approves <paramref name="row"/>, so it begins to authorize. Only a pending
    /// grant can be approved; anything else is refused here with that reason.
    /// Additive, so it runs directly.
    /// </summary>
    /// <param name="row">The grant to approve.</param>
    public async Task ApproveGrantAsync(TenantGrantRow row)
    {
        if (!Allowed || Busy)
        {
            return;
        }

        if (!row.CanAnswer)
        {
            ReportNotPending(row, "approved");
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            var approved = await _domain.Tenants
                .ApproveGrantAsync(row.GranterTenantId, row.GranteeTenantId, row.Scope)
                .ConfigureAwait(false);

            if (!approved.IsSuccess)
            {
                Report(approved.Status, TenantRefusal.DescribeGrantTransition(approved));
                return;
            }

            Report(TenantOperationStatus.Succeeded, DescribeTransition(approved.Value, "approved"));
            await LoadAccessAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    /// <summary>
    /// Asks to decline <paramref name="row"/>, holding the request for an
    /// explicit confirmation. Rejection is terminal: the granter must offer
    /// again.
    /// </summary>
    /// <param name="row">The grant to decline.</param>
    public void RequestRejectGrant(TenantGrantRow row)
    {
        if (!Allowed || Busy)
        {
            return;
        }

        if (!row.CanAnswer)
        {
            ReportNotPending(row, "rejected");
            return;
        }

        Confirmation = BuildGrantConfirmation(
            row,
            TenantConfirmationKind.RejectGrant,
            "Reject this grant offer?",
            "This declines " + row.GranterTenantId + "'s offer of " + row.OperationsText + " over "
                + row.Scope + " to " + row.GranteeTenantId
                + ". Rejection is terminal: the offer cannot be un-rejected, and "
                + row.GranterTenantId + " would have to offer it again.",
            "Reject offer");

        ClearResult();
        RaiseChanged();
    }

    /// <summary>
    /// Asks to withdraw <paramref name="row"/>, holding the request for an
    /// explicit confirmation. Only an active grant can be revoked, and doing so
    /// removes access the grantee currently has.
    /// </summary>
    /// <param name="row">The grant to withdraw.</param>
    public void RequestRevokeGrant(TenantGrantRow row)
    {
        if (!Allowed || Busy)
        {
            return;
        }

        if (!row.CanRevoke)
        {
            Report(
                TenantOperationStatus.GrantTransitionRejected,
                "That grant is " + row.StateLabel.ToLowerInvariant() + ", not active, so there is "
                    + "no live access to withdraw. Only an active grant can be revoked.");
            RaiseChanged();
            return;
        }

        Confirmation = BuildGrantConfirmation(
            row,
            TenantConfirmationKind.RevokeGrant,
            "Withdraw this grant?",
            "This withdraws " + row.GranteeTenantId + "'s " + row.OperationsText + " access to "
                + row.Scope + ", which " + row.GranterTenantId + " granted. The access stops "
                + "immediately and the grant is closed terminally.",
            "Withdraw grant");

        ClearResult();
        RaiseChanged();
    }

    private async Task RemoveAdminSubjectConfirmedAsync(string tenantId, string subjectId)
    {
        BeginBusy();
        try
        {
            var removed = await _domain.Tenants
                .RemoveAdminSubjectAsync(tenantId, subjectId)
                .ConfigureAwait(false);

            if (!removed.IsSuccess)
            {
                Report(removed.Status, TenantRefusal.DescribeAdminChange(removed));
                return;
            }

            AdminSubjects = removed.Value?.Subjects ?? NoSubjects;
            Report(
                TenantOperationStatus.Succeeded,
                removed.Value is { Changed: true }
                    ? "Revoked " + subjectId + "'s admin authority over " + tenantId + "."
                    : subjectId + " held no admin authority over " + tenantId + "; nothing changed.");
        }
        finally
        {
            EndBusy();
        }
    }

    private async Task RejectGrantConfirmedAsync(string granter, string grantee, string scope)
    {
        BeginBusy();
        try
        {
            var rejected = await _domain.Tenants
                .RejectGrantAsync(granter, grantee, scope)
                .ConfigureAwait(false);

            if (!rejected.IsSuccess)
            {
                Report(rejected.Status, TenantRefusal.DescribeGrantTransition(rejected));
                return;
            }

            Report(TenantOperationStatus.Succeeded, DescribeTransition(rejected.Value, "rejected"));
            await LoadAccessAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    private async Task RevokeGrantConfirmedAsync(string granter, string grantee, string scope)
    {
        BeginBusy();
        try
        {
            var revoked = await _domain.Tenants
                .RevokeGrantAsync(granter, grantee, scope)
                .ConfigureAwait(false);

            if (!revoked.IsSuccess)
            {
                Report(revoked.Status, TenantRefusal.DescribeGrantTransition(revoked));
                return;
            }

            Report(TenantOperationStatus.Succeeded, DescribeTransition(revoked.Value, "withdrawn"));
            await LoadAccessAsync(force: true).ConfigureAwait(false);
        }
        finally
        {
            EndBusy();
        }
    }

    private async Task LoadAccessAsync(bool force)
    {
        if (!force && _accessLoaded)
        {
            return;
        }

        if (SelectedTenantId is not { } tenantId)
        {
            ResetAccess();
            return;
        }

        _accessLoaded = true;

        var subjects = await _domain.Tenants.ListAdminSubjectsAsync(tenantId).ConfigureAwait(false);
        if (subjects.IsSuccess)
        {
            AdminSubjects = subjects.Value?.Subjects ?? NoSubjects;
        }
        else
        {
            AdminSubjects = NoSubjects;
            Report(subjects.Status, TenantRefusal.DescribeAdminChange(subjects));
        }

        var grants = await _domain.Tenants.ListGrantsAsync(tenantId).ConfigureAwait(false);
        _issuedGrants.Clear();
        _receivedGrants.Clear();

        if (!grants.IsSuccess)
        {
            Report(grants);
            return;
        }

        var report = grants.Value ?? ExplorerTenantGrants.Empty;
        for (var i = 0; i < report.Issued.Count; i++)
        {
            _issuedGrants.Add(TenantGrantRow.From(report.Issued[i], TenantGrantDirection.Issued));
        }

        for (var i = 0; i < report.Received.Count; i++)
        {
            _receivedGrants.Add(TenantGrantRow.From(report.Received[i], TenantGrantDirection.Received));
        }
    }

    private void ResetAccess()
    {
        _accessLoaded = false;
        AdminSubjects = NoSubjects;
        AddAdminSubjectId = string.Empty;
        OfferCounterpartyTenantId = string.Empty;
        OfferScope = string.Empty;
        OfferOperations = ExplorerTenantGrantAccess.Read;
        OfferFromSelectedTenant = true;
        _issuedGrants.Clear();
        _receivedGrants.Clear();
    }

    private void ReportNotPending(TenantGrantRow row, string transition)
    {
        Report(
            TenantOperationStatus.GrantTransitionRejected,
            "That grant is " + row.StateLabel.ToLowerInvariant() + ", so it cannot be " + transition
                + ". Only a pending grant is awaiting an answer.");
        RaiseChanged();
    }

    private static TenantConfirmation BuildGrantConfirmation(
        TenantGrantRow row,
        TenantConfirmationKind kind,
        string title,
        string body,
        string confirmLabel) =>
        new()
        {
            Kind = kind,
            TenantId = row.GranterTenantId,
            CounterpartyTenantId = row.GranteeTenantId,
            Target = row.Scope,
            Title = title,
            Body = body,
            ConfirmLabel = confirmLabel,
        };

    /// <summary>
    /// Describes a grant transition from the grant as committed rather than from
    /// what the call asked for: the transitions are idempotent, so a repeat lands
    /// the grant in its target state while reporting that nothing moved.
    /// </summary>
    private static string DescribeTransition(ExplorerTenantGrantChange change, string verb)
    {
        var row = TenantGrantRow.From(change.Grant, TenantGrantDirection.Issued);
        var prefix = change.Changed
            ? "Grant " + verb + ": "
            : "The grant was already in that state, so nothing moved. ";

        return prefix + row.GranterTenantId + " to " + row.GranteeTenantId + " over " + row.Scope
            + " is now " + row.StateLabel.ToLowerInvariant() + ". " + row.AuthorityText;
    }
}
