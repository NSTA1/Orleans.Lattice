namespace Orleans.Lattice.Explorer.Tenants.Workspace;

/// <summary>
/// The confirmation gate every destructive operation on the surface passes
/// through, and the reset that clears the tenant-scoped sub-surfaces when the
/// selection changes.
/// <para>
/// Nothing here performs an operation. A request builds a
/// <see cref="TenantConfirmation"/> and stops; the surface renders it; and only
/// <see cref="ConfirmAsync"/> dispatches to the operation the request was for.
/// A destructive action therefore always costs two deliberate steps, and always
/// shows its blast radius in between.
/// </para>
/// </summary>
public sealed partial class TenantsWorkspace
{
    /// <summary>
    /// The destructive operation awaiting an explicit confirmation, or
    /// <see langword="null"/> when none is.
    /// </summary>
    public TenantConfirmation? Confirmation { get; private set; }

    /// <summary>Whether a destructive operation is awaiting confirmation.</summary>
    public bool IsAwaitingConfirmation => Confirmation is not null;

    /// <summary>Abandons the pending confirmation, performing nothing.</summary>
    public void CancelConfirmation()
    {
        if (Confirmation is null)
        {
            return;
        }

        Confirmation = null;
        RaiseChanged();
    }

    /// <summary>
    /// Performs the pending destructive operation and clears the confirmation. A
    /// no-op when nothing is pending, when the gate does not admit the caller, or
    /// while another request is in flight - so a double confirm cannot run the
    /// operation twice.
    /// </summary>
    public async Task ConfirmAsync()
    {
        if (Confirmation is not { } pending || !Allowed || Busy)
        {
            return;
        }

        // Cleared before the operation runs, so the dialog closes on the first
        // confirm and a second click has nothing left to dispatch.
        Confirmation = null;
        ClearResult();

        switch (pending.Kind)
        {
            case TenantConfirmationKind.Delete:
                await DeleteConfirmedAsync(pending.TenantId).ConfigureAwait(false);
                break;
            case TenantConfirmationKind.Suspend:
                await SuspendConfirmedAsync(pending.TenantId).ConfigureAwait(false);
                break;
            case TenantConfirmationKind.RemoveAdminSubject when pending.Target is { } subjectId:
                await RemoveAdminSubjectConfirmedAsync(pending.TenantId, subjectId).ConfigureAwait(false);
                break;
            case TenantConfirmationKind.RevokeGrant
                when pending.Target is { } scope && pending.CounterpartyTenantId is { } grantee:
                await RevokeGrantConfirmedAsync(pending.TenantId, grantee, scope).ConfigureAwait(false);
                break;
            case TenantConfirmationKind.RejectGrant
                when pending.Target is { } scope && pending.CounterpartyTenantId is { } grantee:
                await RejectGrantConfirmedAsync(pending.TenantId, grantee, scope).ConfigureAwait(false);
                break;
            case TenantConfirmationKind.RevokeRegion:
                await AuthorizeRegionsConfirmedAsync(pending.TenantId).ConfigureAwait(false);
                break;
            default:
                // A confirmation missing the target it needs cannot be dispatched.
                // Failing silently closed is correct here: the alternative would
                // be guessing what to destroy.
                RaiseChanged();
                break;
        }
    }

    /// <summary>
    /// Drops everything the three tenant-scoped sub-surfaces hold, so a new
    /// selection can never render the previous tenant's quotas, regions, admin
    /// subjects, or grants while its own are still loading.
    /// </summary>
    private void ResetTenantScopedSurfaces()
    {
        Confirmation = null;
        ResetQuotas();
        ResetRegions();
        ResetAccess();
    }
}
