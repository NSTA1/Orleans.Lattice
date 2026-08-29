using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants.Workspace;

/// <summary>
/// The tenant lifecycle operations: registering a tenant with seeded admin
/// subjects, suspending and resuming one, and deleting one.
/// <para>
/// Suspend and delete are destructive, so neither runs from the click that asks
/// for it: both build a <see cref="TenantConfirmation"/> and wait. A delete
/// confirmation reports how many trees the cascade will take with it, read
/// before the call rather than reported afterwards - and when that count was
/// never measured it says so rather than showing a reassuring zero.
/// </para>
/// </summary>
public sealed partial class TenantsWorkspace
{
    /// <summary>The message shown when the create form has no tenant id.</summary>
    public const string CreateNeedsTenantIdMessage = "Enter the id of the tenant to create.";

    private static readonly char[] SubjectSeparators = [',', ';', '\n', '\r'];

    /// <summary>
    /// The id of the tenant to register, as typed into the create form.
    /// </summary>
    public string CreateTenantId { get; set; } = string.Empty;

    /// <summary>
    /// The admin subjects to seed onto the new tenant, one per line or separated
    /// by commas. Left blank the cluster seeds the calling subject, so the
    /// creator can read the tenant back - tenant visibility resolves from
    /// admin-subject membership, and a tenant with no admin subjects would be
    /// invisible to whoever created it.
    /// </summary>
    public string CreateAdminSubjects { get; set; } = string.Empty;

    /// <summary>Whether the create form is open.</summary>
    public bool CreateFormOpen { get; private set; }

    /// <summary>Opens the create form, clearing whatever was typed before.</summary>
    public void OpenCreateForm()
    {
        CreateFormOpen = true;
        CreateTenantId = string.Empty;
        CreateAdminSubjects = string.Empty;
        ClearResult();
        RaiseChanged();
    }

    /// <summary>Closes the create form without registering anything.</summary>
    public void CloseCreateForm()
    {
        CreateFormOpen = false;
        RaiseChanged();
    }

    /// <summary>
    /// Registers the tenant named in the create form, seeding the admin subjects
    /// it lists. On success the list is reloaded so the new tenant is visible
    /// and its seeded subjects are reported.
    /// </summary>
    public async Task CreateTenantAsync()
    {
        if (!Allowed || Busy)
        {
            return;
        }

        var tenantId = CreateTenantId.Trim();
        if (tenantId.Length == 0)
        {
            Report(TenantOperationStatus.InvalidRequest, CreateNeedsTenantIdMessage);
            RaiseChanged();
            return;
        }

        var subjects = ParseSubjects(CreateAdminSubjects);

        BeginBusy();
        try
        {
            var created = await _domain.Tenants
                .CreateTenantAsync(tenantId, subjects)
                .ConfigureAwait(false);

            if (!created.IsSuccess)
            {
                Report(created);
                return;
            }

            CreateFormOpen = false;
            CreateTenantId = string.Empty;
            CreateAdminSubjects = string.Empty;
            Report(TenantOperationStatus.Succeeded, DescribeCreation(created.Value));
        }
        finally
        {
            EndBusy();
        }

        await ReloadListAfterChangeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asks to suspend <paramref name="tenantId"/>, holding the request for an
    /// explicit confirmation. Suspension refuses the tenant's data-plane
    /// operations until it is resumed, so it is confirmed even though its trees
    /// remain intact.
    /// </summary>
    /// <param name="tenantId">The tenant to suspend.</param>
    public void RequestSuspend(string tenantId)
    {
        ArgumentNullException.ThrowIfNull(tenantId);

        if (!Allowed || Busy)
        {
            return;
        }

        var isDefault = IsReservedDefault(tenantId);
        Confirmation = new TenantConfirmation
        {
            Kind = TenantConfirmationKind.Suspend,
            TenantId = tenantId,
            Title = "Suspend " + tenantId + "?",
            Body = "Suspending " + tenantId + " refuses every data-plane operation for the tenant "
                + "until an operator resumes it. Its trees and their contents remain intact.",
            ConfirmLabel = "Suspend tenant",
            Caution = isDefault
                ? "This is the reserved default tenant. The cluster will refuse to suspend it."
                : null,
        };

        ClearResult();
        RaiseChanged();
    }

    /// <summary>
    /// Resumes <paramref name="tenantId"/>, returning it to the active state.
    /// Not destructive, so it runs directly.
    /// </summary>
    /// <param name="tenantId">The tenant to resume.</param>
    public async Task ResumeTenantAsync(string tenantId)
    {
        ArgumentNullException.ThrowIfNull(tenantId);

        if (!Allowed || Busy)
        {
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            var resumed = await _domain.Tenants.ResumeTenantAsync(tenantId).ConfigureAwait(false);
            ReportStatusChange(resumed, tenantId, "resumed", "already active");
        }
        finally
        {
            EndBusy();
        }

        await ReloadListAfterChangeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asks to delete <paramref name="tenantId"/>, first reading how many trees
    /// the deletion will cascade through so the confirmation can state the blast
    /// radius rather than merely warning about it.
    /// </summary>
    /// <param name="tenantId">The tenant to delete.</param>
    public async Task RequestDeleteAsync(string tenantId)
    {
        ArgumentNullException.ThrowIfNull(tenantId);

        if (!Allowed || Busy)
        {
            return;
        }

        ClearResult();
        BeginBusy();
        try
        {
            Confirmation = new TenantConfirmation
            {
                Kind = TenantConfirmationKind.Delete,
                TenantId = tenantId,
                Title = "Delete " + tenantId + "?",
                Body = "Deleting " + tenantId + " is irreversible and cascades to every tree the "
                    + "tenant owns. " + await DescribeCascadeAsync(tenantId).ConfigureAwait(false),
                ConfirmLabel = "Delete tenant and its trees",
                Caution = IsReservedDefault(tenantId)
                    ? "This is the reserved default tenant. The cluster will refuse to delete it."
                    : null,
            };
        }
        finally
        {
            EndBusy();
        }
    }

    private async Task SuspendConfirmedAsync(string tenantId)
    {
        BeginBusy();
        try
        {
            var suspended = await _domain.Tenants.SuspendTenantAsync(tenantId).ConfigureAwait(false);
            ReportStatusChange(suspended, tenantId, "suspended", "already suspended");
        }
        finally
        {
            EndBusy();
        }

        await ReloadListAfterChangeAsync().ConfigureAwait(false);
    }

    private async Task DeleteConfirmedAsync(string tenantId)
    {
        BeginBusy();
        try
        {
            var deleted = await _domain.Tenants.DeleteTenantAsync(tenantId).ConfigureAwait(false);
            if (!deleted.IsSuccess)
            {
                Report(deleted);
                return;
            }

            var cascaded = deleted.Value.CascadedTreeCount;
            Report(
                TenantOperationStatus.Succeeded,
                "Deleted " + tenantId + " and the "
                    + TenantQuotaFormat.Count(cascaded)
                    + (cascaded == 1 ? " tree" : " trees") + " it owned.");

            if (string.Equals(SelectedTenantId, tenantId, StringComparison.Ordinal))
            {
                ClearSelection();
            }
        }
        finally
        {
            EndBusy();
        }

        await ReloadListAfterChangeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Describes the cascade a delete would perform, from the tenant's owned-tree
    /// usage figure. An unmeasured figure is reported as unknown rather than as
    /// zero, because "this deletes nothing else" and "we did not measure what
    /// this deletes" are opposite things to tell somebody about to delete data.
    /// </summary>
    private async Task<string> DescribeCascadeAsync(string tenantId)
    {
        if (!_headlineUsage.TryGetValue(tenantId, out var usage))
        {
            var read = await _domain.Tenants.GetQuotaUsageAsync(tenantId).ConfigureAwait(false);
            if (!read.IsSuccess || read.Value is null)
            {
                return "The number of trees it owns could not be read, so the size of the cascade "
                    + "is unknown.";
            }

            usage = read.Value;
            _headlineUsage[tenantId] = usage;
        }

        if (usage.TreeCount.Usage is not { } trees)
        {
            return "The number of trees it owns was not measured, so the size of the cascade is "
                + "unknown - it is not zero.";
        }

        return trees == 0
            ? "It currently owns no trees."
            : "It currently owns " + TenantQuotaFormat.Count(trees)
                + (trees == 1 ? " tree, which will be deleted with it."
                    : " trees, which will be deleted with it.");
    }

    private void ReportStatusChange(
        TenantOperationResult<ExplorerTenantStatusChange> result,
        string tenantId,
        string movedVerb,
        string unchangedState)
    {
        if (!result.IsSuccess)
        {
            Report(result);
            return;
        }

        Report(
            TenantOperationStatus.Succeeded,
            result.Value.Changed
                ? "Tenant " + tenantId + " was " + movedVerb + "."
                : "Tenant " + tenantId + " was " + unchangedState + "; nothing changed.");
    }

    private static string DescribeCreation(ExplorerTenantCreation? creation)
    {
        if (creation is null)
        {
            return "The tenant was created.";
        }

        var seeded = creation.AdminSubjects;
        return seeded.Count == 0
            ? "Created " + creation.TenantId + "."
            : "Created " + creation.TenantId + ", seeding "
                + TenantQuotaFormat.Count(seeded.Count)
                + (seeded.Count == 1 ? " admin subject: " : " admin subjects: ")
                + string.Join(", ", seeded) + ".";
    }

    /// <summary>
    /// Re-reads the list after a lifecycle change without clearing the message
    /// that described it, so the operator still sees what happened. A refused
    /// operation changed nothing, so it costs no round trip.
    /// </summary>
    private async Task ReloadListAfterChangeAsync()
    {
        var status = LastStatus;
        if (status != TenantOperationStatus.Succeeded)
        {
            return;
        }

        var message = LastMessage;
        await ReloadAsync().ConfigureAwait(false);
        Report(TenantOperationStatus.Succeeded, message ?? string.Empty);
        RaiseChanged();
    }

    private bool IsReservedDefault(string tenantId)
    {
        for (var i = 0; i < _tenants.Count; i++)
        {
            if (string.Equals(_tenants[i].TenantId, tenantId, StringComparison.Ordinal))
            {
                return _tenants[i].IsDefault;
            }
        }

        return false;
    }

    /// <summary>
    /// Splits the create form's subject box into ids, dropping blanks. Returns
    /// <see langword="null"/> for an empty box, which asks the cluster to seed
    /// the calling subject rather than seeding nothing.
    /// </summary>
    private static IReadOnlyCollection<string>? ParseSubjects(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var parts = raw.Split(SubjectSeparators, StringSplitOptions.RemoveEmptyEntries
            | StringSplitOptions.TrimEntries);

        return parts.Length == 0 ? null : parts;
    }
}
