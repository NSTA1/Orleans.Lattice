using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;

/// <summary>
/// The Members surface: the subjects holding tenant-admin authority over this
/// tenant, and the add and remove operations over them.
/// </summary>
/// <remarks>
/// The last-admin-subject invariant is checked here <em>before</em> the call as
/// well as after it. That is not belt and braces: the gRPC binding collapses
/// every precondition refusal onto one status and carries the reason only in the
/// message, so a wire caller never observes
/// <see cref="TenantOperationStatus.LastAdminSubject"/> at all. Knowing the
/// invariant client-side is what lets the surface name it, and the server's own
/// message is rendered verbatim when it refuses anyway.
/// </remarks>
public sealed partial class MyTenantWorkspace
{
    private static readonly IReadOnlyList<string> NoSubjects = Array.Empty<string>();

    /// <summary>
    /// The refusal shown when the caller reaches for the remove control on the
    /// only remaining admin subject.
    /// </summary>
    public const string LastAdminSubjectRefusal =
        "This is the only admin subject, so removing it would leave nobody able to administer the "
        + "tenant.";

    /// <summary>The refusal shown when the add form carries no subject id.</summary>
    public const string EmptySubjectRefusal = "Enter the id of the subject to grant admin authority to.";

    private bool _adminSubjectsLoaded;

    /// <summary>
    /// The subjects holding tenant-admin authority, as the cluster last reported
    /// them.
    /// </summary>
    public IReadOnlyList<string> AdminSubjects { get; private set; } = NoSubjects;

    /// <summary>The subject id typed into the add form.</summary>
    public string NewAdminSubjectId { get; set; } = string.Empty;

    /// <summary>
    /// Whether the tenant is down to its last admin subject, so the surface can
    /// say why every remove control is disabled rather than disabling them all
    /// silently.
    /// </summary>
    public bool IsLastAdminSubject => AdminSubjects.Count == 1;

    /// <summary>
    /// Whether <paramref name="subjectId"/> may be removed, as far as the client
    /// can tell. The cluster re-checks, and remains the enforcement point.
    /// </summary>
    /// <param name="subjectId">The subject to test.</param>
    /// <returns>
    /// <see langword="false"/> when removing it would leave the tenant with no
    /// admin subject.
    /// </returns>
    public bool CanRemoveAdminSubject(string? subjectId) =>
        !string.IsNullOrEmpty(subjectId) && AdminSubjects.Count > 1;

    /// <summary>
    /// Grants the subject in <see cref="NewAdminSubjectId"/> tenant-admin
    /// authority. Idempotent on an existing member, which the cluster reports as
    /// a success that changed nothing.
    /// </summary>
    public async Task AddAdminSubjectAsync()
    {
        if (!Allowed || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        var subjectId = NewAdminSubjectId.Trim();
        if (subjectId.Length == 0)
        {
            Refuse(TenantOperationStatus.InvalidRequest, EmptySubjectRefusal);
            return;
        }

        var tenantId = TenantId;
        var succeeded = await RunAsync(
            () => _domain.Tenants.AddAdminSubjectAsync(tenantId, subjectId),
            change => AdminSubjects = change.Subjects).ConfigureAwait(false);

        if (succeeded)
        {
            NewAdminSubjectId = string.Empty;
        }
    }

    /// <summary>
    /// Revokes <paramref name="subjectId"/>'s tenant-admin authority, refusing
    /// client-side when it is the last one rather than spending a round trip to
    /// be told so in a message the wire has already made generic.
    /// </summary>
    /// <param name="subjectId">The subject to revoke.</param>
    public async Task RemoveAdminSubjectAsync(string subjectId)
    {
        if (!Allowed || string.IsNullOrEmpty(TenantId) || string.IsNullOrEmpty(subjectId))
        {
            return;
        }

        if (!CanRemoveAdminSubject(subjectId))
        {
            Refuse(
                TenantOperationStatus.LastAdminSubject,
                LastAdminSubjectRefusal,
                MyTenantNotice.LastAdminSubjectGuidance);
            return;
        }

        var tenantId = TenantId;
        await RunAsync(
            () => _domain.Tenants.RemoveAdminSubjectAsync(tenantId, subjectId),
            change => AdminSubjects = change.Subjects).ConfigureAwait(false);
    }

    private async Task LoadAdminSubjectsAsync(bool force)
    {
        if ((!force && _adminSubjectsLoaded) || string.IsNullOrEmpty(TenantId))
        {
            return;
        }

        _adminSubjectsLoaded = true;

        var admins = await _domain.Tenants.ListAdminSubjectsAsync(TenantId).ConfigureAwait(false);
        if (admins.IsSuccess && admins.Value is { } report)
        {
            AdminSubjects = report.Subjects;
            return;
        }

        AdminSubjects = NoSubjects;
        LastNotice = MyTenantNotice.For(admins);
    }
}
