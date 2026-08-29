using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of the transport-agnostic
/// <see cref="ILatticeTenantAccessAdmin"/> tenant access-administration control
/// facade. It is the single narrowest seam at which every admin-subject
/// membership operation is authorized (fail-closed, tenant-tier) and applied to
/// the tenancy engine's <see cref="ITenantRegistry"/>; every transport binding is
/// a thin adapter over this one surface. It is a sibling of
/// <see cref="LatticeTenantAdmin"/> and <see cref="LatticeTenantRegionAdmin"/>,
/// added append-only so the tenant lifecycle facade is unchanged.
/// </summary>
/// <remarks>
/// <para>
/// <b>Tenant-tier authorization.</b> Every operation goes through
/// <see cref="TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync(TenantId, string, CancellationToken)"/>
/// - the platform-operator <b>or</b> live-admin-subject-of-that-tenant check - and
/// deliberately <em>not</em> through <see cref="TenantAdminAccessAuthorizer"/>,
/// whose identically-named <c>AuthorizeTenantAdminAsync</c> is
/// platform-operator-only and gates the lifecycle mutations. Using the
/// operator-only seam here would lock a tenant's own admins out of their
/// membership set; using this tenant-tier seam on a lifecycle mutation would let a
/// tenant admin escalate. The authorizer also returns the tenant record, so each
/// operation reuses that single registry read rather than re-reading.
/// </para>
/// <para>
/// <b>Last-writer-wins stamping.</b> Membership is an LWW-element-set on the
/// record, stamped with a strictly increasing <see cref="ITenantAdminClock"/> clock
/// and the cluster's writer id, so concurrent adds and removes from any replica
/// converge through the record's per-subject CRDT merge instead of clobbering each
/// other.
/// </para>
/// <para>
/// <b>Invariants.</b> The reserved default tenant's membership can never be
/// mutated, and the last admin subject can never be removed. Both are enforced
/// here, fail-closed.
/// </para>
/// </remarks>
internal sealed class LatticeTenantAccessAdmin : ILatticeTenantAccessAdmin
{
    /// <summary>
    /// The surface name interpolated into the authorizer's denial message, so a
    /// refused caller is told which tenant-scoped authority it lacked.
    /// </summary>
    private const string AdminSubjectsAction = "admin subjects";

    private readonly ITenantRegistry _registry;
    private readonly TenantRegionResidencyAuthorizer _authorizer;
    private readonly ITenantAdminClock _clock;
    private readonly ILatticeIdentityDirectory? _identityDirectory;
    private readonly IOptionsMonitor<LatticeIdentityDirectoryOptions>? _identityDirectoryOptions;
    private readonly string? _writerId;

    /// <summary>
    /// Initializes a new <see cref="LatticeTenantAccessAdmin"/>.
    /// </summary>
    /// <param name="registry">The tenancy engine's lifecycle store. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The tenant-tier fail-closed authorization seam. Must not be <c>null</c>.</param>
    /// <param name="clock">The monotonic clock supplying last-writer-wins stamps. Must not be <c>null</c>.</param>
    /// <param name="clusterOptions">The cluster options supplying the writer id stamped on registry writes. Must not be <c>null</c>.</param>
    /// <param name="identityDirectory">
    /// The upstream identity directory used to validate a subject id before it is
    /// granted tenant-admin authority, or <c>null</c> when none is registered (ids
    /// are then accepted without directory validation, as on a cluster with no
    /// directory).
    /// </param>
    /// <param name="identityDirectoryOptions">
    /// The identity-directory options deciding whether validation is required, or
    /// <c>null</c> when none is registered.
    /// </param>
    /// <exception cref="ArgumentNullException">Any argument other than <paramref name="identityDirectory"/> or <paramref name="identityDirectoryOptions"/> is <c>null</c>.</exception>
    public LatticeTenantAccessAdmin(
        ITenantRegistry registry,
        TenantRegionResidencyAuthorizer authorizer,
        ITenantAdminClock clock,
        IOptions<ClusterOptions> clusterOptions,
        ILatticeIdentityDirectory? identityDirectory = null,
        IOptionsMonitor<LatticeIdentityDirectoryOptions>? identityDirectoryOptions = null)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(clusterOptions);

        _registry = registry;
        _authorizer = authorizer;
        _clock = clock;
        _identityDirectory = identityDirectory;
        _identityDirectoryOptions = identityDirectoryOptions;
        _writerId = clusterOptions.Value.ClusterId;
    }

    /// <inheritdoc />
    public async Task<TenantAdminSubjectReport> ListAdminSubjectsAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);

        // The authorizer returns the record, so the read that proved authority is
        // the same read the projection is built from - no second registry hit.
        var record = await _authorizer
            .AuthorizeTenantAdminAsync(tenant, AdminSubjectsAction, cancellationToken)
            .ConfigureAwait(false);

        return new TenantAdminSubjectReport
        {
            TenantId = tenant.Value,
            Subjects = record.AdminSubjects,
        };
    }

    /// <inheritdoc />
    public async Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ValidateSubjectId(subjectId);

        var record = await _authorizer
            .AuthorizeTenantAdminAsync(tenant, AdminSubjectsAction, cancellationToken)
            .ConfigureAwait(false);

        ThrowIfReservedTenant(tenant, "add-admin-subject");

        // Idempotent no-op: the subject already holds tenant-admin authority, so no
        // new membership reference is created and none needs validating.
        if (record.HasAdminSubject(subjectId))
        {
            return Unchanged(tenant, subjectId, record);
        }

        // Membership of the admin-subject set *is* the tenant-admin capability, so
        // this is an administrative membership-reference create path and carries
        // the same directory contract the create path applies to an explicit seed
        // set: a typo'd, retired, or not-yet-provisioned id must never be recorded
        // as a live grant that whoever later registers it would inherit.
        await ValidateDirectorySubjectAsync(subjectId, cancellationToken).ConfigureAwait(false);

        record.AddAdminSubject(subjectId, _clock.Next(), _writerId);

        // The registry's put is a CRDT read-merge-write that returns the committed
        // join, so the response is built from the merged record rather than this
        // caller's pre-merge local view - a concurrent membership write from
        // another replica is therefore reflected instead of silently dropped.
        var merged = await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        return new TenantAdminSubjectChangeResult
        {
            TenantId = tenant.Value,
            SubjectId = subjectId,
            Changed = true,
            Subjects = merged.AdminSubjects,
        };
    }

    /// <inheritdoc />
    public async Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ValidateSubjectId(subjectId);

        var record = await _authorizer
            .AuthorizeTenantAdminAsync(tenant, AdminSubjectsAction, cancellationToken)
            .ConfigureAwait(false);

        ThrowIfReservedTenant(tenant, "remove-admin-subject");

        // Idempotent no-op: the subject holds no authority to revoke. Checked ahead
        // of the last-subject guard so removing an already-absent id can never be
        // mistaken for emptying the set.
        if (!record.HasAdminSubject(subjectId))
        {
            return Unchanged(tenant, subjectId, record);
        }

        // Unbypassable orphan guard: a tenant stripped of its last admin subject
        // disappears from every self-service surface and can only be reached by a
        // platform operator, so the removal is refused rather than silently
        // stranding the tenant. Counted in place - no list materialised.
        if (record.AdminSubjectCount <= 1)
        {
            throw new TenantLastAdminSubjectException(tenant.Value, subjectId);
        }

        record.RemoveAdminSubject(subjectId, _clock.Next(), _writerId);
        var merged = await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        // The guard above is a read-check-write over a CRDT store, so it alone is
        // not sufficient: two concurrent removals of *different* subjects can each
        // observe two live subjects, each pass the check, and land tombstones on
        // disjoint keys that both survive the per-subject merge - emptying the set
        // and orphaning the tenant. The registry's returned join is the first point
        // at which that is observable, so re-check it and self-heal: re-grant this
        // call's own subject at a strictly later stamp (which supersedes the
        // tombstone this call just wrote, and only that one) and refuse the removal.
        // Both racing callers are then told the removal was refused, and the tenant
        // is left with at least one admin subject rather than none - the fail-closed
        // direction. A retry of the refused call now sees a single live subject and
        // is refused by the guard above before it writes, so this terminates.
        if (merged.AdminSubjectCount == 0)
        {
            merged.AddAdminSubject(subjectId, _clock.Next(), _writerId);
            await _registry.PutAsync(merged, cancellationToken).ConfigureAwait(false);
            throw new TenantLastAdminSubjectException(tenant.Value, subjectId);
        }

        return new TenantAdminSubjectChangeResult
        {
            TenantId = tenant.Value,
            SubjectId = subjectId,
            Changed = true,
            Subjects = merged.AdminSubjects,
        };
    }

    private static TenantAdminSubjectChangeResult Unchanged(
        TenantId tenant, string subjectId, TenantRecord record) =>
        new()
        {
            TenantId = tenant.Value,
            SubjectId = subjectId,
            Changed = false,
            Subjects = record.AdminSubjects,
        };

    /// <summary>
    /// Validates a subject id against the upstream identity directory, so a
    /// tenant-admin grant can never be recorded against a principal that does not
    /// exist. Follows the same contract as its siblings on the tenant-create and
    /// authorization-admin paths: validate only when a real directory provider is
    /// active and <see cref="LatticeIdentityDirectoryOptions.ValidationRequired"/>
    /// is set, and deny an unresolvable id before the write.
    /// </summary>
    private async Task ValidateDirectorySubjectAsync(string subjectId, CancellationToken cancellationToken)
    {
        if (_identityDirectory is null
            || _identityDirectoryOptions?.CurrentValue.ValidationRequired != true
            || _identityDirectory is NullIdentityDirectory)
        {
            return;
        }

        var principal = await _identityDirectory
            .ResolveAsync(subjectId, cancellationToken).ConfigureAwait(false);

        if (principal is null)
        {
            throw LatticeDirectoryValidationException.Unresolved(
                subjectId, DirectoryPrincipalKind.User, "subjectId");
        }
    }

    /// <summary>
    /// Rejects a membership mutation against the reserved default tenant. It names
    /// the cluster's own legacy state, so granting it a tenant admin would hand out
    /// tenant-admin authority over the whole legacy keyspace. The reserved id is a
    /// constant, so the refusal leaks nothing about registry contents.
    /// </summary>
    private static void ThrowIfReservedTenant(TenantId tenant, string operation)
    {
        if (tenant.IsDefault)
        {
            throw new ReservedTenantOperationException(tenant.Value, operation);
        }
    }

    private static void ValidateSubjectId(string subjectId)
    {
        if (string.IsNullOrWhiteSpace(subjectId))
        {
            throw new ArgumentException(
                "An admin subject id must not be null, empty, or whitespace.", nameof(subjectId));
        }
    }

    private static TenantId ParseTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (!TenantId.TryParse(tenantId, out var tenant))
        {
            throw new ArgumentException(
                $"'{tenantId}' is not a valid tenant id.", nameof(tenantId));
        }

        return tenant;
    }
}
