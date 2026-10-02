using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of <see cref="ILatticeTenantDirectoryAdmin"/>: a
/// tenant's own groups, their direct members, and the tenant's member set,
/// administered by the tenant's own admins without a platform operator in the loop.
/// It is the single narrowest seam at which every such operation is authorized; every
/// transport binding is a thin adapter over it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Order of checks.</b> Every operation validates its arguments' syntax, then
/// authorizes the caller through
/// <see cref="TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync(TenantId, string, CancellationToken)"/>
/// (a platform operator, or an admin of the tenant directly or through a group),
/// then refuses with <see cref="TenantAccessAdministrationDisabledException"/> when
/// delegated tenant access administration is off, then refuses the reserved default
/// tenant. A caller that is not authorized therefore learns nothing about the
/// cluster's posture, and a non-operator asking about a tenant it does not
/// administer is denied whether or not the tenant exists.
/// </para>
/// <para>
/// <b>Underlay.</b> After authorization the membership operations run under system
/// origin through <see cref="ITenantDirectoryStore"/>, the member set is written
/// through the tenant registry's optimistic, CRDT-merging put, and a group's
/// tenant-tier rules are removed through <see cref="ITenantGroupRuleCascade"/>.
/// Group ids are always composed here with
/// <see cref="LatticeTenantGroupId.Compose(TenantId, string)"/> from the tenant the
/// call names and a local name, so a caller can never name another tenant's group.
/// </para>
/// <para>
/// <b>Caps (D13): verify and compensate.</b> Each addition that a cap governs - a
/// new group, a membership edge, a member-set entry - is checked against the
/// tenant's current count before it is written, and that check alone is a
/// read-check-write with no lock, so concurrent callers at one below the cap could
/// all pass it. Each such addition is therefore verified after it is written: the
/// group and edge counts are re-read, and the member set is read from the registry's
/// committed merge. If the tenant is then over its cap, this call withdraws exactly
/// the addition it made (a new group with its edges, the edge, or the member-set
/// entry at a later stamp) and is refused with
/// <see cref="LatticeQuotaExceededException"/>. Racers that interleave may all be
/// refused, which is the fail-closed direction. The residual window is bounded and
/// transient: between a racer's write and its withdrawal a reader can observe the
/// tenant briefly over its cap, and a call interrupted between the two (a silo
/// crash, a cancelled call, a failed compensating write) leaves the extra item in
/// place until it is removed; the cap is then still enforced on every later
/// addition, so the overshoot can never grow beyond the calls in flight.
/// </para>
/// <para>
/// <b>Entry kinds.</b> The member set and group edges store plain ids, so a listing
/// recovers each entry's <see cref="TenantSubjectKind"/>: an id in the reserved
/// <c>t/</c> namespace is a tenant group, an id with a group record in the
/// membership directory (or which the identity directory resolves as a group) is a
/// cluster group, and anything else is a user.
/// </para>
/// </remarks>
internal sealed partial class LatticeTenantDirectoryAdmin : ILatticeTenantDirectoryAdmin
{
    /// <summary>The surface name interpolated into the authorizer's denial message.</summary>
    internal const string DirectoryAction = "groups and members";

    private readonly ITenantRegistry _registry;
    private readonly TenantRegionResidencyAuthorizer _authorizer;
    private readonly ITenantAdminClock _clock;
    private readonly ITenantDirectoryStore _store;
    private readonly ITenantGroupRuleCascade _rules;
    private readonly Func<bool> _delegatedAccessEnabled;
    private readonly ILatticeIdentityDirectory? _identityDirectory;
    private readonly IOptionsMonitor<LatticeIdentityDirectoryOptions>? _identityDirectoryOptions;
    private readonly string? _writerId;

    /// <summary>Initializes a new <see cref="LatticeTenantDirectoryAdmin"/>.</summary>
    /// <param name="registry">The tenancy registry holding the member and admin sets. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The tenant-tier fail-closed authorization seam. Must not be <c>null</c>.</param>
    /// <param name="clock">The monotonic clock supplying last-writer-wins stamps. Must not be <c>null</c>.</param>
    /// <param name="clusterOptions">The cluster options supplying the writer id. Must not be <c>null</c>.</param>
    /// <param name="store">The membership underlay. Must not be <c>null</c>.</param>
    /// <param name="rules">The tenant-tier rule cascade for group removal. Must not be <c>null</c>.</param>
    /// <param name="delegatedAccessEnabled">The live read of the delegated-access flag, consulted on every call. Must not be <c>null</c>.</param>
    /// <param name="identityDirectory">The upstream identity directory, or <c>null</c> when none is registered.</param>
    /// <param name="identityDirectoryOptions">The identity-directory options, or <c>null</c> when none is registered.</param>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    public LatticeTenantDirectoryAdmin(
        ITenantRegistry registry,
        TenantRegionResidencyAuthorizer authorizer,
        ITenantAdminClock clock,
        IOptions<ClusterOptions> clusterOptions,
        ITenantDirectoryStore store,
        ITenantGroupRuleCascade rules,
        Func<bool> delegatedAccessEnabled,
        ILatticeIdentityDirectory? identityDirectory = null,
        IOptionsMonitor<LatticeIdentityDirectoryOptions>? identityDirectoryOptions = null)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(clusterOptions);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(rules);
        ArgumentNullException.ThrowIfNull(delegatedAccessEnabled);

        _registry = registry;
        _authorizer = authorizer;
        _clock = clock;
        _store = store;
        _rules = rules;
        _delegatedAccessEnabled = delegatedAccessEnabled;
        _identityDirectory = identityDirectory;
        _identityDirectoryOptions = identityDirectoryOptions;
        _writerId = clusterOptions.Value.ClusterId;
    }

    /// <summary>
    /// Authorizes the caller over <paramref name="tenant"/>, then applies the feature
    /// gate and the reserved-tenant refusal, in that order, and returns the record the
    /// authorization read.
    /// </summary>
    private async Task<TenantRecord> AuthorizeAsync(TenantId tenant, string operation, CancellationToken cancellationToken)
    {
        var record = await _authorizer
            .AuthorizeTenantAdminAsync(tenant, DirectoryAction, cancellationToken)
            .ConfigureAwait(false);

        if (!_delegatedAccessEnabled())
        {
            throw new TenantAccessAdministrationDisabledException(tenant.Value);
        }

        if (tenant.IsDefault)
        {
            throw new ReservedTenantOperationException(tenant.Value, operation);
        }

        return record;
    }

    /// <summary>
    /// Maps an entry named by kind to the id stored in the membership trees and the
    /// tenant record: a tenant group's local name composes to <c>t/{tenant}/{name}</c>;
    /// a user or cluster group id is stored verbatim and may never be in the reserved
    /// <c>t/</c> namespace (D2, D4).
    /// </summary>
    private static string ToStoredId(TenantId tenant, string id, TenantSubjectKind kind, string paramName)
    {
        if (kind == TenantSubjectKind.TenantGroup)
        {
            return LatticeTenantGroupId.Compose(tenant, id).Value;
        }

        if (id.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            throw new TenantAccessConfinementException(
                tenant.Value,
                TenantAccessConfinementRule.ForeignTenantGroup,
                $"'{id}' is in the reserved '{LatticeTenantTrees.SegmentPrefix}' tenant group namespace. Name one of "
                    + $"tenant '{tenant}''s own groups by its local name with {nameof(TenantSubjectKind)}."
                    + $"{nameof(TenantSubjectKind.TenantGroup)}; another tenant's group can never be named.",
                paramName);
        }

        return id;
    }

    /// <summary>Recovers the entry kind of a stored id (see the type remarks).</summary>
    private async Task<TenantSubjectKind> ClassifyAsync(string storedId, CancellationToken cancellationToken)
    {
        if (storedId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            return TenantSubjectKind.TenantGroup;
        }

        if (await _store.GetGroupAsync(storedId, cancellationToken).ConfigureAwait(false) is not null)
        {
            return TenantSubjectKind.ClusterGroup;
        }

        if (IdentityDirectoryAvailable)
        {
            var principal = await _identityDirectory!.ResolveAsync(storedId, cancellationToken).ConfigureAwait(false);
            if (principal?.Kind == DirectoryPrincipalKind.Group)
            {
                return TenantSubjectKind.ClusterGroup;
            }
        }

        return TenantSubjectKind.User;
    }

    /// <summary>The id a caller sees for a stored entry: a tenant group's local name, any other id verbatim.</summary>
    private static string ToCallerId(string storedId, TenantSubjectKind kind) =>
        kind == TenantSubjectKind.TenantGroup && LatticeTenantGroupId.TryParse(storedId, out var group)
            ? group.Name
            : storedId;

    private bool IdentityDirectoryAvailable =>
        _identityDirectory is not null and not NullIdentityDirectory;

    /// <summary>
    /// The identity-directory validation the cluster access facade applies on a
    /// membership-reference create path: when validation is required and a real
    /// provider is active, the id must resolve, and to the expected kind.
    /// </summary>
    private async Task ValidateDirectoryPrincipalAsync(
        string principalId, TenantSubjectKind kind, string paramName, CancellationToken cancellationToken)
    {
        if (!IdentityDirectoryAvailable || _identityDirectoryOptions?.CurrentValue.ValidationRequired != true)
        {
            return;
        }

        var expected = kind == TenantSubjectKind.ClusterGroup ? DirectoryPrincipalKind.Group : DirectoryPrincipalKind.User;
        var principal = await _identityDirectory!.ResolveAsync(principalId, cancellationToken).ConfigureAwait(false);
        if (principal is null)
        {
            throw LatticeDirectoryValidationException.Unresolved(principalId, expected, paramName);
        }

        if (principal.Kind != expected)
        {
            throw LatticeDirectoryValidationException.KindMismatch(principalId, expected, principal.Kind, paramName);
        }
    }

    /// <summary>Requires a tenant group to exist, so no entry or edge can name a group that is not there.</summary>
    private async Task RequireGroupAsync(TenantId tenant, string groupId, string localName, string paramName, CancellationToken cancellationToken)
    {
        if (await _store.GetGroupAsync(groupId, cancellationToken).ConfigureAwait(false) is null)
        {
            throw new ArgumentException(
                $"Tenant '{tenant}' has no group named '{localName}'.", paramName);
        }
    }

    /// <summary>
    /// Refuses an addition that the post-write verification found over
    /// <paramref name="cap"/>, through the same exception path as the pre-write check.
    /// </summary>
    private static void ThrowCapExceeded(TenantId tenant, string treeId, string dimension, long cap) =>
        TenantAccessCaps.AdmitAddition(tenant, treeId, dimension, cap, cap);

    private static TenantId ParseTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (!TenantId.TryParse(tenantId, out var tenant))
        {
            throw new ArgumentException($"'{tenantId}' is not a valid tenant id.", nameof(tenantId));
        }

        return tenant;
    }

    private static void ValidateGroupName(string groupName, string paramName)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupName, paramName);
        if (!LatticeTenantGroupId.IsValidName(groupName.AsSpan()))
        {
            throw new ArgumentException(
                $"'{groupName}' is not a valid tenant group name. A tenant group name is 1 to "
                    + $"{LatticeTenantGroupId.MaxNameLength} characters of lower-case ASCII letters, digits, '-', '_' and '.'.",
                paramName);
        }
    }

    private static void ValidateEntry(string id, TenantSubjectKind kind, string idParamName, string kindParamName)
    {
        ArgumentException.ThrowIfNullOrEmpty(id, idParamName);
        if (!Enum.IsDefined(kind))
        {
            throw new ArgumentOutOfRangeException(kindParamName, kind, "Not a defined tenant subject kind.");
        }

        if (kind == TenantSubjectKind.TenantGroup)
        {
            ValidateGroupName(id, idParamName);
        }
    }
}
