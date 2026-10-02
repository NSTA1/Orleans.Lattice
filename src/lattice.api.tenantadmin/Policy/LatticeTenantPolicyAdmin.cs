using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of the transport-agnostic
/// <see cref="ILatticeTenantPolicyAdmin"/> facade for delegated tenant access
/// administration: tenant-tier rules on a tenant's own trees, a layer-aware explain
/// and effective-permissions view over them, and the tenant's access posture. Every
/// transport binding is a thin adapter over this one surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Prologue.</b> Every operation parses the tenant and authorizes the caller
/// through the tenant-tier <see cref="TenantRegionResidencyAuthorizer"/> - a platform
/// operator or an admin of that tenant, fail-closed and independent of the
/// data-plane default effect. Only then does it, except for
/// <see cref="GetPostureAsync"/>, refuse with
/// <see cref="TenantAccessAdministrationDisabledException"/> while the live
/// delegated-access flag is off, and refuse the reserved default tenant with
/// <see cref="ReservedTenantOperationException"/>. Authorizing first means a caller
/// who may not administer the tenant learns nothing, not even that the feature is
/// off; the order matches the tenant directory facade's.
/// </para>
/// <para>
/// <b>System-origin underlay.</b> Once authorized, every policy-store and directory
/// operation runs under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>, so
/// the authorization above is the single enforcement point and the store's
/// tenant-tier write guard admits this facade's <c>tenant:{T}:</c> writes.
/// </para>
/// <para>
/// <b>Rule identity.</b> The policy store keys a rule by its governed tree and id, so
/// the facade keeps a tenant-local id unique across the tenant's trees: a put that
/// moves a rule to another tree removes the copy it replaced. Reads by local id scan
/// the store (the store has no tenant-prefix scan); the policy is bounded by the
/// tenant's <c>MaxTenantRules</c> cap and the cluster's operator rules.
/// </para>
/// <para>
/// <b>Concurrency.</b> Puts are not serialised (no grain or lock): the cap and the
/// one-copy-per-local-id invariant are kept optimistically by verify-and-compensate
/// in <see cref="PutRuleAsync"/>. Concurrent puts that each pass the cap check
/// re-count after writing and withdraw their own new rule when the count is over
/// the cap, so an overshoot self-corrects and at worst every racer is refused (fail
/// closed). The withdrawal is retried a bounded number of times
/// (<see cref="TenantCapCompensation"/>); if it still fails the put is refused with a
/// <see cref="LatticeQuotaExceededException"/> saying the cap may stay exceeded until
/// the rule is removed. Concurrent puts of one local id to different trees settle on a single
/// copy by a deterministic tie-break. The residual window is the time between a
/// racer's write and its re-count, during which the store can briefly hold more
/// rules than the cap or two copies of one id; and in a pathological interleaving
/// of concurrent moves of one id, every copy can be withdrawn, which also fails
/// closed (the rule grants nothing until re-put).
/// </para>
/// </remarks>
internal sealed partial class LatticeTenantPolicyAdmin : ILatticeTenantPolicyAdmin
{
    /// <summary>The surface name interpolated into the authorizer's denial message.</summary>
    private const string PolicyAction = "access policy";

    /// <summary>
    /// The most rules an introspection result lists, so a pathological policy can
    /// never turn one explain or effective-permissions call into an unbounded reply.
    /// </summary>
    internal const int MaxIntrospectionRules = 1000;

    private readonly TenantRegionResidencyAuthorizer _authorizer;
    private readonly ILatticeAuthorizationPolicyStore _store;
    private readonly ILatticeMembershipDirectory _directory;
    private readonly ITenantPolicyDecisionSource _decisions;
    private readonly ILatticeAccessGate _gate;
    private readonly Func<bool> _isEnabled;
    private readonly ILatticeMembershipContext? _membership;
    private readonly ITenantMembershipUsage? _membershipUsage;
    private readonly ILogger _logger;

    /// <summary>Initializes a new <see cref="LatticeTenantPolicyAdmin"/>.</summary>
    /// <param name="authorizer">The tenant-tier fail-closed authorization seam. Must not be <see langword="null"/>.</param>
    /// <param name="store">The authorization policy store. Must not be <see langword="null"/>.</param>
    /// <param name="directory">The membership directory used to resolve a named subject's groups. Must not be <see langword="null"/>.</param>
    /// <param name="decisions">The two-layer policy decision source with its explain trace. Must not be <see langword="null"/>.</param>
    /// <param name="gate">The core access gate, used for the posture's platform-operator test. Must not be <see langword="null"/>.</param>
    /// <param name="isEnabled">Reads the live delegated tenant access administration flag. Must not be <see langword="null"/>.</param>
    /// <param name="membership">The membership context resolving the caller, or <see langword="null"/> when none is registered (every caller is then anonymous).</param>
    /// <param name="membershipUsage">The tenant group and edge counter for the posture, or <see langword="null"/> to report those usages unmeasured.</param>
    /// <param name="logger">The logger for a cap withdrawal that could not land, or <see langword="null"/> for none.</param>
    /// <exception cref="ArgumentNullException">A required argument is <see langword="null"/>.</exception>
    public LatticeTenantPolicyAdmin(
        TenantRegionResidencyAuthorizer authorizer,
        ILatticeAuthorizationPolicyStore store,
        ILatticeMembershipDirectory directory,
        ITenantPolicyDecisionSource decisions,
        ILatticeAccessGate gate,
        Func<bool> isEnabled,
        ILatticeMembershipContext? membership = null,
        ITenantMembershipUsage? membershipUsage = null,
        ILogger<LatticeTenantPolicyAdmin>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(decisions);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(isEnabled);

        _authorizer = authorizer;
        _store = store;
        _directory = directory;
        _decisions = decisions;
        _gate = gate;
        _isEnabled = isEnabled;
        _membership = membership;
        _membershipUsage = membershipUsage;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// The shared prologue, in the same order as the tenant directory facade:
    /// authorizes the caller over the tenant, then - unless the operation answers
    /// while the feature is off - refuses while the live flag is off, then refuses
    /// the reserved default tenant.
    /// </summary>
    /// <param name="tenant">The parsed tenant.</param>
    /// <param name="operation">The operation name, for the reserved-tenant refusal.</param>
    /// <param name="answersWhileDisabled">Whether the operation answers while the feature is off.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>The authorized tenant's record.</returns>
    private async ValueTask<TenantRecord> AuthorizeAsync(
        TenantId tenant, string operation, bool answersWhileDisabled, CancellationToken cancellationToken)
    {
        var record = await _authorizer
            .AuthorizeTenantAdminAsync(tenant, PolicyAction, cancellationToken)
            .ConfigureAwait(false);

        if (!answersWhileDisabled && !_isEnabled())
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
    /// Resolves the ambient caller through the membership seam: the warm per-call
    /// cache first, otherwise a resolution under system origin (the directory reads
    /// it performs must not re-enter the gate). Anonymous when no membership context
    /// is registered.
    /// </summary>
    private ValueTask<LatticeSubject> ResolveCallerAsync(CancellationToken cancellationToken)
    {
        if (_membership is null)
        {
            return new ValueTask<LatticeSubject>(LatticeSubject.Anonymous);
        }

        return _membership.TryResolveCurrent(out var subject)
            ? new ValueTask<LatticeSubject>(subject)
            : ResolveCallerUncachedAsync(cancellationToken);
    }

    private async ValueTask<LatticeSubject> ResolveCallerUncachedAsync(CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return await _membership!.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The tenant-gate verdict for <paramref name="subject"/> acting as the tenant
    /// <paramref name="record"/> describes: the single-home rule the gate itself
    /// applies (<see cref="LatticeTenantPolicyEngine.ValidateActiveTenant(CompiledTenantPolicy, string, IReadOnlyCollection{string}, TenantId)"/>),
    /// decided over the authoritative record the authorization step just read rather
    /// than the asynchronously rebuilt compiled snapshot, so an explanation never
    /// reports a tenant or membership the registry already holds as missing. Compiled
    /// under the live delegated-access flag, as the gate's own registry confirmation is.
    /// </summary>
    /// <param name="record">The tenant's record, from authorization.</param>
    /// <param name="subject">The subject, carrying its resolved group closure.</param>
    /// <returns>The tenant-gate verdict.</returns>
    private TenantAccessDecision ValidateActingAs(TenantRecord record, LatticeSubject subject) =>
        LatticeTenantPolicyEngine.ValidateActiveTenant(
            CompiledTenantPolicy.Compile([record], _isEnabled()),
            subject.SubjectId,
            subject.GroupIds,
            record.Id);

    private static TenantId ParseTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (!TenantId.TryParse(tenantId, out var tenant))
        {
            throw new ArgumentException($"'{tenantId}' is not a valid tenant id.", nameof(tenantId));
        }

        return tenant;
    }
}
