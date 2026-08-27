using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of the transport-agnostic read-only
/// <see cref="ILatticeTenantSelfService"/> tenant self-awareness facade. It is the
/// single narrowest seam at which a caller's tenant context is projected and the
/// tenants it may enumerate or inspect are scoped, fail-closed, to its resolved
/// subject; every transport binding (the MCP tool group) is a thin adapter over
/// this one surface. It is an append-only sibling of <see cref="LatticeTenantAdmin"/>
/// and <see cref="LatticeTenantRegionAdmin"/> and shares their conventions, so the
/// mutating lifecycle facades are unchanged.
/// </summary>
/// <remarks>
/// <para>
/// <b>Leak-free enumeration.</b> Enumeration and inspection are scoped to the
/// caller's resolved subject through the tenancy <see cref="ITenantPolicyEngine"/>:
/// the caller sees only the tenants it administers plus its own non-default
/// current tenant, and <see cref="GetTenantAsync"/> unifies an absent tenant with
/// an inaccessible one into a single <see cref="TenantNotFoundException"/>, so no
/// caller can probe for a tenant outside its authority. An anonymous or unresolved
/// subject administers nothing.
/// </para>
/// <para>
/// <b>Subject resolution mirrors the admin authorizer.</b> The caller subject is
/// resolved through the same membership seam the mutating facades use: a warm
/// synchronous cache hit avoids any directory read, and a cache miss reads the
/// membership directory's own gated trees under a system-origin scope. No
/// authorization gate is consulted here because this facade grants no lifecycle
/// authority; visibility alone is enforced through the policy engine.
/// </para>
/// </remarks>
internal sealed class LatticeTenantSelfService : ILatticeTenantSelfService
{
    private readonly ITenantContextResolver _resolver;
    private readonly ITenantPolicyEngine _policyEngine;
    private readonly ITenantRegistry _registry;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initializes a new <see cref="LatticeTenantSelfService"/>.
    /// </summary>
    /// <param name="resolver">The ambient tenant-context resolver used to report the caller's current tenant. Must not be <c>null</c>.</param>
    /// <param name="policyEngine">The tenancy policy engine that scopes enumeration to the caller's subject. Must not be <c>null</c>.</param>
    /// <param name="registry">The tenancy engine's lifecycle store, read for status and residency. Must not be <c>null</c>.</param>
    /// <param name="membership">
    /// The membership context used to resolve the caller subject, or <c>null</c>
    /// when none is registered (every caller then resolves to
    /// <see cref="LatticeSubject.Anonymous"/>, which administers no tenant).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="resolver"/>, <paramref name="policyEngine"/>, or <paramref name="registry"/> is <c>null</c>.</exception>
    public LatticeTenantSelfService(
        ITenantContextResolver resolver,
        ITenantPolicyEngine policyEngine,
        ITenantRegistry registry,
        ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        ArgumentNullException.ThrowIfNull(policyEngine);
        ArgumentNullException.ThrowIfNull(registry);
        _resolver = resolver;
        _policyEngine = policyEngine;
        _registry = registry;
        _membership = membership;
    }

    /// <inheritdoc />
    public async Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)
    {
        var tenant = await _resolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDenied(tenant);

        var status = await ReadStatusAsync(tenant, cancellationToken).ConfigureAwait(false);
        return new TenantDescriptor
        {
            TenantId = tenant.Value,
            Status = status,
            IsDefault = tenant.IsDefault,
        };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(
        CancellationToken cancellationToken = default)
    {
        var accessible = new SortedSet<string>(StringComparer.Ordinal);

        // The caller's own current tenant is always visible to itself, but the
        // reserved default tenant is the "no tenant" pseudo-tenant, so it is only
        // surfaced when the caller is operating under a real, non-default tenant.
        var current = await _resolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDenied(current);

        if (!current.IsDefault)
        {
            accessible.Add(current.Value);
        }

        // Tenants the caller administers, scoped to its resolved subject. An
        // anonymous or unresolved subject administers nothing, so it fails closed
        // to an empty set.
        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (!subject.IsAnonymous && !string.IsNullOrEmpty(subject.SubjectId))
        {
            foreach (var tenant in _policyEngine.ResolveAllowedTenants(subject.SubjectId))
            {
                accessible.Add(tenant.Value);
            }
        }

        if (accessible.Count == 0)
        {
            return Array.Empty<TenantDescriptor>();
        }

        var descriptors = new List<TenantDescriptor>(accessible.Count);
        foreach (var tenantId in accessible)
        {
            _ = TenantId.TryParse(tenantId, out var tenant);
            var status = await ReadStatusAsync(tenant, cancellationToken).ConfigureAwait(false);
            descriptors.Add(new TenantDescriptor
            {
                TenantId = tenantId,
                Status = status,
                IsDefault = tenant.IsDefault,
            });
        }

        return descriptors;
    }

    /// <inheritdoc />
    public async Task<TenantStatusReport> GetTenantAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);

        if (!await IsAccessibleAsync(tenant, cancellationToken).ConfigureAwait(false))
        {
            // Unify "no such tenant" and "not authorized to see it" into one
            // outcome so a caller can never probe for a tenant outside its
            // authority by distinguishing the two.
            throw new TenantNotFoundException(tenant.Value);
        }

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);

        return new TenantStatusReport
        {
            TenantId = tenant.Value,
            Status = Map(record.Status),
            IsDefault = tenant.IsDefault,
            Regions = BuildDescriptors(record),
            Quotas = TenantQuotasMapping.ToDescriptor(record.Quotas),
        };
    }

    private async Task<bool> IsAccessibleAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        // A caller can always inspect its own non-default current tenant.
        var current = await _resolver.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        if (!current.IsDefault && string.Equals(current.Value, tenant.Value, StringComparison.Ordinal))
        {
            return true;
        }

        var subject = await ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (subject.IsAnonymous || string.IsNullOrEmpty(subject.SubjectId))
        {
            return false;
        }

        foreach (var allowed in _policyEngine.ResolveAllowedTenants(subject.SubjectId))
        {
            if (string.Equals(allowed.Value, tenant.Value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private async Task<TenantLifecycleStatus> ReadStatusAsync(TenantId tenant, CancellationToken cancellationToken)
    {
        // The uninitialised default(TenantId) is the resolver's fail-closed "no
        // valid tenant" sentinel, not the reserved default tenant (whose Value is
        // "default"). It must never be reported as a live tenant: callers guard
        // with ThrowIfDenied before reaching here, so this is a defensive floor
        // rather than a reachable path.
        if (tenant.Value is null)
        {
            throw new LatticeTenantAccessDeniedException();
        }

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        return record is null ? TenantLifecycleStatus.Active : Map(record.Status);
    }

    /// <summary>
    /// Fails closed when the resolver denied the caller's assertion. The resolver
    /// signals a denial by resolving the uninitialised <c>default(TenantId)</c>
    /// "no tenant" value - a <c>null</c> <see cref="TenantId.Value"/>, distinct
    /// from the reserved <see cref="TenantId.Default"/> - which the data plane
    /// already turns into a <see cref="LatticeTenantAccessDeniedException"/>. This
    /// surface must agree: reporting the sentinel as a tenant would answer "which
    /// tenant am I acting as" with a fabricated live descriptor for an assertion
    /// that was actually refused, and would emit an entry with a <c>null</c> id
    /// into the accessible-tenant list.
    /// </summary>
    private static void ThrowIfDenied(TenantId tenant)
    {
        if (tenant.Value is null)
        {
            throw new LatticeTenantAccessDeniedException();
        }
    }

    private ValueTask<LatticeSubject> ResolveSubjectAsync(CancellationToken cancellationToken)
    {
        if (_membership is null)
        {
            return new ValueTask<LatticeSubject>(LatticeSubject.Anonymous);
        }

        // Warm fast path: a cached or anonymous subject resolves synchronously
        // with no directory read, so the system-origin scope is unnecessary.
        if (_membership.TryResolveCurrent(out var subject))
        {
            return new ValueTask<LatticeSubject>(subject);
        }

        // Cache miss: resolution reads the membership directory's own gated trees,
        // so it must run under a system-origin scope to bypass the gate.
        return ResolveUncachedAsync(cancellationToken);
    }

    private async ValueTask<LatticeSubject> ResolveUncachedAsync(CancellationToken cancellationToken)
    {
        using (LatticeSystemOrigin.Enter())
        {
            return await _membership!.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static IReadOnlyList<TenantRegionStatusDescriptor> BuildDescriptors(TenantRecord record)
    {
        var regionIds = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var regionId in record.AllowedRegionIds)
        {
            regionIds.Add(regionId);
        }

        foreach (var entry in record.RegionStatusEntries)
        {
            regionIds.Add(entry.Key);
        }

        var descriptors = new List<TenantRegionStatusDescriptor>(regionIds.Count);
        foreach (var regionId in regionIds)
        {
            descriptors.Add(new TenantRegionStatusDescriptor
            {
                RegionId = regionId,
                Status = Map(record.GetRegionStatus(regionId)),
                IsAllowed = record.IsRegionAllowed(regionId),
            });
        }

        return descriptors;
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

    private static TenantLifecycleStatus Map(TenantStatus status) => status switch
    {
        TenantStatus.Active => TenantLifecycleStatus.Active,
        TenantStatus.Suspended => TenantLifecycleStatus.Suspended,
        _ => TenantLifecycleStatus.Active,
    };

    private static TenantRegionLifecycleStatus Map(TenantRegionStatus status) => status switch
    {
        TenantRegionStatus.Provisioning => TenantRegionLifecycleStatus.Provisioning,
        TenantRegionStatus.Backfilling => TenantRegionLifecycleStatus.Backfilling,
        TenantRegionStatus.Online => TenantRegionLifecycleStatus.Online,
        TenantRegionStatus.Draining => TenantRegionLifecycleStatus.Draining,
        TenantRegionStatus.Offline => TenantRegionLifecycleStatus.Offline,
        TenantRegionStatus.Removed => TenantRegionLifecycleStatus.Removed,
        _ => TenantRegionLifecycleStatus.None,
    };
}
