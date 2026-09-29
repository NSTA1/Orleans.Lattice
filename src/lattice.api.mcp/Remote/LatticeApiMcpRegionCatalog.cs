using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Region;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The <see cref="ILatticeRegionCatalog"/> the MCP binding advertises regions
/// from, projected from the <see cref="ILatticeApiMcpRegionRouter"/> so discovery
/// and routing share a single source of truth. The router owns the static
/// per-region reachability; this adapter enriches the current region's cluster id
/// from the state facade at read time when the router did not already know it (the
/// in-silo topology resolves the cluster id only at runtime), and scopes the
/// advertised catalog to the calling tenant when one is asserted.
/// </summary>
/// <remarks>
/// <para>
/// <b>Tenant scoping.</b> When the call asserts a non-default active tenant and a
/// tenancy engine can answer for it, the catalog advertises only the regions in
/// that tenant's actionable set (<c>allowed</c> union <c>resident</c>) plus the
/// current region, and annotates each with the tenant's standing through
/// <see cref="LatticeRegionDescriptor.TenantScope"/>. The current region is always
/// present - the caller is already talking to it, so omitting it would break the
/// caller's own session rather than conceal anything.
/// </para>
/// <para>
/// <b>The asserted tenant is validated, never trusted.</b> The active tenant
/// arrives on the caller-supplied <c>lattice-active-tenant</c> header and the MCP
/// head's bridge only syntax-checks it, so this catalog re-resolves it through
/// <see cref="ITenantContextResolver"/> - the same validating seam the data plane
/// uses - and honours it only when the resolved tenant matches the assertion. A
/// caller that asserts a tenant it may not act as is refused rather than served
/// that tenant's region set.
/// </para>
/// <para>
/// <b>Fail-closed.</b> A tenant-asserted call whose standing cannot be established
/// (no tenancy engine reachable, the registry read failed, or the assertion did not
/// validate) degrades to the current region alone, with no <c>tenantScope</c>
/// annotation - so a refused assertion is never echoed back to its author. It never
/// falls back to the full routing topology, which is the disclosure this scoping
/// exists to close.
/// </para>
/// <para>
/// <b>Tenancy off costs nothing.</b> The tenant probe is a single ambient-context
/// read - no service resolution, no allocation - and it is false whenever nothing
/// stamped a tenant, so a cluster with no tenancy add-on keeps the original
/// verbatim-snapshot fast path and its answer is byte-for-byte unchanged.
/// </para>
/// </remarks>
internal sealed class LatticeApiMcpRegionCatalog : ILatticeRegionCatalog
{
    private readonly ILatticeApiMcpRegionRouter _router;
    private readonly IServiceProvider _services;

    /// <summary>Initialises the catalog over the region router and service provider.</summary>
    public LatticeApiMcpRegionCatalog(ILatticeApiMcpRegionRouter router, IServiceProvider services)
    {
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _services = services ?? throw new ArgumentNullException(nameof(services));
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<LatticeRegionDescriptor>> ListRegionsAsync(
        CancellationToken cancellationToken = default)
    {
        var snapshot = _router.Snapshot();
        var verifier = _services.GetService<ILatticeApiMcpRegionIdentityVerifier>();
        var needsEnrichment = NeedsClusterIdEnrichment(snapshot);

        // Cheapest possible tenancy probe: one ambient-context read first, and a
        // single singleton lookup only if something actually asserted a tenant.
        var scope = await ResolveTenantScopeAsync(cancellationToken).ConfigureAwait(false);

        // Fast path: no verification configured, every cluster id already known and
        // no tenant asserted, so return the frozen snapshot verbatim with no
        // allocation - byte-for-byte the pre-tenancy answer, same reference.
        if (verifier is null && !needsEnrichment && scope.IsUnscoped)
        {
            return snapshot;
        }

        var clusterId = needsEnrichment
            ? await ResolveCurrentClusterIdAsync(cancellationToken).ConfigureAwait(false)
            : null;

        // Fail closed: a tenant-asserted call whose assertion did not validate, or
        // whose standing cannot be established, resolves to the unresolved verdict,
        // whose every lookup misses, so the loop below prunes every peer and the
        // caller is left with the current region alone rather than the full
        // topology. A refused assertion also carries a null tenant id, so the
        // caller's own unvalidated header value is never annotated back onto the
        // response.
        var visibility = scope switch
        {
            { IsUnscoped: true } => null,
            { Refused: true } => TenantRegionVisibilityMap.Unresolved,
            _ => await ResolveVisibilityAsync(scope.Resolver!, scope.Tenant, cancellationToken)
                .ConfigureAwait(false),
        };
        var tenantId = scope.Refused ? null : scope.Tenant.Value;

        var result = new List<LatticeRegionDescriptor>(snapshot.Count);
        for (var i = 0; i < snapshot.Count; i++)
        {
            var descriptor = snapshot[i];

            if (descriptor.IsCurrent)
            {
                if (string.IsNullOrEmpty(descriptor.ClusterId) && !string.IsNullOrEmpty(clusterId))
                {
                    descriptor = descriptor with { ClusterId = clusterId };
                }

                // The caller is already talking to the current region, so it is
                // always advertised - annotated truthfully, which may well say the
                // tenant is neither allowed into nor resident in it.
                result.Add(Annotate(descriptor, tenantId, visibility));
                continue;
            }

            // Tenant-scoped discovery: a peer outside the tenant's actionable set
            // (allowed union resident) is not this tenant's business, so it is
            // pruned before the identity probe - which also spares the round trip.
            if (visibility is not null && !IsVisible(visibility, descriptor.RegionId))
            {
                continue;
            }

            // Fail-closed discovery: a peer whose endpoint does not provably reach
            // its own cluster (or is unreachable) is omitted, so a caller is never
            // pointed at a region a subsequent tool call could not honour.
            if (verifier is not null)
            {
                var verdict = await verifier.VerifyAsync(descriptor.RegionId, cancellationToken)
                    .ConfigureAwait(false);
                if (verdict is RegionIdentityVerdict.Mismatch or RegionIdentityVerdict.Unreachable)
                {
                    continue;
                }
            }

            result.Add(Annotate(descriptor, tenantId, visibility));
        }

        return result;
    }

    /// <summary>
    /// Establishes whose catalog this call may see: the unscoped topology, a
    /// validated tenant, or a refusal.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The answer is the unscoped topology when nothing asserted a tenant (the
    /// normal case, including every operator call), when the asserted tenant is the
    /// reserved legacy-adoption default (which names the pre-tenancy behaviour by
    /// definition), or when the cluster has no tenancy engine at all.
    /// </para>
    /// <para>
    /// The last case is load-bearing and not merely defensive. The MCP head's
    /// active-tenant bridge is registered unconditionally, so a caller can put a
    /// <c>lattice-active-tenant</c> header on a cluster running <b>no tenancy
    /// add-on</b> and still stamp an ambient tenant. Scoping on that alone would
    /// change the response shape on a single-tenant cluster purely because a header
    /// was present, and - since there is no tenancy engine to validate the
    /// assertion against - would echo the caller's own unvalidated header value back
    /// as a <c>tenantScope</c> annotation. Requiring a live
    /// <see cref="ITenantRegionVisibilityResolver"/> keeps a tenancy-off cluster
    /// byte-for-byte on its pre-tenancy answer whatever headers the caller sends.
    /// </para>
    /// <para>
    /// <b>Tenancy on: the assertion is re-resolved, not trusted.</b> The header is
    /// a caller-supplied assertion and the bridge that stamps it only checks its
    /// syntax, so the value is put through
    /// <see cref="ITenantContextResolver"/> - which the tenancy add-on registers
    /// alongside the visibility resolver, and which validates the assertion against
    /// the caller's own membership - and honoured only when what comes back is the
    /// tenant that was asserted. A refusal (the resolver returns the uninitialised
    /// "no tenant" value), a mismatch, or a missing resolver all fail closed, so an
    /// unauthorized assertion can neither enumerate another tenant's actionable
    /// region set nor probe whether that tenant exists.
    /// </para>
    /// <para>
    /// The warm path stays await-free: <see cref="ITenantContextResolver.TryResolveCurrent"/>
    /// resolves from the ambient context and a warm membership read, and only a
    /// membership cache miss falls through to the asynchronous resolution.
    /// </para>
    /// </remarks>
    private async ValueTask<TenantScopeDecision> ResolveTenantScopeAsync(CancellationToken cancellationToken)
    {
        if (!LatticeActiveTenantContext.IsActive)
        {
            return TenantScopeDecision.Unscoped;
        }

        var tenant = LatticeActiveTenantContext.Current;
        if (tenant is not { } asserted || asserted.Value is null || asserted.IsDefault)
        {
            return TenantScopeDecision.Unscoped;
        }

        var visibility = _services.GetService<ITenantRegionVisibilityResolver>();
        if (visibility is not { IsActive: true })
        {
            return TenantScopeDecision.Unscoped;
        }

        // Tenancy is on, so the assertion must be validated. The tenancy add-on
        // registers both resolvers together; an absent one means a host wired the
        // visibility resolver without the validating seam, which is not a state in
        // which an assertion may be honoured.
        var context = _services.GetService<ITenantContextResolver>();
        if (context is null)
        {
            return TenantScopeDecision.Deny;
        }

        var resolved = context.TryResolveCurrent(out var warm)
            ? warm
            : await context.ResolveCurrentAsync(cancellationToken).ConfigureAwait(false);

        return resolved == asserted
            ? TenantScopeDecision.Scoped(asserted, visibility)
            : TenantScopeDecision.Deny;
    }

    /// <summary>
    /// The outcome of <see cref="ResolveTenantScopeAsync"/>: unscoped (advertise the
    /// whole topology), scoped to a validated tenant, or refused (advertise the
    /// current region alone, unannotated).
    /// </summary>
    private readonly record struct TenantScopeDecision(
        TenantId Tenant,
        ITenantRegionVisibilityResolver? Resolver,
        bool Refused)
    {
        /// <summary>No tenant is in play; the pre-tenancy answer stands.</summary>
        public static TenantScopeDecision Unscoped => default;

        /// <summary>The assertion did not validate; fail closed.</summary>
        public static TenantScopeDecision Deny => new(default, null, true);

        /// <summary>The assertion validated; scope to it.</summary>
        public static TenantScopeDecision Scoped(
            TenantId tenant, ITenantRegionVisibilityResolver resolver) => new(tenant, resolver, false);

        /// <summary><see langword="true"/> when the call is not tenant-scoped at all.</summary>
        public bool IsUnscoped => Resolver is null && !Refused;
    }

    /// <summary>
    /// Resolves the asserted tenant's per-region standing, falling back to the
    /// fail-closed unresolved verdict when the engine reports it cannot answer.
    /// </summary>
    private static async ValueTask<TenantRegionVisibilityMap> ResolveVisibilityAsync(
        ITenantRegionVisibilityResolver resolver, TenantId tenant, CancellationToken cancellationToken) =>
        await resolver.ResolveAsync(tenant, cancellationToken).ConfigureAwait(false);

    private static bool IsVisible(TenantRegionVisibilityMap visibility, string regionId) =>
        visibility.TryGet(regionId, out var standing) && standing.IsVisible;

    /// <summary>
    /// Stamps the tenant's standing onto a descriptor for a tenant-scoped answer, or
    /// returns it untouched when the call is not tenant-scoped, so a non-tenant
    /// answer carries no annotation at all.
    /// </summary>
    private static LatticeRegionDescriptor Annotate(
        LatticeRegionDescriptor descriptor, string? tenantId, TenantRegionVisibilityMap? visibility)
    {
        if (tenantId is null || visibility is null)
        {
            return descriptor;
        }

        // A miss - including every lookup against the fail-closed unresolved
        // verdict - yields the default "not allowed, no residency" standing, which
        // is the truthful answer for a region the tenant has no relationship with.
        visibility.TryGet(descriptor.RegionId, out var standing);

        return descriptor with
        {
            TenantScope = new LatticeRegionTenantScope
            {
                TenantId = tenantId,
                IsAllowed = standing.IsAllowed,
                Status = MapStatus(standing.Status),
                IsResident = standing.IsResident,
            },
        };
    }

    private static TenantRegionLifecycleStatus MapStatus(TenantRegionResidencyStatus status) => status switch
    {
        TenantRegionResidencyStatus.Provisioning => TenantRegionLifecycleStatus.Provisioning,
        TenantRegionResidencyStatus.Backfilling => TenantRegionLifecycleStatus.Backfilling,
        TenantRegionResidencyStatus.Online => TenantRegionLifecycleStatus.Online,
        TenantRegionResidencyStatus.Draining => TenantRegionLifecycleStatus.Draining,
        TenantRegionResidencyStatus.Offline => TenantRegionLifecycleStatus.Offline,
        TenantRegionResidencyStatus.Removed => TenantRegionLifecycleStatus.Removed,
        _ => TenantRegionLifecycleStatus.None,
    };

    private static bool NeedsClusterIdEnrichment(IReadOnlyList<LatticeRegionDescriptor> snapshot)
    {
        for (var i = 0; i < snapshot.Count; i++)
        {
            if (snapshot[i].IsCurrent && string.IsNullOrEmpty(snapshot[i].ClusterId))
            {
                return true;
            }
        }

        return false;
    }

    private async Task<string?> ResolveCurrentClusterIdAsync(CancellationToken cancellationToken)
    {
        var stateQuery = _services.GetService<ILatticeStateQuery>();
        if (stateQuery is null)
        {
            return null;
        }

        try
        {
            var info = await stateQuery.GetClusterInfoAsync(cancellationToken).ConfigureAwait(false);
            return info.ClusterId;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Discovery is best-effort on the cluster-id decoration only; the
            // region set itself is authoritative from the router.
            return null;
        }
    }
}
