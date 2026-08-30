using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The in-process implementation of the transport-agnostic
/// <see cref="ILatticeTenantRegionAdmin"/> per-tenant region-residency control
/// facade. It is the single narrowest seam at which every region-residency
/// operation is authorized (fail-closed, two-tier) and applied to the tenancy
/// engine's <see cref="ITenantRegistry"/>; every transport binding is a thin
/// adapter over this one surface. It is a sibling of <see cref="LatticeTenantAdmin"/>,
/// added append-only so the tenant lifecycle facade is unchanged.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two-tier fail-closed authorization.</b> Authorizing the allowed region set is
/// an operator action (<see cref="TenantRegionResidencyAuthorizer.AuthorizeOperatorAsync"/>);
/// setting residency and reading status are tenant-admin actions
/// (<see cref="TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync"/>). Both
/// tiers are independent of the data-plane default effect.
/// </para>
/// <para>
/// <b>Last-writer-wins stamping.</b> Every region field is stamped with a strictly
/// increasing <see cref="ITenantAdminClock"/> clock and the cluster's writer id, so
/// concurrent operator (allowed-set) and tenant-admin (residency) writes converge
/// through the record's per-field CRDT merge rather than clobbering each other.
/// </para>
/// <para>
/// <b>Invariants.</b> Residency is a subset of the allowed set (a residency region
/// must be allowed; an allowed region that is still resident cannot be revoked);
/// the last resident region can never be removed. Both are enforced here,
/// fail-closed.
/// </para>
/// <para>
/// <b>Concurrency of the last-resident-region guard.</b> A pre-write read-check
/// alone cannot hold that invariant over a CRDT-merged store: two callers
/// removing <i>different</i> regions both pass the guard, and because the
/// removals tombstone disjoint keys the join keeps both, emptying residency. So
/// <see cref="SetResidencyAsync"/> also re-checks the <b>committed, merged</b>
/// record the registry returns. Because the pre-write guard has already refused
/// the legitimate single-writer case, a post-merge resident count of zero is by
/// construction evidence of concurrent interference - which is what licenses the
/// self-heal: this call re-asserts <i>only the regions it drained</i>, at
/// strictly later stamps, and then refuses with
/// <see cref="TenantLastRegionException"/>. Re-asserting the whole pre-merge set
/// would resurrect the other caller's legitimate removal, so the repair is
/// always scoped to this call's own keys. Both racing callers are refused and
/// the tenant keeps at least one resident region - the fail-closed direction.
/// </para>
/// </remarks>
internal sealed class LatticeTenantRegionAdmin : ILatticeTenantRegionAdmin
{
    private readonly ITenantRegistry _registry;
    private readonly TenantRegionResidencyAuthorizer _authorizer;
    private readonly ITenantAdminClock _clock;
    private readonly string? _writerId;

    /// <summary>
    /// Initializes a new <see cref="LatticeTenantRegionAdmin"/>.
    /// </summary>
    /// <param name="registry">The tenancy engine's lifecycle store. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The two-tier fail-closed region-residency authorization seam. Must not be <c>null</c>.</param>
    /// <param name="clock">The monotonic clock supplying last-writer-wins stamps. Must not be <c>null</c>.</param>
    /// <param name="clusterOptions">The cluster options supplying the writer id stamped on registry writes. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public LatticeTenantRegionAdmin(
        ITenantRegistry registry,
        TenantRegionResidencyAuthorizer authorizer,
        ITenantAdminClock clock,
        IOptions<ClusterOptions> clusterOptions)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(clusterOptions);

        _registry = registry;
        _authorizer = authorizer;
        _clock = clock;
        _writerId = clusterOptions.Value.ClusterId;
    }

    /// <inheritdoc />
    public async Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        var desired = ValidateRegionSet(allowedRegions, nameof(allowedRegions));

        await _authorizer.AuthorizeOperatorAsync(cancellationToken).ConfigureAwait(false);

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);

        var current = new HashSet<string>(record.AllowedRegionIds, StringComparer.Ordinal);

        // A region still resident cannot be revoked from the allowed set: residency
        // must remain a subset of the allowed set. Reject before applying anything.
        foreach (var regionId in current)
        {
            if (!desired.Contains(regionId) && TenantRegionLifecycle.IsResident(record.GetRegionStatus(regionId)))
            {
                throw new TenantRegionNotAllowedException(tenant.Value, regionId);
            }
        }

        var changed = false;
        foreach (var regionId in desired)
        {
            if (!current.Contains(regionId))
            {
                record.AuthorizeRegion(regionId, _clock.Next(), _writerId);
                changed = true;
            }
        }

        foreach (var regionId in current)
        {
            if (!desired.Contains(regionId))
            {
                record.RevokeRegion(regionId, _clock.Next(), _writerId);
                changed = true;
            }
        }

        // Report the registry's committed CRDT join, not the pre-merge local
        // view, so a concurrent change from another writer is not silently
        // absent from the reported allowed set.
        var merged = changed
            ? await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false)
            : record;

        return new TenantRegionAuthorizationResult
        {
            TenantId = tenant.Value,
            AllowedRegions = merged.AllowedRegionIds,
        };
    }

    /// <inheritdoc />
    public async Task<TenantResidencyChangeResult> SetResidencyAsync(
        string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        var desired = ValidateRegionSet(residencyRegions, nameof(residencyRegions));

        var record = await _authorizer.AuthorizeTenantAdminAsync(tenant, cancellationToken).ConfigureAwait(false);

        // Last-resident-region guard: a tenant must always be resident somewhere, so
        // an empty residency set is refused whenever the tenant is currently resident.
        if (desired.Count == 0)
        {
            if (record.ResidentRegionCount > 0)
            {
                throw new TenantLastRegionException(tenant.Value);
            }

            return new TenantResidencyChangeResult
            {
                TenantId = tenant.Value,
                AddedRegions = [],
                RemovedRegions = [],
                Regions = BuildDescriptors(record),
            };
        }

        // Every desired residency region must be in the operator-authorized set.
        foreach (var regionId in desired)
        {
            if (!record.IsRegionAllowed(regionId))
            {
                throw new TenantRegionNotAllowedException(tenant.Value, regionId);
            }
        }

        // The three collections below are allocated lazily, on first use: the
        // overwhelmingly common calls are a no-op re-assert of the current set
        // and a single add or single removal, so an unconditional allocation
        // would pay for capacity no call uses. A call that changes nothing now
        // allocates nothing at all on this path.
        List<string>? added = null;

        // The removed regions and the status each held before this call: the
        // exact, minimal scope the post-merge self-heal is allowed to
        // re-assert, kept as parallel lists so the prior statuses cost one
        // value-type array rather than a boxed or tuple-per-entry structure.
        List<string>? removed = null;
        List<TenantRegionStatus>? removedPriorStatuses = null;

        // Regions newly requested that are not already resident begin adding.
        foreach (var regionId in desired)
        {
            if (!TenantRegionLifecycle.IsResident(record.GetRegionStatus(regionId)))
            {
                record.SetRegionStatus(regionId, TenantRegionStatus.Provisioning, _clock.Next(), _writerId);
                (added ??= []).Add(regionId);
            }
        }

        // Currently-resident regions dropped from the set begin draining.
        foreach (var entry in record.RegionStatusEntries)
        {
            if (TenantRegionLifecycle.IsResident(entry.Value) && !desired.Contains(entry.Key))
            {
                record.SetRegionStatus(entry.Key, TenantRegionStatus.Draining, _clock.Next(), _writerId);
                (removed ??= []).Add(entry.Key);
                (removedPriorStatuses ??= []).Add(entry.Value);
            }
        }

        var merged = record;
        if (added is not null || removed is not null)
        {
            merged = await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

            // Post-merge re-check of the last-resident-region invariant against
            // the registry's committed join. Only a call that removed something
            // can empty residency, and the pre-write guard has already refused
            // the single-writer case, so a zero resident count here can only
            // mean a concurrent writer's tombstone landed on a different key.
            if (removed is not null && merged.ResidentRegionCount == 0)
            {
                await HealRemovedRegionsAsync(
                    merged, removed, removedPriorStatuses!, cancellationToken).ConfigureAwait(false);
                throw new TenantLastRegionException(tenant.Value);
            }
        }

        added?.Sort(StringComparer.Ordinal);

        // The prior statuses are positional, so sorting the region ids in place
        // would desynchronise them - but the heal has already run by this point
        // and the pairing is dead, so the sort is safe here and nowhere earlier.
        removed?.Sort(StringComparer.Ordinal);

        return new TenantResidencyChangeResult
        {
            TenantId = tenant.Value,
            AddedRegions = added ?? (IReadOnlyList<string>)[],
            RemovedRegions = removed ?? (IReadOnlyList<string>)[],
            Regions = BuildDescriptors(merged),
        };
    }

    /// <summary>
    /// Repairs a residency set a concurrent writer emptied, by re-asserting the
    /// pre-write status of every region <b>this call</b> removed at a strictly
    /// later stamp, so those slots supersede this call's own tombstones while
    /// the other writer's tombstones - on different keys - stand untouched.
    /// </summary>
    /// <remarks>
    /// Scoping the repair to this call's own keys is what keeps it fail-closed:
    /// re-asserting the whole pre-merge set would silently undo the other
    /// caller's legitimate removal. The caller refuses with
    /// <see cref="TenantLastRegionException"/> immediately afterwards, and a
    /// retry terminates because it re-reads the healed record and meets the
    /// ordinary pre-write guard.
    /// </remarks>
    /// <param name="merged">The registry's committed join, mutated in place and written back.</param>
    /// <param name="removed">The region ids this call moved out of residency.</param>
    /// <param name="priorStatuses">Their pre-write statuses, positionally paired with <paramref name="removed"/>.</param>
    /// <param name="cancellationToken">Cancels the repair write.</param>
    private async Task HealRemovedRegionsAsync(
        TenantRecord merged,
        List<string> removed,
        List<TenantRegionStatus> priorStatuses,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < removed.Count; i++)
        {
            merged.SetRegionStatus(removed[i], priorStatuses[i], _clock.Next(), _writerId);
        }

        await _registry.PutAsync(merged, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        var record = await _authorizer.AuthorizeTenantAdminAsync(tenant, cancellationToken).ConfigureAwait(false);

        return new TenantRegionStatusReport
        {
            TenantId = tenant.Value,
            Regions = BuildDescriptors(record),
        };
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

    private static HashSet<string> ValidateRegionSet(IReadOnlyCollection<string> regions, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(regions, parameterName);

        var set = new HashSet<string>(regions.Count, StringComparer.Ordinal);
        foreach (var regionId in regions)
        {
            if (string.IsNullOrEmpty(regionId))
            {
                throw new ArgumentException(
                    "Region ids must not be null or empty.", parameterName);
            }

            set.Add(regionId);
        }

        return set;
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
