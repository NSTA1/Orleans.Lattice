using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// An in-memory cluster behind all six tenant facades, plus the apps facade's
/// installed list: tenants with admin subjects, regions, quotas and usage, and
/// cross-tenant grants, enforcing the same invariants the real facades do (the
/// reserved default tenant, the last admin subject, the last resident region,
/// residency inside the allowed set, and the two-step grant lifecycle). A test
/// makes any call fail with <see cref="Fail"/>, or holds it open with
/// <see cref="Hold"/>, so loading and error states are reached without timers.
/// </summary>
internal sealed class FakeTenancyCluster :
    ILatticeTenantSelfService,
    ILatticeTenantAdmin,
    ILatticeTenantAccessAdmin,
    ILatticeTenantGrantAdmin,
    ILatticeTenantRegionAdmin,
    ILatticeTenantQuotaUsage,
    ILatticeAppsControl
{
    private readonly Dictionary<string, Exception> _failures = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TaskCompletionSource> _holds = new(StringComparer.Ordinal);
    private readonly List<string> _calls = [];
    private int _grantIds;

    /// <summary>Creates the cluster with the reserved default tenant and the caller's tenant <c>acme</c>.</summary>
    public FakeTenancyCluster()
    {
        WithTenant(TenantId.DefaultId, admins: []);
        WithTenant("acme", admins: [Caller]);
    }

    /// <summary>The subject the caller's credential resolves to.</summary>
    public const string Caller = "ops@example.com";

    /// <summary>The tenants, by id.</summary>
    public SortedDictionary<string, FakeTenant> Tenants { get; } = new(StringComparer.Ordinal);

    /// <summary>The cross-tenant grants.</summary>
    public List<TenantGrantDescriptor> GrantList { get; } = [];

    /// <summary>The tenant the caller's credential resolves to.</summary>
    public string CurrentTenant { get; set; } = "acme";

    /// <summary>How many apps the caller's tenant has installed; <see langword="null"/> refuses the listing.</summary>
    public int? InstalledApps { get; set; } = 2;

    /// <summary>
    /// Whether a tenant created through the admin facade starts with no region, as
    /// the cluster creates it, rather than with the fixture's resident eu-west.
    /// </summary>
    public bool CreatesTenantsWithoutRegions { get; set; }

    /// <summary>Every call made, by method name.</summary>
    /// <remarks>
    /// A page's follower records its reads on its own continuation rather than on
    /// the test's thread, so the log is guarded and handed out as a snapshot:
    /// enumerating the live list while a follower appended to it would otherwise
    /// throw "Collection was modified" part-way through an assertion.
    /// </remarks>
    public IReadOnlyList<string> Calls
    {
        get
        {
            lock (_calls)
            {
                return _calls.ToArray();
            }
        }
    }

    /// <summary>Makes every later call to <paramref name="method"/> throw <paramref name="exception"/>.</summary>
    public void Fail(string method, Exception exception) => _failures[method] = exception;

    /// <summary>Lets <paramref name="method"/> succeed again.</summary>
    public void Heal(string method) => _failures.Remove(method);

    /// <summary>Holds every later call to <paramref name="method"/> until the returned source completes.</summary>
    public TaskCompletionSource Hold(string method)
    {
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _holds[method] = hold;
        return hold;
    }

    /// <summary>Adds (or replaces) a tenant.</summary>
    public FakeTenancyCluster WithTenant(
        string tenantId,
        TenantLifecycleStatus status = TenantLifecycleStatus.Active,
        string[]? admins = null,
        bool listed = true)
    {
        var tenant = new FakeTenant { Status = status, Listed = listed };
        foreach (var admin in admins ?? [Caller])
        {
            tenant.Admins.Add(admin);
        }

        tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = "eu-west", Status = TenantRegionLifecycleStatus.Online, IsAllowed = true });
        Tenants[tenantId] = tenant;
        return this;
    }

    /// <summary>Adds a grant.</summary>
    public FakeTenancyCluster WithGrant(string granter, string grantee, string scope, TenantGrantLifecycleState state, TenantGrantAccess access = TenantGrantAccess.Read)
    {
        GrantList.Add(new TenantGrantDescriptor
        {
            GranterTenantId = granter,
            GranteeTenantId = grantee,
            Scope = scope,
            Operations = access,
            State = state,
            GrantId = "g" + (++_grantIds),
        });
        return this;
    }

    /// <summary>A denial like the cluster's.</summary>
    public static LatticeAuthorizationDeniedException Denied() =>
        new("_lattice_policy", LatticeOperation.Admin, Caller, "not permitted");

    // ILatticeTenantSelfService

    /// <inheritdoc />
    public async Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(GetCurrentTenantAsync));
        var tenant = Tenants.GetValueOrDefault(CurrentTenant);
        return new TenantDescriptor { TenantId = CurrentTenant, Status = tenant?.Status ?? TenantLifecycleStatus.Active, IsDefault = CurrentTenant == TenantId.DefaultId };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListAccessibleTenantsAsync));
        return [.. Tenants.Where(pair => pair.Value.Listed && pair.Key != TenantId.DefaultId)
            .Select(pair => new TenantDescriptor { TenantId = pair.Key, Status = pair.Value.Status, IsDefault = false })];
    }

    /// <inheritdoc />
    public async Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(GetTenantAsync));
        var tenant = Find(tenantId);
        if (!tenant.Listed)
        {
            throw new TenantNotFoundException(tenantId);
        }

        return new TenantStatusReport
        {
            TenantId = tenantId,
            Status = tenant.Status,
            IsDefault = tenantId == TenantId.DefaultId,
            Regions = [.. tenant.Regions],
            Quotas = tenant.Quotas,
        };
    }

    // ILatticeTenantAdmin

    /// <inheritdoc />
    public async Task<TenantCreationResult> CreateTenantAsync(string tenantId, IReadOnlyCollection<string>? adminSubjects = null, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(CreateTenantAsync));
        if (Tenants.ContainsKey(tenantId))
        {
            throw new TenantAlreadyExistsException(tenantId);
        }

        var admins = adminSubjects is { Count: > 0 } ? [.. adminSubjects] : new[] { Caller };
        WithTenant(tenantId, admins: admins);
        if (CreatesTenantsWithoutRegions)
        {
            Tenants[tenantId].Regions.Clear();
        }
        return new TenantCreationResult { TenantId = tenantId, Status = TenantLifecycleStatus.Active, AdminSubjects = admins };
    }

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default) =>
        ChangeStatusAsync(nameof(SuspendTenantAsync), tenantId, TenantLifecycleStatus.Suspended);

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default) =>
        ChangeStatusAsync(nameof(ResumeTenantAsync), tenantId, TenantLifecycleStatus.Active);

    /// <inheritdoc />
    public async Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(DeleteTenantAsync));
        Reserved(tenantId, "delete");
        var tenant = Find(tenantId);
        Tenants.Remove(tenantId);
        return new TenantDeletionResult { TenantId = tenantId, CascadedTreeCount = tenant.Trees };
    }

    /// <inheritdoc />
    public async Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(SetTenantQuotasAsync));
        Reserved(tenantId, "set-quotas");
        Find(tenantId).Quotas = quotas;
        return new TenantQuotasUpdateResult { TenantId = tenantId, Quotas = quotas };
    }

    // ILatticeTenantAccessAdmin

    /// <inheritdoc />
    public async Task<TenantAdminSubjectReport> ListAdminSubjectsAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListAdminSubjectsAsync));
        return new TenantAdminSubjectReport { TenantId = tenantId, Subjects = [.. Find(tenantId).Admins] };
    }

    /// <inheritdoc />
    public async Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(AddAdminSubjectAsync));
        var tenant = Find(tenantId);
        var changed = tenant.Admins.Add(subjectId);
        return new TenantAdminSubjectChangeResult { TenantId = tenantId, SubjectId = subjectId, Changed = changed, Subjects = [.. tenant.Admins] };
    }

    /// <inheritdoc />
    public async Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync(string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(RemoveAdminSubjectAsync));
        var tenant = Find(tenantId);
        if (tenant.Admins.Count == 1 && tenant.Admins.Contains(subjectId))
        {
            throw new TenantLastAdminSubjectException(tenantId, subjectId);
        }

        var changed = tenant.Admins.Remove(subjectId);
        return new TenantAdminSubjectChangeResult { TenantId = tenantId, SubjectId = subjectId, Changed = changed, Subjects = [.. tenant.Admins] };
    }

    // ILatticeTenantGrantAdmin

    /// <inheritdoc />
    public async Task<TenantGrantReport> ListGrantsAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListGrantsAsync));
        Find(tenantId);
        return new TenantGrantReport
        {
            TenantId = tenantId,
            Issued = [.. GrantList.Where(grant => grant.GranterTenantId == tenantId)],
            Received = [.. GrantList.Where(grant => grant.GranteeTenantId == tenantId)],
        };
    }

    /// <inheritdoc />
    public async Task<TenantGrantChangeResult> OfferGrantAsync(string granterTenantId, string granteeTenantId, string scope, TenantGrantAccess operations, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(OfferGrantAsync));
        Reserved(granterTenantId, "offer");
        Reserved(granteeTenantId, "offer");
        Find(granterTenantId);
        var index = GrantList.FindIndex(grant => grant.GranterTenantId == granterTenantId && grant.GranteeTenantId == granteeTenantId && grant.Scope == scope);
        if (index >= 0 && GrantList[index].State == TenantGrantLifecycleState.Active)
        {
            throw new TenantGrantTransitionException(granterTenantId, granteeTenantId, scope, TenantGrantLifecycleState.Active, TenantGrantLifecycleState.Pending);
        }

        var grant = new TenantGrantDescriptor
        {
            GranterTenantId = granterTenantId,
            GranteeTenantId = granteeTenantId,
            Scope = scope,
            Operations = operations,
            State = TenantGrantLifecycleState.Pending,
            GrantId = "g" + (++_grantIds),
        };
        if (index >= 0)
        {
            GrantList[index] = grant;
        }
        else
        {
            GrantList.Add(grant);
        }

        return new TenantGrantChangeResult { Grant = grant, Changed = true };
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> ApproveGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default) =>
        TransitionAsync(nameof(ApproveGrantAsync), granterTenantId, granteeTenantId, scope, TenantGrantLifecycleState.Pending, TenantGrantLifecycleState.Active);

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> RejectGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default) =>
        TransitionAsync(nameof(RejectGrantAsync), granterTenantId, granteeTenantId, scope, TenantGrantLifecycleState.Pending, TenantGrantLifecycleState.Rejected);

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> RevokeGrantAsync(string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default) =>
        TransitionAsync(nameof(RevokeGrantAsync), granterTenantId, granteeTenantId, scope, TenantGrantLifecycleState.Active, TenantGrantLifecycleState.Revoked);

    // ILatticeTenantRegionAdmin

    /// <inheritdoc />
    public async Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(AuthorizeAllowedRegionsAsync));
        var tenant = Find(tenantId);
        foreach (var region in tenant.Regions.Where(region => IsResident(region.Status) && !allowedRegions.Contains(region.RegionId)))
        {
            throw new TenantRegionNotAllowedException(tenantId, region.RegionId);
        }

        for (var i = 0; i < tenant.Regions.Count; i++)
        {
            tenant.Regions[i] = tenant.Regions[i] with { IsAllowed = allowedRegions.Contains(tenant.Regions[i].RegionId) };
        }

        foreach (var added in allowedRegions.Where(region => tenant.Regions.All(existing => existing.RegionId != region)))
        {
            tenant.Regions.Add(new TenantRegionStatusDescriptor { RegionId = added, Status = TenantRegionLifecycleStatus.None, IsAllowed = true });
        }

        return new TenantRegionAuthorizationResult { TenantId = tenantId, AllowedRegions = [.. tenant.Regions.Where(region => region.IsAllowed).Select(region => region.RegionId)] };
    }

    /// <inheritdoc />
    public async Task<TenantResidencyChangeResult> SetResidencyAsync(string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(SetResidencyAsync));
        var tenant = Find(tenantId);
        if (residencyRegions.Count == 0)
        {
            throw new TenantLastRegionException(tenantId);
        }

        foreach (var requested in residencyRegions)
        {
            if (tenant.Regions.FirstOrDefault(region => region.RegionId == requested) is not { IsAllowed: true })
            {
                throw new TenantRegionNotAllowedException(tenantId, requested);
            }
        }

        var added = new List<string>();
        var removed = new List<string>();
        for (var i = 0; i < tenant.Regions.Count; i++)
        {
            var region = tenant.Regions[i];
            var wanted = residencyRegions.Contains(region.RegionId);
            if (wanted && !IsResident(region.Status))
            {
                added.Add(region.RegionId);
                tenant.Regions[i] = region with { Status = TenantRegionLifecycleStatus.Provisioning };
            }
            else if (!wanted && IsResident(region.Status))
            {
                removed.Add(region.RegionId);
                tenant.Regions[i] = region with { Status = TenantRegionLifecycleStatus.Draining };
            }
        }

        return new TenantResidencyChangeResult { TenantId = tenantId, AddedRegions = added, RemovedRegions = removed, Regions = [.. tenant.Regions] };
    }

    /// <inheritdoc />
    public async Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(GetTenantRegionStatusAsync));
        return new TenantRegionStatusReport { TenantId = tenantId, Regions = [.. Find(tenantId).Regions] };
    }

    // ILatticeTenantQuotaUsage

    /// <inheritdoc />
    public async Task<TenantQuotaUsageReport> GetQuotaUsageAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(GetQuotaUsageAsync));
        var tenant = Find(tenantId);
        var quotas = tenant.Quotas;
        return new TenantQuotaUsageReport
        {
            TenantId = tenantId,
            IsDefault = tenantId == TenantId.DefaultId,
            EnforcementScope = TenantQuotaEnforcementScope.GlobalConverged,
            HasUsage = tenant.Usage.Count > 0,
            Bytes = Reading(tenant, "bytes", quotas.MaxBytes),
            Keys = Reading(tenant, "keys", quotas.MaxKeys),
            MemoryBytes = Reading(tenant, "memory", quotas.MaxMemoryBytes),
            TreeCount = Reading(tenant, "trees", quotas.MaxTreeCount),
            OpsPerSecond = Reading(tenant, "ops", quotas.MaxOpsPerSecond),
            BurstPercent = quotas.BurstPercent,
            Quotas = quotas,
        };
    }

    // ILatticeAppsControl (only the installed list is read by the area)

    /// <inheritdoc />
    public async Task<AppCatalog> ListAsync(CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListAsync));
        if (InstalledApps is not { } count)
        {
            throw Denied();
        }

        return new AppCatalog
        {
            Apps = [.. Enumerable.Range(0, count).Select(index => new AppSummary
            {
                Slug = "app" + index,
                Version = "1.0.0",
                Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "test" },
            })],
        };
    }

    /// <inheritdoc />
    public Task<AppLifecycleResult> InstallAsync(AppInstallRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppLifecycleResult> EnableAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppLifecycleResult> DisableAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppLifecycleResult> UninstallAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppDescriptor?> DescribeAsync(string appSlug, string? version = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppConsentReport?> GetConsentAsync(string appSlug, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<AppConsentReport> UpdateConsentAsync(AppConsentUpdate request, CancellationToken cancellationToken = default) => throw new NotSupportedException();

    /// <inheritdoc />
    public Task<LatticeAppsCapabilities> GetCapabilitiesAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    private static bool IsResident(TenantRegionLifecycleStatus status) =>
        status is TenantRegionLifecycleStatus.Provisioning or TenantRegionLifecycleStatus.Backfilling or TenantRegionLifecycleStatus.Online;

    private static TenantQuotaDimensionUsage Reading(FakeTenant tenant, string dimension, long? limit)
    {
        long? burst = limit is { } ceiling && tenant.Quotas.BurstPercent > 0 ? ceiling + (ceiling * tenant.Quotas.BurstPercent / 100) : null;
        var usage = tenant.Usage.TryGetValue(dimension, out var value) ? value : (long?)null;
        var overage = usage is { } used && limit is { } cap && used > cap ? used - cap : 0;
        return new TenantQuotaDimensionUsage { Usage = usage, Limit = limit, BurstLimit = burst, Overage = overage };
    }

    private static void Reserved(string tenantId, string operation)
    {
        if (tenantId == TenantId.DefaultId)
        {
            throw new ReservedTenantOperationException(tenantId, operation);
        }
    }

    private FakeTenant Find(string tenantId) =>
        Tenants.TryGetValue(tenantId, out var tenant) ? tenant : throw new TenantNotFoundException(tenantId);

    private async Task<TenantStatusChangeResult> ChangeStatusAsync(string method, string tenantId, TenantLifecycleStatus next)
    {
        await EnterAsync(method);
        if (next == TenantLifecycleStatus.Suspended)
        {
            Reserved(tenantId, "suspend");
        }

        var tenant = Find(tenantId);
        var previous = tenant.Status;
        tenant.Status = next;
        return new TenantStatusChangeResult { TenantId = tenantId, PreviousStatus = previous, NewStatus = next, Changed = previous != next };
    }

    private async Task<TenantGrantChangeResult> TransitionAsync(string method, string granter, string grantee, string scope, TenantGrantLifecycleState from, TenantGrantLifecycleState to)
    {
        await EnterAsync(method);
        var index = GrantList.FindIndex(grant => grant.GranterTenantId == granter && grant.GranteeTenantId == grantee && grant.Scope == scope);
        if (index < 0)
        {
            throw new TenantGrantNotFoundException(granter, grantee, scope);
        }

        var current = GrantList[index];
        if (current.State == to)
        {
            return new TenantGrantChangeResult { Grant = current, Changed = false };
        }

        if (current.State != from)
        {
            throw new TenantGrantTransitionException(granter, grantee, scope, current.State, to);
        }

        GrantList[index] = current with { State = to };
        return new TenantGrantChangeResult { Grant = GrantList[index], Changed = true };
    }

    private async Task EnterAsync(string method)
    {
        lock (_calls)
        {
            _calls.Add(method);
        }

        if (_holds.TryGetValue(method, out var hold))
        {
            await hold.Task;
        }

        if (_failures.TryGetValue(method, out var failure))
        {
            throw failure;
        }
    }
}
