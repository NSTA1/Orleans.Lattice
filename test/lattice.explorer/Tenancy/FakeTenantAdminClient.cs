using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// A hand-rolled <see cref="ITenantAdminClient"/> fake that lets a test script
/// the outcome of each call: a canned reply, or a fault - one of the facade's
/// typed refusals, a translated
/// <see cref="LatticeAuthorizationDeniedException"/>, a
/// <see cref="TenancyUnavailableException"/> (the cluster does not serve the
/// surface), or a residual <see cref="Grpc.Core.RpcException"/>.
/// <para>
/// Every reply is a fixed literal from <see cref="SampleTenant"/>, so no test
/// here depends on timing, ordering, or a live sampler.
/// </para>
/// </summary>
internal sealed class FakeTenantAdminClient : ITenantAdminClient
{
    /// <summary>Thrown by every operation when set. The single fault switch.</summary>
    public Exception? Throws { get; set; }

    public TenantDescriptor? CurrentTenantResult { get; set; }

    public IReadOnlyList<TenantDescriptor>? AccessibleTenantsResult { get; set; }

    public TenantStatusReport? TenantResult { get; set; }

    public TenantQuotaUsageReport? UsageResult { get; set; }

    public TenantGrantReport? GrantsResult { get; set; }

    public TenantGrantDescriptor? GrantResult { get; set; }

    public IReadOnlyList<string> SubjectsResult { get; set; } = ["user:ada", "user:grace"];

    public IReadOnlyList<string> AllowedRegionsResult { get; set; } = ["westeurope"];

    public TenantQuotasDescriptor QuotasResult { get; set; } = SampleTenant.Quotas();

    public bool ChangedResult { get; set; } = true;

    public int CascadedTreeCount { get; set; } = 4;

    // Recorded call arguments, so a test can assert what the seam actually sent.
    public int CurrentTenantCallCount { get; private set; }

    public string? LastTenantId { get; private set; }

    public string? LastSubjectId { get; private set; }

    public IReadOnlyCollection<string>? LastAdminSubjects { get; private set; }

    public IReadOnlyCollection<string>? LastRegions { get; private set; }

    public TenantQuotasDescriptor? LastQuotas { get; private set; }

    public string? LastGranterTenantId { get; private set; }

    public string? LastGranteeTenantId { get; private set; }

    public string? LastScope { get; private set; }

    public TenantGrantAccess? LastOperations { get; private set; }

    public string? LastGrantOperation { get; private set; }

    public Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)
    {
        CurrentTenantCallCount++;
        Fault();
        return Task.FromResult(CurrentTenantResult ?? SampleTenant.Descriptor());
    }

    public Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)
    {
        Fault();
        return Task.FromResult(AccessibleTenantsResult ?? [SampleTenant.Descriptor()]);
    }

    public Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(TenantResult ?? SampleTenant.StatusReport(tenantId));
    }

    public Task<TenantCreationResult> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastAdminSubjects = adminSubjects;
        Fault();
        return Task.FromResult(new TenantCreationResult
        {
            TenantId = tenantId,
            Status = TenantLifecycleStatus.Active,
            AdminSubjects = adminSubjects is null ? SubjectsResult : [.. adminSubjects],
        });
    }

    public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(new TenantStatusChangeResult
        {
            TenantId = tenantId,
            PreviousStatus = TenantLifecycleStatus.Active,
            NewStatus = TenantLifecycleStatus.Suspended,
            Changed = ChangedResult,
        });
    }

    public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(new TenantStatusChangeResult
        {
            TenantId = tenantId,
            PreviousStatus = TenantLifecycleStatus.Suspended,
            NewStatus = TenantLifecycleStatus.Active,
            Changed = ChangedResult,
        });
    }

    public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(new TenantDeletionResult
        {
            TenantId = tenantId,
            CascadedTreeCount = CascadedTreeCount,
        });
    }

    public Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId,
        TenantQuotasDescriptor quotas,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastQuotas = quotas;
        Fault();
        return Task.FromResult(new TenantQuotasUpdateResult { TenantId = tenantId, Quotas = quotas });
    }

    public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastRegions = allowedRegions;
        Fault();
        return Task.FromResult(new TenantRegionAuthorizationResult
        {
            TenantId = tenantId,
            AllowedRegions = AllowedRegionsResult,
        });
    }

    public Task<TenantResidencyChangeResult> SetTenantResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastRegions = residencyRegions;
        Fault();
        return Task.FromResult(new TenantResidencyChangeResult
        {
            TenantId = tenantId,
            AddedRegions = ["northeurope"],
            RemovedRegions = [],
            Regions = [SampleTenant.RegionDescriptor()],
        });
    }

    public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(new TenantRegionStatusReport
        {
            TenantId = tenantId,
            Regions = [SampleTenant.RegionDescriptor()],
        });
    }

    public Task<TenantQuotaUsageReport> GetTenantQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(UsageResult ?? SampleTenant.UsageReport(tenantId));
    }

    public Task<TenantAdminSubjectReport> ListTenantAdminSubjectsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(new TenantAdminSubjectReport { TenantId = tenantId, Subjects = SubjectsResult });
    }

    public Task<TenantAdminSubjectChangeResult> AddTenantAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default) =>
        SubjectChange(tenantId, subjectId);

    public Task<TenantAdminSubjectChangeResult> RemoveTenantAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default) =>
        SubjectChange(tenantId, subjectId);

    public Task<TenantGrantReport> ListCrossTenantGrantsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        Fault();
        return Task.FromResult(GrantsResult ?? SampleTenant.GrantReport(tenantId));
    }

    public Task<TenantGrantChangeResult> OfferCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        LastOperations = operations;
        return GrantTransition("offer", granterTenantId, granteeTenantId, scope);
    }

    public Task<TenantGrantChangeResult> ApproveCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default) =>
        GrantTransition("approve", granterTenantId, granteeTenantId, scope);

    public Task<TenantGrantChangeResult> RejectCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default) =>
        GrantTransition("reject", granterTenantId, granteeTenantId, scope);

    public Task<TenantGrantChangeResult> RevokeCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default) =>
        GrantTransition("revoke", granterTenantId, granteeTenantId, scope);

    private Task<TenantAdminSubjectChangeResult> SubjectChange(string tenantId, string subjectId)
    {
        LastTenantId = tenantId;
        LastSubjectId = subjectId;
        Fault();
        return Task.FromResult(new TenantAdminSubjectChangeResult
        {
            TenantId = tenantId,
            SubjectId = subjectId,
            Changed = ChangedResult,
            Subjects = SubjectsResult,
        });
    }

    private Task<TenantGrantChangeResult> GrantTransition(
        string operation,
        string granterTenantId,
        string granteeTenantId,
        string scope)
    {
        LastGrantOperation = operation;
        LastGranterTenantId = granterTenantId;
        LastGranteeTenantId = granteeTenantId;
        LastScope = scope;
        Fault();
        return Task.FromResult(new TenantGrantChangeResult
        {
            Grant = GrantResult ?? SampleTenant.Grant(granter: granterTenantId, grantee: granteeTenantId),
            Changed = ChangedResult,
        });
    }

    private void Fault()
    {
        if (Throws is { } fault)
        {
            throw fault;
        }
    }
}
