using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// A hand-rolled <see cref="ITenantAdminService"/> fake: each operation returns
/// a canned result the test scripts, and every call is recorded so a test can
/// assert not only what came back but <em>what the workspace asked for</em>.
/// <para>
/// That recording is what makes the tenant-isolation tests real. Asserting that
/// a refused approval produced an error message would pass even if the call had
/// gone out anyway; asserting that <see cref="ApproveCalls"/> stayed empty
/// proves nothing left the process.
/// </para>
/// </summary>
/// <remarks>
/// Every reply is a fixed literal from <see cref="MyTenantSample"/>, so no test
/// built on this fake depends on timing, ordering, or a live sampler.
/// </remarks>
internal sealed class FakeTenantAdminService : ITenantAdminService
{
    /// <summary>One recorded grant transition: which one, and on which grant.</summary>
    /// <param name="Granter">The granting tenant named in the call.</param>
    /// <param name="Grantee">The grantee tenant named in the call.</param>
    /// <param name="Scope">The scope named in the call.</param>
    internal readonly record struct GrantCall(string Granter, string Grantee, string Scope);

    /// <summary>One recorded admin-subject change.</summary>
    /// <param name="TenantId">The tenant named in the call.</param>
    /// <param name="SubjectId">The subject named in the call.</param>
    internal readonly record struct SubjectCall(string TenantId, string SubjectId);

    /// <summary>One recorded residency change.</summary>
    /// <param name="TenantId">The tenant named in the call.</param>
    /// <param name="Regions">The complete residency set sent.</param>
    internal readonly record struct ResidencyCall(string TenantId, IReadOnlyCollection<string> Regions);

    public TenantOperationResult<ExplorerTenantSummary> CurrentTenant { get; set; } =
        TenantOperationResult<ExplorerTenantSummary>.Success(MyTenantSample.Summary(), "ok");

    public TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>> AccessibleTenants { get; set; } =
        TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
            [MyTenantSample.Summary()],
            "ok");

    public TenantOperationResult<ExplorerTenantDetail> Detail { get; set; } =
        TenantOperationResult<ExplorerTenantDetail>.Success(MyTenantSample.Detail(), "ok");

    public TenantOperationResult<ExplorerTenantQuotaUsage> QuotaUsage { get; set; } =
        TenantOperationResult<ExplorerTenantQuotaUsage>.Success(MyTenantSample.Usage(), "ok");

    public TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>> RegionStatus { get; set; } =
        TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>.Success(
            MyTenantSample.DualResidencyRegions(),
            "ok");

    public TenantOperationResult<ExplorerTenantResidencyChange> ResidencyChange { get; set; } =
        TenantOperationResult<ExplorerTenantResidencyChange>.Success(
            new ExplorerTenantResidencyChange
            {
                TenantId = MyTenantSample.TenantId,
                AddedRegions = [],
                RemovedRegions = ["eastus"],
                Regions = MyTenantSample.SingleResidencyRegions(),
            },
            "ok");

    public TenantOperationResult<ExplorerTenantAdmins> Admins { get; set; } =
        TenantOperationResult<ExplorerTenantAdmins>.Success(
            new ExplorerTenantAdmins
            {
                TenantId = MyTenantSample.TenantId,
                Subjects = ["user:ada", "user:grace"],
            },
            "ok");

    public TenantOperationResult<ExplorerTenantAdminChange> AdminChange { get; set; } =
        TenantOperationResult<ExplorerTenantAdminChange>.Success(
            new ExplorerTenantAdminChange
            {
                TenantId = MyTenantSample.TenantId,
                SubjectId = "user:ada",
                Changed = true,
                Subjects = ["user:grace"],
            },
            "ok");

    public TenantOperationResult<ExplorerTenantGrants> Grants { get; set; } =
        TenantOperationResult<ExplorerTenantGrants>.Success(MyTenantSample.Grants(), "ok");

    public TenantOperationResult<ExplorerTenantGrantChange> GrantChange { get; set; } =
        TenantOperationResult<ExplorerTenantGrantChange>.Success(
            new ExplorerTenantGrantChange(
                MyTenantSample.Grant(state: ExplorerTenantGrantState.Active),
                Changed: true),
            "ok");

    // Recorded calls. Every mutation the workspace makes lands in one of these,
    // so a test can prove a refused action sent nothing.
    public List<GrantCall> OfferCalls { get; } = [];

    public List<GrantCall> ApproveCalls { get; } = [];

    public List<GrantCall> RejectCalls { get; } = [];

    public List<GrantCall> RevokeCalls { get; } = [];

    public List<SubjectCall> AddSubjectCalls { get; } = [];

    public List<SubjectCall> RemoveSubjectCalls { get; } = [];

    public List<ResidencyCall> ResidencyCalls { get; } = [];

    /// <summary>Every tenant id this fake was asked to read or mutate, in call order.</summary>
    public List<string> TenantIdsTouched { get; } = [];

    public int AdminSubjectListCalls { get; private set; }

    public Task<TenantOperationResult<ExplorerTenantSummary>> GetCurrentTenantAsync(
        CancellationToken cancellationToken = default) =>
        Task.FromResult(CurrentTenant);

    public Task<TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>> ListAccessibleTenantsAsync(
        CancellationToken cancellationToken = default) =>
        Task.FromResult(AccessibleTenants);

    public Task<TenantOperationResult<ExplorerTenantDetail>> GetTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        TenantIdsTouched.Add(tenantId);
        return Task.FromResult(Detail);
    }

    public Task<TenantOperationResult<ExplorerTenantCreation>> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("The My Tenant plugin never creates a tenant.");

    public Task<TenantOperationResult<ExplorerTenantStatusChange>> SuspendTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("The My Tenant plugin never suspends a tenant.");

    public Task<TenantOperationResult<ExplorerTenantStatusChange>> ResumeTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("The My Tenant plugin never resumes a tenant.");

    public Task<TenantOperationResult<ExplorerTenantDeletion>> DeleteTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("The My Tenant plugin never deletes a tenant.");

    public Task<TenantOperationResult<ExplorerTenantQuotaLimits>> SetQuotasAsync(
        string tenantId,
        ExplorerTenantQuotaLimits limits,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Authoring quotas is an operator action, not a tenant one.");

    public Task<TenantOperationResult<ExplorerTenantQuotaUsage>> GetQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        TenantIdsTouched.Add(tenantId);
        return Task.FromResult(QuotaUsage);
    }

    public Task<TenantOperationResult<IReadOnlyList<string>>> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Widening the allowed set is an operator action.");

    public Task<TenantOperationResult<ExplorerTenantResidencyChange>> SetResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default)
    {
        TenantIdsTouched.Add(tenantId);
        ResidencyCalls.Add(new ResidencyCall(tenantId, residencyRegions));
        return Task.FromResult(ResidencyChange);
    }

    public Task<TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>> GetRegionStatusAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        TenantIdsTouched.Add(tenantId);
        return Task.FromResult(RegionStatus);
    }

    public Task<TenantOperationResult<ExplorerTenantAdmins>> ListAdminSubjectsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        AdminSubjectListCalls++;
        TenantIdsTouched.Add(tenantId);
        return Task.FromResult(Admins);
    }

    public Task<TenantOperationResult<ExplorerTenantAdminChange>> AddAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        TenantIdsTouched.Add(tenantId);
        AddSubjectCalls.Add(new SubjectCall(tenantId, subjectId));
        return Task.FromResult(AdminChange);
    }

    public Task<TenantOperationResult<ExplorerTenantAdminChange>> RemoveAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        TenantIdsTouched.Add(tenantId);
        RemoveSubjectCalls.Add(new SubjectCall(tenantId, subjectId));
        return Task.FromResult(AdminChange);
    }

    public Task<TenantOperationResult<ExplorerTenantGrants>> ListGrantsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        TenantIdsTouched.Add(tenantId);
        return Task.FromResult(Grants);
    }

    public Task<TenantOperationResult<ExplorerTenantGrantChange>> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        ExplorerTenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        OfferCalls.Add(new GrantCall(granterTenantId, granteeTenantId, scope));
        return Task.FromResult(GrantChange);
    }

    public Task<TenantOperationResult<ExplorerTenantGrantChange>> ApproveGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ApproveCalls.Add(new GrantCall(granterTenantId, granteeTenantId, scope));
        return Task.FromResult(GrantChange);
    }

    public Task<TenantOperationResult<ExplorerTenantGrantChange>> RejectGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        RejectCalls.Add(new GrantCall(granterTenantId, granteeTenantId, scope));
        return Task.FromResult(GrantChange);
    }

    public Task<TenantOperationResult<ExplorerTenantGrantChange>> RevokeGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        RevokeCalls.Add(new GrantCall(granterTenantId, granteeTenantId, scope));
        return Task.FromResult(GrantChange);
    }
}
