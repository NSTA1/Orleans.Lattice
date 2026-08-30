using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// A deterministic <see cref="ITenantAdminService"/> for the Tenants plugin's
/// tests: every reply is a literal the test set, and any operation can be forced
/// to return any classified refusal by name.
/// <para>
/// Nothing here depends on a clock, a timer, a task scheduler, or the order two
/// calls happen to complete in: every method returns an already-completed task
/// over state the test wrote, so a test asserting on it is reproducible.
/// </para>
/// </summary>
internal sealed class FakeTenantAdminService : ITenantAdminService
{
    /// <summary>The operation names <see cref="Failures"/> and <see cref="Calls"/> are keyed by.</summary>
    internal static class Op
    {
        public const string List = "ListAccessibleTenantsAsync";
        public const string Get = "GetTenantAsync";
        public const string Current = "GetCurrentTenantAsync";
        public const string Create = "CreateTenantAsync";
        public const string Suspend = "SuspendTenantAsync";
        public const string Resume = "ResumeTenantAsync";
        public const string Delete = "DeleteTenantAsync";
        public const string SetQuotas = "SetQuotasAsync";
        public const string Usage = "GetQuotaUsageAsync";
        public const string AuthorizeRegions = "AuthorizeAllowedRegionsAsync";
        public const string SetResidency = "SetResidencyAsync";
        public const string RegionStatus = "GetRegionStatusAsync";
        public const string ListAdmins = "ListAdminSubjectsAsync";
        public const string AddAdmin = "AddAdminSubjectAsync";
        public const string RemoveAdmin = "RemoveAdminSubjectAsync";
        public const string ListGrants = "ListGrantsAsync";
        public const string Offer = "OfferGrantAsync";
        public const string Approve = "ApproveGrantAsync";
        public const string Reject = "RejectGrantAsync";
        public const string Revoke = "RevokeGrantAsync";
    }

    /// <summary>The tenants the list operation reports.</summary>
    public List<ExplorerTenantSummary> Tenants { get; } = [];

    /// <summary>The detail the read operation reports, keyed by tenant id.</summary>
    public Dictionary<string, ExplorerTenantDetail> Details { get; } = new(StringComparer.Ordinal);

    /// <summary>The usage the reading operation reports, keyed by tenant id.</summary>
    public Dictionary<string, ExplorerTenantQuotaUsage> Usage { get; } = new(StringComparer.Ordinal);

    /// <summary>The regions the status operation reports, keyed by tenant id.</summary>
    public Dictionary<string, IReadOnlyList<ExplorerTenantRegion>> Regions { get; } =
        new(StringComparer.Ordinal);

    /// <summary>The admin subjects the list operation reports, keyed by tenant id.</summary>
    public Dictionary<string, IReadOnlyList<string>> AdminSubjects { get; } = new(StringComparer.Ordinal);

    /// <summary>The grants the list operation reports, keyed by tenant id.</summary>
    public Dictionary<string, ExplorerTenantGrants> Grants { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Forced refusals keyed by <see cref="Op"/>, so a test can make any single
    /// operation return any classified refusal while every other one still
    /// succeeds.
    /// </summary>
    public Dictionary<string, (TenantOperationStatus Status, string Message)> Failures { get; } =
        new(StringComparer.Ordinal);

    /// <summary>Every operation the workspace invoked, in call order.</summary>
    public List<string> Calls { get; } = [];

    /// <summary>The number of trees the next delete reports cascading through.</summary>
    public int CascadedTreeCount { get; set; }

    /// <summary>Whether the next transition reports having changed anything.</summary>
    public bool ReportsChanged { get; set; } = true;

    /// <summary>The admin subjects an add or remove reports back.</summary>
    public List<string> AdminChangeResult { get; } = [];

    /// <summary>The allowed region ids an authorization reports back.</summary>
    public List<string> AuthorizedRegions { get; } = [];

    /// <summary>The last complete allowed set the workspace asked for.</summary>
    public IReadOnlyCollection<string>? LastAuthorizedRegions { get; private set; }

    /// <summary>The admin subjects the last create asked to seed.</summary>
    public IReadOnlyCollection<string>? LastSeededSubjects { get; private set; }

    /// <summary>The ceilings the last quota save asked for.</summary>
    public ExplorerTenantQuotaLimits? LastQuotaLimits { get; private set; }

    /// <summary>The operations the last offer asked for.</summary>
    public ExplorerTenantGrantAccess? LastOfferedOperations { get; private set; }

    /// <summary>The grant the next transition reports as committed.</summary>
    public ExplorerTenantGrant TransitionResult { get; set; }

    /// <summary>The granter, grantee, and scope of the last grant transition.</summary>
    public List<string> LastGrantArguments { get; } = [];

    /// <summary>Forces <paramref name="operation"/> to report <paramref name="status"/>.</summary>
    /// <param name="operation">One of <see cref="Op"/>'s names.</param>
    /// <param name="status">The refusal to report.</param>
    /// <param name="message">The server message to carry.</param>
    public void Fail(string operation, TenantOperationStatus status, string message = "refused") =>
        Failures[operation] = (status, message);

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantSummary>> GetCurrentTenantAsync(
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.Current);
        return Result(Op.Current, Tenants.Count > 0 ? Tenants[0] : default);
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>> ListAccessibleTenantsAsync(
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.List);
        return Result<IReadOnlyList<ExplorerTenantSummary>>(Op.List, Tenants.ToArray());
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantDetail>> GetTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.Get + ":" + tenantId);
        if (Failures.TryGetValue(Op.Get, out var failure))
        {
            return Task.FromResult(
                TenantOperationResult<ExplorerTenantDetail>.Failure(failure.Status, failure.Message));
        }

        return Details.TryGetValue(tenantId, out var detail)
            ? Task.FromResult(TenantOperationResult<ExplorerTenantDetail>.Success(detail, "ok"))
            : Task.FromResult(TenantOperationResult<ExplorerTenantDetail>.Failure(
                TenantOperationStatus.NotFound,
                "no such tenant"));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantCreation>> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.Create + ":" + tenantId);
        LastSeededSubjects = adminSubjects;
        return Result(
            Op.Create,
            new ExplorerTenantCreation
            {
                TenantId = tenantId,
                Status = ExplorerTenantLifecycle.Active,
                AdminSubjects = adminSubjects?.ToArray() ?? ["caller"],
            });
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantStatusChange>> SuspendTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.Suspend + ":" + tenantId);
        return Result(
            Op.Suspend,
            new ExplorerTenantStatusChange(
                tenantId,
                ExplorerTenantLifecycle.Active,
                ExplorerTenantLifecycle.Suspended,
                ReportsChanged));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantStatusChange>> ResumeTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.Resume + ":" + tenantId);
        return Result(
            Op.Resume,
            new ExplorerTenantStatusChange(
                tenantId,
                ExplorerTenantLifecycle.Suspended,
                ExplorerTenantLifecycle.Active,
                ReportsChanged));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantDeletion>> DeleteTenantAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.Delete + ":" + tenantId);
        return Result(Op.Delete, new ExplorerTenantDeletion(tenantId, CascadedTreeCount));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantQuotaLimits>> SetQuotasAsync(
        string tenantId,
        ExplorerTenantQuotaLimits limits,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.SetQuotas + ":" + tenantId);
        LastQuotaLimits = limits;
        return Result(Op.SetQuotas, limits);
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantQuotaUsage>> GetQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.Usage + ":" + tenantId);
        if (Failures.TryGetValue(Op.Usage, out var failure))
        {
            return Task.FromResult(
                TenantOperationResult<ExplorerTenantQuotaUsage>.Failure(failure.Status, failure.Message));
        }

        return Usage.TryGetValue(tenantId, out var usage)
            ? Task.FromResult(TenantOperationResult<ExplorerTenantQuotaUsage>.Success(usage, "ok"))
            : Task.FromResult(TenantOperationResult<ExplorerTenantQuotaUsage>.Failure(
                TenantOperationStatus.NotFound,
                "no reading"));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<IReadOnlyList<string>>> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.AuthorizeRegions + ":" + tenantId);
        LastAuthorizedRegions = allowedRegions;
        return Result<IReadOnlyList<string>>(Op.AuthorizeRegions, AuthorizedRegions.ToArray());
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantResidencyChange>> SetResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.SetResidency + ":" + tenantId);
        return Result(
            Op.SetResidency,
            new ExplorerTenantResidencyChange
            {
                TenantId = tenantId,
                AddedRegions = residencyRegions.ToArray(),
                RemovedRegions = [],
                Regions = [],
            });
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>> GetRegionStatusAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.RegionStatus + ":" + tenantId);
        if (Failures.TryGetValue(Op.RegionStatus, out var failure))
        {
            return Task.FromResult(
                TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>.Failure(
                    failure.Status,
                    failure.Message));
        }

        return Task.FromResult(TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>.Success(
            Regions.TryGetValue(tenantId, out var regions) ? regions : [],
            "ok"));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantAdmins>> ListAdminSubjectsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.ListAdmins + ":" + tenantId);
        return Result(
            Op.ListAdmins,
            new ExplorerTenantAdmins
            {
                TenantId = tenantId,
                Subjects = AdminSubjects.TryGetValue(tenantId, out var subjects) ? subjects : [],
            });
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantAdminChange>> AddAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.AddAdmin + ":" + tenantId + ":" + subjectId);
        return Result(Op.AddAdmin, AdminChange(tenantId, subjectId));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantAdminChange>> RemoveAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.RemoveAdmin + ":" + tenantId + ":" + subjectId);
        return Result(Op.RemoveAdmin, AdminChange(tenantId, subjectId));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantGrants>> ListGrantsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        Calls.Add(Op.ListGrants + ":" + tenantId);
        return Result(
            Op.ListGrants,
            Grants.TryGetValue(tenantId, out var grants) ? grants : ExplorerTenantGrants.Empty);
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantGrantChange>> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        ExplorerTenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        RecordGrantCall(Op.Offer, granterTenantId, granteeTenantId, scope);
        LastOfferedOperations = operations;
        return Result(Op.Offer, new ExplorerTenantGrantChange(TransitionResult, ReportsChanged));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantGrantChange>> ApproveGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        RecordGrantCall(Op.Approve, granterTenantId, granteeTenantId, scope);
        return Result(Op.Approve, new ExplorerTenantGrantChange(TransitionResult, ReportsChanged));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantGrantChange>> RejectGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        RecordGrantCall(Op.Reject, granterTenantId, granteeTenantId, scope);
        return Result(Op.Reject, new ExplorerTenantGrantChange(TransitionResult, ReportsChanged));
    }

    /// <inheritdoc />
    public Task<TenantOperationResult<ExplorerTenantGrantChange>> RevokeGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        RecordGrantCall(Op.Revoke, granterTenantId, granteeTenantId, scope);
        return Result(Op.Revoke, new ExplorerTenantGrantChange(TransitionResult, ReportsChanged));
    }

    private ExplorerTenantAdminChange AdminChange(string tenantId, string subjectId) => new()
    {
        TenantId = tenantId,
        SubjectId = subjectId,
        Changed = ReportsChanged,
        Subjects = AdminChangeResult.ToArray(),
    };

    private void RecordGrantCall(string operation, string granter, string grantee, string scope)
    {
        Calls.Add(operation);
        LastGrantArguments.Clear();
        LastGrantArguments.Add(granter);
        LastGrantArguments.Add(grantee);
        LastGrantArguments.Add(scope);
    }

    private Task<TenantOperationResult<T>> Result<T>(string operation, T value) =>
        Failures.TryGetValue(operation, out var failure)
            ? Task.FromResult(TenantOperationResult<T>.Failure(failure.Status, failure.Message))
            : Task.FromResult(TenantOperationResult<T>.Success(value, "ok"));
}
