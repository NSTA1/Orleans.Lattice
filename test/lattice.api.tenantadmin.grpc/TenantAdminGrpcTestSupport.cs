using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc.Tests;

/// <summary>
/// Configurable in-memory <see cref="ILatticeTenantAdmin"/> facade for the gRPC
/// service tests. Either returns a canned result per operation or throws a
/// pre-seeded exception, so the service's result-mapping and its
/// exception-to-<see cref="StatusCode"/> translation can both be exercised without
/// a real tenancy engine.
/// </summary>
internal sealed class FakeTenantAdmin : ILatticeTenantAdmin
{
    public Exception? Throw { get; set; }

    public string? LastTenantId { get; private set; }

    public IReadOnlyCollection<string>? LastAdminSubjects { get; private set; }

    public TenantQuotasDescriptor? LastQuotas { get; private set; }

    public Task<TenantCreationResult> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastAdminSubjects = adminSubjects;
        return Throw is not null
            ? Task.FromException<TenantCreationResult>(Throw)
            : Task.FromResult(new TenantCreationResult
            {
                TenantId = tenantId,
                Status = TenantLifecycleStatus.Active,
                AdminSubjects = adminSubjects is null ? [] : [.. adminSubjects],
            });
    }

    public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantStatusChangeResult>(Throw)
            : Task.FromResult(new TenantStatusChangeResult
            {
                TenantId = tenantId,
                PreviousStatus = TenantLifecycleStatus.Active,
                NewStatus = TenantLifecycleStatus.Suspended,
                Changed = true,
            });
    }

    public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantStatusChangeResult>(Throw)
            : Task.FromResult(new TenantStatusChangeResult
            {
                TenantId = tenantId,
                PreviousStatus = TenantLifecycleStatus.Suspended,
                NewStatus = TenantLifecycleStatus.Active,
                Changed = true,
            });
    }

    public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantDeletionResult>(Throw)
            : Task.FromResult(new TenantDeletionResult { TenantId = tenantId, CascadedTreeCount = 2 });
    }

    public Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId, TenantQuotasDescriptor quotas, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastQuotas = quotas;
        return Throw is not null
            ? Task.FromException<TenantQuotasUpdateResult>(Throw)
            : Task.FromResult(new TenantQuotasUpdateResult { TenantId = tenantId, Quotas = quotas });
    }
}

/// <summary>A no-op credential bridge that resolves no credential (anonymous).</summary>
internal sealed class NullCredentialBridge : ILatticeTenantAdminApiCredentialBridge
{
    public LatticeCredential? Resolve(ServerCallContext context) => null;
}

/// <summary>
/// Configurable in-memory <see cref="ILatticeTenantSelfService"/> facade for the
/// gRPC self-service tests. Returns canned read-only results per operation, or
/// throws a pre-seeded exception, so the service's self-service result-mapping and
/// its exception-to-<see cref="StatusCode"/> translation can be exercised without a
/// real tenancy engine.
/// </summary>
internal sealed class FakeTenantSelfService : ILatticeTenantSelfService
{
    public Exception? Throw { get; set; }

    public string? LastTenantId { get; private set; }

    public Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default)
        => Throw is not null
            ? Task.FromException<TenantDescriptor>(Throw)
            : Task.FromResult(new TenantDescriptor { TenantId = "acme", Status = TenantLifecycleStatus.Active, IsDefault = false });

    public Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default)
        => Throw is not null
            ? Task.FromException<IReadOnlyList<TenantDescriptor>>(Throw)
            : Task.FromResult<IReadOnlyList<TenantDescriptor>>(new[]
            {
                new TenantDescriptor { TenantId = "acme", Status = TenantLifecycleStatus.Active, IsDefault = false },
                new TenantDescriptor { TenantId = "beta", Status = TenantLifecycleStatus.Suspended, IsDefault = false },
            });

    public Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantStatusReport>(Throw)
            : Task.FromResult(new TenantStatusReport
            {
                TenantId = tenantId,
                Status = TenantLifecycleStatus.Active,
                IsDefault = false,
                Regions = Array.Empty<TenantRegionStatusDescriptor>(),
                Quotas = TenantQuotasDescriptor.Unbounded,
            });
    }
}

/// <summary>
/// Configurable in-memory <see cref="ILatticeTenantRegionAdmin"/> facade for the
/// gRPC region-residency tests. Returns canned results per operation, or throws a
/// pre-seeded exception, so the service's region-residency result-mapping and its
/// exception-to-<see cref="StatusCode"/> translation can be exercised without a
/// real tenancy engine.
/// </summary>
internal sealed class FakeTenantRegionAdmin : ILatticeTenantRegionAdmin
{
    public Exception? Throw { get; set; }

    public string? LastTenantId { get; private set; }

    public IReadOnlyCollection<string>? LastAllowedRegions { get; private set; }

    public IReadOnlyCollection<string>? LastResidencyRegions { get; private set; }

    public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastAllowedRegions = allowedRegions;
        return Throw is not null
            ? Task.FromException<TenantRegionAuthorizationResult>(Throw)
            : Task.FromResult(new TenantRegionAuthorizationResult
            {
                TenantId = tenantId,
                AllowedRegions = allowedRegions is null ? [] : [.. allowedRegions],
            });
    }

    public Task<TenantResidencyChangeResult> SetResidencyAsync(
        string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastResidencyRegions = residencyRegions;
        return Throw is not null
            ? Task.FromException<TenantResidencyChangeResult>(Throw)
            : Task.FromResult(new TenantResidencyChangeResult
            {
                TenantId = tenantId,
                AddedRegions = residencyRegions is null ? [] : [.. residencyRegions],
                RemovedRegions = [],
                Regions = residencyRegions is null
                    ? []
                    : [.. residencyRegions.Select(r => new TenantRegionStatusDescriptor
                    {
                        RegionId = r,
                        Status = TenantRegionLifecycleStatus.Provisioning,
                        IsAllowed = true,
                    })],
            });
    }

    public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantRegionStatusReport>(Throw)
            : Task.FromResult(new TenantRegionStatusReport
            {
                TenantId = tenantId,
                Regions =
                [
                    new TenantRegionStatusDescriptor
                    {
                        RegionId = "eu-west",
                        Status = TenantRegionLifecycleStatus.Online,
                        IsAllowed = true,
                    },
                ],
            });
    }
}

/// <summary>
/// Configurable in-memory <see cref="ILatticeTenantAccessAdmin"/> facade for the
/// gRPC tenant access-administration tests. Returns canned results per operation,
/// or throws a pre-seeded exception, so the service's admin-subject
/// result-mapping and its exception-to-<see cref="StatusCode"/> translation can be
/// exercised without a real tenancy engine.
/// </summary>
internal sealed class FakeTenantAccessAdmin : ILatticeTenantAccessAdmin
{
    private static readonly string[] SeededSubjects = ["alice@example.com", "bob@example.com"];

    public Exception? Throw { get; set; }

    public string? LastTenantId { get; private set; }

    public string? LastSubjectId { get; private set; }

    public Task<TenantAdminSubjectReport> ListAdminSubjectsAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantAdminSubjectReport>(Throw)
            : Task.FromResult(new TenantAdminSubjectReport
            {
                TenantId = tenantId,
                Subjects = SeededSubjects,
            });
    }

    public Task<TenantAdminSubjectChangeResult> AddAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastSubjectId = subjectId;
        return Throw is not null
            ? Task.FromException<TenantAdminSubjectChangeResult>(Throw)
            : Task.FromResult(new TenantAdminSubjectChangeResult
            {
                TenantId = tenantId,
                SubjectId = subjectId,
                Changed = true,
                Subjects = [.. SeededSubjects, subjectId],
            });
    }

    public Task<TenantAdminSubjectChangeResult> RemoveAdminSubjectAsync(
        string tenantId, string subjectId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        LastSubjectId = subjectId;
        return Throw is not null
            ? Task.FromException<TenantAdminSubjectChangeResult>(Throw)
            : Task.FromResult(new TenantAdminSubjectChangeResult
            {
                TenantId = tenantId,
                SubjectId = subjectId,
                Changed = true,
                Subjects = [.. SeededSubjects.Where(s => !string.Equals(s, subjectId, StringComparison.Ordinal))],
            });
    }
}

/// <summary>A fixed auth-scheme source returning a pre-built advertisement.</summary>
internal sealed class FixedAuthSchemeSource(AuthSchemeAdvertisement advertisement) : ILatticeTenantAdminApiAuthSchemeSource
{
    public AuthSchemeAdvertisement GetAdvertisement() => advertisement;
}

/// <summary>
/// Configurable in-memory <see cref="ILatticeTenantGrantAdmin"/> facade for the
/// gRPC cross-tenant grant tests. It records the arguments each RPC delivered - so
/// a test can prove the two tenant ids and the scope reach the facade unaltered
/// and in the right roles - and returns a canned result in a configurable
/// lifecycle state, or throws a pre-seeded exception so the
/// exception-to-<see cref="StatusCode"/> translation can be exercised without a
/// real tenancy engine.
/// </summary>
internal sealed class FakeTenantGrantAdmin : ILatticeTenantGrantAdmin
{
    public Exception? Throw { get; set; }

    public string? LastTenantId { get; private set; }

    public string? LastGranterTenantId { get; private set; }

    public string? LastGranteeTenantId { get; private set; }

    public string? LastScope { get; private set; }

    public TenantGrantAccess LastOperations { get; private set; }

    /// <summary>The state the canned change result reports; set it per test.</summary>
    public TenantGrantLifecycleState State { get; set; } = TenantGrantLifecycleState.Pending;

    public Task<TenantGrantReport> ListGrantsAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantGrantReport>(Throw)
            : Task.FromResult(new TenantGrantReport
            {
                TenantId = tenantId,
                Issued = [Descriptor(tenantId, "beta", "orders", TenantGrantLifecycleState.Active)],
                Received = [Descriptor("gamma", tenantId, "ledger", TenantGrantLifecycleState.Pending)],
            });
    }

    public Task<TenantGrantChangeResult> OfferGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        LastOperations = operations;
        return Record(granterTenantId, granteeTenantId, scope, operations);
    }

    public Task<TenantGrantChangeResult> ApproveGrantAsync(
        string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)
        => Record(granterTenantId, granteeTenantId, scope, TenantGrantAccess.Read);

    public Task<TenantGrantChangeResult> RejectGrantAsync(
        string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)
        => Record(granterTenantId, granteeTenantId, scope, TenantGrantAccess.Read);

    public Task<TenantGrantChangeResult> RevokeGrantAsync(
        string granterTenantId, string granteeTenantId, string scope, CancellationToken cancellationToken = default)
        => Record(granterTenantId, granteeTenantId, scope, TenantGrantAccess.Read);

    private Task<TenantGrantChangeResult> Record(
        string granterTenantId, string granteeTenantId, string scope, TenantGrantAccess operations)
    {
        LastGranterTenantId = granterTenantId;
        LastGranteeTenantId = granteeTenantId;
        LastScope = scope;

        return Throw is not null
            ? Task.FromException<TenantGrantChangeResult>(Throw)
            : Task.FromResult(new TenantGrantChangeResult
            {
                Grant = Descriptor(granterTenantId, granteeTenantId, scope, State, operations),
                Changed = true,
            });
    }

    private static TenantGrantDescriptor Descriptor(
        string granter,
        string grantee,
        string scope,
        TenantGrantLifecycleState state,
        TenantGrantAccess operations = TenantGrantAccess.Read) =>
        new()
        {
            GranterTenantId = granter,
            GranteeTenantId = grantee,
            Scope = scope,
            Operations = operations,
            State = state,
            GrantId = $"1:{grantee}\u001f{scope}",
        };
}

/// <summary>
/// Configurable in-memory <see cref="ILatticeTenantQuotaUsage"/> facade for the
/// gRPC usage-against-quota tests. Returns a canned report built from fixed
/// figures - deliberately mixing a bounded, an unbounded, and a capped-at-zero
/// dimension plus an unmeasured one - or throws a pre-seeded exception, so both
/// the wire round-trip of every nullable and the exception-to-<see cref="StatusCode"/>
/// translation can be exercised without a real tenancy engine or a live sampler.
/// </summary>
internal sealed class FakeTenantQuotaUsage : ILatticeTenantQuotaUsage
{
    public Exception? Throw { get; set; }

    public string? LastTenantId { get; private set; }

    /// <summary>The report the fake returns; replace it to shape a specific case.</summary>
    public TenantQuotaUsageReport Report { get; set; } = new()
    {
        TenantId = "acme",
        IsDefault = false,
        EnforcementScope = TenantQuotaEnforcementScope.PerCluster,
        HasUsage = true,
        Bytes = new TenantQuotaDimensionUsage
        {
            Usage = 4_100,
            Limit = 10_000,
            BurstLimit = 12_000,
            Overage = 0,
            MeteredOverage = 11,
        },
        Keys = new TenantQuotaDimensionUsage
        {
            Usage = 600,
            Limit = 500,
            BurstLimit = 600,
            Overage = 100,
            MeteredOverage = 22,
        },

        // Unbounded: no ceiling at all, which must survive the wire as null.
        MemoryBytes = new TenantQuotaDimensionUsage { Usage = 9_000 },

        // Capped at zero: a real ceiling of nothing, never to be confused with unbounded.
        TreeCount = new TenantQuotaDimensionUsage
        {
            Usage = 3,
            Limit = 0,
            BurstLimit = 0,
            Overage = 3,
        },

        // Unmeasured: a ceiling with no usage figure behind it.
        OpsPerSecond = new TenantQuotaDimensionUsage { Limit = 250, BurstLimit = 290 },
        BurstPercent = 20,
        Quotas = new TenantQuotasDescriptor { MaxBytes = 10_000, MaxKeys = 500, BurstPercent = 20 },
    };

    public Task<TenantQuotaUsageReport> GetQuotaUsageAsync(
        string tenantId, CancellationToken cancellationToken = default)
    {
        LastTenantId = tenantId;
        return Throw is not null
            ? Task.FromException<TenantQuotaUsageReport>(Throw)
            : Task.FromResult(Report with { TenantId = tenantId });
    }
}

/// <summary>
/// In-memory <see cref="CallInvoker"/> that closes the loop between the
/// <see cref="LatticeTenantAdminApiGrpcClient"/> and the
/// <see cref="LatticeTenantAdminGrpcService"/> without a network or a host. Every
/// request and response is serialized and deserialized with the same Orleans
/// serializer the production gRPC marshaller uses, so a round-trip through this
/// invoker exercises the full client-mapping -> wire-encoding -> service ->
/// wire-encoding -> client-mapping path deterministically.
/// </summary>
internal sealed class LoopbackCallInvoker(LatticeTenantAdminGrpcServiceBase service, IServiceProvider serializers)
    : CallInvoker
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        string? host,
        CallOptions options,
        TRequest request)
    {
        var responseTask = DispatchAsync(method, request, options.CancellationToken);
        return new AsyncUnaryCall<TResponse>(
            responseTask,
            Task.FromResult(new global::Grpc.Core.Metadata()),
            () => Status.DefaultSuccess,
            () => new global::Grpc.Core.Metadata(),
            () => { });
    }

    private async Task<TResponse> DispatchAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        var wireRequest = RoundTrip(request);
        var context = new FakeServerCallContext(method.FullName, cancellationToken: cancellationToken);

        object response = method.Name switch
        {
            "CreateTenant" => await service.CreateTenant((TenantAdminCreateRequest)(object)wireRequest, context),
            "SuspendTenant" => await service.SuspendTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "ResumeTenant" => await service.ResumeTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "DeleteTenant" => await service.DeleteTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "SetTenantQuotas" => await service.SetTenantQuotas((TenantAdminSetQuotasRequest)(object)wireRequest, context),
            "GetAuthScheme" => await service.GetAuthScheme((AuthSchemeAdvertisementRequest)(object)wireRequest, context),
            "GetCurrentTenant" => await service.GetCurrentTenant((TenantSelfCurrentRequest)(object)wireRequest, context),
            "ListAccessibleTenants" => await service.ListAccessibleTenants((TenantSelfListRequest)(object)wireRequest, context),
            "GetTenant" => await service.GetTenant((TenantAdminTenantRequest)(object)wireRequest, context),
            "AuthorizeAllowedRegions" => await service.AuthorizeAllowedRegions((TenantAdminRegionSetRequest)(object)wireRequest, context),
            "SetTenantResidency" => await service.SetTenantResidency((TenantAdminRegionSetRequest)(object)wireRequest, context),
            "GetTenantRegionStatus" => await service.GetTenantRegionStatus((TenantAdminTenantRequest)(object)wireRequest, context),
            "GetTenantQuotaUsage" => await service.GetTenantQuotaUsage((TenantAdminTenantRequest)(object)wireRequest, context),
            "ListTenantAdminSubjects" => await service.ListTenantAdminSubjects((TenantAdminTenantRequest)(object)wireRequest, context),
            "AddTenantAdminSubject" => await service.AddTenantAdminSubject((TenantAdminSubjectRequest)(object)wireRequest, context),
            "RemoveTenantAdminSubject" => await service.RemoveTenantAdminSubject((TenantAdminSubjectRequest)(object)wireRequest, context),
            "ListCrossTenantGrants" => await service.ListCrossTenantGrants((TenantAdminTenantRequest)(object)wireRequest, context),
            "OfferCrossTenantGrant" => await service.OfferCrossTenantGrant((TenantAdminGrantOfferRequest)(object)wireRequest, context),
            "ApproveCrossTenantGrant" => await service.ApproveCrossTenantGrant((TenantAdminGrantRequest)(object)wireRequest, context),
            "RejectCrossTenantGrant" => await service.RejectCrossTenantGrant((TenantAdminGrantRequest)(object)wireRequest, context),
            "RevokeCrossTenantGrant" => await service.RevokeCrossTenantGrant((TenantAdminGrantRequest)(object)wireRequest, context),
            _ => throw new NotSupportedException($"Unmapped loopback method '{method.Name}'."),
        };

        return RoundTrip((TResponse)response);
    }

    private T RoundTrip<T>(T value)
    {
        var serializer = serializers.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    public override TResponse BlockingUnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
        throw new NotSupportedException();

    public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, string? host, CallOptions options) =>
        throw new NotSupportedException();
}
