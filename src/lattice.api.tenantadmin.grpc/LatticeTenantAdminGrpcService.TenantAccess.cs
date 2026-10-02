using System.Globalization;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// The delegated tenant access administration RPCs: thin adapters over the optional
/// <see cref="ILatticeTenantDirectoryAdmin"/> and <see cref="ILatticeTenantPolicyAdmin"/>
/// facades, which own every authorization decision.
/// </summary>
internal sealed partial class LatticeTenantAdminGrpcService
{
    /// <summary>
    /// Trailer key carrying the breached quota dimension of a
    /// <see cref="StatusCode.ResourceExhausted"/> refusal (for example a tenant's
    /// <c>MaxGroups</c> cap), so a client can branch without parsing the message.
    /// The same key the data and schema bindings use.
    /// </summary>
    internal const string QuotaDimensionTrailer = "lattice-quota-dimension";

    /// <summary>Trailer key carrying the observed value on the breached dimension. Omitted when the dimension carries no numeric ceiling.</summary>
    internal const string QuotaCurrentTrailer = "lattice-quota-current";

    /// <summary>Trailer key carrying the configured ceiling on the breached dimension. Omitted when the dimension carries no numeric ceiling.</summary>
    internal const string QuotaLimitTrailer = "lattice-quota-limit";

    private const string DirectoryUnimplemented =
        "This cluster does not serve tenant directory administration. Register the tenant "
        + "directory facade (ILatticeTenantDirectoryAdmin) to enable it.";

    private const string PolicyUnimplemented =
        "This cluster does not serve tenant policy administration. Register the tenant "
        + "policy facade (ILatticeTenantPolicyAdmin) to enable it.";

    /// <inheritdoc />
    public override Task<TenantGroupPage> ListTenantGroups(TenantAdminAccessListRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.ListGroupsAsync(req.TenantId, req.Page, ct));

    /// <inheritdoc />
    public override async Task<TenantAdminGroupLookup> GetTenantGroup(TenantAdminGroupRequest request, ServerCallContext context)
    {
        var group = await InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.GetGroupAsync(req.TenantId, req.GroupName, ct)).ConfigureAwait(false);
        return new TenantAdminGroupLookup { Group = group };
    }

    /// <inheritdoc />
    public override Task<TenantGroupDescriptor> UpsertTenantGroup(TenantAdminGroupUpsertRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.UpsertGroupAsync(req.TenantId, req.Group, ct));

    /// <inheritdoc />
    public override Task<TenantGroupRemovalResult> RemoveTenantGroup(TenantAdminGroupRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.RemoveGroupAsync(req.TenantId, req.GroupName, ct));

    /// <inheritdoc />
    public override async Task<TenantAdminGroupMemberList> ListTenantGroupMembers(TenantAdminGroupRequest request, ServerCallContext context)
    {
        var members = await InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.ListGroupMembersAsync(req.TenantId, req.GroupName, ct)).ConfigureAwait(false);
        return new TenantAdminGroupMemberList { Members = members };
    }

    /// <inheritdoc />
    public override Task<TenantMembershipChangeResult> AddTenantGroupMember(TenantAdminGroupMemberRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.AddGroupMemberAsync(req.TenantId, req.GroupName, req.MemberId, req.MemberKind, ct));

    /// <inheritdoc />
    public override Task<TenantMembershipChangeResult> RemoveTenantGroupMember(TenantAdminGroupMemberRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.RemoveGroupMemberAsync(req.TenantId, req.GroupName, req.MemberId, req.MemberKind, ct));

    /// <inheritdoc />
    public override Task<TenantMemberPage> ListTenantMembers(TenantAdminAccessListRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.ListMembersAsync(req.TenantId, req.Page, ct));

    /// <inheritdoc />
    public override Task<TenantMembershipChangeResult> AddTenantMember(TenantAdminMemberRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.AddMemberAsync(req.TenantId, req.SubjectId, req.SubjectKind, ct));

    /// <inheritdoc />
    public override Task<TenantMembershipChangeResult> RemoveTenantMember(TenantAdminMemberRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.RemoveMemberAsync(req.TenantId, req.SubjectId, req.SubjectKind, ct));

    /// <inheritdoc />
    public override Task<TenantSubjectResolution> ResolveTenantSubject(TenantAdminMemberRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_directoryAdmin, DirectoryUnimplemented, request, context,
            static (admin, req, ct) => admin.ResolveSubjectAsync(req.TenantId, req.SubjectId, req.SubjectKind, ct));

    /// <inheritdoc />
    public override Task<TenantRuleView> PutTenantRule(TenantAdminRulePutRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_policyAdmin, PolicyUnimplemented, request, context,
            static (admin, req, ct) => admin.PutRuleAsync(req.TenantId, req.Rule, ct));

    /// <inheritdoc />
    public override async Task<TenantAdminRuleLookup> GetTenantRule(TenantAdminRuleRequest request, ServerCallContext context)
    {
        var rule = await InvokeTenantAccessAsync(_policyAdmin, PolicyUnimplemented, request, context,
            static (admin, req, ct) => admin.GetRuleAsync(req.TenantId, req.RuleId, ct)).ConfigureAwait(false);
        return new TenantAdminRuleLookup { Rule = rule };
    }

    /// <inheritdoc />
    public override async Task<TenantAdminRuleRemoval> RemoveTenantRule(TenantAdminRuleRequest request, ServerCallContext context)
    {
        var removed = await InvokeTenantAccessAsync(_policyAdmin, PolicyUnimplemented, request, context,
            static (admin, req, ct) => admin.RemoveRuleAsync(req.TenantId, req.RuleId, ct)).ConfigureAwait(false);
        return new TenantAdminRuleRemoval { Removed = removed };
    }

    /// <inheritdoc />
    public override Task<TenantRulePage> ListTenantRules(TenantAdminAccessListRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_policyAdmin, PolicyUnimplemented, request, context,
            static (admin, req, ct) => admin.ListRulesAsync(req.TenantId, req.Page, ct));

    /// <inheritdoc />
    public override Task<TenantExplanation> ExplainTenantAccess(TenantAdminExplainRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_policyAdmin, PolicyUnimplemented, request, context,
            static (admin, req, ct) => admin.ExplainAsync(req.TenantId, req.SubjectId, req.TreeName, req.Key, req.Operation, req.SubjectKind, ct));

    /// <inheritdoc />
    public override Task<TenantEffectivePermissions> GetTenantEffectivePermissions(TenantAdminEffectivePermissionsRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_policyAdmin, PolicyUnimplemented, request, context,
            static (admin, req, ct) => admin.EffectivePermissionsAsync(req.TenantId, req.SubjectId, req.TreeName, req.SubjectKind, ct));

    /// <inheritdoc />
    public override Task<TenantAccessPosture> GetTenantAccessPosture(TenantAdminTenantRequest request, ServerCallContext context)
        => InvokeTenantAccessAsync(_policyAdmin, PolicyUnimplemented, request, context,
            static (admin, req, ct) => admin.GetPostureAsync(req.TenantId, ct));

    /// <summary>
    /// Runs a delegated tenant access call under the caller-credential and
    /// asserted-tenant scopes, mapping the facade's outcomes onto gRPC status codes.
    /// Shared by both facades so the directory and policy RPCs cannot drift apart.
    /// Authorization stays entirely at the facade's own tenant-tier, fail-closed
    /// gate (platform operator, or an admin of the tenant directly or through a
    /// group); the transport interceptor only applies the coarse per-operation host
    /// policy on top.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The facade is <b>optional</b>: a host that binds tenant administration without
    /// it serves every other RPC unchanged and answers these
    /// <see cref="StatusCode.Unimplemented"/> rather than faulting at startup.
    /// </para>
    /// <para>
    /// <see cref="TenantAccessAdministrationDisabledException"/> and
    /// <see cref="TenantLastAdminSubjectException"/> derive directly from
    /// <see cref="Exception"/>, so without explicit arms they would reach the caller
    /// as an opaque <c>Internal</c>; both refuse a well-formed request on cluster
    /// state, so both are <c>FailedPrecondition</c>.
    /// <see cref="LatticeQuotaExceededException"/> derives from
    /// <see cref="InvalidOperationException"/> and
    /// <see cref="TenantAccessConfinementException"/> from
    /// <see cref="ArgumentException"/>, so each is caught ahead of its base: a
    /// reached cap is a capacity outcome (<c>ResourceExhausted</c>, as the data and
    /// schema bindings report it), not a precondition failure.
    /// </para>
    /// </remarks>
    private async Task<TResponse> InvokeTenantAccessAsync<TFacade, TRequest, TResponse>(
        TFacade? facade,
        string unimplementedMessage,
        TRequest request,
        ServerCallContext context,
        Func<TFacade, TRequest, CancellationToken, Task<TResponse>> handler)
        where TFacade : class
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        if (facade is null)
        {
            throw new RpcException(new Status(StatusCode.Unimplemented, unimplementedMessage));
        }

        using var activeTenantScope = StampActiveTenant(context);
        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(facade, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The tenant access-administration request was cancelled."));
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (TenantNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (TenantAccessAdministrationDisabledException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (TenantLastAdminSubjectException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (ReservedTenantOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (LatticeQuotaExceededException ex)
        {
            throw ToResourceExhausted(ex);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (TenantAccessConfinementException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        // A fail-closed tenant resolution: the caller has no valid active tenant, or
        // may not act as the one it asserted. An authorization outcome, not a server
        // fault, so it must not fall through to Internal below.
        catch (LatticeTenantAccessDeniedException ex)
        {
            throw new RpcException(new Status(StatusCode.PermissionDenied, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.TenantAdmin: gRPC tenant access-administration call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The tenant access-administration request failed."));
        }
    }

    /// <summary>
    /// Projects a cap refusal onto <see cref="StatusCode.ResourceExhausted"/>,
    /// attaching the breached dimension - and, where it carries one, the observed
    /// value and the ceiling - as trailers. The shared tree the cap protects and the
    /// tenant attribution are deliberately withheld: the dimension is what the caller
    /// needs to act on.
    /// </summary>
    private static RpcException ToResourceExhausted(LatticeQuotaExceededException ex)
    {
        var trailers = new global::Grpc.Core.Metadata();
        if (!string.IsNullOrEmpty(ex.Dimension))
        {
            trailers.Add(QuotaDimensionTrailer, ex.Dimension);
        }

        if (ex.Limit > 0)
        {
            trailers.Add(QuotaCurrentTrailer, ex.Current.ToString(CultureInfo.InvariantCulture));
            trailers.Add(QuotaLimitTrailer, ex.Limit.ToString(CultureInfo.InvariantCulture));
        }

        return new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message), trailers);
    }
}
