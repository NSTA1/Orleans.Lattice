using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Server-side gRPC interceptor that enforces the configured
/// <see cref="ILatticeAuthApiAuthorizer"/> on every inbound auth-API call.
/// Calls that the authorizer rejects are failed with
/// <see cref="StatusCode.PermissionDenied"/>. Enforcement is scoped to the
/// auth-API service by matching on the service-name prefix, so unrelated gRPC
/// services hosted in the same ASP.NET Core pipeline are unaffected.
/// </summary>
/// <remarks>
/// Registered globally via
/// <c>AddGrpc(o =&gt; o.Interceptors.Add&lt;LatticeAuthApiGrpcAuthInterceptor&gt;())</c>
/// inside
/// <see cref="LatticeAuthApiGrpcServiceCollectionExtensions.AddLatticeAuthApiGrpc"/>.
/// With the default <see cref="DenyAllAuthApiAuthorizer"/> and
/// <see cref="LatticeAuthApiGrpcOptions.RequireAuthorization"/> left at its
/// <see langword="true"/> default, every auth-API call is rejected until a host
/// opts in - the default-deny posture for the membership and policy control
/// plane. This transport meta-authorizer is the first of two gates and is
/// orthogonal to, and runs before, the facade's own per-call administrator check
/// applied to the resolved caller's subject.
/// </remarks>
internal sealed class LatticeAuthApiGrpcAuthInterceptor : Interceptor
{
    private readonly ILatticeAuthApiAuthorizer _authorizer;
    private readonly IOptionsMonitor<LatticeAuthApiGrpcOptions> _options;
    private readonly ILogger<LatticeAuthApiGrpcAuthInterceptor> _logger;

    /// <summary>Initialises the interceptor.</summary>
    public LatticeAuthApiGrpcAuthInterceptor(
        ILatticeAuthApiAuthorizer authorizer,
        IOptionsMonitor<LatticeAuthApiGrpcOptions> options,
        ILogger<LatticeAuthApiGrpcAuthInterceptor> logger)
    {
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _authorizer = authorizer;
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(continuation);

        if (!IsLatticeAuthApiMethod(context.Method))
        {
            return await continuation(request, context).ConfigureAwait(false);
        }

        await EnforceAuthAsync(request, context).ConfigureAwait(false);
        return await continuation(request, context).ConfigureAwait(false);
    }

    private async Task EnforceAuthAsync<TRequest>(TRequest request, ServerCallContext context)
    {
        if (!_options.CurrentValue.RequireAuthorization)
        {
            return;
        }

        var (operation, targetId) = DescribeCall(context.Method, request);
        var authorizationContext = new LatticeAuthApiAuthorizationContext(context, operation, targetId);

        bool authorized;
        try
        {
            authorized = await _authorizer
                .IsAuthorizedAsync(authorizationContext, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(
                StatusCode.Cancelled,
                "Auth-API authorization check was cancelled."));
        }

        if (!authorized)
        {
            _logger.LogWarning(
                "Api.Auth: rejected inbound gRPC call to {Method} - authorizer denied the request.",
                context.Method);
            throw new RpcException(new Status(
                StatusCode.PermissionDenied,
                "Caller is not authorized to reach the Lattice auth API. "
                + "Register a permissive ILatticeAuthApiAuthorizer (or AllowAllAuthApiAuthorizer) to opt in, "
                + "or set LatticeAuthApiGrpcOptions.RequireAuthorization=false when an outer boundary guards the endpoint."));
        }
    }

    /// <summary>
    /// Decodes the inbound call's operation (from the gRPC method name) and the
    /// primary id it administers (from the request payload), so the authorizer
    /// receives a faithful per-operation, per-target description of every
    /// auth-API RPC. The catalog-wide list operations (<c>ListUsers</c>,
    /// <c>ListGroups</c>, <c>ListRules</c>) carry a <see langword="null"/> target.
    /// An unrecognised method maps to
    /// <see cref="LatticeAuthApiOperation.Unknown"/> (never a permissive default)
    /// so a deny-by-default policy refuses it.
    /// </summary>
    /// <remarks>Exposed as <c>internal</c> so the operation/target mapping can be
    /// asserted directly in unit tests without standing up a gRPC server.</remarks>
    internal static (LatticeAuthApiOperation Operation, string? TargetId) DescribeCall<TRequest>(string fullMethodName, TRequest request)
    {
        var methodName = fullMethodName[(fullMethodName.LastIndexOf('/') + 1)..];
        var operation = methodName switch
        {
            LatticeAuthApiGrpcMethods.UpsertUserMethodName => LatticeAuthApiOperation.UpsertUser,
            LatticeAuthApiGrpcMethods.GetUserMethodName => LatticeAuthApiOperation.GetUser,
            LatticeAuthApiGrpcMethods.RemoveUserMethodName => LatticeAuthApiOperation.RemoveUser,
            LatticeAuthApiGrpcMethods.ListUsersMethodName => LatticeAuthApiOperation.ListUsers,
            LatticeAuthApiGrpcMethods.UpsertGroupMethodName => LatticeAuthApiOperation.UpsertGroup,
            LatticeAuthApiGrpcMethods.GetGroupMethodName => LatticeAuthApiOperation.GetGroup,
            LatticeAuthApiGrpcMethods.RemoveGroupMethodName => LatticeAuthApiOperation.RemoveGroup,
            LatticeAuthApiGrpcMethods.ListGroupsMethodName => LatticeAuthApiOperation.ListGroups,
            LatticeAuthApiGrpcMethods.AddMemberMethodName => LatticeAuthApiOperation.AddMember,
            LatticeAuthApiGrpcMethods.RemoveMemberMethodName => LatticeAuthApiOperation.RemoveMember,
            LatticeAuthApiGrpcMethods.ListGroupMembersMethodName => LatticeAuthApiOperation.ListGroupMembers,
            LatticeAuthApiGrpcMethods.ListSubjectGroupsMethodName => LatticeAuthApiOperation.ListSubjectGroups,
            LatticeAuthApiGrpcMethods.PutRuleMethodName => LatticeAuthApiOperation.PutRule,
            LatticeAuthApiGrpcMethods.GetRuleMethodName => LatticeAuthApiOperation.GetRule,
            LatticeAuthApiGrpcMethods.RemoveRuleMethodName => LatticeAuthApiOperation.RemoveRule,
            LatticeAuthApiGrpcMethods.ListRulesMethodName => LatticeAuthApiOperation.ListRules,
            LatticeAuthApiGrpcMethods.ListRulesForTreeMethodName => LatticeAuthApiOperation.ListRulesForTree,
            LatticeAuthApiGrpcMethods.ExplainMethodName => LatticeAuthApiOperation.Explain,
            LatticeAuthApiGrpcMethods.EffectivePermissionsMethodName => LatticeAuthApiOperation.EffectivePermissions,
            LatticeAuthApiGrpcMethods.SearchDirectoryMethodName => LatticeAuthApiOperation.SearchDirectory,
            LatticeAuthApiGrpcMethods.ResolveDirectoryPrincipalMethodName => LatticeAuthApiOperation.ResolveDirectoryPrincipal,
            LatticeAuthApiGrpcMethods.GetAccessModelMethodName => LatticeAuthApiOperation.GetAccessModel,
            _ => LatticeAuthApiOperation.Unknown,
        };

        var targetId = request switch
        {
            AuthUser u => u.UserId,
            AuthUserRef ur => ur.UserId,
            AuthGroup g => g.GroupId,
            AuthGroupRef gr => gr.GroupId,
            AuthMemberRef mr => mr.MemberId,
            AuthMemberEdge me => me.GroupId,
            AuthRuleRef rr => rr.TreeId,
            AuthPutRule pr => pr.Rule?.Scope?.TreeId,
            AuthTreeRulesPage tp => tp.TreeId,
            AuthExplainQuery xq => xq.SubjectId,
            AuthSubjectRef sr => sr.SubjectId,
            AuthPrincipalRef pn => pn.PrincipalId,
            // The catalog-wide list operations and the directory search / access-model
            // read target no single id.
            _ => null,
        };

        return (operation, targetId);
    }

    private static bool IsLatticeAuthApiMethod(string fullMethodName)
    {
        const string ServicePrefix = "/" + LatticeAuthApiGrpcMethods.ServiceName + "/";
        return fullMethodName.StartsWith(ServicePrefix, StringComparison.Ordinal);
    }
}
