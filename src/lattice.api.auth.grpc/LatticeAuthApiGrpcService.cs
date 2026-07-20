using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Abstract base for the auth-API control gRPC service. Carries the
/// <see cref="BindServiceMethodAttribute"/> that <c>Grpc.AspNetCore</c> reflects
/// against to discover and register the unary admin RPCs.
/// </summary>
/// <remarks>
/// The base/derived split mirrors the codegen shape <c>Grpc.Tools</c> produces
/// for a <c>.proto</c> service: the base type bears the binding metadata the
/// binder discovers, and the derived type is the concrete implementation
/// resolved from DI per request. <c>Grpc.AspNetCore</c> calls
/// <see cref="BindService"/> once at startup with a <see langword="null"/>
/// instance to record method metadata, then resolves the actual instance per
/// request.
/// </remarks>
[BindServiceMethod(typeof(LatticeAuthApiGrpcServiceBase), nameof(BindService))]
internal abstract class LatticeAuthApiGrpcServiceBase
{
    /// <summary>Creates or replaces a user. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthAck> UpsertUser(AuthUser request, ServerCallContext context);

    /// <summary>Reads a user. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthUserResult> GetUser(AuthUserRef request, ServerCallContext context);

    /// <summary>Removes a user. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthAck> RemoveUser(AuthUserRef request, ServerCallContext context);

    /// <summary>Lists a page of users. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthUserPage> ListUsers(AuthPageRequest request, ServerCallContext context);

    /// <summary>Creates or replaces a group. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthAck> UpsertGroup(AuthGroup request, ServerCallContext context);

    /// <summary>Reads a group. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthGroupResult> GetGroup(AuthGroupRef request, ServerCallContext context);

    /// <summary>Removes a group. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthAck> RemoveGroup(AuthGroupRef request, ServerCallContext context);

    /// <summary>Lists a page of groups. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthGroupPage> ListGroups(AuthPageRequest request, ServerCallContext context);

    /// <summary>Adds a membership edge. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthAck> AddMember(AuthMemberEdge request, ServerCallContext context);

    /// <summary>Removes a membership edge. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthAck> RemoveMember(AuthMemberEdge request, ServerCallContext context);

    /// <summary>Lists a group's direct members. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthStringList> ListGroupMembers(AuthGroupRef request, ServerCallContext context);

    /// <summary>Lists a subject's transitive groups. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthStringList> ListSubjectGroups(AuthMemberRef request, ServerCallContext context);

    /// <summary>Creates or replaces a rule. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthAck> PutRule(AuthPutRule request, ServerCallContext context);

    /// <summary>Reads a rule. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthRuleResult> GetRule(AuthRuleRef request, ServerCallContext context);

    /// <summary>Removes a rule. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthRuleRemoved> RemoveRule(AuthRuleRef request, ServerCallContext context);

    /// <summary>Lists a page of rules. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthRulePage> ListRules(AuthPageRequest request, ServerCallContext context);

    /// <summary>Lists a page of a tree's rules. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthRulePage> ListRulesForTree(AuthTreeRulesPage request, ServerCallContext context);

    /// <summary>Explains an authorization verdict. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthExplanation> Explain(AuthExplainQuery request, ServerCallContext context);

    /// <summary>Resolves a subject's effective permissions. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthEffectivePermissions> EffectivePermissions(AuthSubjectRef request, ServerCallContext context);

    /// <summary>Searches the identity directory. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<DirectorySearchResult> SearchDirectory(DirectorySearchRequest request, ServerCallContext context);

    /// <summary>Resolves a single directory principal by id. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AuthDirectoryPrincipalResult> ResolveDirectoryPrincipal(AuthPrincipalRef request, ServerCallContext context);

    /// <summary>Reads the cluster access model. Implemented in <see cref="LatticeAuthApiGrpcService"/>.</summary>
    public abstract Task<AccessModelDescriptor> GetAccessModel(AuthAccessModelQuery request, ServerCallContext context);

    /// <summary>
    /// gRPC binding hook invoked by <c>Grpc.AspNetCore</c>. Called once at
    /// startup with <paramref name="serviceImpl"/> set to <see langword="null"/>
    /// to record method metadata; the actual service instance is resolved per
    /// request from DI.
    /// </summary>
    public static void BindService(ServiceBinderBase binder, LatticeAuthApiGrpcServiceBase? serviceImpl)
    {
        ArgumentNullException.ThrowIfNull(binder);

        var m = LatticeAuthApiGrpcMethodsHolder.Current
            ?? throw new InvalidOperationException(
                "LatticeAuthApiGrpcMethodsHolder.Current was not initialised before BindService. "
                + $"Ensure {nameof(LatticeAuthApiGrpcServiceCollectionExtensions.AddLatticeAuthApiGrpc)} ran and that "
                + $"{nameof(LatticeAuthApiGrpcServiceCollectionExtensions.MapLatticeAuthApiGrpc)} pre-resolved "
                + "LatticeAuthApiGrpcMethods before Grpc.AspNetCore reflected on the service type.");

        if (serviceImpl is null)
        {
            binder.AddMethod(m.UpsertUser, (UnaryServerMethod<AuthUser, AuthAck>?)null);
            binder.AddMethod(m.GetUser, (UnaryServerMethod<AuthUserRef, AuthUserResult>?)null);
            binder.AddMethod(m.RemoveUser, (UnaryServerMethod<AuthUserRef, AuthAck>?)null);
            binder.AddMethod(m.ListUsers, (UnaryServerMethod<AuthPageRequest, AuthUserPage>?)null);
            binder.AddMethod(m.UpsertGroup, (UnaryServerMethod<AuthGroup, AuthAck>?)null);
            binder.AddMethod(m.GetGroup, (UnaryServerMethod<AuthGroupRef, AuthGroupResult>?)null);
            binder.AddMethod(m.RemoveGroup, (UnaryServerMethod<AuthGroupRef, AuthAck>?)null);
            binder.AddMethod(m.ListGroups, (UnaryServerMethod<AuthPageRequest, AuthGroupPage>?)null);
            binder.AddMethod(m.AddMember, (UnaryServerMethod<AuthMemberEdge, AuthAck>?)null);
            binder.AddMethod(m.RemoveMember, (UnaryServerMethod<AuthMemberEdge, AuthAck>?)null);
            binder.AddMethod(m.ListGroupMembers, (UnaryServerMethod<AuthGroupRef, AuthStringList>?)null);
            binder.AddMethod(m.ListSubjectGroups, (UnaryServerMethod<AuthMemberRef, AuthStringList>?)null);
            binder.AddMethod(m.PutRule, (UnaryServerMethod<AuthPutRule, AuthAck>?)null);
            binder.AddMethod(m.GetRule, (UnaryServerMethod<AuthRuleRef, AuthRuleResult>?)null);
            binder.AddMethod(m.RemoveRule, (UnaryServerMethod<AuthRuleRef, AuthRuleRemoved>?)null);
            binder.AddMethod(m.ListRules, (UnaryServerMethod<AuthPageRequest, AuthRulePage>?)null);
            binder.AddMethod(m.ListRulesForTree, (UnaryServerMethod<AuthTreeRulesPage, AuthRulePage>?)null);
            binder.AddMethod(m.Explain, (UnaryServerMethod<AuthExplainQuery, AuthExplanation>?)null);
            binder.AddMethod(m.EffectivePermissions, (UnaryServerMethod<AuthSubjectRef, AuthEffectivePermissions>?)null);
            binder.AddMethod(m.SearchDirectory, (UnaryServerMethod<DirectorySearchRequest, DirectorySearchResult>?)null);
            binder.AddMethod(m.ResolveDirectoryPrincipal, (UnaryServerMethod<AuthPrincipalRef, AuthDirectoryPrincipalResult>?)null);
            binder.AddMethod(m.GetAccessModel, (UnaryServerMethod<AuthAccessModelQuery, AccessModelDescriptor>?)null);
            return;
        }

        binder.AddMethod(m.UpsertUser, new UnaryServerMethod<AuthUser, AuthAck>(serviceImpl.UpsertUser));
        binder.AddMethod(m.GetUser, new UnaryServerMethod<AuthUserRef, AuthUserResult>(serviceImpl.GetUser));
        binder.AddMethod(m.RemoveUser, new UnaryServerMethod<AuthUserRef, AuthAck>(serviceImpl.RemoveUser));
        binder.AddMethod(m.ListUsers, new UnaryServerMethod<AuthPageRequest, AuthUserPage>(serviceImpl.ListUsers));
        binder.AddMethod(m.UpsertGroup, new UnaryServerMethod<AuthGroup, AuthAck>(serviceImpl.UpsertGroup));
        binder.AddMethod(m.GetGroup, new UnaryServerMethod<AuthGroupRef, AuthGroupResult>(serviceImpl.GetGroup));
        binder.AddMethod(m.RemoveGroup, new UnaryServerMethod<AuthGroupRef, AuthAck>(serviceImpl.RemoveGroup));
        binder.AddMethod(m.ListGroups, new UnaryServerMethod<AuthPageRequest, AuthGroupPage>(serviceImpl.ListGroups));
        binder.AddMethod(m.AddMember, new UnaryServerMethod<AuthMemberEdge, AuthAck>(serviceImpl.AddMember));
        binder.AddMethod(m.RemoveMember, new UnaryServerMethod<AuthMemberEdge, AuthAck>(serviceImpl.RemoveMember));
        binder.AddMethod(m.ListGroupMembers, new UnaryServerMethod<AuthGroupRef, AuthStringList>(serviceImpl.ListGroupMembers));
        binder.AddMethod(m.ListSubjectGroups, new UnaryServerMethod<AuthMemberRef, AuthStringList>(serviceImpl.ListSubjectGroups));
        binder.AddMethod(m.PutRule, new UnaryServerMethod<AuthPutRule, AuthAck>(serviceImpl.PutRule));
        binder.AddMethod(m.GetRule, new UnaryServerMethod<AuthRuleRef, AuthRuleResult>(serviceImpl.GetRule));
        binder.AddMethod(m.RemoveRule, new UnaryServerMethod<AuthRuleRef, AuthRuleRemoved>(serviceImpl.RemoveRule));
        binder.AddMethod(m.ListRules, new UnaryServerMethod<AuthPageRequest, AuthRulePage>(serviceImpl.ListRules));
        binder.AddMethod(m.ListRulesForTree, new UnaryServerMethod<AuthTreeRulesPage, AuthRulePage>(serviceImpl.ListRulesForTree));
        binder.AddMethod(m.Explain, new UnaryServerMethod<AuthExplainQuery, AuthExplanation>(serviceImpl.Explain));
        binder.AddMethod(m.EffectivePermissions, new UnaryServerMethod<AuthSubjectRef, AuthEffectivePermissions>(serviceImpl.EffectivePermissions));
        binder.AddMethod(m.SearchDirectory, new UnaryServerMethod<DirectorySearchRequest, DirectorySearchResult>(serviceImpl.SearchDirectory));
        binder.AddMethod(m.ResolveDirectoryPrincipal, new UnaryServerMethod<AuthPrincipalRef, AuthDirectoryPrincipalResult>(serviceImpl.ResolveDirectoryPrincipal));
        binder.AddMethod(m.GetAccessModel, new UnaryServerMethod<AuthAccessModelQuery, AccessModelDescriptor>(serviceImpl.GetAccessModel));
    }
}

/// <summary>
/// Server-side implementation of the auth-API control gRPC service. Adapts each
/// unary RPC onto the transport-agnostic <see cref="ILatticeAuthAdmin"/> facade,
/// stamps the caller identity onto the ambient credential context so the facade's
/// own administrator check (an <c>Admin</c> verdict on the reserved policy tree)
/// sees the real caller, and maps the facade's administrator denial onto
/// <see cref="StatusCode.PermissionDenied"/> carrying only the non-sensitive
/// tree / operation / subject / reason fields (never a policy value) as response
/// trailers.
/// </summary>
/// <remarks>
/// This is the second of the binding's two authorization gates. The coarse
/// transport-level <see cref="ILatticeAuthApiAuthorizer"/> (enforced by the
/// interceptor) runs first and decides whether the RPC may reach the facade at
/// all; the facade's per-call administrator check then runs against the stamped
/// caller. Both gates must pass, and both fail closed.
/// </remarks>
internal sealed class LatticeAuthApiGrpcService : LatticeAuthApiGrpcServiceBase
{
    /// <summary>Trailer key carrying the denied control-plane tree id.</summary>
    internal const string DeniedTreeTrailer = "lattice-denied-tree";

    /// <summary>Trailer key carrying the denied operation.</summary>
    internal const string DeniedOperationTrailer = "lattice-denied-operation";

    /// <summary>Trailer key carrying the denied caller's subject id.</summary>
    internal const string DeniedSubjectTrailer = "lattice-denied-subject";

    /// <summary>Trailer key carrying the gate's denial reason.</summary>
    internal const string DeniedReasonTrailer = "lattice-denied-reason";

    private readonly ILatticeAuthAdmin _admin;
    private readonly ILatticeAuthApiCredentialBridge _credentialBridge;
    private readonly ILogger<LatticeAuthApiGrpcService> _logger;

    /// <summary>
    /// Initialises the service. The <paramref name="methods"/> parameter is
    /// unused in the body but load-bearing on the constructor: resolving it
    /// forces the DI container to build the <see cref="LatticeAuthApiGrpcMethods"/>
    /// singleton (whose factory populates
    /// <see cref="LatticeAuthApiGrpcMethodsHolder.Current"/>) before this service
    /// resolves, so the static <see cref="LatticeAuthApiGrpcServiceBase.BindService"/>
    /// hook always observes a populated holder.
    /// </summary>
    public LatticeAuthApiGrpcService(
        LatticeAuthApiGrpcMethods methods,
        ILatticeAuthAdmin admin,
        ILatticeAuthApiCredentialBridge credentialBridge,
        ILogger<LatticeAuthApiGrpcService> logger)
    {
        ArgumentNullException.ThrowIfNull(methods);
        ArgumentNullException.ThrowIfNull(admin);
        ArgumentNullException.ThrowIfNull(credentialBridge);
        ArgumentNullException.ThrowIfNull(logger);

        _admin = admin;
        _credentialBridge = credentialBridge;
        _logger = logger;
    }

    /// <summary>
    /// Bridges the caller identity on <paramref name="context"/> into the ambient
    /// <see cref="LatticeCredentialContext"/> for the duration of the returned
    /// scope, so the facade's administrator check resolves the caller's subject.
    /// Returns <see langword="null"/> (no scope) when the call carries no
    /// credential, leaving the caller anonymous - which the facade's administrator
    /// check default-denies on every operation. This is orthogonal to, and runs
    /// after, the transport-level <see cref="ILatticeAuthApiAuthorizer"/> gate.
    /// </summary>
    private IDisposable? StampCallerCredential(ServerCallContext context)
    {
        var credential = _credentialBridge.Resolve(context);
        return credential is null ? null : LatticeCredentialContext.With(credential);
    }

    /// <inheritdoc />
    public override Task<AuthAck> UpsertUser(AuthUser request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            await admin.UpsertUserAsync(req, ct).ConfigureAwait(false);
            return new AuthAck();
        });

    /// <inheritdoc />
    public override Task<AuthUserResult> GetUser(AuthUserRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            var user = await admin.GetUserAsync(req.UserId, ct).ConfigureAwait(false);
            return new AuthUserResult { User = user };
        });

    /// <inheritdoc />
    public override Task<AuthAck> RemoveUser(AuthUserRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            await admin.RemoveUserAsync(req.UserId, ct).ConfigureAwait(false);
            return new AuthAck();
        });

    /// <inheritdoc />
    public override Task<AuthUserPage> ListUsers(AuthPageRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, req, ct) => admin.ListUsersAsync(req, ct));

    /// <inheritdoc />
    public override Task<AuthAck> UpsertGroup(AuthGroup request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            await admin.UpsertGroupAsync(req, ct).ConfigureAwait(false);
            return new AuthAck();
        });

    /// <inheritdoc />
    public override Task<AuthGroupResult> GetGroup(AuthGroupRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            var group = await admin.GetGroupAsync(req.GroupId, ct).ConfigureAwait(false);
            return new AuthGroupResult { Group = group };
        });

    /// <inheritdoc />
    public override Task<AuthAck> RemoveGroup(AuthGroupRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            await admin.RemoveGroupAsync(req.GroupId, ct).ConfigureAwait(false);
            return new AuthAck();
        });

    /// <inheritdoc />
    public override Task<AuthGroupPage> ListGroups(AuthPageRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, req, ct) => admin.ListGroupsAsync(req, ct));

    /// <inheritdoc />
    public override Task<AuthAck> AddMember(AuthMemberEdge request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            await admin.AddMemberAsync(req.GroupId, req.MemberId, req.MemberKind, ct).ConfigureAwait(false);
            return new AuthAck();
        });

    /// <inheritdoc />
    public override Task<AuthAck> RemoveMember(AuthMemberEdge request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            await admin.RemoveMemberAsync(req.GroupId, req.MemberId, ct).ConfigureAwait(false);
            return new AuthAck();
        });

    /// <inheritdoc />
    public override Task<AuthStringList> ListGroupMembers(AuthGroupRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            var members = await admin.ListGroupMembersAsync(req.GroupId, ct).ConfigureAwait(false);
            return new AuthStringList { Values = members };
        });

    /// <inheritdoc />
    public override Task<AuthStringList> ListSubjectGroups(AuthMemberRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            var groups = await admin.ListSubjectGroupsAsync(req.MemberId, ct).ConfigureAwait(false);
            return new AuthStringList { Values = groups };
        });

    /// <inheritdoc />
    public override Task<AuthAck> PutRule(AuthPutRule request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            await admin.PutRuleAsync(req.Rule, ct).ConfigureAwait(false);
            return new AuthAck();
        });

    /// <inheritdoc />
    public override Task<AuthRuleResult> GetRule(AuthRuleRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            var rule = await admin.GetRuleAsync(req.TreeId, req.RuleId, ct).ConfigureAwait(false);
            return new AuthRuleResult { Rule = rule };
        });

    /// <inheritdoc />
    public override Task<AuthRuleRemoved> RemoveRule(AuthRuleRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            var removed = await admin.RemoveRuleAsync(req.TreeId, req.RuleId, ct).ConfigureAwait(false);
            return new AuthRuleRemoved { Removed = removed };
        });

    /// <inheritdoc />
    public override Task<AuthRulePage> ListRules(AuthPageRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, req, ct) => admin.ListRulesAsync(req, ct));

    /// <inheritdoc />
    public override Task<AuthRulePage> ListRulesForTree(AuthTreeRulesPage request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, req, ct) => admin.ListRulesForTreeAsync(req.TreeId, req.Page, ct));

    /// <inheritdoc />
    public override Task<AuthExplanation> Explain(AuthExplainQuery request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, req, ct) => admin.ExplainAsync(req.SubjectId, req.Operation, req.Scope, req.SubjectKind, ct));

    /// <inheritdoc />
    public override Task<AuthEffectivePermissions> EffectivePermissions(AuthSubjectRef request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, req, ct) => admin.EffectivePermissionsAsync(req.SubjectId, req.SubjectKind, ct));

    /// <inheritdoc />
    public override Task<DirectorySearchResult> SearchDirectory(DirectorySearchRequest request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, req, ct) => admin.SearchDirectoryAsync(req, ct));

    /// <inheritdoc />
    public override Task<AuthDirectoryPrincipalResult> ResolveDirectoryPrincipal(AuthPrincipalRef request, ServerCallContext context)
        => InvokeAsync(request, context, static async (admin, req, ct) =>
        {
            var principal = await admin.ResolveDirectoryPrincipalAsync(req.PrincipalId, ct).ConfigureAwait(false);
            return new AuthDirectoryPrincipalResult { Principal = principal };
        });

    /// <inheritdoc />
    public override Task<AccessModelDescriptor> GetAccessModel(AuthAccessModelQuery request, ServerCallContext context)
        => InvokeAsync(request, context, static (admin, _, ct) => admin.GetAccessModelAsync(ct));

    private async Task<TResponse> InvokeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<ILatticeAuthAdmin, TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(context);

        using var credentialScope = StampCallerCredential(context);

        try
        {
            return await handler(_admin, request, context.CancellationToken).ConfigureAwait(false);
        }
        catch (RpcException)
        {
            throw;
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            // The facade's administrator check denied the caller. This is expected
            // control flow on a control plane; map to PermissionDenied carrying
            // only the non-sensitive tree / operation / subject / reason fields as
            // trailers - never a policy value.
            throw ToPermissionDenied(ex);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "The auth-API request was cancelled."));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Api.Auth: gRPC call to {Method} failed.", context.Method);
            throw new RpcException(new Status(StatusCode.Internal, "The auth-API request failed."));
        }
    }

    private static RpcException ToPermissionDenied(LatticeAuthorizationDeniedException ex)
    {
        var trailers = new global::Grpc.Core.Metadata
        {
            { DeniedTreeTrailer, ex.TreeId },
            { DeniedOperationTrailer, ex.Operation.ToString() },
            { DeniedSubjectTrailer, ex.SubjectId },
            { DeniedReasonTrailer, ex.Reason },
        };

        return new RpcException(
            new Status(StatusCode.PermissionDenied, ex.Message),
            trailers);
    }
}
