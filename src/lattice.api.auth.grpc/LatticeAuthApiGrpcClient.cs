using Grpc.Core;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Strongly-typed client for the auth-API control gRPC surface. Wraps a gRPC
/// <see cref="CallInvoker"/> and the code-first method definitions, exposing one
/// method per RPC over the same public, Orleans-serialized request/response
/// records the server binds. A .NET admin tool, CLI, or dashboard consumes the
/// control plane through this client rather than hand-rolling channel calls; a
/// non-.NET client typically generates its own stub from the wire contract.
/// </summary>
/// <remarks>
/// The client carries no transport policy of its own: address, TLS, retries,
/// deadlines, and call credentials are configured on the <see cref="CallInvoker"/>
/// / <c>GrpcChannel</c> the caller supplies. Build one with
/// <see cref="Create(CallInvoker, IServiceProvider)"/>, passing a service
/// provider that has Orleans serialization registered (<c>AddSerializer()</c>) so
/// the wire marshallers match the server exactly. Every admin call is gated twice
/// on the server (a transport meta-authorizer and the facade's administrator
/// check); a rejection arrives as a <c>PermissionDenied</c>
/// <see cref="RpcException"/>.
/// </remarks>
public sealed class LatticeAuthApiGrpcClient
{
    private readonly CallInvoker _invoker;
    private readonly LatticeAuthApiGrpcMethods _methods;

    internal LatticeAuthApiGrpcClient(CallInvoker invoker, LatticeAuthApiGrpcMethods methods)
    {
        _invoker = invoker ?? throw new ArgumentNullException(nameof(invoker));
        _methods = methods ?? throw new ArgumentNullException(nameof(methods));
    }

    /// <summary>
    /// Creates a client over <paramref name="callInvoker"/>, building the wire
    /// marshallers from the Orleans serializers resolved out of
    /// <paramref name="serializerProvider"/>.
    /// </summary>
    /// <param name="callInvoker">
    /// The gRPC call invoker, typically <c>channel.CreateCallInvoker()</c>.
    /// </param>
    /// <param name="serializerProvider">
    /// A service provider with Orleans serialization registered
    /// (<c>AddSerializer()</c>), used to resolve the per-message serializers.
    /// </param>
    public static LatticeAuthApiGrpcClient Create(CallInvoker callInvoker, IServiceProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(callInvoker);
        ArgumentNullException.ThrowIfNull(serializerProvider);

        return new LatticeAuthApiGrpcClient(
            callInvoker,
            LatticeAuthApiGrpcMethods.FromServiceProvider(serializerProvider));
    }

    /// <summary>Creates or replaces a user record.</summary>
    public Task<AuthAck> UpsertUserAsync(AuthUser request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.UpsertUser, request, cancellationToken);

    /// <summary>Reads a user record; <see cref="AuthUserResult.User"/> is <see langword="null"/> when none exists.</summary>
    public Task<AuthUserResult> GetUserAsync(AuthUserRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetUser, request, cancellationToken);

    /// <summary>Removes a user record. A no-op when no such user exists.</summary>
    public Task<AuthAck> RemoveUserAsync(AuthUserRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.RemoveUser, request, cancellationToken);

    /// <summary>Reads one page of the user catalog in ascending user-id order.</summary>
    public Task<AuthUserPage> ListUsersAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListUsers, request, cancellationToken);

    /// <summary>Creates or replaces a group record.</summary>
    public Task<AuthAck> UpsertGroupAsync(AuthGroup request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.UpsertGroup, request, cancellationToken);

    /// <summary>Reads a group record; <see cref="AuthGroupResult.Group"/> is <see langword="null"/> when none exists.</summary>
    public Task<AuthGroupResult> GetGroupAsync(AuthGroupRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetGroup, request, cancellationToken);

    /// <summary>Removes a group record. A no-op when no such group exists.</summary>
    public Task<AuthAck> RemoveGroupAsync(AuthGroupRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.RemoveGroup, request, cancellationToken);

    /// <summary>Reads one page of the group catalog in ascending group-id order.</summary>
    public Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListGroups, request, cancellationToken);

    /// <summary>Adds a membership edge. Idempotent.</summary>
    public Task<AuthAck> AddMemberAsync(AuthMemberEdge request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.AddMember, request, cancellationToken);

    /// <summary>Removes a membership edge. A no-op when the edge does not exist.</summary>
    public Task<AuthAck> RemoveMemberAsync(AuthMemberEdge request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.RemoveMember, request, cancellationToken);

    /// <summary>Returns the direct members of a group, in ascending ordinal order.</summary>
    public Task<AuthStringList> ListGroupMembersAsync(AuthGroupRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListGroupMembers, request, cancellationToken);

    /// <summary>Returns the transitive set of groups a member belongs to, in ascending ordinal order.</summary>
    public Task<AuthStringList> ListSubjectGroupsAsync(AuthMemberRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListSubjectGroups, request, cancellationToken);

    /// <summary>Creates or replaces an authorization rule.</summary>
    public Task<AuthAck> PutRuleAsync(AuthPutRule request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.PutRule, request, cancellationToken);

    /// <summary>Reads a single rule; <see cref="AuthRuleResult.Rule"/> is <see langword="null"/> when none exists.</summary>
    public Task<AuthRuleResult> GetRuleAsync(AuthRuleRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetRule, request, cancellationToken);

    /// <summary>Removes a rule; <see cref="AuthRuleRemoved.Removed"/> reports whether one matched.</summary>
    public Task<AuthRuleRemoved> RemoveRuleAsync(AuthRuleRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.RemoveRule, request, cancellationToken);

    /// <summary>Reads one page of every rule in the store, ordered by (governed tree id, rule id).</summary>
    public Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListRules, request, cancellationToken);

    /// <summary>Reads one page of the rules governing a single tree, ordered by rule id.</summary>
    public Task<AuthRulePage> ListRulesForTreeAsync(AuthTreeRulesPage request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ListRulesForTree, request, cancellationToken);

    /// <summary>Explains whether a subject may perform an operation over a scope, with the applying rules.</summary>
    public Task<AuthExplanation> ExplainAsync(AuthExplainQuery request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.Explain, request, cancellationToken);

    /// <summary>Returns the authorization rules currently in effect for a subject.</summary>
    public Task<AuthEffectivePermissions> EffectivePermissionsAsync(AuthSubjectRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.EffectivePermissions, request, cancellationToken);

    /// <summary>Searches or browses the configured identity directory for matching principals.</summary>
    public Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.SearchDirectory, request, cancellationToken);

    /// <summary>Resolves a single directory principal by id; <see cref="AuthDirectoryPrincipalResult.Principal"/> is <see langword="null"/> when none exists.</summary>
    public Task<AuthDirectoryPrincipalResult> ResolveDirectoryPrincipalAsync(AuthPrincipalRef request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.ResolveDirectoryPrincipal, request, cancellationToken);

    /// <summary>Reads the cluster's best-effort access model.</summary>
    public Task<AccessModelDescriptor> GetAccessModelAsync(AuthAccessModelQuery request, CancellationToken cancellationToken = default)
        => UnaryAsync(_methods.GetAccessModel, request, cancellationToken);

    private async Task<TResponse> UnaryAsync<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        CancellationToken cancellationToken)
        where TRequest : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        using var call = _invoker.AsyncUnaryCall(
            method,
            host: null,
            new CallOptions(cancellationToken: cancellationToken),
            request);

        return await call.ResponseAsync.ConfigureAwait(false);
    }
}
