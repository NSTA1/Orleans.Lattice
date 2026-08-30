using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Membership;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The production <see cref="IAuthAdminClient"/>. Builds a
/// <see cref="LatticeAuthApiGrpcClient"/> over a gRPC channel to the currently
/// configured endpoint, attaching the same sign-in the state connection uses
/// (read from <see cref="IExplorerAuthSession.CurrentAuthentication"/>). The
/// channel is rebuilt lazily whenever the endpoint or the sign-in changes, so a
/// reconnect or a login is picked up without a restart. A gRPC
/// <see cref="StatusCode.PermissionDenied"/> / <see cref="StatusCode.Unauthenticated"/>
/// is translated back to <see cref="LatticeAuthorizationDeniedException"/> so the
/// rest of the explorer handles a single typed denial. This is the one place the
/// auth-admin control plane's channel, credential, and cancellation live, exactly
/// as <c>GrpcBackupControlClient</c> is for the backup control plane.
/// </summary>
public sealed class GrpcAuthAdminClient : IAuthAdminClient, IDisposable
{
    private readonly IExplorerSession _session;
    private readonly IExplorerAuthSession _auth;
    private readonly IServiceProvider _serializerProvider;
    private readonly object _gate = new();

    private GrpcChannel? _channel;
    private LatticeAuthApiGrpcClient? _client;
    private string? _builtEndpoint;
    private LatticeCallAuthentication? _builtAuthentication;
    private bool _disposed;

    /// <summary>
    /// Creates the client over the explorer session and auth session. A private
    /// Orleans serializer provider is always built and owned, matching the state
    /// connection's self-contained wiring: the explorer's application root has no
    /// Orleans serialization registered, and an injected root provider would make
    /// every admin call fail resolving its per-message serializers.
    /// </summary>
    /// <param name="session">The explorer session that owns the endpoint. Must not be <see langword="null"/>.</param>
    /// <param name="auth">The auth session whose current sign-in is attached. Must not be <see langword="null"/>.</param>
    public GrpcAuthAdminClient(IExplorerSession session, IExplorerAuthSession auth)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(auth);
        _session = session;
        _auth = auth;
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
    }

    /// <inheritdoc />
    public Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.ListGroupsAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public async Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        var result = await InvokeAsync(client =>
            client.GetGroupAsync(new AuthGroupRef { GroupId = groupId }, cancellationToken)).ConfigureAwait(false);
        return result.Group;
    }

    /// <inheritdoc />
    public Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(group);
        return InvokeAsync(client => client.UpsertGroupAsync(group, cancellationToken));
    }

    /// <inheritdoc />
    public Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return InvokeAsync(client => client.RemoveGroupAsync(new AuthGroupRef { GroupId = groupId }, cancellationToken));
    }

    /// <inheritdoc />
    public Task AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        var edge = new AuthMemberEdge { GroupId = groupId, MemberId = memberId, MemberKind = memberKind };
        return InvokeAsync(client => client.AddMemberAsync(edge, cancellationToken));
    }

    /// <inheritdoc />
    public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        var edge = new AuthMemberEdge { GroupId = groupId, MemberId = memberId };
        return InvokeAsync(client => client.RemoveMemberAsync(edge, cancellationToken));
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        var result = await InvokeAsync(client =>
            client.ListGroupMembersAsync(new AuthGroupRef { GroupId = groupId }, cancellationToken)).ConfigureAwait(false);
        return result.Values;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        var result = await InvokeAsync(client =>
            client.ListSubjectGroupsAsync(new AuthMemberRef { MemberId = memberId }, cancellationToken)).ConfigureAwait(false);
        return result.Values;
    }

    /// <inheritdoc />
    public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        return InvokeAsync(client => client.PutRuleAsync(new AuthPutRule { Rule = rule }, cancellationToken));
    }

    /// <inheritdoc />
    public async Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        var result = await InvokeAsync(client =>
            client.GetRuleAsync(new AuthRuleRef { TreeId = treeId, RuleId = ruleId }, cancellationToken)).ConfigureAwait(false);
        return result.Rule;
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        var result = await InvokeAsync(client =>
            client.RemoveRuleAsync(new AuthRuleRef { TreeId = treeId, RuleId = ruleId }, cancellationToken)).ConfigureAwait(false);
        return result.Removed;
    }

    /// <inheritdoc />
    public Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.ListRulesAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(request);
        var envelope = new AuthTreeRulesPage { TreeId = treeId, Page = request };
        return InvokeAsync(client => client.ListRulesForTreeAsync(envelope, cancellationToken));
    }

    /// <inheritdoc />
    public Task<AuthExplanation> ExplainAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentNullException.ThrowIfNull(scope);
        var query = new AuthExplainQuery { SubjectId = subjectId, Operation = operation, Scope = scope, SubjectKind = subjectKind };
        return InvokeAsync(client => client.ExplainAsync(query, cancellationToken));
    }

    /// <inheritdoc />
    public Task<AuthEffectivePermissions> EffectivePermissionsAsync(string subjectId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return InvokeAsync(client =>
            client.EffectivePermissionsAsync(new AuthSubjectRef { SubjectId = subjectId }, cancellationToken));
    }

    /// <inheritdoc />
    public Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.SearchDirectoryAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public async Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(principalId);
        var result = await InvokeAsync(client =>
            client.ResolveDirectoryPrincipalAsync(new AuthPrincipalRef { PrincipalId = principalId }, cancellationToken)).ConfigureAwait(false);
        return result.Principal;
    }

    /// <inheritdoc />
    public Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)
        => InvokeAsync(client => client.GetAccessModelAsync(new AuthAccessModelQuery(), cancellationToken));

    private async Task<T> InvokeAsync<T>(Func<LatticeAuthApiGrpcClient, Task<T>> call)
    {
        var client = ResolveClient();
        try
        {
            return await call(client).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.PermissionDenied or StatusCode.Unauthenticated)
        {
            // Present the transport denial as the same typed exception the rest of
            // the explorer handles, so a UI action can degrade gracefully.
            throw new LatticeAuthorizationDeniedException(ex.Status.Detail, ex);
        }
    }

    private async Task InvokeAsync(Func<LatticeAuthApiGrpcClient, Task> call)
    {
        var client = ResolveClient();
        try
        {
            await call(client).ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode is StatusCode.PermissionDenied or StatusCode.Unauthenticated)
        {
            throw new LatticeAuthorizationDeniedException(ex.Status.Detail, ex);
        }
    }

    /// <summary>
    /// Returns a client bound to the current endpoint and sign-in, rebuilding the
    /// channel when either has changed since it was last built.
    /// </summary>
    private LatticeAuthApiGrpcClient ResolveClient()
    {
        var configuration = _session.Current
            ?? throw new InvalidOperationException("The explorer is not configured with an endpoint yet.");
        var settings = configuration.ToConnectionSettings();
        var authentication = _auth.CurrentAuthentication;

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_client is not null
                && string.Equals(_builtEndpoint, settings.Address, StringComparison.Ordinal)
                && ReferenceEquals(_builtAuthentication, authentication))
            {
                return _client;
            }

            _channel?.Dispose();

            var effective = settings with { Authentication = authentication };
            _channel = LatticeGrpcChannelFactory.CreateChannel(effective);
            var invoker = LatticeGrpcChannelFactory.CreateCallInvoker(_channel, effective);
            _client = LatticeAuthApiGrpcClient.Create(invoker, _serializerProvider);
            _builtEndpoint = settings.Address;
            _builtAuthentication = authentication;
            return _client;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _channel?.Dispose();
            _channel = null;
            _client = null;
        }

        if (_serializerProvider is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
