using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// The production <see cref="ITenantAdminClient"/>. Builds both
/// tenant-administration gRPC clients - the administrative one and the read-only
/// self-service one - over a single gRPC channel to the currently configured
/// endpoint, attaching the same sign-in the state connection uses (read from
/// <see cref="IExplorerAuthSession.CurrentAuthentication"/>). The channel is
/// rebuilt lazily whenever the endpoint or the sign-in changes, so a reconnect or
/// a login is picked up without a restart.
/// <para>
/// This is the one place the tenancy control plane's channel, credential, and
/// fault translation live, exactly as <c>GrpcAuthAdminClient</c> is for the
/// auth-admin control plane and <c>GrpcBackupControlClient</c> for the backup
/// one. Both clients share one channel because both RPC families are served by
/// the same gRPC service.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>Fault translation.</b> Two transport statuses are reconstructed into the
/// typed exceptions the rest of the Explorer handles, and everything else is
/// left as an <see cref="RpcException"/> for
/// <see cref="ITenantAdminService"/> to classify:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///     <see cref="StatusCode.Unimplemented"/> becomes
///     <see cref="TenancyUnavailableException"/>. The binding answers that for
///     an optional facade the cluster did not register, and a host serving no
///     tenant-administration binding at all answers it for every method, so it
///     is the single honest signal that the surface does not exist here.
///     </description>
///   </item>
///   <item>
///     <description>
///     <see cref="StatusCode.PermissionDenied"/> becomes
///     <see cref="LatticeAuthorizationDeniedException"/>, the one typed denial
///     the rest of the Explorer already handles.
///     </description>
///   </item>
/// </list>
/// <para>
/// <see cref="StatusCode.Unauthenticated"/> is deliberately <em>not</em> folded
/// into the denial: "you presented no credential" is recoverable by signing in
/// and "you presented one and were refused" is not, so the seam keeps them
/// apart for the four-state access model. Nor is any other status guessed at -
/// in particular <see cref="StatusCode.FailedPrecondition"/>, which the binding
/// uses for five distinct refusals and distinguishes only in its message.
/// </para>
/// </remarks>
public sealed class GrpcTenantAdminClient : ITenantAdminClient, IDisposable
{
    private readonly IExplorerSession _session;
    private readonly IExplorerAuthSession _auth;
    private readonly IServiceProvider _serializerProvider;
    private readonly object _gate = new();

    private GrpcChannel? _channel;
    private LatticeTenantAdminApiGrpcClient? _admin;
    private LatticeTenantSelfServiceApiGrpcClient? _selfService;
    private string? _builtEndpoint;
    private LatticeCallAuthentication? _builtAuthentication;
    private bool _disposed;

    /// <summary>
    /// Creates the client over the explorer session and auth session. A private
    /// Orleans serializer provider is always built and owned, matching the state
    /// connection's self-contained wiring: the explorer's application root has no
    /// Orleans serialization registered, and an injected root provider would make
    /// every tenancy call fail resolving its per-message serializers.
    /// </summary>
    /// <param name="session">The explorer session that owns the endpoint. Must not be <see langword="null"/>.</param>
    /// <param name="auth">The auth session whose current sign-in is attached. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public GrpcTenantAdminClient(IExplorerSession session, IExplorerAuthSession auth)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(auth);
        _session = session;
        _auth = auth;
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
    }

    /// <inheritdoc />
    public Task<TenantDescriptor> GetCurrentTenantAsync(CancellationToken cancellationToken = default) =>
        InvokeSelfServiceAsync(client => client.GetCurrentTenantAsync(cancellationToken));

    /// <inheritdoc />
    public Task<IReadOnlyList<TenantDescriptor>> ListAccessibleTenantsAsync(CancellationToken cancellationToken = default) =>
        InvokeSelfServiceAsync(client => client.ListAccessibleTenantsAsync(cancellationToken));

    /// <inheritdoc />
    public Task<TenantStatusReport> GetTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeSelfServiceAsync(client => client.GetTenantAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantCreationResult> CreateTenantAsync(
        string tenantId,
        IReadOnlyCollection<string>? adminSubjects = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.CreateTenantAsync(tenantId, adminSubjects, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> SuspendTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.SuspendTenantAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantStatusChangeResult> ResumeTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.ResumeTenantAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantDeletionResult> DeleteTenantAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.DeleteTenantAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantQuotasUpdateResult> SetTenantQuotasAsync(
        string tenantId,
        TenantQuotasDescriptor quotas,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.SetTenantQuotasAsync(tenantId, quotas, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId,
        IReadOnlyCollection<string> allowedRegions,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(allowedRegions);
        return InvokeAdminAsync(client => client.AuthorizeAllowedRegionsAsync(tenantId, allowedRegions, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantResidencyChangeResult> SetTenantResidencyAsync(
        string tenantId,
        IReadOnlyCollection<string> residencyRegions,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(residencyRegions);
        return InvokeAdminAsync(client => client.SetTenantResidencyAsync(tenantId, residencyRegions, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.GetTenantRegionStatusAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantQuotaUsageReport> GetTenantQuotaUsageAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.GetTenantQuotaUsageAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantAdminSubjectReport> ListTenantAdminSubjectsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.ListTenantAdminSubjectsAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantAdminSubjectChangeResult> AddTenantAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return InvokeAdminAsync(client => client.AddTenantAdminSubjectAsync(tenantId, subjectId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantAdminSubjectChangeResult> RemoveTenantAdminSubjectAsync(
        string tenantId,
        string subjectId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return InvokeAdminAsync(client => client.RemoveTenantAdminSubjectAsync(tenantId, subjectId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantGrantReport> ListCrossTenantGrantsAsync(
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return InvokeAdminAsync(client => client.ListCrossTenantGrantsAsync(tenantId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> OfferCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantAccess operations,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(granterTenantId);
        ArgumentException.ThrowIfNullOrEmpty(granteeTenantId);
        ArgumentException.ThrowIfNullOrEmpty(scope);
        return InvokeAdminAsync(client =>
            client.OfferCrossTenantGrantAsync(granterTenantId, granteeTenantId, scope, operations, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> ApproveCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        return InvokeAdminAsync(client =>
            client.ApproveCrossTenantGrantAsync(granterTenantId, granteeTenantId, scope, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> RejectCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        return InvokeAdminAsync(client =>
            client.RejectCrossTenantGrantAsync(granterTenantId, granteeTenantId, scope, cancellationToken));
    }

    /// <inheritdoc />
    public Task<TenantGrantChangeResult> RevokeCrossTenantGrantAsync(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        CancellationToken cancellationToken = default)
    {
        ValidateGrantKey(granterTenantId, granteeTenantId, scope);
        return InvokeAdminAsync(client =>
            client.RevokeCrossTenantGrantAsync(granterTenantId, granteeTenantId, scope, cancellationToken));
    }

    private static void ValidateGrantKey(string granterTenantId, string granteeTenantId, string scope)
    {
        ArgumentException.ThrowIfNullOrEmpty(granterTenantId);
        ArgumentException.ThrowIfNullOrEmpty(granteeTenantId);
        ArgumentException.ThrowIfNullOrEmpty(scope);
    }

    private async Task<T> InvokeAdminAsync<T>(Func<LatticeTenantAdminApiGrpcClient, Task<T>> call)
    {
        var clients = ResolveClients();
        try
        {
            return await call(clients.Admin).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
    }

    private async Task<T> InvokeSelfServiceAsync<T>(Func<LatticeTenantSelfServiceApiGrpcClient, Task<T>> call)
    {
        var clients = ResolveClients();
        try
        {
            return await call(clients.SelfService).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            throw Translate(ex);
        }
    }

    /// <summary>
    /// Reconstructs the two transport statuses that map unambiguously onto a
    /// typed Explorer exception, and returns <paramref name="exception"/>
    /// unchanged otherwise so nothing is guessed at.
    /// </summary>
    private static Exception Translate(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => new TenancyUnavailableException(
            string.IsNullOrWhiteSpace(exception.Status.Detail)
                ? "This cluster does not serve tenant administration."
                : exception.Status.Detail,
            exception),
        StatusCode.PermissionDenied => new LatticeAuthorizationDeniedException(exception.Status.Detail, exception),
        _ => exception,
    };

    /// <summary>
    /// Returns the pair of clients bound to the current endpoint and sign-in,
    /// rebuilding the shared channel when either has changed since it was last
    /// built.
    /// </summary>
    private (LatticeTenantAdminApiGrpcClient Admin, LatticeTenantSelfServiceApiGrpcClient SelfService) ResolveClients()
    {
        var configuration = _session.Current
            ?? throw new InvalidOperationException("The explorer is not configured with an endpoint yet.");
        var settings = configuration.ToConnectionSettings();
        var authentication = _auth.CurrentAuthentication;

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_admin is not null
                && _selfService is not null
                && string.Equals(_builtEndpoint, settings.Address, StringComparison.Ordinal)
                && ReferenceEquals(_builtAuthentication, authentication))
            {
                return (_admin, _selfService);
            }

            _channel?.Dispose();

            var effective = settings with { Authentication = authentication };
            _channel = BuildChannel(effective, out var invoker);
            _admin = LatticeTenantAdminApiGrpcClient.Create(invoker, _serializerProvider);
            _selfService = LatticeTenantSelfServiceApiGrpcClient.Create(invoker, _serializerProvider);
            _builtEndpoint = settings.Address;
            _builtAuthentication = authentication;
            return (_admin, _selfService);
        }
    }

    /// <summary>
    /// Builds the channel and an auth-attaching call invoker for
    /// <paramref name="settings"/>, mirroring the state connection's channel
    /// construction (per-call token provider or static headers, h2c opt-in).
    /// </summary>
    private static GrpcChannel BuildChannel(LatticeConnectionSettings settings, out CallInvoker invoker)
    {
        if (settings.AllowUnencryptedHttp2)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        var channelOptions = new GrpcChannelOptions();
        var auth = settings.Authentication;
        if (auth is { HasCredentialProvider: true }
            && settings.AllowUnencryptedHttp2
            && !IsHttpsAddress(settings.Address))
        {
            channelOptions.UnsafeUseInsecureChannelCallCredentials = true;
        }

        var channel = GrpcChannel.ForAddress(settings.Address, channelOptions);
        invoker = channel.CreateCallInvoker();

        // Transport headers accompany every call regardless of the auth mode (for
        // example an origin-routing header a fronting proxy requires), independent
        // of the sign-in that replaces settings.Authentication.
        invoker = ApplyTransportHeaders(invoker, settings.TransportHeaders);

        if (auth is { HasCredentialProvider: true, CredentialProvider: { } provider })
        {
            var callCredentials = CallCredentials.FromInterceptor(async (context, metadata) =>
            {
                var header = await provider.GetAuthorizationHeaderAsync(context.CancellationToken).ConfigureAwait(false);
                if (!string.IsNullOrEmpty(header))
                {
                    metadata.Add(LatticeCallAuthentication.AuthorizationHeaderName, header);
                }
            });

            invoker = invoker.Intercept(new CallCredentialsInterceptor(callCredentials));
        }
        else if (auth is { HasHeaders: true })
        {
            var headers = auth.Headers!;
            invoker = invoker.Intercept(metadata =>
            {
                foreach (var (key, value) in headers)
                {
                    metadata.Add(key, value);
                }

                return metadata;
            });
        }

        return channel;
    }

    private static bool IsHttpsAddress(string? address) =>
        Uri.TryCreate(address, UriKind.Absolute, out var uri)
        && string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Applies the sign-in-independent transport headers (if any) to every call on
    /// <paramref name="invoker"/>, returning it unchanged when there are none.
    /// </summary>
    private static CallInvoker ApplyTransportHeaders(CallInvoker invoker, IReadOnlyDictionary<string, string>? transportHeaders)
    {
        if (transportHeaders is not { Count: > 0 } headers)
        {
            return invoker;
        }

        return invoker.Intercept(metadata =>
        {
            foreach (var (key, value) in headers)
            {
                metadata.Add(key, value);
            }

            return metadata;
        });
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
            _admin = null;
            _selfService = null;
        }

        if (_serializerProvider is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
