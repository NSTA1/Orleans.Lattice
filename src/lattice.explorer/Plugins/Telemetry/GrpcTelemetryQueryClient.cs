using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Api.Telemetry.Grpc;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The production <see cref="ITelemetryQueryClient"/>. Builds the telemetry gRPC
/// client over a single channel to the currently configured endpoint, attaching
/// the same sign-in the state connection uses (read from
/// <see cref="IExplorerAuthSession.CurrentAuthentication"/>). The channel is
/// rebuilt lazily whenever the endpoint or the sign-in changes, so a reconnect or
/// a login is picked up without a restart.
/// <para>
/// This is the one place the telemetry read plane's channel, credential, and
/// fault translation live, exactly as <c>GrpcTenantAdminClient</c> is for the
/// tenancy control plane and <c>GrpcBackupControlClient</c> for the backup one.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>Fault translation is deliberately minimal.</b> Only the two statuses that
/// map unambiguously onto a typed Explorer exception are translated; every other
/// status stays an <see cref="RpcException"/> for
/// <see cref="TelemetryFaultMapper"/> to classify against the RPC that raised it:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///     <see cref="StatusCode.Unimplemented"/> becomes
///     <see cref="TelemetryUnavailableException"/>. The binding answers that for
///     an optional facade the cluster did not register, and a host serving no
///     telemetry binding at all answers it for every method, so it is the single
///     honest signal that the surface does not exist here.
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
/// <b><see cref="StatusCode.Unavailable"/> is pointedly not translated here.</b>
/// The binding answers it for a metrics backend that could not answer - a
/// retryable backend fault, not an absent surface - and the transport answers it
/// for an endpoint it could not reach. Folding it into
/// <see cref="TelemetryUnavailableException"/> would present a transient backend
/// outage as a permanently missing capability and make a telemetry surface
/// disappear the moment its metrics store hiccupped. Which of the two it means
/// depends on the RPC that raised it, and that is the mapper's judgement to make,
/// not the transport's.
/// </para>
/// <para>
/// <see cref="StatusCode.Unauthenticated"/> is likewise kept apart from the
/// denial: "you presented no credential" is recoverable by signing in and "you
/// presented one and were refused" is not.
/// </para>
/// </remarks>
public sealed class GrpcTelemetryQueryClient : ITelemetryQueryClient, IDisposable
{
    private readonly IExplorerSession _session;
    private readonly IExplorerAuthSession _auth;
    private readonly IServiceProvider _serializerProvider;
    private readonly object _gate = new();

    private GrpcChannel? _channel;
    private LatticeTelemetryApiGrpcClient? _client;
    private string? _builtEndpoint;
    private LatticeCallAuthentication? _builtAuthentication;
    private bool _disposed;

    /// <summary>
    /// Creates the client over the explorer session and auth session. A private
    /// Orleans serializer provider is always built and owned, matching the state
    /// connection's self-contained wiring: the explorer's application root has no
    /// Orleans serialization registered, and an injected root provider would make
    /// every telemetry call fail resolving its per-message serializers.
    /// </summary>
    /// <param name="session">The explorer session that owns the endpoint. Must not be <see langword="null"/>.</param>
    /// <param name="auth">The auth session whose current sign-in is attached. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public GrpcTelemetryQueryClient(IExplorerSession session, IExplorerAuthSession auth)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(auth);
        _session = session;
        _auth = auth;
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
    }

    /// <inheritdoc />
    public async Task<TelemetryQueryCatalog> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        var client = ResolveClient();
        try
        {
            return await client.GetCatalogAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsTranslatable(ex))
        {
            throw Translate(ex);
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Written out rather than routed through a shared helper taking a delegate:
    /// a helper would need a closure over <paramref name="request"/>, and this is
    /// the call a panel makes on every poll tick. Two short duplicated blocks cost
    /// less than a display class and a delegate per measurement.
    /// </remarks>
    public async Task<TelemetryQueryResponse> QueryAsync(
        TelemetryQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var client = ResolveClient();
        try
        {
            return await client.QueryAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsTranslatable(ex))
        {
            throw Translate(ex);
        }
    }

    /// <summary>
    /// Whether <paramref name="exception"/> carries one of the two statuses that
    /// map unambiguously onto a typed Explorer exception. Used as a catch filter
    /// so every other fault propagates untouched, keeping its original stack
    /// trace rather than being caught and rethrown.
    /// </summary>
    private static bool IsTranslatable(RpcException exception) =>
        exception.StatusCode is StatusCode.Unimplemented or StatusCode.PermissionDenied;

    /// <summary>
    /// Reconstructs the typed Explorer exception for a transport status
    /// <see cref="IsTranslatable"/> accepted.
    /// </summary>
    private static Exception Translate(RpcException exception) => exception.StatusCode switch
    {
        StatusCode.Unimplemented => new TelemetryUnavailableException(
            string.IsNullOrWhiteSpace(exception.Status.Detail)
                ? "This cluster does not serve telemetry."
                : exception.Status.Detail,
            exception),
        _ => new LatticeAuthorizationDeniedException(exception.Status.Detail, exception),
    };

    /// <summary>
    /// Returns the client bound to the current endpoint and sign-in, rebuilding
    /// the channel when either has changed since it was last built.
    /// </summary>
    private LatticeTelemetryApiGrpcClient ResolveClient()
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
            _client = LatticeTelemetryApiGrpcClient.Create(invoker, _serializerProvider);
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
