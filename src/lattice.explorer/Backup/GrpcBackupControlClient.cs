using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// The production <see cref="IBackupControlClient"/>. Builds a
/// <see cref="LatticeBackupApiGrpcClient"/> over a gRPC channel to the currently
/// configured endpoint, attaching the same sign-in the state connection uses
/// (read from <see cref="IExplorerAuthSession.CurrentAuthentication"/>). The
/// channel is rebuilt lazily whenever the endpoint or the sign-in changes, so a
/// reconnect or a login is picked up without a restart. A gRPC
/// <see cref="StatusCode.PermissionDenied"/> / <see cref="StatusCode.Unauthenticated"/>
/// is translated back to <see cref="LatticeAuthorizationDeniedException"/> so the
/// rest of the explorer handles a single typed denial.
/// </summary>
public sealed class GrpcBackupControlClient : IBackupControlClient, IDisposable
{
    private readonly IExplorerSession _session;
    private readonly IExplorerAuthSession _auth;
    private readonly IServiceProvider _serializerProvider;
    private readonly object _gate = new();

    private GrpcChannel? _channel;
    private LatticeBackupApiGrpcClient? _client;
    private string? _builtEndpoint;
    private LatticeCallAuthentication? _builtAuthentication;
    private bool _disposed;

    /// <summary>
    /// Creates the client over the explorer session and auth session. A private
    /// Orleans serializer provider is always built and owned, matching the state
    /// connection's self-contained wiring. The client deliberately does not take a
    /// serializer provider from the ambient container: the explorer's application
    /// root has no Orleans serialization registered, and an injected root provider
    /// would make every backup call fail resolving its per-message serializers.
    /// </summary>
    /// <param name="session">The explorer session that owns the endpoint. Must not be <see langword="null"/>.</param>
    /// <param name="auth">The auth session whose current sign-in is attached. Must not be <see langword="null"/>.</param>
    public GrpcBackupControlClient(
        IExplorerSession session,
        IExplorerAuthSession auth)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(auth);
        _session = session;
        _auth = auth;
        _serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
    }

    /// <inheritdoc />
    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return InvokeAsync(client => client.ProbeCapabilitiesAsync(scope, cancellationToken));
    }

    /// <inheritdoc />
    public Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.ListBackupsAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return InvokeAsync(client => client.DescribeBackupAsync(backupId, cancellationToken));
    }

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateBackupAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.CreateBackupAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.CreateIncrementalBackupAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.CreateBackupSetAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public Task<LatticeRestoreResult> RestoreBackupAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return InvokeAsync(client => client.RestoreBackupAsync(request, cancellationToken));
    }

    /// <inheritdoc />
    public Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return InvokeAsync(client => client.DeleteBackupAsync(backupId, cancellationToken));
    }

    private async Task<T> InvokeAsync<T>(Func<LatticeBackupApiGrpcClient, Task<T>> call)
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

    /// <summary>
    /// Returns a client bound to the current endpoint and sign-in, rebuilding the
    /// channel when either has changed since it was last built.
    /// </summary>
    private LatticeBackupApiGrpcClient ResolveClient()
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
            _channel = BuildChannel(effective, out var invoker);
            _client = LatticeBackupApiGrpcClient.Create(invoker, _serializerProvider);
            _builtEndpoint = settings.Address;
            _builtAuthentication = authentication;
            return _client;
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
