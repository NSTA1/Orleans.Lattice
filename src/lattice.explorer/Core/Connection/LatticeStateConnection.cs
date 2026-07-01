using System.Runtime.CompilerServices;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// The default <see cref="ILatticeStateConnection"/>. Owns a single gRPC channel
/// (rebuilt on endpoint change), retries transient failures inline, runs a
/// background health monitor that auto-recovers a faulted endpoint, resubscribes
/// live observe streams across a transient drop, and degrades to a disconnected
/// state once the configured grace window elapses.
/// </summary>
public sealed class LatticeStateConnection : ILatticeStateConnection
{
    private readonly Func<LatticeConnectionSettings, ILatticeStateClient> _clientFactory;
    private readonly TimeProvider _timeProvider;
    private readonly IServiceProvider? _ownedSerializerProvider;
    private readonly object _gate = new();

    private ILatticeStateClient? _client;
    private LatticeConnectionSettings? _settings;
    private LatticeConnectionStatus _status = LatticeConnectionStatus.Disconnected;
    private DateTimeOffset? _disruptedSince;
    private ITimer? _monitor;
    private bool _disposed;

    /// <summary>
    /// Creates a connection that talks to the real state API over gRPC, building
    /// its own Orleans serializer provider.
    /// </summary>
    public LatticeStateConnection()
        : this(timeProvider: TimeProvider.System)
    {
    }

    private LatticeStateConnection(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
        var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _ownedSerializerProvider = serializerProvider;
        _clientFactory = settings => GrpcLatticeStateClient.Create(settings, serializerProvider);
    }

    /// <summary>
    /// Test/advanced constructor: supplies the client factory (so a fake client
    /// can stand in for a live server) and the time source (so the degrade window
    /// can be driven deterministically).
    /// </summary>
    internal LatticeStateConnection(
        Func<LatticeConnectionSettings, ILatticeStateClient> clientFactory,
        TimeProvider timeProvider)
    {
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _ownedSerializerProvider = null;
    }

    /// <inheritdoc />
    public LatticeConnectionStatus Status
    {
        get { lock (_gate) { return _status; } }
    }

    /// <inheritdoc />
    public event Action<LatticeConnectionStatus>? StatusChanged;

    /// <inheritdoc />
    public Task ConfigureAsync(LatticeConnectionSettings settings, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        if (!Rebuild(settings))
        {
            return Task.CompletedTask;
        }

        return ProbeAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> ReconnectAsync(CancellationToken cancellationToken = default)
    {
        LatticeConnectionSettings? settings;
        lock (_gate)
        {
            settings = _settings;
        }

        if (settings is null)
        {
            return Task.FromResult(false);
        }

        return Rebuild(settings)
            ? ProbeAsync(cancellationToken)
            : Task.FromResult(false);
    }

    /// <inheritdoc />
    public async Task<bool> ProbeAsync(CancellationToken cancellationToken = default)
    {
        ILatticeStateClient? client;
        lock (_gate)
        {
            client = _client;
        }

        if (client is null)
        {
            return false;
        }

        try
        {
            await client.ListTreesAsync(new CatalogRequest { PageSize = 1 }, cancellationToken).ConfigureAwait(false);
            OnSuccess();
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (RpcException ex)
        {
            if (IsTransient(ex))
            {
                EnterReconnecting();
            }
            else
            {
                SetFaulted(Friendly(ex), IsAuthFailure(ex));
            }

            return false;
        }
        catch (Exception ex)
        {
            SetFaulted($"Could not reach the state API: {ex.Message}");
            return false;
        }
    }

    /// <inheritdoc />
    public Task<TreeCatalogPage> ListTreesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.ListTreesAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<ViewCatalogPage> ListViewsAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.ListViewsAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<TagIndexCatalogPage> ListTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.ListTagIndexesAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<TagValueCatalogPage> ListTagValuesAsync(CatalogRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.ListTagValuesAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetTreeStructureAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.ScanEntriesAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetEntryAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<EntryHistoryResponse> GetEntryHistoryAsync(EntryHistoryRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetEntryHistoryAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<EntryScanCancelResponse> CancelScanAsync(EntryScanCancelRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.CancelScanAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetMetricsSnapshotAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<ClusterInfo> GetClusterInfoAsync(ClusterInfoRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetClusterInfoAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<StateChangeNotification> ObserveChangesAsync(StateObserveRequest request, CancellationToken cancellationToken = default)
        => StreamAsync((c, ct) => c.ObserveChangesAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<TreeMetricsSnapshot> ObserveMetricsAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => StreamAsync((c, ct) => c.ObserveMetricsAsync(request, ct), cancellationToken);

    /// <summary>
    /// The background health-monitor step: probes a degraded or reconnecting
    /// endpoint to recover it. Invoked by the internal timer and, in tests,
    /// directly to drive recovery and the degrade transition deterministically.
    /// </summary>
    internal async Task CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        LatticeConnectionState state;
        bool hasClient;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            state = _status.State;
            hasClient = _client is not null;
        }

        if (!hasClient || state is LatticeConnectionState.Connected or LatticeConnectionState.Disconnected)
        {
            return;
        }

        await ProbeAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<T> ExecuteAsync<T>(
        Func<ILatticeStateClient, CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        var maxRetries = SnapshotSettings()?.MaxTransientRetries ?? 0;
        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var client = CurrentClientOrThrow();
            try
            {
                var result = await operation(client, cancellationToken).ConfigureAwait(false);
                OnSuccess();
                return result;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (RpcException ex)
            {
                // Application-level back-pressure: the tree is WAL-saturated and
                // shed a heavy snapshot-cursor open (issue #1053), surfaced as
                // gRPC ResourceExhausted. This is not a connection fault, and must
                // not be auto-retried - retrying re-issues the expensive open into
                // an already-collapsing tree, amplifying the very storm the shed
                // exists to stop. Surface a friendly, user-retryable error and
                // leave the connection Connected so other trees stay browsable.
                if (ex.StatusCode == StatusCode.ResourceExhausted)
                {
                    throw new LatticeStateApiException(Friendly(ex), ex)
                    {
                        IsTransient = true,
                        RequiresAuthentication = false,
                    };
                }

                if (IsTransient(ex) && attempt < maxRetries)
                {
                    EnterReconnecting();
                    attempt++;
                    var backoff = SnapshotSettings()?.TransientRetryBackoff ?? TimeSpan.FromMilliseconds(250);
                    await Task.Delay(backoff, _timeProvider, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (IsTransient(ex))
                {
                    EnterReconnecting();
                }
                else
                {
                    SetFaulted(Friendly(ex), IsAuthFailure(ex));
                }

                throw new LatticeStateApiException(Friendly(ex), ex)
                {
                    IsTransient = IsTransient(ex),
                    RequiresAuthentication = IsAuthFailure(ex),
                };
            }
        }
    }

    private async IAsyncEnumerable<T> StreamAsync<T>(
        Func<ILatticeStateClient, CancellationToken, IAsyncEnumerable<T>> operation,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // A live observe stream is pinned to a single silo for its lifetime, so a
        // silo failover (or any transient drop) tears it down even though the
        // unary health probe round-robins onto a healthy silo and reports
        // Connected. Without resubscription the UI would freeze on a "connected"
        // channel with dead feeds, so we transparently re-open the stream against
        // the current client, backing off between attempts, until either the
        // degrade window elapses (Reconnecting -> Faulted) or the caller cancels.
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var client = CurrentClientOrThrow();
            var lost = false;
            await using (var enumerator = operation(client, cancellationToken).GetAsyncEnumerator(cancellationToken))
            {
                while (true)
                {
                    T current;
                    try
                    {
                        if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                        {
                            yield break;
                        }

                        current = enumerator.Current;
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (RpcException ex)
                    {
                        if (IsTransient(ex))
                        {
                            EnterReconnecting();
                            lost = true;
                            break;
                        }

                        SetFaulted(Friendly(ex), IsAuthFailure(ex));
                        throw new LatticeStateApiException(Friendly(ex), ex)
                        {
                            IsTransient = false,
                            RequiresAuthentication = IsAuthFailure(ex),
                        };
                    }

                    OnSuccess();
                    yield return current;
                }
            }

            if (!lost)
            {
                yield break;
            }

            // EnterReconnecting flips to Faulted once the grace window is spent;
            // stop resubscribing then so the UI's disconnected banner takes over.
            if (Status.State is LatticeConnectionState.Faulted or LatticeConnectionState.Disconnected)
            {
                yield break;
            }

            var backoff = SnapshotSettings()?.TransientRetryBackoff ?? TimeSpan.FromMilliseconds(250);
            await Task.Delay(backoff, _timeProvider, cancellationToken).ConfigureAwait(false);
        }
    }

    private bool Rebuild(LatticeConnectionSettings settings)
    {
        LatticeConnectionStatus? raise = null;
        ILatticeStateClient? oldClient;
        ITimer? oldTimer;
        lock (_gate)
        {
            if (_disposed)
            {
                return false;
            }

            oldClient = _client;
            oldTimer = _monitor;
            _client = null;
            _monitor = null;
            _disruptedSince = null;
            _settings = settings;

            try
            {
                _client = _clientFactory(settings);
            }
            catch (Exception ex)
            {
                raise = SetUnlocked(LatticeConnectionState.Faulted, settings.Address, $"Invalid endpoint: {ex.Message}");
            }

            if (_client is not null)
            {
                raise = SetUnlocked(LatticeConnectionState.Connecting, settings.Address, "Connecting to the state API...");
                _monitor = _timeProvider.CreateTimer(
                    static state => _ = ((LatticeStateConnection)state!).MonitorTickAsync(),
                    this,
                    settings.HealthCheckInterval,
                    settings.HealthCheckInterval);
            }
        }

        DisposeClient(oldClient);
        oldTimer?.Dispose();
        if (raise is not null)
        {
            RaiseStatusChanged(raise);
        }

        return _client is not null;
    }

    private async Task MonitorTickAsync()
    {
        try
        {
            await CheckHealthAsync().ConfigureAwait(false);
        }
        catch
        {
            // The monitor must never surface faults; transient errors are
            // reflected through Status, and cancellation is benign.
        }
    }

    private void OnSuccess()
    {
        LatticeConnectionStatus? raise;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disruptedSince = null;
            raise = SetUnlocked(LatticeConnectionState.Connected, _settings?.Address, "Connected.");
        }

        if (raise is not null)
        {
            RaiseStatusChanged(raise);
        }
    }

    private void EnterReconnecting()
    {
        LatticeConnectionStatus? raise;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            var now = _timeProvider.GetUtcNow();
            _disruptedSince ??= now;
            var degradeAfter = _settings?.DegradeAfter ?? TimeSpan.FromSeconds(5);
            raise = now - _disruptedSince.Value >= degradeAfter
                ? SetUnlocked(LatticeConnectionState.Faulted, _settings?.Address, "Disconnected. The state API is not responding.")
                : SetUnlocked(LatticeConnectionState.Reconnecting, _settings?.Address, "Reconnecting to the state API...");
        }

        if (raise is not null)
        {
            RaiseStatusChanged(raise);
        }
    }

    private void SetFaulted(string message, bool requiresAuth = false)
    {
        LatticeConnectionStatus? raise;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            raise = SetUnlocked(LatticeConnectionState.Faulted, _settings?.Address, message, requiresAuth);
        }

        if (raise is not null)
        {
            RaiseStatusChanged(raise);
        }
    }

    private LatticeConnectionStatus? SetUnlocked(LatticeConnectionState state, string? endpoint, string message, bool requiresAuth = false)
    {
        var next = new LatticeConnectionStatus(state, endpoint, message, requiresAuth);
        if (next == _status)
        {
            return null;
        }

        _status = next;
        return next;
    }

    private void RaiseStatusChanged(LatticeConnectionStatus status) => StatusChanged?.Invoke(status);

    private ILatticeStateClient CurrentClientOrThrow()
    {
        lock (_gate)
        {
            return _client ?? throw new LatticeStateApiException("Not connected. Configure a state-API endpoint first.");
        }
    }

    private LatticeConnectionSettings? SnapshotSettings()
    {
        lock (_gate)
        {
            return _settings;
        }
    }

    private static void DisposeClient(ILatticeStateClient? client)
    {
        if (client is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    private static bool IsTransient(RpcException ex) => ex.StatusCode is
        StatusCode.Unavailable or
        StatusCode.DeadlineExceeded or
        StatusCode.Internal or
        StatusCode.Unknown or
        StatusCode.ResourceExhausted or
        StatusCode.Aborted;

    private static bool IsAuthFailure(RpcException ex) => ex.StatusCode is
        StatusCode.Unauthenticated or
        StatusCode.PermissionDenied;

    private static string Friendly(RpcException ex) => ex.StatusCode switch
    {
        StatusCode.Unavailable => "The state API is unavailable. Check the endpoint and that the service is running.",
        StatusCode.DeadlineExceeded => "The state API did not respond in time.",
        StatusCode.Unauthenticated => "Authentication is required to access the state API.",
        StatusCode.PermissionDenied => "Access to the state API was denied.",
        StatusCode.Unimplemented => "The endpoint does not expose the Lattice state API.",
        StatusCode.NotFound => "The requested tree or entry was not found.",
        StatusCode.ResourceExhausted => "This table is very busy right now, so it can't be opened for browsing for a moment. Please wait a few seconds and try again.",
        _ => $"The state-API call failed ({ex.StatusCode}).",
    };

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        ILatticeStateClient? client;
        ITimer? timer;
        lock (_gate)
        {
            if (_disposed)
            {
                return ValueTask.CompletedTask;
            }

            _disposed = true;
            client = _client;
            timer = _monitor;
            _client = null;
            _monitor = null;
        }

        timer?.Dispose();
        DisposeClient(client);
        if (_ownedSerializerProvider is IDisposable disposableProvider)
        {
            disposableProvider.Dispose();
        }

        return ValueTask.CompletedTask;
    }
}
