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
/// background health monitor that auto-recovers a faulted endpoint, and degrades
/// to a disconnected state once the configured grace window elapses.
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
                SetFaulted(Friendly(ex));
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
    public Task<StructureResponse> GetTreeStructureAsync(StructureRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetTreeStructureAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<EntryScanResponse> ScanEntriesAsync(EntryScanRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.ScanEntriesAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<EntryGetResponse> GetEntryAsync(EntryGetRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetEntryAsync(request, ct), cancellationToken);

    /// <inheritdoc />
    public Task<TreeMetricsSnapshot> GetMetricsSnapshotAsync(TreeMetricsRequest request, CancellationToken cancellationToken = default)
        => ExecuteAsync((c, ct) => c.GetMetricsSnapshotAsync(request, ct), cancellationToken);

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
                    SetFaulted(Friendly(ex));
                }

                throw new LatticeStateApiException(Friendly(ex), ex) { IsTransient = IsTransient(ex) };
            }
        }
    }

    private async IAsyncEnumerable<T> StreamAsync<T>(
        Func<ILatticeStateClient, CancellationToken, IAsyncEnumerable<T>> operation,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var client = CurrentClientOrThrow();
        await using var enumerator = operation(client, cancellationToken).GetAsyncEnumerator(cancellationToken);
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
                    yield break;
                }

                SetFaulted(Friendly(ex));
                throw new LatticeStateApiException(Friendly(ex), ex) { IsTransient = false };
            }

            OnSuccess();
            yield return current;
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

    private void SetFaulted(string message)
    {
        LatticeConnectionStatus? raise;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            raise = SetUnlocked(LatticeConnectionState.Faulted, _settings?.Address, message);
        }

        if (raise is not null)
        {
            RaiseStatusChanged(raise);
        }
    }

    private LatticeConnectionStatus? SetUnlocked(LatticeConnectionState state, string? endpoint, string message)
    {
        var next = new LatticeConnectionStatus(state, endpoint, message);
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

    private static string Friendly(RpcException ex) => ex.StatusCode switch
    {
        StatusCode.Unavailable => "The state API is unavailable. Check the endpoint and that the service is running.",
        StatusCode.DeadlineExceeded => "The state API did not respond in time.",
        StatusCode.Unauthenticated => "Authentication is required to access the state API.",
        StatusCode.PermissionDenied => "Access to the state API was denied.",
        StatusCode.Unimplemented => "The endpoint does not expose the Lattice state API.",
        StatusCode.NotFound => "The requested tree or entry was not found.",
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
