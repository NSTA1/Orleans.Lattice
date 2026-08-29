using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Plugins.History;

/// <summary>
/// The one place in this package that touches an Explorer service. It adapts the
/// shared history reader, the live follower, the session store and the
/// connection's <em>health</em> onto <see cref="IHistorySurface"/>.
/// <para>
/// Note what it does not expose: the connection itself never leaves this class.
/// The surface publishes only the connection's state and a disposable
/// subscription to it, so a view cannot issue a call on the connection even by
/// accident.
/// </para>
/// </summary>
/// <param name="reader">The shared history reader.</param>
/// <param name="follower">The shared live-tail follower.</param>
/// <param name="session">The session-scoped UI state store.</param>
/// <param name="connection">The state-API connection, read for its health only.</param>
internal sealed class HistorySurface(
    IHistoryReader reader,
    IHistoryLiveFollower follower,
    IUiSessionStore session,
    ILatticeStateConnection connection) : IHistorySurface
{
    private readonly IHistoryReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));

    private readonly IHistoryLiveFollower _follower =
        follower ?? throw new ArgumentNullException(nameof(follower));

    private readonly IUiSessionStore _session = session ?? throw new ArgumentNullException(nameof(session));

    private readonly ILatticeStateConnection _connection =
        connection ?? throw new ArgumentNullException(nameof(connection));

    /// <inheritdoc />
    public LatticeConnectionState ConnectionState => _connection.Status.State;

    /// <inheritdoc />
    public string? InspectedKey(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var key = _session.GetOrDefault(DataSelection.SelectedKey(treeId), string.Empty);
        return string.IsNullOrEmpty(key) ? null : key;
    }

    /// <inheritdoc />
    public IDisposable ObserveConnection(Action<LatticeConnectionState> onChanged)
    {
        ArgumentNullException.ThrowIfNull(onChanged);
        return new ConnectionSubscription(_connection, onChanged);
    }

    /// <inheritdoc />
    public Task<HistoryPage> LoadAsync(
        string treeId,
        string key,
        int limit,
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(key);
        return _reader.LoadAsync(treeId, key, limit, continuationToken, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<HistoryRevisionRow> FollowAsync(
        string treeId,
        HistoryLiveTail tail,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(tail);
        return _follower.FollowAsync(treeId, tail, cancellationToken);
    }

    /// <summary>
    /// One view's connection-health subscription. Holding the unsubscribe in a
    /// disposable rather than on the surface keeps the handler's lifetime tied to
    /// the view that installed it, so a torn-down view never leaves one behind on
    /// the circuit-scoped connection.
    /// </summary>
    private sealed class ConnectionSubscription : IDisposable
    {
        private readonly ILatticeStateConnection _connection;
        private readonly Action<LatticeConnectionStatus> _handler;
        private bool _disposed;

        public ConnectionSubscription(ILatticeStateConnection connection, Action<LatticeConnectionState> onChanged)
        {
            _connection = connection;
            _handler = status => onChanged(status.State);
            _connection.StatusChanged += _handler;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _connection.StatusChanged -= _handler;
        }
    }
}
