namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Test double for <see cref="IReplicationTopology"/>. Tests drive the
/// initial snapshot via the constructor and emit synthetic Added/Removed
/// notifications via <see cref="EmitAdded"/> / <see cref="EmitRemoved"/>.
/// Used in unit tests of <c>ReplicationDriverActivationService</c> and
/// downstream subscribers; the production default
/// <c>OptionsReplicationTopology</c> is exercised in its own unit suite.
/// </summary>
internal sealed class FakeReplicationTopology : IReplicationTopology
{
    private readonly object _gate = new();
    private readonly HashSet<string> _peers;
    private readonly List<Action<PeerChanged>> _subscribers = new();

    public FakeReplicationTopology(IEnumerable<string>? peers = null)
    {
        _peers = peers is null
            ? new HashSet<string>(StringComparer.Ordinal)
            : new HashSet<string>(peers.Where(p => !string.IsNullOrWhiteSpace(p)), StringComparer.Ordinal);
    }

    public IReadOnlyCollection<string> CurrentPeers
    {
        get
        {
            lock (_gate)
            {
                return _peers.ToArray();
            }
        }
    }

    public IDisposable Subscribe(Action<PeerChanged> onChange)
    {
        ArgumentNullException.ThrowIfNull(onChange);
        lock (_gate)
        {
            _subscribers.Add(onChange);
        }
        return new SubscriptionToken(this, onChange);
    }

    /// <summary>Synthesises a runtime peer-added notification.</summary>
    public void EmitAdded(string peer)
    {
        Action<PeerChanged>[] cbs;
        lock (_gate)
        {
            _peers.Add(peer);
            cbs = _subscribers.ToArray();
        }
        var evt = new PeerChanged(peer, PeerChangeKind.Added);
        foreach (var cb in cbs) cb(evt);
    }

    /// <summary>Synthesises a runtime peer-removed notification.</summary>
    public void EmitRemoved(string peer)
    {
        Action<PeerChanged>[] cbs;
        lock (_gate)
        {
            _peers.Remove(peer);
            cbs = _subscribers.ToArray();
        }
        var evt = new PeerChanged(peer, PeerChangeKind.Removed);
        foreach (var cb in cbs) cb(evt);
    }

    public int SubscriberCount
    {
        get { lock (_gate) return _subscribers.Count; }
    }

    private sealed class SubscriptionToken : IDisposable
    {
        private FakeReplicationTopology? _owner;
        private readonly Action<PeerChanged> _callback;

        public SubscriptionToken(FakeReplicationTopology owner, Action<PeerChanged> callback)
        {
            _owner = owner;
            _callback = callback;
        }

        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _owner, null);
            if (owner is null) return;
            lock (owner._gate)
            {
                owner._subscribers.Remove(_callback);
            }
        }
    }
}
