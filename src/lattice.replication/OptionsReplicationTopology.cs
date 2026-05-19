using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationTopology"/> implementation registered
/// by <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Projects <see cref="LatticeReplicationOptions.ReplicationPeers"/>
/// (the unnamed / cluster-wide options instance) into a runtime-observable
/// surface by subscribing to
/// <see cref="IOptionsMonitor{TOptions}.OnChange(Action{TOptions, string})"/>
/// and diffing each reload against the last-seen peer set.
/// <para>
/// Empty, whitespace-only, and duplicate peer ids are filtered out at
/// snapshot time so the diff produces clean
/// <see cref="PeerChangeKind.Added"/> / <see cref="PeerChangeKind.Removed"/>
/// events. Reloads that are membership no-ops (e.g. a change to an
/// unrelated option) produce zero callbacks; reloads that flip the
/// membership set produce one callback per net addition and one per
/// net removal.
/// </para>
/// <para>
/// Subscriber callbacks are fanned out <em>outside</em> the topology's
/// internal lock so a subscriber that reentrantly calls
/// <see cref="Subscribe"/>, the subscription's <see cref="IDisposable.Dispose"/>,
/// or the topology's <see cref="Dispose"/> from inside its callback does
/// not deadlock. The subscriber set is snapshotted under the lock
/// before fan-out, so a concurrent unsubscribe during fan-out does not
/// mutate the iteration collection. Callbacks should still be
/// non-blocking - the replication driver uses this exclusively to queue
/// activation work on a fire-and-forget task.
/// </para>
/// </summary>
internal sealed class OptionsReplicationTopology : IReplicationTopology, IDisposable
{
    private readonly object _gate = new();
    private readonly HashSet<Subscription> _subscriptions = new();
    private HashSet<string> _current;
    private readonly IDisposable? _changeSubscription;
    private bool _disposed;

    /// <summary>
    /// Initialises the topology with the current options snapshot and
    /// hooks <see cref="IOptionsMonitor{TOptions}.OnChange"/> so
    /// subsequent reloads are diffed against the snapshot. The
    /// <paramref name="optionsMonitor"/> dependency is owned by the
    /// DI container; this type only retains an unsubscribe handle
    /// returned from <c>OnChange</c>.
    /// </summary>
    public OptionsReplicationTopology(IOptionsMonitor<LatticeReplicationOptions> optionsMonitor)
    {
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        _current = Project(optionsMonitor.CurrentValue.ReplicationPeers);
        // OnChange callbacks fire for every named instance the host
        // registers. The peer set lives on the unnamed (cluster-wide)
        // instance only; per-tree named instances inherit it via the
        // unnamed default in IOptionsMonitor.Get. Filter on the
        // canonical Options.DefaultName so a per-tree options reload
        // does not produce spurious peer-set diffs.
        _changeSubscription = optionsMonitor.OnChange((options, name) =>
        {
            if (!string.IsNullOrEmpty(name) && name != Options.DefaultName)
            {
                return;
            }
            Reconcile(Project(options.ReplicationPeers));
        });
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> CurrentPeers
    {
        get
        {
            lock (_gate)
            {
                // Defensive copy: the internal set is mutated under the
                // lock on every reload, and the contract says the
                // returned snapshot is a point-in-time view.
                return _current.Count == 0
                    ? Array.Empty<string>()
                    : _current.ToArray();
            }
        }
    }

    /// <inheritdoc />
    public IDisposable Subscribe(Action<PeerChanged> onChange)
    {
        ArgumentNullException.ThrowIfNull(onChange);
        var subscription = new Subscription(this, onChange);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _subscriptions.Add(subscription);
        }
        return subscription;
    }

    /// <summary>
    /// Diffs <paramref name="next"/> against the last-seen snapshot
    /// and fans Added/Removed callbacks to every active subscriber.
    /// Invoked exclusively from the <c>IOptionsMonitor.OnChange</c>
    /// callback (and from tests via the same monitor-reload seam).
    /// </summary>
    private void Reconcile(HashSet<string> next)
    {
        List<PeerChanged>? events = null;
        Action<PeerChanged>[] subscribers;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }
            // Net-additions: in next, not in current.
            foreach (var peer in next)
            {
                if (!_current.Contains(peer))
                {
                    (events ??= new List<PeerChanged>()).Add(new PeerChanged(peer, PeerChangeKind.Added));
                }
            }
            // Net-removals: in current, not in next.
            foreach (var peer in _current)
            {
                if (!next.Contains(peer))
                {
                    (events ??= new List<PeerChanged>()).Add(new PeerChanged(peer, PeerChangeKind.Removed));
                }
            }
            if (events is null)
            {
                return;
            }
            _current = next;
            // Snapshot the subscriber callbacks under the lock so a
            // concurrent Unsubscribe during fan-out does not mutate
            // the collection mid-iteration. Fan-out itself runs
            // outside the lock so a subscriber that reentrantly
            // calls Subscribe / Unsubscribe / Dispose does not
            // deadlock. Skip allocating the snapshot array when no
            // subscribers are attached (e.g. on tests or in a host
            // whose only consumer disposed its subscription).
            if (_subscriptions.Count == 0)
            {
                return;
            }
            subscribers = new Action<PeerChanged>[_subscriptions.Count];
            var i = 0;
            foreach (var sub in _subscriptions)
            {
                subscribers[i++] = sub.Callback;
            }
        }
        foreach (var evt in events)
        {
            foreach (var cb in subscribers)
            {
                try
                {
                    cb(evt);
                }
                catch
                {
                    // Subscriber exceptions must not break the fan-out
                    // to other subscribers nor poison the topology
                    // state. The replication driver's hosted-service
                    // subscriber logs and swallows; third-party
                    // subscribers are expected to do the same.
                }
            }
        }
    }

    private void Unsubscribe(Subscription subscription)
    {
        lock (_gate)
        {
            _subscriptions.Remove(subscription);
        }
    }

    /// <summary>
    /// Projects the raw <see cref="LatticeReplicationOptions.ReplicationPeers"/>
    /// collection into a hash-set with empty/whitespace/duplicate
    /// entries filtered out. The filter mirrors the long-standing
    /// behaviour of <c>ShardedReplogSink</c> and
    /// <c>ReplicationDriverActivationService</c>, both of which skip
    /// such entries; concentrating it here keeps the topology surface
    /// the single source of truth.
    /// </summary>
    private static HashSet<string> Project(IReadOnlyCollection<string>? raw)
    {
        if (raw is null || raw.Count == 0)
        {
            return new HashSet<string>(StringComparer.Ordinal);
        }
        var set = new HashSet<string>(raw.Count, StringComparer.Ordinal);
        foreach (var peer in raw)
        {
            if (string.IsNullOrWhiteSpace(peer))
            {
                continue;
            }
            set.Add(peer);
        }
        return set;
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
            _subscriptions.Clear();
        }
        _changeSubscription?.Dispose();
    }

    /// <summary>
    /// Per-subscriber handle returned from <see cref="Subscribe"/>.
    /// Disposing the handle removes the callback from the topology's
    /// subscriber set; subsequent <c>Dispose</c> calls are no-ops.
    /// </summary>
    private sealed class Subscription : IDisposable
    {
        private OptionsReplicationTopology? _owner;

        public Subscription(OptionsReplicationTopology owner, Action<PeerChanged> callback)
        {
            _owner = owner;
            Callback = callback;
        }

        public Action<PeerChanged> Callback { get; }

        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _owner, null);
            owner?.Unsubscribe(this);
        }
    }
}
