using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Per-peer replication telemetry state. Backs the observable gauges declared
/// on <see cref="LatticeReplicationMetrics"/>: <c>entries_behind</c>,
/// <c>bytes_behind</c>, <c>consecutive_errors</c>, and
/// <c>last_contact_seconds</c>. Instances are designed to be registered as a
/// singleton by <c>AddLatticeReplication</c> — the constructor wires the
/// observable gauges, so a single instance is sufficient per silo.
/// </summary>
/// <remarks>
/// Updates are recorded by the ship / apply paths in subsequent replication
/// phases. The class is thread-safe: concurrent updates to different
/// <c>(tree, peer)</c> pairs do not contend, and updates to the same pair use
/// per-entry locks. <see cref="GetTimestamp"/> is overridable to support
/// deterministic tests of <see cref="ReplicationPeerSnapshot.LastContactSeconds"/>.
/// </remarks>
public class ReplicationPeerStats
{
    // The four observable gauges declared on LatticeReplicationMetrics.Meter
    // are registered exactly once per process. Their callbacks read from the
    // single _current slot below, which is updated by every constructor.
    // This avoids leaking gauge registrations (and the closures bound to a
    // specific instance) into the static meter every time another instance
    // is created — important for hosts that re-register the singleton during
    // integration-test setup, and for unit tests that intentionally create
    // throw-away instances.
    private static readonly object RegistrationLock = new();
    private static volatile ReplicationPeerStats? _current;
    private static bool _gaugesRegistered;

    private readonly ConcurrentDictionary<PeerKey, PeerState> state = new();

    /// <summary>
    /// Initialises a new instance and ensures the observable gauges declared
    /// on <see cref="LatticeReplicationMetrics"/> are registered on the
    /// shared meter. Gauge registration is process-wide and idempotent;
    /// observation always reflects the most recently constructed instance,
    /// which matches the DI singleton model used by
    /// <c>AddLatticeReplication</c>.
    /// </summary>
    public ReplicationPeerStats()
    {
        lock (RegistrationLock)
        {
            _current = this;
            if (!_gaugesRegistered)
            {
                RegisterGauges();
                _gaugesRegistered = true;
            }
        }
    }

    private static void RegisterGauges()
    {
        var meter = LatticeReplicationMetrics.Meter;

        meter.CreateObservableGauge<long>(
            LatticeReplicationMetrics.EntriesBehindName,
            static () => _current?.ObserveEntriesBehind() ?? Array.Empty<Measurement<long>>(),
            unit: "{entry}",
            description: "WAL entries the local sender has yet to ship to the named peer.");

        meter.CreateObservableGauge<long>(
            LatticeReplicationMetrics.BytesBehindName,
            static () => _current?.ObserveBytesBehind() ?? Array.Empty<Measurement<long>>(),
            unit: "By",
            description: "Cumulative payload bytes the local sender has yet to ship to the named peer.");

        meter.CreateObservableGauge<long>(
            LatticeReplicationMetrics.ConsecutiveErrorsName,
            static () => _current?.ObserveConsecutiveErrors() ?? Array.Empty<Measurement<long>>(),
            unit: "{error}",
            description: "Consecutive ship-attempt failures since the last successful contact.");

        meter.CreateObservableGauge<double>(
            LatticeReplicationMetrics.LastContactSecondsName,
            static () => _current?.ObserveLastContactSeconds() ?? Array.Empty<Measurement<double>>(),
            unit: "s",
            description: "Wall-clock seconds elapsed since the last successful contact with the named peer.");
    }

    /// <summary>
    /// Records the current per-peer backlog. Called by the sender each time
    /// the WAL cursor advances or the peer cursor moves so the
    /// <c>entries_behind</c> / <c>bytes_behind</c> gauges report a current
    /// view.
    /// </summary>
    public void RecordBacklog(string tree, string peer, long entriesBehind, long bytesBehind)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        var entry = state.GetOrAdd(new PeerKey(tree, peer), static _ => new PeerState());
        lock (entry)
        {
            entry.EntriesBehind = entriesBehind;
            entry.BytesBehind = bytesBehind;
        }
    }

    /// <summary>
    /// Records a successful contact with the named peer. Resets the
    /// consecutive-error counter to zero and stamps the last-contact
    /// timestamp.
    /// </summary>
    public void RecordSuccess(string tree, string peer)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        var entry = state.GetOrAdd(new PeerKey(tree, peer), static _ => new PeerState());
        lock (entry)
        {
            entry.ConsecutiveErrors = 0;
            entry.LastContactTimestamp = GetTimestamp();
        }
    }

    /// <summary>
    /// Records a failed ship attempt against the named peer. Increments the
    /// consecutive-error counter; does not update the last-contact timestamp.
    /// </summary>
    public void RecordError(string tree, string peer)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        var entry = state.GetOrAdd(new PeerKey(tree, peer), static _ => new PeerState());
        lock (entry)
        {
            entry.ConsecutiveErrors++;
        }
    }

    /// <summary>
    /// Returns a point-in-time snapshot of every recorded peer's state.
    /// Useful for diagnostics and for asserting on metric inputs in tests.
    /// </summary>
    public IReadOnlyCollection<ReplicationPeerSnapshot> Snapshot()
    {
        var now = GetTimestamp();
        var list = new List<ReplicationPeerSnapshot>(state.Count);
        foreach (var kv in state)
        {
            long entries, bytes, errors;
            DateTimeOffset? lastContact;
            lock (kv.Value)
            {
                entries = kv.Value.EntriesBehind;
                bytes = kv.Value.BytesBehind;
                errors = kv.Value.ConsecutiveErrors;
                lastContact = kv.Value.LastContactTimestamp;
            }

            var elapsed = lastContact is null
                ? double.NaN
                : (now - lastContact.Value).TotalSeconds;

            list.Add(new ReplicationPeerSnapshot(
                kv.Key.Tree,
                kv.Key.Peer,
                entries,
                bytes,
                errors,
                elapsed));
        }
        return list;
    }

    /// <summary>
    /// Returns the current wall-clock timestamp used to compute
    /// <see cref="ReplicationPeerSnapshot.LastContactSeconds"/>. Overridable
    /// to support deterministic tests.
    /// </summary>
    protected virtual DateTimeOffset GetTimestamp() => DateTimeOffset.UtcNow;

    private IEnumerable<Measurement<long>> ObserveEntriesBehind()
    {
        foreach (var kv in state)
        {
            long value;
            lock (kv.Value) { value = kv.Value.EntriesBehind; }
            yield return Measure(value, kv.Key);
        }
    }

    private IEnumerable<Measurement<long>> ObserveBytesBehind()
    {
        foreach (var kv in state)
        {
            long value;
            lock (kv.Value) { value = kv.Value.BytesBehind; }
            yield return Measure(value, kv.Key);
        }
    }

    private IEnumerable<Measurement<long>> ObserveConsecutiveErrors()
    {
        foreach (var kv in state)
        {
            long value;
            lock (kv.Value) { value = kv.Value.ConsecutiveErrors; }
            yield return Measure(value, kv.Key);
        }
    }

    private IEnumerable<Measurement<double>> ObserveLastContactSeconds()
    {
        var now = GetTimestamp();
        foreach (var kv in state)
        {
            DateTimeOffset? lastContact;
            lock (kv.Value) { lastContact = kv.Value.LastContactTimestamp; }
            var elapsed = lastContact is null
                ? double.NaN
                : (now - lastContact.Value).TotalSeconds;
            yield return new Measurement<double>(elapsed,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, kv.Key.Tree),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, kv.Key.Peer));
        }
    }

    private static Measurement<long> Measure(long value, PeerKey key) =>
        new(value,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, key.Tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, key.Peer));

    private readonly record struct PeerKey(string Tree, string Peer);

    private sealed class PeerState
    {
        public long EntriesBehind;
        public long BytesBehind;
        public long ConsecutiveErrors;
        public DateTimeOffset? LastContactTimestamp;
    }
}

/// <summary>
/// Point-in-time snapshot of a single peer's replication telemetry state.
/// </summary>
/// <param name="Tree">The replicated tree id.</param>
/// <param name="Peer">The remote peer cluster id.</param>
/// <param name="EntriesBehind">WAL entries yet to ship to the peer.</param>
/// <param name="BytesBehind">Cumulative payload bytes yet to ship to the peer.</param>
/// <param name="ConsecutiveErrors">Consecutive ship-attempt failures since the last success.</param>
/// <param name="LastContactSeconds">
/// Wall-clock seconds elapsed since the last successful contact, or
/// <see cref="double.NaN"/> if the peer has never been contacted.
/// </param>
public readonly record struct ReplicationPeerSnapshot(
    string Tree,
    string Peer,
    long EntriesBehind,
    long BytesBehind,
    long ConsecutiveErrors,
    double LastContactSeconds);
