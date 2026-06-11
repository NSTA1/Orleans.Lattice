using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Direction of a recorded per-peer contact. Carried by every
/// <see cref="ReplicationPeerSnapshot"/> and stamped onto the
/// <c>direction</c> tag of the bidirectional observable gauges
/// (<see cref="LatticeReplicationMetrics.ConsecutiveErrorsName"/>,
/// <see cref="LatticeReplicationMetrics.LastContactSecondsName"/>).
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationContactDirection)]
public enum ReplicationContactDirection
{
    /// <summary>
    /// Local sender to remote peer. Recorded by the per-<c>(tree, peer)</c>
    /// shipper grain after a peer accepts a shipped batch (including the
    /// periodic empty liveness probe).
    /// </summary>
    Outbound = 0,

    /// <summary>
    /// Remote peer to local receiver. Recorded by the inbound apply
    /// pipeline after a per-origin run of entries authored by the
    /// named peer applies (or fails) on the local cluster.
    /// </summary>
    Inbound = 1,
}

/// <summary>
/// Per-peer replication telemetry state. Backs the observable gauges declared
/// on <see cref="LatticeReplicationMetrics"/>:
/// <c>entries_behind</c> and <c>bytes_behind</c> (outbound-only - the
/// receiver does not track a per-peer backlog into itself), plus
/// <c>consecutive_errors</c> and <c>last_contact_seconds</c>
/// (bidirectional, tagged with <see cref="LatticeReplicationMetrics.TagDirection"/>).
/// Instances are designed to be registered as a singleton by
/// <c>AddLatticeReplication</c> - the constructor wires the observable
/// gauges, so a single instance is sufficient per silo.
/// </summary>
/// <remarks>
/// Updates are recorded by the ship / apply paths. The class is
/// thread-safe: concurrent updates to different
/// <c>(tree, peer, direction)</c> triples do not contend, and updates
/// to the same triple use per-entry locks. <see cref="GetTimestamp"/>
/// is overridable to support deterministic tests of
/// <see cref="ReplicationPeerSnapshot.LastContactSeconds"/>.
/// </remarks>
public class ReplicationPeerStats
{
    // The four observable gauges declared on LatticeReplicationMetrics.Meter
    // are registered exactly once per process. Their callbacks read from the
    // single _current slot below, which is updated by every constructor.
    // This avoids leaking gauge registrations (and the closures bound to a
    // specific instance) into the static meter every time another instance
    // is created - important for hosts that re-register the singleton during
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
            description: "Consecutive contact-attempt failures since the last successful contact, tagged by direction.");

        meter.CreateObservableGauge<double>(
            LatticeReplicationMetrics.LastContactSecondsName,
            static () => _current?.ObserveLastContactSeconds() ?? Array.Empty<Measurement<double>>(),
            unit: "s",
            description: "Wall-clock seconds elapsed since the last successful contact with the named peer, tagged by direction.");
    }

    /// <summary>
    /// Records the current per-peer outbound backlog. Called by the
    /// sender each time the WAL cursor advances or the peer cursor moves
    /// so the <c>entries_behind</c> / <c>bytes_behind</c> gauges report
    /// a current view. Backlog is outbound-only by design - the receiver
    /// does not track a per-peer backlog into itself - so this method
    /// has no inbound counterpart.
    /// </summary>
    public void RecordBacklog(string tree, string peer, long entriesBehind, long bytesBehind)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        var entry = state.GetOrAdd(new PeerKey(tree, peer, ReplicationContactDirection.Outbound), static _ => new PeerState());
        lock (entry)
        {
            entry.EntriesBehind = entriesBehind;
            entry.BytesBehind = bytesBehind;
        }
    }

    /// <summary>
    /// Records a successful outbound contact with the named peer (the
    /// local sender shipped a batch and the peer acknowledged it,
    /// including the periodic empty liveness probe). Resets the
    /// outbound consecutive-error counter to zero and stamps the
    /// outbound last-contact timestamp.
    /// </summary>
    public void RecordSuccess(string tree, string peer) =>
        RecordSuccessCore(tree, peer, ReplicationContactDirection.Outbound);

    /// <summary>
    /// Records a failed outbound ship attempt against the named peer.
    /// Increments the outbound consecutive-error counter; does not
    /// update the outbound last-contact timestamp.
    /// </summary>
    public void RecordError(string tree, string peer) =>
        RecordErrorCore(tree, peer, ReplicationContactDirection.Outbound);

    /// <summary>
    /// Records a successful inbound contact with the named peer (a
    /// per-origin run of entries authored by the peer applied
    /// successfully on the local receiver). Resets the inbound
    /// consecutive-error counter to zero and stamps the inbound
    /// last-contact timestamp.
    /// </summary>
    public void RecordInboundSuccess(string tree, string originPeer) =>
        RecordSuccessCore(tree, originPeer, ReplicationContactDirection.Inbound);

    /// <summary>
    /// Records a failed inbound apply attempt for entries authored by
    /// the named peer. Increments the inbound consecutive-error
    /// counter; does not update the inbound last-contact timestamp.
    /// </summary>
    public void RecordInboundError(string tree, string originPeer) =>
        RecordErrorCore(tree, originPeer, ReplicationContactDirection.Inbound);

    private void RecordSuccessCore(string tree, string peer, ReplicationContactDirection direction)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        var entry = state.GetOrAdd(new PeerKey(tree, peer, direction), static _ => new PeerState());
        lock (entry)
        {
            entry.ConsecutiveErrors = 0;
            entry.LastContactTimestamp = GetTimestamp();
        }
    }

    private void RecordErrorCore(string tree, string peer, ReplicationContactDirection direction)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(peer);

        var entry = state.GetOrAdd(new PeerKey(tree, peer, direction), static _ => new PeerState());
        lock (entry)
        {
            entry.ConsecutiveErrors++;
        }
    }

    /// <summary>
    /// Returns a point-in-time snapshot of every recorded
    /// <c>(tree, peer, direction)</c> triple's state. Useful for
    /// diagnostics and for asserting on metric inputs in tests.
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

            // GetTimestamp() reads DateTimeOffset.UtcNow, which is not
            // guaranteed monotonic, so a contact stamped on another
            // thread can land microseconds ahead of this snapshot's now;
            // floor the elapsed at zero - negative "seconds since last
            // contact" is never meaningful.
            var elapsed = lastContact is null
                ? double.NaN
                : Math.Max(0d, (now - lastContact.Value).TotalSeconds);

            list.Add(new ReplicationPeerSnapshot(
                kv.Key.Tree,
                kv.Key.Peer,
                entries,
                bytes,
                errors,
                elapsed)
            {
                Direction = kv.Key.Direction,
            });
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
            if (kv.Key.Direction != ReplicationContactDirection.Outbound)
            {
                continue;
            }
            long value;
            lock (kv.Value) { value = kv.Value.EntriesBehind; }
            yield return MeasureOutbound(value, kv.Key);
        }
    }

    private IEnumerable<Measurement<long>> ObserveBytesBehind()
    {
        foreach (var kv in state)
        {
            if (kv.Key.Direction != ReplicationContactDirection.Outbound)
            {
                continue;
            }
            long value;
            lock (kv.Value) { value = kv.Value.BytesBehind; }
            yield return MeasureOutbound(value, kv.Key);
        }
    }

    private IEnumerable<Measurement<long>> ObserveConsecutiveErrors()
    {
        foreach (var kv in state)
        {
            long value;
            lock (kv.Value) { value = kv.Value.ConsecutiveErrors; }
            yield return MeasureDirectional(value, kv.Key);
        }
    }

    private IEnumerable<Measurement<double>> ObserveLastContactSeconds()
    {
        var now = GetTimestamp();
        foreach (var kv in state)
        {
            DateTimeOffset? lastContact;
            lock (kv.Value) { lastContact = kv.Value.LastContactTimestamp; }
            // Floor at zero - see Snapshot() for the non-monotonic
            // wall-clock rationale.
            var elapsed = lastContact is null
                ? double.NaN
                : Math.Max(0d, (now - lastContact.Value).TotalSeconds);
            yield return new Measurement<double>(elapsed,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, kv.Key.Tree),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, kv.Key.Peer),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagDirection, DirectionTag(kv.Key.Direction)));
        }
    }

    private static Measurement<long> MeasureOutbound(long value, PeerKey key) =>
        new(value,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, key.Tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, key.Peer));

    private static Measurement<long> MeasureDirectional(long value, PeerKey key) =>
        new(value,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, key.Tree),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, key.Peer),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagDirection, DirectionTag(key.Direction)));

    private static string DirectionTag(ReplicationContactDirection direction) =>
        direction == ReplicationContactDirection.Inbound
            ? LatticeReplicationMetrics.DirectionInbound
            : LatticeReplicationMetrics.DirectionOutbound;

    private readonly record struct PeerKey(string Tree, string Peer, ReplicationContactDirection Direction);

    private sealed class PeerState
    {
        public long EntriesBehind;
        public long BytesBehind;
        public long ConsecutiveErrors;
        public DateTimeOffset? LastContactTimestamp;
    }
}

/// <summary>
/// Point-in-time snapshot of a single peer's replication telemetry state
/// for one <see cref="ReplicationContactDirection"/>.
/// </summary>
/// <param name="Tree">The replicated tree id.</param>
/// <param name="Peer">The remote peer cluster id.</param>
/// <param name="EntriesBehind">WAL entries yet to ship to the peer (outbound rows only; zero on inbound rows).</param>
/// <param name="BytesBehind">Cumulative payload bytes yet to ship to the peer (outbound rows only; zero on inbound rows).</param>
/// <param name="ConsecutiveErrors">Consecutive contact-attempt failures since the last success in this direction.</param>
/// <param name="LastContactSeconds">
/// Wall-clock seconds elapsed since the last successful contact in this
/// direction, or <see cref="double.NaN"/> if the peer has never been
/// contacted in this direction.
/// </param>
public readonly record struct ReplicationPeerSnapshot(
    string Tree,
    string Peer,
    long EntriesBehind,
    long BytesBehind,
    long ConsecutiveErrors,
    double LastContactSeconds)
{
    /// <summary>
    /// Direction of the recorded contact. Defaults to
    /// <see cref="ReplicationContactDirection.Outbound"/> so existing
    /// positional-constructor call sites continue to compile and
    /// describe the historically outbound-only telemetry.
    /// </summary>
    public ReplicationContactDirection Direction { get; init; } = ReplicationContactDirection.Outbound;
}
