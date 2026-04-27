using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/>
/// instruments for <c>Orleans.Lattice.Replication</c>. Mirrors the structure
/// of <c>Orleans.Lattice.LatticeMetrics</c>: every replication instrument is
/// published on a single <see cref="Meter"/> named <see cref="MeterName"/> so
/// an OpenTelemetry pipeline can subscribe once and receive every replication
/// metric. Subsequent replication phases extend this same meter rather than
/// introducing additional meters.
/// </summary>
/// <remarks>
/// Instruments fall into two shapes:
/// <list type="bullet">
///   <item>
///     <b>Per-peer gauges</b> — <c>entries_behind</c>, <c>bytes_behind</c>,
///     <c>consecutive_errors</c>, <c>last_contact_seconds</c>. Implemented as
///     <see cref="ObservableGauge{T}"/> instruments backed by a singleton
///     <see cref="ReplicationPeerStats"/>. Tagged with <see cref="TagTree"/>
///     and <see cref="TagPeer"/>.
///   </item>
///   <item>
///     <b>Per-operation histograms</b> — <c>ship_duration</c>,
///     <c>apply_duration</c>. Reported in milliseconds as <c>double</c>.
///     Tagged with <see cref="TagTree"/>, <see cref="TagPeer"/>, and (for
///     terminal outcomes) <see cref="TagOutcome"/>.
///   </item>
/// </list>
/// </remarks>
public static class LatticeReplicationMetrics
{
    /// <summary>
    /// The root meter name for all <c>Orleans.Lattice.Replication</c>
    /// telemetry. Internal telemetry hooks and external subscribers must
    /// reference this constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice.replication";

    /// <summary>Tag key for the logical tree id.</summary>
    public const string TagTree = "tree";

    /// <summary>
    /// Tag key for the remote peer cluster id. Together with
    /// <see cref="TagTree"/> this uniquely identifies a per-peer cursor.
    /// </summary>
    public const string TagPeer = "peer";

    /// <summary>
    /// Tag key for the terminal outcome of a ship / apply attempt
    /// (e.g. <c>ok</c>, <c>error</c>).
    /// </summary>
    public const string TagOutcome = "outcome";

    /// <summary>
    /// Tag key for the dead-letter enqueue / removal reason. Values are
    /// drawn from <see cref="ReasonDiscarded"/>, <see cref="ReasonReplayed"/>,
    /// <see cref="ReasonEvicted"/>, and <see cref="ReasonUnknown"/>.
    /// </summary>
    public const string TagReason = "reason";

    /// <summary>Reason tag value: entry removed by an explicit operator <c>Discard</c> call.</summary>
    public const string ReasonDiscarded = "discarded";

    /// <summary>Reason tag value: entry removed by a successful <c>Replay</c>.</summary>
    public const string ReasonReplayed = "replayed";

    /// <summary>Reason tag value: entry removed by FIFO capacity eviction during a later enqueue.</summary>
    public const string ReasonEvicted = "evicted";

    /// <summary>
    /// Reason tag value: catch-all bucket for enqueue causes the inbound
    /// apply pipeline could not classify more specifically. Future
    /// observability work will partition this further.
    /// </summary>
    public const string ReasonUnknown = "unknown";

    /// <summary>
    /// The meter that owns every replication instrument. Exposed publicly so
    /// integration tests and custom OpenTelemetry exporters can subscribe by
    /// reference rather than by name.
    /// </summary>
    public static readonly Meter Meter = new(MeterName);

    // --- Per-operation histograms ------------------------------------------------

    /// <summary>
    /// Histogram of outbound ship-batch durations. Recorded by the sender
    /// after each batch attempt — both successful sends (<see cref="TagOutcome"/>
    /// = <c>ok</c>) and failed sends (<see cref="TagOutcome"/> = <c>error</c>)
    /// — so operators can distinguish steady-state ship latency from
    /// failure-path latency.
    /// </summary>
    public static readonly Histogram<double> ShipDuration =
        Meter.CreateHistogram<double>("orleans.lattice.replication.ship.duration", unit: "ms",
            description: "Duration of outbound ship-batch attempts, tagged by tree, peer and outcome.");

    /// <summary>
    /// Histogram of inbound apply-batch durations. Recorded by the receiver
    /// after each batch is applied (or rejected). Tagged by
    /// <see cref="TagTree"/>, <see cref="TagPeer"/>, and
    /// <see cref="TagOutcome"/>.
    /// </summary>
    public static readonly Histogram<double> ApplyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.replication.apply.duration", unit: "ms",
            description: "Duration of inbound apply-batch attempts, tagged by tree, peer and outcome.");

    // --- Dead-letter queue counters ---------------------------------------------

    /// <summary>
    /// Counter of <see cref="ReplogEntry"/> records parked on the per-tree
    /// dead-letter queue after exhausting
    /// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> consecutive
    /// apply attempts on the same
    /// <c>(treeId, originClusterId, timestamp, key, op)</c> tuple. Tagged
    /// by <see cref="TagTree"/> and <see cref="TagReason"/>.
    /// </summary>
    public static readonly Counter<long> DeadLetterEnqueued =
        Meter.CreateCounter<long>("orleans.lattice.replication.dead_letter.enqueued", unit: "{entry}",
            description: "Replog entries parked on the per-tree dead-letter queue, tagged by tree and reason.");

    /// <summary>
    /// Counter of entries removed from the per-tree dead-letter queue.
    /// Tagged by <see cref="TagTree"/> and <see cref="TagReason"/>; the
    /// reason tag distinguishes operator <c>Discard</c>
    /// (<see cref="ReasonDiscarded"/>), successful <c>Replay</c>
    /// (<see cref="ReasonReplayed"/>), and FIFO capacity eviction during
    /// a later enqueue (<see cref="ReasonEvicted"/>).
    /// </summary>
    public static readonly Counter<long> DeadLetterRemoved =
        Meter.CreateCounter<long>("orleans.lattice.replication.dead_letter.removed", unit: "{entry}",
            description: "Entries removed from the per-tree dead-letter queue, tagged by tree and reason.");

    // --- WAL garbage-collection counters ----------------------------------------

    /// <summary>
    /// Counter of WAL entries removed by the per-tree garbage collector
    /// (<see cref="ILatticeReplicationGc"/>). Tagged by
    /// <see cref="TagTree"/>. Incremented once per
    /// <see cref="ILatticeReplicationGc.RunOnceAsync"/> pass with the
    /// total number of entries trimmed across every shard during that
    /// pass; a pass that yielded no eligible entries does not record
    /// (the gauge stays sparse).
    /// </summary>
    public static readonly Counter<long> WalEntriesTrimmed =
        Meter.CreateCounter<long>("orleans.lattice.replication.wal.entries_trimmed", unit: "{entry}",
            description: "WAL entries removed by the per-tree garbage collector, tagged by tree.");

    // --- Per-peer observable gauges ----------------------------------------------
    //
    // The gauges below are registered lazily by ReplicationPeerStats so the
    // constructor of that singleton drives instrument creation. The constants
    // here document the canonical instrument names that subscribers (and
    // assertion-based tests) match against.

    /// <summary>
    /// Canonical name of the <c>entries_behind</c> observable gauge.
    /// Reports the number of WAL entries the local sender has yet to ship
    /// to the named peer, broken down per tree.
    /// </summary>
    public const string EntriesBehindName = "orleans.lattice.replication.peer.entries_behind";

    /// <summary>
    /// Canonical name of the <c>bytes_behind</c> observable gauge.
    /// Reports the cumulative payload size of WAL entries the local sender
    /// has yet to ship to the named peer.
    /// </summary>
    public const string BytesBehindName = "orleans.lattice.replication.peer.bytes_behind";

    /// <summary>
    /// Canonical name of the <c>consecutive_errors</c> observable gauge.
    /// Reports the number of consecutive ship attempts that have failed
    /// since the last success. Resets to zero on the first success.
    /// </summary>
    public const string ConsecutiveErrorsName = "orleans.lattice.replication.peer.consecutive_errors";

    /// <summary>
    /// Canonical name of the <c>last_contact_seconds</c> observable gauge.
    /// Reports the wall-clock seconds elapsed since the local sender last
    /// successfully contacted the named peer. <c>NaN</c> indicates the peer
    /// has never been contacted.
    /// </summary>
    public const string LastContactSecondsName = "orleans.lattice.replication.peer.last_contact_seconds";
}
