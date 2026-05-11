using Orleans.Lattice.BPlusTree.Grains;
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
    /// <see cref="TagOutcome"/> value: the entry was applied successfully
    /// (point apply or range delete). Recorded by
    /// <see cref="ApplyDuration"/> for both directly applied entries and
    /// entries drained from the causal-apply buffer.
    /// </summary>
    public const string OutcomeSuccess = "success";

    /// <summary>
    /// <see cref="TagOutcome"/> value: the entry was short-circuited by
    /// the receiver before merge — either the per-origin high-water-mark
    /// already covers <see cref="WalRecord.Timestamp"/>, or a
    /// defence-in-depth gate detected a local-origin entry that must
    /// not loop back onto its authoring cluster.
    /// </summary>
    public const string OutcomeDedup = "dedup";

    /// <summary>
    /// <see cref="TagOutcome"/> value: the apply attempt threw. Recorded
    /// by <see cref="ApplyDuration"/> in the <c>finally</c> path before
    /// the exception unwinds. Includes payload-shape faults
    /// (<see cref="ArgumentException"/>, <see cref="InvalidOperationException"/>),
    /// transport / IO failures, and any other unhandled exception out
    /// of the apply pipeline.
    /// </summary>
    public const string OutcomeFailure = "failure";

    /// <summary>
    /// <see cref="TagOutcome"/> value: the entry parked on the causal-apply
    /// buffer because its declared <see cref="WalRecord.VectorClock"/>
    /// was not yet dominated by the local vector clock. The original
    /// delivery is not advanced through the high-water-mark; the entry
    /// re-enters the apply pipeline through the buffer drain when its
    /// dependencies arrive.
    /// </summary>
    public const string OutcomeParkedCausalBuffer = "parked-causal-buffer";

    /// <summary>
    /// <see cref="TagOutcome"/> value: the entry was suppressed by the
    /// per-tree shadow-forward dedupe cache because an identity tuple
    /// (<c>(originClusterId, timestamp, key, op)</c>) matching this
    /// entry was already applied since the last cache eviction. The
    /// duplicate arises naturally when a structural rewrite (shard
    /// split / merge / saga compensate) shadow-forwards a user write
    /// into a different shard: both emits ride the WAL with identical
    /// identity tuples. The receiver applies the shadow-forwarded
    /// write exactly once.
    /// </summary>
    public const string OutcomeShadowForwardDedup = "shadow-forward-dedup";

    /// <summary>
    /// Tag key for the dead-letter enqueue / removal reason. Values are
    /// drawn from <see cref="ReasonDiscarded"/>, <see cref="ReasonReplayed"/>,
    /// <see cref="ReasonEvicted"/>, <see cref="ReasonSchema"/>,
    /// <see cref="ReasonHlcSkew"/>, <see cref="ReasonOversized"/>,

    /// and <see cref="ReasonUnknown"/>.
    /// </summary>
    public const string TagReason = "reason";

    /// <summary>
    /// Tag key for the per-tree shard component of the causal-apply buffer
    /// (<see cref="ApplyBufferedEntries"/> / <see cref="ApplyBufferBytes"/>).
    /// The current causal-apply buffer is one-per-tree, so the canonical
    /// tag value is <c>"0"</c>; the dimension is reserved up front so a
    /// future per-shard buffer can populate it without a wire-format break.
    /// </summary>
    public const string TagShard = "shard";

    /// <summary>
    /// Tag key for the authoring cluster id of an inbound replication
    /// entry. Used by per-origin diagnostic instruments such as
    /// <see cref="ApplyFifoViolations"/>. Distinct from
    /// <see cref="TagPeer"/>, which identifies the immediate transport
    /// hop: under transitive replication (A &#8594; B &#8594; C) the
    /// origin and peer can differ.
    /// </summary>
    public const string TagOrigin = "origin";

    /// <summary>Reason tag value: entry removed by an explicit operator <c>Discard</c> call.</summary>
    public const string ReasonDiscarded = "discarded";

    /// <summary>Reason tag value: entry removed by a successful <c>Replay</c>.</summary>
    public const string ReasonReplayed = "replayed";

    /// <summary>Reason tag value: entry removed by FIFO capacity eviction during a later enqueue.</summary>
    public const string ReasonEvicted = "evicted";

    /// <summary>
    /// Reason tag value: enqueue cause was a malformed or self-inconsistent
    /// <see cref="WalRecord"/> the receiver could not interpret. Examples
    /// include a <see cref="MutationKind.Set"/> with a <see langword="null"/>
    /// <see cref="WalRecord.Value"/>, an unrecognised
    /// <see cref="WalRecord.Mode"/>, or a missing required field
    /// (<see cref="WalRecord.TreeId"/> / <see cref="WalRecord.OriginClusterId"/>).
    /// Surfaces from <see cref="ArgumentException"/> /
    /// <see cref="InvalidOperationException"/> raised by the canonical
    /// <see cref="ReplicationApplier"/>.
    /// </summary>
    public const string ReasonSchema = "schema";

    /// <summary>
    /// Reason tag value: enqueue cause was implausible HLC skew between
    /// the receiver's wall clock and the entry's
    /// <see cref="WalRecord.Timestamp"/>. Reserved for receivers that
    /// surface <see cref="HybridLogicalClock"/>-related faults as
    /// classified exceptions; the canonical applier does not currently
    /// raise this class of failure.
    /// </summary>
    public const string ReasonHlcSkew = "hlc_skew";

    /// <summary>
    /// Reason tag value: enqueue cause was an entry whose serialised size
    /// (key length + value length, plus envelope overhead) exceeded the
    /// receiver's per-entry size ceiling. Reserved for hosts that wrap
    /// the canonical applier in a size-validating decorator.
    /// </summary>
    public const string ReasonOversized = "oversized";

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

    /// <summary>
    /// Histogram of receiver-side replication lag, computed at successful
    /// apply time as <c>now - entry.Timestamp.WallClockTicks</c>. Reported
    /// in milliseconds and clamped to a non-negative value (a future
    /// timestamp from a faster-moving peer reports as <c>0</c> rather than
    /// a negative sample). Recorded once per successfully applied point
    /// operation (<see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>);
    /// range deletes carry <see cref="HybridLogicalClock.Zero"/> by design
    /// and do not contribute. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagPeer"/> (the entry's <see cref="WalRecord.OriginClusterId"/>
    /// , which under transitive replication may differ from the immediate
    /// transport hop).
    /// </summary>
    public static readonly Histogram<double> ApplyLag =
        Meter.CreateHistogram<double>("orleans.lattice.replication.apply.lag", unit: "ms",
            description: "Receiver-side replication lag at successful apply, tagged by tree and peer.");

    // --- Throughput counters (replog growth vs. ship rate) ----------------------

    /// <summary>
    /// Counter of <see cref="WalRecord"/> records appended to the
    /// per-tree write-ahead log on the local cluster. Incremented once
    /// per successful append at the
    /// <see cref="ShardedReplogSink"/> seam — i.e. counts entries that
    /// have committed durably onto the WAL, not entries the producer
    /// merely attempted to capture. Tagged by <see cref="TagTree"/>.
    /// <para>
    /// Pairs with <see cref="WalEntriesShipped"/> to surface the
    /// "replog growth-rate vs. ship-rate" ratio operators monitor for
    /// back-pressure: a steady-state replicating peer keeps the two
    /// counters tracking each other; a stalled or overwhelmed receiver
    /// shows growth outpacing ship.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalEntriesAppended =
        Meter.CreateCounter<long>("orleans.lattice.replication.wal.entries_appended", unit: "{entry}",
            description: "Replog entries committed to the local WAL, tagged by tree.");

    /// <summary>
    /// Counter of <see cref="WalRecord"/> records the local sender
    /// successfully shipped to a remote peer. Incremented once per
    /// entry inside an acknowledged outbound batch (i.e. by the count
    /// of entries in the batch envelope, summed only on successful
    /// ack). A failed ship — exception, RPC error, transport disposal —
    /// does not contribute. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Counter<long> WalEntriesShipped =
        Meter.CreateCounter<long>("orleans.lattice.replication.wal.entries_shipped", unit: "{entry}",
            description: "Replog entries acknowledged by a remote peer, tagged by tree and peer.");

    // --- Dead-letter queue counters ---------------------------------------------

    /// <summary>
    /// Counter of <see cref="WalRecord"/> records parked on the per-tree
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

    /// <summary>
    /// Canonical name of the <see cref="ApplyLag"/> histogram. Subscribers
    /// match against this constant rather than hard-coding the string.
    /// </summary>
    public const string ApplyLagName = "orleans.lattice.replication.apply.lag";

    /// <summary>
    /// Canonical name of the <see cref="ApplyDuration"/> histogram.
    /// Subscribers match against this constant rather than hard-coding
    /// the string.
    /// </summary>
    public const string ApplyDurationName = "orleans.lattice.replication.apply.duration";

    /// <summary>
    /// Canonical name of the <see cref="WalEntriesAppended"/> counter.
    /// </summary>
    public const string WalEntriesAppendedName = "orleans.lattice.replication.wal.entries_appended";

    /// <summary>
    /// Canonical name of the <see cref="WalEntriesShipped"/> counter.
    /// </summary>
    public const string WalEntriesShippedName = "orleans.lattice.replication.wal.entries_shipped";

    // --- Causal+ apply-buffer instruments ---------------------------------------

    /// <summary>
    /// UpDownCounter of <see cref="WalRecord"/> records currently parked
    /// in the receiver-side causal-apply buffer pending dependency
    /// satisfaction. Incremented on park, decremented on drain or
    /// overflow eviction. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagShard"/>.
    /// </summary>
    public static readonly UpDownCounter<long> ApplyBufferedEntries =
        Meter.CreateUpDownCounter<long>("orleans.lattice.replication.apply.buffered_entries", unit: "{entry}",
            description: "Replog entries currently parked in the causal-apply buffer, tagged by tree and shard.");

    /// <summary>
    /// UpDownCounter of cumulative serialised payload bytes parked in the
    /// receiver-side causal-apply buffer. Tracks the same lifecycle as
    /// <see cref="ApplyBufferedEntries"/>; together they bound the buffer
    /// against <see cref="LatticeReplicationOptions.CausalBufferMaxEntries"/>
    /// and <see cref="LatticeReplicationOptions.CausalBufferMaxBytes"/>.
    /// Tagged by <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// </summary>
    public static readonly UpDownCounter<long> ApplyBufferBytes =
        Meter.CreateUpDownCounter<long>("orleans.lattice.replication.apply.buffer_bytes", unit: "By",
            description: "Cumulative serialised payload size parked in the causal-apply buffer, tagged by tree and shard.");

    /// <summary>
    /// Histogram of the wall-clock interval, in milliseconds, between an
    /// entry parking on the causal-apply buffer and its subsequent
    /// successful drain. Recorded once per drained entry; clamped to a
    /// non-negative value. Tagged by <see cref="TagTree"/>. An entry that
    /// is evicted (overflow) instead of drained does not contribute to
    /// this histogram — only successful waits are observed.
    /// </summary>
    public static readonly Histogram<double> ApplyDependencyWaitMs =
        Meter.CreateHistogram<double>("orleans.lattice.replication.apply.dependency_wait_ms",
            description: "Wait time between park and drain for a buffered causal-apply entry, tagged by tree.");

    /// <summary>
    /// Counter of <see cref="WalRecord"/> records that the receiver
    /// could not apply immediately because their declared causal
    /// dependencies were not yet satisfied by the local vector clock.
    /// Incremented once per park (including overflow-evicted parks);
    /// duplicates are not counted. An alert on
    /// <c>rate &gt; 0</c> flags causal-skew health regardless of
    /// whether buffered entries eventually drain or evict.
    /// Tagged by <see cref="TagTree"/>.
    /// </summary>
    public static readonly Counter<long> ApplyCausalViolationsBlocked =
        Meter.CreateCounter<long>("orleans.lattice.replication.apply.causal_violations_blocked", unit: "{entry}",
            description: "Replog entries blocked by an unsatisfied causal dependency at apply time, tagged by tree.");

    /// <summary>
    /// Canonical name of the <see cref="ApplyBufferedEntries"/>
    /// up/down counter.
    /// </summary>
    public const string ApplyBufferedEntriesName = "orleans.lattice.replication.apply.buffered_entries";

    /// <summary>
    /// Canonical name of the <see cref="ApplyBufferBytes"/> up/down counter.
    /// </summary>
    public const string ApplyBufferBytesName = "orleans.lattice.replication.apply.buffer_bytes";

    /// <summary>
    /// Canonical name of the <see cref="ApplyDependencyWaitMs"/> histogram.
    /// </summary>
    public const string ApplyDependencyWaitMsName = "orleans.lattice.replication.apply.dependency_wait_ms";

    /// <summary>
    /// Canonical name of the <see cref="ApplyCausalViolationsBlocked"/>
    /// counter.
    /// </summary>
    public const string ApplyCausalViolationsBlockedName = "orleans.lattice.replication.apply.causal_violations_blocked";

    // --- Per-origin FIFO invariant ----------------------------------------------

    /// <summary>
    /// Counter of successful point applies whose source HLC was strictly
    /// less than the most recently applied source HLC for the same
    /// <c>(treeId, originClusterId)</c> pair. Pins the per-origin FIFO
    /// contract the causal-apply buffer (<see cref="CausalApplyBuffer"/>)
    /// relies on for occupancy bounds: under correct sender + transport
    /// behaviour the producer's partitioned change feed yields per-shard
    /// in WAL-offset order and each shard's WAL is HLC-monotonic per
    /// origin, so per-(origin, shard) FIFO is preserved end-to-end with
    /// no cross-shard sender serialisation. A sustained nonzero rate
    /// flags a transport-side regression that broke that invariant.
    /// <para>
    /// The counter is recorded after a successful apply (direct or drained)
    /// — never on park — so the underlying invariant tracks "what has
    /// been merged" rather than "what has been observed". A violation
    /// does not change apply behaviour: the entry is still applied, the
    /// HWM is still advanced. This is purely an observability surface.
    /// </para>
    /// <para>
    /// Tagged by <see cref="TagTree"/> and <see cref="TagOrigin"/>; the
    /// origin tag carries the entry's <see cref="WalRecord.OriginClusterId"/>
    /// so operators can attribute a regression to the authoring cluster
    /// rather than the immediate transport peer.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ApplyFifoViolations =
        Meter.CreateCounter<long>("orleans.lattice.replication.apply.fifo_violations", unit: "{entry}",
            description: "Successful applies whose source HLC was strictly less than the previous apply for the same (tree, origin), tagged by tree and origin.");

    /// <summary>
    /// Canonical name of the <see cref="ApplyFifoViolations"/> counter.
    /// </summary>
    public const string ApplyFifoViolationsName = "orleans.lattice.replication.apply.fifo_violations";

    // --- Auto-bootstrap detector ------------------------------------------------

    /// <summary>
    /// Counter incremented once per call to
    /// <see cref="ILatticeFallOffLogDetector.CheckAndTriggerAsync"/>
    /// that detected a fall-off-the-log condition for a
    /// <c>(treeName, sourceClusterId)</c> pair — i.e. the receiver's
    /// per-origin high-water-mark was strictly less than the sender's
    /// oldest available WAL entry HLC. Increments fire regardless of
    /// whether
    /// <see cref="LatticeReplicationOptions.AutoBootstrapOnFallOffLog"/>
    /// is enabled, so operators can alert on the detection event
    /// even when auto-recovery is disabled. Tagged by
    /// <see cref="TagTree"/> and <see cref="TagOrigin"/>.
    /// </summary>
    public static readonly Counter<long> PeerFellOffLog =
        Meter.CreateCounter<long>("orleans.lattice.replication.peer.fell_off_log", unit: "{event}",
            description: "Receiver fall-off-the-log detection events, tagged by tree and origin.");

    /// <summary>
    /// Canonical name of the <see cref="PeerFellOffLog"/> counter.
    /// </summary>
    public const string PeerFellOffLogName = "orleans.lattice.replication.peer.fell_off_log";
}
