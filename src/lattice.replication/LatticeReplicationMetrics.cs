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
///     <b>Per-peer gauges</b> - <c>entries_behind</c>, <c>bytes_behind</c>,
///     <c>consecutive_errors</c>, <c>last_contact_seconds</c>. Implemented as
///     <see cref="ObservableGauge{T}"/> instruments backed by a singleton
///     <see cref="ReplicationPeerStats"/>. Tagged with <see cref="TagTree"/>
///     and <see cref="TagPeer"/>.
///   </item>
///   <item>
///     <b>Per-operation histograms</b> - <c>ship_duration</c>,
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
    /// Tag key for the direction of a per-peer contact - <see cref="DirectionOutbound"/>
    /// when recorded by the local sender after a successful ship to the
    /// named peer, <see cref="DirectionInbound"/> when recorded by the
    /// local receiver after a successful apply of a batch authored by the
    /// named peer. Carried by the <see cref="ConsecutiveErrorsName"/> and
    /// <see cref="LastContactSecondsName"/> observable gauges - both of
    /// which are now bidirectional - so dashboards can split or aggregate
    /// the outbound and inbound timelines per <c>(tree, peer)</c> pair.
    /// The <see cref="EntriesBehindName"/> and <see cref="BytesBehindName"/>
    /// gauges remain outbound-only (the receiver does not track a per-peer
    /// backlog into itself) and emit a single series per
    /// <c>(tree, peer)</c> pair without the direction tag.
    /// </summary>
    public const string TagDirection = "direction";

    /// <summary>
    /// <see cref="TagDirection"/> value stamped on the outbound recordings -
    /// the local sender's <see cref="ReplicationPeerStats.RecordSuccess(string, string)"/>
    /// and <see cref="ReplicationPeerStats.RecordError(string, string)"/>
    /// call sites in <c>ReplicationShipperGrain</c>.
    /// </summary>
    public const string DirectionOutbound = "outbound";

    /// <summary>
    /// <see cref="TagDirection"/> value stamped on the inbound recordings -
    /// the local receiver's <see cref="ReplicationPeerStats.RecordInboundSuccess(string, string)"/>
    /// and <see cref="ReplicationPeerStats.RecordInboundError(string, string)"/>
    /// call sites in <c>ReplicationApplier</c>'s per-origin run path.
    /// </summary>
    public const string DirectionInbound = "inbound";

    /// <summary>
    /// <see cref="TagOutcome"/> value: the entry was applied successfully
    /// (point apply or range delete). Recorded by
    /// <see cref="ApplyDuration"/> for both directly applied entries and
    /// entries drained from the causal-apply buffer.
    /// </summary>
    public const string OutcomeSuccess = "success";

    /// <summary>
    /// <see cref="TagOutcome"/> value: the entry was short-circuited by
    /// the receiver before merge - either the per-origin high-water-mark
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
    /// after each batch attempt - both successful sends (<see cref="TagOutcome"/>
    /// = <c>ok</c>) and failed sends (<see cref="TagOutcome"/> = <c>error</c>)
    /// - so operators can distinguish steady-state ship latency from
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
    /// <see cref="ShardedReplogSink"/> seam - i.e. counts entries that
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
    /// ack). A failed ship - exception, RPC error, transport disposal -
    /// does not contribute. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Counter<long> WalEntriesShipped =
        Meter.CreateCounter<long>("orleans.lattice.replication.wal.entries_shipped", unit: "{entry}",
            description: "Replog entries acknowledged by a remote peer, tagged by tree and peer.");

    /// <summary>
    /// Counter of shipped <see cref="MutationKind.Set"/> entries whose
    /// value bytes were byte-identical to the value most recently
    /// shipped for the same key - the content-hash payload re-send rate.
    /// Incremented once per redundant entry as the shipper drains it
    /// onto the wire, only when
    /// <see cref="LatticeReplicationOptions.ContentHashDedupEnabled"/>
    /// is set (the counter never fires under the default-off behaviour).
    /// Pairs with <see cref="WalEntriesShipped"/> so operators can read
    /// the redundant fraction directly: a high ratio signals idempotent
    /// upstream retry logic re-sending the same value, which is the
    /// signal that justifies opting into a sender-manifest /
    /// receiver-pull-missing round trip. Tagged by <see cref="TagTree"/>
    /// and <see cref="TagPeer"/>.
    /// <para>
    /// The measurement counts entries as they are framed onto the wire,
    /// so a batch re-shipped after a transient transport failure counts
    /// its entries again - which is correct, because a re-ship is itself
    /// a redundant wire payload. The counter is observability-only and
    /// never elides or alters the bytes shipped.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShipRedundantPayloads =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.redundant_payloads", unit: "{entry}",
            description: "Shipped Set entries whose value was byte-identical to the last value shipped for the same key, tagged by tree and peer.");

    /// <summary>
    /// Counter of value bytes shipped redundantly - the sum of the
    /// shipped value lengths for the entries counted by
    /// <see cref="ShipRedundantPayloads"/>. Lets operators quantify the
    /// bandwidth a sender-manifest / receiver-pull-missing round trip
    /// could reclaim, not just the entry count. Same firing conditions,
    /// tags (<see cref="TagTree"/> and <see cref="TagPeer"/>), and
    /// observability-only contract as
    /// <see cref="ShipRedundantPayloads"/>.
    /// </summary>
    public static readonly Counter<long> ShipRedundantPayloadBytes =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.redundant_payload_bytes", unit: "By",
            description: "Value bytes shipped redundantly (re-set of byte-identical content for a key), tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="ShipRedundantPayloads"/> counter.
    /// </summary>
    public const string ShipRedundantPayloadsName = "orleans.lattice.replication.ship.redundant_payloads";

    /// <summary>
    /// Canonical name of the <see cref="ShipRedundantPayloadBytes"/> counter.
    /// </summary>
    public const string ShipRedundantPayloadBytesName = "orleans.lattice.replication.ship.redundant_payload_bytes";

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
    /// Reports the number of consecutive contact attempts that have
    /// failed since the last success in each direction. Resets to zero
    /// on the first success in that direction.
    /// <para>
    /// <b>Bidirectional.</b> Every measurement is tagged with
    /// <see cref="TagDirection"/> = <see cref="DirectionOutbound"/>
    /// (sender-side ship attempts) or
    /// <see cref="TagDirection"/> = <see cref="DirectionInbound"/>
    /// (receiver-side per-origin apply attempts). Dashboards that
    /// previously matched the gauge without filtering by direction
    /// must add <c>direction="outbound"</c> to preserve the
    /// pre-bidirectional shape.
    /// </para>
    /// </summary>
    public const string ConsecutiveErrorsName = "orleans.lattice.replication.peer.consecutive_errors";

    /// <summary>
    /// Canonical name of the <c>last_contact_seconds</c> observable gauge.
    /// Reports the wall-clock seconds elapsed since the most recent
    /// successful contact with the named peer in each direction.
    /// <c>NaN</c> indicates the peer has never been contacted in that
    /// direction.
    /// <para>
    /// <b>Bidirectional.</b> Every measurement is tagged with
    /// <see cref="TagDirection"/> = <see cref="DirectionOutbound"/>
    /// (recorded by the local sender after a peer accepts a shipped
    /// batch - including the periodic empty liveness probe so the
    /// outbound gauge no longer climbs unbounded between local-write
    /// bursts on a healthy idle link) or
    /// <see cref="TagDirection"/> = <see cref="DirectionInbound"/>
    /// (recorded by the local receiver after a per-origin run of
    /// inbound entries applies successfully). A host that opts into
    /// both directions sees two series per <c>(tree, peer)</c> pair;
    /// dashboards that previously matched the gauge without filtering
    /// by direction must add <c>direction="outbound"</c> to the matcher
    /// to preserve the pre-bidirectional shape, or accept the doubled
    /// series.
    /// </para>
    /// </summary>
    public const string LastContactSecondsName = "orleans.lattice.replication.peer.last_contact_seconds";

    /// <summary>
    /// Canonical name of the <c>ship_in_flight</c> observable gauge.
    /// Reports the number of outbound replication batches the local
    /// sender currently has shipped-but-unacknowledged to the named
    /// peer - the live depth of the sender-side pipelining window
    /// bounded by <see cref="LatticeReplicationOptions.ShipMaxInFlight"/>.
    /// <para>
    /// Outbound-only (the receiver does not pipeline into itself) and
    /// emitted as a single series per <c>(tree, peer)</c> pair without
    /// the direction tag, matching <see cref="EntriesBehindName"/> and
    /// <see cref="BytesBehindName"/>. A value at or near
    /// <see cref="LatticeReplicationOptions.ShipMaxInFlight"/> signals
    /// the sender is keeping the pipeline saturated; a value pinned at
    /// <c>0</c> on a backlogged peer signals the window collapsed under
    /// receiver flow-control back-pressure.
    /// </para>
    /// </summary>
    public const string ShipInFlightName = "orleans.lattice.replication.peer.ship_in_flight";

    /// <summary>
    /// Canonical name of the <c>wire_version.negotiated</c> observable
    /// gauge. Reports the framing wire-format version the local sender
    /// has negotiated as the target for each peer
    /// (<c>min(localCurrent, peerAdvertised)</c>, or the conservative
    /// unknown-peer floor until the peer advertises a capability).
    /// Backed by <see cref="WireVersionNegotiationState"/> and tagged
    /// with <see cref="TagTree"/> and <see cref="TagPeer"/>.
    /// </summary>
    public const string WireVersionNegotiatedName = "orleans.lattice.replication.wire_version.negotiated";

    /// <summary>
    /// Canonical name of the <c>wire_version.downgrade_active</c>
    /// observable gauge. Reports <c>1</c> when the negotiated target
    /// version is strictly below the sender's current wire version and
    /// <c>0</c> otherwise, so operators can see at a glance when a fleet
    /// is running mixed wire versions during a rolling upgrade (a future
    /// re-encode seam would down-encode while this reads <c>1</c>).
    /// Backed by <see cref="WireVersionNegotiationState"/> and tagged
    /// with <see cref="TagTree"/> and <see cref="TagPeer"/>.
    /// </summary>
    public const string WireVersionDowngradeActiveName = "orleans.lattice.replication.wire_version.downgrade_active";

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
    /// this histogram - only successful waits are observed.
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
    /// - never on park - so the underlying invariant tracks "what has
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

    // --- Parallel receiver apply ------------------------------------------------

    /// <summary>
    /// Histogram of the effective degree of parallelism the receiver-side
    /// batch-apply path used for a single inbound batch - the number of
    /// independent <c>(treeId, originClusterId)</c> run-groups applied
    /// concurrently. Recorded once per multi-entry batch. A value of
    /// <c>1</c> denotes fully-sequential apply (the default posture, or a
    /// single-tree batch where cross-tree parallelism does not apply); a
    /// value greater than <c>1</c> reports the achieved concurrency, which
    /// is the host's configured
    /// <see cref="LatticeReplicationOptions.ApplyMaxParallelRuns"/> clamped
    /// to the number of distinct trees in the batch. Operators use the
    /// distribution to confirm parallel apply is actually engaging under
    /// multi-tree load and to correlate it with <see cref="ApplyLag"/>.
    /// Untagged - the measurement describes the batch as a whole, which
    /// may span multiple trees.
    /// </summary>
    public static readonly Histogram<int> ApplyParallelRuns =
        Meter.CreateHistogram<int>("orleans.lattice.replication.apply.parallel_runs", unit: "{run}",
            description: "Effective number of independent run-groups applied concurrently per inbound batch.");

    /// <summary>
    /// Canonical name of the <see cref="ApplyParallelRuns"/> histogram.
    /// </summary>
    public const string ApplyParallelRunsName = "orleans.lattice.replication.apply.parallel_runs";

    // --- Auto-bootstrap detector ------------------------------------------------

    /// <summary>
    /// Counter incremented once per call to
    /// <see cref="ILatticeFallOffLogDetector.CheckAndTriggerAsync"/>
    /// that detected a fall-off-the-log condition for a
    /// <c>(treeName, sourceClusterId)</c> pair - i.e. the receiver's
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

    /// <summary>
    /// Counter incremented once per call to
    /// <see cref="ILatticeFallOffLogDetector.CheckAndTriggerAsync"/>
    /// that detected a fall-off-the-log condition but was absorbed by
    /// the receiver-side bootstrap coordinator because a bootstrap
    /// for the same <c>(treeName, sourceClusterId)</c> was already in
    /// flight (one of
    /// <see cref="LatticeBootstrapState.RequestingSnapshot"/>,
    /// <see cref="LatticeBootstrapState.ApplyingSnapshot"/>, or
    /// <see cref="LatticeBootstrapState.IncrementalHandoff"/>). The
    /// detector does **not** increment <see cref="PeerFellOffLog"/>
    /// in that case so a long-running drain does not produce a
    /// detection event per probe; instead it increments this counter
    /// so operators can still observe the suppressed probes and tell
    /// them apart from "no detection". Tagged by <see cref="TagTree"/>
    /// and <see cref="TagOrigin"/>.
    /// </summary>
    public static readonly Counter<long> PeerFellOffLogSuppressed =
        Meter.CreateCounter<long>("orleans.lattice.replication.peer.fell_off_log_suppressed", unit: "{event}",
            description: "Receiver fall-off-the-log probes suppressed because the bootstrap coordinator is already draining from the same origin, tagged by tree and origin.");

    /// <summary>
    /// Canonical name of the <see cref="PeerFellOffLogSuppressed"/> counter.
    /// </summary>
    public const string PeerFellOffLogSuppressedName = "orleans.lattice.replication.peer.fell_off_log_suppressed";

    // --- Bootstrap progress instruments ----------------------------------------
    //
    // Receiver-side bootstrap (cross-cluster snapshot drain) progress
    // telemetry. The coordinator increments these counters per applied
    // entry and records the histogram once per terminal phase
    // transition. All three instruments are tagged with the local
    // tree name and the remote source cluster id so a single bootstrap
    // run is uniquely addressable across trees and origins.

    /// <summary>
    /// Counter incremented once per snapshot entry successfully applied
    /// by the bootstrap coordinator, post-decorator chain. Lets
    /// operators watch real-time drain progress and compute an entries
    /// per second rate independent of the histogram's terminal recording.
    /// Tagged by <see cref="TagTree"/> and <see cref="TagOrigin"/>.
    /// </summary>
    public static readonly Counter<long> BootstrapEntriesReceived =
        Meter.CreateCounter<long>("orleans.lattice.replication.bootstrap.entries_received", unit: "{entry}",
            description: "Snapshot entries applied by the bootstrap coordinator, tagged by tree and origin.");

    /// <summary>
    /// Canonical name of the <see cref="BootstrapEntriesReceived"/> counter.
    /// </summary>
    public const string BootstrapEntriesReceivedName = "orleans.lattice.replication.bootstrap.entries_received";

    /// <summary>
    /// Counter incremented by the byte length of the applied entry's
    /// value (<c>entry.Value?.Length ?? 0</c>) per snapshot entry
    /// successfully applied by the bootstrap coordinator. Lets
    /// operators watch real-time payload throughput during a drain.
    /// Tagged by <see cref="TagTree"/> and <see cref="TagOrigin"/>.
    /// </summary>
    public static readonly Counter<long> BootstrapBytesReceived =
        Meter.CreateCounter<long>("orleans.lattice.replication.bootstrap.bytes_received", unit: "By",
            description: "Bytes applied by the bootstrap coordinator, tagged by tree and origin.");

    /// <summary>
    /// Canonical name of the <see cref="BootstrapBytesReceived"/> counter.
    /// </summary>
    public const string BootstrapBytesReceivedName = "orleans.lattice.replication.bootstrap.bytes_received";

    /// <summary>
    /// Histogram recorded once per terminal bootstrap phase transition,
    /// reporting the wall-clock duration from the
    /// <see cref="LatticeBootstrapState.RequestingSnapshot"/> persist
    /// to the terminal transition. Tagged by <see cref="TagTree"/>,
    /// <see cref="TagOrigin"/>, and <see cref="TagOutcome"/>; the
    /// outcome value is one of <see cref="BootstrapOutcomeLive"/>,
    /// <see cref="BootstrapOutcomeFailed"/>, or
    /// <see cref="BootstrapOutcomeTimedOut"/>.
    /// </summary>
    /// <remarks>
    /// The duration timer is anchored on a per-activation in-memory
    /// stopwatch, not persistent state, so a silo failover between
    /// kickoff and completion truncates the measured interval to the
    /// span since the most recent reactivation. This is the right
    /// shape for "how long did the active drain take" but operators
    /// monitoring cross-failover durations should pair the histogram
    /// with the per-entry counters.
    /// </remarks>
    public static readonly Histogram<double> BootstrapDuration =
        Meter.CreateHistogram<double>("orleans.lattice.replication.bootstrap.duration", unit: "ms",
            description: "Bootstrap drain duration from RequestingSnapshot to the terminal phase, tagged by tree, origin, and outcome.");

    /// <summary>
    /// Canonical name of the <see cref="BootstrapDuration"/> histogram.
    /// </summary>
    public const string BootstrapDurationName = "orleans.lattice.replication.bootstrap.duration";

    /// <summary>
    /// <see cref="TagOutcome"/> value used when the bootstrap reached
    /// <see cref="LatticeBootstrapState.LiveIncremental"/> and the
    /// snapshot/incremental handoff completed cleanly.
    /// </summary>
    public const string BootstrapOutcomeLive = "live";

    /// <summary>
    /// <see cref="TagOutcome"/> value used when the bootstrap aborted
    /// to <see cref="LatticeBootstrapState.Failed"/> via the
    /// coordinator's catch-and-persist path.
    /// </summary>
    public const string BootstrapOutcomeFailed = "failed";

    /// <summary>
    /// <see cref="TagOutcome"/> value reserved for the timeout path
    /// (the receiver gave up waiting on the snapshot transport).
    /// Not emitted by the in-tree coordinator today; reserved for a
    /// future transport-timeout policy so consumers can dashboard
    /// against the constant without a metrics schema churn when the
    /// timeout path lands.
    /// </summary>
    public const string BootstrapOutcomeTimedOut = "timed_out";

    /// <summary>
    /// Counter incremented every time the receiver-side bootstrap
    /// coordinator classifies an exception thrown by its snapshot
    /// drain as transient and consumes one slot of the configured
    /// bounded retry budget
    /// (<see cref="LatticeReplicationOptions.BootstrapTransientRetry"/>).
    /// Tagged by <see cref="TagTree"/> and <see cref="TagOrigin"/>;
    /// a sustained non-zero rate signals either a flaky cross-cluster
    /// transport or an over-aggressive classifier. The counter only
    /// fires on retried attempts - the bootstrap reaching
    /// <see cref="LatticeBootstrapState.Failed"/> after exhausting
    /// the budget surfaces through the
    /// <see cref="BootstrapDuration"/> histogram's
    /// <see cref="BootstrapOutcomeFailed"/> tag instead.
    /// </summary>
    public static readonly Counter<long> BootstrapTransientRetries =
        Meter.CreateCounter<long>("orleans.lattice.replication.bootstrap.transient_retries", unit: "{retry}",
            description: "Number of transient-fault retries consumed by the receiver-side bootstrap drain, tagged by tree and origin.");

    /// <summary>
    /// Canonical name of the <see cref="BootstrapTransientRetries"/> counter.
    /// </summary>
    public const string BootstrapTransientRetriesName = "orleans.lattice.replication.bootstrap.transient_retries";

    // --- Anti-entropy peer digest probe (detect stage) --------------------------

    /// <summary>
    /// Counter incremented once per per-shard digest comparison whose
    /// versions agreed but whose digest hashes differed - i.e. the local
    /// cluster and the named peer have diverged for that
    /// <c>(tree, shard)</c>. Tagged by <see cref="TagTree"/>,
    /// <see cref="TagShard"/>, and <see cref="TagPeer"/>. A version-skew
    /// comparison does <b>not</b> contribute (the hashes are not
    /// comparable across contribution-function versions). This counter is
    /// the headline drift-detection signal for the anti-entropy chain;
    /// every increment is also reflected as a
    /// <see cref="DigestProbeOutcomeMismatch"/>-tagged sample on
    /// <see cref="DigestProbeCompared"/>.
    /// </summary>
    public static readonly Counter<long> DigestProbeMismatch =
        Meter.CreateCounter<long>("orleans.lattice.replication.digest_probe.mismatch", unit: "{comparison}",
            description: "Per-shard digest comparisons whose versions matched but hashes differed, tagged by tree, shard, and peer.");

    /// <summary>
    /// Canonical name of the <see cref="DigestProbeMismatch"/> counter.
    /// </summary>
    public const string DigestProbeMismatchName = "orleans.lattice.replication.digest_probe.mismatch";

    /// <summary>
    /// Counter incremented once per per-shard digest comparison performed
    /// by the anti-entropy digest-probe scheduler, regardless of result.
    /// Tagged by <see cref="TagTree"/>, <see cref="TagShard"/>,
    /// <see cref="TagPeer"/>, and <see cref="TagOutcome"/>; the outcome
    /// value is one of <see cref="DigestProbeOutcomeMatch"/>,
    /// <see cref="DigestProbeOutcomeMismatch"/>,
    /// <see cref="DigestProbeOutcomeVersionSkew"/>, or
    /// <see cref="DigestProbeOutcomeRemoteUnavailable"/>. Pairs with
    /// <see cref="DigestProbeMismatch"/> so operators can compute the
    /// divergence ratio (mismatches divided by comparisons) per peer.
    /// </summary>
    public static readonly Counter<long> DigestProbeCompared =
        Meter.CreateCounter<long>("orleans.lattice.replication.digest_probe.compared", unit: "{comparison}",
            description: "Per-shard digest comparisons performed by the anti-entropy probe scheduler, tagged by tree, shard, peer, and outcome.");

    /// <summary>
    /// Canonical name of the <see cref="DigestProbeCompared"/> counter.
    /// </summary>
    public const string DigestProbeComparedName = "orleans.lattice.replication.digest_probe.compared";

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="DigestProbeCompared"/>:
    /// versions matched and the digest hashes were byte-identical.
    /// Corresponds to <see cref="DigestProbeOutcome.Match"/>.
    /// </summary>
    public const string DigestProbeOutcomeMatch = "match";

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="DigestProbeCompared"/>:
    /// versions matched but the digest hashes differed. Corresponds to
    /// <see cref="DigestProbeOutcome.Mismatch"/> and is the only outcome
    /// that also increments <see cref="DigestProbeMismatch"/>.
    /// </summary>
    public const string DigestProbeOutcomeMismatch = "mismatch";

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="DigestProbeCompared"/>:
    /// the local and remote digests carry different contribution-function
    /// versions, so their hashes are not comparable. Corresponds to
    /// <see cref="DigestProbeOutcome.VersionSkew"/>; never raises a
    /// mismatch.
    /// </summary>
    public const string DigestProbeOutcomeVersionSkew = "version_skew";

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="DigestProbeCompared"/>:
    /// the remote peer could not produce a digest (projection-digest
    /// maintenance disabled or latched off remotely). Corresponds to
    /// <see cref="DigestProbeOutcome.RemoteUnavailable"/>; never raises a
    /// mismatch.
    /// </summary>
    public const string DigestProbeOutcomeRemoteUnavailable = "remote_unavailable";

    /// <summary>
    /// Maps a <see cref="DigestProbeOutcome"/> to its canonical
    /// <see cref="TagOutcome"/> string value for
    /// <see cref="DigestProbeCompared"/>.
    /// </summary>
    /// <param name="outcome">The comparison outcome.</param>
    /// <returns>The matching outcome-tag string constant.</returns>
    public static string DigestProbeOutcomeTag(DigestProbeOutcome outcome) => outcome switch
    {
        DigestProbeOutcome.Match => DigestProbeOutcomeMatch,
        DigestProbeOutcome.Mismatch => DigestProbeOutcomeMismatch,
        DigestProbeOutcome.VersionSkew => DigestProbeOutcomeVersionSkew,
        DigestProbeOutcome.RemoteUnavailable => DigestProbeOutcomeRemoteUnavailable,
        _ => DigestProbeOutcomeRemoteUnavailable,
    };

    // --- Sender-side adaptive batch sizing --------------------------------------

    /// <summary>
    /// Histogram of the effective outbound batch-size cap the
    /// per-<c>(tree, peer)</c> shipper used for a single ship attempt -
    /// the entry cap the sender actually applied after composing the
    /// configured <see cref="LatticeReplicationOptions.ShipBatchSize"/>
    /// ceiling, any active receiver flow-control hint
    /// (<see cref="ReplicationAck.SuggestedBatchSize"/>), and the
    /// sender-side adaptive batch-size controller's current size when
    /// <see cref="LatticeReplicationOptions.AdaptiveBatchSizingEnabled"/>
    /// is on. Recorded once per acknowledged batch (not per liveness
    /// probe). With adaptive sizing off the distribution collapses onto
    /// the static cap (<see cref="LatticeReplicationOptions.ShipBatchSize"/>
    /// modulated only by the receiver hint); with it on the distribution
    /// tracks the AIMD controller's output. Tagged by <see cref="TagTree"/>
    /// and <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Histogram<int> ShipEffectiveBatchSize =
        Meter.CreateHistogram<int>("orleans.lattice.replication.ship.effective_batch_size", unit: "{entry}",
            description: "Effective per-tick outbound batch-size cap the sender applied, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="ShipEffectiveBatchSize"/> histogram.
    /// </summary>
    public const string ShipEffectiveBatchSizeName = "orleans.lattice.replication.ship.effective_batch_size";

    /// <summary>
    /// Histogram of the measured outbound ack latency - the wall-clock
    /// interval, in milliseconds, between the sender launching a batch's
    /// <see cref="IReplicationTransport.SendAsync"/> and that batch's ack
    /// returning. Recorded once per acknowledged batch (not per liveness
    /// probe), measured with <see cref="System.Diagnostics.Stopwatch.GetElapsedTime(long)"/>
    /// so it is allocation-free and monotonic. On the bounded-pipelining
    /// path the interval includes the time the batch spent queued behind
    /// lower-HLC batches in the FIFO window, so it reflects the effective
    /// per-batch round-trip the sender observes. This is the signal the
    /// sender-side adaptive batch-size controller acts on when
    /// <see cref="LatticeReplicationOptions.AdaptiveBatchSizingEnabled"/>
    /// is on, and is emitted regardless of that flag. Tagged by
    /// <see cref="TagTree"/> and <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Histogram<double> ShipAckLatency =
        Meter.CreateHistogram<double>("orleans.lattice.replication.ship.ack_latency", unit: "ms",
            description: "Measured outbound ack latency per acknowledged batch, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="ShipAckLatency"/> histogram.
    /// </summary>
    public const string ShipAckLatencyName = "orleans.lattice.replication.ship.ack_latency";
}
