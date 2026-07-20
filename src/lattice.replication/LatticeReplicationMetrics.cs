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
    /// <see cref="TagOutcome"/> value: the inbound entry was rejected by the
    /// receiver-side enrollment gate because its
    /// <see cref="WalRecord.TreeId"/> is not enrolled for replication on this
    /// receiver (the local per-tree resolver returns no merge mode for it).
    /// The entry is dropped without applying and without dead-lettering - a
    /// non-enrolled tree id is peer-controlled, so parking it would let a peer
    /// spawn unbounded dead-letter-queue activations. Guards against a peer
    /// holding the mesh secret writing a deliberately cluster-local tree (for
    /// example a <c>sys-auth-*</c> / <c>sys-membership-*</c> authorization or
    /// identity tree) that this cluster kept out of its replicated set.
    /// </summary>
    public const string OutcomeRejectedNotReplicated = "rejected-not-replicated";

    /// <summary>
    /// <see cref="TagOutcome"/> value: the inbound entry was rejected by the
    /// receiver-side merge-mode gate because its peer-supplied
    /// <see cref="WalRecord.Mode"/> disagrees with the merge mode the receiver
    /// resolves locally for the entry's <see cref="WalRecord.TreeId"/>. The
    /// entry is not applied; because the tree is enrolled (and therefore
    /// bounded) the entry is dead-lettered with
    /// <see cref="ReasonModeMismatch"/> for operator visibility rather than
    /// silently dropped. Guards against a peer overriding the algebra the
    /// receiver applies to a tree by supplying a different wire mode.
    /// </summary>
    public const string OutcomeRejectedModeMismatch = "rejected-mode-mismatch";

    /// <summary>
    /// Tag key for the dead-letter enqueue / removal reason. Values are
    /// drawn from <see cref="ReasonDiscarded"/>, <see cref="ReasonReplayed"/>,
    /// <see cref="ReasonEvicted"/>, <see cref="ReasonSchema"/>,
    /// <see cref="ReasonHlcSkew"/>, <see cref="ReasonOversized"/>,
    ///
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
    /// Reason tag value: enqueue cause was an inbound entry whose
    /// peer-supplied <see cref="WalRecord.Mode"/> disagreed with the merge
    /// mode the receiver resolves locally for the entry's
    /// <see cref="WalRecord.TreeId"/>. Raised by the receiver-side merge-mode
    /// gate in <see cref="ReplicationApplier"/> when an enrolled tree is
    /// shipped entries whose wire mode does not match the locally configured
    /// algebra, so the receiver never trusts the wire mode field.
    /// </summary>
    public const string ReasonModeMismatch = "mode_mismatch";

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

    // --- Throughput counters (ship rate) ----------------------

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

    /// <summary>
    /// Counter of per-batch wire-version down-stamp outcomes recorded while
    /// the shipper negotiates the framing wire version for an older peer
    /// during a rolling upgrade. Incremented once per negotiation that has a
    /// down-stamp outcome worth surfacing, tagged by <see cref="TagTree"/>,
    /// <see cref="TagPeer"/>, and <see cref="TagReason"/>. The reason values
    /// are <see cref="DownStampReasonCompressionDropped"/> (the tree's framing
    /// compression was dropped for this peer so a compressed last-writer-wins
    /// tree keeps replicating uncompressed rather than stalling),
    /// <see cref="DownStampReasonBlockedCrdtMode"/> (a CRDT-mode tree cannot be
    /// faithfully down-encoded for a pre-current-version receiver, so
    /// replication to the peer is paused until it is upgraded), and
    /// <see cref="DownStampReasonBlockedUnsupportedVersion"/> (the negotiated
    /// target is below the down-encode floor, so the frame cannot be made
    /// decodable for the peer). The blocked reasons make a paused stream an
    /// observable, operator-actionable signal rather than a silent stall.
    /// </summary>
    public static readonly Counter<long> ShipWireVersionDownStamp =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.wire_version_down_stamp", unit: "{batch}",
            description: "Per-batch wire-version down-stamp outcomes during negotiation, tagged by tree, peer and reason (compression_dropped / blocked_crdt_mode / blocked_unsupported_version).");

    /// <summary>
    /// Canonical name of the <see cref="ShipWireVersionDownStamp"/> counter.
    /// </summary>
    public const string ShipWireVersionDownStampName = "orleans.lattice.replication.ship.wire_version_down_stamp";

    /// <summary>
    /// <see cref="TagReason"/> value for <see cref="ShipWireVersionDownStamp"/>:
    /// the negotiated down-stamp target could not carry the tree's configured
    /// framing compression, so the shipper dropped compression for this peer's
    /// batch and shipped it uncompressed. Lossless - compression rides the
    /// framing tail only - so a compressed last-writer-wins tree keeps
    /// replicating to an older peer instead of stalling.
    /// </summary>
    public const string DownStampReasonCompressionDropped = "compression_dropped";

    /// <summary>
    /// <see cref="TagReason"/> value for <see cref="ShipWireVersionDownStamp"/>:
    /// the tree is in a CRDT merge mode whose per-entry merge dispatch depends
    /// on the hoisted framing-header mode a pre-current-version receiver cannot
    /// read, so it cannot be faithfully down-encoded. Replication to this peer
    /// is paused until the peer is upgraded.
    /// </summary>
    public const string DownStampReasonBlockedCrdtMode = "blocked_crdt_mode";

    /// <summary>
    /// <see cref="TagReason"/> value for <see cref="ShipWireVersionDownStamp"/>:
    /// the negotiated target wire version is below the oldest version this
    /// build can down-stamp to, so the frame cannot be made decodable for the
    /// peer. Replication to this peer is paused until the peer is upgraded.
    /// </summary>
    public const string DownStampReasonBlockedUnsupportedVersion = "blocked_unsupported_version";

    // --- Shared-dictionary compression counters --------------------------------

    /// <summary>
    /// Counter of uncompressed tail bytes fed into the shared-dictionary
    /// Zstandard compressor on the framing encode path - the "before"
    /// half of the dictionary before/after ratio. Incremented by the
    /// uncompressed tail length each time a batch is framed with the
    /// <see cref="LatticeCompression.ZstdDictionary"/> tag (i.e. only
    /// when shared-dictionary compression is opted into and a dictionary
    /// is resolvable on this silo; the default-off build never fires
    /// it). Pairs with <see cref="CompressDictionaryBytesOut"/> so an
    /// operator can compute the achieved compression ratio directly.
    /// Tagged by <see cref="TagTree"/>.
    /// </summary>
    public static readonly Counter<long> CompressDictionaryBytesIn =
        Meter.CreateCounter<long>("orleans.lattice.replication.compress.dictionary.bytes_in", unit: "By",
            description: "Uncompressed tail bytes fed into the shared-dictionary Zstandard compressor, tagged by tree.");

    /// <summary>
    /// Counter of compressed tail bytes emitted by the shared-dictionary
    /// Zstandard compressor on the framing encode path - the "after"
    /// half of the dictionary before/after ratio. Incremented by the
    /// compressed tail length each time a batch is framed with the
    /// <see cref="LatticeCompression.ZstdDictionary"/> tag. Pairs with
    /// <see cref="CompressDictionaryBytesIn"/>: the ratio of out to in
    /// quantifies the shared-dictionary win against the uncompressed
    /// baseline. Tagged by <see cref="TagTree"/>.
    /// </summary>
    public static readonly Counter<long> CompressDictionaryBytesOut =
        Meter.CreateCounter<long>("orleans.lattice.replication.compress.dictionary.bytes_out", unit: "By",
            description: "Compressed tail bytes emitted by the shared-dictionary Zstandard compressor, tagged by tree.");

    /// <summary>
    /// Canonical name of the <see cref="CompressDictionaryBytesIn"/> counter.
    /// </summary>
    public const string CompressDictionaryBytesInName = "orleans.lattice.replication.compress.dictionary.bytes_in";

    /// <summary>
    /// Canonical name of the <see cref="CompressDictionaryBytesOut"/> counter.
    /// </summary>
    public const string CompressDictionaryBytesOutName = "orleans.lattice.replication.compress.dictionary.bytes_out";

    // --- Pre-ship coalescing counters -------------------------------------------

    /// <summary>
    /// Counter of WAL entries elided from an outbound batch by pre-ship
    /// coalescing - the count of redundant per-key versions dropped
    /// before they reach the cross-cluster wire. Incremented once per
    /// elided entry as the shipper compacts a drained batch, only when
    /// <see cref="LatticeReplicationOptions.PreShipCoalescingEnabled"/>
    /// is set (the counter never fires under the default-off behaviour).
    /// <para>
    /// Coalescing collapses a hot key rewritten several times within a
    /// single drained batch down to the single version a last-writer-wins
    /// receiver would have converged to, so the elided entries are the
    /// intermediate versions the receiver would have overwritten anyway.
    /// Pairs with <see cref="WalEntriesShipped"/> so operators can read
    /// the elided fraction directly. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagPeer"/>. Distinct from
    /// <see cref="ShipRedundantPayloads"/>, which is a measurement-only
    /// content-hash signal that never alters the bytes shipped; this
    /// counter records entries that were actually dropped from the wire.
    /// </para>
    /// </summary>
    public static readonly Counter<long> CoalesceEntriesElided =
        Meter.CreateCounter<long>("orleans.lattice.replication.coalesce.entries_elided", unit: "{entry}",
            description: "WAL entries dropped from an outbound batch by pre-ship coalescing, tagged by tree and peer.");

    /// <summary>
    /// Counter of pre-encoded entry-payload bytes elided from an outbound
    /// batch by pre-ship coalescing - the sum of the wire-segment lengths
    /// of the entries counted by <see cref="CoalesceEntriesElided"/>. Lets
    /// operators quantify the cross-cluster bandwidth coalescing reclaimed,
    /// not just the entry count. Same firing conditions and tags
    /// (<see cref="TagTree"/> and <see cref="TagPeer"/>) as
    /// <see cref="CoalesceEntriesElided"/>.
    /// </summary>
    public static readonly Counter<long> CoalesceBytesElided =
        Meter.CreateCounter<long>("orleans.lattice.replication.coalesce.bytes_elided", unit: "By",
            description: "Pre-encoded entry-payload bytes dropped from an outbound batch by pre-ship coalescing, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="CoalesceEntriesElided"/> counter.
    /// </summary>
    public const string CoalesceEntriesElidedName = "orleans.lattice.replication.coalesce.entries_elided";

    /// <summary>
    /// Canonical name of the <see cref="CoalesceBytesElided"/> counter.
    /// </summary>
    public const string CoalesceBytesElidedName = "orleans.lattice.replication.coalesce.bytes_elided";

    // --- Writer-side doorbell coalescing counters -------------------------------

    /// <summary>
    /// Counter of shipper doorbell rings actually dispatched to a
    /// <c>(tree, peer)</c> shipper after writer-side coalescing. A doorbell
    /// is an edge-triggered "there is work" wake, so the commit-time sink
    /// collapses a burst of per-write rings for the same <c>(tree, peer)</c>
    /// into at most one in-flight ring plus one pending follow-up; this
    /// counter fires once per ring that reaches the grain. Read together with
    /// <see cref="DoorbellCoalesced"/> it gives the coalescing ratio -
    /// a burst of thousands of writes should dispatch only a couple of rings.
    /// Tagged by <see cref="TagTree"/> and <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Counter<long> DoorbellRung =
        Meter.CreateCounter<long>("orleans.lattice.replication.doorbell.rung", unit: "{ring}",
            description: "Shipper doorbell rings dispatched to a (tree, peer) shipper after writer-side coalescing, tagged by tree and peer.");

    /// <summary>
    /// Counter of shipper doorbell rings elided by writer-side coalescing -
    /// a per-write ring request that arrived while a ring for the same
    /// <c>(tree, peer)</c> was already in flight, and was collapsed into the
    /// single pending follow-up rather than dispatched as its own grain call.
    /// This is the storm the coalescer absorbs: without it every such request
    /// would enqueue a fresh <c>OnDoorbellAsync</c> message on the
    /// non-reentrant shipper activation. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Counter<long> DoorbellCoalesced =
        Meter.CreateCounter<long>("orleans.lattice.replication.doorbell.coalesced", unit: "{ring}",
            description: "Shipper doorbell rings elided by writer-side coalescing (collapsed into an in-flight or pending ring), tagged by tree and peer.");

    /// <summary>Canonical name of the <see cref="DoorbellRung"/> counter.</summary>
    public const string DoorbellRungName = "orleans.lattice.replication.doorbell.rung";

    /// <summary>Canonical name of the <see cref="DoorbellCoalesced"/> counter.</summary>
    public const string DoorbellCoalescedName = "orleans.lattice.replication.doorbell.coalesced";

    /// <summary>
    /// Counter of source CRDT deltas folded into a combined delta by the
    /// CRDT branch of pre-ship coalescing - the number of same-key typed
    /// delta entries whose payloads were merged into the single combined
    /// delta re-encoded onto the kept entry. Only fires when
    /// <see cref="LatticeReplicationOptions.PreShipCoalescingEnabled"/> is
    /// set and the tree is a recognised CRDT mode (the last-writer-wins
    /// branch only elides entries, it never merges deltas, so it leaves
    /// this counter at zero). Pairs with <see cref="CoalesceEntriesElided"/>
    /// - which still records the source entries dropped from the wire on
    /// the CRDT path - to let operators distinguish CRDT delta-merge
    /// coalescing from last-writer-wins elision. Tagged by
    /// <see cref="TagTree"/> and <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Counter<long> CoalesceDeltasMerged =
        Meter.CreateCounter<long>("orleans.lattice.replication.coalesce.deltas_merged", unit: "{delta}",
            description: "Source CRDT deltas folded into a combined delta by pre-ship coalescing, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="CoalesceDeltasMerged"/> counter.
    /// </summary>
    public const string CoalesceDeltasMergedName = "orleans.lattice.replication.coalesce.deltas_merged";

    // --- Content-hash payload-elision round-trip counters -----------------------

    /// <summary>
    /// Counter of <see cref="MutationKind.Set"/> entries whose value
    /// payload was elided from an outbound batch by the sender-manifest /
    /// receiver-pull-missing content-hash round trip - the receiver already
    /// held byte-identical content for the key, so only metadata (the
    /// high-water-mark advance) was needed and the payload never travelled.
    /// Incremented once per elided entry, only when
    /// <see cref="LatticeReplicationOptions.ContentHashDedupElisionEnabled"/>
    /// is set and the peer advertised it can perform the exchange (the
    /// counter never fires under the default-off behaviour). Tagged by
    /// <see cref="TagTree"/> and <see cref="TagPeer"/>.
    /// <para>
    /// Distinct from <see cref="ShipRedundantPayloads"/>, which is a
    /// measurement-only signal that never changes the bytes shipped, and
    /// from <see cref="CoalesceEntriesElided"/>, which drops intra-batch
    /// duplicate versions without a cross-cluster round trip; this counter
    /// records payloads dropped because the remote peer confirmed it
    /// already holds the content.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShipElidedPayloads =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.elided_payloads", unit: "{entry}",
            description: "Set-entry payloads elided from an outbound batch by the content-hash pull-missing round trip, tagged by tree and peer.");

    /// <summary>
    /// Counter of value-payload bytes elided from an outbound batch by the
    /// content-hash pull-missing round trip - the sum of the wire-segment
    /// lengths of the entries counted by <see cref="ShipElidedPayloads"/>.
    /// Lets operators quantify the cross-cluster bandwidth the elision
    /// reclaimed. Same firing conditions and tags (<see cref="TagTree"/>
    /// and <see cref="TagPeer"/>) as <see cref="ShipElidedPayloads"/>.
    /// </summary>
    public static readonly Counter<long> ShipElidedPayloadBytes =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.elided_payload_bytes", unit: "By",
            description: "Pre-encoded entry-payload bytes elided from an outbound batch by the content-hash pull-missing round trip, tagged by tree and peer.");

    /// <summary>
    /// Counter of content-hash manifest exchanges the sender performed with
    /// a peer - one increment per outbound batch for which a manifest was
    /// advertised and a pull-missing response was received. Incremented
    /// only when
    /// <see cref="LatticeReplicationOptions.ContentHashDedupElisionEnabled"/>
    /// is set and the peer advertised the exchange capability. Pairs with
    /// <see cref="ShipElidedPayloads"/> so an operator can read the average
    /// payloads elided per exchange and judge whether the round trip is
    /// paying for itself. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagPeer"/>.
    /// </summary>
    public static readonly Counter<long> ManifestExchanges =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.manifest_exchanges", unit: "{exchange}",
            description: "Content-hash manifest exchanges performed with a peer on the outbound ship path, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="ShipElidedPayloads"/> counter.
    /// </summary>
    public const string ShipElidedPayloadsName = "orleans.lattice.replication.ship.elided_payloads";

    /// <summary>
    /// Canonical name of the <see cref="ShipElidedPayloadBytes"/> counter.
    /// </summary>
    public const string ShipElidedPayloadBytesName = "orleans.lattice.replication.ship.elided_payload_bytes";

    /// <summary>
    /// Canonical name of the <see cref="ManifestExchanges"/> counter.
    /// </summary>
    public const string ManifestExchangesName = "orleans.lattice.replication.ship.manifest_exchanges";

    // --- Receiver-side content-hash exchange counters ---------------------------

    /// <summary>
    /// Counter of content-hash manifest exchanges the receiver handled - one
    /// increment per inbound pull-missing request the receiver answered for a
    /// peer's advertised manifest. Pairs with the sender-side
    /// <see cref="ManifestExchanges"/> so an operator can confirm both ends
    /// of the round trip agree on the exchange volume. Recorded on the
    /// receiver's content-manifest handler regardless of how many entries the
    /// receiver held, so an exchange that ends up eliding nothing still
    /// counts. Tagged by <see cref="TagTree"/> and <see cref="TagPeer"/> (the
    /// requesting origin cluster id).
    /// </summary>
    public static readonly Counter<long> ReceiverContentManifestExchanges =
        Meter.CreateCounter<long>("orleans.lattice.replication.receiver.content_manifest_exchanges", unit: "{exchange}",
            description: "Content-hash manifest exchanges answered by the receiver, tagged by tree and origin peer.");

    /// <summary>
    /// Counter of manifest entries the receiver reported it already holds
    /// byte-identical content for - the entries the receiver told the sender
    /// it does not need shipped, so the sender elides their payloads.
    /// Incremented by the count of held (non-missing) entries each exchange.
    /// Pairs with the sender-side <see cref="ShipElidedPayloads"/>: the two
    /// counters track the same elided entries observed from each end of the
    /// round trip. Tagged by <see cref="TagTree"/> and <see cref="TagPeer"/>
    /// (the requesting origin cluster id).
    /// </summary>
    public static readonly Counter<long> ReceiverContentEntriesElided =
        Meter.CreateCounter<long>("orleans.lattice.replication.receiver.content_entries_elided", unit: "{entry}",
            description: "Manifest entries the receiver reported it already holds, tagged by tree and origin peer.");

    /// <summary>
    /// Counter of metadata-only high-water-mark advances the receiver
    /// performed during a content-hash exchange - one increment per exchange
    /// that durably advanced the per-origin high-water-mark for an
    /// identical-content entry carrying a newer clock (the idempotent
    /// re-set), without the payload ever travelling. Incremented once per
    /// exchange whose durable advance succeeded, never under the default-off
    /// behaviour (a cold or empty applied-content index reports every entry
    /// as missing and performs no advance). Tagged by <see cref="TagTree"/>
    /// and <see cref="TagPeer"/> (the requesting origin cluster id).
    /// </summary>
    public static readonly Counter<long> ReceiverContentHwmAdvances =
        Meter.CreateCounter<long>("orleans.lattice.replication.receiver.content_hwm_advances", unit: "{advance}",
            description: "Metadata-only high-water-mark advances performed by the receiver during a content-hash exchange, tagged by tree and origin peer.");

    /// <summary>
    /// Canonical name of the <see cref="ReceiverContentManifestExchanges"/> counter.
    /// </summary>
    public const string ReceiverContentManifestExchangesName = "orleans.lattice.replication.receiver.content_manifest_exchanges";

    /// <summary>
    /// Canonical name of the <see cref="ReceiverContentEntriesElided"/> counter.
    /// </summary>
    public const string ReceiverContentEntriesElidedName = "orleans.lattice.replication.receiver.content_entries_elided";

    /// <summary>
    /// Canonical name of the <see cref="ReceiverContentHwmAdvances"/> counter.
    /// </summary>
    public const string ReceiverContentHwmAdvancesName = "orleans.lattice.replication.receiver.content_hwm_advances";

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
    // --- Anti-entropy Merkle-walk drift localisation (localise stage) -----------

    /// <summary>
    /// Tag key for the depth a successful Merkle-walk localisation reached
    /// within the shard's internal-node tree - <c>0</c> for the shard root
    /// itself, incrementing by one per level descended. Carried by
    /// <see cref="MerkleWalkLocalised"/>.
    /// </summary>
    public const string TagDepth = "depth";

    /// <summary>
    /// Counter incremented when the read-only Merkle-walk drift-localisation
    /// pass narrows a shard-level digest mismatch down to a single leaf or a
    /// small set of leaves. The increment amount is the number of diverging
    /// leaves localised. Tagged by <see cref="TagTree"/> and
    /// <see cref="TagDepth"/> (the depth in the internal-node tree at which the
    /// localisation completed). The walk is strictly read-only and ships dark
    /// behind <see cref="LatticeReplicationOptions.MerkleWalkEnabled"/>; it runs
    /// only after the digest probe reports a
    /// <see cref="DigestProbeOutcome.Mismatch"/>.
    /// </summary>
    public static readonly Counter<long> MerkleWalkLocalised =
        Meter.CreateCounter<long>("orleans.lattice.replication.merkle_walk.localised", unit: "{leaf}",
            description: "Diverging leaves localised by the read-only anti-entropy Merkle walk, tagged by tree and depth.");

    /// <summary>
    /// Canonical name of the <see cref="MerkleWalkLocalised"/> counter.
    /// </summary>
    public const string MerkleWalkLocalisedName = "orleans.lattice.replication.merkle_walk.localised";

    /// <summary>
    /// Counter incremented once per Merkle-walk pass that aborts before
    /// localising - because the recursion-depth cap or the byte budget was
    /// exhausted, a remote peer could not answer a range probe, or a
    /// contribution-function version skew made the hashes incomparable. Tagged
    /// by <see cref="TagReason"/>; the reason value is one of
    /// <see cref="MerkleWalkAbortDepthCap"/>, <see cref="MerkleWalkAbortByteBudget"/>,
    /// <see cref="MerkleWalkAbortRemoteUnavailable"/>, or
    /// <see cref="MerkleWalkAbortVersionSkew"/>.
    /// </summary>
    public static readonly Counter<long> MerkleWalkAborted =
        Meter.CreateCounter<long>("orleans.lattice.replication.merkle_walk.aborted", unit: "{walk}",
            description: "Read-only anti-entropy Merkle walks that aborted before localising, tagged by reason.");

    /// <summary>
    /// Canonical name of the <see cref="MerkleWalkAborted"/> counter.
    /// </summary>
    public const string MerkleWalkAbortedName = "orleans.lattice.replication.merkle_walk.aborted";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="MerkleWalkAborted"/>: the
    /// configured recursion-depth cap
    /// (<see cref="LatticeReplicationOptions.MerkleWalkMaxDepth"/>) was reached
    /// before the walk localised a leaf. Corresponds to
    /// <see cref="MerkleWalkAbortReason.DepthCapExceeded"/>.
    /// </summary>
    public const string MerkleWalkAbortDepthCap = "depth_cap";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="MerkleWalkAborted"/>: the
    /// per-probe byte budget
    /// (<see cref="LatticeReplicationOptions.MerkleWalkMaxBytes"/>) was exhausted
    /// before the walk localised a leaf. Corresponds to
    /// <see cref="MerkleWalkAbortReason.ByteBudgetExceeded"/>.
    /// </summary>
    public const string MerkleWalkAbortByteBudget = "byte_budget";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="MerkleWalkAborted"/>: a
    /// remote peer could not answer a key-range subtree-digest probe, so the
    /// walk could not compare apples-to-apples and stopped. Corresponds to
    /// <see cref="MerkleWalkAbortReason.RemoteUnavailable"/>.
    /// </summary>
    public const string MerkleWalkAbortRemoteUnavailable = "remote_unavailable";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="MerkleWalkAborted"/>: the
    /// local and remote digests carry different contribution-function versions,
    /// so their hashes are not comparable and the walk stopped. Corresponds to
    /// <see cref="MerkleWalkAbortReason.VersionSkew"/>.
    /// </summary>
    public const string MerkleWalkAbortVersionSkew = "version_skew";

    /// <summary>
    /// Maps a <see cref="MerkleWalkAbortReason"/> to its canonical
    /// <see cref="TagReason"/> string value for <see cref="MerkleWalkAborted"/>.
    /// </summary>
    /// <param name="reason">The abort reason.</param>
    /// <returns>The matching reason-tag string constant.</returns>
    public static string MerkleWalkAbortReasonTag(MerkleWalkAbortReason reason) => reason switch
    {
        MerkleWalkAbortReason.DepthCapExceeded => MerkleWalkAbortDepthCap,
        MerkleWalkAbortReason.ByteBudgetExceeded => MerkleWalkAbortByteBudget,
        MerkleWalkAbortReason.RemoteUnavailable => MerkleWalkAbortRemoteUnavailable,
        MerkleWalkAbortReason.VersionSkew => MerkleWalkAbortVersionSkew,
        _ => MerkleWalkAbortRemoteUnavailable,
    };

    // --- Anti-entropy targeted leaf re-replay (repair stage) --------------------

    /// <summary>
    /// Counter incremented by the number of write-ahead-log entries the
    /// targeted leaf re-replay repair pass re-ships to a diverged peer. Tagged
    /// by <see cref="TagTree"/> and <see cref="TagPeer"/>. The repair ships dark
    /// behind <see cref="LatticeReplicationOptions.LeafReReplayEnabled"/>; it
    /// runs only after the read-only Merkle walk localises at least one
    /// diverging leaf. Re-shipped entries carry their source clock verbatim and
    /// are deduplicated at the receiver, so the counter measures repair effort,
    /// not net new visible writes.
    /// </summary>
    public static readonly Counter<long> LeafReReplayEntries =
        Meter.CreateCounter<long>("orleans.lattice.replication.leaf_rereplay.entries", unit: "{entry}",
            description: "WAL entries re-shipped to a diverged peer by the targeted leaf re-replay repair pass, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="LeafReReplayEntries"/> counter.
    /// </summary>
    public const string LeafReReplayEntriesName = "orleans.lattice.replication.leaf_rereplay.entries";

    /// <summary>
    /// Counter incremented once per targeted leaf re-replay pass that is
    /// skipped without re-shipping - because the feature is disabled, the
    /// localised range produced no candidate entries, or the local WAL has been
    /// garbage-collected past the divergence point. Tagged by
    /// <see cref="TagTree"/>, <see cref="TagPeer"/>, and <see cref="TagReason"/>;
    /// the reason value is one of <see cref="LeafReReplaySkipDisabled"/>,
    /// <see cref="LeafReReplaySkipRangeEmpty"/>, or
    /// <see cref="LeafReReplaySkipWalTrimmed"/>. A
    /// <see cref="LeafReReplaySkipWalTrimmed"/> skip is the operator-only alert
    /// signal: the repair cannot proceed from the WAL and a bootstrap-snapshot
    /// remediation (tracked as a separate follow-up) is required.
    /// </summary>
    public static readonly Counter<long> LeafReReplaySkipped =
        Meter.CreateCounter<long>("orleans.lattice.replication.leaf_rereplay.skipped", unit: "{skip}",
            description: "Targeted leaf re-replay repair passes skipped without re-shipping, tagged by tree, peer, and reason.");

    /// <summary>
    /// Canonical name of the <see cref="LeafReReplaySkipped"/> counter.
    /// </summary>
    public const string LeafReReplaySkippedName = "orleans.lattice.replication.leaf_rereplay.skipped";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="LeafReReplaySkipped"/>: the
    /// repair stage is disabled
    /// (<see cref="LatticeReplicationOptions.LeafReReplayEnabled"/> is
    /// <see langword="false"/>) even though localisation found a divergent leaf.
    /// Corresponds to <see cref="LeafReReplaySkipReason.Disabled"/>.
    /// </summary>
    public const string LeafReReplaySkipDisabled = "disabled";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="LeafReReplaySkipped"/>: the
    /// localised leaf range yielded no write-ahead-log entries to re-ship (the
    /// localiser produced no ranges, or no retained entry sat in-range above the
    /// peer's cursor). Corresponds to
    /// <see cref="LeafReReplaySkipReason.RangeEmpty"/>.
    /// </summary>
    public const string LeafReReplaySkipRangeEmpty = "range_empty";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="LeafReReplaySkipped"/>: the
    /// local write-ahead-log has been garbage-collected past the divergence
    /// point, so the repair cannot source the missing entries. This is the
    /// operator-only alert signal - the feature does not attempt repair and a
    /// bootstrap-snapshot remediation is the follow-up. Corresponds to
    /// <see cref="LeafReReplaySkipReason.WalTrimmed"/>.
    /// </summary>
    public const string LeafReReplaySkipWalTrimmed = "wal_trimmed";

    /// <summary>
    /// Maps a <see cref="LeafReReplaySkipReason"/> to its canonical
    /// <see cref="TagReason"/> string value for <see cref="LeafReReplaySkipped"/>.
    /// </summary>
    /// <param name="reason">The skip reason.</param>
    /// <returns>The matching reason-tag string constant.</returns>
    public static string LeafReReplaySkipReasonTag(LeafReReplaySkipReason reason) => reason switch
    {
        LeafReReplaySkipReason.Disabled => LeafReReplaySkipDisabled,
        LeafReReplaySkipReason.RangeEmpty => LeafReReplaySkipRangeEmpty,
        LeafReReplaySkipReason.WalTrimmed => LeafReReplaySkipWalTrimmed,
        _ => LeafReReplaySkipRangeEmpty,
    };

    // --- Anti-entropy bootstrap-snapshot fallback (GC'd-divergence repair) ------

    /// <summary>
    /// Counter incremented once per scoped bootstrap-snapshot fallback pass
    /// that begins re-deriving the divergent leaf range from the live tree
    /// after a targeted leaf re-replay reported the local write-ahead-log had
    /// been garbage-collected past the divergence point
    /// (<see cref="LeafReReplaySkipReason.WalTrimmed"/>). Tagged by
    /// <see cref="TagTree"/> and <see cref="TagPeer"/>. The fallback ships dark
    /// behind <see cref="LatticeReplicationOptions.BootstrapFallbackEnabled"/>;
    /// it runs only after the WAL-trimmed signal and only when at least one
    /// leaf range was localised, so its scope is bounded to the drift rather
    /// than the whole tree.
    /// </summary>
    public static readonly Counter<long> BootstrapFallbackTriggered =
        Meter.CreateCounter<long>("orleans.lattice.replication.bootstrap_fallback.triggered", unit: "{fallback}",
            description: "Scoped bootstrap-snapshot fallback passes triggered after a WAL-trimmed leaf re-replay, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="BootstrapFallbackTriggered"/> counter.
    /// </summary>
    public const string BootstrapFallbackTriggeredName = "orleans.lattice.replication.bootstrap_fallback.triggered";

    /// <summary>
    /// Counter incremented by the number of committed-projection snapshot
    /// entries the scoped bootstrap-snapshot fallback re-ships to a diverged
    /// peer (the live committed state of the divergent leaf range), tagged by
    /// <see cref="TagTree"/> and <see cref="TagPeer"/>. Re-shipped entries
    /// carry their source clock verbatim and are deduplicated at the receiver,
    /// so the counter measures repair effort, not net new visible writes.
    /// </summary>
    public static readonly Counter<long> BootstrapFallbackEntries =
        Meter.CreateCounter<long>("orleans.lattice.replication.bootstrap_fallback.entries", unit: "{entry}",
            description: "Committed-projection snapshot entries re-shipped to a diverged peer by the scoped bootstrap-snapshot fallback, tagged by tree and peer.");

    /// <summary>
    /// Canonical name of the <see cref="BootstrapFallbackEntries"/> counter.
    /// </summary>
    public const string BootstrapFallbackEntriesName = "orleans.lattice.replication.bootstrap_fallback.entries";

    /// <summary>
    /// Counter incremented once per scoped bootstrap-snapshot fallback that is
    /// skipped without re-shipping - because the fallback is disabled even
    /// though the WAL-trimmed signal fired, the localised range set was empty,
    /// or the scoped export yielded no committed entries. Tagged by
    /// <see cref="TagTree"/>, <see cref="TagPeer"/>, and <see cref="TagReason"/>;
    /// the reason value is one of <see cref="BootstrapFallbackSkipDisabled"/>,
    /// <see cref="BootstrapFallbackSkipRangeEmpty"/>, or
    /// <see cref="BootstrapFallbackSkipEmpty"/>. A
    /// <see cref="BootstrapFallbackSkipDisabled"/> skip is the operator-only
    /// signal: a divergence that re-replay could not repair from the WAL is
    /// available for the fallback, but the host has not opted in.
    /// </summary>
    public static readonly Counter<long> BootstrapFallbackSkipped =
        Meter.CreateCounter<long>("orleans.lattice.replication.bootstrap_fallback.skipped", unit: "{skip}",
            description: "Scoped bootstrap-snapshot fallback passes skipped without re-shipping, tagged by tree, peer, and reason.");

    /// <summary>
    /// Canonical name of the <see cref="BootstrapFallbackSkipped"/> counter.
    /// </summary>
    public const string BootstrapFallbackSkippedName = "orleans.lattice.replication.bootstrap_fallback.skipped";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="BootstrapFallbackSkipped"/>:
    /// the fallback is disabled
    /// (<see cref="LatticeReplicationOptions.BootstrapFallbackEnabled"/> is
    /// <see langword="false"/>) even though a leaf re-replay reported the WAL
    /// was trimmed past the divergence point. Corresponds to
    /// <see cref="BootstrapFallbackSkipReason.Disabled"/>.
    /// </summary>
    public const string BootstrapFallbackSkipDisabled = "disabled";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="BootstrapFallbackSkipped"/>:
    /// the localiser produced no leaf ranges to scope the snapshot to, so the
    /// fallback had no bounded scope to export. Corresponds to
    /// <see cref="BootstrapFallbackSkipReason.RangeEmpty"/>.
    /// </summary>
    public const string BootstrapFallbackSkipRangeEmpty = "range_empty";

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="BootstrapFallbackSkipped"/>:
    /// the range-scoped snapshot export yielded no committed-projection entries
    /// in the divergent leaf range (the range is empty on the local tree).
    /// Corresponds to <see cref="BootstrapFallbackSkipReason.Empty"/>.
    /// </summary>
    public const string BootstrapFallbackSkipEmpty = "empty";

    /// <summary>
    /// Maps a <see cref="BootstrapFallbackSkipReason"/> to its canonical
    /// <see cref="TagReason"/> string value for
    /// <see cref="BootstrapFallbackSkipped"/>.
    /// </summary>
    /// <param name="reason">The skip reason.</param>
    /// <returns>The matching reason-tag string constant.</returns>
    public static string BootstrapFallbackSkipReasonTag(BootstrapFallbackSkipReason reason) => reason switch
    {
        BootstrapFallbackSkipReason.Disabled => BootstrapFallbackSkipDisabled,
        BootstrapFallbackSkipReason.RangeEmpty => BootstrapFallbackSkipRangeEmpty,
        BootstrapFallbackSkipReason.Empty => BootstrapFallbackSkipEmpty,
        _ => BootstrapFallbackSkipEmpty,
    };

    // --- Anti-entropy remediation guards (opt-in, rate cap, circuit breaker) ---

    /// <summary>
    /// Counter incremented once per automatic anti-entropy remediation pass that
    /// is skipped before any repair traffic is sent, because the host has not
    /// opted in, the per-tree/peer remediation traffic budget is exhausted for
    /// the current window, or the remediation circuit breaker is open. Tagged by
    /// <see cref="TagTree"/>, <see cref="TagPeer"/>, and <see cref="TagReason"/>;
    /// the reason value is one of
    /// <see cref="DigestRemediationReasonOptOut"/>,
    /// <see cref="DigestRemediationReasonBudgetExhausted"/>, or
    /// <see cref="DigestRemediationReasonCircuitOpen"/>. The drift was still
    /// detected and probed; only the repair action was suppressed.
    /// </summary>
    public static readonly Counter<long> DigestRemediationSkipped =
        Meter.CreateCounter<long>("orleans.lattice.replication.digest_remediation.skipped", unit: "{skip}",
            description: "Automatic anti-entropy remediation passes skipped without sending repair traffic, tagged by tree, peer, and reason.");

    /// <summary>
    /// Canonical name of the <see cref="DigestRemediationSkipped"/> counter.
    /// </summary>
    public const string DigestRemediationSkippedName = "orleans.lattice.replication.digest_remediation.skipped";

    /// <summary>
    /// Canonical name of the observable gauge that reports, with value <c>1</c>,
    /// each <c>(tree, peer)</c> for which automatic anti-entropy remediation is
    /// currently disabled. Tagged by <see cref="TagTree"/>,
    /// <see cref="TagPeer"/>, and <see cref="TagReason"/> (one of
    /// <see cref="DigestRemediationReasonOptOut"/>,
    /// <see cref="DigestRemediationReasonBudgetExhausted"/>, or
    /// <see cref="DigestRemediationReasonCircuitOpen"/>). The gauge emits no
    /// series for a <c>(tree, peer)</c> that is not currently disabled, so the
    /// absence of a series means remediation is permitted. The gauge is
    /// registered process-wide and backed by <see cref="RemediationGuard"/>.
    /// </summary>
    public const string DigestRemediationDisabledName = "orleans.lattice.replication.digest_remediation.disabled";

    /// <summary>
    /// <see cref="TagReason"/> value on the
    /// <see cref="DigestRemediationSkippedName"/> counter and the
    /// <see cref="DigestRemediationDisabledName"/> gauge: the host has not opted
    /// into automatic remediation
    /// (<see cref="LatticeReplicationOptions.AutoRemediateOnDigestMismatch"/> is
    /// <see langword="false"/>). Corresponds to
    /// <see cref="RemediationDisabledReason.OptOut"/>.
    /// </summary>
    public const string DigestRemediationReasonOptOut = "opt_out";

    /// <summary>
    /// <see cref="TagReason"/> value on the
    /// <see cref="DigestRemediationSkippedName"/> counter and the
    /// <see cref="DigestRemediationDisabledName"/> gauge: the per-tree/peer
    /// remediation traffic budget for the current window has been spent.
    /// Corresponds to <see cref="RemediationDisabledReason.BudgetExhausted"/>.
    /// </summary>
    public const string DigestRemediationReasonBudgetExhausted = "budget_exhausted";

    /// <summary>
    /// <see cref="TagReason"/> value on the
    /// <see cref="DigestRemediationSkippedName"/> counter and the
    /// <see cref="DigestRemediationDisabledName"/> gauge: the remediation
    /// circuit breaker for the tree/peer is open after
    /// <see cref="LatticeReplicationOptions.RemediationFailureThreshold"/>
    /// consecutive failures. Corresponds to
    /// <see cref="RemediationDisabledReason.CircuitOpen"/>.
    /// </summary>
    public const string DigestRemediationReasonCircuitOpen = "circuit_open";

    /// <summary>
    /// Maps a <see cref="RemediationDisabledReason"/> to its canonical
    /// <see cref="TagReason"/> string value for the
    /// <see cref="DigestRemediationSkippedName"/> counter and the
    /// <see cref="DigestRemediationDisabledName"/> gauge.
    /// </summary>
    /// <param name="reason">The remediation-disabled reason.</param>
    /// <returns>The matching reason-tag string constant.</returns>
    public static string DigestRemediationDisabledReasonTag(RemediationDisabledReason reason) => reason switch
    {
        RemediationDisabledReason.OptOut => DigestRemediationReasonOptOut,
        RemediationDisabledReason.BudgetExhausted => DigestRemediationReasonBudgetExhausted,
        RemediationDisabledReason.CircuitOpen => DigestRemediationReasonCircuitOpen,
        _ => DigestRemediationReasonOptOut,
    };

    // --- Per-peer shared-dictionary capability negotiation (opt-in) ---

    /// <summary>
    /// Tag key for whether a shipped batch used a shared compression
    /// dictionary. Carried by the <see cref="DictionaryBatches"/> counter;
    /// the value is one of <see cref="DictionaryBatchWith"/> or
    /// <see cref="DictionaryBatchWithout"/>.
    /// </summary>
    public const string TagDictionary = "dictionary";

    /// <summary>
    /// Counter incremented once per pump tick on which the shipper computes
    /// a per-peer shared-dictionary capability negotiation against a peer's
    /// advertised dictionary ids. Tagged by <see cref="TagTree"/>,
    /// <see cref="TagPeer"/>, and <see cref="TagOutcome"/> (one of
    /// <see cref="DictionaryNegotiationOutcomeMatched"/>,
    /// <see cref="DictionaryNegotiationOutcomeFellBack"/>, or
    /// <see cref="DictionaryNegotiationOutcomeUnknown"/>). Lets operators
    /// watch how often a configured dictionary is honoured versus falling
    /// back to dictionary-less compression across a mixed fleet. Emitted
    /// only when
    /// <see cref="LatticeReplicationOptions.DictionaryNegotiationEnabled"/>
    /// is set and a shared dictionary is configured.
    /// </summary>
    public static readonly Counter<long> DictionaryNegotiation =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.dictionary_negotiation", unit: "{negotiation}",
            description: "Per-peer shared-dictionary capability negotiations, tagged by tree, peer, and outcome.");

    /// <summary>Canonical name of the <see cref="DictionaryNegotiation"/> counter.</summary>
    public const string DictionaryNegotiationName = "orleans.lattice.replication.ship.dictionary_negotiation";

    /// <summary>
    /// Counter incremented once per shipped batch on the dictionary-eligible
    /// path (configured <see cref="LatticeCompression.ZstdDictionary"/> with
    /// a large-enough tail). Tagged by <see cref="TagTree"/>,
    /// <see cref="TagPeer"/>, and <see cref="TagDictionary"/> (one of
    /// <see cref="DictionaryBatchWith"/> or <see cref="DictionaryBatchWithout"/>),
    /// so a dashboard can show the share of batches compressed with a shared
    /// dictionary versus dictionary-less.
    /// </summary>
    public static readonly Counter<long> DictionaryBatches =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.dictionary_batches", unit: "{batch}",
            description: "Batches shipped on the dictionary-eligible path, tagged by tree, peer, and whether a shared dictionary was used.");

    /// <summary>Canonical name of the <see cref="DictionaryBatches"/> counter.</summary>
    public const string DictionaryBatchesName = "orleans.lattice.replication.ship.dictionary_batches";

    /// <summary>
    /// <see cref="TagOutcome"/> value on the
    /// <see cref="DictionaryNegotiation"/> counter: the peer advertised the
    /// configured dictionary id, so the sender compressed with it.
    /// </summary>
    public const string DictionaryNegotiationOutcomeMatched = "matched";

    /// <summary>
    /// <see cref="TagOutcome"/> value on the
    /// <see cref="DictionaryNegotiation"/> counter: the peer advertised a
    /// capability that did not include the configured dictionary id, so the
    /// sender fell back to dictionary-less compression for this peer.
    /// </summary>
    public const string DictionaryNegotiationOutcomeFellBack = "fell_back";

    /// <summary>
    /// <see cref="TagOutcome"/> value on the
    /// <see cref="DictionaryNegotiation"/> counter: the peer has not
    /// advertised a dictionary capability yet (a build predating dictionary
    /// negotiation, or no ack observed since activation), so the sender fell
    /// back to dictionary-less compression conservatively.
    /// </summary>
    public const string DictionaryNegotiationOutcomeUnknown = "unknown";

    /// <summary>
    /// <see cref="TagOutcome"/> value on the
    /// <see cref="DictionaryNegotiation"/> counter: the peer advertised the
    /// configured dictionary id but with a content fingerprint that differs
    /// from the sender's configured dictionary bytes (a
    /// same-id/different-bytes misconfiguration). The sender fell back to
    /// dictionary-less compression; the distinct outcome makes the
    /// misconfiguration legible instead of letting it surface as a
    /// receiver-side decode failure.
    /// </summary>
    public const string DictionaryNegotiationOutcomeFingerprintMismatch = "fingerprint_mismatch";

    /// <summary>
    /// <see cref="TagDictionary"/> value on the <see cref="DictionaryBatches"/>
    /// counter: the batch was compressed with a shared dictionary.
    /// </summary>
    public const string DictionaryBatchWith = "with_dictionary";

    /// <summary>
    /// <see cref="TagDictionary"/> value on the <see cref="DictionaryBatches"/>
    /// counter: the batch was compressed without a shared dictionary (plain
    /// Zstd, or below the compression threshold).
    /// </summary>
    public const string DictionaryBatchWithout = "without_dictionary";

    /// <summary>
    /// Maps a <see cref="SharedDictionaryNegotiationResult"/> to its
    /// canonical <see cref="TagOutcome"/> string value for the
    /// <see cref="DictionaryNegotiation"/> counter.
    /// </summary>
    /// <param name="result">The negotiation result.</param>
    /// <returns>The matching outcome-tag string constant.</returns>
    public static string DictionaryNegotiationOutcomeTag(SharedDictionaryNegotiationResult result) =>
        result.FellBack
            ? (result.FingerprintMismatch
                ? DictionaryNegotiationOutcomeFingerprintMismatch
                : (result.PeerCapabilityKnown
                    ? DictionaryNegotiationOutcomeFellBack
                    : DictionaryNegotiationOutcomeUnknown))
            : DictionaryNegotiationOutcomeMatched;

    // --- Self-distributing shared-dictionary convergence (opt-in) ---

    /// <summary>
    /// Counter incremented once per shared-dictionary pull attempt the
    /// shipper makes against a peer-advertised id it does not yet hold,
    /// when the auto-distributing shared dictionary is opted into. Tagged
    /// by <see cref="TagTree"/>, <see cref="TagPeer"/>, and
    /// <see cref="TagOutcome"/> (one of
    /// <see cref="DictionaryConvergenceOutcomeInstalled"/>,
    /// <see cref="DictionaryConvergenceOutcomeRejected"/>, or
    /// <see cref="DictionaryConvergenceOutcomeUnavailable"/>), so an
    /// operator can watch how a fleet converges onto a shared trained
    /// dictionary and spot fingerprint rejections.
    /// </summary>
    public static readonly Counter<long> DictionaryConvergence =
        Meter.CreateCounter<long>("orleans.lattice.replication.ship.dictionary_convergence", unit: "{pull}",
            description: "Shared-dictionary convergence pulls, tagged by tree, peer, and outcome.");

    /// <summary>Canonical name of the <see cref="DictionaryConvergence"/> counter.</summary>
    public const string DictionaryConvergenceName = "orleans.lattice.replication.ship.dictionary_convergence";

    /// <summary>
    /// <see cref="TagOutcome"/> value on the
    /// <see cref="DictionaryConvergence"/> counter: the peer served the
    /// dictionary bytes, their fingerprint matched the advertised
    /// fingerprint, and they were installed locally.
    /// </summary>
    public const string DictionaryConvergenceOutcomeInstalled = "installed";

    /// <summary>
    /// <see cref="TagOutcome"/> value on the
    /// <see cref="DictionaryConvergence"/> counter: the peer served bytes
    /// whose fingerprint did not match the advertised fingerprint, or a
    /// local id collision rejected the install, so the bytes were
    /// discarded without installing.
    /// </summary>
    public const string DictionaryConvergenceOutcomeRejected = "rejected";

    /// <summary>
    /// <see cref="TagOutcome"/> value on the
    /// <see cref="DictionaryConvergence"/> counter: the peer (or transport)
    /// did not serve the pull - an un-upgraded peer, a momentarily
    /// unreachable hop, or the peer no longer holds the id - so the shipper
    /// leaves the dictionary uninstalled and retries on a later tick.
    /// </summary>
    public const string DictionaryConvergenceOutcomeUnavailable = "unavailable";

    // --- Coordinated cross-cluster restore saga (observability) ------------------

    /// <summary>
    /// Tag key for the coordinated-restore saga phase carried by
    /// <see cref="SagaPhaseDuration"/>. Values are
    /// <see cref="SagaPhasePrepare"/>, <see cref="SagaPhaseCommit"/>, and
    /// <see cref="SagaPhaseAbort"/>.
    /// </summary>
    public const string TagPhase = "phase";

    /// <summary>
    /// Tag key for the cause of a saga compensation carried by
    /// <see cref="SagaCompensations"/>. Values are
    /// <see cref="SagaCauseVoteAbort"/> (a participant voted abort and the
    /// coordinator drove a rollback) and <see cref="SagaCauseCoordinatorLoss"/>
    /// (the cutover fence expired without a coordinator decision and the
    /// participant auto-compensated).
    /// </summary>
    public const string TagCause = "cause";

    /// <summary><see cref="TagPhase"/> value: the unfenced, resumable prepare (shadow build) phase.</summary>
    public const string SagaPhasePrepare = "prepare";

    /// <summary><see cref="TagPhase"/> value: the fenced cutover commit (atomic alias swap) phase.</summary>
    public const string SagaPhaseCommit = "commit";

    /// <summary><see cref="TagPhase"/> value: the compensation / rollback (abort) phase.</summary>
    public const string SagaPhaseAbort = "abort";

    /// <summary>
    /// <see cref="TagCause"/> value on <see cref="SagaCompensations"/>: the
    /// compensation was driven by the coordinator after at least one participant
    /// voted abort (an all-or-nothing rollback of every prepared cluster).
    /// </summary>
    public const string SagaCauseVoteAbort = "vote-abort";

    /// <summary>
    /// <see cref="TagCause"/> value on <see cref="SagaCompensations"/>: the
    /// compensation was driven by a participant's own cutover-fence expiry after
    /// the coordinator decision never arrived (the coordinator-loss safety net).
    /// </summary>
    public const string SagaCauseCoordinatorLoss = "coordinator-loss";

    /// <summary><see cref="TagReason"/> value on the participant vote counter: the participant voted commit.</summary>
    public const string SagaReasonCommit = "commit";

    /// <summary>
    /// <see cref="TagReason"/> value on the participant vote counter: the backup
    /// package is not wired on this cluster, so there is nothing to restore and
    /// the participant votes abort.
    /// </summary>
    public const string SagaReasonEngineUnavailable = "engine-unavailable";

    /// <summary>
    /// <see cref="TagReason"/> value on the participant vote counter: admission
    /// refused an infeasible target (the tree cannot fit the target cluster) or
    /// the admission probe itself failed, before any shadow build started.
    /// </summary>
    public const string SagaReasonInfeasible = "infeasible";

    /// <summary>
    /// <see cref="TagReason"/> value on the participant vote counter: a build
    /// precondition failed (a missing backup or base in the manifest chain) - a
    /// permanent, non-retryable refusal.
    /// </summary>
    public const string SagaReasonPrecondition = "precondition";

    /// <summary>
    /// <see cref="TagReason"/> value on the participant vote counter: the shadow
    /// build exhausted its bounded retry budget (for example a persistent
    /// capacity exhaustion), so the participant garbage collected any partial
    /// shadow and voted abort.
    /// </summary>
    public const string SagaReasonBuildFailed = "build-failed";

    /// <summary>
    /// <see cref="TagReason"/> value on the participant commit / abort counters:
    /// the saga restored a single tree (the ordinary single-tree restore path).
    /// </summary>
    public const string SagaReasonSingle = "single";

    /// <summary>
    /// <see cref="TagReason"/> value on the participant commit / abort counters:
    /// the saga restored a backup set as one group-atomic unit.
    /// </summary>
    public const string SagaReasonSet = "set";

    /// <summary>
    /// Histogram of coordinated-restore saga phase durations, recorded by the
    /// cross-cluster coordinator after each phase transition. Reported in
    /// milliseconds as <c>double</c> and tagged by <see cref="TagPhase"/>
    /// (<see cref="SagaPhasePrepare"/> for the fan-out prepare/vote-collection
    /// window, <see cref="SagaPhaseCommit"/> for the commit fan-out, and
    /// <see cref="SagaPhaseAbort"/> for the compensation fan-out). Lets operators
    /// separate the long unfenced build window from the short cutover.
    /// </summary>
    public static readonly Histogram<double> SagaPhaseDuration =
        Meter.CreateHistogram<double>("orleans.lattice.replication.saga.phase.duration", unit: "ms",
            description: "Coordinated-restore saga phase durations, tagged by phase (prepare/commit/abort).");

    /// <summary>Canonical name of the <see cref="SagaPhaseDuration"/> histogram.</summary>
    public const string SagaPhaseDurationName = "orleans.lattice.replication.saga.phase.duration";

    /// <summary>
    /// Counter of participant prepare votes, incremented once per participant
    /// prepare with the vote outcome carried by <see cref="TagReason"/>
    /// (<see cref="SagaReasonCommit"/>, <see cref="SagaReasonInfeasible"/>,
    /// <see cref="SagaReasonPrecondition"/>, <see cref="SagaReasonBuildFailed"/>,
    /// or <see cref="SagaReasonEngineUnavailable"/>). Lets operators watch the
    /// commit-vote fraction and the distribution of abort refusals.
    /// </summary>
    public static readonly Counter<long> SagaParticipantVotes =
        Meter.CreateCounter<long>("orleans.lattice.replication.saga.participant.votes", unit: "{vote}",
            description: "Participant prepare votes, tagged by reason (the vote outcome).");

    /// <summary>Canonical name of the <see cref="SagaParticipantVotes"/> counter.</summary>
    public const string SagaParticipantVotesName = "orleans.lattice.replication.saga.participant.votes";

    /// <summary>
    /// Counter of participant commits (the fenced cutover alias swap), incremented
    /// once per committed participant and tagged by <see cref="TagReason"/>
    /// (<see cref="SagaReasonSingle"/> or <see cref="SagaReasonSet"/>).
    /// </summary>
    public static readonly Counter<long> SagaParticipantCommits =
        Meter.CreateCounter<long>("orleans.lattice.replication.saga.participant.commits", unit: "{commit}",
            description: "Participant commits (cutover alias swaps), tagged by reason (single/set).");

    /// <summary>Canonical name of the <see cref="SagaParticipantCommits"/> counter.</summary>
    public const string SagaParticipantCommitsName = "orleans.lattice.replication.saga.participant.commits";

    /// <summary>
    /// Counter of participant aborts (compensation / rollback), incremented once
    /// per aborted participant and tagged by <see cref="TagReason"/>
    /// (<see cref="SagaReasonSingle"/>, <see cref="SagaReasonSet"/>, or
    /// <see cref="SagaReasonEngineUnavailable"/>).
    /// </summary>
    public static readonly Counter<long> SagaParticipantAborts =
        Meter.CreateCounter<long>("orleans.lattice.replication.saga.participant.aborts", unit: "{abort}",
            description: "Participant aborts (compensations), tagged by reason.");

    /// <summary>Canonical name of the <see cref="SagaParticipantAborts"/> counter.</summary>
    public const string SagaParticipantAbortsName = "orleans.lattice.replication.saga.participant.aborts";

    /// <summary>
    /// Histogram of write-fence window durations per tree, recorded by the durable
    /// write-fence grain when the write fence is lifted (on the local cutover flip
    /// or on the self-lifting deadline). Reported in milliseconds as <c>double</c>
    /// and tagged by <see cref="TagTree"/>. This measures only the write-blocking
    /// cutover window (engage to write-fence lift), NOT the longer globally-gated
    /// shipping-pause window, so operators can confirm the fence stays bounded to
    /// the cutover and healthy clusters are not write-starved for the whole build.
    /// </summary>
    public static readonly Histogram<double> SagaFenceDuration =
        Meter.CreateHistogram<double>("orleans.lattice.replication.saga.fence.duration", unit: "ms",
            description: "Write-fence window duration per tree (engage to write-fence lift), tagged by tree.");

    /// <summary>Canonical name of the <see cref="SagaFenceDuration"/> histogram.</summary>
    public const string SagaFenceDurationName = "orleans.lattice.replication.saga.fence.duration";

    /// <summary>
    /// Counter of saga compensations, incremented once per participant grain that
    /// rolls back a prepared saga and tagged by <see cref="TagCause"/>
    /// (<see cref="SagaCauseVoteAbort"/> for a coordinator-driven rollback after a
    /// vote abort, or <see cref="SagaCauseCoordinatorLoss"/> for a fence-expiry
    /// auto-compensation after the coordinator decision never arrived).
    /// </summary>
    public static readonly Counter<long> SagaCompensations =
        Meter.CreateCounter<long>("orleans.lattice.replication.saga.compensations", unit: "{compensation}",
            description: "Saga compensations, tagged by cause (vote-abort/coordinator-loss).");

    /// <summary>Canonical name of the <see cref="SagaCompensations"/> counter.</summary>
    public const string SagaCompensationsName = "orleans.lattice.replication.saga.compensations";
}
