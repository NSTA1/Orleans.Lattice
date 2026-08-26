using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="IReplicationShipperGrain"/> implementation.
/// Hosts the per-<c>(tree, peer)</c> outbound ship loop using the
/// shared <see cref="CoordinatorGrain{TSelf}"/> reminder + phase-timer
/// scaffold.
/// <para>
/// Steady-state drain is a partition-resume hot path: each pump tick
/// reads one bounded page per WAL partition starting from a durable
/// per-partition sequence cursor (<see cref="ReplicationShipperState.PartitionCursors"/>),
/// merges the pages by <see cref="HybridLogicalClock"/> ascending via
/// a heap-free linear scan-for-min over partition heads (O(P), and
/// O(1) for the canonical single-partition case), and emits up to
/// <see cref="LatticeReplicationOptions.ShipBatchSize"/> entries per
/// outbound batch. <see cref="IChangeFeed"/> is reserved for bootstrap
/// / test / future-materialiser consumers that have no notion of
/// partition routing.
/// </para>
/// </summary>
internal sealed class ReplicationShipperGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<ReplicationShipperGrain> logger,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    IReplicationTransport transport,
    IReplicationBatchEncoder encoder,
    IWalRecordEncoder walRecordEncoder,
    IWalCursorRegistry cursorRegistry,
    IGrainFactory grainFactory,
    [PersistentState("replication-shipper", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationShipperState> state,
    ReplicationPeerStats peerStats,
    ILatticeMergeModeResolver modeResolver,
    WireVersionNegotiationState negotiationState,
    IReplicationDigestProbeTransport digestProbeTransport,
    CrdtShapeRegistry? crdtShapeRegistry = null,
    SharedDictionaryNegotiationState? dictionaryNegotiationState = null,
    ILatticeCompressionDictionaryProvider? dictionaryProvider = null)
    : CoordinatorGrain<ReplicationShipperGrain>(context, reminderRegistry, logger),
      IReplicationShipperGrain
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly IReplicationTransport _transport =
        transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly IReplicationBatchEncoder _encoder =
        encoder ?? throw new ArgumentNullException(nameof(encoder));
    private readonly IWalRecordEncoder _walRecordEncoder =
        walRecordEncoder ?? throw new ArgumentNullException(nameof(walRecordEncoder));
    private readonly IWalCursorRegistry _cursorRegistry =
        cursorRegistry ?? throw new ArgumentNullException(nameof(cursorRegistry));
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ReplicationPeerStats _peerStats =
        peerStats ?? throw new ArgumentNullException(nameof(peerStats));
    private readonly ILatticeMergeModeResolver _modeResolver =
        modeResolver ?? throw new ArgumentNullException(nameof(modeResolver));
    private readonly WireVersionNegotiationState _negotiationState =
        negotiationState ?? throw new ArgumentNullException(nameof(negotiationState));
    private readonly IReplicationDigestProbeTransport _digestProbeTransport =
        digestProbeTransport ?? throw new ArgumentNullException(nameof(digestProbeTransport));

    /// <summary>
    /// The CRDT shape registry used by the pre-ship coalescing pass to
    /// decode, combine, and re-encode the typed delta payloads of a
    /// CRDT tree's same-key point writes. Injected from DI (registered
    /// as a singleton by the core <c>AddLattice</c> call) when the
    /// replication package runs inside a configured silo; <c>null</c>
    /// only in unit-test constructions that exercise non-CRDT paths, in
    /// which case the CRDT coalescing pass lazily instantiates a private
    /// registry the first time a CRDT tree opts into coalescing. Pre-ship
    /// coalescing now defaults on, so a stock host touches this field for
    /// CRDT trees unless it sets
    /// <see cref="LatticeReplicationOptions.PreShipCoalescingEnabled"/> to
    /// <see langword="false"/>.
    /// </summary>
    private CrdtShapeRegistry? _crdtShapeRegistry = crdtShapeRegistry;

    // Per-(tree, peer) shared-dictionary negotiation telemetry. Optional
    // constructor dependency so the many direct-construction unit tests
    // continue to compile unchanged; the DI registration always supplies
    // the process-wide singleton, and the null-coalescing fallback gives
    // a self-contained instance when one is not injected. Never null.
    private readonly SharedDictionaryNegotiationState _dictionaryNegotiationState =
        dictionaryNegotiationState ?? new SharedDictionaryNegotiationState();

    // Resolves the sender's own configured shared-dictionary bytes so the
    // ship path can fingerprint them and gate dictionary compression on
    // (id, fingerprint) against a peer that advertised the fingerprint-bearing
    // capability. Optional constructor dependency: the DI registration supplies
    // the same provider the encoder uses; null in unit-test constructions that
    // never exercise the fingerprint path (those fall through to the id-only
    // negotiation, matching a peer that predates the fingerprint slot).
    private readonly ILatticeCompressionDictionaryProvider? _dictionaryProvider =
        dictionaryProvider;

    private string _treeName = "";
    private string _peerClusterId = "";
    private bool _keyParsed;

    // The physical tree id the WAL shards are addressed by. A logical tree can
    // be repointed to a new physical tree by a registry alias swap (shadow-
    // cutover restore, resize, reshard); WAL shards are keyed by the physical
    // id. Re-resolved from _treeName each pump tick by
    // EnsureBoundToCurrentSourceIdentityAsync so a mid-stream swap does not
    // silently orphan the ship cursor. Defaults to _treeName (logical ==
    // physical for a tree that has never been swapped) until the first resolve.
    private string _walTreeId = "";

    /// <summary>
    /// The peer's most recently advertised
    /// <see cref="ReplicationAck.SupportedWireVersion"/>, or
    /// <see langword="null"/> until the peer has acknowledged a batch
    /// (or the peer is a build that predates wire-version negotiation
    /// and never stamps the slot). Feeds
    /// <see cref="WireVersionNegotiation.Negotiate(int, int, int, int?)"/>
    /// on the next pump tick when
    /// <see cref="LatticeReplicationOptions.WireVersionNegotiationEnabled"/>
    /// is set. Activation-scoped: lost on grain deactivation, at which
    /// point the next ack re-advertises the peer's capability.
    /// </summary>
    private int? _peerWireVersion;

    /// <summary>
    /// The framing wire version the shipper stamps on the next batch's
    /// <see cref="EncodedBatchHeader"/>. Defaults to
    /// <see cref="EncodedBatchHeader.CurrentWireVersion"/> and is only
    /// ever lowered when
    /// <see cref="LatticeReplicationOptions.WireVersionNegotiationEnabled"/>
    /// is set and <see cref="TryNegotiateWireVersion"/> has negotiated a
    /// down-stamp target for a peer running an older build. When
    /// negotiation is off the field is never read (the header sites stamp
    /// <see cref="EncodedBatchHeader.CurrentWireVersion"/> directly), so a
    /// stale value left over from a runtime options flip cannot leak onto
    /// the wire. Activation-scoped.
    /// </summary>
    private int _negotiatedWireVersion = EncodedBatchHeader.CurrentWireVersion;

    /// <summary>
    /// Set by <see cref="TryNegotiateWireVersion"/> when the negotiated
    /// down-stamp target cannot carry the configured framing compression but
    /// is otherwise down-encodable (<see cref="LatticeMergeMode.LwwRegister"/>,
    /// version &gt;= <see cref="WireVersionDownEncoder.MinimumDownEncodableWireVersion"/>).
    /// When set, <see cref="ResolveFramingCompression"/> forces the per-peer
    /// batch uncompressed so a compressed LWW tree keeps replicating to an
    /// older peer instead of stalling. Activation-scoped; recomputed every
    /// negotiation.
    /// </summary>
    private bool _downStampDropsCompression;

    /// <summary>
    /// The peer's most recently advertised
    /// <see cref="ReplicationAck.AdvertisedDictionaryIds"/>, or
    /// <see langword="null"/> until the peer has acknowledged a batch (or
    /// the peer is a build that predates dictionary negotiation and never
    /// stamps the slot). Feeds
    /// <see cref="SharedDictionaryNegotiation.Negotiate(uint, System.Collections.Generic.IReadOnlyCollection{uint})"/>
    /// on the next pump tick when
    /// <see cref="LatticeReplicationOptions.DictionaryNegotiationEnabled"/>
    /// is set. Activation-scoped: lost on grain deactivation, at which
    /// point the next ack re-advertises the peer's dictionary capability.
    /// </summary>
    private uint[]? _peerAdvertisedDictionaryIds;

    /// <summary>
    /// The peer's most recently advertised
    /// <see cref="ReplicationAck.AdvertisedDictionaries"/> (the
    /// fingerprint-bearing <c>(id, fingerprint)</c> capability), or
    /// <see langword="null"/> until the peer has acknowledged a batch with the
    /// slot populated (a build predating the fingerprint slot never stamps it).
    /// When non-null this takes precedence over
    /// <see cref="_peerAdvertisedDictionaryIds"/>: the ship path negotiates on
    /// <c>(id, fingerprint)</c> so a same-id/different-bytes peer falls back to
    /// dictionary-less compression. Activation-scoped: lost on deactivation, at
    /// which point the next ack re-advertises the peer's capability.
    /// </summary>
    private AdvertisedCompressionDictionary[]? _peerAdvertisedDictionaries;

    /// <summary>
    /// One-shot latch so the same-id/different-fingerprint misconfiguration is
    /// logged at most once per activation (the distinct telemetry counter still
    /// increments every tick). Reset implicitly on deactivation.
    /// </summary>
    private bool _dictionaryFingerprintMismatchWarned;

    // Caches the fingerprint of the sender's own configured dictionary bytes so
    // the per-tick negotiation does not re-resolve and re-hash on every pump.
    // Keyed on the configured id: a runtime options flip to a different id
    // invalidates the cache. Activation-scoped.
    private uint _cachedFingerprintForId;
    private ulong _cachedFingerprint;
    private bool _cachedFingerprintResolved;

    /// <summary>
    /// The shared compression-dictionary id the shipper stamps on the next
    /// batch's <see cref="EncodedBatchHeader"/>. Defaults to <c>0</c> ("no
    /// dictionary") and is only set to a non-zero id when
    /// <see cref="LatticeReplicationOptions.DictionaryNegotiationEnabled"/>
    /// is set and <see cref="TryNegotiateSharedDictionary"/> has confirmed
    /// the peer advertised the configured id. When negotiation is off the
    /// field is never read (the header sites stamp
    /// <see cref="LatticeReplicationOptions.FramingCompressionDictionaryId"/>
    /// directly), so a stale value cannot leak onto the wire.
    /// Activation-scoped.
    /// </summary>
    private uint _negotiatedDictionaryId;

    /// <summary>
    /// Wall-clock instant at or after which the next phase tick is
    /// allowed to attempt a send. Set to a future value on transient
    /// transport failure to apply backoff. <see cref="DateTime.MinValue"/>
    /// (the default) means "no backoff in effect".
    /// </summary>
    private DateTime _nextRetryAtUtc = DateTime.MinValue;

    /// <summary>Re-entrancy guard. Orleans serialises grain turns, so the field is for clarity rather than concurrency.</summary>
    private bool _pumpInFlight;

    /// <summary>
    /// Receiver-stamped <see cref="ReplicationAck.SuggestedBatchSize"/>
    /// from the most recent successful ack, or <see langword="null"/>
    /// when the receiver has not stamped a preference (or stamped
    /// <see langword="null"/> to re-accelerate). Clamps the per-tick
    /// batch cap on the next pump tick to
    /// <c>min(SuggestedBatchSize, options.ShipBatchSize)</c>.
    /// Activation-scoped: lost on grain deactivation, at which point
    /// the receiver re-stamps its preference on the next ack.
    /// </summary>
    private int? _receiverSuggestedBatchSize;

    /// <summary>
    /// Lazily-created sender-side adaptive batch-size controller, present
    /// only when <see cref="LatticeReplicationOptions.AdaptiveBatchSizingEnabled"/>
    /// is on. Activation-scoped and in-memory: a grain reactivation resets
    /// the controller (the effective size returns to
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/> and the
    /// controller re-learns from the live link). The receiver flow-control
    /// hint remains the hard ceiling - the controller only ever shrinks the
    /// per-tick cap in the headroom beneath it.
    /// </summary>
    private AdaptiveBatchSizeController? _adaptiveController;

    /// <summary>
    /// Random source for backoff jitter. Aliased to the process-wide
    /// thread-safe singleton (<see cref="Random.Shared"/>) - shared
    /// across every shipper activation on this silo. Sufficient for
    /// jitter purposes; not cryptographically random.
    /// </summary>
    private readonly Random _jitterRandom = Random.Shared;

    /// <summary>
    /// Activation-scoped drain buffer reused across pump ticks. Cleared
    /// at the start of every <see cref="PumpOnceAsync"/>. Holds the
    /// typed <see cref="WalRecord"/> head decoded from each shipping
    /// page entry (used to apply <see cref="ShouldShip"/> and the HLC
    /// filter); the matching pre-encoded byte segments are stored in
    /// lockstep in <see cref="_drainEncodedSegments"/>. Bounded in
    /// size by <see cref="LatticeReplicationOptions.ShipBatchSize"/>.
    /// </summary>
    private readonly List<WalRecord> _drainBuffer = new();

    /// <summary>
    /// Activation-scoped parallel list of pre-encoded entry segments
    /// that mirrors <see cref="_drainBuffer"/>. Each
    /// <see cref="ArraySegment{T}"/> borrows the bytes from the
    /// shipping page returned by
    /// <see cref="IWalShardGrain.ReadShippingAsync"/> for this tick;
    /// the segments are passed through to
    /// <see cref="ReplicationBatch.EncodedEnvelope"/> verbatim so the
    /// framing-aware transport can write the bytes straight onto the
    /// wire without re-encoding the typed entries.
    /// </summary>
    private readonly List<ArraySegment<byte>> _drainEncodedSegments = new();

    /// <summary>
    /// Activation-scoped reusable backing array for the
    /// <see cref="ReplicationBatchEncodedEnvelope.EncodedEntries"/>
    /// <see cref="ReadOnlyMemory{T}"/> handed to the framing-aware
    /// transport. Grown on demand via <see cref="Array.Resize{T}"/>
    /// and reused across pump ticks so the steady-state ship path
    /// allocates nothing beyond the per-page DTOs the WAL grain
    /// returns. The borrowed segments are stable for the duration
    /// of the surrounding <see cref="IReplicationTransport.SendAsync"/>
    /// call (Orleans serialises grain turns and <c>SendAsync</c>
    /// awaits inline), and the array is overwritten in place at the
    /// start of every tick.
    /// </summary>
    private ArraySegment<byte>[] _encodedEnvelopeScratch = Array.Empty<ArraySegment<byte>>();

    /// <summary>
    /// Running byte total of the segments staged in
    /// <see cref="_drainEncodedSegments"/> for the current tick.
    /// Reported as the <c>bytes_behind</c> peer-stat floor on
    /// success so the ship path is observably tracked on the
    /// dashboard. Reset to zero at the start of every
    /// <see cref="PumpOnceAsync"/>.
    /// </summary>
    private long _drainEncodedByteCount;

    /// <summary>
    /// Activation-scoped content-hash dedup measurement cache, lazily
    /// created on the first pump tick that observes
    /// <see cref="LatticeReplicationOptions.ContentHashDedupEnabled"/>
    /// set. Maps each recently-shipped key to the content hash of the
    /// last value shipped for it; a re-send of byte-identical content
    /// for a cached key increments the
    /// <see cref="LatticeReplicationMetrics.ShipRedundantPayloads"/> /
    /// <see cref="LatticeReplicationMetrics.ShipRedundantPayloadBytes"/>
    /// counters. <see langword="null"/> while the option is off (the
    /// default), so a host that never opts in pays no cache memory and
    /// the steady-state ship path is byte-identical to today's. The
    /// cache only measures - it never elides or alters the bytes
    /// shipped.
    /// </summary>
    private ShippedContentHashCache? _contentHashCache;

    /// <summary>
    /// Per-shipper-activation cache of whether the peer can perform the
    /// content-hash manifest exchange (the sender-manifest /
    /// receiver-pull-missing payload-elision round trip). <see langword="null"/>
    /// until the first batch attempts the exchange while
    /// <see cref="LatticeReplicationOptions.ContentHashDedupElisionEnabled"/>
    /// is set; <see langword="true"/> once a peer has replied that it
    /// supports the exchange; <see langword="false"/> once a peer (or the
    /// default no-op transport) has reported it cannot, after which the
    /// shipper permanently falls back to shipping the full batch verbatim
    /// for the rest of the activation - the rolling-upgrade-safe behaviour
    /// identical to today's wire. Reset on grain deactivation, at which
    /// point capability is re-learned on the next elision-eligible batch.
    /// </summary>
    private bool? _peerSupportsManifestExchange;

    /// <summary>
    /// Activation-scoped scratch map reused by the pre-ship coalescing
    /// pass (<see cref="CoalesceDrainBuffer"/>) to record, per coalescable
    /// key, the index of its last (highest-HLC) occurrence in the current
    /// drained batch. Cleared at the start of every coalescing pass and
    /// only allocated the first time coalescing runs, so a host that never
    /// opts into
    /// <see cref="LatticeReplicationOptions.PreShipCoalescingEnabled"/>
    /// pays no map memory and the steady-state ship path is byte-identical
    /// to today's.
    /// </summary>
    private Dictionary<string, int>? _coalesceLastIndex;

    /// <summary>
    /// Activation-scoped scratch map reused by the CRDT branch of the
    /// pre-ship coalescing pass
    /// (<see cref="CoalesceCrdtDrainBuffer"/>) to record, per coalescable
    /// key, the running combined delta plus the index of its last
    /// (highest-HLC) occurrence in the current drained batch. Cleared at
    /// the start of every CRDT coalescing pass and only allocated the
    /// first time the CRDT branch runs, so a host that never opts into
    /// <see cref="LatticeReplicationOptions.PreShipCoalescingEnabled"/>
    /// on a CRDT tree pays no map memory and the steady-state ship path
    /// is byte-identical to today's.
    /// </summary>
    private Dictionary<string, CrdtCoalesceState>? _coalesceCrdtState;

    /// <summary>
    /// Activation-scoped reusable buffer writer the CRDT coalescing pass
    /// re-encodes a combined-delta entry into before swapping it into the
    /// drain segment list. Lazily allocated on the first CRDT merge and
    /// reset (not reallocated) on each reuse, so the steady-state ship
    /// path allocates nothing extra once warm.
    /// </summary>
    private ArrayBufferWriter<byte>? _coalesceReencodeWriter;

    /// <summary>
    /// Per-key accumulator for the CRDT branch of the pre-ship coalescing
    /// pass. Holds the running combined delta, the index of the last
    /// contributing entry (whose HLC / causal metadata the merged result
    /// inherits), the count of source deltas folded so far, and a flag
    /// that goes false the moment a same-key entry is encountered that
    /// cannot participate (a null typed delta on a CRDT mode), forcing the
    /// whole key to ship verbatim.
    /// </summary>
    private struct CrdtCoalesceState
    {
        /// <summary>The running combined typed delta (deserialised DTO).</summary>
        public object? Combined;

        /// <summary>Index in the drain buffer of the last contributing entry.</summary>
        public int LastIndex;

        /// <summary>Number of source deltas folded into <see cref="Combined"/>.</summary>
        public int FoldCount;

        /// <summary>
        /// Whether every same-key entry seen so far can participate in the
        /// combine. False once an opaque (null-delta) entry is observed,
        /// in which case the key ships verbatim.
        /// </summary>
        public bool CanCombine;
    }

    // ── Activation-scoped scratch arrays for the k-way HLC merge ──
    //
    // Sized lazily on first pump tick (and resized on partition-count
    // change via Array.Resize). Reused across every subsequent tick;
    // steady-state pump allocates nothing beyond the per-page DTOs the
    // shard grain returns.
    //
    // Index range: [0, _partitionCount).
    //
    //   _partitionPages[p]    - current shipping page from partition p
    //                           (pre-encoded entry payloads from the
    //                           WAL plus their sequence numbers), or
    //                           null when that partition is "drained
    //                           for this tick" (no more entries past
    //                           the saved cursor right now). Each head
    //                           entry is decoded once (lazily, on first
    //                           candidate inspection) into
    //                           _partitionHead[p] so ShouldShip / HLC
    //                           predicates can run without re-decoding
    //                           on every merge step.
    //   _partitionPageIndex[p]- next entry index inside the page;
    //                           equals _partitionPages[p].Count when
    //                           the page is exhausted and a refill is
    //                           required to advance further.
    //   _partitionNextSeq[p]  - fromSequence to pass on the next
    //                           ReadShippingAsync call; mirrors
    //                           state.PartitionCursors[p] but kept as a
    //                           primitive long to avoid dictionary
    //                           lookups inside the merge loop.
    //   _partitionMaxReadSeq[p] - highest sequence we have *consumed*
    //                           (shipped or filtered) from partition p
    //                           this tick. -1 means "none consumed yet";
    //                           on positive ack the partition cursor
    //                           advances to this value + 1.
    //   _partitionAdvanced[p] - whether the current tick consumed at
    //                           least one entry from partition p (used
    //                           to bound the cursor write to changed
    //                           partitions on ack).
    //   _partitionHead[p]     - lazily-decoded WalRecord for the
    //                           current head entry on partition p;
    //                           valid only when
    //                           _partitionHeadDecoded[p] is true.
    private IReadOnlyList<WalShardShippingEntry>?[] _partitionPages =
        Array.Empty<IReadOnlyList<WalShardShippingEntry>?>();
    private int[] _partitionPageIndex = Array.Empty<int>();
    private long[] _partitionNextSeq = Array.Empty<long>();
    private long[] _partitionMaxReadSeq = Array.Empty<long>();
    private bool[] _partitionAdvanced = Array.Empty<bool>();
    //   _legacyCursorMigrationPending - whether THIS pump tick is the
    //                           one-time legacy migration from a pre-
    //                           partition-cursor build: a non-zero
    //                           state.Cursor but an entirely EMPTY
    //                           PartitionCursors dictionary. This is the
    //                           only situation in which the defensive
    //                           scalar-HLC drop in the merge loop may fire.
    //                           It must be keyed on the whole dictionary
    //                           being empty, NOT on a single partition
    //                           lacking a cursor: a genuinely cold
    //                           partition in a modern build (dict non-empty
    //                           overall, missing just this partition) has
    //                           real unshipped entries whose per-leaf HLC
    //                           can legitimately sit below the scalar
    //                           cursor, and dropping them silently strands
    //                           them - see MergeOneBatchAsync.
    private bool _legacyCursorMigrationPending;
    private IWalShardGrain?[] _partitionGrainCache = Array.Empty<IWalShardGrain?>();
    private WalRecord[] _partitionHead = Array.Empty<WalRecord>();
    private bool[] _partitionHeadDecoded = Array.Empty<bool>();
    private int _partitionCount;

    /// <summary>
    /// Number of successful cursor advances since the last durable
    /// <c>WriteStateAsync</c>. Reset
    /// to <c>0</c> on every flush. Counter rather than wall-clock
    /// because the relevant cost is per-batch persistence I/O, and
    /// the per-batch rate is what the operator tunes via
    /// <see cref="LatticeReplicationOptions.ShipCursorWriteInterval"/>.
    /// </summary>
    private int _pendingCursorWrites;

    /// <summary>
    /// Wall-clock instant at which the first un-flushed cursor advance
    /// since the last durable flush was booked. Anchors the time
    /// dimension of the coalescing rule
    /// (<see cref="LatticeReplicationOptions.ShipCursorWriteMaxDelay"/>):
    /// a flush is forced once <c>now - this</c> reaches the configured
    /// max delay, even if fewer than
    /// <see cref="LatticeReplicationOptions.ShipCursorWriteInterval"/>
    /// acks have accumulated. Set when <see cref="_pendingCursorWrites"/>
    /// transitions <c>0 -&gt; 1</c> and reset to
    /// <see cref="DateTime.MinValue"/> on every flush.
    /// </summary>
    private DateTime _oldestPendingCursorWriteUtc = DateTime.MinValue;

    /// <summary>
    /// Clock used to evaluate the time dimension of the cursor-write
    /// coalescing rule. Aliased to <see cref="TimeProvider.System"/> in
    /// production; unit tests substitute a controllable provider via
    /// <see cref="SetCursorFlushClockForTesting(TimeProvider)"/> so the
    /// elapsed-since-first-pending check is deterministic without a real
    /// wall-clock wait.
    /// </summary>
    private TimeProvider _cursorFlushClock = TimeProvider.System;

    /// <summary>
    /// Whether the shipper has resolved and bound to the source tree's physical
    /// identity at least once for this activation. Until it has, the next pump
    /// tick performs the authoritative registry resolve regardless of the
    /// backstop clock, so a freshly activated shipper always binds before it
    /// ships. Set by <see cref="ApplyResolvedIdentityAsync(string, int)"/>.
    /// </summary>
    private bool _sourceIdentityResolved;

    /// <summary>
    /// Wall-clock time of the last source-identity resolve or event-driven
    /// rebind, measured on <see cref="_cursorFlushClock"/>. The gated per-tick
    /// refresh (<see cref="MaybeRefreshSourceIdentityAsync"/>) reads the registry
    /// again only once the
    /// <see cref="LatticeReplicationOptions.ShipSourceIdentityBackstopInterval"/>
    /// has elapsed since this instant, so an idle tree performs at most one
    /// registry read per backstop interval rather than one per pump tick.
    /// </summary>
    private DateTime _lastSourceIdentityResolveUtc;

    /// <summary>
    /// Highest HLC reported to the registry (i.e. successfully
    /// persisted in a previous flush). Used to suppress redundant
    /// <see cref="IWalCursorRegistry.ReportCursorAsync"/>
    /// calls when a flush did not actually advance the durable cursor
    /// (e.g. only partition cursors changed since the last flush).
    /// </summary>
    private HybridLogicalClock _lastReportedCursor = HybridLogicalClock.Zero;

    /// <summary>
    /// Wall-clock instant of the most recent successful outbound
    /// contact with the peer - a non-empty acked batch, or an empty
    /// acked liveness probe. Anchored at activation in
    /// <see cref="DateTime.MinValue"/> so the first pump tick whose
    /// drain finds no work fires an immediate liveness probe (the
    /// "(MinValue) - now &gt;= interval" branch trivially passes for
    /// every finite interval). Activation-scoped; no persisted state
    /// is added.
    /// </summary>
    private DateTime _lastSuccessfulContactUtc = DateTime.MinValue;

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "shipper-keepalive";

    /// <inheritdoc />
    protected override TimeSpan KeepaliveReminderPeriod => TimeSpan.FromSeconds(90);

    /// <inheritdoc />
    /// <remarks>
    /// Read once at activation via <see cref="IOptionsMonitor{TOptions}.CurrentValue"/>.
    /// The Orleans timer infrastructure registers the period at
    /// <see cref="CoordinatorGrain{TSelf}.StartPhaseTimer"/> time, so a runtime
    /// option change does not propagate until the activation is recycled -
    /// which is the same scope as "silo restart picks up the new value".
    /// </remarks>
    protected override TimeSpan PhaseTimerPeriod => _optionsMonitor.CurrentValue.ShipPhaseTimerPeriod;

    /// <inheritdoc />
    protected override bool InProgress => true; // The shipper is steady-state - always running.

    /// <inheritdoc />
    protected override string LogContext => $"shipper {_treeName}/{_peerClusterId}";

    /// <inheritdoc />
    public async Task EnsureActiveAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();
        // RegisterOrUpdateReminder is idempotent; StartPhaseTimer's
        // _phaseTimer ??= guard makes the second call a no-op. Safe
        // for repeated invocation.
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Arms the steady-state phase timer on every activation, however the
    /// activation arose (the driver's <see cref="EnsureActiveAsync"/>, an
    /// incoming doorbell, a client call, or a reminder-driven
    /// reactivation). The shipper is perpetual (<see cref="InProgress"/>
    /// is always <c>true</c>), so the timer is the single authority for
    /// draining and shipping; anchoring it here means a coalesced or
    /// dropped doorbell can never leave the backlog un-drained, and the
    /// doorbell path can stay a cheap edge-triggered wake (see
    /// <see cref="OnDoorbellAsync"/>). Registering a grain timer inside
    /// the activation hook is the supported Orleans pattern; the keepalive
    /// reminder remains a defence-in-depth re-arm if the activation is
    /// ever recycled without this hook running.
    /// </summary>
    protected override Task OnActivateCoreAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();
        StartPhaseTimer();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Flushes any pending deferred-persist cursor on graceful
    /// deactivation. Crash deactivations bypass this hook by design -
    /// the receiver's HLC dedupe bounds the replay cost in that case
    /// (at most <see cref="LatticeReplicationOptions.ShipCursorWriteInterval"/>
    /// &#xD7; <see cref="LatticeReplicationOptions.ShipBatchSize"/>
    /// entries get re-shipped and no-op'd at the receiver). A storage
    /// failure during the flush must not block deactivation; the
    /// pending advance is recovered on the next activation by
    /// re-shipping from the last durable cursor.
    /// </summary>
    protected override async Task OnDeactivateCoreAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_pendingCursorWrites == 0)
        {
            return;
        }
        try
        {
            await FlushCursorAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Pending cursor flush failed during deactivation of {Context}; "
                + "recovery will re-ship at most {Pending} batches' worth of entries (receiver dedupes).",
                LogContext, _pendingCursorWrites);
        }
        _ = reason;
    }

    /// <inheritdoc />
    public Task OnDoorbellAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Edge-triggered wake only - deliberately cheap. The steady-state
        // phase timer (armed on activation in OnActivateCoreAsync and
        // re-armed by the keepalive reminder) performs the actual
        // drain+ship, so a doorbell's sole effect is to (re)activate this
        // grain if it had been deactivated - which the act of delivering
        // this call has already done. Returning immediately keeps this
        // non-reentrant activation free for the timer instead of running a
        // full cross-cluster ship inline: under receiver back-pressure an
        // inline pump would hold the activation for the whole ship
        // round-trip, head-of-line-block the timer and every queued
        // doorbell, and time out at the producer-side caller - dropping
        // the very wake that was meant to be the shipping safety net. The
        // next timer tick (at most one ShipPhaseTimerPeriod away) picks up
        // the freshly-appended work.
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task PauseShippingAsync(string sagaId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();

        if (string.Equals(state.State.AdminPauseSagaId, sagaId, StringComparison.Ordinal))
        {
            return;
        }

        state.State.AdminPauseSagaId = sagaId;
        await state.WriteStateAsync();
        Logger.LogInformation(
            "{Context}: shipping durably paused for cross-cluster saga {SagaId}.",
            LogContext, sagaId);
    }

    /// <inheritdoc />
    public async Task ResumeShippingAsync(string sagaId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();

        if (!string.Equals(state.State.AdminPauseSagaId, sagaId, StringComparison.Ordinal))
        {
            return;
        }

        state.State.AdminPauseSagaId = null;
        await state.WriteStateAsync();
        Logger.LogInformation(
            "{Context}: shipping resumed after cross-cluster saga {SagaId}.",
            LogContext, sagaId);

        // Re-arm the pump so the resume takes effect on the next tick rather
        // than waiting a full keepalive period.
        await StartCoordinatorAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsShippingPausedAsync() =>
        Task.FromResult(state.State.AdminPauseSagaId is not null);

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        ParseGrainKey();

        // Durable administrative pause (saga cutover): no post-cut entry may
        // leave the cluster while a saga is in flight. The cursor is never
        // advanced, so shipping resumes from the same point when the pause is
        // lifted. Checked before the transient backoff gate because it is a
        // stronger, longer-lived stop.
        if (state.State.AdminPauseSagaId is not null)
        {
            return;
        }

        // Honour the backoff budget set by the previous failed pump.
        if (_nextRetryAtUtc > DateTime.UtcNow)
        {
            return;
        }

        if (_pumpInFlight)
        {
            return;
        }

        _pumpInFlight = true;
        try
        {
            await PumpOnceAsync(CancellationToken.None);
        }
        finally
        {
            _pumpInFlight = false;
        }
    }

    /// <summary>
    /// Entry point for a single pump tick. Resolves the per-tree
    /// options, sizes the sender-side pipelining window from
    /// <see cref="LatticeReplicationOptions.ShipMaxInFlight"/>, and
    /// dispatches to either the strict-serial path (window of one -
    /// the default, behaviour-identical to the pre-pipelining shipper)
    /// or the bounded-pipelining path (window &gt; 1).
    /// <para>
    /// Receiver flow-control collapses the window back to one whenever
    /// the receiver's most recent ack stamped a
    /// <see cref="ReplicationAck.SuggestedBatchSize"/> hint: a struggling
    /// receiver that is asking the sender to ship smaller batches is
    /// also asking it to stop pipelining, so the two throttles compose.
    /// A <see cref="ReplicationAck.PauseForMs"/> hint is honoured
    /// independently by the retry-deadline gate in
    /// <see cref="ProcessNextPhaseAsync"/>, which short-circuits the
    /// whole tick before this method runs.
    /// </para>
    /// </summary>
    private async Task PumpOnceAsync(CancellationToken cancellationToken)
    {
        var options = _optionsMonitor.Get(_treeName);

        var window = Math.Max(1, options.ShipMaxInFlight);
        // Receiver flow-control: a non-null SuggestedBatchSize hint
        // collapses the pipeline back toward serial. The receiver only
        // stamps a hint when it wants the sender to slow down; honouring
        // it by dropping to a window of one keeps the in-flight depth
        // gauge truthful and stops the sender saturating a struggling
        // receiver. A null hint (the default / re-acceleration signal)
        // restores the configured window on the next tick.
        if (window > 1 && _receiverSuggestedBatchSize is not null)
        {
            window = 1;
        }

        // Content-hash payload elision composes with the bounded-pipelining
        // window: the per-batch manifest exchange runs inline in the drain
        // loop before each batch ships, so it no longer forces the window
        // back to one. A fully-elided batch advances the durable cursor
        // in-order through the same FIFO in-flight queue via a synthetic
        // completed ack, preserving per-origin FIFO and atomic-batch
        // boundaries. Elision is opt-in and gated on the content-hash dedup
        // master switch, so the default path is unaffected.

        if (window == 1)
        {
            await PumpSerialOnceAsync(options, cancellationToken);
            return;
        }

        await PumpPipelinedOnceAsync(options, window, cancellationToken);
    }

    /// <summary>
    /// Computes the effective per-tick batch-size cap for this pump tick,
    /// composing the three independent throttles as a minimum so each one
    /// can only ever lower the cap:
    /// <list type="number">
    ///   <item>the configured <see cref="LatticeReplicationOptions.ShipBatchSize"/>
    ///   ceiling (floored at <c>1</c>);</item>
    ///   <item>the sender-side adaptive batch-size controller's current
    ///   size, when <see cref="LatticeReplicationOptions.AdaptiveBatchSizingEnabled"/>
    ///   is on;</item>
    ///   <item>any active receiver flow-control hint
    ///   (<see cref="ReplicationAck.SuggestedBatchSize"/>), which remains the
    ///   hard upper bound and always wins.</item>
    /// </list>
    /// The result is <c>min(adaptive, receiver-suggested, ShipBatchSize)</c>,
    /// floored at <c>1</c>. When adaptive sizing is off and no receiver hint
    /// is active this returns exactly <c>max(1, ShipBatchSize)</c> - the
    /// byte-identical static path.
    /// </summary>
    private int ComputeMaxPerBatch(LatticeReplicationOptions options)
    {
        var configuredMax = Math.Max(1, options.ShipBatchSize);
        var maxPerBatch = configuredMax;

        if (options.AdaptiveBatchSizingEnabled)
        {
            maxPerBatch = Math.Min(maxPerBatch, GetOrCreateAdaptiveController(options).CurrentBatchSize);
        }

        // Receiver flow-control hint is the hard ceiling and always wins.
        // A non-null, strictly-positive hint clamps the cap; min(...) is
        // commutative so its position in the composition does not matter.
        if (_receiverSuggestedBatchSize is { } suggested && suggested > 0)
        {
            maxPerBatch = Math.Min(maxPerBatch, Math.Max(1, suggested));
        }

        return Math.Max(1, maxPerBatch);
    }

    /// <summary>
    /// Returns the activation-scoped adaptive batch-size controller,
    /// creating it on first use from the current per-tree options. Only
    /// called when <see cref="LatticeReplicationOptions.AdaptiveBatchSizingEnabled"/>
    /// is on.
    /// </summary>
    private AdaptiveBatchSizeController GetOrCreateAdaptiveController(LatticeReplicationOptions options) =>
        _adaptiveController ??= new AdaptiveBatchSizeController(
            maxBatchSize: Math.Max(1, options.ShipBatchSize),
            additiveIncrement: options.AdaptiveBatchIncrement,
            multiplicativeDecreaseFactor: options.AdaptiveBatchDecreaseFactor,
            latencyThreshold: options.AdaptiveBatchLatencyThreshold,
            windowLength: options.AdaptiveBatchWindowLength);

    /// <summary>
    /// Records the per-batch adaptive-sizing observability (the effective
    /// cap and the measured ack latency) and, when adaptive sizing is on,
    /// feeds the latency sample to the AIMD controller so its additive
    /// increase / multiplicative decrease fires on the next tick. Called
    /// once per acknowledged batch on both the serial and pipelined paths;
    /// not called for liveness probes.
    /// </summary>
    private void OnShipAckObserved(LatticeReplicationOptions options, int effectiveBatchSize, TimeSpan ackLatency)
    {
        LatticeReplicationMetrics.ShipEffectiveBatchSize.Record(
            effectiveBatchSize,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
        LatticeReplicationMetrics.ShipAckLatency.Record(
            ackLatency.TotalMilliseconds,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));

        if (options.AdaptiveBatchSizingEnabled)
        {
            GetOrCreateAdaptiveController(options).RecordAck(ackLatency);
        }
    }

    /// <summary>
    /// Feeds a failed peer round-trip (transport throw or ack rejection)
    /// to the adaptive batch-size controller so its multiplicative
    /// decrease fires. No-op when adaptive sizing is off. Local drain
    /// failures are not peer faults and must not be reported here, matching
    /// the per-peer error-tally semantics of <see cref="ApplyBackoff"/>.
    /// </summary>
    private void OnShipErrorObserved(LatticeReplicationOptions options)
    {
        if (options.AdaptiveBatchSizingEnabled)
        {
            GetOrCreateAdaptiveController(options).RecordError();
        }
    }

    /// <summary>
    /// Primes the per-tick partition merge state once, then drains and ships
    /// successive batches back-to-back until the WAL tail is exhausted for
    /// this tick, applying producer-side filters, calling the transport,
    /// advancing the cursor on each positive ack, and applying backoff on
    /// transient failure. Schema-shaped failures during encode park every
    /// offending entry on the per-tree dead-letter queue (reason
    /// <see cref="LatticeReplicationMetrics.ReasonSchema"/>) and then advance
    /// the cursor past the batch so a single poison entry never stalls the
    /// stream forever; operators inspect / replay / discard via
    /// <see cref="ILatticeReplicationDeadLetters"/>.
    /// <para>
    /// This is the strict-serial path: ship one batch, await its ack, advance
    /// the cursor, then immediately carve and ship the next from the same
    /// primed pages - a single prime amortised across every batch the tick
    /// ships. It stops early only when a batch comes up short (the WAL tail is
    /// reached) or the receiver applies flow control (a shrink hint or a
    /// pause). It runs whenever the effective pipelining window is one (the
    /// default <see cref="LatticeReplicationOptions.ShipMaxInFlight"/> of
    /// <c>1</c>, or a higher configured window collapsed by receiver
    /// flow-control).
    /// </para>
    /// </summary>
    private async Task PumpSerialOnceAsync(LatticeReplicationOptions options, CancellationToken cancellationToken)
    {
        // Effective per-tick cap = min(adaptive, receiver hint, ShipBatchSize),
        // floored at 1. The receiver flow-control hint is the hard ceiling
        // and always wins; the adaptive controller only operates in the
        // headroom beneath it. With adaptive sizing off and no active hint
        // this is byte-identical to the static path's max(1, ShipBatchSize).
        var maxPerBatch = ComputeMaxPerBatch(options);

        // Prime each partition's shipping page once for the whole tick, then
        // carve and ship successive batches back-to-back until the WAL tail is
        // drained (or the receiver applies flow control). Historically this
        // path shipped exactly one batch per phase-timer tick and returned, so
        // a backlog larger than one batch drained at only one batch per
        // ShipPhaseTimerPeriod - leaving multi-second gaps between batches
        // arriving at the peer under a slow storage tier, even though the
        // receiver had ample headroom and more entries were already durable.
        // Draining within the tick (mirroring the bounded-pipelining path's
        // single-prime, many-batch structure) closes those gaps and amortises
        // the per-tick partition prime across every batch it feeds. The drain
        // reuses the same activation-scoped scratch buffers each iteration, so
        // it allocates nothing per batch. Per-batch acks still advance and
        // (deferred-)persist the durable cursor in strict HLC order exactly as
        // before, so crash-replay bounds are unchanged.
        try
        {
            await InitializeDrainTickAsync(options, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Drain failure is transient by definition - back off and retry.
            ApplyBackoff(options, ex, "drain");
            return;
        }

        // Wire-version capability negotiation and shared-dictionary
        // convergence / negotiation run once per tick (before the first
        // batch), mirroring the bounded-pipelining path: every batch the drain
        // loop ships this tick encodes at the negotiated version / dictionary.
        // Fail fast when the peer is older than the sender's minimum-supported
        // floor. All three are no-ops when their options are off, so the
        // default path is byte-identical to before.
        if (options.WireVersionNegotiationEnabled && !TryNegotiateWireVersion(options))
        {
            return;
        }
        await MaybeConvergeSharedDictionariesAsync(options, cancellationToken);
        TryNegotiateSharedDictionary(options);

        var shippedAny = false;
        while (true)
        {
            try
            {
                await MergeOneBatchAsync(options, maxPerBatch, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                ApplyBackoff(options, ex, "drain");
                return;
            }

            if (_drainBuffer.Count == 0)
            {
                // WAL tail exhausted for this tick. Fold any consumed-but-
                // filtered partition cursors. The drain may have consumed (and
                // advanced _partitionMaxReadSeq over) a run of entries that
                // ShouldShip rejected - on a receiver cluster the WAL tail of a
                // peer-authored tree is entirely foreign-origin, so every
                // drained entry is filtered and the ship buffer ends empty.
                // Without folding those cursors here the durable partition
                // cursor never advances past the foreign suffix and the next
                // pump re-reads it - and it grows with every peer write - from
                // WAL storage on every tick.
                //
                // When nothing shipped this tick the two idle-only
                // responsibilities also run:
                //
                //   1. Force a deferred cursor flush if the time dimension
                //      (ShipCursorWriteMaxDelay) has elapsed. A stream that
                //      shipped a partial batch and then quiesced would
                //      otherwise leave its last advances un-flushed until the
                //      next advance - which may never come on an idle link -
                //      keeping the crash-replay window open and pinning the
                //      WAL GC trim frontier. The empty-drain tick is the only
                //      place the time dimension can fire when no new acks are
                //      arriving.
                //   2. Consider firing an empty liveness probe so the outbound
                //      peer.last_contact_seconds gauge does not climb unbounded
                //      on a healthy idle link. The probe rides the same
                //      transport ack contract as a normal batch; the receiver
                //      sees a zero-entry envelope and acks immediately.
                //      Preserves any accumulated backoff by short-circuiting
                //      when one is in flight.
                await FoldFilteredOnlyConsumedCursorsAsync(options, cancellationToken);
                if (!shippedAny)
                {
                    await TryFlushPendingCursorOnIdleAsync(options, cancellationToken);
                    await TryEmitLivenessProbeAsync(options, cancellationToken);
                }
                return;
            }

            // Whether this batch filled the cap is captured *before* the ship
            // body runs any content-hash elision (which can shrink the drain
            // buffer): a full batch means the WAL may hold more and the loop
            // keeps draining; a short batch means the tail is exhausted for
            // this tick.
            var hitBatchCap = _drainBuffer.Count >= maxPerBatch;

            if (await ShipMergedSerialBatchAsync(options, maxPerBatch, cancellationToken))
            {
                // Hard stop for this tick: a transient failure parked a backoff
                // or the receiver asked the sender to pause / shrink via flow
                // control. The next pump tick re-evaluates.
                return;
            }
            shippedAny = true;
            if (!hitBatchCap)
            {
                // Short batch shipped: WAL tail drained for this tick.
                return;
            }
        }
    }

    /// <summary>
    /// Ships the single already-merged batch currently held in
    /// <see cref="_drainBuffer"/> down the strict-serial path: optional
    /// content-hash elision, framing-header encode, transport send, ack
    /// handling, and durable cursor advance. Assumes the caller has already
    /// primed the tick (<see cref="InitializeDrainTickAsync"/>), negotiated
    /// the wire version / shared dictionary once, and carved this batch
    /// (<see cref="MergeOneBatchAsync"/>). Reuses the activation-scoped scratch
    /// buffers, so it allocates nothing per batch beyond the per-page DTOs the
    /// WAL grain returns. Returns <see langword="true"/> when the caller's
    /// drain loop must stop for this tick (a transient failure parked a
    /// backoff, or the receiver requested a pause / smaller batch via flow
    /// control) and <see langword="false"/> when the batch was handled and the
    /// loop may carve and ship the next batch.
    /// </summary>
    private async Task<bool> ShipMergedSerialBatchAsync(
        LatticeReplicationOptions options,
        int maxPerBatch,
        CancellationToken cancellationToken)
    {
        var sourceHlc = _drainBuffer[^1].Timestamp;

        // Content-hash payload elision (opt-in, default off). When enabled
        // and the peer can perform the exchange, advertise a per-entry
        // content-hash manifest, learn which entries the receiver is
        // missing, and drop the payloads it already holds byte-identical
        // (the receiver advances its high-water-mark for those via a
        // metadata-only apply during the exchange, so HWM still advances
        // without the payload travelling). A no-op when the option is off
        // or the peer cannot perform the exchange: the full batch ships
        // verbatim exactly as today. Runs after the drain captured
        // sourceHlc above, so the cursor still advances past every
        // originally-drained entry regardless of how many were elided.
        await TryElideViaManifestExchangeAsync(options, cancellationToken);
        if (_drainBuffer.Count == 0)
        {
            // Every drained entry was elided (the receiver already held all
            // of them and advanced its HWM via the exchange). Advance the
            // sender cursor past the drained range and finish the tick
            // without shipping an empty batch.
            await AdvanceCursorAsync(sourceHlc, options, cancellationToken);
            state.State.ConsecutiveFailures = 0;
            _nextRetryAtUtc = DateTime.MinValue;
            _peerStats.RecordSuccess(_treeName, _peerClusterId);
            _lastSuccessfulContactUtc = DateTime.UtcNow;
            return false;
        }

        // Build the framing-only EncodedEnvelope. The drain has
        // already populated _drainEncodedSegments with the
        // pre-encoded WAL entry payloads (the bytes the canonical
        // IWalRecordEncoder wrote at append time); we wrap them in a
        // fixed-size header and hand them to the framing-aware
        // transport verbatim. No producer-side
        // IReplicationBatchEncoder.Encode call runs on the steady-
        // state ship path - the bytes the WAL already wrote are
        // reused exactly once on the wire, achieving the
        // one-encode-per-entry target end to end.
        ReplicationBatchEncodedEnvelope encodedEnvelope;
        try
        {
            var (framingCompression, framingDictionaryId) = ResolveFramingCompression(options);
            var header = new EncodedBatchHeader
            {
                Magic = EncodedBatchHeader.MagicValue,
                WireVersion = options.WireVersionNegotiationEnabled
                    ? _negotiatedWireVersion
                    : EncodedBatchHeader.CurrentWireVersion,
                OriginClusterIdHash = EncodedBatchHeader.HashClusterId(options.ClusterId),
                EntryCount = _drainEncodedSegments.Count,
                BatchSequence = 0,
                // Hoist Mode from per-entry bytes since wire version
                // 5: the receiver re-stamps every decoded entry with
                // header.Mode on the apply path. Resolve via the
                // injected ILatticeMergeModeResolver; null (tree not
                // declared replicated) collapses to LwwRegister, which
                // matches both the producer-side WAL writer's stamp
                // and the wire-baseline default of pre-Mode-hoist receivers.
                Mode = _modeResolver.Resolve(_treeName) ?? LatticeMergeMode.LwwRegister,
                // Framing-tail compression and shared-dictionary id are
                // resolved together by ResolveFramingCompression: it
                // honours the threshold/algorithm exactly as before, and
                // applies per-peer shared-dictionary negotiation when the
                // option is on (falling back to dictionary-less Zstd for a
                // peer that has not advertised the configured id). When
                // negotiation is off the result is byte-identical to the
                // prior inline computation.
                Compression = framingCompression,
                DictionaryId = framingDictionaryId,
            };
            // CollectionsMarshal.AsSpan(...) of List<T> exposes the
            // List's backing array as a contiguous Memory<T>; we copy
            // into an activation-scoped scratch array (resized
            // lazily, never shrunk) so the steady-state ship path
            // allocates nothing beyond the per-page DTOs the WAL
            // grain returns. The receiver-side framing decode does
            // not retain the segments past the surrounding SendAsync
            // call.
            var count = _drainEncodedSegments.Count;
            if (_encodedEnvelopeScratch.Length < count)
            {
                Array.Resize(ref _encodedEnvelopeScratch, count);
            }
            var src = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_drainEncodedSegments);
            src.CopyTo(_encodedEnvelopeScratch.AsSpan(0, count));
            encodedEnvelope = new ReplicationBatchEncodedEnvelope
            {
                Header = header,
                EncodedEntries = new ReadOnlyMemory<ArraySegment<byte>>(_encodedEnvelopeScratch, 0, count),
            };
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            // Schema-shaped failure during framing-header construction:
            // the entries can never be shipped in their current form.
            // Park every entry in the offending batch on the per-tree
            // DLQ tagged ReasonSchema and advance the cursor past the
            // batch so the stream makes progress. Operators inspect /
            // replay / discard via ILatticeReplicationDeadLetters.
            Logger.LogWarning(ex,
                "Encode failed for {EntryCount}-entry batch on {Context}; routing to DLQ and advancing cursor to {Hlc}",
                _drainBuffer.Count, LogContext, sourceHlc);
            await RouteBatchToDeadLetterAsync(ex, cancellationToken);
            await AdvanceCursorAsync(sourceHlc, options, cancellationToken);
            return false;
        }

        ReplicationAck ack;
        TimeSpan ackLatency;
        try
        {
            var batch = new ReplicationBatch
            {
                TargetClusterId = _peerClusterId,
                TreeName = _treeName,
                OriginClusterId = options.ClusterId,
                // Payload is empty on the framing path - the
                // transport consumes EncodedEnvelope. Bytes-only
                // transports that need a serialised form are not
                // supported on the steady-state ship path; the
                // typed-envelope sender path was retired alongside
                // the framing-only ship-path migration.
                Payload = ReadOnlyMemory<byte>.Empty,
                // Envelope is left null on the steady-state ship
                // path. The slot is preserved on ReplicationBatch
                // for in-process loopback transports that already
                // hold a typed envelope, but the shipper itself no
                // longer materialises one - the framing path is
                // unconditional.
                Envelope = null,
                // Pre-encoded entry segments for the framing
                // transport. Borrowed from the per-tick shipping
                // pages; safe for synchronous consumption inside
                // the SendAsync call because Orleans serialises
                // grain turns and SendAsync awaits inline.
                EncodedEnvelope = encodedEnvelope,
            };
            // Measure ack latency for the ship.ack_latency histogram and
            // the adaptive controller. Stopwatch.GetElapsedTime is
            // allocation-free and monotonic.
            var sendStartTimestamp = Stopwatch.GetTimestamp();
            ack = await _transport.SendAsync(batch, cancellationToken);
            ackLatency = Stopwatch.GetElapsedTime(sendStartTimestamp);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            OnShipErrorObserved(options);
            ApplyBackoff(options, ex, "transport");
            return true;
        }

        if (!ack.Accepted)
        {
            // Receiver rejected the batch; treat as transient (the
            // sender's cursor stays put and we retry after backoff).
            OnShipErrorObserved(options);
            ApplyBackoff(options, exception: null, reason: "ack-rejected");
            return true;
        }

        // Trust the receiver's ack frontier. A receiver that fully
        // applied the batch returns the highest entry HLC; a receiver
        // that partially applied returns the partial frontier; a
        // receiver that fully deduped returns a frontier at or below
        // ours and we fall back to sourceHlc below to make progress.
        var advancedTo = ack.HighestAppliedHlc;
        if (advancedTo <= state.State.Cursor)
        {
            // Receiver acknowledged a frontier at or below ours
            // (e.g. every entry was deduped). Still resets the
            // backoff because the round-trip succeeded; advance the
            // cursor to the last shipped entry's HLC so we don't
            // re-ship the same batch next tick.
            advancedTo = sourceHlc;
        }

        await AdvanceCursorAsync(advancedTo, options, cancellationToken);
        // Successful round-trip resets the backoff counter.
        state.State.ConsecutiveFailures = 0;
        _nextRetryAtUtc = DateTime.MinValue;

        // Receiver-side flow control: stash the receiver's
        // SuggestedBatchSize for the next pump tick's cap, and apply
        // any requested PauseForMs by extending (never shortening)
        // the per-peer retry deadline. PauseForMs composes with the
        // shipper's existing exponential-backoff retry budget via
        // max(currentBackoffDeadline, now + PauseForMs); because the
        // success path just cleared _nextRetryAtUtc to MinValue, the
        // composition collapses to "now + PauseForMs" on the steady-
        // state success path, and to "max(...)" only when a late
        // pause races a still-in-flight backoff.
        _receiverSuggestedBatchSize = ack.SuggestedBatchSize;
        // Capture the peer's advertised wire-version capability for the
        // next tick's negotiation. Harmless when negotiation is off
        // (the value is simply never read).
        _peerWireVersion = ack.SupportedWireVersion;
        // Capture the peer's advertised shared-dictionary capability for
        // the next tick's dictionary negotiation. Harmless when
        // dictionary negotiation is off (the value is never read).
        _peerAdvertisedDictionaryIds = ack.AdvertisedDictionaryIds;
        _peerAdvertisedDictionaries = ack.AdvertisedDictionaries;
        if (ack.PauseForMs is { } pauseMs && pauseMs > 0)
        {
            var requested = DateTime.UtcNow.AddMilliseconds(pauseMs);
            if (requested > _nextRetryAtUtc)
            {
                _nextRetryAtUtc = requested;
            }
        }

        // Per-peer telemetry. RecordSuccess clears the consecutive-error
        // counter and stamps the last-contact timestamp; RecordBacklog
        // updates the entries_behind / bytes_behind gauges. The backlog
        // reading is a *lower bound* derived from this tick's drain
        // outcome: when the drain hit ShipBatchSize the WAL had at
        // least one batch worth of entries past our cursor, so we
        // report the just-shipped count and bytes as a floor; when
        // the drain returned fewer than the cap we know we caught up
        // and report zero. This avoids a hot-path WAL frontier query
        // (one extra grain call per partition per tick) while still
        // making "is this peer keeping up?" answerable on the dashboard.
        //
        // bytes_behind sums the pre-encoded entry segment lengths
        // (already counted into _drainEncodedByteCount during the
        // drain) - the same bytes that just travelled the wire.
        _peerStats.RecordSuccess(_treeName, _peerClusterId);
        _lastSuccessfulContactUtc = DateTime.UtcNow;
        var hitBatchCap = _drainBuffer.Count >= maxPerBatch;
        var entriesBehind = hitBatchCap ? (long)_drainBuffer.Count : 0L;
        var bytesBehind = hitBatchCap ? _drainEncodedByteCount : 0L;
        _peerStats.RecordBacklog(_treeName, _peerClusterId, entriesBehind, bytesBehind);

        // Adaptive batch sizing: record the effective cap and the measured
        // ack latency, and feed the latency to the AIMD controller (when
        // enabled) so it grows / backs off on the next tick.
        OnShipAckObserved(options, maxPerBatch, ackLatency);

        // Stop the drain loop for this tick when the receiver applied flow
        // control: a strictly-positive SuggestedBatchSize (shrink hint, the
        // same condition ComputeMaxPerBatch treats as an active ceiling - a
        // null or zero hint means "re-accelerate" and must not halt the drain)
        // or a PauseForMs that pushed the per-peer retry deadline into the
        // future. The success path above cleared _nextRetryAtUtc to MinValue,
        // so a future deadline here can only be a pause just applied.
        // Otherwise report no stop so the caller ships the next batch
        // back-to-back.
        return (_receiverSuggestedBatchSize is { } suggested && suggested > 0)
            || _nextRetryAtUtc > DateTime.UtcNow;
    }

    /// <summary>
    /// Producer-side filter: applies <see cref="LatticeReplicationOptions.KeyFilter"/> /
    /// <see cref="LatticeReplicationOptions.KeyPrefixes"/> and the
    /// durable origin-based cycle-break (skip entries whose
    /// <see cref="WalRecord.OriginClusterId"/> matches the peer's
    /// own cluster id). Also drops entries whose
    /// <see cref="WalRecord.OriginClusterId"/> is null or empty - these
    /// are durability-only WAL appends authored by the core
    /// <c>ICommitLogWriter</c> path on the same per-tree shard the
    /// replication observer ships from, and have no defined origin for
    /// the receiver's per-origin high-water-mark dedup path. The
    /// replication observer fires alongside the durability writer on
    /// every commit and stamps a non-empty origin onto its own append,
    /// so the corresponding stamped entry is what propagates to peers.
    /// </summary>
    private bool ShouldShip(WalRecord entry, LatticeReplicationOptions options)
    {
        // Skip durability-only entries with no replication origin. The
        // receiver's per-origin HWM dedup path requires a non-empty
        // OriginClusterId; shipping them would surface as ArgumentException
        // and dead-letter every such entry on every pump tick.
        if (string.IsNullOrEmpty(entry.OriginClusterId))
        {
            return false;
        }

        // Tombstone-reap envelopes are emitted by the per-leaf
        // `CompactTombstonesAsync` path to durably record a local
        // structural cleanup (physically remove a tombstone or expired
        // entry whose grace period has elapsed). They carry
        // `MutationKind.Tombstone`, are tagged
        // `MutationCategory.Maintenance` via the producer-side
        // `LatticeMaintenanceContext` scope, and have no defined
        // receiver-side semantics: every peer cluster runs its own
        // compaction pass against its own copy of the data and reaps
        // independently when its local grace period elapses. Shipping
        // them would (a) generate apply-side failures because
        // `ReplicationApplier` has no `MutationKind.Tombstone` apply
        // rule registered, (b) pollute the per-origin HWM with marks
        // that never advance user-visible state, and (c) inflate every
        // peer's apply traffic with envelopes that produce no semantic
        // change. Skip them at the shipper boundary. The category
        // signal is not preserved through `WalRecord` (no Category
        // slot), so the filter keys on `Op` directly.
        if (entry.Op == MutationKind.Tombstone)
        {
            return false;
        }

        // Cycle-break: only ship entries authored by the *local*
        // cluster. Under the WAL-as-sole-durability-boundary contract,
        // the per-shard WAL also captures entries installed by
        // `IReplicationApplier` on this cluster - those entries stamp
        // `OriginClusterId` with the *source* cluster id (set by
        // `LatticeOriginContext.With(originClusterId)` inside
        // `LatticeGrain.ApplySetAsync` / `ApplyDeleteAsync` /
        // `ApplyDeleteRangeAsync`). Without this filter, a three-way
        // topology (A authors -> ships to B; B applies, WAL-appends,
        // and re-ships the apply-installed entry to C) would re-route
        // A-origin writes back through B's outbound pipeline, breaking
        // the producer-side "ship this cluster's authored writes only"
        // contract and inflating apply traffic everywhere. Restricting
        // the shipper to local-origin entries subsumes the older
        // "don't ship a peer its own writes back" rule because
        // `_peerClusterId != options.ClusterId` is a wire-shape
        // invariant on every replication peer.
        if (!string.Equals(entry.OriginClusterId, options.ClusterId, StringComparison.Ordinal))
        {
            return false;
        }

        // Saga terminal-mark records carry Key=ShardIndex.ToString()
        // (an internal shard-routing token, not a user key) and never
        // match a user-supplied KeyFilter / KeyPrefixes filter. Bypass
        // those filters for terminals so cross-cluster atomic
        // visibility delivers the linearization point even on trees
        // with restrictive key filters. The receiver-side
        // ApplyTxTerminalAsync is idempotent on duplicate delivery
        // (per-leaf _recentlyTerminal HashSet + registry
        // repeat-same-outcome no-op), so shipping every terminal that
        // applies to this peer is safe.
        if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort)
        {
            return true;
        }

        if (options.KeyFilter is { } filter)
        {
            if (entry.Key is null || !filter(entry.Key))
            {
                return false;
            }
        }

        if (options.KeyPrefixes is { } prefixes && prefixes.Count > 0)
        {
            if (entry.Key is null)
            {
                return false;
            }
            var matched = false;
            foreach (var prefix in prefixes)
            {
                if (entry.Key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    matched = true;
                    break;
                }
            }
            if (!matched)
            {
                return false;
            }
        }

        return true;
    }

    private async Task AdvanceCursorAsync(
        HybridLogicalClock newCursor,
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        // Mutate in-memory state up front so the next pump tick within
        // this activation resumes from the latest known-good cursor
        // even if the durable write is deferred.
        var hlcAdvanced = newCursor.CompareTo(state.State.Cursor) > 0;
        var partitionsAdvanced = AdvancePartitionCursorsInState();

        if (!hlcAdvanced && !partitionsAdvanced)
        {
            return;
        }

        if (hlcAdvanced)
        {
            state.State.Cursor = newCursor;
        }

        _pendingCursorWrites++;
        if (_pendingCursorWrites == 1)
        {
            // First un-flushed advance since the last flush - anchor the
            // time-dimension countdown. Subsequent advances inside the
            // same window leave this anchor in place so the elapsed check
            // measures from the oldest pending write, not the newest.
            _oldestPendingCursorWriteUtc = _cursorFlushClock.GetUtcNow().UtcDateTime;
        }

        var interval = Math.Max(1, options.ShipCursorWriteInterval);
        if (_pendingCursorWrites < interval && !CursorWriteMaxDelayElapsed(options))
        {
            // Defer the durable write. Receiver-side apply is
            // HLC-monotonic and dedupes on (originClusterId, originHlc),
            // so a silo crash inside this window replays at most
            // (interval × ShipBatchSize) entries - the receiver no-ops
            // the duplicates. The WAL GC's view of this peer is
            // pinned at the last reported cursor (_lastReportedCursor)
            // until the flush completes, so the trim frontier never
            // exceeds the durably-recoverable point. The time dimension
            // (ShipCursorWriteMaxDelay) forces a flush before the count
            // threshold on a low-throughput stream so the window cannot
            // stay open indefinitely while the stream is quiet.
            return;
        }

        await FlushCursorAsync(cancellationToken);
        _ = options; // Reserved for future per-tree report flavours.
    }

    /// <summary>
    /// Folds the partition read cursors after a drain that consumed entries
    /// the merge filtered out before they reached the ship buffer - entries a
    /// receiver cluster must never ship back (foreign <c>OriginClusterId</c>,
    /// see <see cref="ShouldShip"/>), entries at or below the HLC cursor, or
    /// key-filtered entries. The merge advances
    /// <see cref="_partitionMaxReadSeq"/> / <see cref="_partitionAdvanced"/>
    /// for every entry it consumes, but when the whole drained window is
    /// filtered the ship buffer ends empty and the pump bails before
    /// <see cref="AdvanceCursorAsync"/> runs, so the durable partition cursor
    /// never moves past the filtered suffix. On an actively-replicated tree
    /// that suffix grows every time the peer ships another write, so the next
    /// pump re-reads an ever-larger foreign range from WAL storage on every
    /// tick - the receiver-side idle read storm. Advancing only the partition
    /// cursors (the HLC ship cursor stays put - nothing was shipped) lets the
    /// next pump skip the already-consumed range.
    /// <para>
    /// Allocation-aware: the <see cref="AnyPartitionAdvanced"/> guard returns a
    /// cached completed task on a genuinely idle tail (the merge consumed
    /// nothing), so the async <see cref="AdvanceCursorAsync"/> state machine is
    /// entered only when there is a real filtered-only advance to fold; that
    /// method mutates in-memory state and schedules the existing deferred
    /// durable write without allocating.
    /// </para>
    /// </summary>
    private Task FoldFilteredOnlyConsumedCursorsAsync(
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
        => AnyPartitionAdvanced()
            ? AdvanceCursorAsync(state.State.Cursor, options, cancellationToken)
            : Task.CompletedTask;

    /// <summary>
    /// Whether the current tick's merge consumed at least one entry from any
    /// partition (<see cref="_partitionAdvanced"/>). Used to skip the
    /// filtered-only cursor fold on a genuinely idle tail without entering an
    /// async state machine. Allocation-free.
    /// </summary>
    private bool AnyPartitionAdvanced()
    {
        for (var p = 0; p < _partitionCount; p++)
        {
            if (_partitionAdvanced[p])
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Whether the wall-clock time dimension of the cursor-write
    /// coalescing rule has elapsed - i.e. at least one cursor advance is
    /// pending and more than
    /// <see cref="LatticeReplicationOptions.ShipCursorWriteMaxDelay"/> has
    /// passed since the oldest un-flushed advance was booked. Returns
    /// <see langword="false"/> when the max delay is
    /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> (time
    /// dimension disabled) or when no write is pending.
    /// </summary>
    private bool CursorWriteMaxDelayElapsed(LatticeReplicationOptions options)
    {
        if (_pendingCursorWrites == 0)
        {
            return false;
        }
        var maxDelay = options.ShipCursorWriteMaxDelay;
        if (maxDelay == System.Threading.Timeout.InfiniteTimeSpan)
        {
            return false;
        }
        var elapsed = _cursorFlushClock.GetUtcNow().UtcDateTime - _oldestPendingCursorWriteUtc;
        return elapsed >= maxDelay;
    }

    /// <summary>
    /// Flushes a pending deferred cursor write on an idle pump tick when
    /// the wall-clock time dimension of the coalescing rule
    /// (<see cref="LatticeReplicationOptions.ShipCursorWriteMaxDelay"/>)
    /// has elapsed. No-op when nothing is pending, when the time
    /// dimension is disabled, or when the max delay has not yet elapsed.
    /// This is the seam that lets a stream which quiesces below the
    /// <see cref="LatticeReplicationOptions.ShipCursorWriteInterval"/>
    /// batch-count threshold still checkpoint within the configured
    /// time bound.
    /// </summary>
    private async Task TryFlushPendingCursorOnIdleAsync(
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        if (!CursorWriteMaxDelayElapsed(options))
        {
            return;
        }
        await FlushCursorAsync(cancellationToken);
    }

    /// <summary>
    /// Persists <c>state</c> via
    /// <c>WriteStateAsync</c> and then
    /// (only on success) reports the durable HLC cursor to the
    /// registry. Idempotent - safe to call when no in-memory advance
    /// is pending. The persistence-then-report ordering is
    /// load-bearing: the WAL GC consumes the reported cursor to
    /// compute the trim frontier, so reporting before persistence
    /// would risk trimming entries we cannot recover after a crash.
    /// </summary>
    private async Task FlushCursorAsync(CancellationToken cancellationToken)
    {
        if (_pendingCursorWrites == 0)
        {
            return;
        }

        await state.WriteStateAsync();
        _pendingCursorWrites = 0;
        _oldestPendingCursorWriteUtc = DateTime.MinValue;

        var durableCursor = state.State.Cursor;
        if (durableCursor.CompareTo(_lastReportedCursor) <= 0)
        {
            // Only partition cursors changed since the last flush -
            // nothing new for the GC.
            return;
        }

        // Best-effort registry report: a registry-side failure does
        // not unwind the durable cursor advance. We still update
        // _lastReportedCursor so a transient registry outage does not
        // wedge the report indefinitely; the next flush re-reports
        // through the same suppression check on the next advance.
        try
        {
            await _cursorRegistry
                .ReportCursorAsync(_treeName, _peerClusterId, durableCursor, cancellationToken)
                ;
            _lastReportedCursor = durableCursor;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cursor registry report failed for {Context}; persisted cursor remains {Cursor}",
                LogContext, durableCursor);
            _lastReportedCursor = durableCursor;
        }
    }

    /// <summary>
    /// Folds the per-tick <see cref="_partitionMaxReadSeq"/> /
    /// <see cref="_partitionAdvanced"/> scratch arrays into the
    /// durable <see cref="ReplicationShipperState.PartitionCursors"/>
    /// dictionary. Returns <see langword="true"/> when at least one
    /// partition cursor actually moved forward (so the caller knows
    /// whether a <c>WriteStateAsync</c> is required).
    /// <para>
    /// Resets the scratch arrays' "advanced" flag once consumed -
    /// the next pump tick starts from a clean slate.
    /// </para>
    /// </summary>
    private bool AdvancePartitionCursorsInState()
    {
        var changed = false;
        for (var p = 0; p < _partitionCount; p++)
        {
            if (!_partitionAdvanced[p])
            {
                continue;
            }
            // _partitionMaxReadSeq[p] is the highest sequence we
            // *consumed* this tick (shipped or filtered). Resume on
            // the next tick from the entry just past it.
            var nextSeq = _partitionMaxReadSeq[p] + 1;
            // Idempotent: only advance the durable cursor when the
            // computed next-seq is strictly greater than what's
            // already there. (Guards against a degenerate case where
            // _partitionAdvanced[p] flips true but no entry was
            // actually consumed past the prior cursor - should not
            // happen given the merge-loop semantics, but the guard
            // is cheap and removes a sharp edge.)
            if (state.State.PartitionCursors.TryGetValue(p, out var existing) && existing >= nextSeq)
            {
                continue;
            }
            state.State.PartitionCursors[p] = nextSeq;
            changed = true;
            // Reset the per-tick flag; _partitionMaxReadSeq stays as
            // the last value (fine - it gets overwritten on the next
            // consume from this partition).
            _partitionAdvanced[p] = false;
        }
        return changed;
    }

    /// <summary>
    /// Folds an explicit per-partition consumed-sequence snapshot into
    /// the durable <see cref="ReplicationShipperState.PartitionCursors"/>
    /// dictionary. Used by the bounded-pipelining path, which snapshots
    /// <see cref="_partitionMaxReadSeq"/> / <see cref="_partitionAdvanced"/>
    /// per batch (the shared scratch arrays accumulate across the whole
    /// tick) and folds the matching snapshot when that batch's ack
    /// lands in FIFO order. Idempotent: a partition cursor only moves
    /// forward, never back. Returns <see langword="true"/> when at
    /// least one partition cursor advanced.
    /// </summary>
    private bool FoldPartitionCursors(long[] maxReadSeq, bool[] advanced)
    {
        var changed = false;
        for (var p = 0; p < _partitionCount; p++)
        {
            if (!advanced[p])
            {
                continue;
            }
            var nextSeq = maxReadSeq[p] + 1;
            if (state.State.PartitionCursors.TryGetValue(p, out var existing) && existing >= nextSeq)
            {
                continue;
            }
            state.State.PartitionCursors[p] = nextSeq;
            changed = true;
        }
        return changed;
    }

    /// <summary>
    /// Per-batch handle held in the bounded-pipelining in-flight window.
    /// Captures the in-flight <see cref="IReplicationTransport.SendAsync"/>
    /// task plus everything needed to advance the durable cursor when
    /// the batch's ack lands in FIFO order: the batch's source HLC
    /// frontier, the per-partition consumed-sequence snapshot, and the
    /// entry / byte counts for the backlog gauges.
    /// <para>
    /// <paramref name="Elided"/> marks a batch every entry of which the
    /// receiver already held byte-identical (the content-hash manifest
    /// exchange dropped the whole batch). Such a batch ships no envelope;
    /// its <see cref="SendTask"/> is a synthetic already-completed ack so it
    /// still flows through the FIFO drain and advances the durable cursor
    /// strictly in-order, but the drain path skips the ship-specific
    /// telemetry a zero-latency synthetic ack would otherwise pollute.
    /// </para>
    /// </summary>
    private readonly record struct InFlightShipBatch(
        Task<ReplicationAck> SendTask,
        HybridLogicalClock SourceHlc,
        long[] MaxReadSeqSnapshot,
        bool[] AdvancedSnapshot,
        int EntryCount,
        long ByteCount,
        bool HitBatchCap,
        long LaunchTimestamp,
        bool Elided = false);

    /// <summary>
    /// Bounded sender-side pipelining path. Maintains a window of up to
    /// <paramref name="window"/> in-flight unacked batches per
    /// <c>(tree, peer)</c>, draining the WAL into successive
    /// strictly-ascending-HLC batches and launching each
    /// <see cref="IReplicationTransport.SendAsync"/> without awaiting it
    /// inline. Acks are consumed in strict FIFO order, and the durable
    /// cursor advances past a batch only once that batch <b>and</b>
    /// every lower-HLC batch before it have acked (advance-strictly-on-ack,
    /// no cursor hole), preserving the per-origin FIFO invariant.
    /// <para>
    /// On the first transport throw or ack rejection the window stops
    /// advancing cursors; remaining in-flight sends are observed (to
    /// avoid unobserved-task faults) but their cursors are intentionally
    /// left un-advanced. The next tick re-drains from the durable cursor
    /// and the receiver dedupes the overlap. Receiver flow-control
    /// (a <see cref="ReplicationAck.SuggestedBatchSize"/> hint) collapses
    /// the window back to one in <see cref="PumpOnceAsync"/> before this
    /// method is ever entered.
    /// </para>
    /// </summary>
    private async Task PumpPipelinedOnceAsync(
        LatticeReplicationOptions options,
        int window,
        CancellationToken cancellationToken)
    {
        // Effective per-tick cap = min(adaptive, receiver hint, ShipBatchSize),
        // floored at 1. When this path runs, no receiver SuggestedBatchSize
        // hint is active (a non-null hint collapses the window to 1 in the
        // dispatcher), so the receiver-hint term is inert here; the adaptive
        // controller can still lower the cap below the configured ceiling.
        var maxPerBatch = ComputeMaxPerBatch(options);

        try
        {
            await InitializeDrainTickAsync(options, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ApplyBackoff(options, ex, "drain");
            return;
        }

        // Wire-version capability negotiation (opt-in) for the pipelining
        // path, mirroring the serial path. Computes the version to stamp
        // against the peer's advertised capability, publishes the
        // negotiated / downgrade telemetry, and fails fast when the peer
        // is older than the sender's floor or the negotiated target is a
        // down-stamp this build cannot produce for the tree. Skipped
        // entirely when the option is off, so the batch ships at the
        // current wire version exactly as before.
        if (options.WireVersionNegotiationEnabled && !TryNegotiateWireVersion(options))
        {
            return;
        }

        // Self-distributing shared-dictionary convergence (opt-in) for the
        // pipelining path, mirroring the serial path. A no-op when off.
        await MaybeConvergeSharedDictionariesAsync(options, cancellationToken);

        // Per-peer shared-dictionary capability negotiation (opt-in),
        // mirroring the serial path. A no-op when the option is off.
        TryNegotiateSharedDictionary(options);

        var inFlight = new Queue<InFlightShipBatch>(window);
        var failed = false;
        var shippedAny = false;
        Exception? encodeFailure = null;
        long[] failedMaxReadSeq = Array.Empty<long>();
        bool[] failedAdvanced = Array.Empty<bool>();
        var failedSourceHlc = HybridLogicalClock.Zero;

        try
        {
            while (true)
            {
                try
                {
                    await MergeOneBatchAsync(options, maxPerBatch, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    ApplyBackoff(options, ex, "drain");
                    failed = true;
                    break;
                }

                if (_drainBuffer.Count == 0)
                {
                    break; // WAL drained for this tick
                }

                var entryCount = _drainBuffer.Count;
                var hitBatchCap = entryCount >= maxPerBatch;
                var sourceHlc = _drainBuffer[^1].Timestamp;
                var maxReadSnapshot = SnapshotPartitionMaxReadSeq();
                var advancedSnapshot = SnapshotPartitionAdvanced();

                // Content-hash payload elision (opt-in, default off). Runs
                // after the full-range cursor-advance inputs above were
                // captured from the pre-elision drain, so the cursor still
                // advances past every originally-drained entry regardless of
                // how many were elided - identical to the serial path. It
                // mutates the drain buffers in place and is a no-op when the
                // option is off or the peer cannot exchange, so the default
                // pipelined path is byte-identical to today. The short-batch
                // "WAL exhausted" break and the window cap below both key off
                // the pre-elision entryCount, not the survivor count.
                await TryElideViaManifestExchangeAsync(options, cancellationToken);

                if (_drainBuffer.Count == 0)
                {
                    // Every drained entry was elided: the receiver already
                    // held all of them and advanced its high-water-mark via
                    // the exchange. Ship no envelope; enqueue a synthetic
                    // already-completed successful ack so the fully-elided
                    // batch flows through the same FIFO DrainOneInFlightAsync
                    // ordering and advances the durable cursor strictly
                    // in-order (never before an earlier real in-flight batch).
                    var elidedAck = Task.FromResult(new ReplicationAck
                    {
                        Accepted = true,
                        HighestAppliedHlc = sourceHlc,
                    });
                    inFlight.Enqueue(new InFlightShipBatch(
                        elidedAck, sourceHlc, maxReadSnapshot, advancedSnapshot,
                        entryCount, 0L, hitBatchCap, Stopwatch.GetTimestamp(), Elided: true));
                    shippedAny = true;
                    _peerStats.RecordInFlight(_treeName, _peerClusterId, inFlight.Count);

                    if (inFlight.Count >= window)
                    {
                        if (!await DrainOneInFlightAsync(inFlight, options, maxPerBatch, cancellationToken))
                        {
                            failed = true;
                            break;
                        }
                    }

                    // A short batch (pre-elision) means the WAL is exhausted
                    // for this tick; stop drawing new batches.
                    if (entryCount < maxPerBatch)
                    {
                        break;
                    }

                    continue;
                }

                ReplicationBatchEncodedEnvelope encodedEnvelope;
                long byteCount;
                try
                {
                    encodedEnvelope = BuildEncodedEnvelope(options, out byteCount);
                }
                catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
                {
                    // Schema-shaped framing-header failure for this
                    // batch. Defer DLQ + cursor advance until the
                    // already-in-flight (lower-HLC) batches have acked
                    // in order, so the cursor never skips a hole. The
                    // failed batch's drain buffers are left intact for
                    // RouteBatchToDeadLetterAsync below (no further
                    // MergeOneBatchAsync runs after this break).
                    encodeFailure = ex;
                    failedMaxReadSeq = maxReadSnapshot;
                    failedAdvanced = advancedSnapshot;
                    failedSourceHlc = sourceHlc;
                    break;
                }

                Task<ReplicationAck> sendTask;
                long launchTimestamp;
                try
                {
                    var batch = new ReplicationBatch
                    {
                        TargetClusterId = _peerClusterId,
                        TreeName = _treeName,
                        OriginClusterId = options.ClusterId,
                        Payload = ReadOnlyMemory<byte>.Empty,
                        Envelope = null,
                        EncodedEnvelope = encodedEnvelope,
                    };
                    launchTimestamp = Stopwatch.GetTimestamp();
                    sendTask = _transport.SendAsync(batch, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Synchronous throw from SendAsync (before returning
                    // a task): treat as a transport failure for this
                    // batch and stop the window.
                    OnShipErrorObserved(options);
                    ApplyBackoff(options, ex, "transport");
                    failed = true;
                    break;
                }

                inFlight.Enqueue(new InFlightShipBatch(
                    sendTask, sourceHlc, maxReadSnapshot, advancedSnapshot, entryCount, byteCount, hitBatchCap, launchTimestamp));
                shippedAny = true;
                _peerStats.RecordInFlight(_treeName, _peerClusterId, inFlight.Count);

                if (inFlight.Count >= window)
                {
                    if (!await DrainOneInFlightAsync(inFlight, options, maxPerBatch, cancellationToken))
                    {
                        failed = true;
                        break;
                    }
                }

                // A short batch means the WAL is exhausted for this
                // tick; stop drawing new batches.
                if (entryCount < maxPerBatch)
                {
                    break;
                }
            }

            // Drain the remaining window in FIFO order while the round
            // trip is still healthy.
            while (!failed && inFlight.Count > 0)
            {
                if (!await DrainOneInFlightAsync(inFlight, options, maxPerBatch, cancellationToken))
                {
                    failed = true;
                    break;
                }
            }

            // Handle a deferred schema-shaped encode failure now that
            // every lower-HLC batch has acked: DLQ the offending batch
            // and advance the cursor strictly past it so a poison batch
            // never stalls the stream.
            if (!failed && encodeFailure is not null)
            {
                Logger.LogWarning(encodeFailure,
                    "Encode failed for {EntryCount}-entry batch on {Context}; routing to DLQ and advancing cursor to {Hlc}",
                    _drainBuffer.Count, LogContext, failedSourceHlc);
                await RouteBatchToDeadLetterAsync(encodeFailure, cancellationToken);
                await AdvanceCursorPipelinedAsync(
                    failedSourceHlc, failedMaxReadSeq, failedAdvanced, options, cancellationToken);
            }
        }
        finally
        {
            // Observe any still-pending sends to avoid unobserved-task
            // faults. Their cursors are intentionally NOT advanced (a
            // failure earlier in the FIFO window means we cannot know
            // whether these applied); the next tick re-ships from the
            // durable cursor and the receiver dedupes the overlap.
            while (inFlight.Count > 0)
            {
                var pending = inFlight.Dequeue();
                try
                {
                    await pending.SendTask;
                }
                catch (Exception)
                {
                    // Swallowed: cursor is not advanced for this batch.
                }
            }
            _peerStats.RecordInFlight(_treeName, _peerClusterId, 0);
        }

        // Fold any trailing consumed-but-filtered partition cursors. The final
        // merge that ended the ship loop with an empty drain buffer may have
        // consumed (and advanced _partitionMaxReadSeq over) a run of
        // foreign-origin / already-seen entries that produced no batch; without
        // folding, the durable partition cursor never advances past that suffix
        // and the next tick re-reads it from WAL storage. Runs only on the
        // fully-healthy path (no in-flight failure, no deferred encode failure)
        // so a mid-pipeline failure's cursor reset is never overridden, and
        // independently of shippedAny because a trailing filtered-only suffix
        // can follow successfully-shipped batches in the same tick. See
        // FoldFilteredOnlyConsumedCursorsAsync.
        if (!failed && encodeFailure is null)
        {
            await FoldFilteredOnlyConsumedCursorsAsync(options, cancellationToken);
        }

        // No batch ever shipped this tick (and no encode failure to
        // account for): fall back to the same idle-link liveness probe
        // the serial path emits.
        if (!failed && !shippedAny && encodeFailure is null)
        {
            await TryEmitLivenessProbeAsync(options, cancellationToken);
        }
    }

    /// <summary>
    /// Awaits the oldest in-flight batch (FIFO), and on a positive ack
    /// advances the durable cursor strictly past it via that batch's
    /// captured partition snapshot. Returns <see langword="false"/> on
    /// transport throw or ack rejection (the caller stops advancing the
    /// window); <see langword="true"/> on success.
    /// </summary>
    private async Task<bool> DrainOneInFlightAsync(
        Queue<InFlightShipBatch> inFlight,
        LatticeReplicationOptions options,
        int effectiveBatchSize,
        CancellationToken cancellationToken)
    {
        var batch = inFlight.Dequeue();
        _peerStats.RecordInFlight(_treeName, _peerClusterId, inFlight.Count);

        if (batch.Elided)
        {
            // Fully-elided batch: the receiver already held every entry and
            // advanced its high-water-mark during the manifest exchange, so
            // no envelope was ever shipped and the synthetic completed ack
            // carries zero latency. Advance the durable cursor strictly
            // in-order over the full pre-elision range (it sits behind every
            // earlier real in-flight batch in this FIFO queue) and reset the
            // per-peer failure budget, but SKIP the ship-specific telemetry a
            // zero-latency synthetic ack would pollute: no adaptive-latency
            // sample (OnShipAckObserved), no error tally, no ship-bytes
            // backlog reading, and no _receiverSuggestedBatchSize overwrite.
            // The real ManifestExchanges / ShipElidedPayloads counters were
            // already emitted inside TryElideViaManifestExchangeAsync, so the
            // elision is still observable.
            await AdvanceCursorPipelinedAsync(
                batch.SourceHlc, batch.MaxReadSeqSnapshot, batch.AdvancedSnapshot, options, cancellationToken);
            state.State.ConsecutiveFailures = 0;
            _nextRetryAtUtc = DateTime.MinValue;
            _peerStats.RecordSuccess(_treeName, _peerClusterId);
            _lastSuccessfulContactUtc = DateTime.UtcNow;
            return true;
        }

        ReplicationAck ack;
        TimeSpan ackLatency;
        try
        {
            ack = await batch.SendTask;
            ackLatency = Stopwatch.GetElapsedTime(batch.LaunchTimestamp);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            OnShipErrorObserved(options);
            ApplyBackoff(options, ex, "transport");
            return false;
        }

        if (!ack.Accepted)
        {
            OnShipErrorObserved(options);
            ApplyBackoff(options, exception: null, reason: "ack-rejected");
            return false;
        }

        var advancedTo = ack.HighestAppliedHlc;
        if (advancedTo <= state.State.Cursor)
        {
            advancedTo = batch.SourceHlc;
        }

        await AdvanceCursorPipelinedAsync(
            advancedTo, batch.MaxReadSeqSnapshot, batch.AdvancedSnapshot, options, cancellationToken);

        state.State.ConsecutiveFailures = 0;
        _nextRetryAtUtc = DateTime.MinValue;
        _receiverSuggestedBatchSize = ack.SuggestedBatchSize;
        if (ack.PauseForMs is { } pauseMs && pauseMs > 0)
        {
            var requested = DateTime.UtcNow.AddMilliseconds(pauseMs);
            if (requested > _nextRetryAtUtc)
            {
                _nextRetryAtUtc = requested;
            }
        }

        _peerStats.RecordSuccess(_treeName, _peerClusterId);
        _lastSuccessfulContactUtc = DateTime.UtcNow;
        var entriesBehind = batch.HitBatchCap ? (long)batch.EntryCount : 0L;
        var bytesBehind = batch.HitBatchCap ? batch.ByteCount : 0L;
        _peerStats.RecordBacklog(_treeName, _peerClusterId, entriesBehind, bytesBehind);

        // Adaptive batch sizing: record the effective cap and the measured
        // ack latency, and feed the latency to the AIMD controller (when
        // enabled) so it grows / backs off on the next tick.
        OnShipAckObserved(options, effectiveBatchSize, ackLatency);
        return true;
    }

    /// <summary>
    /// Builds a fresh framing-only <see cref="ReplicationBatchEncodedEnvelope"/>
    /// from the current drain buffers, copying the borrowed entry
    /// segments into a batch-owned array. Unlike the serial path's
    /// reused <see cref="_encodedEnvelopeScratch"/>, the pipelining path
    /// allocates a per-batch array because multiple envelopes are
    /// concurrently in flight - the cost of concurrency, paid only when
    /// an operator opts into a window &gt; 1. The borrowed entry bytes
    /// themselves are immutable per WAL entry, so retaining them across
    /// concurrent sends is safe.
    /// </summary>
    private ReplicationBatchEncodedEnvelope BuildEncodedEnvelope(LatticeReplicationOptions options, out long byteCount)
    {
        var count = _drainEncodedSegments.Count;
        var entries = new ArraySegment<byte>[count];
        System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_drainEncodedSegments).CopyTo(entries);
        byteCount = _drainEncodedByteCount;
        var (framingCompression, framingDictionaryId) = ResolveFramingCompression(options);
        var header = new EncodedBatchHeader
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = options.WireVersionNegotiationEnabled
                ? _negotiatedWireVersion
                : EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId(options.ClusterId),
            EntryCount = count,
            BatchSequence = 0,
            Mode = _modeResolver.Resolve(_treeName) ?? LatticeMergeMode.LwwRegister,
            Compression = framingCompression,
            DictionaryId = framingDictionaryId,
        };
        return new ReplicationBatchEncodedEnvelope
        {
            Header = header,
            EncodedEntries = entries,
        };
    }

    /// <summary>
    /// Cursor-advance variant for the bounded-pipelining path. Identical
    /// persistence semantics to <see cref="AdvanceCursorAsync"/> but
    /// folds an explicit per-batch partition snapshot rather than the
    /// shared scratch arrays (which by ack time reflect the last batch
    /// drained, not the batch being acked).
    /// </summary>
    private async Task AdvanceCursorPipelinedAsync(
        HybridLogicalClock newCursor,
        long[] maxReadSeq,
        bool[] advanced,
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        var hlcAdvanced = newCursor.CompareTo(state.State.Cursor) > 0;
        var partitionsAdvanced = FoldPartitionCursors(maxReadSeq, advanced);

        if (!hlcAdvanced && !partitionsAdvanced)
        {
            return;
        }

        if (hlcAdvanced)
        {
            state.State.Cursor = newCursor;
        }

        _pendingCursorWrites++;
        var interval = Math.Max(1, options.ShipCursorWriteInterval);
        if (_pendingCursorWrites < interval)
        {
            return;
        }

        await FlushCursorAsync(cancellationToken);
    }

    private long[] SnapshotPartitionMaxReadSeq()
    {
        var copy = new long[_partitionCount];
        Array.Copy(_partitionMaxReadSeq, copy, _partitionCount);
        return copy;
    }

    private bool[] SnapshotPartitionAdvanced()
    {
        var copy = new bool[_partitionCount];
        Array.Copy(_partitionAdvanced, copy, _partitionCount);
        return copy;
    }

    /// <summary>
    /// Initialises the per-tick partition scratch arrays from the
    /// durable per-partition resume cursors and primes one shipping
    /// page per partition. Runs exactly once per pump tick - before
    /// the first <see cref="MergeOneBatchAsync"/> call - so the
    /// bounded-pipelining path can carve multiple ordered batches out
    /// of a single primed merge state without re-seeding from the
    /// (not-yet-advanced) durable cursor between batches.
    /// </summary>
    private async Task InitializeDrainTickAsync(
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        var partitions = Math.Max(1, options.ReplogPartitions);
        var pageSize = Math.Max(1, options.ShipPartitionPageSize);

        EnsureScratchSized(partitions);

        // Rebind to the source tree's current physical identity before seeding
        // this tick's cursors. A registry alias swap (shadow-cutover restore,
        // resize, reshard) can repoint the logical tree to a new physical WAL
        // underneath a live shipper; the persisted per-partition cursors are
        // absolute offsets into the retired log, so on an identity change they
        // are reset and the shipper re-ships from the new source's log start.
        // The rebind is driven primarily by an event-driven push
        // (NotifySourceIdentityChangedAsync); this per-tick call only reads the
        // registry on the first bind or once the backstop interval has elapsed,
        // so an idle tree does not pay a registry read every tick.
        await MaybeRefreshSourceIdentityAsync(options, partitions);


        // and _partitionPageIndex always reset (they're tick-scoped);
        // _partitionNextSeq seeds from the durable cursor;
        // _partitionMaxReadSeq is initialised from the cursor minus 1
        // so a partition that contributes nothing this tick reports
        // "no advance" in AdvancePartitionCursorsInState.
        // A non-zero scalar cursor with an entirely empty PartitionCursors
        // map is the signature of a state persisted by a pre-partition-
        // cursor build: every partition resumes from sequence 0 and would
        // otherwise re-ship the whole already-shipped prefix. This is the
        // ONLY tick in which the defensive scalar-HLC drop may fire. From
        // the first saved partition cursor onward the drop is disabled for
        // every partition - including genuinely cold ones - because a cold
        // partition's unshipped entries may legitimately carry a per-leaf
        // HLC below the scalar cursor.
        _legacyCursorMigrationPending =
            state.State.Cursor != HybridLogicalClock.Zero
            && state.State.PartitionCursors.Count == 0;

        for (var p = 0; p < partitions; p++)
        {
            _partitionPages[p] = null;
            _partitionPageIndex[p] = 0;
            _partitionAdvanced[p] = false;
            _partitionHeadDecoded[p] = false;
            var seeded = state.State.PartitionCursors.TryGetValue(p, out var saved) ? saved : 0L;
            _partitionNextSeq[p] = seeded;
            _partitionMaxReadSeq[p] = seeded - 1;
        }

        // Prime each partition's page from its saved cursor. Done up
        // front so the merge loop below is allocation-free apart from
        // page refills triggered when a partition exhausts its page
        // mid-batch.
        //
        // Fan the per-partition shipping reads out concurrently rather
        // than awaiting them one-by-one. Each read is an independent
        // WAL-shard grain call that writes only its own partition's
        // scratch slot (TryRefillPartitionAsync touches index [p]
        // alone), so N partitions prime in a single read latency
        // instead of N serialized latencies. On a multi-partition tree
        // whose WAL shards are activated on a different silo this is the
        // dominant per-pump cost under a write burst: a serial prime of
        // 8 partitions pays 8 cross-silo/durable round-trips every tick
        // - even for partitions that turn out to be idle - which stalls
        // the pump for seconds and collapses steady-state throughput.
        // Issuing the reads together and awaiting once holds the grain
        // turn for one latency; Orleans keeps the activation
        // non-reentrant across the fan-out, and the scratch writes are
        // index-disjoint, so this is safe. The single-partition case
        // keeps the direct await to avoid the Task[] allocation.
        cancellationToken.ThrowIfCancellationRequested();
        if (partitions == 1)
        {
            await TryRefillPartitionAsync(0, pageSize, cancellationToken);
            return;
        }

        var primingTasks = new Task[partitions];
        for (var p = 0; p < partitions; p++)
        {
            primingTasks[p] = TryRefillPartitionAsync(p, pageSize, cancellationToken);
        }
        await Task.WhenAll(primingTasks);
    }

    /// <summary>
    /// Drains up to <paramref name="maxPerBatch"/> entries past the
    /// current (in-memory, possibly mid-page) partition resume state
    /// into <see cref="_drainBuffer"/> / <see cref="_drainEncodedSegments"/>,
    /// k-way merging by HLC ascending. Clears the drain buffers at the
    /// start so it is safe to call repeatedly within a tick: each call
    /// produces the next strictly-ascending-HLC batch, resuming exactly
    /// where the prior call left off (the partition page cursors carry
    /// over). Requires <see cref="InitializeDrainTickAsync"/> to have
    /// run first.
    /// <para>
    /// Crucially, this method does <b>not</b> reset
    /// <see cref="_partitionMaxReadSeq"/> / <see cref="_partitionAdvanced"/>:
    /// those accumulate the highest consumed sequence per partition
    /// across every batch in the tick, so the bounded-pipelining path
    /// can snapshot them per batch and fold the right cursor frontier
    /// into durable state when each batch's ack lands in order.
    /// </para>
    /// </summary>
    private async Task MergeOneBatchAsync(
        LatticeReplicationOptions options,
        int maxPerBatch,
        CancellationToken cancellationToken)
    {
        // The drain buffers are activation-scoped and reused across
        // pump ticks and (on the pipelining path) across batches within
        // a tick; Orleans serialises grain turns and the _pumpInFlight
        // guard prevents re-entry, so clearing in place is safe.
        _drainBuffer.Clear();
        _drainEncodedSegments.Clear();
        _drainEncodedByteCount = 0L;

        var partitions = _partitionCount;
        var pageSize = Math.Max(1, options.ShipPartitionPageSize);

        // K-way merge: at every step pick the partition whose head
        // entry has the smallest HLC, consume one entry from it, and
        // advance. When a partition's page is exhausted, refill from
        // the saved next-sequence; if the refill returns empty the
        // partition is "drained for this tick" and excluded from the
        // candidate set.
        while (_drainBuffer.Count < maxPerBatch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var minPartition = -1;
            HybridLogicalClock minHlc = default;

            for (var p = 0; p < partitions; p++)
            {
                var page = _partitionPages[p];
                if (page is null)
                {
                    continue; // drained this tick
                }
                if (_partitionPageIndex[p] >= page.Count)
                {
                    // Page exhausted - try to refill. We already
                    // advanced _partitionNextSeq on the prior consume
                    // so the refill picks up where we left off.
                    await TryRefillPartitionAsync(p, pageSize, cancellationToken);
                    page = _partitionPages[p];
                    if (page is null)
                    {
                        continue;
                    }
                }

                // Decode the head entry once per candidate position;
                // _partitionHeadDecoded is reset to false on consume
                // and on refill so the decoded record stays in lock
                // step with the head index.
                if (!_partitionHeadDecoded[p])
                {
                    // Re-stamp TreeId from the shipper's owning grain
                    // key: the producer's Encode stripped the slot,
                    // and the shipper drains exactly one tree per
                    // grain activation (the grain key is
                    // "<treeName>/<peerClusterId>"), so _treeName is
                    // the authoritative source of the tree id.
                    _partitionHead[p] = _walRecordEncoder.Decode(
                        page[_partitionPageIndex[p]].EncodedPayload,
                        _treeName);
                    _partitionHeadDecoded[p] = true;
                }

                var head = _partitionHead[p].Timestamp;
                if (minPartition < 0 || head.CompareTo(minHlc) < 0)
                {
                    minPartition = p;
                    minHlc = head;
                }
            }

            if (minPartition < 0)
            {
                // Every partition drained for this tick.
                break;
            }

            var winningPage = _partitionPages[minPartition]!;
            var winningShipping = winningPage[_partitionPageIndex[minPartition]];
            var winningRecord = _partitionHead[minPartition];
            _partitionPageIndex[minPartition]++;
            _partitionHeadDecoded[minPartition] = false;
            _partitionMaxReadSeq[minPartition] = winningShipping.Sequence;
            _partitionAdvanced[minPartition] = true;

            // Defensive HLC filter - legacy-migration case ONLY.
            //
            // The durable per-partition sequence cursor
            // (state.PartitionCursors[p], mirrored in _partitionNextSeq /
            // _partitionMaxReadSeq) is the authoritative exactly-once
            // resume token: the outer merge loop presents each WAL
            // sequence in a partition exactly once, and the cursor only
            // advances past a sequence on a positive ack. So once a
            // partition has a saved durable cursor, every entry past it
            // is genuinely unshipped and MUST be shipped - dropping it on
            // a scalar-HLC comparison would silently strand it.
            //
            // This matters because the source HLC is NOT monotonic with
            // WAL-append order within a partition: HLCs are stamped per
            // leaf (BPlusLeafGrain's own clock) and a partition is keyed
            // by WalPartitionHash(key), so many leaves interleave in one
            // partition. A genuinely-new point write can therefore arrive
            // with a source HLC below the running scalar cursor
            // (state.Cursor, which tracks the max-shipped / ack frontier).
            // The old unconditional `Timestamp <= state.Cursor` drop
            // treated every such entry as already-seen and skipped it
            // while still advancing the partition cursor past it - the
            // silent replication-gap bug (#1060). The receiver upholds
            // at-most-once via its per-(origin,hlc,key,op) shadow-forward
            // dedup cache and per-key LWW source-HLC guard, so shipping a
            // below-cursor-but-new entry is safe; a true duplicate is a
            // receiver-side no-op.
            //
            // The scalar filter is retained solely for the one-time
            // legacy-migration tick: a state persisted by a pre-partition
            // -cursor build carries a non-zero state.Cursor but an EMPTY
            // PartitionCursors dictionary, so every partition resumes from
            // sequence 0 and would re-ship the entire already-shipped
            // prefix. `_legacyCursorMigrationPending` (computed once per
            // tick in InitializeDrainTickAsync from the WHOLE dictionary
            // being empty) scopes the drop to that case; from the first ack
            // onward at least one partition cursor is saved and the drop is
            // disabled for every partition - including genuinely cold ones,
            // whose unshipped entries may legitimately carry a per-leaf HLC
            // below the scalar cursor and MUST NOT be dropped. Zero-HLC
            // (DeleteRange) and prepared-atomic-batch entries are excluded
            // even in the legacy case - both carry non-monotonic per-leaf
            // HLCs and are tracked purely by partition sequence.
            var isPreparedAtomicBatch = winningRecord.IsPrepared && winningRecord.AtomicBatchSize > 0;
            if (_legacyCursorMigrationPending
                && !isPreparedAtomicBatch
                && winningRecord.Timestamp != HybridLogicalClock.Zero
                && winningRecord.Timestamp.CompareTo(state.State.Cursor) <= 0)
            {
                continue;
            }

            if (!ShouldShip(winningRecord, options))
            {
                continue;
            }

            _drainBuffer.Add(winningRecord);
            // Wrap the pre-encoded payload bytes verbatim. The
            // shipping page borrows the bytes from the WAL grain's
            // page DTO; safe for synchronous consumption inside the
            // outbound SendAsync call because Orleans serialises
            // grain turns and SendAsync awaits inline.
            var payload = winningShipping.EncodedPayload;
            _drainEncodedSegments.Add(new ArraySegment<byte>(payload));
            _drainEncodedByteCount += payload.Length;

            // Content-hash dedup measurement (default on, overridable off).
            // The measurement is observability-only: the entry above is
            // shipped verbatim regardless of the outcome, so LWW / HLC
            // convergence semantics and the on-the-wire bytes are
            // unaffected. We hash the entry's content and record a
            // redundant-payload sample when the same key was last
            // shipped with byte-identical content - the idempotent
            // re-set signal upstream retry logic generates.
            MeasureContentHashRedundancy(in winningRecord, options);
        }

        // Pre-ship coalescing (default on, overridable off). Collapse
        // redundant per-key versions out of the freshly-drained batch before
        // they reach the wire. A no-op when the option is overridden off (the
        // drained buffers are shipped verbatim), so opting out restores the
        // byte-identical verbatim drain.
        CoalesceDrainBuffer(options);
    }

    /// <summary>
    /// Pre-ship coalescing pass over the freshly-drained batch buffers
    /// (<see cref="_drainBuffer"/> / <see cref="_drainEncodedSegments"/>).
    /// No-op unless <see cref="LatticeReplicationOptions.PreShipCoalescingEnabled"/>
    /// is set. The pass dispatches on the tree's declared
    /// <see cref="LatticeMergeMode"/>: a
    /// <see cref="LatticeMergeMode.LwwRegister"/> tree keeps only the
    /// highest-HLC same-key write and elides the rest
    /// (<see cref="CoalesceLwwDrainBuffer"/>); a recognised CRDT tree
    /// folds the typed deltas of a same-key run into a single combined
    /// delta whose apply effect equals applying the run in order
    /// (<see cref="CoalesceCrdtDrainBuffer"/>); any other mode ships the
    /// batch verbatim.
    /// <para>
    /// For a last-writer-wins tree the receiver applies each entry LWW on
    /// the value bytes ordered by <c>(HybridLogicalClock, OriginClusterId)</c>,
    /// so when a key is rewritten several times within a single drained
    /// batch only the highest-HLC version survives convergence; the earlier
    /// versions are invisible after apply. This pass keeps only the last
    /// (highest-HLC) coalescable point write per key and drops the earlier
    /// same-key ones. The shipper drains only its own cluster's authored
    /// writes (<see cref="ShouldShip"/> filters to <c>options.ClusterId</c>),
    /// so every coalescable entry shares one origin and the ordering
    /// tie-break collapses to a pure HLC comparison - the drain buffer is
    /// already HLC-ascending, so the last occurrence of a key is the
    /// highest-HLC one.
    /// </para>
    /// <para>
    /// For a CRDT tree the receiver applies each entry by folding its
    /// typed delta into the loaded state, so dropping an intermediate
    /// version would lose its contribution. The CRDT branch instead
    /// merges the same-key deltas into one combined delta (a join over
    /// each primitive's semilattice - union for OR-Set adds / removes,
    /// pointwise-max for PN-Counter and version-vector components,
    /// dot-dominance merge for the multi-value register, grow-only union
    /// for RGA), re-encodes that one delta onto the kept (highest-HLC)
    /// entry, and elides the earlier same-key ones. Because each combine
    /// is commutative, associative, and idempotent and so is the
    /// receiver-side apply, the merged result converges to the identical
    /// state as shipping the run individually.
    /// </para>
    /// <para>
    /// Only plain point <see cref="MutationKind.Set"/> /
    /// <see cref="MutationKind.Delete"/> entries that are not atomic-batch
    /// prepare-phase writes and do not carry
    /// <see cref="HybridLogicalClock.Zero"/> are eligible (see
    /// <see cref="IsCoalescable"/>). Range deletes, saga terminal marks,
    /// prepared atomic-batch entries, and zero-HLC entries are never
    /// elided and never participate, so atomic-batch boundaries, causal
    /// dependencies, and per-origin FIFO ordering are preserved verbatim.
    /// </para>
    /// <para>
    /// Coalescing is purely a transform over what gets shipped: every
    /// drained entry's per-partition sequence was already folded into the
    /// resume bookkeeping (<see cref="_partitionMaxReadSeq"/> /
    /// <see cref="_partitionAdvanced"/>) inside the merge loop before this
    /// pass runs, so the cursor still advances past every elided entry and
    /// nothing is re-shipped or stranded. The coalesced output is a strict
    /// subset of the verbatim batch, so an unmodified receiver decodes and
    /// applies it to the identical converged state.
    /// </para>
    /// </summary>
    private void CoalesceDrainBuffer(LatticeReplicationOptions options)
    {
        if (!options.PreShipCoalescingEnabled || _drainBuffer.Count <= 1)
        {
            return;
        }

        // Resolve the tree's declared mode once (a cached dictionary read)
        // and dispatch. A tree not declared replicated resolves to null,
        // which collapses to the LWW default - but such a tree never
        // activates a shipper, so the null case is unreachable here.
        var mode = _modeResolver.Resolve(_treeName) ?? LatticeMergeMode.LwwRegister;
        if (mode == LatticeMergeMode.LwwRegister)
        {
            CoalesceLwwDrainBuffer();
        }
        else
        {
            CoalesceCrdtDrainBuffer(mode);
        }
    }

    /// <summary>
    /// Last-writer-wins branch of <see cref="CoalesceDrainBuffer"/>. Keeps
    /// only the highest-HLC same-key point write in the drained batch and
    /// elides the earlier same-key versions; the receiver's LWW apply
    /// converges to the same state because the earlier versions are
    /// invisible after merge. See <see cref="CoalesceDrainBuffer"/> for the
    /// safety argument.
    /// </summary>
    private void CoalesceLwwDrainBuffer()
    {
        var lastIndex = _coalesceLastIndex ??= new Dictionary<string, int>(StringComparer.Ordinal);
        lastIndex.Clear();

        var count = _drainBuffer.Count;

        // Pass 1: record, per coalescable key, the index of its last
        // occurrence. The drain buffer is HLC-ascending and single-origin,
        // so the last index is the highest-HLC version - the one the
        // receiver's LWW apply converges to.
        for (var i = 0; i < count; i++)
        {
            var entry = _drainBuffer[i];
            if (IsCoalescable(in entry))
            {
                lastIndex[entry.Key ?? string.Empty] = i;
            }
        }

        // Pass 2: compact in place. Keep every non-coalescable entry
        // verbatim and, among coalescable entries, keep only the last
        // occurrence of each key. Elide the rest.
        var write = 0;
        var elidedCount = 0;
        var elidedBytes = 0L;
        for (var i = 0; i < count; i++)
        {
            var entry = _drainBuffer[i];
            var keep = true;
            if (IsCoalescable(in entry) && lastIndex[entry.Key ?? string.Empty] != i)
            {
                keep = false;
            }

            if (keep)
            {
                if (write != i)
                {
                    _drainBuffer[write] = entry;
                    _drainEncodedSegments[write] = _drainEncodedSegments[i];
                }
                write++;
            }
            else
            {
                elidedCount++;
                elidedBytes += _drainEncodedSegments[i].Count;
            }
        }

        if (elidedCount == 0)
        {
            return;
        }

        _drainBuffer.RemoveRange(write, count - write);
        _drainEncodedSegments.RemoveRange(write, count - write);
        _drainEncodedByteCount -= elidedBytes;

        LatticeReplicationMetrics.CoalesceEntriesElided.Add(
            elidedCount,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
        LatticeReplicationMetrics.CoalesceBytesElided.Add(
            elidedBytes,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
    }

    /// <summary>
    /// CRDT branch of <see cref="CoalesceDrainBuffer"/>. Folds the typed
    /// deltas of a same-key run into a single combined delta, re-encodes it
    /// onto the kept (highest-HLC) entry, and elides the earlier same-key
    /// ones. Falls back to shipping a key's entries verbatim when the
    /// mode's <see cref="CrdtShape"/> has no combine - an OR-Map tree whose
    /// shape is unregistered (the registry returns no descriptor) or any
    /// other mode without a combiner - or any of the key's entries carries
    /// an opaque (null) typed delta, so no data is ever lost. Registered
    /// OR-Map shapes now carry a combiner (folding the dot-tagged adds /
    /// tombstones with same-dot value snapshots lattice-merged through the
    /// value CRDT), so they coalesce like the closed primitives. The merged
    /// result inherits the last contributing entry's HLC and causal
    /// metadata.
    /// </summary>
    private void CoalesceCrdtDrainBuffer(LatticeMergeMode mode)
    {
        // Resolve the shape descriptor for this tree's mode. The registry
        // is injected from DI in a configured silo; unit-test
        // constructions may pass null, in which case we lazily build a
        // private registry carrying the closed-shape combiners. An
        // unregistered OR-Map tree returns null (no combine available), so
        // we ship verbatim.
        var registry = _crdtShapeRegistry ??= new CrdtShapeRegistry();
        var shape = registry.TryGet(_treeName, mode);
        if (shape is null || shape.CombineDeltas is null || shape.SerializeDelta is null)
        {
            return;
        }

        var states = _coalesceCrdtState ??= new Dictionary<string, CrdtCoalesceState>(StringComparer.Ordinal);
        states.Clear();

        var count = _drainBuffer.Count;

        // Pass 1: accumulate, per coalescable key, the running combined
        // delta and the index of the last contributing entry. An opaque
        // (null-delta) same-key entry flips the key to non-combinable so
        // every one of its entries ships verbatim. The drain buffer is
        // HLC-ascending and single-origin, so folding in iteration order
        // is the same causal order the receiver would apply.
        for (var i = 0; i < count; i++)
        {
            var entry = _drainBuffer[i];
            if (!IsCoalescable(in entry))
            {
                continue;
            }

            var key = entry.Key ?? string.Empty;
            if (!states.TryGetValue(key, out var state))
            {
                state = new CrdtCoalesceState { CanCombine = true };
            }

            if (entry.Delta is null)
            {
                // Opaque payload on a CRDT mode: cannot be combined safely.
                // Force the whole key to ship verbatim.
                state.CanCombine = false;
                states[key] = state;
                continue;
            }

            if (!state.CanCombine)
            {
                states[key] = state;
                continue;
            }

            var delta = shape.DeserializeDelta(entry.Delta);
            if (state.FoldCount == 0)
            {
                state.Combined = delta;
            }
            else
            {
                state.Combined = shape.CombineDeltas(state.Combined!, delta);
            }
            state.LastIndex = i;
            state.FoldCount++;
            states[key] = state;
        }

        // Pass 2: compact in place. Keep every non-coalescable entry and
        // every entry of a non-combinable key verbatim. For a combinable
        // key with two-or-more folded deltas, keep only the last
        // contributing entry (re-encoded with the combined delta) and
        // elide the rest.
        var writer = _coalesceReencodeWriter ??= new ArrayBufferWriter<byte>();
        var write = 0;
        var elidedCount = 0;
        var elidedBytes = 0L;
        var reencodeByteDelta = 0L;
        var deltasMerged = 0L;
        for (var i = 0; i < count; i++)
        {
            var entry = _drainBuffer[i];
            var keep = true;
            var replace = false;
            if (IsCoalescable(in entry))
            {
                var key = entry.Key ?? string.Empty;
                var state = states[key];
                if (state.CanCombine && state.FoldCount >= 2)
                {
                    if (i == state.LastIndex)
                    {
                        replace = true;
                    }
                    else
                    {
                        keep = false;
                    }
                }
            }

            if (!keep)
            {
                elidedCount++;
                elidedBytes += _drainEncodedSegments[i].Count;
                continue;
            }

            if (replace)
            {
                var key = entry.Key ?? string.Empty;
                var state = states[key];
                var combinedBytes = shape.SerializeDelta!(state.Combined!);
                // The merged result inherits the last contributing entry's
                // HLC / causal metadata (entry is that entry); only the
                // typed delta payload changes. Re-encode through the same
                // codec the producer used so the wire shape is identical to
                // a natively-emitted single delta entry.
                var merged = entry with { Delta = combinedBytes };
                writer.Clear();
                _walRecordEncoder.Encode(in merged, writer);
                var newSegment = new ArraySegment<byte>(writer.WrittenSpan.ToArray());
                reencodeByteDelta += newSegment.Count - _drainEncodedSegments[i].Count;
                deltasMerged += state.FoldCount;
                _drainBuffer[write] = merged;
                _drainEncodedSegments[write] = newSegment;
                write++;
                continue;
            }

            if (write != i)
            {
                _drainBuffer[write] = entry;
                _drainEncodedSegments[write] = _drainEncodedSegments[i];
            }
            write++;
        }

        if (elidedCount == 0)
        {
            return;
        }

        _drainBuffer.RemoveRange(write, count - write);
        _drainEncodedSegments.RemoveRange(write, count - write);
        _drainEncodedByteCount += reencodeByteDelta - elidedBytes;

        LatticeReplicationMetrics.CoalesceEntriesElided.Add(
            elidedCount,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
        LatticeReplicationMetrics.CoalesceBytesElided.Add(
            elidedBytes,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
        LatticeReplicationMetrics.CoalesceDeltasMerged.Add(
            deltasMerged,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
    }

    /// <summary>
    /// Whether <paramref name="entry"/> is a candidate for pre-ship
    /// coalescing: a plain point <see cref="MutationKind.Set"/> /
    /// <see cref="MutationKind.Delete"/> write that is not part of an
    /// atomic batch (saga) prepare phase and carries a real (non-zero)
    /// <see cref="HybridLogicalClock"/>. Range deletes, saga terminal
    /// marks, prepared atomic-batch entries, tombstone-reap envelopes,
    /// and zero-HLC entries return <see langword="false"/> so they are
    /// always shipped verbatim and never elided, preserving atomic-batch
    /// boundaries, causal ordering, and per-origin FIFO.
    /// </summary>
    private static bool IsCoalescable(in WalRecord entry) =>
        entry.Op is MutationKind.Set or MutationKind.Delete
        && !entry.IsPrepared
        && entry.AtomicBatchSize == 0
        && entry.Timestamp != HybridLogicalClock.Zero;

    /// <summary>
    /// Records the content-hash payload-re-send measurement for a single
    /// entry being shipped. No-op unless
    /// <see cref="LatticeReplicationOptions.ContentHashDedupEnabled"/> is
    /// set; only <see cref="MutationKind.Set"/> entries (the only
    /// mutation kind that carries a value payload) are measured. When
    /// the entry's content hash matches the value most recently shipped
    /// for the same key the
    /// <see cref="LatticeReplicationMetrics.ShipRedundantPayloads"/> and
    /// <see cref="LatticeReplicationMetrics.ShipRedundantPayloadBytes"/>
    /// counters are incremented.
    /// </summary>
    private void MeasureContentHashRedundancy(in WalRecord record, LatticeReplicationOptions options)
    {
        if (!options.ContentHashDedupEnabled || record.Op != MutationKind.Set)
        {
            return;
        }

        var cache = _contentHashCache ??=
            new ShippedContentHashCache(Math.Max(64, options.ContentHashDedupCacheSize));
        var key = record.Key ?? string.Empty;
        var hash = ReplicationContentHash.Compute(in record);
        if (!cache.Observe(key, hash))
        {
            return;
        }

        var valueBytes = record.Value?.Length ?? 0;
        LatticeReplicationMetrics.ShipRedundantPayloads.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
        LatticeReplicationMetrics.ShipRedundantPayloadBytes.Add(
            valueBytes,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
    }

    /// <summary>
    /// Performs the opt-in sender-manifest / receiver-pull-missing
    /// content-hash round trip over the freshly-drained batch and elides
    /// the payloads the receiver already holds byte-identical. No-op unless
    /// both <see cref="LatticeReplicationOptions.ContentHashDedupEnabled"/>
    /// and <see cref="LatticeReplicationOptions.ContentHashDedupElisionEnabled"/>
    /// are set and the peer has not already reported (this activation) that
    /// it cannot perform the exchange.
    /// <para>
    /// Capability is learned lazily: the first eligible batch attempts the
    /// exchange; a peer that reports
    /// <see cref="ContentManifestResponse.ExchangeSupported"/> =
    /// <see langword="false"/> (the default no-op transport, or an
    /// un-upgraded peer) latches
    /// <see cref="_peerSupportsManifestExchange"/> off for the rest of the
    /// activation so subsequent batches ship the full payload verbatim -
    /// wire-identical to today and rolling-upgrade safe. An exchange RPC
    /// failure is swallowed (the full batch ships; the downstream
    /// <see cref="IReplicationTransport.SendAsync"/> handles any genuine
    /// transport fault and backoff).
    /// </para>
    /// <para>
    /// The receiver advances its per-origin high-water-mark for every
    /// elided entry via a metadata-only apply inside the exchange, so the
    /// high-water-mark still advances for the identical-content-newer-HLC
    /// case even though the payload is never re-shipped. Only eligible
    /// point <see cref="MutationKind.Set"/> entries are ever placed in the
    /// manifest (see <see cref="ContentManifestPlanner.BuildManifest"/>);
    /// range deletes, saga terminal marks, prepared atomic-batch entries,
    /// and zero-HLC entries are never manifested and always ship verbatim,
    /// preserving atomic-batch boundaries, causal ordering, and per-origin
    /// FIFO. Each drained entry's per-partition sequence was already folded
    /// into the resume bookkeeping at drain time, so the cursor still
    /// advances past every elided entry.
    /// </para>
    /// </summary>
    private async Task TryElideViaManifestExchangeAsync(
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        if (!options.ContentHashDedupEnabled
            || !options.ContentHashDedupElisionEnabled
            || _peerSupportsManifestExchange == false
            || _drainBuffer.Count == 0)
        {
            return;
        }

        var manifest = ContentManifestPlanner.BuildManifest(_drainBuffer);
        if (manifest.Count == 0)
        {
            // No value-carrying point-Set entries in this batch: nothing to
            // elide, and no point paying for an exchange round trip.
            return;
        }

        var request = new ContentManifestRequest
        {
            TreeName = _treeName,
            OriginClusterId = options.ClusterId,
            Entries = manifest,
        };

        ContentManifestResponse response;
        try
        {
            response = await _digestProbeTransport
                .ExchangeContentManifestAsync(_peerClusterId, request, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Exchange RPC failure: skip elision and ship the full batch
            // verbatim. SendAsync below handles a genuine transport fault.
            Logger.LogDebug(ex,
                "Content-hash manifest exchange failed for {Context}; shipping the full batch",
                LogContext);
            return;
        }

        if (!response.ExchangeSupported)
        {
            _peerSupportsManifestExchange = false;
            return;
        }

        _peerSupportsManifestExchange = true;
        LatticeReplicationMetrics.ManifestExchanges.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));

        var elided = ContentManifestPlanner.ComputeElidedIndices(manifest, response.MissingEntryIndices);
        if (elided.Count == 0)
        {
            return;
        }

        var count = _drainBuffer.Count;
        var write = 0;
        var elidedBytes = 0L;
        var elidedCount = 0;
        for (var i = 0; i < count; i++)
        {
            if (elided.Contains(i))
            {
                elidedBytes += _drainEncodedSegments[i].Count;
                elidedCount++;
                continue;
            }
            if (write != i)
            {
                _drainBuffer[write] = _drainBuffer[i];
                _drainEncodedSegments[write] = _drainEncodedSegments[i];
            }
            write++;
        }

        if (elidedCount == 0)
        {
            return;
        }

        _drainBuffer.RemoveRange(write, count - write);
        _drainEncodedSegments.RemoveRange(write, count - write);
        _drainEncodedByteCount -= elidedBytes;

        LatticeReplicationMetrics.ShipElidedPayloads.Add(
            elidedCount,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
        LatticeReplicationMetrics.ShipElidedPayloadBytes.Add(
            elidedBytes,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId));
    }

    /// <summary>
    /// Issues a <see cref="IWalShardGrain.ReadShippingAsync"/> against
    /// the requested partition starting at <see cref="_partitionNextSeq"/>,
    /// stores the page in the scratch arrays, and updates
    /// <see cref="_partitionNextSeq"/> to the page's
    /// <see cref="WalShardShippingPage.NextSequence"/>. An empty result
    /// leaves <see cref="_partitionPages"/> at <see langword="null"/>
    /// for that partition - the caller treats that as "drained this
    /// tick" and stops considering the partition for the rest of the
    /// merge loop.
    /// </summary>
    private async Task TryRefillPartitionAsync(int partition, int pageSize, CancellationToken cancellationToken)
    {
        var grain = _partitionGrainCache[partition] ??=
            _grainFactory.GetGrain<IWalShardGrain>($"{_walTreeId}/{partition}");
        var page = await grain
            .ReadShippingAsync(_partitionNextSeq[partition], pageSize, cancellationToken)
            ;
        if (page.Entries.Count == 0)
        {
            _partitionPages[partition] = null;
            return;
        }
        _partitionPages[partition] = page.Entries;
        _partitionPageIndex[partition] = 0;
        _partitionHeadDecoded[partition] = false;
        _partitionNextSeq[partition] = page.NextSequence;
    }

    /// <summary>
    /// Grows the activation-scoped scratch arrays in lockstep when the
    /// configured <see cref="LatticeReplicationOptions.ReplogPartitions"/>
    /// changes (or on first activation). Idempotent - a no-op when the
    /// arrays are already at the requested size.
    /// </summary>
    private void EnsureScratchSized(int partitions)
    {
        _partitionCount = partitions;
        if (_partitionPages.Length >= partitions)
        {
            return;
        }
        Array.Resize(ref _partitionPages, partitions);
        Array.Resize(ref _partitionPageIndex, partitions);
        Array.Resize(ref _partitionNextSeq, partitions);
        Array.Resize(ref _partitionMaxReadSeq, partitions);
        Array.Resize(ref _partitionAdvanced, partitions);
        Array.Resize(ref _partitionGrainCache, partitions);
        Array.Resize(ref _partitionHead, partitions);
        Array.Resize(ref _partitionHeadDecoded, partitions);
    }

    /// <summary>
    /// Sends an empty <see cref="ReplicationBatch"/> as a liveness
    /// probe when the pump tick found no entries to ship AND the
    /// configured <see cref="LatticeReplicationOptions.LivenessProbeInterval"/>
    /// has elapsed since the last successful outbound contact. The
    /// peer acks the empty batch and the standard success-recording
    /// path runs so the
    /// <c>peer.last_contact_seconds{direction="outbound"}</c> gauge
    /// resets and no longer climbs unbounded between local-write
    /// bursts on a healthy idle link. Disabled by setting the
    /// interval to <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>.
    /// Transport throws apply the standard backoff path; ack
    /// rejection leaves the cursor untouched (there is nothing to
    /// advance past). The encoded payload is the 16-byte framing
    /// header alone.
    /// </summary>
    private async Task TryEmitLivenessProbeAsync(LatticeReplicationOptions options, CancellationToken cancellationToken)
    {
        if (options.LivenessProbeInterval == System.Threading.Timeout.InfiniteTimeSpan)
        {
            return;
        }
        var now = DateTime.UtcNow;
        if (_lastSuccessfulContactUtc == DateTime.MinValue)
        {
            // First idle tick on this activation: anchor the
            // probe-interval timer to now so the probe fires
            // ProbeInterval after activation rather than
            // immediately - matches the semantics operators
            // expect (a quiet but healthy link refreshes at the
            // configured cadence) and preserves the "empty drain
            // = no transport call" invariant existing tests
            // depend on for the activation's first pump tick.
            _lastSuccessfulContactUtc = now;
            return;
        }
        if (now - _lastSuccessfulContactUtc < options.LivenessProbeInterval)
        {
            return;
        }

        // Honour wire-version negotiation on the probe path too: a
        // probe still rides the framing header, and a peer below the
        // minimum-supported floor must fail fast rather than ship an
        // un-decodable probe. Skipped entirely when negotiation is off.
        if (options.WireVersionNegotiationEnabled && !TryNegotiateWireVersion(options))
        {
            return;
        }

        ReplicationBatchEncodedEnvelope encodedEnvelope;
        try
        {
            var header = new EncodedBatchHeader
            {
                Magic = EncodedBatchHeader.MagicValue,
                WireVersion = options.WireVersionNegotiationEnabled
                    ? _negotiatedWireVersion
                    : EncodedBatchHeader.CurrentWireVersion,
                OriginClusterIdHash = EncodedBatchHeader.HashClusterId(options.ClusterId),
                EntryCount = 0,
                BatchSequence = 0,
                Mode = _modeResolver.Resolve(_treeName) ?? LatticeMergeMode.LwwRegister,
                Compression = LatticeCompression.None,
            };
            encodedEnvelope = new ReplicationBatchEncodedEnvelope
            {
                Header = header,
                EncodedEntries = ReadOnlyMemory<ArraySegment<byte>>.Empty,
            };
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            // Header construction failure on a probe is logged and
            // swallowed - there are no per-entry side effects to
            // dead-letter, and the next pump tick (or the next
            // doorbell) will try again.
            Logger.LogWarning(ex,
                "Liveness-probe header construction failed for {Context}; skipping probe", LogContext);
            return;
        }

        ReplicationAck ack;
        try
        {
            var batch = new ReplicationBatch
            {
                TargetClusterId = _peerClusterId,
                TreeName = _treeName,
                OriginClusterId = options.ClusterId,
                Payload = ReadOnlyMemory<byte>.Empty,
                Envelope = null,
                EncodedEnvelope = encodedEnvelope,
            };
            ack = await _transport.SendAsync(batch, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ApplyBackoff(options, ex, "transport");
            return;
        }

        if (!ack.Accepted)
        {
            ApplyBackoff(options, exception: null, reason: "ack-rejected");
            return;
        }

        // Successful probe: stamp last-contact and refresh the
        // outbound peer-stats success/backlog gauges. Receiver-side
        // flow-control hints are still honoured (a receiver may
        // pause an idle link).
        state.State.ConsecutiveFailures = 0;
        _nextRetryAtUtc = DateTime.MinValue;
        _receiverSuggestedBatchSize = ack.SuggestedBatchSize;
        _peerWireVersion = ack.SupportedWireVersion;
        _peerAdvertisedDictionaryIds = ack.AdvertisedDictionaryIds;
        _peerAdvertisedDictionaries = ack.AdvertisedDictionaries;
        if (ack.PauseForMs is { } pauseMs && pauseMs > 0)
        {
            var requested = DateTime.UtcNow.AddMilliseconds(pauseMs);
            if (requested > _nextRetryAtUtc)
            {
                _nextRetryAtUtc = requested;
            }
        }
        _peerStats.RecordSuccess(_treeName, _peerClusterId);
        _peerStats.RecordBacklog(_treeName, _peerClusterId, entriesBehind: 0, bytesBehind: 0);
        _lastSuccessfulContactUtc = DateTime.UtcNow;
    }

    /// <summary>
    /// Computes the wire-version negotiation for the peer against its
    /// most recently advertised <see cref="ReplicationAck.SupportedWireVersion"/>,
    /// publishes the negotiated version and downgrade signal to the
    /// <c>wire_version.negotiated</c> / <c>wire_version.downgrade_active</c>
    /// gauges, records the version the header sites will stamp into
    /// <see cref="_negotiatedWireVersion"/>, and returns
    /// <see langword="true"/> when the batch may ship. Returns
    /// <see langword="false"/> - after logging an error, recording the
    /// <see cref="LatticeReplicationMetrics.ShipWireVersionDownStamp"/>
    /// counter, and applying backoff - for the genuinely-unsupported cases
    /// that must pause rather than ship an un-applyable frame: the peer
    /// advertised a version older than the configured
    /// <see cref="LatticeReplicationOptions.MinimumSupportedWireVersion"/>
    /// floor, the tree is in a CRDT merge mode (reason
    /// <see cref="LatticeReplicationMetrics.DownStampReasonBlockedCrdtMode"/>),
    /// or the negotiated target is below
    /// <see cref="WireVersionDownEncoder.MinimumDownEncodableWireVersion"/>
    /// (reason
    /// <see cref="LatticeReplicationMetrics.DownStampReasonBlockedUnsupportedVersion"/>).
    /// A compressed last-writer-wins tree down-stamping to an otherwise
    /// down-encodable target is NOT paused: framing compression is dropped for
    /// that peer's batch (via <see cref="_downStampDropsCompression"/>, reason
    /// <see cref="LatticeReplicationMetrics.DownStampReasonCompressionDropped"/>)
    /// so it keeps replicating uncompressed - lossless, because compression
    /// rides the framing tail only. When the negotiated target equals the
    /// current wire version this is a true no-op: the shipper keeps its
    /// verbatim pre-encoded entry hot path and the bytes on the wire are
    /// byte-identical to a build that never negotiated.
    /// </summary>
    private bool TryNegotiateWireVersion(LatticeReplicationOptions options)
    {
        WireVersionNegotiationResult negotiation;
        try
        {
            negotiation = WireVersionNegotiation.Negotiate(
                EncodedBatchHeader.CurrentWireVersion,
                options.MinimumSupportedWireVersion,
                options.UnknownPeerWireVersionFloor,
                _peerWireVersion);
        }
        catch (NotSupportedException ex)
        {
            Logger.LogError(ex,
                "Peer {Peer} on tree {Tree} advertised a wire version below the sender's "
                + "minimum supported floor {Floor}; cannot ship until the peer upgrades.",
                _peerClusterId, _treeName, options.MinimumSupportedWireVersion);
            ApplyBackoff(options, exception: null, reason: "wire-version-unsupported");
            return false;
        }

        _negotiationState.Record(_treeName, _peerClusterId, negotiation);

        // Validate that the negotiated target is a down-stamp this build
        // can actually produce for the batch's shape before committing to
        // it. The merge mode is per-tree-constant (resolved once from the
        // activation-cached resolver) and the framing-tail compression
        // intent is the tree-level option, so a CRDT-mode tree, a
        // compression-configured tree, or a target below the down-encode
        // floor surfaces a fail-fast error here rather than emitting a
        // frame the older peer would mis-apply. Skipped entirely when no
        // downgrade is in effect (same-version peers stay the verbatim
        // no-op).
        _downStampDropsCompression = false;
        if (negotiation.DowngradeActive)
        {
            var mode = _modeResolver.Resolve(_treeName) ?? LatticeMergeMode.LwwRegister;
            var target = negotiation.EffectiveWireVersion;
            // Compression rides the framing tail only; dropping it for this peer
            // is lossless, so a compressed LWW tree degrades to an uncompressed
            // frame rather than stalling. Determine whether the tree intends any
            // framing compression (explicit option or the auto-distributing
            // dictionary) - if so and we are down-stamping, plan to drop it and
            // validate down-encodability with LatticeCompression.None.
            var intendsCompression = AutoDictionaryActive(options)
                || options.FramingCompression != LatticeCompression.None;
            var compressionForValidation = intendsCompression && target < EncodedBatchHeader.CurrentWireVersion
                ? LatticeCompression.None
                : options.FramingCompression;
            var willDropCompression = intendsCompression && target < EncodedBatchHeader.CurrentWireVersion;

            try
            {
                WireVersionDownEncoder.EnsureDownEncodable(target, mode, compressionForValidation);
            }
            catch (NotSupportedException ex)
            {
                // Genuinely un-down-encodable: a CRDT-mode tree (cannot be faithfully
                // represented for a pre-version-5 receiver) or a target below the
                // down-encode floor. This is NOT a silent stall - surface a metered,
                // operator-actionable signal then back off.
                var reason = mode != LatticeMergeMode.LwwRegister
                    ? LatticeReplicationMetrics.DownStampReasonBlockedCrdtMode
                    : LatticeReplicationMetrics.DownStampReasonBlockedUnsupportedVersion;
                RecordWireVersionDownStamp(reason);
                Logger.LogError(ex,
                    "Cannot down-stamp tree {Tree} to wire version {Version} for peer {Peer} ({Reason}); "
                    + "replication to this peer is paused until it is upgraded. CRDT-mode trees and "
                    + "sub-floor targets cannot be down-encoded.",
                    _treeName, target, _peerClusterId, reason);
                ApplyBackoff(options, exception: null, reason: "wire-version-unsupported");
                return false;
            }

            if (willDropCompression)
            {
                _downStampDropsCompression = true;
                RecordWireVersionDownStamp(LatticeReplicationMetrics.DownStampReasonCompressionDropped);
            }
        }

        _negotiatedWireVersion = negotiation.EffectiveWireVersion;
        return true;
    }

    /// <summary>
    /// Records a single per-batch wire-version down-stamp outcome on the
    /// <see cref="LatticeReplicationMetrics.ShipWireVersionDownStamp"/> counter,
    /// tagged with the tree, peer, and outcome <paramref name="reason"/>.
    /// </summary>
    private void RecordWireVersionDownStamp(string reason)
        => LatticeReplicationMetrics.ShipWireVersionDownStamp.Add(1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason));

    /// <summary>
    /// Computes the per-peer shared-dictionary capability negotiation for
    /// the next batch. When the peer advertised the fingerprint-bearing
    /// <see cref="ReplicationAck.AdvertisedDictionaries"/> capability the
    /// negotiation gates on <c>(id, fingerprint)</c> - resolving this sender's
    /// own configured dictionary fingerprint via
    /// <see cref="ResolveConfiguredDictionaryFingerprint(uint)"/> - so a
    /// same-id/different-bytes peer falls back rather than shipping a frame the
    /// receiver cannot decode; otherwise it negotiates on the id-only
    /// <see cref="ReplicationAck.AdvertisedDictionaryIds"/> exactly as a peer
    /// predating the fingerprint slot would. Records the outcome to the
    /// <see cref="LatticeReplicationMetrics.DictionaryNegotiation"/> counter
    /// and the process-wide <see cref="SharedDictionaryNegotiationState"/>,
    /// logs a one-shot warning on a fingerprint mismatch, and stores the
    /// effective dictionary id the header sites will stamp into
    /// <see cref="_negotiatedDictionaryId"/>. Only acts when
    /// <see cref="LatticeReplicationOptions.DictionaryNegotiationEnabled"/>
    /// is set and a shared dictionary is actually configured
    /// (<see cref="LatticeCompression.ZstdDictionary"/> with a non-zero
    /// <see cref="LatticeReplicationOptions.FramingCompressionDictionaryId"/>);
    /// otherwise it resets the negotiated id to <c>0</c>. Unlike
    /// <see cref="TryNegotiateWireVersion"/> this never fails fast: an
    /// unmatched, mismatching, or as-yet-unknown peer capability falls back to
    /// dictionary-less compression, which every peer can decode, so no peer
    /// ever receives a frame compressed with a dictionary it has not
    /// advertised.
    /// </summary>
    private void TryNegotiateSharedDictionary(LatticeReplicationOptions options)
    {
        var autoActive = AutoDictionaryActive(options);
        var configuredId = EffectiveConfiguredDictionaryId(options);
        var dictionaryFraming = autoActive || options.FramingCompression == LatticeCompression.ZstdDictionary;
        var negotiationOn = autoActive || options.DictionaryNegotiationEnabled;

        if (!negotiationOn || !dictionaryFraming || configuredId == 0)
        {
            _negotiatedDictionaryId = 0u;
            return;
        }

        var negotiation = _peerAdvertisedDictionaries is { } advertised
            ? SharedDictionaryNegotiation.Negotiate(
                configuredId,
                ResolveConfiguredDictionaryFingerprint(configuredId),
                advertised)
            : SharedDictionaryNegotiation.Negotiate(
                configuredId, _peerAdvertisedDictionaryIds);

        _dictionaryNegotiationState.Record(_treeName, _peerClusterId, negotiation);
        LatticeReplicationMetrics.DictionaryNegotiation.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId),
            new KeyValuePair<string, object?>(
                LatticeReplicationMetrics.TagOutcome,
                LatticeReplicationMetrics.DictionaryNegotiationOutcomeTag(negotiation)));

        if (negotiation.FingerprintMismatch && !_dictionaryFingerprintMismatchWarned)
        {
            _dictionaryFingerprintMismatchWarned = true;
            Logger.LogWarning(
                "Shared-dictionary fingerprint mismatch shipping tree {Tree} to peer {Peer}: " +
                "the peer advertised dictionary id {DictionaryId} with different bytes than this " +
                "sender holds. Falling back to dictionary-less compression for this peer to avoid " +
                "a receiver-side decode failure. Reconcile the dictionary bytes behind id " +
                "{MismatchedDictionaryId} across both deployments.",
                _treeName,
                _peerClusterId,
                configuredId,
                configuredId);
        }

        _negotiatedDictionaryId = negotiation.EffectiveDictionaryId;
    }

    /// <summary>
    /// Whether the self-distributing auto-trained shared dictionary is opted
    /// into for this tree and the injected provider currently has an active
    /// trained dictionary to compress with. When <see langword="true"/> the
    /// ship path treats the provider's
    /// <see cref="ILatticeActiveCompressionDictionary.ActiveDictionaryId"/> as
    /// the configured dictionary id and frames with
    /// <see cref="LatticeCompression.ZstdDictionary"/>, negotiating it against
    /// the peer exactly as a statically configured dictionary would be.
    /// </summary>
    private bool AutoDictionaryActive(LatticeReplicationOptions options)
        => options.AutoSharedDictionaryEnabled
           && _dictionaryProvider is ILatticeActiveCompressionDictionary { ActiveDictionaryId: not 0u };

    /// <summary>
    /// Resolves the shared-dictionary id this sender should negotiate and stamp:
    /// the auto-trainer's live
    /// <see cref="ILatticeActiveCompressionDictionary.ActiveDictionaryId"/> when
    /// the auto-distributing dictionary is opted in and active, otherwise the
    /// statically configured
    /// <see cref="LatticeReplicationOptions.FramingCompressionDictionaryId"/>.
    /// </summary>
    private uint EffectiveConfiguredDictionaryId(LatticeReplicationOptions options)
        => options.AutoSharedDictionaryEnabled
           && _dictionaryProvider is ILatticeActiveCompressionDictionary active
           && active.ActiveDictionaryId != 0u
            ? active.ActiveDictionaryId
            : options.FramingCompressionDictionaryId;

    /// <summary>
    /// Converges this sender onto the peer's trained shared compression
    /// dictionaries when the auto-distributing shared dictionary is opted into
    /// (<see cref="LatticeReplicationOptions.AutoSharedDictionaryEnabled"/>).
    /// For every <c>(id, fingerprint)</c> the peer advertised that the local
    /// provider does not yet hold, pulls the bytes over the digest-probe
    /// transport, verifies them against the advertised fingerprint, and
    /// installs them through the provider's
    /// <see cref="ILatticeCompressionDictionarySink"/>, so the very next
    /// <see cref="TryNegotiateSharedDictionary"/> can compress with the freshly
    /// adopted dictionary instead of falling back. A no-op when the option is
    /// off, when no provider that can install is injected, or when the peer has
    /// advertised nothing new - so the default-off build never pulls.
    /// </summary>
    private async Task MaybeConvergeSharedDictionariesAsync(
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        if (!options.AutoSharedDictionaryEnabled
            || _dictionaryProvider is null
            || _peerAdvertisedDictionaries is not { Length: > 0 } advertised)
        {
            return;
        }

        await CompressionDictionaryConvergence.ConvergeAsync(
            _digestProbeTransport,
            _dictionaryProvider,
            _peerClusterId,
            advertised,
            _treeName,
            cancellationToken);
    }

    /// <summary>
    /// Resolves the content fingerprint of this sender's own configured
    /// shared-dictionary bytes for <paramref name="dictionaryId"/>, caching the
    /// result per id so the per-tick negotiation does not re-resolve and re-hash
    /// on every pump. Returns <c>0</c> when no dictionary provider is injected
    /// or the provider cannot resolve the id; a <c>0</c> fingerprint never
    /// matches a peer's advertised non-zero fingerprint, so the sender falls
    /// back to dictionary-less compression rather than risk a decode failure.
    /// </summary>
    private ulong ResolveConfiguredDictionaryFingerprint(uint dictionaryId)
    {
        if (_cachedFingerprintResolved && _cachedFingerprintForId == dictionaryId)
        {
            return _cachedFingerprint;
        }

        var fingerprint = 0UL;
        if (dictionaryId != 0u
            && _dictionaryProvider is not null
            && _dictionaryProvider.TryGetDictionary(dictionaryId, out var bytes))
        {
            fingerprint = CompressionDictionaryFingerprint.Compute(bytes.Span);
        }

        _cachedFingerprintForId = dictionaryId;
        _cachedFingerprint = fingerprint;
        _cachedFingerprintResolved = true;
        return fingerprint;
    }

    /// <summary>
    /// Resolves the framing-tail compression algorithm and shared-dictionary
    /// id to stamp on the next batch's <see cref="EncodedBatchHeader"/>.
    /// Honours the configured algorithm and the
    /// <see cref="LatticeReplicationOptions.FramingCompressionMinBatchBytes"/>
    /// threshold exactly as before; on the dictionary-eligible path
    /// (configured <see cref="LatticeCompression.ZstdDictionary"/> with a
    /// large-enough tail) it applies per-peer shared-dictionary negotiation
    /// when <see cref="LatticeReplicationOptions.DictionaryNegotiationEnabled"/>
    /// is set: a matched peer keeps the negotiated dictionary id, while an
    /// unmatched or unknown peer is stamped with plain dictionary-less
    /// <see cref="LatticeCompression.Zstd"/> so the receiver decodes a frame
    /// it can always handle. When negotiation is off the returned pair is
    /// byte-identical to the prior inline computation. The method records
    /// the per-batch
    /// <see cref="LatticeReplicationMetrics.DictionaryBatches"/> counter on
    /// the dictionary-eligible path; metric recording never changes the
    /// bytes on the wire.
    /// </summary>
    private (LatticeCompression Compression, uint DictionaryId) ResolveFramingCompression(
        LatticeReplicationOptions options)
    {
        if (_downStampDropsCompression)
        {
            // A down-stamped pre-current-version peer cannot decode framing-tail
            // compression (plain or dictionary), so ship this peer's batch
            // uncompressed. Lossless: compression is framing-only.
            return (LatticeCompression.None, 0u);
        }

        var autoActive = AutoDictionaryActive(options);
        var framingCompression = autoActive ? LatticeCompression.ZstdDictionary : options.FramingCompression;
        var eligible = framingCompression != LatticeCompression.None
                       && _drainEncodedByteCount >= options.FramingCompressionMinBatchBytes;
        if (!eligible)
        {
            return (LatticeCompression.None, 0u);
        }

        if (framingCompression != LatticeCompression.ZstdDictionary)
        {
            return (framingCompression, 0u);
        }

        // Dictionary-eligible path. When negotiation is off, stamp the
        // configured id exactly as before. When on (explicitly, or implied
        // by the auto-distributing dictionary), stamp the negotiated id
        // (0 unless the peer advertised the configured id) and degrade the
        // tag to plain Zstd on fallback.
        var negotiationOn = autoActive || options.DictionaryNegotiationEnabled;
        var dictionaryId = negotiationOn
            ? _negotiatedDictionaryId
            : EffectiveConfiguredDictionaryId(options);

        if (negotiationOn && dictionaryId == 0)
        {
            RecordDictionaryBatch(LatticeReplicationMetrics.DictionaryBatchWithout);
            return (LatticeCompression.Zstd, 0u);
        }

        RecordDictionaryBatch(dictionaryId != 0
            ? LatticeReplicationMetrics.DictionaryBatchWith
            : LatticeReplicationMetrics.DictionaryBatchWithout);
        return (LatticeCompression.ZstdDictionary, dictionaryId);
    }

    private void RecordDictionaryBatch(string dictionaryTag) =>
        LatticeReplicationMetrics.DictionaryBatches.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, _treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, _peerClusterId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagDictionary, dictionaryTag));

    private void ApplyBackoff(LatticeReplicationOptions options, Exception? exception, string reason)
    {
        state.State.ConsecutiveFailures = checked(state.State.ConsecutiveFailures + 1);
        var failures = state.State.ConsecutiveFailures;
        var initialMs = options.ShipBackoffInitial.TotalMilliseconds;
        var maxMs = options.ShipBackoffMax.TotalMilliseconds;
        var multiplier = Math.Pow(2, Math.Max(0, failures - 1));
        var delayMs = Math.Min(maxMs, initialMs * multiplier);

        var jitter = options.ShipBackoffJitter;
        if (jitter > 0.0)
        {
            // Symmetric: [1 - jitter, 1 + jitter]
            var spread = (_jitterRandom.NextDouble() * 2.0 - 1.0) * jitter;
            delayMs = Math.Max(0.0, delayMs * (1.0 + spread));
        }

        var delay = TimeSpan.FromMilliseconds(delayMs);
        _nextRetryAtUtc = DateTime.UtcNow.Add(delay);

        // Per-peer error tally: only count failures that are attributable
        // to the peer round-trip (transport throw, receiver ack rejection).
        // "drain" failures are local WAL read errors - the peer is fine,
        // so they must not bump the consecutive_errors gauge for that peer.
        if (string.Equals(reason, "transport", StringComparison.Ordinal)
            || string.Equals(reason, "ack-rejected", StringComparison.Ordinal))
        {
            _peerStats.RecordError(_treeName, _peerClusterId);
        }

        if (exception is not null)
        {
            Logger.LogWarning(exception,
                "Shipper {Context} {Reason} failed (consecutive={Failures}); backing off {Delay}",
                LogContext, reason, failures, delay);
        }
        else
        {
            Logger.LogDebug(
                "Shipper {Context} {Reason} (consecutive={Failures}); backing off {Delay}",
                LogContext, reason, failures, delay);
        }
    }

    /// <summary>
    /// Routes every entry in the current drain buffer to the per-tree
    /// dead-letter queue, tagged with
    /// <see cref="LatticeReplicationMetrics.ReasonSchema"/>. A
    /// best-effort enqueue failure is logged and swallowed - the
    /// cursor still advances past the batch so a deterministically-
    /// failing DLQ does not pin the ship loop forever; the WAL
    /// retains the originals until the GC pass trims them, so an
    /// operator can still recover off the WAL even when the DLQ is
    /// unavailable.
    /// </summary>
    private async Task RouteBatchToDeadLetterAsync(Exception encodeFailure, CancellationToken cancellationToken)
    {
        var failureReason = encodeFailure.Message ?? "<no message>";
        var dlq = _grainFactory.GetGrain<IReplicationDeadLetterGrain>(_treeName);
        foreach (var entry in _drainBuffer)
        {
            try
            {
                await dlq.EnqueueAsync(
                    entry,
                    failureReason,
                    retryCount: 0,
                    LatticeReplicationMetrics.ReasonSchema,
                    cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogWarning(ex,
                    "Failed to park entry on DLQ for {Context} (key={Key}, hlc={Hlc}); proceeding with cursor advance",
                    LogContext, entry.Key, entry.Timestamp);
            }
        }
    }

    private void ParseGrainKey()
    {
        if (_keyParsed)
        {
            return;
        }

        var key = Context.GrainId.Key.ToString() ?? "";
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationShipperGrain)} activation key is empty; expected '{{treeName}}/{{peerClusterId}}'.");
        }

        var slash = key.LastIndexOf('/');
        if (slash <= 0 || slash >= key.Length - 1)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationShipperGrain)} activation key '{key}' is not in the expected '{{treeName}}/{{peerClusterId}}' format.");
        }

        _treeName = key[..slash];
        _peerClusterId = key[(slash + 1)..];
        _keyParsed = true;
        _walTreeId = _treeName;

        // System trees (any id starting with
        // LatticeConstants.SystemTreePrefix - the tree registry
        // _lattice_trees, every _lattice_replog_* WAL tree, and any
        // future internal tree) describe local topology and durability,
        // not user data. Their WAL records must never propagate to
        // peer clusters: every cluster runs its own registry / WAL
        // independently, and routing a system-tree mutation through a
        // peer's IReplicationApplier would either (a) collide with the
        // peer's own registry state under the same transaction id when
        // a user-tree apply path inadvertently writes to the registry
        // under saga ambient context, or (b) install a meaningless
        // tree-registration record on the peer. The
        // ReplicationDriverActivationService only iterates user
        // ReplicatedTrees, so this branch is also a defense-in-depth
        // guard against future seams (custom shipping registrations,
        // bespoke driver hosts) that might activate a shipper for a
        // system tree.
        if (_treeName.StartsWith(Orleans.Lattice.BPlusTree.LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationShipperGrain)} cannot be activated for system tree '{_treeName}'. "
                + $"Names starting with '{Orleans.Lattice.BPlusTree.LatticeConstants.SystemTreePrefix}' "
                + "are reserved for internal Lattice system trees and are not eligible for cross-cluster replication.");
        }
    }

    /// <summary>
    /// Resolves the shipper's logical source tree id to its current physical
    /// tree id via the registry alias. WAL shards are keyed by the physical id,
    /// so the ship path must address them by the resolved value rather than the
    /// logical grain key. Returns the logical id unchanged when resolution
    /// yields nothing (the direct-construction unit-test path, where no registry
    /// is wired), so the shipper keeps addressing <c>{logical}/{partition}</c>
    /// exactly as it did before the identity-swap heal existed.
    /// </summary>
    private async Task<string> ResolveSourcePhysicalAsync()
    {
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(
            Orleans.Lattice.BPlusTree.LatticeConstants.RegistryTreeId);
        var physical = await registry.ResolveAsync(_treeName);
        return string.IsNullOrEmpty(physical) ? _treeName : physical;
    }

    /// <summary>
    /// Gated per-tick source-identity refresh. The shipper's binding to the
    /// source tree's physical WAL is maintained primarily by an event-driven
    /// push (<see cref="NotifySourceIdentityChangedAsync"/>) that the tree
    /// registry fires on an alias swap, so the steady-state pump does not read
    /// the registry on every tick. This method performs the authoritative
    /// registry resolve only when the binding has never been established for this
    /// activation, or when the backstop interval
    /// (<see cref="LatticeReplicationOptions.ShipSourceIdentityBackstopInterval"/>)
    /// has elapsed since the last resolve or rebind - a safety net that heals a
    /// missed notification without reintroducing a per-tick registry read on an
    /// idle tree.
    /// </summary>
    private async Task MaybeRefreshSourceIdentityAsync(LatticeReplicationOptions options, int partitions)
    {
        if (_sourceIdentityResolved)
        {
            var elapsed = _cursorFlushClock.GetUtcNow().UtcDateTime - _lastSourceIdentityResolveUtc;
            if (elapsed < options.ShipSourceIdentityBackstopInterval)
            {
                return;
            }
        }

        var physical = await ResolveSourcePhysicalAsync();
        await ApplyResolvedIdentityAsync(physical, partitions);
    }

    /// <summary>
    /// Event-driven rebind entry point. Invoked by the replication tree-alias
    /// observer when the tree registry swaps the logical source tree's alias to a
    /// new physical identity (shadow-cutover restore, resize, reshard), so the
    /// shipper rebinds immediately rather than waiting for the backstop poll to
    /// notice. The new physical id is supplied by the registry alias change
    /// itself, so no registry read is needed here; the backstop
    /// (<see cref="MaybeRefreshSourceIdentityAsync"/>) still covers the rare case
    /// where this notification is lost.
    /// </summary>
    public async Task NotifySourceIdentityChangedAsync(string newPhysicalTreeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(newPhysicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();

        var options = _optionsMonitor.Get(_treeName);
        var partitions = Math.Max(1, options.ReplogPartitions);
        EnsureScratchSized(partitions);

        await ApplyResolvedIdentityAsync(newPhysicalTreeId, partitions);

        // No pump re-arm is needed here: the steady-state phase timer is armed on
        // every activation (OnActivateCoreAsync), so an already-active shipper
        // picks the rebind up on its next tick (<= ShipPhaseTimerPeriod), and a
        // notification that reactivates a deactivated shipper has just re-armed
        // the timer as part of that activation.
    }

    /// <summary>
    /// Applies a resolved source physical identity to the shipper: updates the
    /// address the ship reads target and stamps the backstop clock, and, when the
    /// identity has changed since the cursors were last bound, resets the
    /// per-partition resume cursors and drops the cached shard-grain references so
    /// the shipper tails the new physical WAL from its log start. Shared by the
    /// gated per-tick backstop (<see cref="MaybeRefreshSourceIdentityAsync"/>) and
    /// the event-driven push (<see cref="NotifySourceIdentityChangedAsync"/>). A
    /// registry alias swap repoints a logical tree to a freshly minted physical
    /// tree; the persisted <see cref="ReplicationShipperState.PartitionCursors"/>
    /// are absolute sequence offsets into the retired log and are meaningless
    /// against the new one. Re-shipping from the new log start is safe because the
    /// peer merges every entry by <see cref="HybridLogicalClock"/> (LWW), making
    /// the replay idempotent.
    /// </summary>
    private async Task ApplyResolvedIdentityAsync(string physical, int partitions)
    {
        _walTreeId = physical;
        _sourceIdentityResolved = true;
        _lastSourceIdentityResolveUtc = _cursorFlushClock.GetUtcNow().UtcDateTime;

        var bound = state.State.BoundPhysicalTreeId;
        if (string.IsNullOrEmpty(bound))
        {
            // First bind for this activation lineage. Record the identity in
            // memory without an eager flush: a cold shipper (empty cursors) and
            // a warm one already resuming against this same physical log both
            // stay correct, and the slot is persisted opportunistically by the
            // next cursor-advance write. Avoiding a dedicated write here keeps
            // the shipper's deferred-persist accounting unchanged.
            state.State.BoundPhysicalTreeId = physical;
            return;
        }

        if (string.Equals(bound, physical, StringComparison.Ordinal))
        {
            return;
        }

        // Physical identity changed under the logical alias. Discard the retired
        // log's cursors, rebind, and drop the cached shard-grain references so
        // the next refill addresses the new physical WAL.
        Logger.LogWarning(
            "Shipper {Tree}/{Peer} source physical identity changed from '{OldPhysical}' to '{NewPhysical}'; "
            + "resetting partition cursors and re-shipping from the new source log.",
            _treeName, _peerClusterId, bound, physical);

        state.State.PartitionCursors.Clear();
        state.State.Cursor = HybridLogicalClock.Zero;
        state.State.BoundPhysicalTreeId = physical;
        await state.WriteStateAsync();

        if (_partitionGrainCache.Length >= partitions)
        {
            Array.Clear(_partitionGrainCache, 0, partitions);
        }
        else
        {
            Array.Clear(_partitionGrainCache, 0, _partitionGrainCache.Length);
        }
    }

    /// <summary>
    /// Test seam: bypasses Orleans activation and key parsing for
    /// direct unit tests against a fake Orleans context.
    /// </summary>
    internal void InitializeForTesting(string treeName, string peerClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        _treeName = treeName;
        _peerClusterId = peerClusterId;
        _keyParsed = true;
        _walTreeId = treeName;
    }

    /// <summary>
    /// Test seam: drives a single phase-pump tick synchronously - the
    /// exact hook the steady-state phase timer invokes. Unit tests use
    /// this to pump deterministically now that <see cref="OnDoorbellAsync"/>
    /// is a cheap edge-triggered wake that no longer ships inline.
    /// </summary>
    internal Task PumpForTestingAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ProcessNextPhaseAsync();
    }

    /// <summary>
    /// Test seam: substitutes the clock used to evaluate the wall-clock
    /// time dimension of the cursor-write coalescing rule so unit tests
    /// can advance time deterministically without a real wall-clock wait.
    /// </summary>
    internal void SetCursorFlushClockForTesting(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _cursorFlushClock = timeProvider;
    }

    /// <summary>
    /// Cursor-registry consumer-id prefix under which the shipper
    /// publishes the cross-cluster propagation of the receiver-side
    /// blocked-floor pin. Each (tree, peer) shipper publishes under
    /// the prefix concatenated with the peer cluster id, so multiple
    /// peer pins for the same tree do not collide. Cursor=Zero on
    /// every report so the registry's GC min(cursor) branch is not
    /// double-counted (the per-peer cursor advance already feeds
    /// that branch).
    /// </summary>
    private const string PeerBlockedFloorConsumerIdPrefix = "shipper:peer-blocked-floor:";

    /// <summary>
    /// Whether <see cref="PublishPeerBlockedFloorAsync"/> has reported
    /// at least once on this activation. Combined with
    /// <see cref="_peerBlockedFloorLast"/>, this lets the helper skip
    /// duplicate reports (the registry already enforces
    /// replace-semantics, but the per-tree semaphore inside
    /// <see cref="InMemoryWalCursorRegistry"/> still costs a
    /// Wait/Release pair per call we can avoid).
    /// </summary>
    private bool _peerBlockedFloorReported;

    /// <summary>Last receiver pin reported under <see cref="PeerBlockedFloorConsumerIdPrefix"/>; used to skip identical re-reports.</summary>
    private HybridLogicalClock? _peerBlockedFloorLast;

    /// <summary>
    /// Publishes <paramref name="receiverPin"/> (the value of
    /// <see cref="ReplicationAck.BlockedAtHlc"/> on the most recent
    /// successful ack) into the local cursor registry under the
    /// peer-specific consumer id, skipping when the pin has not
    /// changed. Failures are logged at Warning level and swallowed:
    /// a registry outage does not unwind the cursor advance the
    /// caller already booked, and a subsequent ack re-publishes the
    /// pin.
    /// </summary>
    private async Task PublishPeerBlockedFloorAsync(
        HybridLogicalClock? receiverPin,
        CancellationToken cancellationToken)
    {
        if (_peerBlockedFloorReported
            && Nullable.Equals(_peerBlockedFloorLast, receiverPin))
        {
            return;
        }

        var consumerId = PeerBlockedFloorConsumerIdPrefix + _peerClusterId;
        try
        {
            await _cursorRegistry.ReportCursorAsync(
                _treeName,
                consumerId,
                HybridLogicalClock.Zero,
                receiverPin,
                cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            _peerBlockedFloorReported = true;
            _peerBlockedFloorLast = receiverPin;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(
                ex,
                "Peer blocked-floor registry report failed for {Context}; pin {Pin} will be retried on the next ack.",
                LogContext,
                receiverPin);
        }
    }
}
