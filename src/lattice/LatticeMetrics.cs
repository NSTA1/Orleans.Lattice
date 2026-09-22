using System.Diagnostics.Metrics;
using System.Reflection;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/> instruments
/// for Orleans.Lattice. All instruments are published on a single <see cref="Meter"/>
/// named <see cref="MeterName"/> so an OpenTelemetry pipeline can subscribe once and
/// receive every Lattice metric.
/// </summary>
/// <remarks>
/// Instruments fall into five tiers:
/// <list type="bullet">
///   <item>
///     <b>Shard-level</b> - per-shard read / write / split counters, sourced from
///     <c>ShardRootGrain</c>. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
///   </item>
///   <item>
///     <b>Leaf-level</b> - write-state latency, scan latency, compaction duration,
///     tombstone churn (explicit deletes + TTL expiries reported separately),
///     and leaf-split counters sourced from <c>BPlusLeafGrain</c>. Tagged with
///     <see cref="TagTree"/> (leaf grain ids are too high-cardinality to publish directly).
///   </item>
///   <item>
///     <b>Cache</b> - hit / miss counters from <c>LeafCacheGrain</c>.
///   </item>
///   <item>
///     <b>Saga / coordinator / lifecycle</b> - terminal-state counters for
///     <c>SetManyAtomicAsync</c> sagas, long-running coordinator completions
///     (snapshot / resize / reshard / merge / compaction), and tree-lifecycle
///     transitions (deleted / recovered / purged).
///   </item>
///   <item>
///     <b>Events &amp; configuration</b> - event-publisher health (dispatches
///     succeeded vs. dropped) and per-tree configuration-change counters.
///   </item>
/// </list>
/// All durations are reported in <em>milliseconds</em> as <c>double</c>.
/// </remarks>
public static class LatticeMetrics
{
    /// <summary>
    /// The root meter / instrument / activity-source name for all Orleans.Lattice telemetry.
    /// All internal telemetry hooks must reference this constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice";

    /// <summary>Tag key for the logical tree id.</summary>
    public const string TagTree = "tree";

    /// <summary>Tag key for the participating-tree count of a cross-tree atomic write.</summary>
    public const string TagTreeCount = "tree_count";

    /// <summary>Tag key for the physical shard index.</summary>
    public const string TagShard = "shard";

    /// <summary>
    /// Tag key for the admission-control quota dimension
    /// (<see cref="DimensionKeys"/> or <see cref="DimensionBytes"/>) on the
    /// <c>orleans.lattice.admission.*</c> instruments.
    /// </summary>
    public const string TagDimension = "dimension";

    /// <summary><see cref="TagDimension"/> = <c>keys</c> (live-key admission dimension).</summary>
    public static readonly KeyValuePair<string, object?> DimensionKeys = new(TagDimension, "keys");

    /// <summary><see cref="TagDimension"/> = <c>bytes</c> (estimated-byte admission dimension).</summary>
    public static readonly KeyValuePair<string, object?> DimensionBytes = new(TagDimension, "bytes");

    /// <summary>
    /// Tag key for the WAL writer partition index. Distinct from
    /// <see cref="TagShard"/>: the writer partition is the producer-side
    /// routing key (one entry-batch per partition per call into
    /// <c>WalCommitLogWriter.AppendForPartitionAsync</c>) and lines up
    /// 1:1 with the destination shard's index, but is reported on the
    /// writer-layer instruments so a future fan-out shape that decouples
    /// the two does not silently overload <see cref="TagShard"/>.
    /// </summary>
    public const string TagPartition = "partition";

    /// <summary>
    /// Tag key for the durable leaf-materialiser <b>pin shard</b> index.
    /// <para>
    /// <b>This does not join to <see cref="TagShard"/> and must never be
    /// reported as it.</b> <see cref="TagShard"/> is the physical WAL shard: a
    /// mutation's partition, chosen by hashing the mutation key modulo
    /// <see cref="LatticeOptions.WalPartitions"/>, and it is the axis
    /// <see cref="WalEntriesTrimmed"/> is attributed on. A pin shard is chosen
    /// by hashing the <i>consumer id</i> modulo
    /// <see cref="LatticeOptions.WalMaterialiserPinShards"/>
    /// (<c>WalMaterialiserPinRouting.ShardKey</c>). The two are unrelated
    /// routing functions over unrelated inputs.
    /// </para>
    /// <para>
    /// The hazard is concrete rather than theoretical, because both options
    /// default to <b>8</b>: every series on both axes is labelled <c>0</c>
    /// through <c>7</c>, so a dashboard or ad-hoc query that joins
    /// <c>pin_shard=N</c> to <c>shard=N</c> returns a well-formed, entirely
    /// meaningless correlation rather than an obvious error. This follows the
    /// precedent set by <see cref="TagPartition"/>, which exists for the weaker
    /// version of the same problem - a routing key that <i>does</i> line up 1:1
    /// with the shard index today, separated anyway so a later fan-out shape
    /// could not silently overload <see cref="TagShard"/>.
    /// </para>
    /// </summary>
    public const string TagPinShard = "pin_shard";

    /// <summary>Tag key for the operation kind (e.g. <c>keys</c> or <c>entries</c> on scan histograms).</summary>
    public const string TagOperation = "operation";

    /// <summary>
    /// Tag key for the terminal outcome of a saga / coordinator (e.g.
    /// <c>committed</c>, <c>compensated</c>, <c>failed</c> on
    /// <see cref="AtomicWriteCompleted"/>).
    /// </summary>
    public const string TagOutcome = "outcome";

    /// <summary>
    /// Tag key for a discriminated-kind dimension (e.g. coordinator kind,
    /// tree-lifecycle kind, event kind).
    /// </summary>
    public const string TagKind = "kind";

    /// <summary>Tag key for the reason a publication / operation was dropped.</summary>
    public const string TagReason = "reason";

    /// <summary>
    /// Tag key for the cache surface that released a leaf's lazily hydrated
    /// snapshot frame. Paired with <see cref="TagReason"/> on
    /// <see cref="LeafBisectRefusals"/>, where it is what separates a leaf that
    /// never attached a frame from one whose frame an unrelated whole-leaf
    /// operation consumed.
    /// </summary>
    public const string TagDetachSeam = "detach_seam";

    /// <summary>
    /// Tag key for the class of failure that faulted an operation. Paired with
    /// <see cref="TagOutcome"/> = <c>faulted</c> on
    /// <see cref="LeafSplitAttempts"/>, where it routes the remedy: a division
    /// that ran out of memory and one that ran out of time present identically
    /// as a bare fault, yet the first calls for a smaller leaf or a larger
    /// hydration budget and the second for the storage path to be examined.
    /// Deliberately distinct from <see cref="TagReason"/>, which names why
    /// something was <em>refused</em> - a refusal is a decision the code took,
    /// whereas a failure class is a property of an exception it caught.
    /// </summary>
    public const string TagFailureClass = "failure_class";

    /// <summary>
    /// Tag key for the decision a control loop reached on one observation
    /// pass (e.g. <c>admitted</c>, <c>not_over_split</c>, <c>backpressure</c>
    /// on <see cref="ShardHealingDecisions"/>). Distinct from
    /// <see cref="TagReason"/>, which names only why something was refused:
    /// a decision dimension carries the admitting value too, so the series is
    /// a complete account of every pass rather than of the failures.
    /// </summary>
    public const string TagDecision = "decision";

    /// <summary>
    /// Tag key for a configuration dimension name (e.g.
    /// <c>publish_events</c> on <see cref="ConfigChanged"/>).
    /// </summary>
    public const string TagConfig = "config";

    /// <summary>
    /// Tag key for a per-step dimension on the leaf commit path
    /// (e.g. <c>wal</c>, <c>apply</c>, <c>observer</c> on
    /// <see cref="LeafCommitDuration"/>) so operators can attribute
    /// total commit latency to its constituent stages.
    /// </summary>
    public const string TagStep = "step";

    /// <summary>
    /// Tag key for the concrete <see cref="IMutationObserver"/> implementation
    /// a measurement is attributed to on <see cref="ObserverDuration"/>. The
    /// value is the observer's CLR type name (<see cref="Type.FullName"/>,
    /// falling back to the short <c>Type.Name</c> for a type that reports
    /// none), matching the identifier the dispatcher already writes into its
    /// swallow-and-log warning. Cardinality is bounded by the number of
    /// observers registered in the silo's DI container, which is a
    /// deployment-time constant.
    /// </summary>
    public const string TagObserver = "observer";

    /// <summary>
    /// Tag key for the trigger that initiated a tombstone-compaction pass
    /// (e.g. <c>reminder</c>, <c>ratio</c>, <c>size</c>, <c>operator</c>
    /// on <see cref="CompactionPassDuration"/>). The tag is also emitted on
    /// <see cref="LeafCompactionDuration"/>, <see cref="LeafTombstonesReaped"/>,
    /// and <see cref="LeafTombstonesExpired"/> when at least one policy knob
    /// (<c>MinTombstoneRatioForCompaction</c> or
    /// <c>MaxLeafEntriesBeforeForcedCompaction</c>) is non-default; when
    /// every policy knob holds its default the tag is omitted so existing
    /// dashboards that filter on <c>trigger=""</c> keep matching.
    /// <para>
    /// <b>Also the trigger that withheld a WAL replay permit</b> on
    /// <see cref="WalReplayPermitAdaptations"/>
    /// (<see cref="PermitAdaptationTriggerFault"/> or
    /// <see cref="PermitAdaptationTriggerOccupancy"/>), issue #2883. The two uses
    /// share only the key: the value vocabularies are disjoint, and no query
    /// spans both instruments. Note that the <c>restored</c> arm of that counter
    /// carries no trigger at all and so matches <c>trigger=""</c> - deliberately,
    /// for the reason given on that instrument.
    /// </para>
    /// </summary>
    public const string TagTrigger = "trigger";

    /// <summary>
    /// Tag key for the path a tombstone-compaction pass took through a
    /// shard's leaves: <c>walk</c> for the legacy chain walk and
    /// <c>dirty-set</c> for the dirty-leaves fast path that consults
    /// the shard-root dirty-leaves snapshot. Emitted on
    /// <see cref="CompactionLeavesVisited"/>.
    /// </summary>
    public const string TagPath = "path";

    /// <summary>
    /// Tag key for a leaf-grain identifier on per-leaf instruments
    /// (e.g. <see cref="LeafTombstoneRatio"/>). Cardinality follows the
    /// same caveats as any per-leaf tag - operators that run very wide
    /// trees should expect to either drop the tag at the OpenTelemetry
    /// view layer or sample it.
    /// </summary>
    public const string TagLeaf = "leaf";

    /// <summary>
    /// Tag key for the Orleans grain type of a call's target, on the grain-call
    /// observation instruments (<see cref="GrainCallOutstandingDepth"/> and
    /// <see cref="GrainCallDuration"/>). The value is the runtime grain type
    /// name (<c>bplusleaf</c>, <c>latticeregistry</c>, and so on), <b>not</b> a
    /// per-activation key, so cardinality is bounded by the number of grain
    /// classes the deployed application defines and does not grow with traffic
    /// or with the size of a tree.
    /// </summary>
    public const string TagGrainType = "grain_type";

    /// <summary>
    /// Tag key for the activation temperature of an activation-time leaf
    /// materialiser replay on <see cref="LeafActivationReplays"/>: either
    /// <see cref="ActivationTemperatureCold"/> or
    /// <see cref="ActivationTemperatureWarm"/>. Cardinality is exactly two, so
    /// the tag splits each existing per-tree series in half rather than
    /// multiplying the series count.
    /// <para>
    /// Both arms are emitted from the one call site in
    /// <c>BPlusLeafGrain.OnActivateAsync</c>, under the same condition, which
    /// is why the cold:warm ratio is a property of a <b>single</b> scrape of
    /// this counter. Two independent counters would not give that: they can be
    /// scraped at different instants, reset independently, or one can be
    /// dropped by a pipeline, and the quotient of two such series is not
    /// guaranteed to be a ratio of anything.
    /// </para>
    /// </summary>
    public const string TagActivationTemperature = "activation_temperature";

    /// <summary>
    /// <see cref="TagActivationTemperature"/> = <c>cold</c>: the activation
    /// neither rehydrated from a leaf snapshot nor resumed a populated entry
    /// cache, so the replay starts from the <c>-1</c> sentinel and covers the
    /// whole readable WAL window.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ActivationTemperatureCold = new(TagActivationTemperature, "cold");

    /// <summary>
    /// <see cref="TagActivationTemperature"/> = <c>warm</c>: the activation
    /// resumed from an anchor - a snapshot rehydrate or an already-populated
    /// entry cache - so the replay covers only the tail above that anchor.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ActivationTemperatureWarm = new(TagActivationTemperature, "warm");

    /// <summary>
    /// Tag key for the Orleans <c>DeactivationReasonCode</c> observed on a
    /// grain's deactivation hook, used by
    /// <see cref="LeafDeactivationCheckpointDelta"/>. Cardinality is bounded by
    /// the Orleans enum, so it is safe to tag with.
    /// <para>
    /// Deliberately DISTINCT from <see cref="TagReason"/>. The two carry
    /// unrelated value vocabularies - this one names how a grain was torn down,
    /// while <see cref="TagReason"/> on
    /// <see cref="LeafActivationFailures"/> names how an activation failed
    /// (<c>canceled</c> / <c>faulted</c>). Sharing one key would invite a
    /// reader to join two series that have no value in common.
    /// </para>
    /// </summary>
    public const string TagDeactivationReason = "deactivation_reason";

    /// <summary><see cref="TagReason"/> = <c>canceled</c> on <see cref="LeafActivationFailures"/>.</summary>
    public static readonly KeyValuePair<string, object?> ActivationFailureCanceled = new(TagReason, "canceled");

    /// <summary>
    /// <see cref="TagReason"/> = <c>canceled_awaiting_permit</c> on
    /// <see cref="LeafActivationFailures"/>: the activation was cancelled while
    /// still queued for the per-silo replay concurrency permit, so it never
    /// began replaying and had no in-progress work to lose.
    /// <para>
    /// Kept distinct from <see cref="ActivationFailureCanceled"/> deliberately.
    /// The two describe different events - one lost work in flight, the other
    /// never started - and the width of this queue window is set by the same
    /// replay saturation under investigation, so folding them together would
    /// let a rise in queueing masquerade as a rise in abandoned replays.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> ActivationFailureCanceledAwaitingPermit =
        new(TagReason, "canceled_awaiting_permit");

    /// <summary>
    /// <see cref="TagReason"/> = <c>canceled_resolving_options</c> on
    /// <see cref="LeafActivationFailures"/>: the activation was cancelled while
    /// resolving the tree's options, which happens BEFORE the replay permit is
    /// requested. It never reached the permit queue and the gate was never
    /// contended on its behalf.
    /// <para>
    /// Split out of <see cref="ActivationFailureCanceledAwaitingPermit"/> for
    /// issue #2770, because that value was reported for this arm too and made
    /// the series unreadable in the one situation it exists for. Options
    /// resolution calls <c>ILatticeRegistry.GetEntryAsync</c>, a non-reentrant
    /// cluster singleton every cold activation queues behind, so under a cold
    /// start this arm can be the whole population while the replay gate sits
    /// completely idle - and the folded series reported that as
    /// "queued for a replay permit", indicting the gate.
    /// </para>
    /// <para>
    /// The reading to take from the split: this value rising means activations
    /// are serialised behind a shared dependency, whereas
    /// <see cref="ActivationFailureCanceledAwaitingPermit"/> rising means the
    /// replay gate itself is genuinely saturated. Those call for opposite
    /// remedies, which is why one value could not carry both.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> ActivationFailureCanceledResolvingOptions =
        new(TagReason, "canceled_resolving_options");

    /// <summary>
    /// <see cref="TagReason"/> = <c>canceled_rehydrating_snapshot</c> on
    /// <see cref="LeafActivationFailures"/>: the activation was cancelled in
    /// the snapshot rehydrate, which runs before replay admission is entered.
    /// <para>
    /// Added by issue #2770. This arm was previously not counted AT ALL: the
    /// rehydrate ran outside the observed region, so a cancellation there
    /// escaped without incrementing <see cref="LeafActivationFailures"/> under
    /// any value. An arm that is invisible is worse than one that is
    /// mislabelled, because a mislabelled arm at least shows up in the total.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> ActivationFailureCanceledRehydratingSnapshot =
        new(TagReason, "canceled_rehydrating_snapshot");

    /// <summary><see cref="TagReason"/> = <c>faulted</c> on <see cref="LeafActivationFailures"/>.</summary>
    public static readonly KeyValuePair<string, object?> ActivationFailureFaulted = new(TagReason, "faulted");

    /// <summary>
    /// <see cref="TagReason"/> = <c>refused_replay_admission</c> on
    /// <see cref="LeafActivationFailures"/>: the activation was refused a place
    /// in the WAL replay permit queue because the admitted-waiter bound derived
    /// from <see cref="LatticeOptions.WalReplayPermitQueueDepthPerPermit"/> was
    /// already reached and the queue was also failing to drain within
    /// <see cref="LatticeOptions.WalReplayPermitMaxQueueWait"/>. Issues #3284
    /// and #3290.
    /// <para>
    /// <b>Distinct from <see cref="ActivationFailureCanceledAwaitingPermit"/>,
    /// and the pair is the whole point of the arm.</b> That value means an
    /// activation was admitted, waited, and ran out of request budget while
    /// waiting - the gate was contended and it lost. This value means the
    /// activation was never admitted at all, because the queue in front of the
    /// gate was already deeper than could be served in time. Before admission
    /// control the second population did not exist: every arrival was admitted
    /// and the whole backlog surfaced under the first value, which made a queue
    /// nobody was bounding indistinguishable from a gate that was merely busy.
    /// </para>
    /// <para>
    /// This arm rising is <b>the bound working</b>, not a fault, and it is the
    /// cheap failure: a refusal is immediate and the caller retries after a
    /// backoff, where an admitted-but-doomed waiter holds an activation for the
    /// whole request deadline and then enqueues its replacement.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> ActivationFailureRefusedReplayAdmission =
        new(TagReason, "refused_replay_admission");

    /// <summary>
    /// Tag key for the storage-provider commit phase
    /// (e.g. <c>phase1</c> = per-batch partition transaction,
    /// <c>phase2</c> = manifest partition transaction). Emitted on
    /// <see cref="ProviderCommitDuration"/> and
    /// <see cref="ProviderRetryExhausted"/>.
    /// </summary>
    public const string TagPhase = "phase";

    /// <summary>
    /// Tag key for the sub-stage label inside
    /// <c>ShardRootGrain.AppendTxTerminalAsync</c>. Emitted on
    /// <see cref="SagaBroadcastShardStageDuration"/>. Values:
    /// <c>resolve</c> (step 1 affected-leaves resolution),
    /// <c>hlc</c> (step 2 <c>ComputeTerminalHlcAsync</c> fan-out + tick),
    /// <c>wal</c> (step 3 commit-log adapter append; absent when no
    /// adapter is registered), and <c>fanout</c> (step 4 per-leaf
    /// <c>ApplyTxTerminalAsync</c> dispatch + shadow-forward).
    /// </summary>
    public const string TagStage = "stage";

    /// <summary>
    /// Tag key for an Azure Tables HTTP status string on
    /// <see cref="ProviderRetryExhausted"/>. Cardinality is bounded by
    /// the small set of HTTP status codes the SDK surfaces on the WAL
    /// hot path; an unmapped status reports as <c>unknown</c>.
    /// </summary>
    public const string TagStatus = "status";

    /// <summary>
    /// Tag key for the activation's effective <c>WalPartitions</c>
    /// setting. Emitted on the Phase A WAL / saga instruments so a
    /// single Prometheus / dashboard query can pivot the same metric
    /// stream across the diagnostic attribution sweep
    /// (<c>WalPartitions in {1, 4, 16}</c>). Captured once on grain
    /// activation and reused per record - no per-call allocation.
    /// </summary>
    public const string TagWalPartitions = "wal_partitions";

    /// <summary>
    /// Tag key for the activation's effective <c>WalMaxPendingBatches</c>
    /// setting. Same allocation-free, activation-cached pattern as
    /// <see cref="TagWalPartitions"/>; lets the attribution sweep
    /// distinguish runs that vary the in-flight-flush ceiling.
    /// </summary>
    public const string TagWalMaxPendingBatches = "wal_max_pending_batches";

    /// <summary>
    /// Tag key for the Azure Tables provider's effective
    /// <c>PipelinePhaseTwoCommits</c> setting (the values <c>true</c>
    /// / <c>false</c>). Emitted by the Azure Tables WAL provider on
    /// the Phase A provider instruments so dashboards can pivot the
    /// same series between synchronous and pipelined phase-2 modes.
    /// </summary>
    public const string TagPipelinePhaseTwo = "pipeline_phase2";

    /// <summary>
    /// The meter that owns every Lattice instrument. Exposed publicly so integration
    /// tests and custom OpenTelemetry exporters can subscribe by reference rather
    /// than by name.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This field must stay above every instrument declared below it, and every
    /// instrument must be constructed from it.</b> Static field initialisers execute
    /// in declaration order, and <c>MeterListener.Start()</c> raises
    /// <c>InstrumentPublished</c> for already-existing instruments outside the lock
    /// that registers the listener. A listener callback that is the first code in the
    /// process to touch this class therefore runs this initialiser re-entrantly, part
    /// way through: any field declared below the instrument being published is still
    /// <see langword="null"/> at that moment.
    /// </para>
    /// <para>
    /// The many fixtures that match on
    /// <c>ReferenceEquals(instrument.Meter, LatticeMetrics.Meter)</c> would then
    /// compare against <see langword="null"/>, never enable the instrument, and record
    /// zero measurements without throwing - surfacing as a missing production emission
    /// rather than a broken harness. Building every instrument from this field keeps
    /// any such mistake loud instead: the reordered initialiser throws
    /// <see cref="TypeInitializationException"/> on first use.
    /// </para>
    /// <para>
    /// Enforced by <c>MeterFieldDeclarationOrderTests</c>; both orderings are
    /// demonstrated by <c>MeterListeningTests</c>. See the Metrics section of
    /// <c>.github/copilot-instructions.md</c>.
    /// </para>
    /// </remarks>
    public static readonly Meter Meter = new(MeterName);

    // --- Process identity (deployment liveness) ----------------------------------

    /// <summary>Tag key for the Orleans.Lattice package version the running process was built from.</summary>
    public const string TagVersion = "version";

    /// <summary>
    /// Tag key for the full 40-character git commit sha the running process was
    /// built from.
    /// </summary>
    public const string TagSha = "sha";

    /// <summary>
    /// Value reported for <see cref="TagVersion"/> or <see cref="TagSha"/> when the
    /// running assembly carries no usable build stamp.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Reported as an explicit sentinel rather than an empty string or an absent
    /// series, so "this build cannot identify itself" stays distinguishable from
    /// "nothing is publishing build identity at all". The two have different
    /// causes - a build-system regression versus an undeployed image - and an
    /// absent series cannot tell them apart.
    /// </para>
    /// <para>
    /// <b>This value must never reach a real deployment</b>, because an info gauge
    /// carrying a placeholder is worse than one that is missing: it reads as a
    /// working detector while identifying nothing. It survives here only as a
    /// last-resort degradation, and
    /// <c>BuildInfoMetricTests.Build_info_sha_is_a_full_forty_character_sha</c>
    /// fails outright on it, so a build configuration that stops stamping
    /// <c>SourceRevisionId</c> reddens in CI rather than shipping a gauge that
    /// lies quietly.
    /// </para>
    /// </remarks>
    public const string BuildMetadataUnknown = "unknown";

    /// <summary>Canonical name of <see cref="BuildInfo"/>.</summary>
    public const string BuildInfoName = "orleans.lattice.build.info";

    /// <summary>
    /// The version and commit sha the running assembly was compiled from, resolved
    /// once at type initialisation.
    /// </summary>
    /// <remarks>
    /// Declared above <see cref="BuildVersion"/>, <see cref="BuildCommitSha"/> and
    /// <see cref="BuildInfo"/>, all of which read it. See the remarks on
    /// <see cref="Meter"/> for why declaration order is load-bearing for anything
    /// an observable callback reaches.
    /// </remarks>
    private static readonly (string Version, string Commit) BuildIdentity = ResolveBuildIdentity();

    /// <summary>
    /// Package version of the running Orleans.Lattice assembly, or
    /// <see cref="BuildMetadataUnknown"/> when the assembly carries no informational
    /// version.
    /// </summary>
    public static readonly string BuildVersion = BuildIdentity.Version;

    /// <summary>
    /// Full 40-character git commit sha the running Orleans.Lattice assembly was
    /// compiled from, or <see cref="BuildMetadataUnknown"/> when the build carried no
    /// source-revision stamp.
    /// </summary>
    /// <remarks>
    /// Sourced from the <c>+&lt;sha&gt;</c> build-metadata suffix the SDK appends to
    /// <see cref="AssemblyInformationalVersionAttribute"/> from <c>SourceRevisionId</c>.
    /// This is a <b>compile-time</b> stamp, so it identifies the image rather than the
    /// checkout the process happens to be running beside.
    /// </remarks>
    public static readonly string BuildCommitSha = BuildIdentity.Commit;

    /// <summary>
    /// Constant <c>1</c> carrying the identity of the running build as tags
    /// (<see cref="TagVersion"/>, <see cref="TagSha"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This instrument answers a question no other instrument in the estate can.</b>
    /// "Is the build under test actually the build that is running?" is a property of
    /// the <i>process</i>, not of any feature. Every other instrument here reports
    /// something a feature did, so each one is silent until that feature is exercised -
    /// which makes every one of them unusable as a deployment signal. Before this
    /// instrument existed, the only way to answer the question from a scrape was to
    /// pick some feature metric and hope it had fired, which conflates "the image did
    /// not deploy" with "the code deployed and that feature was simply never reached".
    /// Those are opposite conclusions drawn from byte-identical evidence.
    /// </para>
    /// <para>
    /// It therefore emits <b>exactly one measurement, unconditionally, on every
    /// collection</b>, from process start, with no registry to populate and no work to
    /// wait for. It has no empty state to prime: absence of this series means the
    /// process is not running or is not exporting at all, and means nothing else. That
    /// is the entire point, and it is why this gauge is deliberately not modelled on
    /// any of its neighbours.
    /// </para>
    /// <para>
    /// The value carries no information and is always <c>1</c>; all of the content is
    /// in the tags. This is the conventional shape for an info-style metric, and it
    /// makes the series safe to join against in a query.
    /// </para>
    /// </remarks>
    public static readonly ObservableGauge<long> BuildInfo =
        Meter.CreateObservableGauge(BuildInfoName, ObserveBuildInfo, unit: "{build}",
            description: "Always 1, tagged with the version and full commit sha the running process was built from. Process-scoped deployment liveness: emitted unconditionally from process start, so its absence means the process is not running or not exporting, and never that a feature went unexercised.");

    private static (string Version, string Commit) ResolveBuildIdentity()
    {
        var informational = typeof(LatticeMetrics).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;

        if (string.IsNullOrWhiteSpace(informational))
        {
            return (BuildMetadataUnknown, BuildMetadataUnknown);
        }

        var plus = informational.IndexOf('+');
        if (plus < 0)
        {
            return (informational, BuildMetadataUnknown);
        }

        var version = informational[..plus];
        var commit = informational[(plus + 1)..];

        return (
            string.IsNullOrWhiteSpace(version) ? BuildMetadataUnknown : version,
            string.IsNullOrWhiteSpace(commit) ? BuildMetadataUnknown : commit);
    }

    private static IEnumerable<Measurement<long>> ObserveBuildInfo()
    {
        yield return new Measurement<long>(
            1,
            new KeyValuePair<string, object?>(TagVersion, BuildVersion),
            new KeyValuePair<string, object?>(TagSha, BuildCommitSha),
            LatticeTenantLabel.Platform);
    }

    // --- Shard-level counters (ShardRootGrain) -----------------------------------

    /// <summary>Counter incremented on every read operation observed by a shard root.</summary>
    public static readonly Counter<long> ShardReads =
        Meter.CreateCounter<long>("orleans.lattice.shard.reads", unit: "{op}",
            description: "Read operations served by a shard root (GetAsync, ExistsAsync, scan, count, etc.).");

    /// <summary>
    /// Counter incremented once per write <b>operation</b> observed by a shard root.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is a per-<b>operation</b> counter, not a per-record one. A batched or
    /// bulk operation - <c>SetManyAsync</c>, <c>MergeManyAsync</c>,
    /// <c>SetManyWherePredicateAsync</c>, <c>DeleteRangeAsync</c>,
    /// <c>BulkLoadAsync</c>, <c>BulkLoadRawAsync</c>, <c>BulkAppendAsync</c> -
    /// contributes exactly <b>one</b> increment regardless of how many entries it
    /// carries, so a 5000-record bulk import ticks this counter on the order of
    /// (shards touched x bulk operations), not 5000.
    /// </para>
    /// <para>
    /// Use <see cref="ShardRecordsWritten"/> when you need the record rate. Plotting
    /// this instrument as "write throughput" on a batch-heavy or bulk-ingesting
    /// estate under-represents the real volume; plot both, or label this one
    /// explicitly as operations per second.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> ShardWrites =
        Meter.CreateCounter<long>("orleans.lattice.shard.writes", unit: "{op}",
            description: "Write operations served by a shard root (SetAsync, DeleteAsync, MergeManyAsync, BulkLoadAsync, etc.). One increment per operation: a batched or bulk operation counts once regardless of entry count - see orleans.lattice.shard.records_written for the per-record rate.");

    /// <summary>
    /// Counter incremented by the number of individual <b>records</b> each write
    /// operation carried - the per-record companion to <see cref="ShardWrites"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A single-key write (<c>SetAsync</c>, <c>DeleteAsync</c>,
    /// <c>SetIfVersionAsync</c>, <c>GetOrSetAsync</c>, <c>ApplyCrdtDeltaAsync</c>)
    /// contributes 1. A batched or bulk operation contributes its entry count, so a
    /// 5000-record bulk import contributes 5000 here while
    /// <see cref="ShardWrites"/> advances by only the number of bulk operations.
    /// Together the two make both the operation rate and the record rate
    /// observable, and their ratio is the effective batch size.
    /// </para>
    /// <para>
    /// For the two operations whose affected-record count is only known once the
    /// operation completes - <c>DeleteRangeAsync</c> and
    /// <c>SetManyWherePredicateAsync</c> - the increment is the number of records
    /// actually tombstoned or matched, and is published after the operation
    /// succeeds. Every other path publishes the entry count it was handed. A write
    /// that throws before completing contributes to <see cref="ShardWrites"/> but
    /// not here.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> ShardRecordsWritten =
        Meter.CreateCounter<long>("orleans.lattice.shard.records_written", unit: "{record}",
            description: "Individual records written by a shard root, incremented by the entry count of each write operation (1 for a single-key write, the batch size for SetManyAsync / MergeManyAsync / bulk load, the affected count for DeleteRangeAsync / SetManyWherePredicateAsync). The per-record companion to orleans.lattice.shard.writes.");

    /// <summary>
    /// Counter incremented once per <c>IShardRootGrain.GetShardProjectionDigestAsync</c>
    /// call, tagged with <see cref="TagTree"/> and <see cref="TagShard"/>. Lets operators
    /// (and integration tests) verify that a whole-tree poll of
    /// <see cref="ILattice.GetLeafProjectionDigestAsync"/> issues exactly one grain
    /// call per physical shard - the chained-fold design's headline operational
    /// invariant - rather than degrading to an O(shardCount x leafCount) walk.
    /// </summary>
    public static readonly Counter<long> ShardDigestReads =
        Meter.CreateCounter<long>("orleans.lattice.shard.digest_reads", unit: "{op}",
            description: "Projection-digest reads served by a shard root (one per GetShardProjectionDigestAsync call).");

    /// <summary>
    /// Counter incremented once for every leaf-side projection-digest decision
    /// point, tagged with <see cref="TagTree"/> and <see cref="TagPath"/>:
    /// <list type="bullet">
    ///   <item><description><c>coalesced_scheduled</c> - the leaf scheduled a
    ///   fresh one-shot timer (first dirty mutation inside a new coalescing
    ///   window). Future <c>coalesced_skipped</c> increments share its
    ///   eventual <c>coalesced_fired</c> publish.</description></item>
    ///   <item><description><c>coalesced_skipped</c> - a dirty mutation arrived
    ///   while a coalesced publish was already scheduled, so the
    ///   cross-grain hop was deferred onto the existing window. This is the
    ///   "publishes saved" surface that justifies the coalescing default.</description></item>
    ///   <item><description><c>coalesced_fired</c> - the coalescing timer
    ///   tick issued the cross-grain
    ///   <c>OnChildDigestPublishedAsync</c> RPC to the parent. One per
    ///   window per leaf (unless an inline publish or a graceful flush
    ///   cancelled the timer first).</description></item>
    ///   <item><description><c>inline</c> - the leaf issued the cross-grain
    ///   publish synchronously (either because <c>DigestCoalescingWindowMs</c>
    ///   is zero, the timer registration failed in a test harness, or the
    ///   call came from a structural caller via
    ///   <c>PublishDigestUpwardInlineAsync</c>).</description></item>
    ///   <item><description><c>deactivation_flush</c> - the leaf's graceful
    ///   <c>OnDeactivateAsync</c> drained a pending coalesced publish before
    ///   the activation tore down.</description></item>
    /// </list>
    /// <para>
    /// The headline operational invariant the coalescing path was designed
    /// for is "N writes inside one window produce one cross-grain hop". That
    /// translates to <c>coalesced_scheduled + coalesced_fired</c> per window
    /// regardless of write count, with <c>coalesced_skipped</c> absorbing
    /// the remaining N-1 dirtying mutations.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafDigestPublishes =
        Meter.CreateCounter<long>("orleans.lattice.leaf.digest.publishes", unit: "{publish}",
            description: "Leaf-side projection-digest publish decisions, partitioned by path (coalesced scheduling, skip, fire, inline, deactivation flush).");

    /// <summary><see cref="TagPath"/> = <c>coalesced_scheduled</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathCoalescedScheduledTag = new(TagPath, "coalesced_scheduled");

    /// <summary><see cref="TagPath"/> = <c>coalesced_skipped</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathCoalescedSkippedTag = new(TagPath, "coalesced_skipped");

    /// <summary><see cref="TagPath"/> = <c>coalesced_fired</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathCoalescedFiredTag = new(TagPath, "coalesced_fired");

    /// <summary><see cref="TagPath"/> = <c>inline</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathInlineTag = new(TagPath, "inline");

    /// <summary><see cref="TagPath"/> = <c>deactivation_flush</c> on <see cref="LeafDigestPublishes"/>.</summary>
    public static readonly KeyValuePair<string, object?> PathDeactivationFlushTag = new(TagPath, "deactivation_flush");

    /// <summary>
    /// Counter incremented once per adaptive shard-split commit, fired from
    /// <c>TreeShardSplitGrain.FinaliseAsync</c> immediately after the shard
    /// map swap succeeds.
    /// </summary>
    public static readonly Counter<long> ShardSplitsCommitted =
        Meter.CreateCounter<long>("orleans.lattice.shard.splits_committed", unit: "{split}",
            description: "Adaptive shard-split commits (ShardMap swap published).");

    /// <summary>
    /// Counter incremented once per online shard-consolidation commit, fired
    /// from <c>TreeShardConsolidationGrain.FinaliseAsync</c> immediately after
    /// the terminal state write succeeds, so an increment always corresponds to
    /// a durably-committed fold.
    /// <para>
    /// The exact inverse of <see cref="ShardSplitsCommitted"/>, and the metric
    /// that proves a tree an over-eager splitter shattered is actually being
    /// healed: plotted together, a sustained gap between the two is a tree
    /// whose physical shard count is still climbing. Tagged
    /// <see cref="TagShard"/> with the <em>donor</em> shard index - the shard
    /// being retired from the routing map.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardConsolidationsCommitted =
        Meter.CreateCounter<long>("orleans.lattice.shard.consolidations_committed", unit: "{consolidation}",
            description: "Online shard-consolidation commits (donor shard retired from the ShardMap).");

    /// <summary>
    /// Per-tree count of physical shards above the tree's configured base
    /// shard count, sampled once every healing-orchestrator sweep. The healing
    /// <em>work outstanding</em>: how many folds separate the tree from its
    /// intended shape.
    /// <para>
    /// A tree is healed exactly when this reaches zero, so "trees healed" is
    /// read off this instrument directly (<c>count</c> of series at zero) with
    /// no second instrument to keep consistent. Plotted alongside
    /// <see cref="ShardConsolidationsCommitted"/> - the reclaimed-shard rate -
    /// it answers both halves of the question: how much damage is left, and is
    /// it going down. Tagged <see cref="TagTree"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> ShardHealingBacklog =
        Meter.CreateHistogram<int>("orleans.lattice.shard.healing.backlog", unit: "{shard}",
            description: "Per-tree physical shards above the configured base count, sampled every healing sweep.");

    /// <summary>
    /// Counter incremented exactly once per healing-orchestrator sweep with
    /// the decision that sweep reached, tagged <see cref="TagTree"/> and
    /// <see cref="TagDecision"/>.
    /// <para>
    /// The series whose rate is currently non-zero for a tree <em>is</em> that
    /// tree's current healing decision, so an operator can tell a tree that
    /// needs no healing (<c>not_over_split</c>) from one that needs healing and
    /// is being held back (<c>skewed_load</c>, <c>backpressure</c>,
    /// <c>cooldown</c>, <c>split_in_flight</c>, <c>at_capacity</c>) from one
    /// where the mechanism is off (<c>disabled</c>, <c>admission_closed</c>).
    /// Without it, a tree that never heals is indistinguishable from a tree
    /// that never needed to.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardHealingDecisions =
        Meter.CreateCounter<long>("orleans.lattice.shard.healing.decisions", unit: "{decision}",
            description: "Healing-orchestrator sweeps by decision (one increment per tree per sweep).");

    /// <summary><see cref="TagDecision"/> = <c>admitted</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingAdmittedDecisionTag = new(TagDecision, "admitted");

    /// <summary><see cref="TagDecision"/> = <c>disabled</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingDisabledDecisionTag = new(TagDecision, "disabled");

    /// <summary><see cref="TagDecision"/> = <c>admission_closed</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingAdmissionClosedDecisionTag = new(TagDecision, "admission_closed");

    /// <summary><see cref="TagDecision"/> = <c>not_over_split</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingNotOverSplitDecisionTag = new(TagDecision, "not_over_split");

    /// <summary><see cref="TagDecision"/> = <c>skewed_load</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingSkewedLoadDecisionTag = new(TagDecision, "skewed_load");

    /// <summary><see cref="TagDecision"/> = <c>split_in_flight</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingSplitInFlightDecisionTag = new(TagDecision, "split_in_flight");

    /// <summary><see cref="TagDecision"/> = <c>tree_maintenance</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingTreeMaintenanceDecisionTag = new(TagDecision, "tree_maintenance");

    /// <summary><see cref="TagDecision"/> = <c>cooldown</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingCooldownDecisionTag = new(TagDecision, "cooldown");

    /// <summary><see cref="TagDecision"/> = <c>backpressure</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingBackpressureDecisionTag = new(TagDecision, "backpressure");

    /// <summary><see cref="TagDecision"/> = <c>at_capacity</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingAtCapacityDecisionTag = new(TagDecision, "at_capacity");

    /// <summary><see cref="TagDecision"/> = <c>no_foldable_pair</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingNoFoldablePairDecisionTag = new(TagDecision, "no_foldable_pair");

    /// <summary><see cref="TagDecision"/> = <c>not_observed</c> on <see cref="ShardHealingDecisions"/>.</summary>
    public static readonly KeyValuePair<string, object?> HealingNotObservedDecisionTag = new(TagDecision, "not_observed");

    // --- Leaf-level instruments (BPlusLeafGrain) ---------------------------------

    /// <summary>
    /// Histogram of <c>IPersistentState.WriteStateAsync</c> durations observed by
    /// <c>BPlusLeafGrain</c>. Captures storage-provider write latency from the
    /// perspective of the leaf grain that issued the persist.
    /// </summary>
    public static readonly Histogram<double> LeafWriteDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.write.duration", unit: "ms",
            description: "Duration of IPersistentState.WriteStateAsync calls from BPlusLeafGrain.");

    /// <summary>
    /// Histogram of leaf-level scan durations. Tagged with <see cref="TagOperation"/>
    /// = <c>keys</c> (for <c>GetKeysAsync</c>) or <c>entries</c> (for
    /// <c>GetEntriesAsync</c>).
    /// </summary>
    public static readonly Histogram<double> LeafScanDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.scan.duration", unit: "ms",
            description: "Duration of leaf-level range scans (GetKeysAsync / GetEntriesAsync).");

    /// <summary>Histogram of <c>CompactTombstonesAsync</c> durations.</summary>
    public static readonly Histogram<double> LeafCompactionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.compaction.duration", unit: "ms",
            description: "Duration of tombstone compaction passes on a single leaf.");

    /// <summary>Counter of tombstone entries removed by <c>CompactTombstonesAsync</c>.</summary>
    public static readonly Counter<long> LeafTombstonesReaped =
        Meter.CreateCounter<long>("orleans.lattice.leaf.tombstones.reaped", unit: "{tombstone}",
            description: "Tombstone entries permanently removed by compaction.");

    /// <summary>
    /// Counter of tombstones created - incremented on every <c>DeleteAsync</c>
    /// success and once per deleted key in <c>DeleteRangeAsync</c>.
    /// </summary>
    public static readonly Counter<long> LeafTombstonesCreated =
        Meter.CreateCounter<long>("orleans.lattice.leaf.tombstones.created", unit: "{tombstone}",
            description: "Tombstone entries newly written by delete operations.");

    /// <summary>
    /// Counter of live entries removed by compaction because their per-entry
    /// TTL (set via the TTL overload of <c>SetAsync</c>) elapsed past the
    /// configured grace period. Separate from <see cref="LeafTombstonesReaped"/>
    /// so operators can distinguish explicit-delete reap throughput from TTL
    /// churn.
    /// </summary>
    public static readonly Counter<long> LeafTombstonesExpired =
        Meter.CreateCounter<long>("orleans.lattice.leaf.tombstones.expired", unit: "{tombstone}",
            description: "Live entries reaped by compaction because their TTL elapsed past the grace period.");

    /// <summary>Counter of leaf-level splits (leaf capacity exceeded, sibling allocated).</summary>
    public static readonly Counter<long> LeafSplits =
        Meter.CreateCounter<long>("orleans.lattice.leaf.splits", unit: "{split}",
            description: "Leaf-node splits triggered by MaxLeafKeys or MaxLeafBytes overflow.");

    /// <summary>
    /// Counter of leaves observed over the <see cref="BPlusTree.LatticeOptions.MaxLeafBytes"/>
    /// byte bound, tagged with <see cref="TagOutcome"/> = <c>split</c> when the
    /// leaf was divided back under the bound, or <c>irreducible</c> when it
    /// could not be, because a split pivots on a median key and a leaf holding
    /// a single oversized entry has no median to pivot on.
    /// <para>
    /// The <c>irreducible</c> series is the one to alert on. It names the only
    /// case this bound cannot repair, and such a leaf stays uncapturable, which
    /// keeps its tree's WAL trim floor pinned at zero. The remedy is at the
    /// application layer (store the oversized value across several keys), so
    /// the condition has to be visible rather than silently tolerated.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafByteOverflows =
        Meter.CreateCounter<long>("orleans.lattice.leaf.byte.overflow", unit: "{leaf}",
            description: "Leaves observed over the MaxLeafBytes bound, by whether they could be split.");

    /// <summary>
    /// Histogram of per-step latency on the leaf commit path
    /// (build-and-WAL-append, in-memory Apply, observer-publish,
    /// parent-digest publish). Tagged with <see cref="TagStep"/> =
    /// <c>wal</c>, <c>apply</c>, <c>observer</c>, or <c>digest</c> so
    /// operators can attribute total commit latency to its constituent
    /// stages. The <c>digest</c> step covers the awaited cross-grain
    /// <c>OnChildDigestPublishedAsync</c> RPC to the parent internal
    /// node emitted from every foreground write path (single-key
    /// <c>SetAsync</c> / <c>DeleteAsync</c>, per-leaf
    /// <c>DeleteRangeAsync</c>); cold / structural digest publishes
    /// (leaf-split topology, projection-checkpoint flush, saga
    /// terminal) are deliberately excluded so the histogram remains
    /// attributable to the per-write pipeline.
    /// </summary>
    public static readonly Histogram<double> LeafCommitDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.commit.duration", unit: "ms",
            description: "Per-step latency on the BPlusLeafGrain commit path.");

    /// <summary>
    /// Histogram of the in-flight commit-count snapshot taken at the
    /// moment a <c>BPlusLeafGrain</c> commit (either <c>CommitSetAsync</c>
    /// or <c>CommitSetManyAsync</c>) enters the commit path. The
    /// recorded value is the number of commits already in flight on the
    /// same leaf activation at the entry instant (i.e. zero on the very
    /// first concurrent commit, one on the second, and so on). Tagged
    /// with <see cref="TagTree"/> so operators can plot leaf-side
    /// commit concurrency per tree.
    /// <para>
    /// Under the default Orleans non-reentrant grain scheduling - the
    /// shipping shape of <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetAsync(string, byte[])"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/>, neither marked
    /// <c>[AlwaysInterleave]</c> - this histogram pins at <c>0</c>: the
    /// next commit cannot enter until the current one has returned. A
    /// non-zero quantile therefore signals one of two things:
    /// (i) a future change has applied <c>[AlwaysInterleave]</c> to the
    /// leaf-side commit entrypoint and disjoint-key sub-batches now
    /// overlap on the same leaf activation, or
    /// (ii) a reentrant-by-design code path (saga terminal write under
    /// commit-log scope) routed back through the commit-set path while
    /// an outer commit was still awaiting a WAL append.
    /// Either reading is informative for the U9m / leaf-side-commit-concurrency
    /// probe: a steady pin at <c>0</c> falsifies
    /// the leaf turn-queue hypothesis and routes the next probe to
    /// WAL-side fan-in (U9n); a steady lift above <c>0</c> identifies
    /// the leaf grain as the binding constraint.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> LeafCommitInFlight =
        Meter.CreateHistogram<int>("orleans.lattice.leaf.commit.in_flight", unit: "{commit}",
            description: "In-flight commit count snapshot at the moment a BPlusLeafGrain commit enters the commit path.");

    // --- Tree-registry fan-in instruments (ILatticeRegistry) ---------------------

    /// <summary>
    /// Histogram of how long the tree-registry singleton took to SERVE one
    /// <see cref="Orleans.Lattice.BPlusTree.ILatticeRegistry"/> read, measured
    /// inside the grain body and tagged with <see cref="TagOperation"/>
    /// (<c>exists</c>, <c>get_entry</c>, <c>resolve</c>, <c>get_shard_map</c>,
    /// <c>get_all_tree_ids</c>).
    /// <para>
    /// The registry is a cluster singleton that every per-tree background
    /// service addresses, so a cold start fans a whole estate onto one
    /// activation. Callers then see only a response-deadline
    /// <c>TimeoutException</c>, which is the same observation whether the call
    /// was served slowly or never served at all - and those have opposite
    /// remedies. Because this histogram records only calls the grain actually
    /// admitted, reading its tail and its count against the caller-side timeout
    /// population separates them: a short tail with a call count far below the
    /// offered load means the calls never reached the body (blocked upstream, in
    /// activation or in the turn queue), whereas a tail approaching the caller's
    /// deadline with a matching count means they were admitted and the time went
    /// on the awaited hop to the backing <c>_lattice_trees</c> tree.
    /// </para>
    /// <para>
    /// Deliberately not derived from <c>orleans-storage-read-latency</c>: that
    /// instrument's <c>state_name</c> tag comes from a grain's
    /// <c>[PersistentState]</c> declaration, and the registry grain declares
    /// none, so no <c>state_name</c> series for it can exist.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> RegistryCallDuration =
        Meter.CreateHistogram<double>("orleans.lattice.registry.call.duration", unit: "ms",
            description: "Service time of one ILatticeRegistry read, measured inside the registry singleton's grain body.");

    /// <summary>
    /// Histogram of the concurrent registry-call count observed at the moment a
    /// new <see cref="Orleans.Lattice.BPlusTree.ILatticeRegistry"/> read is
    /// admitted to the grain body, tagged with <see cref="TagOperation"/>. The
    /// recorded value excludes the arriving call, so it is zero on the first
    /// concurrent call, one on the second, and so on - the same convention as
    /// <see cref="LeafCommitInFlight"/>.
    /// <para>
    /// This is the fan-in width the singleton is actually carrying, and it is
    /// the half of the picture the duration histogram cannot supply: a long tail
    /// with a flat-zero width is a slow backing hop, while a long tail whose
    /// width climbs with estate size is the (trees x per-tree background
    /// services) scaling law saturating one activation.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> RegistryCallInFlight =
        Meter.CreateHistogram<int>("orleans.lattice.registry.call.in_flight", unit: "{call}",
            description: "Concurrent ILatticeRegistry reads in flight on the registry singleton at the moment a new read is admitted.");

    /// <summary>
    /// Histogram of how long a registry read waited for admission through the
    /// caller-side fan-in bound before its round trip was dispatched, in
    /// milliseconds.
    /// <para>
    /// <b>Why this instrument has to exist.</b> Bounding concurrent fan-in does
    /// not remove the work, it queues it, so the stall can relocate from the
    /// registry activation to the caller waiting for admission. Every
    /// registry-side signal - including
    /// <c>orleans_app_requests_timedout_total{grain_type="latticeregistry"}</c>
    /// and <see cref="RegistryCallInFlight"/> - is scoped to the registry grain,
    /// so a stall that moved to caller-side admission would drive all of them to
    /// zero and read as a clean recovery rather than as a relocated fault. This
    /// histogram is the only series that observes the relocated wait, so a
    /// registry-side improvement is interpretable only when read alongside it.
    /// </para>
    /// <para>
    /// A healthy silo records values at or near zero: admission dispatches
    /// immediately whenever the bound has room, so a non-trivial tail here means
    /// real queueing, and a tail that grows with estate size means the bound is
    /// converting a registry-side saturation into a caller-side one.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> RegistryAdmissionWait =
        Meter.CreateHistogram<double>("orleans.lattice.registry.admission.wait", unit: "ms",
            description: "Time a registry read waited for the caller-side fan-in bound before its round trip was dispatched.");

    /// <summary>
    /// Histogram of the number of fan-in permits this silo held at the moment a
    /// gated registry round trip was dispatched - the width the bound in
    /// <c>RegistryFanInGate.GlobalMaxConcurrentReads</c> actually caps.
    /// <para>
    /// <b>Why this is not <see cref="RegistryCallInFlight"/>.</b> That instrument
    /// counts calls executing inside the registry singleton's grain body, summed
    /// over every caller in the cluster and including callers that never pass
    /// through the gate at all (an Orleans client addressing
    /// <c>ILatticeRegistry</c> directly, for one). This instrument counts permits
    /// held by one silo's gate. They are different populations with different
    /// ceilings, and only this one is bounded by
    /// <c>GlobalMaxConcurrentReads</c>. Reading the registry-side width against
    /// that constant compares two quantities that were never the same number, and
    /// a low reading there is not evidence that the bound has room.
    /// </para>
    /// <para>
    /// <b>The recorded value INCLUDES the dispatch being recorded</b>, so it runs
    /// 1..<c>GlobalMaxConcurrentReads</c> and a recorded value equal to that
    /// constant means the ceiling was actually reached. This deliberately differs
    /// from the exclude-the-arrival convention of <see cref="RegistryCallInFlight"/>
    /// and <see cref="LeafCommitInFlight"/>: under that convention a fully
    /// saturated gate would top out one below its own bound, and a saturated
    /// reading would be indistinguishable from an unsaturated one by inspection.
    /// An off-by-one that makes saturation unobservable is the failure this
    /// instrument exists to remove, so the convention is chosen for comparability
    /// with the constant rather than for consistency with its neighbours.
    /// </para>
    /// <para>
    /// Read the distribution, never the mean: permit occupancy is bursty, so a
    /// window mean is dominated by idle time and measures something other than
    /// the peak. The actionable reading is the share of dispatches at the
    /// ceiling - <c>_bucket{le="16"}</c> against the total count.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> RegistryAdmissionInFlight =
        Meter.CreateHistogram<int>("orleans.lattice.registry.admission.in_flight", unit: "{dispatch}",
            description: "Fan-in permits held by this silo's registry gate when a gated round trip was dispatched, counting that dispatch.");

    /// <summary>
    /// Histogram of how many distinct tree ids one gated registry round trip
    /// carried, recorded once per dispatch.
    /// <para>
    /// A value of one is the single-key <c>GetEntryAsync</c> path a silo takes
    /// when nothing is queued behind the bound - byte-for-byte the traffic it had
    /// before the gate existed - and two or more is the batched
    /// <c>GetEntriesAsync</c> path. The share above one is therefore the share of
    /// registry reads the bound actually coalesced, which is the only direct
    /// evidence that the batching half of the gate ran at all. A population
    /// sitting almost entirely at one means the gate never queued, which is a
    /// statement about the offered load rather than about the batching.
    /// </para>
    /// <para>
    /// Read beside <see cref="RegistryAdmissionInFlight"/>: batches larger than
    /// one can only form once the permits are saturated, so a batch-size
    /// distribution above one with a width distribution below the ceiling is
    /// contradictory and means one of the two is being read wrongly.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> RegistryAdmissionBatchSize =
        Meter.CreateHistogram<int>("orleans.lattice.registry.admission.batch.size", unit: "{tree}",
            description: "Distinct tree ids carried by one gated registry round trip.");

    /// <summary>
    /// Histogram of how many distinct tree ids were already waiting for a fan-in
    /// permit at the moment another one arrived, counting the arrival.
    /// <para>
    /// This is the <em>offered</em> fan-in, and it is the signal that separates
    /// "the bound had room" from "nothing asked for it". Admission dispatches
    /// synchronously on the arriving thread whenever a permit is free, so when
    /// the gate is not binding every other gate instrument reports its floor - a
    /// wait of microseconds, a width of one, a batch of one - and those floors
    /// look exactly like a comfortable bound. They are not a weak measurement of
    /// headroom; they are what no demand looks like. A queue depth that stays at
    /// one says the demand never arrived, and until it rises above one no reading
    /// from the other three is evidence about the bound at all.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> RegistryAdmissionQueueDepth =
        Meter.CreateHistogram<int>("orleans.lattice.registry.admission.queue.depth", unit: "{tree}",
            description: "Distinct tree ids waiting for a registry fan-in permit when another arrived, counting the arrival.");

    // --- Warm-up instruments (ILattice.WarmUpAsync) ------------------------------

    /// <summary>
    /// Counter of completed <see cref="Orleans.Lattice.ILattice.WarmUpAsync"/>
    /// invocations observed on this silo. Tagged with <see cref="TagTree"/> so
    /// operators can confirm per-tree warm-up fired exactly once before the
    /// first hot-path write. A zero value on a tree whose first
    /// <c>SetManyAsync</c> coincides with steady-state means warm-up was
    /// skipped and the cold-start placement-directory storm landed against
    /// producer-driven flush concurrency.
    /// </summary>
    public static readonly Counter<long> WarmUpInvocations =
        Meter.CreateCounter<long>("orleans.lattice.warmup.invocations", unit: "{call}",
            description: "Completed ILattice.WarmUpAsync calls observed on this silo.");

    /// <summary>
    /// Histogram of <see cref="Orleans.Lattice.ILattice.WarmUpAsync"/>
    /// wall-clock duration, in milliseconds. One observation per call,
    /// covering routing resolution plus every bounded-concurrency per-shard
    /// probe round-trip. Tagged with <see cref="TagTree"/> and <c>shard_count</c>
    /// so the per-tree warm-start cost is attributable in phase-A scrapes.
    /// Useful as the headline "did warm-up actually fire and how long did it
    /// take" signal alongside <see cref="WarmUpInvocations"/>.
    /// </summary>
    public static readonly Histogram<double> WarmUpDurationMs =
        Meter.CreateHistogram<double>("orleans.lattice.warmup.duration", unit: "ms",
            description: "Wall-clock duration of ILattice.WarmUpAsync including all per-shard probes.");

    /// <summary>
    /// Counter of leaf caches successfully primed by a shard root's opt-in
    /// post-restart pre-warm (<c>LatticeOptions.LeafCachePreWarmCount</c>).
    /// Tagged with <see cref="TagTree"/>, <c>shard</c>, and the tenant label.
    /// Stays at zero while the feature is disabled, which is the default. A
    /// value materially below the configured pre-warm count means individual
    /// priming calls are failing - each failure is swallowed by design, so this
    /// counter is the only signal that they happened.
    /// </summary>
    public static readonly Counter<long> LeafCachePreWarmed =
        Meter.CreateCounter<long>("orleans.lattice.warmup.leaf_cache.prewarmed", unit: "{leaf}",
            description: "Leaf caches successfully primed by a shard root's post-restart pre-warm.");

    /// <summary>
    /// Histogram of the wall-clock duration, in milliseconds, of a shard root's
    /// leaf-cache pre-warm fan-out. One observation per shard per warm-up when
    /// the feature is enabled and the access model ranked at least one leaf.
    /// Read alongside <see cref="WarmUpDurationMs"/> to attribute how much of a
    /// tree's warm-up cost is leaf priming.
    /// </summary>
    public static readonly Histogram<double> LeafCachePreWarmDurationMs =
        Meter.CreateHistogram<double>("orleans.lattice.warmup.leaf_cache.duration", unit: "ms",
            description: "Wall-clock duration of a shard root's leaf-cache pre-warm fan-out.");

    /// <summary>
    /// Histogram of the number of leaves resident in a shard root's leaf-access
    /// histogram, observed each time the model is persisted. Bounded above by
    /// the model's own tracked-leaf cap, so a distribution pinned at that cap
    /// means the shard's read set is wider than the model can represent and the
    /// ranking is being drawn from a pruned view.
    /// </summary>
    public static readonly Histogram<int> LeafAccessModelLeaves =
        Meter.CreateHistogram<int>("orleans.lattice.leaf_access.model.leaves", unit: "{leaf}",
            description: "Leaves resident in a shard root's leaf-access histogram at persist time.");

    /// <summary>
    /// Counter of shard-root coalescing flush loops that suspended themselves after
    /// hitting the consecutive-failure ceiling. Tagged by <see cref="TagKind"/> with
    /// the loop that gave up (<c>dirty-leaves</c> or <c>leaf-access</c>).
    /// <para>
    /// Any non-zero value means a shard root is persistently unable to write its own
    /// state - most commonly a stale ETag that no longer matches the stored row, which
    /// no amount of retrying resolves. Before this counter existed the leaf-access loop
    /// reported such a failure only at <c>Debug</c>, so a shard root could fail every
    /// flush indefinitely with nothing visible above the storage provider.
    /// </para>
    /// <para>
    /// Alert on any increase. Suspension bounds the wasted writes, it does not repair
    /// the shard: the loop stays suspended until the activation is collected and a
    /// later one re-reads its state.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardRootFlushRetriesSuspended =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.flush.retries_suspended", unit: "{suspension}",
            description: "Shard-root coalescing flush loops suspended after repeated consecutive failures.");

    // --- Cache instruments (LeafCacheGrain) --------------------------------------

    /// <summary>
    /// Counter of cache hits observed by <c>LeafCacheGrain</c> - a key was present
    /// and live in the local cache after (possibly) refreshing the delta.
    /// </summary>
    public static readonly Counter<long> CacheHits =
        Meter.CreateCounter<long>("orleans.lattice.cache.hits", unit: "{hit}",
            description: "LeafCacheGrain reads served by a live, cached entry.");

    /// <summary>
    /// Counter of cache misses observed by <c>LeafCacheGrain</c> - the key was
    /// absent or tombstoned in the local cache after the delta refresh.
    /// </summary>
    public static readonly Counter<long> CacheMisses =
        Meter.CreateCounter<long>("orleans.lattice.cache.misses", unit: "{miss}",
            description: "LeafCacheGrain reads that did not find a live cached entry.");

    // --- Saga / coordinator / lifecycle instruments ------------------------------

    /// <summary>
    /// Counter incremented once per terminal transition of an <c>AtomicWriteGrain</c>
    /// saga. Tagged with <see cref="TagOutcome"/> = <c>committed</c> (all writes
    /// applied), <c>compensated</c> (prepare / execute failure rolled back via LWW),
    /// <c>failed</c> (post-compensation surrogate failure), or <c>shutdown_refused</c>
    /// (the saga's batched dispatch tripped the writer-side drain refusal
    /// because the silo is shutting down; the saga short-circuited the retry loop
    /// and the compensate-broadcast pass and surfaced
    /// <see cref="LatticeShuttingDownException"/> to the caller without persisting
    /// post-detection state).
    /// </summary>
    public static readonly Counter<long> AtomicWriteCompleted =
        Meter.CreateCounter<long>("orleans.lattice.atomic_write.completed", unit: "{saga}",
            description: "Terminal transitions of SetManyAtomicAsync sagas, tagged by outcome.");

    /// <summary>
    /// Histogram of end-to-end <c>SetManyAtomicAsync</c> saga durations,
    /// recorded once per terminal transition of an <c>AtomicWriteGrain</c>
    /// saga next to <see cref="AtomicWriteCompleted"/>. The duration is
    /// measured from the wall-clock time the saga's first
    /// <c>AtomicWritePhase.Prepare</c> ran (persisted on the saga state
    /// so it survives a silo crash) to the time the saga reached
    /// <c>AtomicWritePhase.Completed</c>. Tagged with <see cref="TagOutcome"/>
    /// = <c>committed</c>, <c>compensated</c>, <c>failed</c>, or
    /// <c>shutdown_refused</c> so operators can plot rollback-path latency
    /// separately from happy-path latency, and shutdown-coincidence sagas
    /// separately from genuine commit-conflict sagas.
    /// <para>
    /// Combine with <see cref="AtomicWriteBatchSize"/> when building dashboards:
    /// duration is meaningful only relative to the size of the batch that
    /// produced it. A duration spike with a constant batch-size distribution
    /// is a regression; a duration spike accompanied by a batch-size spike is
    /// a workload change.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> AtomicWriteDuration =
        Meter.CreateHistogram<double>("orleans.lattice.atomic_write.duration", unit: "ms",
            description: "End-to-end SetManyAtomicAsync saga duration, tagged by outcome.");

    /// <summary>
    /// Histogram of saga batch sizes, recorded once per terminal transition of
    /// an <c>AtomicWriteGrain</c> saga next to <see cref="AtomicWriteCompleted"/>.
    /// The value is the entry count submitted to <c>SetManyAtomicAsync</c>
    /// (or the per-entry list length on apply-mode sagas). Tagged with
    /// <see cref="TagOutcome"/> = <c>committed</c>, <c>compensated</c>,
    /// <c>failed</c>, or <c>shutdown_refused</c>. Lets operators interpret
    /// <see cref="AtomicWriteDuration"/>
    /// in context - a 10-entry batch and a 1000-entry batch both appear as one
    /// data point on the duration histogram, and only the batch-size histogram
    /// disambiguates them.
    /// </summary>
    public static readonly Histogram<int> AtomicWriteBatchSize =
        Meter.CreateHistogram<int>("orleans.lattice.atomic_write.batch_size", unit: "{entry}",
            description: "Entry count of each SetManyAtomicAsync saga, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per terminal completion of a <b>cross-tree</b>
    /// atomic write (the coordinator grain), tagged with <see cref="TagOutcome"/>
    /// = <c>committed</c> or <c>precondition_failed</c> and a
    /// <see cref="TagTreeCount"/> bucket. Those two are the whole domain: the
    /// coordinator has no compensation arm of its own, so a panel filtering
    /// this counter for <c>failed</c> or <c>compensated</c> selects zero series
    /// forever. Distinguishes multi-tree saga volume
    /// from the single-tree <see cref="AtomicWriteCompleted"/> stream.
    /// </summary>
    public static readonly Counter<long> CrossTreeAtomicWriteCompleted =
        Meter.CreateCounter<long>("orleans.lattice.atomic_write.cross_tree.completed", unit: "{saga}",
            description: "Terminal transitions of cross-tree atomic-write sagas, tagged by outcome and tree count.");

    /// <summary>
    /// Histogram of end-to-end cross-tree atomic-write coordinator latency,
    /// measured from first submit to the terminal phase. Tagged with
    /// <see cref="TagOutcome"/> so commit-path and abort-path latency can be
    /// plotted separately.
    /// </summary>
    public static readonly Histogram<double> CrossTreeAtomicWriteDuration =
        Meter.CreateHistogram<double>("orleans.lattice.atomic_write.cross_tree.duration", unit: "ms",
            description: "End-to-end cross-tree atomic-write coordinator duration, tagged by outcome.");

    /// <summary>
    /// Histogram of the number of participating trees in each cross-tree atomic
    /// write, recorded once per terminal completion next to
    /// <see cref="CrossTreeAtomicWriteCompleted"/>. Lets operators interpret
    /// <see cref="CrossTreeAtomicWriteDuration"/> relative to fan-out width.
    /// </summary>
    public static readonly Histogram<int> CrossTreeAtomicWriteParticipants =
        Meter.CreateHistogram<int>("orleans.lattice.atomic_write.cross_tree.participants", unit: "{tree}",
            description: "Participating-tree count of each cross-tree atomic-write saga, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per <c>AcquireAsync</c> / <c>TryAcquireAsync</c>
    /// terminal outcome on <c>LatticeLockGrain</c>, tagged with
    /// <see cref="TagOutcome"/> = <c>granted</c> (the caller received the lease),
    /// <c>timeout</c> (the FIFO wait elapsed before a grant), or
    /// <c>unavailable</c> (a non-blocking <c>TryAcquireAsync</c> found the lock
    /// held). Lets operators watch lock contention as the ratio of non-granted to
    /// granted outcomes.
    /// </summary>
    public static readonly Counter<long> LockAcquired =
        Meter.CreateCounter<long>("orleans.lattice.lock.acquired", unit: "{acquire}",
            description: "Distributed-lock acquire outcomes (granted / timeout / unavailable), tagged by outcome.");

    /// <summary>
    /// Counter incremented once per honoured <c>ReleaseAsync</c> on
    /// <c>LatticeLockGrain</c> - a release presenting the current holder's fencing
    /// token that actually freed the lock. A stale-token release is a no-op and is
    /// not counted here.
    /// </summary>
    public static readonly Counter<long> LockReleased =
        Meter.CreateCounter<long>("orleans.lattice.lock.released", unit: "{release}",
            description: "Distributed-lock releases that freed the lock under the current holder's fencing token.");

    /// <summary>
    /// Counter incremented once per lease reclamation on <c>LatticeLockGrain</c> -
    /// a holder whose lease expired without a renew or release, whose lock was
    /// reclaimed (and handed to the next FIFO waiter, if any). A sustained non-zero
    /// rate indicates holders crashing or pausing past their lease duration, the
    /// exact condition the fencing token protects downstream resources against.
    /// </summary>
    public static readonly Counter<long> LockLeaseReclaimed =
        Meter.CreateCounter<long>("orleans.lattice.lock.lease_reclaimed", unit: "{lease}",
            description: "Distributed-lock leases reclaimed after expiry without renew or release.");

    /// <summary>
    /// Histogram of the wall-clock time a granted acquire spent waiting in the
    /// FIFO queue, recorded once per <c>granted</c> outcome on
    /// <c>LatticeLockGrain</c> (zero for an uncontended immediate grant). Lets
    /// operators plot lock-wait latency percentiles distinctly from the
    /// granted/timeout counts on <see cref="LockAcquired"/>.
    /// </summary>
    public static readonly Histogram<double> LockAcquireWait =
        Meter.CreateHistogram<double>("orleans.lattice.lock.acquire.wait", unit: "ms",
            description: "Time a granted distributed-lock acquire spent waiting in the FIFO queue.");

    /// <summary>
    /// Counter incremented once per terminal transition of an <c>AtomicActionGrain</c>
    /// saga (the generic atomic-action / TCC coordinator). Tagged with
    /// <see cref="TagOutcome"/> = <c>committed</c> (every forward step committed),
    /// <c>compensated</c> (a forward step faulted and every committed step was
    /// rolled back in reverse order), or <c>compensation_failed</c> (a compensating
    /// effect itself faulted, so the saga parked for operator intervention). Lets
    /// operators watch the rollback and parked-saga rates as fractions of total
    /// saga volume.
    /// </summary>
    public static readonly Counter<long> AtomicActionCompleted =
        Meter.CreateCounter<long>("orleans.lattice.atomic_action.completed", unit: "{saga}",
            description: "Terminal transitions of atomic-action (saga / TCC) coordinators, tagged by outcome.");

    /// <summary>
    /// Histogram of end-to-end atomic-action saga durations, recorded once per
    /// terminal transition of an <c>AtomicActionGrain</c> next to
    /// <see cref="AtomicActionCompleted"/>. The duration is measured from the
    /// wall-clock time the saga first started (persisted on the saga state so it
    /// survives a silo crash) to the time it reached its terminal outcome. Tagged
    /// with <see cref="TagOutcome"/> = <c>committed</c>, <c>compensated</c>, or
    /// <c>compensation_failed</c> so operators can plot rollback-path latency
    /// separately from happy-path latency.
    /// </summary>
    public static readonly Histogram<double> AtomicActionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.atomic_action.duration", unit: "ms",
            description: "End-to-end atomic-action (saga / TCC) coordinator duration, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per step effect an <c>AtomicActionGrain</c> saga
    /// runs. Tagged with <see cref="TagPhase"/> = <c>forward</c> (a forward effect
    /// committed) or <c>compensate</c> (a compensating effect committed), and
    /// <see cref="TagOutcome"/> = <c>ok</c> (the effect succeeded) or <c>fault</c>
    /// (the effect threw). Lets operators watch the compensation rate and per-phase
    /// fault rate at step granularity, below the per-saga
    /// <see cref="AtomicActionCompleted"/> stream.
    /// </summary>
    public static readonly Counter<long> AtomicActionStep =
        Meter.CreateCounter<long>("orleans.lattice.atomic_action.step", unit: "{step}",
            description: "Atomic-action saga step effects, tagged by phase (forward / compensate) and outcome (ok / fault).");

    /// <summary>
    /// Counter incremented once per successful coordinator-grain completion.
    /// Tagged with <see cref="TagKind"/> = <c>snapshot</c>, <c>resize</c>,
    /// <c>reshard</c>, <c>merge</c>, or <c>compaction</c>.
    /// </summary>
    public static readonly Counter<long> CoordinatorCompleted =
        Meter.CreateCounter<long>("orleans.lattice.coordinator.completed", unit: "{operation}",
            description: "Long-running coordinator-grain completions (snapshot, resize, reshard, merge, compaction).");

    /// <summary>
    /// Counter incremented once per coordinator phase-timer tick whose phase step
    /// threw. The base coordinator swallows that exception by design so the timer
    /// survives, which means the tick made no progress and the next tick starts the
    /// same step over; without this counter that outcome is visible only as a log
    /// line (issue #2705). Tagged with <see cref="TagKind"/> = the coordinator's
    /// keepalive reminder name (<c>snapshot-keepalive</c>, <c>reshard-keepalive</c>,
    /// <c>repo-context-ann-index-build-keepalive</c>, ...), <see cref="TagTree"/> =
    /// the tree or repository the coordinator serves, and the tenant label.
    /// <para>
    /// <b>Zero-primed</b> once per activation, when the coordinator first arms its
    /// phase timer. A <see cref="Counter{T}"/> exports no series until its first
    /// <c>Add</c>, so an unprimed instrument answers "has this coordinator failed a
    /// tick?" with silence, which reads identically to a dead subsystem or a broken
    /// instrument. Priming at the arm point makes the population exactly the
    /// coordinators that are actually ticking, so <c>0</c> on a primed series is a
    /// measurement: this coordinator ran and no tick threw. It does <b>not</b> mean
    /// the coordinator is making progress - a tick that returns without advancing
    /// its phase machine is a success here - and the absence of a series still
    /// means only that no coordinator of that kind has armed a timer in this
    /// process.
    /// </para>
    /// </summary>
    public static readonly Counter<long> CoordinatorPhaseTickFailures =
        Meter.CreateCounter<long>("orleans.lattice.coordinator.phase_tick.failures", unit: "{failure}",
            description: "Coordinator phase-timer ticks whose phase step threw and was swallowed, tagged by coordinator kind and tree. Zero-primed when a coordinator arms its phase timer, so zero on a live series is a reading rather than an absence.");

    /// <summary>
    /// The name of the observable gauge reporting, per coordinator kind and tree,
    /// the length of the <i>current run</i> of consecutive failed phase ticks.
    /// </summary>
    public const string CoordinatorPhaseTickConsecutiveFailuresGaugeName =
        "orleans.lattice.coordinator.phase_tick.consecutive_failures";

    /// <summary>
    /// Observable gauge reporting the length of the current run of consecutive
    /// failed phase ticks, tagged identically to
    /// <see cref="CoordinatorPhaseTickFailures"/> so the two series join.
    /// <para>
    /// <b>It answers the one question the counter beside it cannot.</b> A
    /// cumulative counter has no notion of consecutiveness, so a coordinator that
    /// fails one tick in a thousand and a coordinator that has failed every tick
    /// since the process started both present as a rising total - yet the first is
    /// a transient the pump absorbs by design and the second is a phase machine
    /// that has stopped advancing. Issue #2814 settled exactly that distinction for
    /// the repository-context approximate-index build, and the evidence that made
    /// it a wedge rather than a flaky read was the phrase "156 times in a row" in a
    /// 53 MB log stream. This gauge makes that reading available from one scrape.
    /// </para>
    /// <para>
    /// <b>Every live coordinator reports</b>, enrolling at <c>0</c> when it arms
    /// its phase timer - the same priming point as the counter, and for the same
    /// reason: an unprimed zero is byte-identical to an absent series. So <c>0</c>
    /// here means "the last tick succeeded", not "no data". It does <b>not</b> mean
    /// the coordinator is advancing: a tick that returns without moving its phase
    /// machine forward is a success by this measure.
    /// </para>
    /// <para>
    /// Reported as the <b>maximum</b> over the activations sharing a tag set, which
    /// is coarser than the activation because a coordinator with a composite key
    /// deliberately reports under the subject alone. See
    /// <c>CoordinatorPhaseTickCensus</c> - which both registers this gauge and
    /// supplies its callback, so that the tenant dimension is emitted in the same
    /// file the instrument is created in - for why <c>max</c> is the correct
    /// reduction.
    /// </para>
    /// </summary>
    public static readonly ObservableGauge<long> CoordinatorPhaseTickConsecutiveFailures =
        CoordinatorPhaseTickCensus.Gauge;

    /// <summary>
    /// Counter incremented once per tree-lifecycle transition. Tagged with
    /// <see cref="TagKind"/> = <c>deleted</c>, <c>recovered</c>, or <c>purged</c>.
    /// </summary>
    public static readonly Counter<long> TreeLifecycle =
        Meter.CreateCounter<long>("orleans.lattice.tree.lifecycle", unit: "{event}",
            description: "Tree-lifecycle transitions emitted by TreeDeletionGrain.");

    /// <summary>
    /// Counter incremented once per successfully-dispatched
    /// <see cref="LatticeTreeEvent"/>. Tagged with <see cref="TagKind"/> =
    /// the event kind name (e.g. <c>Set</c>, <c>SnapshotCompleted</c>).
    /// </summary>
    public static readonly Counter<long> EventsPublished =
        Meter.CreateCounter<long>("orleans.lattice.events.published", unit: "{event}",
            description: "LatticeTreeEvent instances successfully dispatched to the configured stream provider.");

    /// <summary>
    /// Counter incremented once per event drop. Tagged with <see cref="TagReason"/>
    /// = <c>missing_provider</c> (no stream provider by the configured name) or
    /// <c>publish_error</c> (stream provider threw during dispatch).
    /// </summary>
    public static readonly Counter<long> EventsDropped =
        Meter.CreateCounter<long>("orleans.lattice.events.dropped", unit: "{event}",
            description: "LatticeTreeEvent instances dropped because the stream provider was missing or threw.");

    /// <summary>
    /// Counter incremented once per per-tree configuration change applied at
    /// runtime. Tagged with <see cref="TagConfig"/> = the configuration
    /// dimension (e.g. <c>publish_events</c>) and <see cref="TagTree"/>.
    /// </summary>
    public static readonly Counter<long> ConfigChanged =
        Meter.CreateCounter<long>("orleans.lattice.config.changed", unit: "{change}",
            description: "Per-tree configuration changes applied at runtime via ILattice overrides.");

    // --- Mutation-observer instruments (MutationObserverDispatcher) ----------

    /// <summary>
    /// Histogram of the wall-clock time one registered
    /// <see cref="IMutationObserver"/> spent inside a single
    /// <c>OnMutationAsync</c> callback. Observers run <em>inline</em> on the
    /// grain write path, so every millisecond recorded here is a millisecond
    /// added to the caller's write latency - this instrument attributes that
    /// cost to the specific observer that incurred it, on the same pipeline as
    /// the traffic it slows down.
    /// <para>
    /// Tagged with <see cref="TagObserver"/> (the observer's CLR type name) and
    /// <see cref="TagTree"/> (the mutated tree). Recorded on the faulting path
    /// too: an observer that throws slowly is exactly the misbehaviour this
    /// instrument exists to surface, and the dispatcher still suppresses the
    /// exception. The sample spans only the callback - the dispatcher's own
    /// swallow-and-log work is excluded, so a slow log sink cannot be
    /// mistaken for a slow observer.
    /// </para>
    /// <para>
    /// Zero-cost when unused. The dispatcher's no-observer fast path returns
    /// before any timing work, and the remaining timestamp capture is elided
    /// when <see cref="Instrument.Enabled"/> is <c>false</c> - so a caller who
    /// registers an observer but attaches no metrics listener pays one boolean
    /// read per publish.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> ObserverDuration =
        Meter.CreateHistogram<double>("orleans.lattice.observer.duration", unit: "ms",
            description: "Inline duration of one IMutationObserver callback on the write path, tagged by observer type and tree.");

    // --- Snapshot-cursor instruments ----------------------------------------

    /// <summary>
    /// Histogram of per-shard WAL-replay duration observed during
    /// snapshot-leaf open. Emitted by <c>SnapshotLeafGrain</c> after a
    /// successful replay over <c>[0, capturedOffset)</c>. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/> (the virtual
    /// shard the snapshot leaf materialises).
    /// </summary>
    public static readonly Histogram<double> SnapshotReplayDuration =
        Meter.CreateHistogram<double>("orleans.lattice.snapshot.replay.duration", unit: "ms",
            description: "Per-shard WAL-replay duration observed during zero-observable-writes snapshot-leaf open.");

    /// <summary>
    /// Counter of WAL entries fed to the snapshot-leaf replay engine
    /// during a snapshot-leaf open. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>. One increment per
    /// <c>CommitLogSliceEntry</c> processed; filtered or skipped
    /// records are still counted because they contribute to wall-clock
    /// replay cost.
    /// </summary>
    public static readonly Counter<long> SnapshotReplayEntries =
        Meter.CreateCounter<long>("orleans.lattice.snapshot.replay.entries", unit: "{entry}",
            description: "WAL entries consumed by the zero-observable-writes snapshot-leaf replay engine.");

    /// <summary>
    /// Name of the observable gauge reporting the number of live WAL retention
    /// pins registered by snapshot cursors against
    /// <see cref="IWalCursorRegistry"/>, tagged with <see cref="TagTree"/> and
    /// the tenant label. Published by
    /// <see cref="BPlusTree.Grains.SnapshotPinCensus"/>, which derives the value
    /// from the registry's live pin set for the tree.
    /// <para>
    /// This was an <c>UpDownCounter</c> until issue #2700. A counter is
    /// process-lifetime state, and the <c>+1</c> / <c>-1</c> were guarded by a
    /// per-<i>activation</i> boolean on the cursor grain, so the increment was
    /// repeatable across activations while the decrement was not guaranteed:
    /// an activation collected, migrated, or lost with its silo while holding a
    /// pin never emitted its compensating <c>-1</c> and the series ratcheted
    /// permanently upward - which made a genuine pin leak indistinguishable
    /// from accumulated drift, the one question the instrument exists to
    /// answer. An observable gauge reporting present truth has no compensating
    /// write to lose, so a deployment already carrying drift returns to
    /// reporting the truth on its own after upgrade.
    /// </para>
    /// <para>
    /// The tree set is seeded by the WAL GC scheduler, so a tree that has never
    /// opened a snapshot cursor still exports an explicit <c>0</c> rather than
    /// no series at all - the same priming convention issue #2694 established
    /// for the WAL-retention counters, and for the same reason.
    /// </para>
    /// </summary>
    public const string SnapshotPinsGaugeName = "orleans.lattice.snapshot.pins";

    // --- WAL garbage-collector instruments ----------------------------------

    /// <summary>
    /// Counter of WAL entries removed by a <see cref="ILatticeWalGc.RunOnceAsync"/>
    /// pass, tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Emitted from <see cref="LatticeWalGc"/> once per shard the pass
    /// actually scanned, including when that shard trimmed nothing, so every
    /// scanned shard publishes a series.
    /// <para>
    /// The shard tag was added by issue #3206. Trimming is decided and
    /// performed per shard, so a tree-scoped total is a sum over per-shard
    /// results and cannot say which shard produced it; read against the
    /// equally shard-tagged <see cref="WalCompactions"/> it is the
    /// discriminator that separates a shard which trims and compacts from one
    /// which trims and strands the bytes.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalEntriesTrimmed =
        Meter.CreateCounter<long>("orleans.lattice.wal.entries_trimmed", unit: "{entry}",
            description: "WAL entries removed by the per-tree garbage collector, tagged by tree and by the shard the scan trimmed. Emitted once per shard the pass scanned, including a zero for a shard that reclaimed nothing, so an absent series means the shard was not scanned on this silo rather than that it reclaimed nothing.");

    /// <summary>
    /// Counter of WAL shard compactions - the operation that actually returns
    /// trimmed space to the filesystem - tagged with <see cref="TagTree"/>,
    /// <see cref="TagShard"/> and <see cref="TagTrigger"/>. Emitted by the
    /// file WAL provider.
    /// <para>
    /// Trimming and reclaiming are different events, and conflating them is the
    /// blindness issue #3107 was filed for. A trim only marks a prefix dead;
    /// for a log-structured backend the bytes are returned to the filesystem
    /// only when the shard is rewritten. So
    /// <see cref="WalEntriesTrimmed"/> can climb steadily for hours while disk
    /// usage does not move at all, which is normal rather than a fault - and
    /// with no counter for the second event there was no way to tell that
    /// healthy convergence apart from dead space stranded permanently below the
    /// compaction threshold.
    /// </para>
    /// <para>
    /// The <see cref="TagTrigger"/> arms say which rule fired. <c>ratio</c> is
    /// the default amortised policy (dead bytes reached
    /// <c>CompactionThreshold</c> of payload). <c>ceiling</c> is the opt-in
    /// absolute bound <c>CompactionMaximumDeadBytes</c>, and a sustained
    /// non-zero rate on it means the deployment is buying bounded disk at the
    /// cost of write amplification. <c>reconcile</c> is the unconditional
    /// activation-time compaction, which reclaims whatever the steady-state
    /// triggers left behind. All arms are zero-primed per shard.
    /// </para>
    /// <para>
    /// <see cref="TagShard"/> was added by issue #3206. Every compaction
    /// trigger is shard-local - the ratio test compares one shard's dead bytes
    /// against its own payload - so a tree-scoped series averages one
    /// threshold test per shard and reports a dead fraction no shard holds. On
    /// the estate that motivated the change, three of eight shards holding 81%
    /// of a 1.6 GB WAL had never compacted while the tree-level counters
    /// advanced healthily. <see cref="TagShard"/> is the correct key rather
    /// than <see cref="TagPartition"/>, which names the producer-side writer
    /// partition and is reserved for the writer-layer instruments.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalCompactions =
        Meter.CreateCounter<long>("orleans.lattice.wal.compactions", unit: "{compaction}",
            description: "WAL shard compactions - the operation that returns trimmed space to the filesystem - tagged by tree, by the storage shard the rewrite ran on, and by the trigger that fired. 'ratio' is the default amortised policy (dead bytes reached the configured fraction of payload); 'ceiling' is the opt-in absolute dead-byte bound, whose sustained use trades write amplification for bounded disk; 'reconcile' is the unconditional activation-time compaction. Every trigger is shard-local, so the discriminator must be read per shard: summed to the tree an active minority of shards masks a stranded majority. Distinct from orleans.lattice.wal.entries_trimmed, which counts entries marked dead rather than bytes returned: the two can diverge for hours, and that divergence is normal convergence rather than a fault. All arms are zero-primed per shard.");

    /// <summary>Canonical name of <see cref="WalCompactions"/>.</summary>
    public const string WalCompactionsName = "orleans.lattice.wal.compactions";

    /// <summary>
    /// <see cref="TagTrigger"/> value on <see cref="WalCompactions"/> for the
    /// default amortised policy: dead bytes reached the configured fraction of
    /// payload. Compaction rewrites every live byte to reclaim the dead ones,
    /// so triggering on a fraction is what keeps the cost near one byte written
    /// per byte reclaimed.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalCompactionTriggerRatio =
        new(TagTrigger, "ratio");

    /// <summary>
    /// <see cref="TagTrigger"/> value on <see cref="WalCompactions"/> for the
    /// opt-in absolute dead-byte ceiling. Deliberately checked before the
    /// ratio, because a large shard can sit far below its threshold while
    /// holding an absolutely unacceptable amount of dead space.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalCompactionTriggerCeiling =
        new(TagTrigger, "ceiling");

    /// <summary>
    /// <see cref="TagTrigger"/> value on <see cref="WalCompactions"/> for the
    /// unconditional activation-time compaction. Note this fires only when a
    /// WAL shard grain activates, so it never rescues an actively-written tree,
    /// whose shard grain does not deactivate.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalCompactionTriggerReconcile =
        new(TagTrigger, "reconcile");

    /// <summary>
    /// Counter of bytes physically returned to the filesystem by WAL shard
    /// compaction, tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Zero-primed per shard.
    /// <para>
    /// Monotonic by design. The obvious alternative - an up/down gauge of dead
    /// bytes currently held - needs a compensating write per compaction, and
    /// issue #2700 established in this codebase that a compensating write which
    /// can be lost ratchets the series permanently and makes real waste
    /// indistinguishable from accumulated drift. Present-truth occupancy is
    /// reported instead by
    /// <see cref="BPlusTree.TreeStorageUsageReport.WalPhysicalBytes"/>, which is
    /// derived on each sample and so cannot drift.
    /// </para>
    /// <para>
    /// <see cref="TagShard"/> was added by issue #3206, for the reason given on
    /// <see cref="WalCompactions"/>: the rewrite that released the bytes ran on
    /// one shard, so a tree-scoped total lets an active minority of shards mask
    /// a stranded majority.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalCompactionReclaimedBytes =
        Meter.CreateCounter<long>("orleans.lattice.wal.compaction.reclaimed_bytes", unit: "By",
            description: "Bytes physically returned to the filesystem by WAL shard compaction, tagged by tree and by the storage shard the rewrite ran on. Monotonic by design: an up/down gauge of currently-held dead bytes would need a compensating write that can be lost, which ratchets the series (issue #2700). For present-truth occupancy compare the tree's physical and retained WAL bytes on the storage-usage report instead. Zero-primed per shard.");

    /// <summary>Canonical name of <see cref="WalCompactionReclaimedBytes"/>.</summary>
    public const string WalCompactionReclaimedBytesName = "orleans.lattice.wal.compaction.reclaimed_bytes";

    /// <summary>
    /// Bytes truncated from a WAL shard log by activation-time recovery
    /// because the bytes occupying them were never sealed by a commit record,
    /// tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Zero-primed per shard.
    /// <para>
    /// Recovery rolls forward every committed batch and truncates the unsealed
    /// tail. That truncation is correct - an unsealed record was never durable,
    /// and replaying it would violate the batch atomicity the commit record
    /// exists to provide - but until issue #3366 it was also entirely silent,
    /// which left a shard that discarded a tail indistinguishable from one that
    /// recovered with nothing to discard.
    /// </para>
    /// <para>
    /// <b>This counter does not measure a fault on its own.</b> It measures a
    /// quantity whose interpretation depends on how the previous host exited.
    /// After a crash or a kill, a non-zero reading is the expected and benign
    /// case: writes were in flight and never sealed. After a drain that
    /// completed, the expected reading is exactly zero, because a completed
    /// drain has flushed and sealed every batch it acknowledged - so a non-zero
    /// reading there reports acknowledged writes that did not survive the
    /// restart. Read it against the recorded exit, never alone.
    /// </para>
    /// <para>
    /// It is also the only point at which the quantity is observable at all:
    /// recovery destroys the evidence it is derived from by truncating the
    /// file, so a measurement not taken here cannot be recovered afterwards
    /// from the log, from the shard, or from a snapshot.
    /// </para>
    /// <para>
    /// Zero-priming is load-bearing for the reason given on
    /// <see cref="WalCompactions"/>: an absent series and a zero series carry
    /// opposite meanings, and issue #3107 established in this codebase how
    /// expensive that particular ambiguity is to diagnose. Priming is per
    /// shard, so a shard that is not reporting stays distinguishable from a
    /// shard that recovered cleanly even after a sibling has reported.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalRecoveryTornTailBytes =
        Meter.CreateCounter<long>("orleans.lattice.wal.recovery.torn_tail_bytes", unit: "By",
            description: "Bytes truncated from a WAL shard log by activation-time recovery because they were never sealed by a commit record, tagged by tree and by storage shard. Not a fault on its own: after a crash a non-zero reading is expected and benign, while after a drain that completed the expected reading is exactly zero, so a non-zero reading there reports acknowledged writes that did not survive the restart. Read it against the recorded exit. Recovery truncates the evidence this is derived from, so a measurement not taken here cannot be recovered later. Zero-primed per shard.");

    /// <summary>Canonical name of <see cref="WalRecoveryTornTailBytes"/>.</summary>
    public const string WalRecoveryTornTailBytesName = "orleans.lattice.wal.recovery.torn_tail_bytes";

    /// <summary>
    /// Count of complete data records discarded from a WAL shard log by
    /// activation-time recovery because no commit record ever sealed them,
    /// tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Zero-primed per shard.
    /// <para>
    /// The companion to <see cref="WalRecoveryTornTailBytes"/>, and both are
    /// needed because they separate two different causes that the byte figure
    /// alone conflates. The byte figure also covers the torn trailing bytes of
    /// a single partially-written record, which are not a complete record and
    /// so contribute no count. A reading of bytes greater than zero with a
    /// record count of zero is therefore one interrupted write, which is the
    /// ordinary shape of a process killed mid-append. A non-zero record count
    /// is a run of complete records that were written and never committed,
    /// which is a different and more serious shape: the batch reached the file
    /// intact and the commit that would have made it durable never followed.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalRecoveryTornTailRecords =
        Meter.CreateCounter<long>("orleans.lattice.wal.recovery.torn_tail_records", unit: "{record}",
            description: "Complete data records discarded from a WAL shard log by activation-time recovery because no commit record sealed them, tagged by tree and by storage shard. Read with orleans.lattice.wal.recovery.torn_tail_bytes, which it disambiguates: bytes above zero with a record count of zero is a single interrupted write (the ordinary shape of a kill mid-append), whereas a non-zero record count is a run of complete records that were written and never committed. Zero-primed per shard.");

    /// <summary>Canonical name of <see cref="WalRecoveryTornTailRecords"/>.</summary>
    public const string WalRecoveryTornTailRecordsName = "orleans.lattice.wal.recovery.torn_tail_records";

    /// <summary>
    /// Retained (live) payload bytes a WAL shard held at the moment its
    /// compaction threshold was evaluated, tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>. Sampled once per evaluation, before any arm
    /// fires, so a shard that is evaluated and declines is distinguishable from
    /// a shard that is never evaluated at all.
    /// <para>
    /// This and its three siblings -
    /// <see cref="WalCompactionEvalDeadBytes"/>,
    /// <see cref="WalCompactionEvalRetainedEntries"/> and
    /// <see cref="WalCompactionEvalDeadEntries"/> - are the complete input set
    /// of the shard-local compaction gate, published so the decision can be
    /// reconstructed exactly from outside the process. Issue #3206 established
    /// why the byte figures alone are not enough: both counters track
    /// <b>payload</b> length, while the file stores <b>framed</b> records, so a
    /// dead ratio derived by subtracting published byte aggregates carries the
    /// per-record framing overhead in numerator and denominator alike and is
    /// therefore only an upper bound on the ratio the gate itself tests. The
    /// entry counts make the mean payload observable, which makes the framing
    /// term computable and the gate's own ratio exact rather than bounded.
    /// </para>
    /// <para>
    /// A histogram rather than an up/down counter for the reason issue #2700
    /// established and <see cref="WalCompactionReclaimedBytes"/> restates: a
    /// level maintained by compensating writes ratchets permanently when one is
    /// lost. Each evaluation is an independent sample of present truth instead,
    /// which is the same shape <see cref="WalGcBacklogBytes"/> already uses.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalCompactionEvalRetainedBytes =
        Meter.CreateHistogram<long>("orleans.lattice.wal.compaction.eval.retained_bytes", unit: "By",
            description: "Retained (live) payload bytes a WAL shard held when its compaction threshold was evaluated, tagged by tree and storage shard. Sampled once per evaluation before any arm fires, so an evaluated-and-declined shard is distinguishable from an unevaluated one. Paired with the dead-byte and the two entry-count samples so the shard-local gate's ratio can be reconstructed exactly, including the per-record framing the payload-only byte figures omit (issue #3206).");

    /// <summary>Canonical name of <see cref="WalCompactionEvalRetainedBytes"/>.</summary>
    public const string WalCompactionEvalRetainedBytesName =
        "orleans.lattice.wal.compaction.eval.retained_bytes";

    /// <summary>
    /// Dead (trimmed but not yet reclaimed) payload bytes a WAL shard held at
    /// the moment its compaction threshold was evaluated, tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>. This is the numerator
    /// the ratio arm tests and the quantity both the minimum-dead floor and the
    /// absolute ceiling are compared against.
    /// <para>
    /// See <see cref="WalCompactionEvalRetainedBytes"/> for why the four
    /// evaluation samples are published together and why payload bytes alone
    /// under-determine the gate.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalCompactionEvalDeadBytes =
        Meter.CreateHistogram<long>("orleans.lattice.wal.compaction.eval.dead_bytes", unit: "By",
            description: "Dead (trimmed but not yet reclaimed) payload bytes a WAL shard held when its compaction threshold was evaluated, tagged by tree and storage shard. The numerator of the ratio arm and the quantity the minimum-dead floor and absolute ceiling are tested against. Sampled once per evaluation before any arm fires (issue #3206).");

    /// <summary>Canonical name of <see cref="WalCompactionEvalDeadBytes"/>.</summary>
    public const string WalCompactionEvalDeadBytesName =
        "orleans.lattice.wal.compaction.eval.dead_bytes";

    /// <summary>
    /// Retained (live) entry count a WAL shard held at the moment its
    /// compaction threshold was evaluated, tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>.
    /// <para>
    /// Divided into <see cref="WalCompactionEvalRetainedBytes"/> this yields the
    /// shard's mean live payload, which is the quantity that decides how much
    /// per-record framing a physical-minus-retained subtraction has silently
    /// folded into a derived dead ratio. Before issue #3206 no published series
    /// carried an entry count except
    /// <see cref="WalEntriesTrimmed"/>, so mean payload was not derivable from
    /// telemetry at all and the derived ratio could not be corrected.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalCompactionEvalRetainedEntries =
        Meter.CreateHistogram<long>("orleans.lattice.wal.compaction.eval.retained_entries", unit: "{entry}",
            description: "Retained (live) entry count a WAL shard held when its compaction threshold was evaluated, tagged by tree and storage shard. Divided into the retained-byte sample it yields mean live payload, which is what decides how much per-record framing a physical-minus-retained subtraction folds into a derived dead ratio (issue #3206).");

    /// <summary>Canonical name of <see cref="WalCompactionEvalRetainedEntries"/>.</summary>
    public const string WalCompactionEvalRetainedEntriesName =
        "orleans.lattice.wal.compaction.eval.retained_entries";

    /// <summary>
    /// Dead (trimmed but not yet reclaimed) entry count a WAL shard held at the
    /// moment its compaction threshold was evaluated, tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>. Counts the records
    /// whose payload <see cref="WalCompactionEvalDeadBytes"/> totals, so the
    /// two together give the dead records' mean payload and hence their framing
    /// contribution.
    /// <para>
    /// Distinct from <see cref="WalEntriesTrimmed"/>, which is a monotonic
    /// count of trim events: this is the present backlog awaiting a rewrite and
    /// returns to zero each time one completes.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalCompactionEvalDeadEntries =
        Meter.CreateHistogram<long>("orleans.lattice.wal.compaction.eval.dead_entries", unit: "{entry}",
            description: "Dead (trimmed but not yet reclaimed) entry count a WAL shard held when its compaction threshold was evaluated, tagged by tree and storage shard. Gives the dead records' mean payload when divided into the dead-byte sample, and so their framing contribution. Distinct from wal.entries_trimmed, which counts trim events monotonically; this is the present backlog awaiting a rewrite and returns to zero when one completes (issue #3206).");

    /// <summary>Canonical name of <see cref="WalCompactionEvalDeadEntries"/>.</summary>
    public const string WalCompactionEvalDeadEntriesName =
        "orleans.lattice.wal.compaction.eval.dead_entries";

    /// <summary>
    /// Counter of WAL garbage-collection passes the per-silo scheduler drove for a
    /// tree, tagged with <see cref="TagTree"/> and <see cref="TagOutcome"/>.
    /// <para>
    /// <b>Exactly one arm is affirmative.</b> Only
    /// <see cref="OutcomeReclaimed"/> states that WAL came back; every other arm
    /// states that nothing was trimmed and differs only in <i>why</i>. In
    /// particular <see cref="OutcomeIdle"/> must never be read as health, and
    /// <c>blocked = 0</c> is not evidence of reclamation - it is evidence that
    /// one named predicate did not fire (issue #2850).
    /// </para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <see cref="OutcomeReclaimed"/> - the pass trimmed at least one entry.
    ///     <b>Affirmative.</b>
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="OutcomeBlocked"/> - it reclaimed nothing because an
    ///     unusable durable materialiser pin disabled the cursor branch
    ///     (<see cref="WalGcCursorFloorState.BlockedByUnusablePin"/>).
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="OutcomeNoConsumer"/> - it reclaimed nothing because no
    ///     consumer has ever reported a cursor
    ///     (<see cref="WalGcCursorFloorState.NoCursorReported"/>), so the cursor
    ///     branch could not be evaluated at all.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="OutcomeIdle"/> - it evaluated a usable cursor floor
    ///     (<see cref="WalGcCursorFloorState.Available"/>) and found nothing
    ///     above it, with the tree inside its byte ceiling (or no ceiling
    ///     configured). The genuinely quiet, healthy case, and <i>only</i> that.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="OutcomeOverCeiling"/> - it evaluated a usable cursor floor
    ///     and found nothing above it, yet the tree is still over its configured
    ///     <see cref="LatticeOptions.WalMaxRetainedBytes"/>. Same floor state as
    ///     <see cref="OutcomeIdle"/> and the opposite condition: the safe trim
    ///     frontier is pinned below bytes the policy wants back (issue #3119).
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="OutcomeStranded"/> - it evaluated a usable cursor floor,
    ///     trimmed nothing, and its scan stopped on WAL it had to retain, with no
    ///     configured byte ceiling complaining about it. The <i>"could not"</i>
    ///     case that <see cref="OutcomeIdle"/> absorbed on every deployment that
    ///     set no ceiling (issue #3213).
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="OutcomeUnclassified"/> - the floor state was one this
    ///     build does not name. Structurally unreachable today and expected to
    ///     read a permanent measured zero; it exists so that a floor state added
    ///     later cannot be silently absorbed into <see cref="OutcomeIdle"/>.
    ///   </description></item>
    ///   <item><description>
    ///     <see cref="OutcomeFailed"/> - the pass threw.
    ///   </description></item>
    /// </list>
    /// <para>
    /// Pairing the reclaimed rate against the total pass rate gives the per-tree
    /// reclaim rate, and the failed rate isolates a wedged tree without needing
    /// to read the scheduler's logs.
    /// </para>
    /// <para>
    /// <b>Reclaimed outranks blocked on a partially blocked tree.</b> The
    /// cursor-branch block is evaluated per WAL partition (issue #2849), and the
    /// TTL branch is independent of the cursor branch, so a pass can trim in one
    /// partition while another stays blocked. Such a pass is labelled
    /// <see cref="OutcomeReclaimed"/>, because bytes genuinely came back. The
    /// block is still visible - the scheduler reads
    /// <see cref="LatticeWalGcReport.CursorFloorState"/> directly to drive the
    /// blocked-leaf remedy - so do not read a reclaimed pass as proof that no
    /// partition is blocked.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcPasses =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.passes", unit: "{pass}",
            description: "WAL garbage-collection passes driven by the per-silo scheduler, tagged by tree and outcome.");

    /// <summary>
    /// Histogram of the adaptive WAL garbage-collection cadence, in seconds: the
    /// interval the scheduler selected for a tree after its most recent pass,
    /// tagged with <see cref="TagTree"/>. The value moves inside the configured
    /// band <c>[<see cref="LatticeOptions.WalGcMinInterval"/>,
    /// <see cref="LatticeOptions.WalGcInterval"/>]</c>, so a sustained reading at
    /// the floor is a tree whose log is growing faster than one pass reclaims and
    /// a reading at the ceiling is a quiet tree. This is the instrument that shows
    /// the cadence responding to backlog.
    /// </summary>
    public static readonly Histogram<double> WalGcInterval =
        Meter.CreateHistogram<double>("orleans.lattice.wal.gc.interval", unit: "s",
            description: "Adaptive WAL garbage-collection interval selected per tree, tagged by tree.");

    /// <summary>
    /// Tag key naming <b>why</b> the WAL GC scheduler selected the wait it did on
    /// a pass, carried by <see cref="WalGcSchedulerBackoff"/> and
    /// <see cref="WalGcSchedulerConsecutiveFaults"/> (issue #3064).
    /// </summary>
    /// <remarks>
    /// The scheduler runs two independent scheduler-wide ladders and, before this
    /// tag existed, fed both into one unlabelled wait. A registry that could not
    /// be read and a silo with nothing to collect therefore produced byte-identical
    /// scheduler behaviour, which is the single most important distinction this
    /// scheduler has and it was unobservable.
    /// </remarks>
    public const string TagWalGcBackoffCause = "cause";

    /// <summary>
    /// <see cref="TagWalGcBackoffCause"/> = <c>scheduled</c> - the pass enumerated
    /// the registry, found trees, and selected an ordinary per-tree cadence. No
    /// scheduler-wide backoff is in force.
    /// </summary>
    /// <remarks>
    /// This is the steady-state arm, and it is also the <b>deployment witness</b>
    /// for the whole instrument pair: it is recorded on a healthy pass, which is
    /// the common case, so the series is present on any silo running this build.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> WalGcBackoffScheduled =
        new(TagWalGcBackoffCause, "scheduled");

    /// <summary>
    /// <see cref="TagWalGcBackoffCause"/> = <c>faulted</c> - the registry
    /// enumeration threw, so the pass learned nothing at all about what wanted
    /// collecting. This is the alarm arm.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcBackoffFaulted =
        new(TagWalGcBackoffCause, "faulted");

    /// <summary>
    /// <see cref="TagWalGcBackoffCause"/> = <c>empty</c> - the registry
    /// enumeration <b>succeeded</b> and reported no collectable tree. A correct
    /// observation of an idle silo, and deliberately not an alarm.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcBackoffEmpty =
        new(TagWalGcBackoffCause, "empty");

    /// <summary>
    /// Histogram of the scheduler-wide WAL garbage-collection backoff currently in
    /// force, in seconds, tagged with <see cref="TagWalGcBackoffCause"/> (issue
    /// #3064). Distinct from <see cref="WalGcInterval"/>, which is the adaptive
    /// cadence of a single <i>tree</i>; this is the whole scheduler's retry wait
    /// after a pass that collected nothing.
    /// <para>
    /// <b>Why this exists.</b> A WAL GC sweep that has backed off to its ceiling
    /// and one that is dead emit byte-identical scrapes: every per-tree series
    /// simply freezes, because a pass that collects nothing writes no per-tree
    /// series at all. An operator could not distinguish "asleep for an hour" from
    /// "the subsystem is gone", and that ambiguity cost the investigation behind
    /// issues #3064 / #3065 several days. This instrument is the discriminator.
    /// </para>
    /// <para>
    /// <b>Reading it.</b> <c>cause=scheduled</c> reports the floor and means no
    /// backoff is in force. <c>cause=faulted</c> is the registry enumeration
    /// throwing, and its ladder is capped well below the quiet ceiling so recovery
    /// is bounded. <c>cause=empty</c> is a successful enumeration of an idle silo
    /// and relaxes to the full <see cref="LatticeOptions.WalGcInterval"/> ceiling.
    /// The two ladders have <b>disjoint ranges above the fault ceiling</b>, so a
    /// high reading is self-identifying even before the tag is read.
    /// </para>
    /// <para>
    /// <b>Priming, and why the site must not move.</b> This is recorded once per
    /// pass from the scheduler's own loop, on <b>every</b> path - including the
    /// very first pass, and including a silo with no trees at all. That siting is
    /// load-bearing and deliberately <b>not</b> on the fault path. Primed at a site
    /// that only runs when a fault occurs, an absent series would mean either "no
    /// fault has happened" or "this build is not deployed" - and this epic
    /// confused exactly those two, more than once, at serious cost. Recorded
    /// unconditionally, an absent series means <b>only</b> "this build is not
    /// deployed, or the scheduler never started", which is a fact about the
    /// deployment and never about the system's health. Do not "tidy" this onto the
    /// fault path, and do not make it conditional on having trees:
    /// <see cref="WalGcInterval"/> is silent on a zero-tree silo and so cannot
    /// serve as the witness there, which is precisely why this one does not share
    /// its site.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalGcSchedulerBackoff =
        Meter.CreateHistogram<double>("orleans.lattice.wal.gc.scheduler_backoff", unit: "s",
            description: "Scheduler-wide WAL garbage-collection backoff currently in force, tagged by cause.");

    /// <summary>
    /// Histogram of consecutive failed WAL GC registry enumerations, tagged with
    /// <see cref="TagWalGcBackoffCause"/> (issue #3064). Reset to zero by any pass
    /// whose enumeration succeeded, so it is a <i>current streak</i> rather than a
    /// lifetime total - which is why it is a histogram and not a counter, a counter
    /// being unable to go back down.
    /// <para>
    /// Recorded beside <see cref="WalGcSchedulerBackoff"/> at the same
    /// unconditional per-pass site, so the same priming argument applies verbatim:
    /// a zero here is a measured zero, and an absent series is a statement about
    /// the build rather than about the registry. A streak that keeps returning to
    /// zero is transient fault absorption; one that only climbs is a registry the
    /// scheduler cannot read, and it is operator-actionable.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalGcSchedulerConsecutiveFaults =
        Meter.CreateHistogram<long>("orleans.lattice.wal.gc.scheduler_consecutive_faults", unit: "{fault}",
            description: "Consecutive failed WAL garbage-collection registry enumerations, tagged by cause.");

    /// <summary>
    /// Histogram of retained WAL bytes remaining after a garbage-collection pass,
    /// tagged with <see cref="TagTree"/>. Sampled from the pass's own
    /// <see cref="LatticeWalGcReport.RetainedBytesAfter"/>, so it costs no extra
    /// I/O. Read against <see cref="WalGcInterval"/> it answers the operational
    /// question this instrument pair exists for: is the backlog falling, and is
    /// the cadence tightening while it does.
    /// <para>
    /// <b>Byte accounting is a capability, and its absence is knowable rather
    /// than silent.</b> The series exists for a tree only when the byte-pressure
    /// policy is enabled (<see cref="LatticeOptions.WalMaxRetainedBytes"/> is
    /// set) <i>and</i> the configured <see cref="IWalStorageProvider"/> reports a
    /// retained byte size. <see cref="WalGcPasses"/> is emitted unconditionally
    /// for every pass, so a tree that is reporting passes but no backlog bytes is
    /// positively identifying a host without byte accounting - the two series are
    /// read together, and no consumer has to distinguish "no backlog" from "not
    /// measured". Reclaimed volume in that configuration is still observable in
    /// records through <see cref="WalEntriesTrimmed"/> and
    /// <see cref="OutcomeReclaimed"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalGcBacklogBytes =
        Meter.CreateHistogram<long>("orleans.lattice.wal.gc.backlog_bytes", unit: "By",
            description: "Retained WAL bytes remaining after a garbage-collection pass, tagged by tree.");

    /// <summary>
    /// How long, in seconds, since a tree's durable materialiser offset floor
    /// last ADVANCED, recorded once per garbage-collection pass and tagged with
    /// <see cref="TagTree"/> and <see cref="TagStatus"/> (issue #3300).
    /// <para>
    /// This is the instrument that answers "has this tree's durable floor moved
    /// in the last N hours?" directly, and it exists because nothing in the
    /// previous set could. Issue #3300 was a store whose durable floor did not
    /// advance by a single entry across eleven hours and at least 120 writes,
    /// losing every one of them; establishing that took a manual census,
    /// an archive diff and a controlled restart. Every instrument that was
    /// available either tracked WRITE VOLUME (which climbs happily while
    /// nothing becomes durable, and so answers a severity question in neither
    /// direction) or was ABSENT (which reads as "no problem here" to anyone who
    /// does not already know the series should exist). An operator should not
    /// have to run that experiment, and should not have to reason from a
    /// missing series.
    /// </para>
    /// <para>
    /// The <see cref="TagStatus"/> arm is the part that must not be collapsed,
    /// because it is where this repository's signature defect would otherwise
    /// reappear (see <see cref="WalGcTrimStopReason.DurabilityUnverified"/>):
    /// </para>
    /// <list type="bullet">
    /// <item><c>advanced</c> - the floor moved on this pass. Records 0, which is
    /// the genuinely healthy zero.</item>
    /// <item><c>stalled</c> - a floor exists and did not move. Records the age
    /// since it last did.</item>
    /// <item><c>absent</c> - NO floor could be established at all. Records the
    /// age since this pass first observed the tree, NOT zero. Recording zero
    /// here would make the worst state - nothing is known to be durable -
    /// byte-identical to the best one, which is the exact confusion that hid
    /// issue #3300.</item>
    /// </list>
    /// <para>
    /// Read it as a maximum per tree. A sustained <c>absent</c> arm, or a
    /// <c>stalled</c> age that grows without bound while the tree is taking
    /// writes, is a tree whose data is not becoming durable - regardless of how
    /// healthy any volume-tracking counter looks. Because the age is measured
    /// from process start when the floor has never advanced, a value close to
    /// the process uptime means the floor has NEVER moved in this process, which
    /// is the #3300 signature.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalGcDurableFloorStallSeconds =
        Meter.CreateHistogram<long>("orleans.lattice.wal.gc.durable_floor_stall_seconds", unit: "s",
            description: "Seconds since a tree's durable materialiser offset floor last advanced, tagged by tree and by state: advanced, stalled or absent.");

    /// <summary>
    /// Counter of WAL garbage-collection partition scans that trimmed a tree
    /// with <b>no durable materialiser offset floor</b> despite the durability
    /// hold (<see cref="LatticeOptions.WalDurabilityHoldCeilingBytes"/>) - either
    /// because the hold engaged and exhausted its ceiling, or because it could
    /// not engage at all - tagged with <see cref="TagTree"/> and
    /// <see cref="TagReason"/> (issue #3300).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the signal the hold exists to produce, and it is not a tuning
    /// hint. A non-zero value states that the collector released WAL entries
    /// that nothing is known to have durably applied, having first tried to
    /// retain them and run out of the budget it was given. That is the exact
    /// condition of issue #3300, which ran for eleven hours while every series
    /// the collector published read healthy. Alert on it.
    /// </para>
    /// <para>
    /// Raising the ceiling suppresses this counter for longer without changing
    /// anything about the underlying fault: the durable floor is still not
    /// advancing, and <see cref="WalGcDurableFloorStallSeconds"/> is the series
    /// that says so. Treat the pair together - this one says data was
    /// discarded, that one says for how long the cause has been present.
    /// </para>
    /// <para>
    /// Zero, here, is genuinely good news rather than an absence of news,
    /// because the hold is on by default
    /// (<see cref="LatticeOptions.DefaultWalDurabilityHoldCeilingBytes"/>): a
    /// flat zero means the check ran and found nothing to force. That holds only
    /// while the ceiling is left positive - set it to <c>0</c> and the hold never
    /// engages, so zero reverts to meaning the check is switched off rather than
    /// that it passed.
    /// </para>
    /// <para>
    /// <see cref="TagReason"/> says which way the hold failed to protect the
    /// partition, and the two arms are not interchangeable.
    /// <see cref="ReasonHoldForcedCeilingExhausted"/> means the hold engaged,
    /// retained up to its ceiling and then yielded - the fault is that the floor
    /// never arrived, and the ceiling merely bounded the damage.
    /// <see cref="ReasonHoldForcedUnmeasurableFootprint"/> means the hold never
    /// engaged at all, because the provider reports no retained bytes and a hold
    /// with nothing to bound it cannot be allowed to run by default. The second
    /// is a deployment defect in the storage provider and raising the ceiling
    /// will not touch it. Collapsing the two would repeat the conflation issue
    /// #3309 was raised to undo, where one arm meant both "checked and exhausted"
    /// and "never checked".
    /// </para>
    /// </remarks>
    public static readonly Counter<long> WalGcDurabilityHoldForced =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.durability_hold_forced",
            description: "WAL garbage-collection partition scans that trimmed without a durable materialiser offset floor despite the durability hold, tagged by tree and by reason: ceiling_exhausted or unmeasurable_footprint.");

    /// <summary>
    /// <see cref="TagReason"/> = <c>ceiling_exhausted</c> - the durability hold
    /// engaged, retained to
    /// <see cref="LatticeOptions.WalDurabilityHoldCeilingBytes"/> and then
    /// yielded, trimming records with no durable floor.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonHoldForcedCeilingExhausted =
        new(TagReason, "ceiling_exhausted");

    /// <summary>
    /// <see cref="TagReason"/> = <c>unmeasurable_footprint</c> - the durability
    /// hold did not engage because the provider reports no retained-byte figure,
    /// leaving the ceiling with nothing to measure and the hold with no bound.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonHoldForcedUnmeasurableFootprint =
        new(TagReason, "unmeasurable_footprint");

    /// <summary>
    /// Counter of WAL garbage-collection passes on which the durability hold
    /// actually engaged and retained a scan, tagged with <see cref="TagTree"/>
    /// and with <see cref="TagReason"/> = <c>never_pinned</c> or
    /// <c>pin_regressed</c> (issue #3300).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Distinct from <see cref="WalGcDurabilityHoldForced"/>, which counts the
    /// hold <i>yielding</i>. This counts it <i>working</i>, and exists because
    /// the <see cref="WalGcTrimStopReason.DurabilityHold"/> stop reason cannot
    /// say which of two very different conditions produced it.
    /// </para>
    /// <para>
    /// <c>never_pinned</c> is a tree on which no durable materialiser offset
    /// floor has ever been observed in this process: it is stalled, it will hold
    /// until its ceiling forces it, and it needs an operator to wire or repair a
    /// materialiser. <c>pin_regressed</c> is a tree whose floor existed and is
    /// currently absent - a rolling upgrade or leaf churn - which resolves
    /// without intervention as the leaves re-pin. Reporting both on one arm
    /// would tell an operator mid-upgrade that they had an outage.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> WalGcDurabilityHoldEngaged =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.durability_hold_engaged",
            description: "WAL garbage-collection passes on which the durability hold engaged and retained the scan, tagged by tree and by reason: never_pinned, pin_regressed, or cursor_unreadable.");

    /// <summary>
    /// <see cref="TagReason"/> = <c>never_pinned</c> - the durability hold
    /// engaged on a tree for which no durable materialiser offset floor has ever
    /// been observed. Stalled; it will not clear without intervention.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonHoldEngagedNeverPinned =
        new(TagReason, "never_pinned");

    /// <summary>
    /// <see cref="TagReason"/> = <c>pin_regressed</c> - the durability hold
    /// engaged on a tree whose durable materialiser offset floor was observed
    /// earlier in this process and is absent now. A bounded transient (rolling
    /// upgrade, leaf churn) that clears itself when the leaves re-pin.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonHoldEngagedPinRegressed =
        new(TagReason, "pin_regressed");

    /// <summary>
    /// <see cref="TagReason"/> = <c>cursor_unreadable</c> - the durability hold
    /// engaged because the cursor registry could not be read, so what is
    /// watching the tree is unknown (issue #3366). Distinct from the two arms
    /// above because it is not a statement about the pin history: the
    /// classification did not run, so neither of those arms has been measured.
    /// A non-zero value here means the WAL is being retained on absent evidence
    /// rather than on observed evidence, and the repair is to the registry
    /// read path - not to a materialiser.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonHoldEngagedCursorUnreadable =
        new(TagReason, "cursor_unreadable");

    /// <summary>
    /// <see cref="TagStatus"/> = <c>advanced</c> (the durable materialiser offset
    /// floor moved on this pass).
    /// </summary>
    public static readonly KeyValuePair<string, object?> StatusDurableFloorAdvanced =
        new(TagStatus, "advanced");

    /// <summary>
    /// <see cref="TagStatus"/> = <c>stalled</c> (a durable materialiser offset
    /// floor exists for this tree but did not move on this pass).
    /// </summary>
    public static readonly KeyValuePair<string, object?> StatusDurableFloorStalled =
        new(TagStatus, "stalled");

    /// <summary>
    /// <see cref="TagStatus"/> = <c>absent</c> (no durable materialiser offset
    /// floor could be established for this tree at all - the issue #3300 state,
    /// deliberately NOT reported as a zero stall age).
    /// </summary>
    public static readonly KeyValuePair<string, object?> StatusDurableFloorAbsent =
        new(TagStatus, "absent");

    /// <summary>
    /// Counter of WAL garbage-collection passes that found the tree's configured
    /// <see cref="LatticeOptions.WalMaxRetainedBytes"/> to be <b>arithmetically
    /// unreachable</b> against the working set the pass just measured - that is,
    /// below
    /// <see cref="LatticeOptions.WalMaxRetainedBytesWorkingSetMultiple"/> times
    /// the tree's logical retained payload. Tagged with <see cref="TagTree"/>.
    /// <para>
    /// <b>This is a statement about configuration, not about lag, and that is why
    /// it is a separate instrument rather than an eighth arm of
    /// <see cref="WalGcPasses"/>.</b> Those arms partition invocations - exactly
    /// one is recorded per pass, so their sum can never over-count - and an
    /// unsatisfiable ceiling is not a pass outcome at all. It co-occurs with
    /// <see cref="OutcomeReclaimed"/>, <see cref="OutcomeOverCeiling"/> and
    /// <see cref="OutcomeStranded"/> alike, so an arm would have had to take
    /// invocations away from whichever arm names them today: an operator alerting
    /// on <see cref="OutcomeStranded"/> would have watched it fall silent at the
    /// exact moment the condition it describes got worse. Recording the condition
    /// on its own counter leaves every existing arm meaning what it meant.
    /// </para>
    /// <para>
    /// <b>The two conditions demand opposite responses.</b>
    /// <see cref="OutcomeStranded"/> and <see cref="OutcomeOverCeiling"/> both say
    /// "bytes could not be reclaimed", which an operator reads as a lagging
    /// consumer or a pinned floor and acts on by unblocking the consumer. This
    /// counter says the ceiling itself cannot be met by a healthy tree of this
    /// size, whose remedy is to raise the ceiling (or shrink the tree) and for
    /// which chasing consumers is wasted effort. Before it existed the two
    /// rendered identically.
    /// </para>
    /// <para>
    /// <b>Why the multiple.</b> A log-structured provider reclaims space only by
    /// rewriting a segment, so dead bytes are a designed-in component of
    /// occupancy up to its compaction policy's share of the file, and the ceiling
    /// has been compared against physical occupancy since issue #3107. Designed
    /// steady-state occupancy is therefore a multiple of the live set, and a
    /// ceiling below that multiple is breached by a perfectly healthy tree. With
    /// <see cref="LatticeOptions.WalBytePressureReclaimTarget"/> at its default
    /// the disarm point sits below the natural floor of the compaction cycle too,
    /// so such a tree arms the advisory byte-pressure alarm permanently and can
    /// never clear it (issue #3242).
    /// </para>
    /// <para>
    /// <b>Why it reads the logical working set and not the occupancy figure the
    /// ceiling is actually compared against.</b> Compaction fires on a long
    /// period - one shard at a time, once its dead bytes reach the configured
    /// fraction - so physical occupancy is not a level but a sawtooth
    /// oscillating between the live set and that multiple of it. A check against
    /// occupancy would therefore read whatever phase of the cycle the pass
    /// happened to land in: at the peak it condemns a ceiling that is in fact
    /// reachable, and at the trough it reports a comfortable ceiling for a tree
    /// that will breach again at its next peak. Both readings are of the same
    /// tree under the same unchanged configuration, which is what makes
    /// occupancy the wrong quantity to decide a <i>sizing</i> question against -
    /// the gap between the two is routinely a large fraction of the live set on
    /// a real deployment, not a rounding difference. <b>The ceiling must clear
    /// the peak of the sawtooth, and only the logical total predicts where that
    /// peak is.</b>
    /// </para>
    /// <para>
    /// <b>This instrument is justified by the condition's silence, not by any
    /// particular deployment exhibiting it.</b> An unsatisfiable ceiling is
    /// indistinguishable, in every series that existed before this one, from a
    /// tree whose consumers are merely lagging - so it can persist indefinitely
    /// while looking exactly like a transient. That is true whether or not any
    /// tree is in the condition today, which is the point: a guard argued from a
    /// live incident stops being justified the moment the incident clears, and
    /// this one does not.
    /// </para>
    /// <para>
    /// Zero-primed per tree beside the pass-outcome arms, so a flat zero is a
    /// measured "this tree's ceiling is satisfiable" rather than silence. It is
    /// emitted only for a tree that has a ceiling configured <i>and</i> whose
    /// provider accounts logical bytes; on any other tree it reads the primed
    /// zero, exactly as <see cref="OutcomeOverCeiling"/> does. Pair the rate
    /// against <see cref="WalGcPasses"/> to see whether every pass agrees.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcCeilingUnsatisfiable =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.ceiling_unsatisfiable", unit: "{pass}",
            description: "WAL garbage-collection passes that found the configured WalMaxRetainedBytes unreachable against the tree's measured logical working set, tagged by tree.");

    /// <summary>
    /// Counter of WAL garbage-collection passes for which the durable
    /// leaf-materialiser <em>offset</em> floor could not be computed because the
    /// pin store was unreachable, tagged with <see cref="TagTree"/>. Emitted from
    /// <see cref="LatticeWalGc"/> whenever the offset-floor read
    /// (<c>IWalMaterialiserPinGrain.GetPinOffsetsAsync</c>) throws and the GC
    /// falls back to no offset floor for that pass.
    /// <para>
    /// The fallback is safe for that pass (the HLC floor still constrains the
    /// trim), but it was previously <b>completely silent</b>: a persistently
    /// unreachable pin store removed the offset floor on <em>every</em> pass with
    /// no signal, indistinguishable from a tree that legitimately has no offset
    /// floor to apply. The healthy "no offset floor" outcomes - a host that never
    /// wired the durable pin store, or a store that is reachable but reports no
    /// offsets - do <b>not</b> reach the swallowing catch and so do <b>not</b>
    /// increment this counter, which is what makes "no floor because unreachable"
    /// separable from "no floor because none needed" (issue #2314).
    /// </para>
    /// <para>
    /// A <b>transient</b> tick is expected and benign - the next pass retries once
    /// the store is reachable - and this counter also ticks during a rolling
    /// upgrade where an older pin grain has no <c>GetPinOffsetsAsync</c>. A
    /// <em>sustained</em> non-zero rate is the operational signal: the offset
    /// floor is not being applied and the low-HLC/high-offset reap class it exists
    /// to retain (see <see cref="WalGcPasses"/>) is protected only by the HLC
    /// floor until the store recovers.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcOffsetFloorUnavailable =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.offset_floor_unavailable", unit: "{pass}",
            description: "WAL GC passes that could not compute the durable offset floor because the pin store was unreachable, tagged by tree.");

    /// <summary>
    /// Counter of WAL GC per-shard trim scans, tagged with <see cref="TagTree"/>,
    /// with <see cref="TagShard"/> carrying the partition the scan covered, and
    /// with <see cref="TagReason"/> carrying the
    /// <see cref="WalGcTrimStopReason"/> that stopped the scan (issues #3149,
    /// #3207).
    /// <para>
    /// This is the series that says <b>why</b> a pass reclaimed nothing. Before
    /// it, a tree could sit permanently over its byte ceiling with a healthy
    /// consumer-cursor floor, classify every pass as
    /// <see cref="OutcomeOverCeiling"/>, reclaim zero bytes, and publish no
    /// statement anywhere as to the cause - because
    /// <see cref="WalGcBlockingPinState"/> is written only on the
    /// <see cref="OutcomeBlocked"/> path, which such a tree never takes. The
    /// breach was observable; the reason for it was not.
    /// </para>
    /// <para>
    /// The distinction the arms draw is the load-bearing part.
    /// <c>offset_floor</c> means a stale durable leaf checkpoint is holding the
    /// scan, and because that floor is a minimum over leaves, one lagging leaf
    /// strands the whole tree. The next three each mean the HLC eligibility
    /// predicate is holding it, and they name which of its independent clauses
    /// did (issue #3155): <c>cursor_floor</c> is a consumer cursor or TTL ceiling
    /// that has not advanced, <c>causal_frontier</c> is a replication origin whose
    /// stable frontier has not advanced, and <c>block_pin</c> is a buffering
    /// receiver holding entries back behind a published pin. Those three were one
    /// <c>not_eligible</c> arm until a tree stopping every scan on it could be
    /// shown to be stranded with no way to say by what. <c>exhausted</c> and
    /// <c>empty</c> are the healthy readings. A sustained run of
    /// <c>offset_floor</c> alongside a flat
    /// <see cref="StoragePolicyBytesReclaimed"/> is the signature of a tree whose
    /// floor covers none of its retained range.
    /// </para>
    /// <para>
    /// <b>The shard dimension is what separates a slow tree from a wedged one
    /// (issue #3207).</b> A stop arm on its own does not indict: a perfectly
    /// healthy shard releases thousands of entries and then stops at the first
    /// one it must retain, so it reports <c>offset_floor</c> exactly like a
    /// shard that has never released an entry in its life. Summed to the tree
    /// those two estates are indistinguishable. Per shard they separate
    /// exactly, by reading this arm against the equally shard-attributed
    /// <see cref="WalEntriesTrimmed"/>: an arm advancing for a shard whose
    /// entries-trimmed counter stays flat over the window is a shard that is
    /// asked on every pass and releases nothing. That shard is invisible on
    /// every other arm it publishes, because all of them are derived from the
    /// provider's dead-byte accounting and dead bytes only rise as a
    /// consequence of the release that is not happening - so it reports zero
    /// dead bytes, a zero dead ratio, zero compactions and zero reclaimed
    /// bytes, which is the best score in the fleet on all four.
    /// </para>
    /// <para>
    /// Every arm - <c>exhausted</c>, <c>empty</c>, <c>offset_floor</c>,
    /// <c>cursor_floor</c>, <c>causal_frontier</c>, <c>block_pin</c>,
    /// <c>durability_unverified</c>, <c>durability_hold</c> and
    /// <c>durable_offset_refusal</c> - is
    /// zero-primed per shard per tree on every pass, so an absent series
    /// means this silo is not running WAL GC for the tree rather than that the
    /// tree never stopped a scan. That priming is what lets a reader treat a flat
    /// <c>offset_floor</c> zero as a measured absence, which is precisely the
    /// inference that was unavailable before this instrument existed. Priming
    /// covers the whole partition range rather than only the partitions this
    /// silo resolves a provider for, so a partition pinned elsewhere publishes
    /// nine flat zeros and no entries-trimmed series, which is a distinguishable
    /// reading rather than an absent one.
    /// </para>
    /// <para>
    /// Recorded once per shard per pass, so a tree with eight shards contributes
    /// eight increments per pass and the arms sum to the shard count. Diagnostic
    /// only for every arm but <c>durability_hold</c> and
    /// <c>durable_offset_refusal</c>, which report stops the collector chose
    /// rather than ones it merely observed (issue #3300).
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcTrimStops =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.trim_stop", unit: "{scan}",
            description: "WAL GC per-shard trim scans tagged by tree, by shard and by the reason the scan stopped: offset_floor, cursor_floor, causal_frontier, block_pin, durability_unverified, durability_hold, durable_offset_refusal, exhausted or empty.");

    /// <summary>
    /// <see cref="TagReason"/> = <c>exhausted</c> (a trim scan that consumed
    /// every entry the provider offered without meeting one it had to retain).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimExhausted = new(TagReason, "exhausted");

    /// <summary>
    /// <see cref="TagReason"/> = <c>empty</c> (a trim scan over a shard that held
    /// no entries at all).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimEmpty = new(TagReason, "empty");

    /// <summary>
    /// <see cref="TagReason"/> = <c>offset_floor</c> (a trim scan stopped by the
    /// durable materialiser offset floor - see
    /// <see cref="WalGcTrimStopReason.OffsetFloor"/>, the arm that identifies a
    /// tree stranded by one lagging leaf checkpoint).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimOffsetFloor = new(TagReason, "offset_floor");

    /// <summary>
    /// <see cref="TagReason"/> = <c>cursor_floor</c> (a trim scan stopped by the
    /// HLC clause of the eligibility predicate - see
    /// <see cref="WalGcTrimStopReason.CursorFloor"/>, the arm that indicts a
    /// consumer cursor or a TTL ceiling that has not advanced).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimCursorFloor = new(TagReason, "cursor_floor");

    /// <summary>
    /// <see cref="TagReason"/> = <c>causal_frontier</c> (a trim scan stopped by
    /// the causal-stable clause - see
    /// <see cref="WalGcTrimStopReason.CausalFrontier"/>, the arm that indicts a
    /// replication origin whose stable frontier has not advanced).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimCausalFrontier = new(TagReason, "causal_frontier");

    /// <summary>
    /// <see cref="TagReason"/> = <c>block_pin</c> (a trim scan stopped by a
    /// consumer's buffer pin - see <see cref="WalGcTrimStopReason.BlockPin"/>,
    /// the arm that indicts a buffering receiver holding entries back).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimBlockPin = new(TagReason, "block_pin");

    /// <summary>
    /// <see cref="TagReason"/> = <c>durability_unverified</c> (a trim scan
    /// consumed a non-empty shard with no durable materialiser offset floor
    /// available at all - see
    /// <see cref="WalGcTrimStopReason.DurabilityUnverified"/>, the arm that
    /// separates "everything was releasable" from "I could not tell whether any
    /// of it was", which issue #3300 was lost inside of).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimDurabilityUnverified =
        new(TagReason, "durability_unverified");

    /// <summary>
    /// <see cref="TagReason"/> = <c>durability_hold</c> - the scan retained a
    /// non-empty shard untouched because the tree had no durable materialiser
    /// offset floor and a durability hold is configured and not yet exhausted
    /// (<see cref="WalGcTrimStopReason.DurabilityHold"/>, issue #3300).
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimDurabilityHold =
        new(TagReason, "durability_hold");

    /// <summary>
    /// <see cref="TagReason"/> = <c>durable_offset_refusal</c> - the scan stopped
    /// at an entry the consumer cursor would have admitted and the durable
    /// materialiser offset floor overruled
    /// (<see cref="WalGcTrimStopReason.DurableOffsetRefusal"/>, issue #3300).
    /// <para>
    /// Read against <see cref="ReasonTrimOffsetFloor"/> rather than alongside it.
    /// <c>offset_floor</c> means the scan walked up to the floor and stopped at
    /// its edge; this means it stopped BELOW the floor because a consumer the
    /// floor does not speak for still needs the entry. A sustained run indicts a
    /// leaf whose durable checkpoint or snapshot coverage has stopped advancing,
    /// not a consumer that is merely behind.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReasonTrimDurableOffsetRefusal =
        new(TagReason, "durable_offset_refusal");

    /// <summary>
    /// Counter of WAL garbage-collection passes for which no retained-byte
    /// backlog could be sampled, tagged with <see cref="TagTree"/> and with
    /// <see cref="TagReason"/> = <c>policy_disabled</c> or
    /// <c>provider_unsupported</c>. This is the positive "not measured" signal
    /// for <see cref="WalGcBacklogBytes"/> (issue #2694).
    /// <para>
    /// <see cref="WalGcBacklogBytes"/> records only when the pass actually
    /// sampled bytes, so on a host with byte accounting turned off it publishes
    /// <b>no series at all</b> - a shape a reader cannot distinguish from a dead
    /// subsystem, a broken instrument, or a genuine zero backlog without opening
    /// the source. That ambiguity is the defect: the prior contract asked the
    /// reader to infer "not measured" from the <i>absence</i> of one series
    /// beside the presence of another (<see cref="WalGcPasses"/>), which is an
    /// inference from silence and is exactly what produced the wrong,
    /// publicly-retracted diagnosis in issue #2692.
    /// </para>
    /// <para>
    /// This counter states it instead, and separates the two causes a reader
    /// would act on differently: <c>policy_disabled</c> means
    /// <see cref="LatticeOptions.WalMaxRetainedBytes"/> is unset and setting it
    /// turns byte accounting on, whereas <c>provider_unsupported</c> means the
    /// policy <i>is</i> enabled but the configured <see cref="IWalStorageProvider"/>
    /// returned no retained byte size, so the remedy is a different provider.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcBacklogBytesUnavailable =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.backlog_bytes_unavailable", unit: "{pass}",
            description: "WAL GC passes that sampled no retained-byte backlog, tagged by tree and by reason (policy_disabled/provider_unsupported).");

    /// <summary><see cref="TagReason"/> = <c>policy_disabled</c> (byte accounting is off because <see cref="LatticeOptions.WalMaxRetainedBytes"/> is unset).</summary>
    public static readonly KeyValuePair<string, object?> ReasonBytePolicyDisabled = new(TagReason, "policy_disabled");

    /// <summary><see cref="TagReason"/> = <c>provider_unsupported</c> (the byte-pressure policy is enabled but the WAL storage provider reports no retained byte size).</summary>
    public static readonly KeyValuePair<string, object?> ReasonByteProviderUnsupported = new(TagReason, "provider_unsupported");

    /// <summary><see cref="TagOutcome"/> = <c>reclaimed</c> (a WAL GC pass that trimmed at least one entry).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeReclaimed = new(TagOutcome, "reclaimed");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>idle</c> (a WAL GC pass that evaluated a
    /// usable consumer-cursor floor - <see cref="WalGcCursorFloorState.Available"/> -
    /// and found nothing above the trim floor).
    /// <para>
    /// <b>Idle means "nothing was trimmed", not "healthy".</b> It is the quiet
    /// steady state only because every other reason for trimming nothing now has
    /// its own arm. Before <see cref="OutcomeBlocked"/> existed this value also
    /// absorbed blocked passes, which is what let a stranded tree and a quiet one
    /// present identically (issue #2702); it then went on absorbing
    /// <see cref="WalGcCursorFloorState.NoCursorReported"/> - a pass that could
    /// not evaluate the cursor branch at all - which is what let
    /// <c>blocked = 0, idle = n</c> be misread as n healthy trees (issue #2850).
    /// Both are now separate arms, so a rise in <c>idle</c> is a statement about
    /// a tree with a working floor and no backlog above it.
    /// </para>
    /// <para>
    /// <see cref="OutcomeOverCeiling"/> is the third such split (issue #3119),
    /// and the one that most directly contradicted the word: a tree over its
    /// configured <see cref="LatticeOptions.WalMaxRetainedBytes"/> that reclaims
    /// nothing has a working floor and an enormous backlog, which is the
    /// opposite of quiet. Only with that arm split out is <c>idle</c> the
    /// healthy case rather than merely the unexplained one.
    /// </para>
    /// <para>
    /// <see cref="OutcomeStranded"/> is the fourth (issue #3213), and it is what
    /// makes the previous sentence true on a silo that configured no ceiling.
    /// <c>over_ceiling</c> can only fire where
    /// <see cref="LatticeOptions.WalMaxRetainedBytes"/> is set, and that option
    /// has no default, so until this arm existed a stranded tree on a stock
    /// deployment still reported <c>idle</c> - the arm that asserts health - for
    /// as long as it stayed stranded.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeIdle = new(TagOutcome, "idle");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>over_ceiling</c> (a WAL GC pass that
    /// evaluated a usable consumer-cursor floor, trimmed nothing, and left the
    /// tree still above its configured
    /// <see cref="LatticeOptions.WalMaxRetainedBytes"/> - see
    /// <see cref="LatticeWalGcReport.BytePressureOverThreshold"/>).
    /// <para>
    /// Split out of <see cref="OutcomeIdle"/> by issue #3119. The floor state is
    /// <see cref="WalGcCursorFloorState.Available"/> in both cases, so before
    /// this arm existed the two were the same measurement: a tree whose
    /// consumers are lagging far enough to breach the operator's byte ceiling
    /// reported as quiet and healthy. That is not a presentational complaint -
    /// <c>idle</c> is the arm an operator reads as "nothing to do", so the
    /// breach was visible only on a different instrument
    /// (<see cref="StoragePolicyOverThresholdName"/>) that a pass-rate panel
    /// does not show.
    /// </para>
    /// <para>
    /// This arm says the safe trim frontier is pinned below bytes the policy
    /// wants back: the cursor branch ran, found nothing it was permitted to
    /// remove, and the footprint is still over the ceiling. It is advisory about
    /// the <i>cause</i> and definite about the <i>condition</i> - the GC never
    /// trims past the safe frontier to honour a ceiling, so this is "the bytes
    /// could not be safely reclaimed", never "the trim failed".
    /// </para>
    /// <para>
    /// It is reported only when the byte-pressure policy is enabled and the
    /// provider supports byte accounting; otherwise there is no ceiling to
    /// breach and such a pass stays <see cref="OutcomeIdle"/>. Like the other
    /// arms it is primed at zero per collected tree, so an absent series means
    /// this silo is not reporting rather than that the tree never breached.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeOverCeiling = new(TagOutcome, "over_ceiling");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>stranded</c> (a WAL GC pass that evaluated a
    /// usable cursor floor, trimmed nothing, and left WAL behind that it could
    /// not reclaim - see <see cref="LatticeWalGcReport.RetainedBacklog"/>).
    /// <para>
    /// The fourth split out of <see cref="OutcomeIdle"/> (issue #3213), and the
    /// one that closes it for a deployment that configured nothing. <c>idle</c>
    /// conflated <i>"nothing to do"</i> with <i>"could not do anything"</i>: a
    /// quiet tree and a tree whose whole retained range sits behind a pinned trim
    /// frontier both report a usable floor and trim nothing. <c>over_ceiling</c>
    /// separates the second case only where an operator set
    /// <see cref="LatticeOptions.WalMaxRetainedBytes"/>, and that option has no
    /// default, so on every other deployment the conflation survived intact.
    /// </para>
    /// <para>
    /// This arm asks a question the byte policy cannot: the trim scan already
    /// records <i>where it stopped</i>, and a scan that stopped at an entry it had
    /// to retain is a direct observation of backlog needing no byte accounting and
    /// no configuration. So a tree stranded behind an offset floor is nameable on
    /// a stock silo, which is what it was not before.
    /// </para>
    /// <para>
    /// It sits below <see cref="OutcomeOverCeiling"/> in precedence, so the arms
    /// stay mutually exclusive and a breaching tree keeps its more specific
    /// diagnosis; a tree reaching this arm is one whose backlog no configured
    /// ceiling is complaining about. Like the other arms it is primed at zero per
    /// collected tree, so an absent series means this silo is not reporting rather
    /// than that no tree was ever stranded.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeStranded = new(TagOutcome, "stranded");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>no_consumer</c> (a WAL GC pass that
    /// reclaimed nothing because no consumer has ever reported a cursor for the
    /// tree - <see cref="WalGcCursorFloorState.NoCursorReported"/>).
    /// <para>
    /// Split out of <see cref="OutcomeIdle"/> by issue #2850. The two are not
    /// interchangeable: <c>idle</c> says the cursor branch ran and found nothing,
    /// whereas this says the cursor branch could not run, so the pass produced no
    /// information about the tree's backlog at all. A tree nobody consumes is
    /// legitimately quiet, but a tree whose consumers were expected to report and
    /// did not is a wiring fault, and that is the distinction this arm makes
    /// available.
    /// </para>
    /// <para>
    /// Primed at zero per tree alongside the other arms, so an absent series
    /// means this silo is not reporting rather than that the state never
    /// occurred.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeNoConsumer = new(TagOutcome, "no_consumer");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>blocked</c> (a WAL GC pass that reclaimed
    /// nothing because the consumer-cursor branch was disabled by an unusable
    /// durable materialiser pin - see
    /// <see cref="WalGcCursorFloorState.BlockedByUnusablePin"/>).
    /// <para>
    /// This is a defect state, not a quiet one: the tree cannot reclaim at all,
    /// so the WAL it already holds is permanently unreleasable and no retention
    /// or compaction setting can release it. It is separated from
    /// <see cref="OutcomeIdle"/> because the two demand opposite responses, and
    /// because the same predicate drives the scheduler's backoff - so before
    /// this value existed a blocked tree was scheduled <i>least</i> often
    /// precisely when it needed attention most.
    /// </para>
    /// <para>
    /// <b>This arm is the discriminator; the footprint is not.</b> Do not use
    /// growth, absence of growth, byte count or growth stopping to tell a
    /// blocked tree from a quiet one. A tree only grows while it is being
    /// written to, so a blocked tree reads flat the rest of the time:
    /// <c>repo-context-vector-payload</c>, which has never trimmed a byte in its
    /// lifetime, measured flat for 5.5 minutes of an 8-minute window. Each of
    /// those readings returns the benign answer at exactly the moment it should
    /// not, which is why the classification is recorded as its own arm here.
    /// </para>
    /// <para>
    /// The series is primed at zero for every tree the scheduler collects, so
    /// its absence means "this silo is not reporting" and a flat zero means
    /// "measured, never blocked". That distinction is load-bearing: a repair
    /// that unblocks a tree makes the counter stop advancing, and without
    /// priming the series would instead <i>vanish</i> at exactly the moment a
    /// reader needs to confirm the tree is healthy rather than silent.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeBlocked = new(TagOutcome, "blocked");

    /// <summary><see cref="TagOutcome"/> = <c>failed</c> (a WAL GC pass that threw).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeFailed = new(TagOutcome, "failed");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>unclassified</c> (a WAL GC pass that
    /// reclaimed nothing and whose
    /// <see cref="LatticeWalGcReport.CursorFloorState"/> is a value this build
    /// does not name).
    /// <para>
    /// No pass can reach this arm today: <see cref="WalGcCursorFloorState"/> has
    /// three members and all three are named -
    /// <see cref="WalGcCursorFloorState.Available"/> by
    /// <see cref="OutcomeIdle"/>, <see cref="OutcomeOverCeiling"/> and
    /// <see cref="OutcomeStranded"/> between them, and the other two by
    /// <see cref="OutcomeNoConsumer"/> and
    /// <see cref="OutcomeBlocked"/>. A permanent measured zero here is therefore
    /// the expected reading and is exactly the point: the arm exists so that a
    /// floor state added later falls somewhere it can be seen, instead of being
    /// absorbed by whichever arm happens to be the classifier's fallback.
    /// </para>
    /// <para>
    /// This is the same defect class the partition was split to remove (issue
    /// #2850): a catch-all arm destroys the guarantee that an arm reading zero is
    /// a measured absence, because it silently swallows the states nobody named.
    /// Making the fallback its own arm keeps <see cref="OutcomeIdle"/> honest
    /// without having to predict which state gets added next.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeUnclassified = new(TagOutcome, "unclassified");

    // --- Leaf-materialiser durable pin instruments (issue #1030) ------------

    /// <summary>
    /// Counter of durable writes to the leaf-materialiser pin store, emitted by
    /// <c>WalMaterialiserPinGrain</c> on every <c>WriteStateAsync</c>. Tagged
    /// with <see cref="TagOutcome"/> = <c>birth</c> (a synchronous through-write
    /// seeded by a new leaf's block pin) or <c>coalesced</c> (a debounced flush
    /// draining one or more advancing reports). The pre-#1030 shape wrote once
    /// per advancing report through a single per-tree grain; coalescing collapses
    /// a report burst to one write per shard per flush window, so a sustained
    /// <c>coalesced</c> rate far below the report rate confirms the fan-in
    /// hotspot fix is engaged.
    /// </summary>
    public static readonly Counter<long> MaterialiserPinDurableWrites =
        Meter.CreateCounter<long>("orleans.lattice.materialiser.pin.durable_writes", unit: "{write}",
            description: "Durable writes to the leaf-materialiser pin store, tagged by birth/coalesced outcome.");

    /// <summary>
    /// Histogram of <b>caller-observed</b> durable leaf-materialiser pin write
    /// duration, in milliseconds, recorded by <c>LeafCursorReporter</c> around
    /// each pin grain call whose duration reached
    /// <see cref="LatticeOptions.WalSaturationMaterialiserPinLatencyThreshold"/>
    /// (or which faulted). Tagged with <see cref="TagTree"/>.
    /// <para>
    /// This is the durable-storage counterpart to
    /// <see cref="MaterialiserDrainLag"/>, which is derived from the in-memory
    /// cursor registry and therefore cannot observe a stalled pin store (issue
    /// #2015). Measured at the call site so it includes the time a report spent
    /// queued ahead of the shard's non-reentrant activation - what the reporting
    /// leaf actually experiences. Only emitted when the input is enabled; the
    /// option defaults to <c>null</c>.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> MaterialiserPinDurableWriteLatency =
        Meter.CreateHistogram<double>("orleans.lattice.materialiser.pin.durable_write_latency", unit: "ms",
            description: "Caller-observed durable leaf-materialiser pin write duration, tagged by tree.");

    /// <summary>
    /// Counter of coalescible leaf-materialiser pin reports shed by
    /// <c>LeafCursorReporter</c> because the target shard's most recent durable
    /// write demonstrated the store is not keeping up. Tagged with
    /// <see cref="TagTree"/>.
    /// <para>
    /// Shedding is safe for <i>durability</i> - a shed report leaves the durable
    /// pin staler, which only ever retains more WAL - and it is the caller-side
    /// half of the issue #2012 fix: declining to enqueue removes queueing delay
    /// that a grain-side refusal could not, because a refusal still has to reach
    /// the front of the non-reentrancy queue before it can be issued. A
    /// sustained non-zero rate means the pin store is the bottleneck; pair it
    /// with <see cref="MaterialiserPinDurableWriteLatency"/> and consider raising
    /// <see cref="LatticeOptions.WalMaterialiserPinBuckets"/>.
    /// </para>
    /// <para>
    /// <b>"Only ever retains more WAL" is a statement about correctness, not
    /// about boundedness, and issue #3310 is the case where the distinction
    /// binds.</b> Because a shed report is not merely deferred but dropped, and
    /// because the write whose cost opens the shed window is itself exempt from
    /// the gate, a shard under sustained pressure can re-open its own window
    /// indefinitely. The durable pin then stops restamping entirely while the
    /// checkpoint it should be tracking advances - observed on a live estate as
    /// an offset floor frozen at a single value across 40 minutes while the WAL
    /// grew 1610 -> 1785 MB and the checkpoint passed it by more than 9,000
    /// offsets. Read this counter with
    /// <see cref="MaterialiserPinShedStallSeconds"/>, which is the series that
    /// distinguishes a healthy burst of shedding from a shard that has not let a
    /// report through in hours: this counter rises identically in both cases.
    /// </para>
    /// <para>
    /// Tagged with <see cref="TagPinShard"/> as well as <see cref="TagTree"/>
    /// since issue #3310. Summed to the tree, an actively-reporting majority of
    /// pin shards masks a stalled minority - the same masking that
    /// <see cref="WalEntriesTrimmed"/> warns about on its own axis. Note the two
    /// shard axes are <b>not</b> joinable; see <see cref="TagPinShard"/>.
    /// </para>
    /// </summary>
    public static readonly Counter<long> MaterialiserPinReportsShed =
        Meter.CreateCounter<long>("orleans.lattice.materialiser.pin.reports_shed", unit: "{report}",
            description: "Coalescible leaf-materialiser pin reports shed under durable pin-store pressure, tagged by tree and pin shard.");

    /// <summary>
    /// Counter of coalescible leaf-materialiser pin reports that were
    /// <b>forced through</b> a live shed window because the shard had been
    /// shedding continuously for longer than
    /// <see cref="LatticeOptions.WalMaterialiserPinShedCeiling"/>. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagPinShard"/>.
    /// <para>
    /// <b>This is the loud half of a bound, and a non-zero value is a report
    /// about the estate rather than an error.</b> It means the self-tuning shed
    /// window of issue #2012 stopped behaving as the short hold-off it was
    /// designed to be and became a de facto latch, and that the ceiling added
    /// for issue #3310 broke that latch to let coverage restamp. Forcing costs
    /// exactly one enqueued write per ceiling period per shard, so it bounds
    /// pin staleness - and therefore retained WAL - at the price of a duty cycle
    /// the #2012 shedding still dominates.
    /// </para>
    /// <para>
    /// <b>Forcing cannot cause data loss and is not a fail-open.</b> A forced
    /// report publishes <i>more</i> durability evidence, never less, and the
    /// offset it carries was already clamped to
    /// <c>min(checkpoint, durable snapshot coverage)</c> inside the leaf by
    /// <c>ResolveDurablePinForPartition</c> before the reporter ever saw it. No
    /// scheduling decision at this seam can produce an offset exceeding proven
    /// durable coverage, so this path cannot authorise a trim past it - the
    /// failure mode of issue #3300, which is this same seam failing in the
    /// opposite direction.
    /// </para>
    /// <para>
    /// The ceiling is opt-in, so this counter is flat at zero unless
    /// <see cref="LatticeOptions.WalMaterialiserPinShedCeiling"/> is configured.
    /// A flat zero therefore means "no bound is armed", not "no stall is
    /// happening"; <see cref="MaterialiserPinShedStallSeconds"/> is the series
    /// that reports the stall itself and is emitted either way.
    /// </para>
    /// </summary>
    public static readonly Counter<long> MaterialiserPinShedForced =
        Meter.CreateCounter<long>("orleans.lattice.materialiser.pin.shed_forced", unit: "{report}",
            description: "Pin reports forced through a shed window that exceeded the configured ceiling, tagged by tree and pin shard.");

    /// <summary>
    /// Observable gauge of how long each durable leaf-materialiser pin shard has
    /// been shedding <b>continuously</b> - the age of its current unbroken run
    /// of shed reports, in seconds. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagPinShard"/>. Reads <c>0</c> for a shard that is not
    /// currently shedding, and resets the instant any report gets through,
    /// whether the window lapsed naturally or
    /// <see cref="MaterialiserPinShedForced"/> broke it.
    /// <para>
    /// <b>This is the only series that separates healthy shedding from a latched
    /// shard, and it exists because a counter cannot.</b>
    /// <see cref="MaterialiserPinReportsShed"/> rises at the same rate whether a
    /// shard sheds a burst and recovers within a second or has not restamped
    /// coverage since the process started - the volume of shed work is identical,
    /// and only elapsed time without progress tells the two apart. That is the
    /// same reasoning that produced
    /// <see cref="WalGcDurableFloorStallSeconds"/> for issue #3300, one layer
    /// further down: that series reports <i>that</i> a tree's durable floor has
    /// stalled and explicitly directs the operator to suspect the materialiser,
    /// the checkpoint flush, or the collector; this one answers which, and on
    /// which shard.
    /// </para>
    /// <para>
    /// <b>Emitted regardless of whether the ceiling is configured, which is the
    /// point.</b> With
    /// <see cref="LatticeOptions.WalMaterialiserPinShedCeiling"/> armed, no run
    /// can exceed the ceiling and this gauge is a bounded sawtooth. With the
    /// ceiling left at its default of <c>null</c>, runs are unbounded and this
    /// gauge is the <i>only</i> thing that makes that visible - so a stall
    /// cannot be both unbounded and silent, which is the pair of properties
    /// issue #3310 set out to break.
    /// </para>
    /// <para>
    /// A shard that stops being reported to altogether holds its run open and
    /// the value keeps climbing. That is deliberate: the quantity is "time since
    /// this shard last let a coalescible report through", and a shard nobody
    /// reports to is restamping coverage exactly as little as one that sheds
    /// every report.
    /// </para>
    /// </summary>
    public static readonly ObservableGauge<long> MaterialiserPinShedStallSeconds =
        Meter.CreateObservableGauge(
            "orleans.lattice.materialiser.pin.shed_stall_seconds",
            static () => BPlusTree.Grains.WalMaterialiserPinPressure.ObserveShedStalls(),
            unit: "s",
            description: "Age of each pin shard's current unbroken shed run, tagged by tree and pin shard.");

    /// <summary>
    /// Counter of leaf-materialiser pin merges classified by what the merge
    /// actually moved, tagged with <see cref="TagTree"/> and
    /// <see cref="TagOutcome"/> = <c>both</c>, <c>offset_only</c>,
    /// <c>frontier_only</c>, or <c>none</c> (issues #2694, #3163).
    /// <para>
    /// <b>The four arms are a partition</b> - the complete truth table of the
    /// two axes a merge advances independently - so exactly one is recorded per
    /// merged report and each axis's marginal is recoverable by summing:
    /// offset advanced is <c>both</c> + <c>offset_only</c>, frontier advanced
    /// is <c>both</c> + <c>frontier_only</c>. They were three first-match arms
    /// until issue #3163, when <c>offset</c> was found to mean "the offset
    /// advanced and the frontier is unknown": it absorbed every merge that also
    /// advanced the frontier, so an absent <c>frontier_only</c> series did not
    /// show a flat frontier, only one that never advanced alone. See
    /// <see cref="BPlusTree.MaterialiserPinAdvanceOutcome"/>.
    /// </para>
    /// <para>
    /// <see cref="MaterialiserPinDurableWrites"/> counts <i>writes</i> and tags
    /// them <c>birth</c>/<c>coalesced</c>, which describes how a write was
    /// scheduled and not what it achieved. Neither value distinguishes a pin
    /// whose checkpoint <b>offset advanced</b> from one rewritten at the same
    /// offset, and with bucketing a single advancing pin rewrites its whole
    /// bucket - so the write rate is not even proportional to the advance rate.
    /// </para>
    /// <para>
    /// Offset advancement is the quantity that determines whether the WAL GC
    /// <i>offset</i> floor can move (the GC reads
    /// <c>IWalMaterialiserPinGrain.GetPinOffsetsAsync</c>), so <c>both</c> +
    /// <c>offset_only</c> is the sum to read when asking "why is retained WAL
    /// not being reclaimed?". A healthy <c>frontier_only</c> rate with both
    /// offset arms flat is the specific shape of a floor that cannot move while
    /// pins are otherwise being maintained; the converse - a healthy
    /// <c>offset_only</c> rate with <c>both</c> flat - is a consumer advancing
    /// in offset space alone, which cannot release a WAL entry either, because
    /// the offset floor only lowers a trim point that the HLC clauses already
    /// authorised. <c>none</c> counts a report that was fully coalesced away.
    /// </para>
    /// <para>
    /// Every arm is zero-primed once per pin-shard activation, so all four
    /// series exist for any tree with a live pin grain and a flat arm is a
    /// measured zero rather than an absence.
    /// </para>
    /// </summary>
    public static readonly Counter<long> MaterialiserPinAdvances =
        Meter.CreateCounter<long>("orleans.lattice.materialiser.pin.advances", unit: "{report}",
            description: "Leaf-materialiser pin merges partitioned by which axes advanced: both, offset_only, frontier_only, or none.");

    /// <summary><see cref="TagOutcome"/> = <c>both</c> (a pin merge that advanced the HLC frontier and the durable checkpoint offset together).</summary>
    public static readonly KeyValuePair<string, object?> OutcomePinBothAdvanced = new(TagOutcome, "both");

    /// <summary><see cref="TagOutcome"/> = <c>offset_only</c> (a pin merge that advanced the consumer's durable checkpoint offset while leaving the HLC frontier where it was).</summary>
    public static readonly KeyValuePair<string, object?> OutcomePinOffsetOnly = new(TagOutcome, "offset_only");

    /// <summary><see cref="TagOutcome"/> = <c>frontier_only</c> (a pin merge that advanced the HLC frontier while leaving the checkpoint offset where it was).</summary>
    public static readonly KeyValuePair<string, object?> OutcomePinFrontierOnly = new(TagOutcome, "frontier_only");

    /// <summary><see cref="TagOutcome"/> = <c>none</c> (a pin merge fully coalesced away: neither the frontier nor the offset moved).</summary>
    public static readonly KeyValuePair<string, object?> OutcomePinNoAdvance = new(TagOutcome, "none");

    /// <summary>
    /// Histogram of leaf-materialiser drain lag, in milliseconds, recorded by the
    /// WAL saturation sampler on every tick
    /// (<see cref="LatticeOptions.WalSaturationSampleInterval"/>, default 200 ms)
    /// while the drain-lag input is enabled. Measured live from in-memory state as
    /// the WAL head wall-clock timestamp minus the slowest fresh
    /// leaf-materialiser cursor frontier, clamped at zero. The freshness filter is
    /// a lag-plane-only classifier input; cold consumers remain registered for the
    /// WAL GC trim floor. Recorded for every checked tree, not only the
    /// over-threshold ones, so the histogram carries the whole distribution leading
    /// up to a trip; a tree with no fresh materialiser frontier yet records a zero
    /// rather than being skipped.
    /// <para>
    /// A rising drain lag is the back-pressure signal (issue #1030): when it stays
    /// at or above <see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/>
    /// for <see cref="LatticeOptions.WalSaturationMaterialiserLagSampleWindows"/>
    /// consecutive sampler windows the tree is held at
    /// <see cref="WalSaturationState.Throttled"/>. Unlike the dispatch-timeout,
    /// provider-failure, and flush-latency inputs, this one never escalates to
    /// <see cref="WalSaturationState.Saturated"/>: it paces callers without ever
    /// tripping the writer admission gate's fast-fail path.
    /// </para>
    /// <para>
    /// Measures the <b>in-memory</b> cursor, so it does not observe the durable
    /// materialiser pin store that actually sets the WAL retention floor. A stalled
    /// pin store therefore reads as healthy drain lag (issue #2015); pair this with
    /// <see cref="MaterialiserPinDurableWriteLatency"/>, which measures the durable
    /// write directly and is the input that closes that gap.
    /// </para>
    /// Tagged with <see cref="TagTree"/>.
    /// </summary>
    public static readonly Histogram<double> MaterialiserDrainLag =
        Meter.CreateHistogram<double>("orleans.lattice.materialiser.drain_lag", unit: "ms",
            description: "Leaf-materialiser drain lag sampled by the WAL saturation sampler, tagged by tree.");

    /// <summary>
    /// Number of individually lagging WAL cursor consumers behind a tree's
    /// materialiser drain-lag, recorded by the WAL saturation sampler for a tree
    /// whose aggregate lag is <b>already over</b>
    /// <see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/> on that
    /// tick. A consumer counts when its own reported cursor trails the WAL head
    /// wall clock by more than that same threshold and its report is fresh under
    /// <see cref="LatticeOptions.WalDrainLagConsumerFreshness"/>; consumers that
    /// have never reported a cursor (<see cref="HybridLogicalClock.Zero"/>) are
    /// excluded, as they are from the lag-plane meet that produces the aggregate.
    /// <para>
    /// <b>Why this exists (issue #2444).</b>
    /// <see cref="MaterialiserDrainLag"/> is a minimum across consumers, so a
    /// single value cannot distinguish <i>one</i> dormant consumer holding the
    /// minimum down from <i>many</i> consumers genuinely falling behind. Those
    /// two conditions have opposite responses, and the aggregate reads
    /// identically for both. This count separates them.
    /// </para>
    /// <para>
    /// <b>What it deliberately does not do.</b> It does not name the contributing
    /// consumer. Consumer identity is unbounded cardinality and is not a safe tag,
    /// so this makes the aggregate <b>triageable, not diagnosable</b>: it tells an
    /// operator which of the two shapes they are in, not which consumer to look
    /// at. Naming the contributor is tracked out of band as issue #2505, where
    /// <see cref="IWalCursorRegistry.SnapshotAsync"/> already supplies the
    /// identity.
    /// </para>
    /// <para>
    /// <b>Cost.</b> Sampled only for trees already found over threshold, so a
    /// healthy estate adds no per-tick work: a tree that never trips never
    /// triggers the snapshot read that backs this instrument.
    /// </para>
    /// Tagged with <see cref="TagTree"/> and the derived tenant label.
    /// </summary>
    public static readonly Histogram<int> MaterialiserLaggingConsumers =
        Meter.CreateHistogram<int>("orleans.lattice.materialiser.lagging_consumers", unit: "{consumer}",
            description: "Count of individually lagging WAL cursor consumers on a tree already over the drain-lag threshold, tagged by tree.");

    /// <summary>
    /// Counter of activation-time leaf materialiser replays started, emitted by
    /// <c>BPlusLeafGrain.OnActivateAsync</c> once a per-silo replay permit
    /// (<see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/>) is
    /// acquired. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagActivationTemperature"/>. A reactivation storm (issue
    /// #1030) shows as a spike in this counter; pairing it with
    /// <see cref="WalReplayPermitQueueWait"/> reveals whether the per-silo
    /// concurrency ceiling is queueing replays under load - queueing is time spent
    /// waiting for a permit, which that instrument measures directly on every
    /// admission, rather than time spent replaying once one is held.
    /// <para>
    /// The temperature tag makes the cold:warm activation ratio a direct read
    /// off a single scrape (issue #2148). It is per-tree because #2104 found
    /// the replay backlog is <b>not</b> uniform across trees, so a global
    /// number would hide known structure - that is an argument for
    /// decomposability, not evidence of any particular per-tree pattern.
    /// </para>
    /// <para>
    /// <b>Scope.</b> The increment sits inside the <c>replayPermit is not null</c>
    /// branch, so an activation of a leaf with no tree id bound takes no replay
    /// permit and is counted on <b>neither</b> arm. That excludes both arms
    /// identically, so it cannot bias the ratio - but it does mean the sum of
    /// the two arms counts permitted replays, not every activation.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafActivationReplays =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation_replays", unit: "{replay}",
            description: "Activation-time leaf materialiser replays started, tagged by tree and by "
                + "activation_temperature (cold = replayed from the -1 sentinel with no snapshot or "
                + "cache anchor; warm = resumed above an anchor).");

    /// <summary>
    /// Counter of activation-time leaf materialiser replays that ran <b>beyond</b>
    /// the configured <see cref="LatticeOptions.MaxLeafReplayEntries"/> budget
    /// while the write-ahead log still covered the whole needed window. Tagged
    /// with <see cref="TagTree"/> and <see cref="TagPartition"/> (the WAL
    /// partition ordinal, bounded by
    /// <see cref="LatticeOptions.WalPartitions"/>).
    /// <para>
    /// Since issue #2149 this counts the leaf's <b>exact post-range-filter</b>
    /// applied-entry count crossing the budget, measured during the replay
    /// itself. It previously counted the classifier's partition-wide WAL gap
    /// crossing it, which is a different quantity in different units: the gap
    /// spans every leaf pinned to the partition, so on a partition carrying
    /// ~1,350 leaves it overstated a single leaf's work by up to that fan-out
    /// and the counter tracked partition depth rather than per-leaf cost. The
    /// counter is now in the same units as the budget it is named after, so a
    /// non-zero rate means leaves really are individually over budget.
    /// </para>
    /// <para>
    /// These replays converge correctly - they are simply longer than the budget
    /// anticipated - so the condition is a capacity signal, not a fault. A tree
    /// that trips this persistently is checkpointing too slowly relative to its
    /// write rate: raise the budget, shorten the materialiser checkpoint cadence
    /// (<see cref="LatticeOptions.MaterialiserCheckpointInterval"/> /
    /// <see cref="LatticeOptions.MaterialiserCheckpointEntries"/>), or accept the
    /// longer activation. Before issue #1738 this condition was fatal and bricked
    /// the tree, so this counter also measures how often that would have fired.
    /// </para>
    /// <para>
    /// The counter is deliberately <b>not</b> tagged by leaf: the leaf count is
    /// unbounded, so it cannot be a time-series dimension. It therefore measures
    /// the rate only, and cannot on its own distinguish many leaves each
    /// replaying once from one leaf replaying forever. That distinction is made
    /// from the accompanying warning log, which names the leaf and its
    /// persisted checkpoint (issue #2023), and from the separate stalled-replay
    /// warning, which is raised when a leaf re-enters replay from an unchanged
    /// checkpoint (issue #2149, fault shape of issue #2165).
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafActivationOverBudgetReplays =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation_replays_over_budget", unit: "{replay}",
            description: "Activation-time leaf replays whose own post-range-filter applied-entry count exceeded the configured replay budget with an intact WAL, tagged by tree and WAL partition.");

    /// <summary>
    /// Tag marking a permit <b>withheld</b> from the replay concurrency gate on
    /// <see cref="WalReplayPermitAdaptations"/>, because the heap cannot afford
    /// the concurrency the gate is configured for. Always accompanied by
    /// <see cref="TagTrigger"/> naming <i>which</i> of the two mechanisms
    /// withheld it (issue #2883).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PermitAdaptationWithheld = new(TagOutcome, "withheld");

    /// <summary>
    /// Tag marking a previously withheld permit <b>restored</b> to the replay
    /// concurrency gate on <see cref="WalReplayPermitAdaptations"/>, because a
    /// replay completed cleanly and occupancy has receded. Carries <b>no</b>
    /// <see cref="TagTrigger"/> - see the instrument's own remarks for why that
    /// asymmetry is correct rather than an oversight.
    /// </summary>
    public static readonly KeyValuePair<string, object?> PermitAdaptationRestored = new(TagOutcome, "restored");

    /// <summary>
    /// <see cref="TagTrigger"/> = <c>fault</c>: the permit was withheld because the
    /// replay <b>escaped its guarded region</b> with an exception carrying a memory
    /// verdict (the reactive trigger, issue #2781).
    /// </summary>
    /// <remarks>
    /// This is the arm that read zero through acceptance run 10's 625
    /// <see cref="OutOfMemoryException"/>s, because the slice-narrowing retry of
    /// issue #2742 absorbs the very fault it watches for. It is retained as a
    /// backstop rather than replaced: a fault that does escape is still evidence,
    /// and the two triggers fail independently.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> PermitAdaptationTriggerFault = new(TagTrigger, "fault");

    /// <summary>
    /// <see cref="TagTrigger"/> = <c>occupancy</c>: the permit was withheld because
    /// managed heap occupancy had reached the withholding band when the replay
    /// returned it, whether or not the replay faulted (the proactive trigger,
    /// issue #2862).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PermitAdaptationTriggerOccupancy = new(TagTrigger, "occupancy");

    /// <summary>
    /// Counter of adaptations to the per-silo WAL replay concurrency gate, tagged
    /// with <see cref="TagOutcome"/> (<see cref="PermitAdaptationWithheld"/> when a
    /// permit was withheld from circulation, <see cref="PermitAdaptationRestored"/>
    /// when one was returned after a replay completed cleanly). Issues #2781 and
    /// #2883.
    /// <para>
    /// <b>The withheld arm carries <see cref="TagTrigger"/>, and that dimension is
    /// the whole of issue #2883.</b> Two independent mechanisms withhold:
    /// <see cref="PermitAdaptationTriggerFault"/> (a replay escaped its guarded
    /// region with a memory verdict, #2781) and
    /// <see cref="PermitAdaptationTriggerOccupancy"/> (occupancy had reached the
    /// withholding band when the permit came back, #2862). Before the tag existed
    /// both wrote the same untagged series, so <c>withheld = N</c> was a
    /// <i>sum</i> that no scrape could attribute - and that ambiguity already
    /// produced a wrong published conclusion, when run 12's <c>withheld = 6</c>
    /// was read as evidence that the fault trigger works. It is equally
    /// consistent with the fault trigger firing zero times.
    /// </para>
    /// <para>
    /// <b>Continuity is preserved:</b> summing over <see cref="TagTrigger"/>
    /// recovers the historical untagged total, so every comparison against runs
    /// that predate the tag stays valid. Adding a dimension is the non-destructive
    /// fix; minting a second counter would not have been.
    /// </para>
    /// <para>
    /// <b>The restored arm is deliberately untagged, and a per-trigger level is
    /// therefore NOT derivable.</b> Withheld permits are <i>fungible</i> - the
    /// accounting is a single process-wide count, not a per-trigger ledger - so a
    /// restore cannot know which trigger withheld the permit it is handing back,
    /// and tagging it would only manufacture a number that looks attributable and
    /// is not. Consequently
    /// <c>withheld{trigger="fault"} - restored</c> is <b>meaningless</b>: the only
    /// valid level is the total <c>withheld - restored</c>, summed over triggers.
    /// Read the trigger split as <i>which mechanism is doing the work</i>, never
    /// as <i>how much each mechanism is currently holding</i>.
    /// </para>
    /// <para>
    /// <b>Deliberately not tagged by tree.</b> The gate is process-wide, so a
    /// per-tree tag would imply a per-tree ceiling that does not exist and would
    /// invite a reader to sum arms that share one underlying resource.
    /// </para>
    /// <para>
    /// The difference <c>withheld - restored</c> is the number of permits currently
    /// withheld, so the effective ceiling is
    /// <c>configured - (withheld - restored)</c>. <b>All three arms</b> - withheld
    /// under each trigger, and restored - are <b>zero-primed</b> when the gate is
    /// sized, which is the one site that proves the gate was actually created:
    /// without priming, "this trigger never engaged" and "this build does not have
    /// that trigger" would both read as an absent series.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalReplayPermitAdaptations =
        Meter.CreateCounter<long>("orleans.lattice.wal.replay.permit_adaptations", unit: "{permit}",
            description: "Adaptations to the per-silo WAL replay concurrency gate under memory pressure, tagged by outcome, and on the withheld arm by the trigger (fault or occupancy) that withheld. Zero-primed on all three arms when the gate is sized. The restored arm carries no trigger because withheld permits are fungible, so a per-trigger level is not derivable.");

    /// <summary>
    /// The name of the observable gauge reporting the number of replay permits
    /// <b>currently</b> withheld from the per-silo WAL replay concurrency gate
    /// (issue #2784). The counterpart level to
    /// <see cref="WalReplayPermitAdaptations"/>, which is the history.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The instrument itself is <b>not</b> declared here. It is built in
    /// <c>BPlusLeafGrain.Activation.cs</c>, beside the process-wide static count
    /// its callback reads, because an observable gauge must be declared below
    /// every piece of static state that callback touches and that state does not
    /// live on this class.
    /// </para>
    /// <para>
    /// The name is nevertheless exported <b>here</b>, as a constant, and that is a
    /// requirement rather than a convenience. The dashboard drift guard resolves a
    /// panel's metric token against two populations: instruments a snapshot
    /// <c>MeterListener</c> can see, and the <c>public const string ...Name</c>
    /// fields on this class. An instrument whose factory runs only when its
    /// declaring type is first touched is in <b>neither</b> population, so a panel
    /// referencing it fails the guard as an unknown token even though the
    /// instrument is perfectly correct. Exporting the name is what puts it in the
    /// second population. The same convention already carries
    /// <see cref="CoordinatorPhaseTickConsecutiveFailuresGaugeName"/> and
    /// <see cref="LeafResidencyBudgetBytesName"/>, whose instruments are likewise
    /// declared outside this class.
    /// </para>
    /// </remarks>
    public const string WalReplayPermitsWithheldName = "orleans.lattice.wal.replay.permits_withheld";

    /// <summary>
    /// Metric name for the gauge publishing the <b>ceiling</b> the per-silo WAL
    /// replay concurrency gate was sized to, or <c>0</c> before any activation
    /// has sized it (issue #3047).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the <b>denominator</b> for every other permit series. The withheld
    /// level and the adaptation counters are absolute counts, and an absolute
    /// count cannot be judged without the total it is drawn from: one permit
    /// withheld from a ceiling of sixteen is noise, and one withheld from a
    /// ceiling of two is half the silo's replay throughput. Those two readings
    /// are numerically identical on a scrape and operationally opposite.
    /// </para>
    /// <para>
    /// Like <see cref="WalReplayPermitsWithheldName"/>, the instrument is
    /// declared in <c>BPlusLeafGrain.Activation.cs</c> beside the static it
    /// reads, and the name is exported here so the dashboard drift guard can
    /// resolve a panel token against it. See the remarks on that field for why
    /// exporting the name is a requirement rather than a convenience.
    /// </para>
    /// </remarks>
    public const string WalReplayPermitCeilingName = "orleans.lattice.wal.replay.permit_ceiling";

    /// <summary>
    /// Metric name for the gauge publishing the permits currently
    /// <b>available</b> on the per-silo WAL replay concurrency gate (issue
    /// #3047).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Never read alone.</b> Zero on this series is ambiguous between an
    /// unsized gate and a fully saturated one, and only
    /// <see cref="WalReplayPermitCeilingName"/> separates them: a ceiling of
    /// <c>0</c> means the gate does not exist yet and this figure is meaningless,
    /// while a ceiling of one or more makes a zero here a measured saturation.
    /// Saturation is the reading the instrument exists for, which is why the pair
    /// is documented as a pair.
    /// </para>
    /// <para>
    /// It sizes <b>headroom, not backlog</b>. The underlying count cannot fall
    /// below zero, so it says how much room is left and nothing about how many
    /// activations are waiting once there is none;
    /// <see cref="WalReplayPermitsQueuedName"/> is the instrument for that.
    /// </para>
    /// <para>
    /// Declared outside this class and exported here for the drift guard, as
    /// above.
    /// </para>
    /// </remarks>
    public const string WalReplayPermitsAvailableName = "orleans.lattice.wal.replay.permits_available";

    /// <summary>
    /// Metric name for the gauge publishing the activations currently
    /// <b>queued</b> on the per-silo WAL replay concurrency gate (issue #3047).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The only non-terminal instrument on the admission path.</b>
    /// <see cref="WalReplayPermitQueueWait"/> records once a wait has ended and
    /// <see cref="LeafActivationReplays"/> records once a permit is held, so both
    /// sit downstream of the wait and an activation that is <em>still queued</em>
    /// appears on neither. A permanently saturated gate is therefore silent
    /// across the whole surface, and renders identically to a gate nothing ever
    /// asked for a permit. That is a property of measuring terminated events
    /// only, and the sole repair is to measure the population that has not
    /// terminated.
    /// </para>
    /// <para>
    /// Not derivable from <see cref="WalReplayPermitsAvailableName"/>, which
    /// saturates at zero and so reports the same figure for one waiter and for a
    /// thousand. Read the three together: the ceiling says how wide the door is,
    /// availability says whether it is open, and this says how many are waiting
    /// at it.
    /// </para>
    /// <para>
    /// Declared outside this class and exported here for the drift guard, as
    /// above.
    /// </para>
    /// </remarks>
    public const string WalReplayPermitsQueuedName = "orleans.lattice.wal.replay.permits_queued";

    /// <summary>
    /// Tag marking a replay permit queue wait that ended in the permit being
    /// <b>acquired</b>, on <see cref="WalReplayPermitQueueWait"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> PermitQueueWaitAcquired = new(TagOutcome, "acquired");

    /// <summary>
    /// Tag marking a replay permit queue wait that ended in the activation being
    /// <b>canceled</b> while still queued, on
    /// <see cref="WalReplayPermitQueueWait"/>. This is the same population that
    /// increments <see cref="LeafActivationFailures"/> with
    /// <see cref="ActivationFailureCanceledAwaitingPermit"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> PermitQueueWaitCanceled = new(TagOutcome, "canceled");

    /// <summary>
    /// Explicit bucket boundaries for <see cref="WalReplayPermitQueueWait"/>
    /// (issue #3044), in milliseconds.
    /// <para>
    /// Without them the histogram is exported with no bucket series at all, so
    /// the only readable statistic is a mean - and a mean over a handful of
    /// samples cannot distinguish a gate that is uniformly slow from one that is
    /// fast except for a few pathological holds. Those two have different causes
    /// and different remedies, so collapsing them loses the distinction the
    /// instrument exists to draw.
    /// </para>
    /// <para>
    /// The boundaries span sub-millisecond to half an hour because the observed
    /// range genuinely does: a warm resume on an idle gate returns in under a
    /// millisecond, while a real queue wait has been measured in minutes. Two
    /// boundaries are chosen rather than merely spaced - <c>30000</c> is the
    /// Orleans response deadline, so the bucket above it isolates waits that
    /// outlived the caller that was waiting on them, and <c>1000</c> separates
    /// "queued behind someone" from "queued behind a replay".
    /// </para>
    /// <para>
    /// <b>Declared above the histogram that consumes it, and that ordering is
    /// load-bearing.</b> Static field initialisers run in declaration order, and
    /// the <c>advice</c> parameter is nullable, so an advice field declared
    /// below its histogram is read as <c>null</c> and the histogram is built
    /// with no buckets at all. Nothing throws and every test that does not
    /// inspect bucket boundaries still passes - the failure is silent and
    /// presents as the exact bucketless export this field exists to fix. This is
    /// the same ordering hazard the <c>Meter</c>-above-instruments rule guards,
    /// arriving through a different field.
    /// </para>
    /// </summary>
    private static readonly InstrumentAdvice<double> WalReplayPermitQueueWaitAdvice = new()
    {
        HistogramBucketBoundaries =
        [
            1d, 5d, 10d, 50d, 100d, 500d, 1_000d, 5_000d,
            15_000d, 30_000d, 60_000d, 300_000d, 900_000d, 1_800_000d,
        ],
    };

    /// <summary>
    /// Wall-clock ms an activation spent queued on the per-silo WAL replay
    /// concurrency gate, tagged with <see cref="TagTree"/>,
    /// <see cref="TagOutcome"/> (<see cref="PermitQueueWaitAcquired"/> or
    /// <see cref="PermitQueueWaitCanceled"/>), and the derived tenant dimension.
    /// Issue #2873.
    /// <para>
    /// <b>This is the discriminator for
    /// <see cref="ActivationFailureCanceledAwaitingPermit"/>, which cannot
    /// discriminate on its own.</b> That reason tag is assigned from the
    /// admission phase, so it is honest about <em>where</em> an activation was
    /// canceled and silent about <em>why</em>. The Orleans request deadline spans
    /// the whole grain call, so an activation that burned most of its budget
    /// upstream - in the snapshot rehydrate, in options resolution, or on the
    /// shared storage file - arrives at the gate already doomed and is canceled
    /// there within seconds. The count is therefore identical whether the gate
    /// was saturated for the full budget or idle the entire time. The duration is
    /// what separates them:
    /// </para>
    /// <list type="bullet">
    ///   <item>a <c>canceled</c> distribution sitting near the request budget is
    ///   <b>real permit starvation</b> - the activation genuinely waited;</item>
    ///   <item>a <c>canceled</c> distribution of a fraction of a second is the
    ///   gate being <b>blamed for an upstream cost</b> - it was already out of
    ///   budget when it arrived.</item>
    /// </list>
    /// <para>
    /// <b>Tagged by tree, deliberately, unlike its sibling
    /// <see cref="WalReplayPermitAdaptations"/>.</b> The gate's <em>ceiling</em>
    /// is a process-wide property and a per-tree tag on it would imply a per-tree
    /// ceiling that does not exist. A <em>wait</em> is the opposite: it is one
    /// activation's own experience, and it is attributable to the tree whose leaf
    /// was activating. The tag is what lets a reading be scoped to a single tree
    /// rather than taken corpus-wide, which is not a convenience - a corpus-wide
    /// aggregate read as if it were tree-scoped is exactly how an acceptance run
    /// scored a pass whose true tree-scoped value was a fail.
    /// </para>
    /// <para>
    /// <b>Deliberately NOT zero-primed, and an absent series is therefore
    /// UNINTERPRETABLE rather than a measured zero.</b> Priming a histogram means
    /// recording a fabricated <c>0 ms</c> sample, which would bias the very
    /// distribution the instrument exists to read - and bias it toward
    /// "the gate is innocent", one of the two conclusions it is meant to
    /// discriminate between. Under a low-traffic tree a single synthetic sample
    /// can move the median outright. The discrimination that priming would have
    /// bought is instead available by corroboration: the <c>acquired</c> arm
    /// records on <b>every</b> admission, so if any replay was admitted this
    /// series exists, and whether any replay was admitted is independently
    /// visible in the zero-primed <see cref="WalReplayPermitAdaptations"/> and in
    /// the activation counters. Read an absent series as a prompt to diagnose,
    /// never as evidence that no activation waited.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalReplayPermitQueueWait =
        Meter.CreateHistogram<double>("orleans.lattice.wal.replay.permit_queue_wait", unit: "ms",
            description: "Wall-clock ms an activation spent queued on the per-silo WAL replay concurrency gate, tagged by tree and by outcome (acquired or canceled).",
            tags: null,
            advice: WalReplayPermitQueueWaitAdvice);

    /// <summary>
    /// Count of WAL GC starvation drives abandoned at
    /// <see cref="LatticeOptions.StarvationDriveBudget"/> with their replay
    /// permit forcibly released, tagged by tree (issue #3065).
    /// <para>
    /// <b>This is the sole discriminator for the fault it reports, and that is a
    /// consequence of the fix rather than a design preference.</b> Before the
    /// budget existed, a drive parked in host-supplied storage held its permit
    /// for the life of the process, and the signature a reader used to recognise
    /// it was <c>attempted</c> climbing while <c>undelivered</c> and
    /// <c>drove_already_driving</c> climbed with it. A bounded drive reproduces
    /// that signature exactly: the scheduler's touch abandons at the Orleans
    /// response deadline, which is far below this budget, so every bounded drive
    /// that outlives its caller emits one <c>undelivered</c> per cadence tick and
    /// answers <c>AlreadyDriving</c> to each retry until it terminates. Those
    /// arms therefore no longer separate "slow and recovering" from "wedged", and
    /// this counter is what does.
    /// </para>
    /// <para>
    /// <b>Zero-primed alongside <c>attempted</c> in the scheduler's
    /// <c>PrimeRetentionSeries</c>, deliberately and not at the drive itself.</b>
    /// Priming at the drive would mint the series only once a drive had been
    /// entered, so an absent series would be equally consistent with "no drive
    /// has ever run here" and with "this build is not deployed" - and that second
    /// reading is the ambiguity this epic has lost the most time to, because a
    /// counter present in source and absent from the running container makes
    /// every downstream reading uninterpretable. Co-priming with an instrument
    /// that is known to fire converts absence into a positive statement:
    /// <c>attempted &gt; 0</c> with this series present and flat means measured
    /// and never abandoned, while <c>attempted &gt; 0</c> with this series absent
    /// means the build carrying the budget is not running here. Do not "tidy"
    /// the priming back to the drive entry point; the co-presence is the point.
    /// </para>
    /// <para>
    /// Tagged <c>tree</c> plus the universal derived <c>tenant</c> dimension,
    /// which the leaf grain resolves from the same tree id the scheduler does, so
    /// the zero the scheduler primes and the one the grain records share a single
    /// series identity. It is a separate instrument rather than a new arm
    /// on <see cref="WalGcBlockedLeafReactivations"/> because the verdict arms
    /// there partition <c>attempted</c> by construction, and a grain-side arm
    /// recorded on a different schedule would break that sum.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalReplayStarvationDriveAbandonments =
        Meter.CreateCounter<long>("orleans.lattice.wal.replay.starvation_drive_abandonments",
            description: "Count of WAL GC starvation drives abandoned at their configured budget with the replay permit forcibly released, tagged by tree. Zero-primed alongside the blocked-leaf 'attempted' arm, so an absent series means this build is not deployed rather than that no drive has been abandoned.");

    // ---- Issue #3044: permit waits that never terminate --------------------
    //
    // WalReplayPermitQueueWait records on exactly two arms and BOTH are
    // terminal: `acquired` after the semaphore is entered, `canceled` from the
    // catch around the wait. A wait that never returns records on neither, so
    // the instrument built to measure gate contention is structurally silent
    // about the single state that matters most - an activation parked on the
    // gate indefinitely. That state is not an edge case; it is what a saturated
    // gate looks like from the inside, and it is invisible precisely when the
    // gate is worst.
    //
    // No amount of priming reaches it. A primed `canceled` arm reading zero says
    // "no cancellation completed", which is true and irrelevant while a wait is
    // still in flight. The missing observable is a level, not a terminal event,
    // so it needs a gauge.
    //
    // These two name that state as a measurement rather than a threshold, in the
    // shape issue #2967 established for wedged split completions: the count of
    // waits currently parked, and the age of the oldest. No constant is encoded
    // here - "stuck" is read off the age climbing, a judgement made against real
    // data rather than guessed in code.

    /// <summary>
    /// Live registry of activations currently queued on the per-silo WAL replay
    /// concurrency gate, keyed by a process-unique token and carrying the tree
    /// whose leaf is activating and the monotonic timestamp the wait began.
    /// Declared above the gauges that read it so their observation callbacks can
    /// never see it uninitialised.
    /// </summary>
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<long, (string Tree, long StartTimestamp)>
        WalReplayPermitWaitsInFlightRegistry = new();

    /// <summary>
    /// Trees that have entered the permit queue at least once in this process,
    /// so <see cref="WalReplayPermitWaitsInFlight"/> can keep reporting an
    /// explicit zero for them once their waits drain.
    /// <para>
    /// This is what makes a zero on that gauge <b>admissible</b>. Without it an
    /// idle tree has no series, and "no activation is queued" is byte-identical
    /// to "the instrument never ran" - the exact ambiguity that made the
    /// terminal arms unusable as evidence. Bounded by the number of trees the
    /// process has ever activated a leaf for, which is small and does not grow
    /// with traffic. Declared above the gauges that read it.
    /// </para>
    /// </summary>
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte>
        WalReplayPermitWaitObservedTrees = new(StringComparer.Ordinal);

    private static long _walReplayPermitWaitToken;

    /// <summary>
    /// Registers an activation as queued on the replay permit gate for the
    /// duration of the returned scope. Disposing it deregisters the wait; a
    /// scope that is never disposed - an activation suspended forever on the
    /// gate - keeps its entry live, which is precisely the state
    /// <see cref="WalReplayPermitWaitsInFlight"/> and
    /// <see cref="WalReplayPermitWaitOldestAge"/> exist to make observable.
    /// </summary>
    /// <param name="treeId">The tree whose leaf is activating.</param>
    public static WalReplayPermitWaitScope EnterWalReplayPermitWait(string? treeId)
    {
        var tree = treeId ?? string.Empty;
        WalReplayPermitWaitObservedTrees.TryAdd(tree, 0);
        var token = System.Threading.Interlocked.Increment(ref _walReplayPermitWaitToken);
        WalReplayPermitWaitsInFlightRegistry[token] = (tree, System.Diagnostics.Stopwatch.GetTimestamp());
        return new WalReplayPermitWaitScope(token);
    }

    /// <summary>
    /// Disposable scope returned by <see cref="EnterWalReplayPermitWait"/>.
    /// Removes its registry entry on <see cref="Dispose"/>. A value type so the
    /// mass-reactivation path takes no per-wait heap allocation.
    /// </summary>
    public readonly struct WalReplayPermitWaitScope : IDisposable
    {
        private readonly long _token;

        internal WalReplayPermitWaitScope(long token) => _token = token;

        /// <summary>Deregisters the in-flight permit wait this scope tracks.</summary>
        public void Dispose() => WalReplayPermitWaitsInFlightRegistry.TryRemove(_token, out _);
    }

    /// <summary>
    /// Per-tree count of activations currently queued on the per-silo WAL replay
    /// concurrency gate (issue #3044). Reports an explicit <c>0</c> for any tree
    /// that has queued at least once in this process, so a zero is a measured
    /// zero rather than an absent series.
    /// </summary>
    public static readonly ObservableGauge<long> WalReplayPermitWaitsInFlight =
        Meter.CreateObservableGauge("orleans.lattice.wal.replay.permit_waits_in_flight",
            ObserveWalReplayPermitWaitsInFlight, unit: "{activation}",
            description: "Activations currently queued on the per-silo WAL replay concurrency gate, tagged by tree. Reports an explicit zero for any tree that has queued at least once in this process, so a zero is measured rather than absent. Read with permit_wait.oldest_age: the terminal arms of permit_queue_wait cannot name a wait that is still in flight.");

    /// <summary>Canonical name of <see cref="WalReplayPermitWaitsInFlight"/>.</summary>
    public const string WalReplayPermitWaitsInFlightName = "orleans.lattice.wal.replay.permit_waits_in_flight";

    /// <summary>
    /// Per-tree age in seconds of the oldest activation currently queued on the
    /// replay permit gate (issue #3044), or no series for a tree with none
    /// queued.
    /// <para>
    /// Deliberately not zero-primed, unlike its sibling count: the age of the
    /// oldest waiter when there is no waiter is not zero, it is undefined, and
    /// fabricating a zero would report the healthiest possible value for the
    /// emptiest possible state. The count is the instrument that answers
    /// "is anyone queued"; this one answers "for how long" and is meaningful
    /// only when the answer to the first is yes.
    /// </para>
    /// </summary>
    public static readonly ObservableGauge<double> WalReplayPermitWaitOldestAge =
        Meter.CreateObservableGauge("orleans.lattice.wal.replay.permit_wait.oldest_age",
            ObserveWalReplayPermitWaitOldestAge, unit: "s",
            description: "Age in seconds of the oldest activation currently queued on the per-silo WAL replay concurrency gate, tagged by tree (no series when none is queued). A climbing value is an activation parked on a saturated gate; a value near zero is healthy contention.");

    /// <summary>Canonical name of <see cref="WalReplayPermitWaitOldestAge"/>.</summary>
    public const string WalReplayPermitWaitOldestAgeName = "orleans.lattice.wal.replay.permit_wait.oldest_age";

    private static IEnumerable<Measurement<long>> ObserveWalReplayPermitWaitsInFlight()
    {
        var counts = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var tree in WalReplayPermitWaitObservedTrees.Keys)
        {
            counts[tree] = 0;
        }

        foreach (var entry in WalReplayPermitWaitsInFlightRegistry)
        {
            counts.TryGetValue(entry.Value.Tree, out var current);
            counts[entry.Value.Tree] = current + 1;
        }

        foreach (var kv in counts)
        {
            yield return new Measurement<long>(
                kv.Value,
                new KeyValuePair<string, object?>(TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    private static IEnumerable<Measurement<double>> ObserveWalReplayPermitWaitOldestAge()
    {
        var now = System.Diagnostics.Stopwatch.GetTimestamp();
        var oldestStart = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var entry in WalReplayPermitWaitsInFlightRegistry)
        {
            if (!oldestStart.TryGetValue(entry.Value.Tree, out var start) || entry.Value.StartTimestamp < start)
            {
                oldestStart[entry.Value.Tree] = entry.Value.StartTimestamp;
            }
        }

        foreach (var kv in oldestStart)
        {
            var seconds = (now - kv.Value) / (double)System.Diagnostics.Stopwatch.Frequency;
            if (seconds < 0)
            {
                seconds = 0;
            }

            yield return new Measurement<double>(
                seconds,
                new KeyValuePair<string, object?>(TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    /// <summary>
    /// Counter of activation-time replay slice narrowings: the number of times a
    /// leaf replay could not afford a commit-log read at its current slice width
    /// and retried the same range at a quarter of it (issue #2867). Tagged with
    /// <see cref="TagTree"/>, <see cref="TagPartition"/>, and the derived tenant
    /// dimension.
    /// <para>
    /// <b>This is the second of the two factors that set peak replay memory, and
    /// it is the one an operator cannot configure.</b> Peak memory is the product
    /// of how many replays run at once and how much each one buffers. The first
    /// factor is <see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/>,
    /// which is an option, is surfaced on the container's tuning overlay, and is
    /// already measured from both sides by
    /// <see cref="WalReplayPermitAdaptations"/> and
    /// <see cref="WalReplayPermitQueueWait"/>. The second is the per-replay slice
    /// width, which is a private constant with no option behind it, so the only
    /// thing that ever moves it is this reactive narrowing. Without this counter
    /// that entire factor is invisible: the narrowing is reported by a warning
    /// log alone, and a log is not a series.
    /// </para>
    /// <para>
    /// <b>What it discriminates.</b> A managed <c>OutOfMemoryException</c> during
    /// a mass reactivation is consistent with two different stories, and the
    /// remedies point in opposite directions. Either the narrowing engaged and
    /// was not enough - in which case the width is genuinely too coarse for the
    /// host and the fix is to lower it - or the narrowing never engaged at all,
    /// because the allocation that failed was not the slice read this clause
    /// guards, in which case lowering the width would change nothing and the
    /// cost lies elsewhere. The count separates those two readings; nothing else
    /// in the surface does. Read it against
    /// <c>orleans.lattice.leaf.activation.failures</c> on the same tree: failures
    /// climbing while this stays at zero is the second story.
    /// </para>
    /// <para>
    /// <b>Tagged by tree, deliberately.</b> The width is a per-replay property
    /// and a replay belongs to the tree whose leaf is activating, so unlike the
    /// process-wide gate ceiling this genuinely has a per-tree value. That tag is
    /// what allows one tree to be shown buffering far harder than its siblings
    /// under identical cycling, which is the observation issue #2867 exists to
    /// make readable.
    /// </para>
    /// <para>
    /// <b>Zero-primed per (tree, partition)</b> when a partition replay begins,
    /// alongside <see cref="LeafDeferredTerminalsDroppedAtCap"/>. A counter
    /// exports no series until its first <c>Add</c>, so without priming "this
    /// replay never had to narrow" and "this build has no narrowing" would both
    /// read as an absent series - and the first of those is the healthy steady
    /// state, so the ambiguity would cover the common case. Priming at the same
    /// (tree, partition) tag set a later narrowing carries cannot perturb the
    /// value.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalReplaySliceNarrowings =
        Meter.CreateCounter<long>("orleans.lattice.wal.replay.slice_narrowings", unit: "{narrowing}",
            description: "Activation-time replay slice-width narrowings forced by memory pressure on a commit-log read, tagged by tree and WAL partition. Zero-primed per partition replay.");

    /// <summary>
    /// Counter of activation-time leaf replays that re-entered from a persisted
    /// checkpoint which had <b>not advanced</b> since the same leaf partition's
    /// previous replay on this silo, emitted by <c>BPlusLeafGrain</c>'s
    /// stalled-replay check (issue #2285). A non-zero value means the previous
    /// activation banked no durable forward progress at all for that leaf
    /// partition.
    /// <para>
    /// This is the <b>fault</b> arm, and it is a different condition from
    /// <see cref="LeafActivationOverBudgetReplays"/>, which is a capacity
    /// signal. An over-budget replay converges, just slowly; a non-advancing
    /// replay has not converged at all. Before this counter existed the fault
    /// arm was reported <b>only</b> by a warning log, which is throttled to one
    /// line per (tree, leaf, partition) per minute, so the observable rate was
    /// the throttle's rate and not the condition's. This counter records every
    /// occurrence and is the exact census; the log is a bounded sample of it.
    /// </para>
    /// <para>
    /// <b>Read it as a rate over time, not as a level.</b> The condition is
    /// transient whenever an activation is torn down mid-replay - a burst of
    /// cancellations or timeouts produces a cluster of these and then stops -
    /// and it is persistent only when the same leaf partition keeps reporting
    /// across many minutes. The two are indistinguishable in a single sample
    /// and were conflated in issue #2285, where a 70-second burst of 47
    /// occurrences was read as a permanent convergence defect. Alert on the
    /// condition <i>continuing</i>, not on its appearance.
    /// </para>
    /// <para>
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// Deliberately <b>not</b> tagged by leaf: leaf count is unbounded, so it
    /// cannot be a time-series dimension. The accompanying warning names the
    /// leaf.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafActivationStalledReplays =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation_stalled_replays", unit: "{replay}",
            description: "Activation-time leaf replays that re-entered from a persisted checkpoint that had not advanced since the previous replay, tagged by tree and WAL partition.");

    /// <summary>
    /// Counter of activation-time eager cursor-publish failures, emitted by
    /// <c>BPlusLeafGrain.OnActivateAsync</c> when the post-replay cursor report
    /// throws (a non-fatal failure the next foreground flush recovers from).
    /// Under a reactivation storm against a saturated silo (issue #1030) these
    /// failures fan out across every reactivating leaf; the warning log is
    /// rate-limited per silo to avoid a self-amplifying log flood, but this
    /// counter records every occurrence so the true rate stays observable.
    /// Tagged with <see cref="TagTree"/>.
    /// </summary>
    public static readonly Counter<long> LeafActivationCursorPublishFailures =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation_cursor_publish_failures", unit: "{failure}",
            description: "Activation-time eager cursor-publish failures, tagged by tree.");

    /// <summary>
    /// Histogram of the projection-checkpoint offsets a leaf activation banked
    /// during its graceful deactivation - the sum across WAL partitions of
    /// (checkpoint on leaving the hook - checkpoint on entering it), recorded
    /// exactly once per <c>BPlusLeafGrain.OnDeactivateAsync</c> call. Tagged
    /// with <see cref="TagTree"/>, <see cref="TagDeactivationReason"/> and
    /// <see cref="TagActivationTemperature"/>. Never tagged by leaf: the leaf
    /// population is unbounded, so per-leaf detail goes to the accompanying log
    /// line instead (issue #2280).
    /// <para>
    /// <b>THIS IS A LOWER BOUND, NOT A CENSUS. Do not quote it as one.</b>
    /// Three populations are structurally invisible to it, and each absence is
    /// indistinguishable from a healthy zero:
    /// </para>
    /// <list type="number">
    ///   <item>Crash, process kill and silo failure bypass the deactivation
    ///   hook by design, so a leaf lost that way is never counted.</item>
    ///   <item>An activation that FAILED never reaches this hook at all.
    ///   Orleans does not run <c>OnDeactivateAsync</c> when
    ///   <c>OnActivateAsync</c> throws - measured, with a positive control, on
    ///   Orleans 10.2.2 - and a cancelled cold replay throws
    ///   <see cref="OperationCanceledException"/> out of activation by design
    ///   ("failures propagate"). That population is counted by
    ///   <see cref="LeafActivationFailures"/> instead, and reading either
    ///   series alone understates the whole.</item>
    ///   <item>A deactivation whose flush throws is swallowed by the hook's
    ///   catch, so its exit checkpoint reflects whatever was banked before the
    ///   failure rather than a completed flush.</item>
    /// </list>
    /// <para>
    /// A ZERO observation on the <c>cold</c> arm is EXPECTED and is not by
    /// itself a fault (issue #2280). A cold replay restarts from the -1
    /// sentinel and
    /// <c>ILeafProjection.SetCheckpointOffsetAsync</c>
    /// enforces strict monotonicity, so a cold activation cannot bank anything
    /// at all until its scanned-through offset passes the checkpoint it started
    /// above. Progress below that mark is not discarded, it is UNREPRESENTABLE.
    /// Read a zero as "did not pass the existing mark", never as "did no work".
    /// </para>
    /// <para>
    /// The offsets counted here are SCANNED-THROUGH, not applied-through
    /// (issue #2270): replay advances the checkpoint over entries it skips as
    /// another leaf's work, deliberately, because the WAL retention floor is
    /// the MINIMUM of these offsets and a leaf that owns no key in a partition
    /// would otherwise pin truncation for the whole tree. A non-zero delta here
    /// is therefore durable forward progress through the log, which is the
    /// quantity issue #2280 is about, and NOT a count of mutations this leaf
    /// applied. For that, read the <c>entriesApplied</c> field on the
    /// accompanying log line, which is taken at the
    /// <c>ILeafProjection.Apply</c> seam.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> LeafDeactivationCheckpointDelta =
        Meter.CreateHistogram<long>("orleans.lattice.leaf.deactivation.checkpoint_delta", unit: "{offset}",
            description: "Projection-checkpoint offsets banked by a leaf activation during graceful deactivation, tagged by tree, deactivation reason and activation temperature. LOWER BOUND: crash teardowns and failed activations never reach the hook.");

    /// <summary>
    /// Counter of leaf activations that ended by THROWING out of
    /// <c>BPlusLeafGrain.OnActivateAsync</c> rather than coming online. Tagged
    /// with <see cref="TagTree"/>, <see cref="TagActivationTemperature"/> and
    /// <see cref="TagReason"/>: <c>canceled</c> when a replay in progress was
    /// cancelled, <c>canceled_awaiting_permit</c> when the cancellation arrived
    /// while the activation was still queued for the replay permit and no
    /// replay had begun, and <c>faulted</c> for any other failure.
    /// <para>
    /// This exists because <see cref="LeafDeactivationCheckpointDelta"/> is
    /// structurally blind to it (issue #2280). A failed activation never runs
    /// the deactivation hook, so without this counter a cancelled cold replay
    /// would be reported as zero at EVERY rate of occurrence, including the
    /// highest - an absence indistinguishable from health. The two series are
    /// complementary and must be read together: this one counts activations
    /// that never completed, the other measures what completed activations
    /// banked on the way out.
    /// </para>
    /// <para>
    /// <b>Still a lower bound, though a much tighter one.</b> The permit
    /// acquisition was moved inside the guarded region so the queue-wait window
    /// is covered, which matters because that window's width is set by the very
    /// replay saturation under investigation. What remains uncounted is any
    /// failure raised before the guarded region is entered at all - the
    /// snapshot rehydrate and the coherence reset that precede it - and a
    /// process killed outright, which reaches no observation site anywhere.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafActivationFailures =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation.failures", unit: "{activation}",
            description: "Leaf activations that threw out of OnActivateAsync, tagged by tree, activation temperature and reason (canceled/canceled_awaiting_permit/faulted). LOWER BOUND: faults raised before the guarded replay region, and outright process kills, are not counted.");

    /// <summary>
    /// Counter of leaf activations cancelled while COLD that carried the leaf's
    /// run of consecutive cold cancellations to or past the escalation
    /// threshold - the self-reinforcing cold replay loop of issue #2280 caught
    /// in the act. Tagged with <see cref="TagTree"/> only (plus the tenant label
    /// derived from it).
    /// <para>
    /// <b>Why this is not derivable from <see cref="LeafActivationFailures"/>.</b>
    /// That counter is an aggregate over the tree, as is the distinct-cold-leaf
    /// population carried on the activation-temperature sample line. Neither can
    /// express <em>this same leaf again, with no successful activation in
    /// between</em>, which is the whole pathology: a leaf whose cancellation
    /// reproduces exactly the condition that caused it. Summing cancellations
    /// cannot recover that, because the sum cannot tell one leaf cancelled five
    /// times from five leaves cancelled once - and those are a defect and a
    /// cost respectively.
    /// </para>
    /// <para>
    /// <b>Not tagged by leaf, deliberately</b>, for the same reason
    /// <see cref="LeafDeactivationCheckpointDelta"/> is not: the leaf population
    /// is unbounded and would be an unbounded metric dimension. The leaf
    /// identity is carried on the accompanying warning instead, which is where
    /// an operator needs it anyway - this counter answers "is it happening and
    /// how often", the log line answers "to which leaf".
    /// </para>
    /// <para>
    /// It counts EVERY cancellation at or above the threshold, not just the
    /// crossing, so a leaf that stays stuck keeps registering and the series
    /// carries a rate rather than a one-shot edge. The paired warning is
    /// throttled per leaf; this is not, because a counter has no flood to
    /// prevent.
    /// </para>
    /// <para>
    /// A zero here is a genuine and expected reading: it is the healthy state,
    /// and the threshold is calibrated so that the measured field distribution
    /// (55 leaves cancelled once, 12 twice, none more) produces no observations
    /// at all. That is the point - a diagnostic that fired on that distribution
    /// would be muted, and would take the real signal with it.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafColdReplayLoop =
        Meter.CreateCounter<long>("orleans.lattice.leaf.activation.cold_replay_loop", unit: "{activation}",
            description: "Cold leaf activations cancelled at or past the consecutive-cancellation escalation threshold, tagged by tree: the self-reinforcing cold WAL replay loop (issue #2280). Per-leaf identity is on the paired warning, never a tag. Zero is the healthy reading.");

    /// <summary>
    /// Counter of activation-time leaf-snapshot loads that FAILED, emitted by
    /// <c>BPlusLeafGrain.TryRehydrateFromSnapshotAsync</c> (issue #2364).
    /// Tagged with <see cref="TagTree"/> and <see cref="TagReason"/>:
    /// <see cref="SnapshotLoadFailureResourceExhausted"/> when an
    /// <see cref="OutOfMemoryException"/> appears anywhere in the thrown
    /// exception's chain, and <see cref="SnapshotLoadFailureFaulted"/>
    /// otherwise.
    /// <para>
    /// The rehydrate path treats a failed load as best-effort and returns
    /// "no snapshot", which is correct for availability but made the failure
    /// <b>indistinguishable from a leaf that genuinely has no snapshot</b>: both
    /// render as the same declined rehydrate, and the activation then takes the
    /// <c>-1</c> replay-start override and replays its whole readable WAL
    /// window. Without this counter the failure population reads as zero at
    /// every rate of occurrence, so the two arms of "no snapshot" cannot be
    /// separated at all.
    /// </para>
    /// <para>
    /// The <c>resource_exhausted</c> arm exists because that failure arrives
    /// <b>wearing a storage fault's clothes</b>. Under a container memory limit
    /// the .NET GC heap hard limit is sized from the cgroup limit, so the
    /// runtime is not OOM-killed - it throws
    /// <see cref="OutOfMemoryException"/> inside the provider's deserialise of
    /// the snapshot blob, and the only line an operator sees is the provider's
    /// own "Error reading grain state". Nothing in that presentation names
    /// memory, which is why a deployment can run in this state for a long time
    /// undiagnosed. It also COMPOUNDS: the failed load forces the cold
    /// whole-window replay, which costs more memory again, so the same few
    /// leaves go cold repeatedly. A sustained non-zero <c>resource_exhausted</c>
    /// rate means the host's memory limit is below the deployment's true
    /// working set, and is not a storage-provider fault.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotLoadFailures =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.load_failures", unit: "{load}",
            description: "Activation-time leaf-snapshot loads that failed and were swallowed as \"no snapshot\", tagged by tree and reason (resource_exhausted/faulted). A resource_exhausted reading is memory exhaustion presenting as a storage fault, not a provider defect.");

    /// <summary>Canonical name of <see cref="LeafSnapshotLoadFailures"/>.</summary>
    public const string LeafSnapshotLoadFailuresName = "orleans.lattice.leaf.snapshot.load_failures";

    /// <summary>
    /// <see cref="TagReason"/> = <c>resource_exhausted</c> on
    /// <see cref="LeafSnapshotLoadFailures"/>. An
    /// <see cref="OutOfMemoryException"/> was present in the failure's
    /// exception chain, so the load did not fail because the store was
    /// unreachable or the row unreadable - it failed because the blob could not
    /// be materialised within the available heap.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotLoadFailureResourceExhausted =
        new(TagReason, "resource_exhausted");

    /// <summary>
    /// <see cref="TagReason"/> = <c>faulted</c> on
    /// <see cref="LeafSnapshotLoadFailures"/>: any load failure with no
    /// <see cref="OutOfMemoryException"/> in its chain (an unreachable store, a
    /// rejected activation, a deserialisation defect). Kept apart from
    /// <see cref="SnapshotLoadFailureResourceExhausted"/> because the two call
    /// for opposite operator responses - raise the memory limit, versus
    /// investigate the storage provider - and folding them together is exactly
    /// the conflation this counter exists to undo.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotLoadFailureFaulted =
        new(TagReason, "faulted");

    /// <summary>
    /// Counter of activation-time leaf-snapshot hydrations that passed through
    /// the byte-budgeted admission gate, tagged with <see cref="TagTree"/>,
    /// <see cref="TagOutcome"/> (<c>immediate</c>/<c>queued</c>) and the tenant
    /// label (issue #2765).
    /// <para>
    /// The gate bounds the aggregate bytes of snapshot loads materialising at
    /// once, so a cold start costs a function of this process's heap rather than
    /// of however many leaves Orleans happens to activate together. The
    /// <c>queued</c> arm is the one that carries information: it counts the
    /// hydrations that actually had to wait, and a sustained non-zero rate means
    /// the deployment's leaves are large enough, or numerous enough, that
    /// unbounded activation would have exceeded the heap hard limit - which is
    /// precisely the condition that used to present as a clean-exit restart
    /// loop with no OOM kill recorded anywhere.
    /// </para>
    /// <para>
    /// Both arms are primed to zero per tree at the first hydration, because a
    /// counter that is only ever incremented cannot distinguish "the gate never
    /// had to queue anything" from "the gate is not deployed in this build" from
    /// "nothing has activated yet". Those have entirely different responses, and
    /// an absent series reads identically for all three.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotHydrationAdmissions =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.hydration_admissions", unit: "{hydration}",
            description: "Activation-time leaf-snapshot hydrations admitted through the byte-budgeted concurrency gate, tagged by tree and outcome (immediate/queued). A sustained queued rate means unbounded cold activation would have exceeded the heap hard limit.");

    /// <summary>Canonical name of <see cref="LeafSnapshotHydrationAdmissions"/>.</summary>
    public const string LeafSnapshotHydrationAdmissionsName = "orleans.lattice.leaf.snapshot.hydration_admissions";

    /// <summary>
    /// Counter incremented once per segment read during a segmented
    /// leaf-snapshot hydration, tagged by tree and <see cref="TagOutcome"/>
    /// (<c>loaded</c> / <c>missing</c> / <c>failed</c>) (issue #2914).
    /// <para>
    /// A segmented snapshot is one whose encoded frame exceeded
    /// <see cref="LatticeOptions.LeafSnapshotSegmentBytes"/> and was therefore
    /// spread across one grain-state row per segment, so that hydration reads
    /// and releases one bounded window at a time instead of demanding a single
    /// contiguous array for the whole payload.
    /// </para>
    /// <para>
    /// All three arms are primed to zero per tree at the first segmented
    /// hydration. Without priming, an absent <c>missing</c> series cannot be
    /// distinguished from a build in which the fail-closed branch does not
    /// exist - and those have opposite readings, since the first means every
    /// segment read back and the second means nothing is checking.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotSegmentReads =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.segment_reads", unit: "{read}",
            description: "Segment reads performed during segmented leaf-snapshot hydration, tagged by tree and outcome (loaded/missing/failed). A non-zero missing or failed rate means a segmented snapshot could not be reproduced in full and the leaf fell back to WAL replay.");

    /// <summary>Canonical name of <see cref="LeafSnapshotSegmentReads"/>.</summary>
    public const string LeafSnapshotSegmentReadsName = "orleans.lattice.leaf.snapshot.segment_reads";

    /// <summary>
    /// Counter incremented once per completed segmented leaf-snapshot
    /// hydration, tagged by tree (issue #2914). Primed to zero per tree so
    /// "no snapshot was large enough to segment" is distinguishable from
    /// "segmentation is not in this build".
    /// </summary>
    public static readonly Counter<long> LeafSnapshotSegmentedHydrations =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.segmented_hydrations", unit: "{hydration}",
            description: "Leaf-snapshot hydrations served by reading a segmented snapshot one bounded window at a time, tagged by tree.");

    /// <summary>Canonical name of <see cref="LeafSnapshotSegmentedHydrations"/>.</summary>
    public const string LeafSnapshotSegmentedHydrationsName = "orleans.lattice.leaf.snapshot.segmented_hydrations";

    /// <summary>
    /// Per-tree high-water mark of the largest single segment frame read during
    /// a segmented hydration. Declared above the gauge that reads it so the
    /// observation callback can never see it uninitialised.
    /// </summary>
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> SegmentPeakBytesByTree = new();

    /// <summary>
    /// Monotonic high-water gauge of the largest single <b>contiguous</b>
    /// segment frame this process has materialised for a tree during segmented
    /// leaf-snapshot hydration (issue #2914).
    /// <para>
    /// A high-water gauge rather than a histogram, deliberately. The Prometheus
    /// exporter this project is measured through renders a histogram as a
    /// summary carrying only <c>_sum</c> and <c>_count</c> - no buckets and no
    /// quantiles - so the only statistic recoverable from one is the mean, and
    /// a mean over many small segments dilutes the single large allocation that
    /// is the entire quantity of interest to nothing. The peak is what fails,
    /// so the peak is what is recorded.
    /// </para>
    /// <para>
    /// This is the series that evidences the bound: it must stay at or below
    /// the configured <see cref="LatticeOptions.LeafSnapshotSegmentBytes"/>
    /// window regardless of how large the underlying snapshot is. A value that
    /// tracks snapshot size instead means segments are being reassembled
    /// somewhere before being decoded, which would restore the contiguous
    /// allocation while leaving every other signal looking healthy.
    /// </para>
    /// </summary>
    public static readonly ObservableGauge<long> LeafSnapshotSegmentPeakBytes =
        Meter.CreateObservableGauge("orleans.lattice.leaf.snapshot.segment_peak_bytes",
            ObserveSegmentPeakBytes, unit: "By",
            description: "High-water mark of the largest contiguous segment frame materialised during segmented leaf-snapshot hydration, tagged by tree. Bounded by LeafSnapshotSegmentBytes; a value tracking snapshot size means the payload is being reassembled contiguously.");

    /// <summary>Canonical name of <see cref="LeafSnapshotSegmentPeakBytes"/>.</summary>
    public const string LeafSnapshotSegmentPeakBytesName = "orleans.lattice.leaf.snapshot.segment_peak_bytes";

    private static IEnumerable<Measurement<long>> ObserveSegmentPeakBytes()
    {
        foreach (var entry in SegmentPeakBytesByTree)
        {
            yield return new Measurement<long>(
                entry.Value,
                new KeyValuePair<string, object?>(TagTree, entry.Key),
                LatticeTenantLabel.ForTree(entry.Key));
        }
    }

    /// <summary>
    /// Raises the high-water mark reported by
    /// <see cref="LeafSnapshotSegmentPeakBytes"/> for <paramref name="treeId"/>
    /// to <paramref name="bytes"/> when it exceeds the current mark.
    /// Monotonic: the mark never falls, so a peak that has already happened
    /// stays visible to a scrape that arrives afterwards.
    /// </summary>
    public static void RecordSegmentPeakBytes(string treeId, long bytes)
    {
        if (treeId is not { Length: > 0 })
        {
            return;
        }

        // Priming at zero so the series exists for a tree that has segmented a
        // snapshot but whose peak has not yet been observed by a scrape.
        SegmentPeakBytesByTree.AddOrUpdate(treeId, bytes, (_, current) => Math.Max(current, bytes));
    }

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>loaded</c> on
    /// <see cref="LeafSnapshotSegmentReads"/>: the segment read back and
    /// validated, and its rows were folded into the leaf's entry cache.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotSegmentLoaded =
        new(TagOutcome, "loaded");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>missing</c> on
    /// <see cref="LeafSnapshotSegmentReads"/>: the segment was absent or did
    /// not validate, so the hydration failed closed, emptied the cache, and
    /// fell back to WAL replay.
    /// <para>
    /// Distinct from <see cref="SnapshotSegmentFailed"/> because the two have
    /// different causes: <c>missing</c> is a durable-state problem (a torn or
    /// retired segment), whereas <c>failed</c> is a transient storage fault.
    /// Folding them together would let a persistent corruption hide inside a
    /// rate that an operator reads as flaky I/O.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotSegmentMissing =
        new(TagOutcome, "missing");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>failed</c> on
    /// <see cref="LeafSnapshotSegmentReads"/>: the segment read threw, so the
    /// hydration failed closed and fell back to WAL replay.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotSegmentFailed =
        new(TagOutcome, "failed");

    /// <summary>
    /// Counter incremented once per leaf division that could not take a split
    /// pivot from the snapshot frame alone and fell back to the ordered view,
    /// tagged by tree, <see cref="TagReason"/> and <see cref="TagDetachSeam"/>.
    /// <para>
    /// The fallback materialises the whole leaf and ends in a detach, so every
    /// row is resident for the life of the activation and no later eviction can
    /// recover the footprint. On an oversized leaf that is the allocation the
    /// division can least afford, which makes dividing it require an allocation
    /// proportional to its size - so a leaf that cannot afford it stays over
    /// threshold and keeps growing.
    /// </para>
    /// <para>
    /// The two tags are only useful together. <see cref="TagReason"/> =
    /// <c>no_snapshot_attached</c> with <see cref="TagDetachSeam"/> = <c>none</c>
    /// is benign: the leaf was replayed from the write-ahead log, never attached
    /// a frame, and its rows were already resident, so the fallback costs
    /// nothing extra. The same reason with any other seam is a forfeiture, and
    /// the seam names the surface that caused it. Reading the reason alone
    /// conflates the two, and they have opposite costs.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafBisectRefusals =
        Meter.CreateCounter<long>("orleans.lattice.leaf.bisect_refusals", unit: "{refusal}",
            description: "Leaf divisions that fell back to the whole-cache ordered view because no split pivot could be taken from the snapshot frame, tagged by tree, reason and the cache surface that detached the frame.");

    /// <summary>Canonical name of <see cref="LeafBisectRefusals"/>.</summary>
    public const string LeafBisectRefusalsName = "orleans.lattice.leaf.bisect_refusals";

    /// <summary>
    /// Counter incremented once per leaf division sought on an over-capacity
    /// leaf, tagged by tree, <see cref="TagOutcome"/> and the tenant label.
    /// <para>
    /// This exists to make <see cref="LeafBisectRefusals"/> interpretable when
    /// it reads zero, and it is the only thing that does. A refusal count of
    /// zero on a leaf that is over threshold and undivided spans two states
    /// with opposite meanings: a division was sought and completed (not a
    /// defect), or no division was ever sought at all (says nothing either
    /// way). Without a separate attempt signal those are indistinguishable, so
    /// a zero would read as a refutation of the forfeiture when it is no
    /// evidence at all.
    /// </para>
    /// <para>
    /// All five outcomes are zero-primed at the capture seam, for the reason
    /// established by issue #2756 on <see cref="LeafByteOverflows"/>: a
    /// <see cref="Counter{T}"/> exports nothing until its first
    /// <c>Add</c>, so an absent series and a measured zero are the same
    /// observation to a reader. Priming makes "sought a division and never got
    /// one" a positive reading rather than an absence, which is precisely the
    /// state that must not collapse into "divided successfully".
    /// </para>
    /// <para>
    /// <b><see cref="LeafSplitFaulted"/> is what makes that promise true rather
    /// than merely stated (issue #2845).</b> Until it existed the outcome arms
    /// covered only the ways a division could <em>decline</em> to run, so a
    /// division that began and threw incremented
    /// <see cref="LeafSplits"/> and nothing at all here - and the primed zero
    /// and the every-division-threw zero were the same reading, which is the
    /// exact collapse this counter exists to prevent. That was not a corner
    /// case: it was the observed state of a production tree, where fifteen
    /// divisions had begun, none had recorded an outcome, and the instrument
    /// read as though no division had ever been sought.
    /// </para>
    /// <para>
    /// The fault arm additionally carries <see cref="TagFailureClass"/>,
    /// because the two classes that occur in practice have opposite remedies -
    /// see <see cref="LeafSplitFaultUnaffordable"/> and
    /// <see cref="LeafSplitFaultTimeout"/>. Read the fault arm against
    /// <see cref="LeafSplits"/> rather than on its own: that counter increments
    /// only once the split intent is durable, so <c>splits - divided</c> is the
    /// number of divisions that stranded a leaf mid-division, while a fault
    /// with no matching <see cref="LeafSplits"/> increment threw before the
    /// intent was persisted and stranded nothing. The subtraction is the
    /// reading; the fault count alone does not distinguish the two.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSplitAttempts =
        Meter.CreateCounter<long>("orleans.lattice.leaf.split_attempts", unit: "{attempt}",
            description: "Leaf divisions sought on an over-capacity leaf, tagged by tree and outcome (divided/gate_contended/already_under_capacity/no_admissible_pivot/faulted, the last also tagged failure_class as unaffordable/timeout/other). Read alongside leaf bisect refusals, which is uninterpretable at zero without it.");

    /// <summary>Canonical name of <see cref="LeafSplitAttempts"/>.</summary>
    public const string LeafSplitAttemptsName = "orleans.lattice.leaf.split_attempts";

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>divided</c> on
    /// <see cref="LeafSplitAttempts"/>: the division ran to completion.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitDivided =
        new(TagOutcome, "divided");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>gate_contended</c> on
    /// <see cref="LeafSplitAttempts"/>: another turn held the split gate, so
    /// this turn returned without evaluating the leaf. No division was
    /// attempted and none was refused.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitGateContended =
        new(TagOutcome, "gate_contended");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>already_under_capacity</c> on
    /// <see cref="LeafSplitAttempts"/>: the in-gate re-check found the leaf
    /// back under threshold, because a concurrent turn had already divided it.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitAlreadyUnderCapacity =
        new(TagOutcome, "already_under_capacity");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>no_admissible_pivot</c> on
    /// <see cref="LeafSplitAttempts"/>: the division was declined because no
    /// admissible pivot exists. A pivot must fall strictly inside the leaf's own
    /// declared <c>(LowKeyInclusive, HighKeyExclusive)</c> range, or one of the
    /// two halves is born owning an empty key range. A leaf holding only
    /// out-of-span rows - orphans parked by the fail-open span-admission path,
    /// or a migration graft - has no such pivot, and dividing it would mint a
    /// leaf that can never be routed a write. Issue 3117.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitNoAdmissiblePivot =
        new(TagOutcome, "no_admissible_pivot");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>faulted</c> on
    /// <see cref="LeafSplitAttempts"/>: the division began and threw before it
    /// could complete. Always accompanied by <see cref="TagFailureClass"/>.
    /// <para>
    /// This arm is the difference between the counter answering its own
    /// question and silently refusing to (issue #2845). The other three
    /// outcomes all describe a division that declined to start, so before this
    /// existed there was no arm a started-and-failed division could land on,
    /// and it landed on none of them - leaving the instrument reading exactly
    /// as it reads on a tree where nothing was ever attempted.
    /// </para>
    /// <para>
    /// It is also the wedge signal on this counter, but only in conjunction
    /// with <see cref="LeafSplits"/>, and the two must be read together.
    /// <see cref="LeafSplits"/> is incremented only after
    /// <c>SplitState = SplitInProgress</c> is persisted, so a fault whose
    /// division got that far left a leaf holding a half-finished division - a
    /// pivot chosen, a sibling grain id allocated, the next-sibling pointer
    /// repointed - which the recovery path will re-enter on the next write.
    /// A fault thrown <em>before</em> that point persisted no intent and
    /// stranded nothing, and the hydration admission gate is exactly that case:
    /// it declines in advance, so a
    /// <see cref="LeafSplitFaultUnaffordable"/> fault is the arm least likely
    /// to have wedged anything. The count that means "wedged" is therefore
    /// <c>splits - divided</c>, not the fault count; a non-zero fault arm with
    /// <see cref="LeafSplits"/> flat is a tree failing to start divisions, not
    /// one stuck in the middle of them, and reaching for recovery tooling on
    /// the strength of the fault arm alone will find nothing to recover.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitFaulted =
        new(TagOutcome, "faulted");

    /// <summary>
    /// <see cref="TagFailureClass"/> = <c>unaffordable</c> on
    /// <see cref="LeafSplitAttempts"/>: the division could not be paid for in
    /// memory, either because the hydration admission gate refused it
    /// (<c>LeafSnapshotUnaffordableException</c>) or because the allocation
    /// failed outright. The remedy is to reduce what a division has to
    /// materialise or to raise the hydration budget; waiting does not help,
    /// because the leaf only grows.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitFaultUnaffordable =
        new(TagFailureClass, "unaffordable");

    /// <summary>
    /// <see cref="TagFailureClass"/> = <c>timeout</c> on
    /// <see cref="LeafSplitAttempts"/>: the division exceeded a deadline -
    /// an Orleans response deadline, or one of the typed
    /// <see cref="TimeoutException"/> subclasses this library raises. The
    /// remedy is the opposite of the unaffordable one: the division was
    /// affordable and the path under it was too slow, so the storage path and
    /// the configured deadline are what to examine.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitFaultTimeout =
        new(TagFailureClass, "timeout");

    /// <summary>
    /// <see cref="TagFailureClass"/> = <c>other</c> on
    /// <see cref="LeafSplitAttempts"/>: the division threw something outside
    /// the two classified families. Deliberately a real arm rather than an
    /// absent tag, so that a fault is never uncounted merely because it was
    /// unrecognised - an unclassifiable fault must still be visible as a
    /// fault.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafSplitFaultOther =
        new(TagFailureClass, "other");


    // --- Leaf-split completion in-flight registry (issue #2967) -------------
    //
    // The leaf-split outcome taxonomy (LeafSplitAttempts: divided / faulted /
    // the declining arms) is a partition of *terminated* divisions. A division
    // suspended inside CompleteSplitAsync at scrape time has terminated on
    // none of them, and because the catch that records `faulted` is
    // unconditional a division carrying neither `divided` nor `faulted` did not
    // throw - it is genuinely in flight. That un-terminated state has no member
    // in the outcome taxonomy, so a permanently-stuck division reports as
    // healthy concurrency forever and nothing separates `busy` from `stuck`.
    //
    // These two gauges name that state as a *measurement*, not a threshold: the
    // count of completions currently suspended, and the age of the oldest. No
    // constant is encoded here - an operator or alert reads `stuck` off the age
    // climbing, which is a judgement made against real data rather than a guess
    // baked into the code. Both cover the forward path and the recovery path,
    // because both funnel through CompleteSplitAsync, which is what the issue
    // names; the signal is suspension duration, orthogonal to the recovery
    // *outcome* metering tracked separately by issue #2860.

    /// <summary>
    /// Live registry of leaf-split completions currently suspended inside
    /// <c>CompleteSplitAsync</c>, keyed by a process-unique token and carrying
    /// the tree the completion belongs to and the monotonic timestamp it
    /// entered. Declared above the gauges that read it so their observation
    /// callbacks can never see it uninitialised.
    /// <para>
    /// An entry lives for exactly as long as one call is suspended in the
    /// completion body: added when the call enters and removed when the call
    /// returns or throws. A completion that never resumes - the wedge this
    /// exists to surface - keeps its entry, and its reported age climbs without
    /// bound.
    /// </para>
    /// </summary>
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<long, (string Tree, long StartTimestamp)>
        LeafSplitCompletionsInFlightRegistry = new();

    private static long _leafSplitCompletionToken;

    /// <summary>
    /// Registers a leaf-split completion as in flight for the duration of the
    /// returned scope. Disposing the scope (normally via <c>using</c>)
    /// deregisters it; a scope that is never disposed - a completion suspended
    /// forever at an <c>await</c> - keeps the entry live, which is precisely the
    /// state <see cref="LeafSplitCompletionsInFlight"/> and
    /// <see cref="LeafSplitCompletionOldestAge"/> exist to make observable.
    /// </summary>
    public static LeafSplitCompletionScope EnterLeafSplitCompletion(string treeId)
    {
        var token = System.Threading.Interlocked.Increment(ref _leafSplitCompletionToken);
        LeafSplitCompletionsInFlightRegistry[token] = (treeId ?? string.Empty, System.Diagnostics.Stopwatch.GetTimestamp());
        return new LeafSplitCompletionScope(token);
    }

    /// <summary>
    /// Disposable scope returned by <see cref="EnterLeafSplitCompletion"/>.
    /// Removes its registry entry on <see cref="Dispose"/>. A value type so the
    /// hot split path takes no per-completion heap allocation; it is never
    /// copied because it is only ever bound by a <c>using</c> local.
    /// </summary>
    public readonly struct LeafSplitCompletionScope : IDisposable
    {
        private readonly long _token;

        internal LeafSplitCompletionScope(long token) => _token = token;

        /// <summary>Deregisters the in-flight completion this scope tracks.</summary>
        public void Dispose() => LeafSplitCompletionsInFlightRegistry.TryRemove(_token, out _);
    }

    /// <summary>
    /// Per-tree count of leaf-split completions currently suspended inside
    /// <c>CompleteSplitAsync</c> (issue #2967). A sustained non-zero value with
    /// a rising <see cref="LeafSplitCompletionOldestAge"/> is a division wedged
    /// mid-completion, which the terminal outcome counters cannot name because
    /// it has terminated on none of them.
    /// </summary>
    public static readonly ObservableGauge<long> LeafSplitCompletionsInFlight =
        Meter.CreateObservableGauge("orleans.lattice.leaf.split.completion.in_flight",
            ObserveLeafSplitCompletionsInFlight, unit: "{completion}",
            description: "Leaf-split completions currently suspended inside CompleteSplitAsync, tagged by tree. Read with completion.oldest_age: a non-zero count whose oldest age climbs is a division wedged mid-completion, a state the divided/faulted outcome counters cannot name.");

    /// <summary>Canonical name of <see cref="LeafSplitCompletionsInFlight"/>.</summary>
    public const string LeafSplitCompletionsInFlightName = "orleans.lattice.leaf.split.completion.in_flight";

    /// <summary>
    /// Per-tree age in seconds of the oldest leaf-split completion currently
    /// suspended inside <c>CompleteSplitAsync</c> (issue #2967), or no series
    /// for a tree with none in flight. This is the signal that separates a
    /// division that is merely momentarily in flight (age near zero, falling as
    /// scrapes advance) from one that is stuck (age climbing without bound). It
    /// is a measurement, not a threshold: the code encodes no age at which a
    /// completion is declared abandoned, leaving that judgement to an operator
    /// reading real values.
    /// </summary>
    public static readonly ObservableGauge<double> LeafSplitCompletionOldestAge =
        Meter.CreateObservableGauge("orleans.lattice.leaf.split.completion.oldest_age",
            ObserveLeafSplitCompletionOldestAge, unit: "s",
            description: "Age in seconds of the oldest leaf-split completion currently suspended inside CompleteSplitAsync, tagged by tree (no series when none in flight). A climbing value is a division stuck mid-completion; a value near zero is healthy concurrency.");

    /// <summary>Canonical name of <see cref="LeafSplitCompletionOldestAge"/>.</summary>
    public const string LeafSplitCompletionOldestAgeName = "orleans.lattice.leaf.split.completion.oldest_age";

    private static IEnumerable<Measurement<long>> ObserveLeafSplitCompletionsInFlight()
    {
        var counts = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var entry in LeafSplitCompletionsInFlightRegistry)
        {
            counts.TryGetValue(entry.Value.Tree, out var current);
            counts[entry.Value.Tree] = current + 1;
        }

        foreach (var kv in counts)
        {
            yield return new Measurement<long>(
                kv.Value,
                new KeyValuePair<string, object?>(TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    private static IEnumerable<Measurement<double>> ObserveLeafSplitCompletionOldestAge()
    {
        var now = System.Diagnostics.Stopwatch.GetTimestamp();
        var oldestStart = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var entry in LeafSplitCompletionsInFlightRegistry)
        {
            if (!oldestStart.TryGetValue(entry.Value.Tree, out var start) || entry.Value.StartTimestamp < start)
            {
                oldestStart[entry.Value.Tree] = entry.Value.StartTimestamp;
            }
        }

        foreach (var kv in oldestStart)
        {
            var seconds = (now - kv.Value) / (double)System.Diagnostics.Stopwatch.Frequency;
            if (seconds < 0)
            {
                seconds = 0;
            }

            yield return new Measurement<double>(
                seconds,
                new KeyValuePair<string, object?>(TagTree, kv.Key),
                LatticeTenantLabel.ForTree(kv.Key));
        }
    }

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>immediate</c> on
    /// <see cref="LeafSnapshotHydrationAdmissions"/>: the hydration fitted inside
    /// the remaining budget and started without waiting.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotHydrationAdmittedImmediately =
        new(TagOutcome, "immediate");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>queued</c> on
    /// <see cref="LeafSnapshotHydrationAdmissions"/>: the hydration waited behind
    /// the budget before starting. This is the gate doing its job, not a fault,
    /// but a sustained rate is the signal that cold activation is memory-bound.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotHydrationQueued =
        new(TagOutcome, "queued");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>sole_occupancy</c> on
    /// <see cref="LeafSnapshotHydrationAdmissions"/>: the hydration's largest
    /// single <b>contiguous</b> allocation exceeded what the gate will attempt
    /// alongside other hydrations, so it was serialised and ran with the gate
    /// otherwise empty (issue #2844).
    /// <para>
    /// Reported as a third outcome rather than folded into <c>queued</c>
    /// because the two say different things about what to do. A <c>queued</c>
    /// hydration waited on <b>aggregate</b> bytes and drains as the storm
    /// clears; a <c>sole_occupancy</c> hydration was serialised on a predicate
    /// that does not improve with the memory grant at all, so a sustained rate
    /// is a signal to divide leaves or lower <c>MaxLeafBytes</c>, and
    /// specifically NOT to provision more memory - which would raise every
    /// grant-derived limit and admit more of exactly these claims.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotHydrationSoleOccupancy =
        new(TagOutcome, "sole_occupancy");

    /// <summary>
    /// <see cref="TagReason"/> = <c>contiguity_exhausted</c> on
    /// <see cref="LeafSnapshotLoadFailures"/>: the load ran out of memory while
    /// its hydration claim was comfortably <b>inside</b> the admission gate's
    /// budget, so what ran out was one unbroken run of memory rather than the
    /// process's total (issue #2844).
    /// <para>
    /// Separated from <c>resource_exhausted</c> because that arm's documented
    /// remedy - the host is provisioned below this deployment's working set -
    /// is actively harmful here. Every byte-denominated limit in this process is
    /// derived from the memory grant and admits more concurrent work as the
    /// grant grows, while contiguous feasibility does not improve with it, so
    /// treating this arm as a provisioning shortfall makes it more frequent. It
    /// is the arm that says the gate admitted by byte accounting and the
    /// allocation failed regardless.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotLoadFailureContiguityExhausted =
        new(TagReason, "contiguity_exhausted");

    /// <summary>
    /// Counter of leaf activations shed by the per-silo resident leaf working
    /// set, tagged with <see cref="TagTree"/>, <see cref="TagKind"/>
    /// (<c>banked</c>/<c>unbanked</c>) and the tenant label (issue #2767).
    /// <para>
    /// Where <see cref="LeafSnapshotHydrationAdmissions"/> counts hydrations
    /// bounded while they materialise, this counts activations deactivated
    /// because what they retained <b>after</b> materialising exceeded the
    /// process's resident budget. The two measure disjoint costs: a hydration
    /// that queued and then completed is invisible here, and a leaf shed here
    /// was admitted there without waiting.
    /// </para>
    /// <para>
    /// The class tag is the one to watch. A <c>banked</c> shed returns on the
    /// fast path by re-attaching its snapshot; an <c>unbanked</c> shed
    /// reactivates cold and must first queue for a replay permit, so a
    /// sustained <c>unbanked</c> rate means the working set has run out of cheap
    /// candidates and is now paying whole-window replays to stay under budget.
    /// That is the signal to look at snapshot coverage, not at this bound.
    /// </para>
    /// <para>
    /// Both arms are primed to zero per tree when a tree first registers, for
    /// the same reason the admission arms are: an absent series otherwise reads
    /// identically for "never needed to shed", "not deployed in this build" and
    /// "nothing has activated yet", which have entirely different responses.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafResidencySheds =
        Meter.CreateCounter<long>("orleans.lattice.leaf.residency.sheds", unit: "{activation}",
            description: "Leaf activations deactivated by the per-silo resident leaf working set to stay under its derived byte budget, tagged by tree and by whether the shed leaf was snapshot-banked (cheap reload) or unbanked (cold whole-window replay).");

    /// <summary>Canonical name of <see cref="LeafResidencySheds"/>.</summary>
    public const string LeafResidencyShedsName = "orleans.lattice.leaf.residency.sheds";

    /// <summary>
    /// <see cref="TagKind"/> = <c>banked</c> on <see cref="LeafResidencySheds"/>:
    /// the shed activation had rehydrated from a durable snapshot, so it
    /// reactivates by re-attaching that snapshot and replaying only the tail.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafResidencyClassBanked =
        new(TagKind, "banked");

    /// <summary>
    /// <see cref="TagKind"/> = <c>unbanked</c> on <see cref="LeafResidencySheds"/>:
    /// the shed activation held no snapshot, so it reactivates cold, replaying
    /// the whole readable WAL window behind a replay permit.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafResidencyClassUnbanked =
        new(TagKind, "unbanked");

    /// <summary>
    /// Name of the observable gauge reporting the <b>resolved</b> resident leaf
    /// working-set budget in bytes (issue #2788). Registered lazily by
    /// <c>LeafResidencyMetrics</c>.
    /// <para>
    /// This reports the number the bound actually enforces, not the inputs it
    /// was derived from, because the question it exists to answer is asked of
    /// the outcome: an all-zero <see cref="LeafResidencySheds"/> is ambiguous
    /// between "the working set is correctly quiescent under its budget" and
    /// "the budget is so large the bound can never engage", and only the
    /// resolved figure separates them. Read it against the container's memory
    /// grant: a budget of the same order as the whole grant means the bound is
    /// structurally unable to fire and the zero is meaningless.
    /// </para>
    /// </summary>
    public const string LeafResidencyBudgetBytesName = "orleans.lattice.leaf.residency.budget_bytes";

    /// <summary>
    /// Name of the observable gauge reporting the bytes currently accounted to
    /// live, un-shed leaf registrations (issue #2788). Registered lazily by
    /// <c>LeafResidencyMetrics</c>.
    /// <para>
    /// Together with <see cref="LeafResidencyBudgetBytesName"/> this is the
    /// headroom reading: the ratio of the two says how close the working set is
    /// to its first shed, which a counter of sheds cannot say while it reads
    /// zero.
    /// </para>
    /// </summary>
    public const string LeafResidencyResidentBytesName = "orleans.lattice.leaf.residency.resident_bytes";

    /// <summary>
    /// Name of the observable gauge reporting the number of leaf registrations
    /// currently held by the resident working set (issue #2788). Registered
    /// lazily by <c>LeafResidencyMetrics</c>.
    /// <para>
    /// This is the arm that distinguishes an <b>empty</b> ledger from a
    /// <b>populated but under-budget</b> one. Those have opposite remedies -
    /// registration is broken, versus the bound is working - and
    /// <see cref="LeafResidencySheds"/> reads zero for both, because a counter
    /// observes an <i>action</i> and the question is about <i>state</i>.
    /// </para>
    /// </summary>
    public const string LeafResidencyRegistrationsName = "orleans.lattice.leaf.residency.registrations";

    /// <summary>
    /// Counter of leaf-snapshot capture <b>attempts</b>, emitted by
    /// <c>BPlusLeafGrain.CaptureSnapshotCoreAsync</c> once per attempt that
    /// passes the eligibility gates, tagged with <see cref="TagTree"/>,
    /// <see cref="TagOutcome"/>
    /// (<c>succeeded</c>/<c>failed</c>/<c>abandoned</c>) and the tenant label.
    /// <para>
    /// This exists because before issue #2696 the capture path carried
    /// <b>no instrument at all</b>: the write half was untimed and uncounted,
    /// and <c>TryCaptureSnapshotForAdvisoryAsync</c> caught every exception and
    /// only logged it. The single capture-derived series on the endpoint,
    /// <see cref="StorageSnapshotBytesName"/>, is fed only <b>after</b> a
    /// successful save, so it reads <c>0</c> both when every capture is failing
    /// and when no capture has ever been attempted. Those are opposite
    /// operational states - a broken provider versus a correctly idle one - and
    /// they were byte-identical on <c>/metrics</c>. Nothing exported could tell
    /// them apart.
    /// </para>
    /// <para>
    /// A failure counter <b>alone</b> would not have fixed that, which is why
    /// this counts attempts and tags the outcome rather than counting failures.
    /// A bare failure counter reading zero is ambiguous in exactly the same way
    /// the original defect was - no failures because everything succeeded, or
    /// no failures because nothing ran - so it would have reproduced the defect
    /// one level up. Read <c>succeeded + failed + abandoned</c> as "attempted";
    /// a tree absent from this counter entirely has genuinely never attempted a
    /// capture, and that is now a distinguishable, positively-readable state.
    /// </para>
    /// <para>
    /// Counted at the single-flight boundary inside the core capture method
    /// rather than at the advisory call site, so that every caller (activation
    /// advisory, periodic recheck, cold-progress banking, graceful
    /// deactivation) is denominated identically and this counter shares its
    /// population exactly with
    /// <see cref="LeafSnapshotCaptureDuration"/>. Instrumenting the advisory
    /// catch block alone would have counted failures from one caller against
    /// durations from four.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotCaptures =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.captures", unit: "{capture}",
            description: "Leaf-snapshot capture attempts, tagged by tree and outcome (succeeded/failed/abandoned). Sum across outcomes is the attempt count, which is what distinguishes \"every capture failed\" from \"capture never ran\" - a distinction no exported series could make before issue #2696.");

    /// <summary>Canonical name of <see cref="LeafSnapshotCaptures"/>.</summary>
    public const string LeafSnapshotCapturesName = "orleans.lattice.leaf.snapshot.captures";

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>succeeded</c> on
    /// <see cref="LeafSnapshotCaptures"/>: the blob reached the snapshot
    /// storage grain and durable coverage was advanced.
    /// <para>
    /// <b>This records that a capture completed and persisted, not that the
    /// result is loadable</b>, and the distinction is not pedantic. A capture
    /// whose <c>SnapshotOffset</c> normalises to null is saved successfully and
    /// is then discarded on the load path by <c>HasCapturedPrefix</c>, so for
    /// that population this counter reports a success for a blob that can never
    /// be read back. The limitation is named here rather than left to be
    /// inferred, because the alternative - a reader treating
    /// <c>succeeded</c> as end-to-end coverage - is exactly the class of
    /// unstated guarantee this instrument exists to remove. Correlate with
    /// <c>orleans.lattice.leaf.snapshot.load_failures</c> and
    /// <c>orleans.lattice.storage.snapshot_bytes</c> before reading it as
    /// coverage.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotCaptureSucceeded =
        new(TagOutcome, "succeeded");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>failed</c> on
    /// <see cref="LeafSnapshotCaptures"/>: the attempt threw and the exception
    /// was swallowed by the caller. This is the reading that was previously
    /// invisible - the advisory handler logs it and increments nothing, so a
    /// deployment in which every capture fails presented as total silence.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotCaptureFailed =
        new(TagOutcome, "failed");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>abandoned</c> on
    /// <see cref="LeafSnapshotCaptures"/>: the caller's token was cancelled, so
    /// the attempt was deliberately dropped on a deactivation deadline (issue
    /// #1965) rather than failing. Kept apart from
    /// <see cref="SnapshotCaptureFailed"/> because a fleet-wide shutdown and a
    /// broken storage provider would otherwise look identical. The existing
    /// log-flood argument for swallowing these silently does not extend to a
    /// counter: an increment is O(1) against a bounded tag set, not a line.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotCaptureAbandoned =
        new(TagOutcome, "abandoned");

    /// <summary>
    /// Histogram of wall-clock leaf-snapshot capture duration in milliseconds,
    /// emitted by <c>BPlusLeafGrain.CaptureSnapshotCoreAsync</c>, tagged with
    /// <see cref="TagTree"/>, <see cref="TagOutcome"/> and the tenant label.
    /// Shares its population exactly with <see cref="LeafSnapshotCaptures"/>.
    /// <para>
    /// Timing starts at the single-flight boundary, <b>after</b> the
    /// eligibility gates, so a leaf that declines to capture contributes no
    /// sample. That boundary is load-bearing rather than tidy: the gates return
    /// in microseconds and are taken far more often than a capture runs, so
    /// timing the whole method would let near-zero no-ops dominate the count
    /// and drag the mean toward zero - an instrument that reports "captures are
    /// fast" precisely when none are happening.
    /// </para>
    /// <para>
    /// The capture is awaited <b>inline inside <c>OnActivateAsync</c></b>, and
    /// Orleans delivers no request to a grain until activation completes, so
    /// this duration is paid by every caller waiting on that leaf. It is
    /// therefore the direct measurement of the activation-latency contribution
    /// that previously had to be inferred from caller-side
    /// <see cref="GrainCallDuration"/>, which conflates activation with call
    /// and queue latency.
    /// </para>
    /// <para>
    /// <b>Readable as an interval mean only.</b> The container's metrics
    /// exposition renders a histogram as <c>_sum</c>/<c>_count</c> with no
    /// quantiles and no buckets, and both are cumulative since process start,
    /// so a percentile is not derivable from this instrument as deployed and a
    /// threshold on the raw cumulative mean is heavily damped. Read it as
    /// <c>(sum2-sum1)/(count2-count1)</c> across two scrapes. Do not "improve"
    /// this into an explicit-bucket histogram expecting to read percentiles;
    /// nothing in the pipeline renders them.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> LeafSnapshotCaptureDuration =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.snapshot.capture.duration", unit: "ms",
            description: "Wall-clock duration of leaf-snapshot capture attempts, tagged by tree and outcome. Measured from the single-flight boundary so declined captures contribute no sample. Exported with explicit buckets like every other ms-unit histogram on this meter, so histogram_quantile over _bucket is available; delta _sum over delta _count gives an interval mean.");

    /// <summary>Canonical name of <see cref="LeafSnapshotCaptureDuration"/>.</summary>
    public const string LeafSnapshotCaptureDurationName = "orleans.lattice.leaf.snapshot.capture.duration";

    /// <summary>
    /// Canonical name of the observable gauge published by
    /// <c>LeafSnapshotCaptureConcurrencyCensus</c>, reporting the greatest number
    /// of leaf-snapshot captures seen executing at once on this silo since
    /// process start.
    /// <para>
    /// The gauge is <b>monotone non-decreasing</b> and that is the whole design.
    /// The quantity worth measuring here is a transient fan-out spike, which an
    /// instantaneous gauge samples only at scrape time and therefore usually
    /// misses - the objection on which PR #2723 deferred this metric. A
    /// high-water mark is reported by the scrape that follows the spike and by
    /// every scrape after it, so scrape timing stops mattering rather than merely
    /// being unlikely to matter. Read it as "the worst this silo has ever been",
    /// not as a current depth; a restart is what resets it.
    /// </para>
    /// <para>
    /// The instrument lives on its own census class rather than here because an
    /// observable instrument's callback must be declared below every piece of
    /// static state it reads, and this file declares none of that state.
    /// </para>
    /// </summary>
    public const string LeafSnapshotCaptureConcurrencyPeakGaugeName = "orleans.lattice.leaf.snapshot.capture.concurrency_peak";

    /// <summary>
    /// Counter of leaf-snapshot capture attempts that crossed the attempt
    /// boundary while at least one <b>other</b> capture was already in flight
    /// somewhere on the same silo, tagged <see cref="TagTree"/>.
    /// <para>
    /// It exists because a peak alone cannot separate the two readings that the
    /// decision rests on. A recorded peak of 3 is produced both by a single
    /// three-deep burst during silo start and by a silo that has sat three-deep
    /// continuously for hours, and those call for opposite responses. The peak
    /// answers <i>how bad it got</i>; this counter answers <i>how often it
    /// happens</i>, and the pair is readable where either alone is not.
    /// </para>
    /// <para>
    /// <b>Zero-primed at the same site it is incremented.</b> Every capture that
    /// crosses the attempt boundary adds to this counter - <c>1</c> when it
    /// entered into company, <c>0</c> when it entered alone - so the series
    /// exists for every tree that has ever captured, whether or not that tree has
    /// ever contended. Without the zero-add, a tree that never contends would
    /// have no series at all, and its absence would be indistinguishable from an
    /// instrument that was never reached. That distinction is the entire
    /// evidential value of the measurement, so the extra add is deliberate and
    /// must not be optimised away.
    /// </para>
    /// <para>
    /// Tree-tagged, unlike the silo-wide peak gauge, because the capture that
    /// entered into contention does belong to one tree, so the counter answers
    /// which trees are producing the fan-out. The peak cannot be split that way:
    /// it is a maximum across trees, and per-tree maxima would each be smaller
    /// than the depth the shared storage provider actually saw.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotCaptureConcurrentEntries =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.capture.concurrent_entries", unit: "{capture}",
            description: "Leaf-snapshot capture attempts that crossed the attempt boundary while another capture was already in flight on the same silo, tagged by tree. Incremented by zero when a capture enters alone, so every capturing tree has a series and a reported zero means measured-none rather than no-detector.");

    /// <summary>Canonical name of <see cref="LeafSnapshotCaptureConcurrentEntries"/>.</summary>
    public const string LeafSnapshotCaptureConcurrentEntriesName = "orleans.lattice.leaf.snapshot.capture.concurrent_entries";

    /// <summary>
    /// Counter of leaf-snapshot capture invocations that <b>declined</b> before
    /// reaching the attempt boundary, tagged <see cref="TagTree"/> and
    /// <see cref="TagReason"/>.
    /// <para>
    /// This is a separate instrument from
    /// <see cref="LeafSnapshotCaptures"/> rather than a fourth value of that
    /// counter's <c>outcome</c> tag, and the separation is load-bearing.
    /// <see cref="LeafSnapshotCaptures"/> and
    /// <see cref="LeafSnapshotCaptureDuration"/> are written at one boundary
    /// precisely so they share a population exactly - every counted attempt is
    /// timed and every timed attempt is counted - which makes their ratio a
    /// within-family comparison that cannot drift. A counter-only outcome value
    /// would break that invariant silently, since declines are never timed.
    /// Keeping declines in their own family preserves it.
    /// </para>
    /// <para>
    /// It exists because without it the capture instruments reproduce, one gate
    /// higher, the very ambiguity they were added to remove: a deployment in
    /// which every capture is <i>declined</i> and one in which the capture path
    /// is never reached at all would both leave
    /// <see cref="LeafSnapshotCaptures"/> at zero. A declined capture is a third
    /// state, distinct from both a failed capture and an idle deployment, and it
    /// is the most likely way a self-heal silently never runs.
    /// </para>
    /// <para>
    /// The three reasons carry different operational meanings and are the reason
    /// a tag beats a bare count. <c>already_in_flight</c> dominating is a
    /// <b>contention</b> signal - captures are being requested faster than the
    /// shared snapshot storage provider retires them, which is the pressure
    /// issue #2696 describes. <c>not_eligible</c> dominating is the
    /// <b>starved-leaf</b> signal this diagnostic exists for: the leaf has
    /// nothing checkpointed and no live data, so it will never cover itself.
    /// <c>no_tree_id</c> above zero is a <b>bug</b> - capture was invoked on a
    /// leaf that was never attached to a tree. <c>no_coverage_claim</c> is the
    /// <b>unclaimable-rows</b> signal (issue #2725): the leaf holds live rows
    /// but has never checkpointed, so any blob it wrote would claim no coverage
    /// and be refused by the load gate. It is benign - WAL replay covers such a
    /// leaf - but it names the population whose retained WAL cannot shrink yet.
    /// </para>
    /// <para>
    /// A <c>no_tree_id</c> decline carries <b>no</b> tree or tenant tag, because
    /// at that point the leaf has no tree identity to report. That is a property
    /// of the state being counted, not an omission: read it as a global count,
    /// and do not expect it to appear under a per-tree filter.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotCaptureDeclines =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.capture.declines", unit: "{decline}",
            description: "Leaf-snapshot capture invocations that declined before the attempt boundary, tagged by tree and reason (no_tree_id, not_eligible, already_in_flight, no_coverage_claim). Separate from the capture counter so attempts and durations stay exactly co-populated.");

    /// <summary>Canonical name of <see cref="LeafSnapshotCaptureDeclines"/>.</summary>
    public const string LeafSnapshotCaptureDeclinesName = "orleans.lattice.leaf.snapshot.capture.declines";

    /// <summary>
    /// <see cref="TagReason"/> value for a capture declined because the leaf has
    /// no <c>TreeId</c>, so it was never attached to a tree. Above zero this is
    /// a bug, not a workload characteristic.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotDeclineNoTreeId =
        new(TagReason, "no_tree_id");

    /// <summary>
    /// <see cref="TagReason"/> value for a capture declined because no partition
    /// holds a checkpoint or live data. This is the starved-leaf signal: a leaf
    /// declining for this reason will never cover itself, so a sustained rate
    /// here is the population that snapshot coverage is failing to reach.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotDeclineNotEligible =
        new(TagReason, "not_eligible");

    /// <summary>
    /// <see cref="TagReason"/> value for a capture declined by the single-flight
    /// guard because an earlier capture's save is still in flight. A sustained
    /// rate here is a contention signal against the shared snapshot storage
    /// provider, not an error: the in-flight capture will land and the next
    /// advisory re-evaluates.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotDeclineAlreadyInFlight =
        new(TagReason, "already_in_flight");

    /// <summary>
    /// <see cref="TagReason"/> value for a capture declined because the blob it
    /// would write could carry no coverage claim, and so could never be loaded
    /// back (issue #2725). The leaf holds live rows but has never checkpointed
    /// any partition, so every per-partition slot the capture could stamp is the
    /// <c>-1</c> sentinel - exactly the shape
    /// <c>LeafSnapshotStorageGrain.HasCapturedPrefix</c> refuses.
    /// <para>
    /// This is the leaf-holds-unclaimable-rows signal, and it is distinct from
    /// <see cref="SnapshotDeclineNotEligible"/>: that one means the leaf has
    /// nothing at all, whereas this one means the leaf has data whose only
    /// durable copy is the WAL. It is not an error and needs no intervention -
    /// WAL replay covers such a leaf completely - but a sustained rate names the
    /// population whose retained WAL cannot shrink until it checkpoints.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> SnapshotDeclineNoCoverageClaim =
        new(TagReason, "no_coverage_claim");

    /// <summary>
    /// <see cref="TagReason"/> value for a graceful-deactivation capture declined
    /// by the #1535 no-loss gate while the leaf still holds STALE coverage, so
    /// the pin it leaves behind is frozen below its own checkpoint.
    /// <para>
    /// This is the MINTING RATE of the frozen floor-holder population, and it is
    /// the arm to read. <c>BPlusLeafGrain.TryCaptureSnapshotOnDeactivateAsync</c>
    /// captures only when this activation either advanced a checkpoint over
    /// cache-resident applies or cold-rebuilt its cache from the WAL start;
    /// a leaf satisfying neither cannot honestly stamp coverage, so declining is
    /// CORRECT and the frozen pin it leaves is an honest one. The decline is not
    /// the defect - the defect, if any, is the rate.
    /// </para>
    /// <para>
    /// Each such decline creates an obligation dischargeable only by the WAL GC
    /// reactivation drive, which replays the leaf and restamps it. That drive is
    /// bounded per sweep (<c>MaxReactivationTouchesPerPass</c>) and per consumer
    /// (<c>ReactivationRetryCooldown</c>), whereas this rate is bounded by
    /// nothing the collector controls. Read it AGAINST the drive's completion
    /// rate: sustained above it, the frozen population grows without bound and no
    /// drive-side tuning can converge, because the mismatch is asymptotic rather
    /// than a matter of constants (issue #3185).
    /// </para>
    /// <para>
    /// Distinguish from <see cref="DriverDeclineDeactivateCoverageCurrent"/>,
    /// which is the same gate declining harmlessly. Only THIS arm mints a frozen
    /// pin; counting both together overstates the refill.
    /// </para>
    /// <para>
    /// NOTE: unlike most arms in this file, the declines counter is NOT
    /// zero-primed, so this series is absent until it first fires. Absence here
    /// therefore does NOT establish that a build lacks the instrument - the usual
    /// "presence of a series is the deployment fact" reading does not apply.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> DriverDeclineDeactivateCoverageStale =
        new(TagReason, "deactivate_unproven_coverage_stale");

    /// <summary>
    /// <see cref="TagReason"/> value for a graceful-deactivation capture declined
    /// by the #1535 no-loss gate when the leaf's durable coverage ALREADY matches
    /// its checkpoint on every partition, so the decline costs nothing.
    /// <para>
    /// The benign majority, and the control for
    /// <see cref="DriverDeclineDeactivateCoverageStale"/>: it proves the gate is
    /// being reached and evaluated, so a zero on the stale arm means "no leaf
    /// minted a frozen pin" rather than "the gate never ran". Without this arm the
    /// two are indistinguishable.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> DriverDeclineDeactivateCoverageCurrent =
        new(TagReason, "deactivate_unproven_coverage_current");

    /// <summary>
    /// <see cref="TagReason"/> value for a graceful-deactivation capture declined
    /// by the #1535 no-loss gate whose coverage state could not be classified,
    /// because resolving the leaf's options or partition coverage threw.
    /// <para>
    /// Exists so the two informative arms stay honest rather than absorbing an
    /// unknown. Folding this case into
    /// <see cref="DriverDeclineDeactivateCoverageStale"/> would inflate the
    /// measured refill rate and send a reader after a phantom; folding it into
    /// <see cref="DriverDeclineDeactivateCoverageCurrent"/> would hide a real
    /// one. Expected to be zero outside a test harness with no grain runtime, so
    /// a sustained rate here is a bug in the classification, not a workload
    /// characteristic.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> DriverDeclineDeactivateUnclassified =
        new(TagReason, "deactivate_unproven_unclassified");

    /// <summary>
    /// <see cref="TagReason"/> value for the periodic recheck declining because a
    /// capture was already in flight on this leaf. A contention signal, not an
    /// error: the in-flight capture will land and the next cadence tick
    /// re-evaluates.
    /// </summary>
    public static readonly KeyValuePair<string, object?> DriverDeclineRecheckCaptureInFlight =
        new(TagReason, "recheck_capture_in_flight");

    /// <summary>
    /// <see cref="TagReason"/> value for the periodic recheck declining because
    /// every partition's durable coverage already matches its checkpoint. The
    /// healthy majority, and the control that makes a zero on the other arms
    /// readable as "nothing needed doing" rather than "the recheck never ran".
    /// </summary>
    public static readonly KeyValuePair<string, object?> DriverDeclineRecheckCoverageCurrent =
        new(TagReason, "recheck_coverage_current");

    /// <summary>
    /// <see cref="TagReason"/> value for the periodic recheck declining because
    /// the persist-driven cadence has not yet reached
    /// <c>LeafSnapshotReClassifyEveryNCheckpoints</c>.
    /// <para>
    /// The expected steady-state arm on a write-serving leaf, and the one that
    /// distinguishes a recheck path that is ticking normally from one that is not
    /// being reached at all. A leaf serving only READS never advances this
    /// cadence, so a leaf whose only driver-decline arm is this one, at a rate
    /// that does not track its write volume, is the read-only population the
    /// coverage-lag timer exists to cover.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> DriverDeclineRecheckCadenceNotReached =
        new(TagReason, "recheck_cadence_not_reached");

    /// <summary>
    /// <see cref="TagReason"/> value for the coverage-lag timer finding this
    /// leaf holding rows with NO durable checkpoint on any partition, and
    /// routing it to the starvation drive rather than declining (issue #3300).
    /// <para>
    /// Split out of <see cref="DriverDeclineRecheckCoverageCurrent"/>, and the
    /// split is the point. That arm means "every partition's coverage already
    /// matches its checkpoint", which on a never-checkpointed leaf is
    /// vacuously true - <c>-1 &gt; -1</c> is false - so the starved leaf was
    /// counted on the one arm documented as the healthy majority. The two
    /// states are opposite in consequence and were indistinguishable in
    /// telemetry: a tree that had never made anything durable reported the
    /// same arm as a tree that had made everything durable.
    /// </para>
    /// <para>
    /// A non-zero rate here is not itself a fault - it is the timer doing its
    /// job on a leaf that has not yet replayed. A rate that does not fall
    /// toward zero is, because it means the drive it routes to is not
    /// supplying a checkpoint.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> DriverDeclineRecheckNoDurableCheckpoint =
        new(TagReason, "recheck_no_durable_checkpoint");

    /// <summary>
    /// Counter of leaf snapshot capture DRIVERS that declined to drive a capture,
    /// tagged <see cref="TagTree"/> and <see cref="TagReason"/> (issue #3185).
    /// <para>
    /// Deliberately a SEPARATE instrument from
    /// <see cref="LeafSnapshotCaptureDeclines"/>, and the separation is
    /// load-bearing in the same way that counter's own separation from
    /// <c>leaf.snapshot.captures</c> is. Those declines are raised INSIDE
    /// <c>CaptureSnapshotCoreAsync</c>, so declines and attempts together
    /// partition its invocations exactly and stay co-populated with
    /// <c>leaf.snapshot.capture.duration</c>. The declines counted HERE happen in
    /// a caller that never reached that method, so folding them in would count a
    /// strict superset and silently corrupt the decline-versus-attempt ratio the
    /// other counter exists to report.
    /// </para>
    /// <para>
    /// It exists because the capture drivers were the last unlit segment of the
    /// snapshot-coverage path: the zero-coverage repair reports six outcomes and
    /// <c>CaptureSnapshotCoreAsync</c> reports four declines, but the
    /// graceful-deactivation hook and the periodic recheck returned in complete
    /// silence on every gate. That silence is why the frozen floor-holder
    /// population reads as a STATIC POPULATION rather than as the FLOW it is:
    /// only its accumulated consequence was ever observable, never its rate.
    /// </para>
    /// <para>
    /// <b>Reading it.</b> The arm that matters is
    /// <see cref="DriverDeclineDeactivateCoverageStale"/>, the minting rate of
    /// frozen floor-holding pins. Read it against the WAL GC reactivation drive's
    /// completion rate: sustained above it, the frozen population grows without
    /// bound and no drive-side tuning can converge, because the mismatch is
    /// asymptotic rather than a matter of constants. Below it, the drive is merely
    /// slow and tuning is applicable.
    /// </para>
    /// <para>
    /// NOT zero-primed, matching <see cref="LeafSnapshotCaptureDeclines"/>: a
    /// series appears on first decline. Absence therefore does NOT establish that
    /// a build lacks the instrument, so the usual "presence of a series is the
    /// deployment fact" reading does not apply to this family.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotDriverDeclines =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.driver.declines", unit: "{decline}",
            description: "Leaf snapshot capture DRIVERS that declined to drive a capture, tagged by tree and reason. Separate from leaf.snapshot.capture.declines, which counts declines inside CaptureSnapshotCoreAsync and must stay exactly co-populated with the capture attempt and duration instruments.");

    /// <summary>Canonical name of <see cref="LeafSnapshotDriverDeclines"/>.</summary>
    public const string LeafSnapshotDriverDeclinesName = "orleans.lattice.leaf.snapshot.driver.declines";

    /// <summary>
    /// Counter of graceful-deactivation DURABILITY BARRIERS that faulted,
    /// tagged with <see cref="TagTree"/>, <see cref="TagReason"/> (which barrier)
    /// and the tenant dimension.
    /// <para>
    /// <b>Why this exists (issue #3366).</b> The deactivation hook runs four
    /// durability barriers in order - the digest publish, the projection
    /// checkpoint flush, the deactivate-time snapshot capture, and the durable
    /// materialiser frontier pin. They used to share one <c>try</c> and one
    /// ANONYMOUS bare <c>catch</c> with no logger, so a fault in an early
    /// barrier silently cancelled every later one and emitted nothing at all.
    /// Because the snapshot capture's own decline instrument
    /// (<see cref="LeafSnapshotDriverDeclines"/>) is raised INSIDE that capture,
    /// a fault before it produced neither a capture, nor a decline, nor a log
    /// line - three distinct failures rendered as byte-identical silence, with
    /// three different remedies.
    /// </para>
    /// <para>
    /// Each barrier is now contained independently, so this counter names
    /// exactly which one faulted and the later barriers still run. Read a
    /// non-zero value as a durability barrier that did NOT complete for that
    /// tree: the checkpoint-flush arm in particular means an activation's
    /// projection progress was not banked.
    /// </para>
    /// <para>
    /// NOT zero-primed, matching the rest of this family: a series appears on
    /// first fault. Absence therefore does not establish that the build carries
    /// the instrument, only that no fault was recorded.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafDeactivationBarrierFailures =
        Meter.CreateCounter<long>("orleans.lattice.leaf.deactivation.barrier.failures", unit: "{failure}",
            description: "Graceful-deactivation durability barriers that faulted, tagged by tree and barrier (digest_publish, checkpoint_flush, snapshot_capture, frontier_pin). Each barrier is contained independently, so a fault in one no longer cancels the barriers after it.");

    /// <summary>Canonical name of <see cref="LeafDeactivationBarrierFailures"/>.</summary>
    public const string LeafDeactivationBarrierFailuresName = "orleans.lattice.leaf.deactivation.barrier.failures";

    /// <summary>
    /// <see cref="TagReason"/> value for the coalesced projection-digest publish
    /// barrier. Benign in isolation - the digest is staleness-tolerant and the
    /// next mutation republishes it.
    /// </summary>
    public static readonly KeyValuePair<string, object?> DeactivationBarrierDigestPublish =
        new(TagReason, "digest_publish");

    /// <summary>
    /// <see cref="TagReason"/> value for the projection-checkpoint flush barrier.
    /// Above zero this is a DURABILITY fault: the activation's checkpoint
    /// progress was not banked, so the next activation replays from an older
    /// offset and any progress the WAL no longer carries is lost.
    /// </summary>
    public static readonly KeyValuePair<string, object?> DeactivationBarrierCheckpointFlush =
        new(TagReason, "checkpoint_flush");

    /// <summary>
    /// <see cref="TagReason"/> value for the deactivate-time snapshot capture
    /// barrier (the issue #1537 liveness barrier).
    /// </summary>
    public static readonly KeyValuePair<string, object?> DeactivationBarrierSnapshotCapture =
        new(TagReason, "snapshot_capture");

    /// <summary>
    /// <see cref="TagReason"/> value for the durable materialiser frontier pin
    /// barrier. Above zero the leaf may leave a pin below its own checkpoint,
    /// which retains the tree's shared WAL.
    /// </summary>
    public static readonly KeyValuePair<string, object?> DeactivationBarrierFrontierPin =
        new(TagReason, "frontier_pin");

    /// <summary>
    /// Counter of zero-coverage leaf snapshot repair EVALUATIONS (issues #2692,
    /// #2940), tagged with <see cref="TagTree"/> and <see cref="TagOutcome"/>.
    /// Six arms partition every invocation of
    /// <c>BPlusLeafGrain.TryRepairZeroCoverageAsync</c>:
    /// <list type="bullet">
    /// <item><description><see cref="CoverageRepairRepaired"/> - a capture gave
    /// every checkpointed partition the durable coverage it lacked.</description></item>
    /// <item><description><see cref="CoverageRepairUnsatisfied"/> - a capture ran
    /// and a checkpointed partition is STILL uncovered.</description></item>
    /// <item><description><see cref="CoverageRepairExhausted"/> - the
    /// attempt budget was spent with a partition still uncovered. Issue #3195
    /// re-arms that budget in place after a doubling backoff, so this is once
    /// per backoff cycle and NOT, as it was originally, once per activation.</description></item>
    /// <item><description><see cref="CoverageRepairCaptureInFlight"/> - declined
    /// because a capture was already running on this leaf.</description></item>
    /// <item><description><see cref="CoverageRepairNoUncoveredPartition"/> -
    /// declined because no checkpointed partition lacks coverage. The healthy
    /// majority, and the only positive observation that a leaf is NOT in the
    /// repairable population.</description></item>
    /// <item><description><see cref="CoverageRepairBackingOff"/> - suppressed
    /// because an earlier exhaustion armed a re-arm backoff that has not yet
    /// elapsed. This arm sits BELOW both entry guards, so before issue #3194
    /// armed it the invocation returned recording nothing at all.</description></item>
    /// </list>
    /// <para>
    /// <b>Why the arms share one instrument.</b> A counter publishes no series at
    /// all until its first <c>Add</c>, so a dedicated exhaustion counter would sit
    /// dark in the healthy case and read identically to an instrument that was
    /// never wired up. Sharing one instrument means any repair traffic whatsoever
    /// proves the series is live. That argument was load-bearing but CONDITIONAL -
    /// it held only for a tree that had already emitted some arm - so issue #2940
    /// made it unconditional by zero-priming all seven arms the first time a tree
    /// is seen in this process. An absent series now means the repair path never
    /// ran for that tree (or the build predates the instrument); a zero is a
    /// measured zero.
    /// </para>
    /// <para>
    /// <b>The two rejection arms are why this instrument was widened.</b> Before
    /// issue #2940 the guard exit and the attempted-but-unsatisfied exit both
    /// recorded NOTHING, so "the repair never ran" and "the repair ran and failed"
    /// were byte-identical silence - and they have opposite remedies, the first at
    /// the pin/guard seam and the second at the capture seam.
    /// </para>
    /// <para>
    /// <c>exhausted</c> is the alarm condition and is never benign. A
    /// checkpointed partition with no durable snapshot coverage resolves its
    /// durable materialiser pin to the Zero block value, which disables
    /// cursor-based WAL trimming for the leaf's ENTIRE tree - one such leaf
    /// retains every other leaf's WAL without bound. The leaf identity is
    /// carried on the paired warning rather than as a tag, because the leaf
    /// population is unbounded and would be an unbounded metric dimension.
    /// </para>
    /// <para>
    /// <b>Boundary on the partition claim.</b> Every invocation records exactly
    /// one of the six terminal arms, with no exception, and the sum across those
    /// six is therefore an EXACT invocation count rather than a lower bound. The
    /// fixtures pin it as an equality (41 evaluations, 41 increments) so that a
    /// new silent return added below the entry guards fails the build instead of
    /// reappearing as a quiet under-count.
    /// </para>
    /// <para>
    /// This previously claimed a lower bound, and attributed the shortfall to the
    /// once-per-activation exhaustion dedup in
    /// <c>ReportZeroCoverageRepairExhaustion</c>. That attribution was wrong as
    /// well as incomplete: the suppressed invocations never reached that latch at
    /// all - they took the backoff-suppression branch, which recorded nothing
    /// until issue #3194 armed it as <see cref="CoverageRepairBackingOff"/>. The
    /// latch is in fact unreachable in the present flow, because its one call site
    /// is guarded by a null re-arm deadline that the same branch sets before
    /// calling it, and only the re-arm clears it, in the same step. It is retained
    /// as a defence because that reachability argument is a property of the
    /// call-site guards rather than of the latch.
    /// </para>
    /// <para>
    /// <c>rearmed</c> is a lifecycle transition rather than a terminal outcome and
    /// is NOT one of the six: it co-occurs with a terminal arm instead of
    /// excluding one, so summing all seven over-counts. Sum the six terminal arms
    /// for an invocation tally and read <c>rearmed</c> separately as an event.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafSnapshotCoverageRepairs =
        Meter.CreateCounter<long>("orleans.lattice.leaf.snapshot.coverage_repairs", unit: "{evaluation}",
            description: "Zero-coverage leaf snapshot repair evaluations (issues #2692, #2940), tagged by tree and outcome: 'repaired' (capture left every checkpointed partition covered), 'unsatisfied' (capture ran, a checkpointed partition is still uncovered - this includes a capture that THREW or timed out, because the advisory capture wrapper swallows every exception, so a leaf whose snapshot store is failing is counted here rather than going silent), 'exhausted' (attempt budget spent with a partition still uncovered, recorded once per backoff cycle rather than once per activation), 'capture_in_flight' (declined, a capture was already running), 'no_checkpointed_uncovered_partition' (declined, nothing to repair - the healthy majority; recorded once per activation and once per checkpoint persist, so its magnitude tracks write volume rather than severity, and it is NOT comparable with 'orleans.lattice.wal.gc.blocking_pin_state' on that instrument's blocked arm, whose population of consumers with no live cursor is disjoint from this one by construction; on that instrument's floor-holder arm the two DO intersect by design, because issue #3164 drives the pins it classifies 'checkpointed_uncovered' into this very repair, and a 'checkpointed_uncovered' classification standing against a climbing 'no_checkpointed_uncovered_partition' for the same leaf is a direct contradiction rather than two unrelated readings - issue #3168, in which the leaf was right) and 'backing_off' (suppressed, an earlier exhaustion armed a re-arm backoff that has not yet elapsed; this arm sits below both entry guards, so before issue #3194 armed it the invocation recorded nothing at all). Those six terminal arms partition every invocation; the seventh tag value 'rearmed' is a lifecycle transition that CO-OCCURS with a terminal arm, so sum the six for an invocation tally and read 'rearmed' separately. All seven arms are zero-primed the first time a tree is seen in this process, so an absent series means the path never ran for that tree and a zero is a measured zero. Every invocation records exactly one of the six terminal arms with no exception, so their sum is an EXACT invocation count rather than a lower bound; the earlier claim that repeat exhaustions were deduplicated away misattributed a shortfall that was in fact the unarmed backoff branch.");

    /// <summary>Canonical name of <see cref="LeafSnapshotCoverageRepairs"/>.</summary>
    public const string LeafSnapshotCoverageRepairsName = "orleans.lattice.leaf.snapshot.coverage_repairs";

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="LeafSnapshotCoverageRepairs"/> for a repair capture after
    /// which every checkpointed partition holds durable snapshot coverage, so
    /// the leaf no longer blocks its tree's cursor trim.
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageRepairRepaired =
        new(TagOutcome, "repaired");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="LeafSnapshotCoverageRepairs"/> for an activation that spent
    /// its entire per-activation repair budget with a checkpointed partition
    /// still uncovered. Emitted once per activation at the moment the budget is
    /// spent, not once per attempt, so the series counts stuck activations
    /// rather than retries.
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageRepairExhausted =
        new(TagOutcome, "exhausted");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="LeafSnapshotCoverageRepairs"/> for a spent repair budget that
    /// has been re-armed after its backoff, returning the repair to service on
    /// an activation that is still alive.
    /// <para>
    /// Paired with <see cref="CoverageRepairExhausted"/>, and the pair is the
    /// series to read together: an <c>exhausted</c> count that keeps pace with
    /// <c>rearmed</c> is a leaf retrying on its backoff, while an
    /// <c>exhausted</c> count with no matching <c>rearmed</c> is a leaf whose
    /// activation was replaced before the backoff elapsed. The same shape as
    /// <c>blocked_leaf_reactivations_total</c>'s abandoned/rearmed pair.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageRepairRearmed =
        new(TagOutcome, "rearmed");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="LeafSnapshotCoverageRepairs"/> for a repair capture that RAN
    /// and left a checkpointed partition still uncovered, without yet spending
    /// the per-activation budget (issue #2940).
    /// <para>
    /// This arm is the discriminator the instrument previously lacked. It says
    /// the repair is REACHING the fault and failing, so the remedy belongs at the
    /// capture seam - the snapshot store, the eligibility gate inside
    /// <c>CaptureSnapshotCoreAsync</c>, or the coverage stamp - and specifically
    /// NOT at the pin/guard seam that
    /// <see cref="CoverageRepairNoUncoveredPartition"/> points at. Before it
    /// existed this exit recorded nothing at all, so it was indistinguishable
    /// from a repair that never ran.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageRepairUnsatisfied =
        new(TagOutcome, "unsatisfied");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="LeafSnapshotCoverageRepairs"/> for an evaluation declined
    /// because a snapshot capture was already in flight on this leaf (issue
    /// #2940).
    /// <para>
    /// The first conjunct of the entry guard, and it short-circuits: when this
    /// arm is recorded the uncovered-partition predicate was NOT evaluated, so
    /// this reading makes no claim about whether the leaf is repairable. A
    /// sustained rate means repair evaluations are colliding with captures, which
    /// burns no attempt budget but does defer the repair to a later driver.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageRepairCaptureInFlight =
        new(TagOutcome, "capture_in_flight");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="LeafSnapshotCoverageRepairs"/> for an evaluation declined
    /// because no checkpointed partition on this leaf lacks durable snapshot
    /// coverage (issue #2940).
    /// <para>
    /// The healthy majority - every leaf that is not in the repairable
    /// population records this on every activation and every checkpoint persist -
    /// and it is deliberately recorded rather than left silent. It is the only
    /// POSITIVE observation that a leaf blocking its tree's WAL cursor floor is
    /// doing so for a reason this repair cannot address, which routes the remedy
    /// to the pin/guard seam rather than to the capture seam. Without it, "the
    /// repair guard never passed" and "the repair ran and failed" were the same
    /// silence.
    /// </para>
    /// <para>
    /// <b>Its magnitude is a measure of write volume, not of severity, and it is
    /// not comparable with the blocking-pin classifier's counts</b> (issue
    /// #3157). This arm is recorded once per activation and once per checkpoint
    /// persist, so on a busy tree it scales with leaf writes: measured on a live
    /// estate, one tree recorded 27683 declines against 27735 leaf writes, a
    /// 0.19% difference. Because WAL size is likewise downstream of write
    /// volume, the decline count co-ranks with retained WAL across trees, and
    /// reading that correlation as evidence that the repair is failing on the
    /// worst trees is a confound rather than a finding.
    /// </para>
    /// <para>
    /// <b>It shares a population with <see cref="WalGcBlockingPinState"/> on
    /// one arm, and the overlap is deliberate.</b> The original claim here was
    /// that the two sets are disjoint by construction: this repair runs only
    /// inside a live leaf activation, while that classifier examines consumers
    /// with NO live cursor, gated by the registry-presence skip in
    /// <c>LatticeWalGc.ApplyDurableMaterialiserFloorAsync</c>. That held while
    /// the classifier had a single call site. It no longer does. The
    /// floor-holder sample added by issue #3158 walks <i>every</i> durable pin
    /// on the tree with no registry-presence filter at all, and issue #3164
    /// then drives the pins it classifies <c>checkpointed_uncovered</c> into
    /// this very repair - so on that path the classifier's output is precisely
    /// what causes this instrument to record. The two now intersect by design,
    /// and counts from them are comparable there.
    /// </para>
    /// <para>
    /// That intersection is what made issue #3168 legible. The classifier read
    /// all eight partitions of one leaf as <c>checkpointed_uncovered</c> while
    /// this instrument recorded <c>no_checkpointed_uncovered_partition</c> for
    /// the same leaf over four thousand times. Under the old disjointness claim
    /// that would have been two unrelated readings; under the real topology it
    /// was a direct contradiction, and the leaf was right. The classifier had
    /// inferred the "uncovered" half rather than measuring it, on an arm whose
    /// premise does not support the inference - see
    /// <see cref="WalGcBlockingPinState.CheckpointedCoverageUnknown"/>.
    /// </para>
    /// <para>
    /// Away from that path they remain separate populations, and the original
    /// caution still applies there: a large decline count alongside a small
    /// <c>checkpointed_uncovered</c> count is not a contradiction, because the
    /// blocked arm's declines are other, live, healthy leaves this repair
    /// examined and that classifier never saw.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageRepairNoUncoveredPartition =
        new(TagOutcome, "no_checkpointed_uncovered_partition");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="LeafSnapshotCoverageRepairs"/> for an evaluation that reached
    /// the repairable population but was suppressed because a re-arm backoff
    /// from an earlier budget exhaustion is still outstanding (issue #3194).
    /// <para>
    /// <b>Why this arm exists at all.</b> This exit previously returned in
    /// silence, and it sits BELOW both entry guards, so it was a genuine
    /// repairable-population invocation that no arm covered. That made the
    /// documented partition false and, worse, made the documented lower bound
    /// useless on exactly the leaves this repair is working hardest on: against
    /// one <see cref="CoverageRepairExhausted"/> and one
    /// <see cref="CoverageRepairRearmed"/> per cycle, a 300-second recheck
    /// cadence puts roughly six silent invocations inside a 30-minute backoff
    /// and roughly ninety-six inside the eight-hour ceiling, so the recorded
    /// arms understated the real invocation count by one to two orders of
    /// magnitude while reading as though they were complete.
    /// </para>
    /// <para>
    /// <b>What it buys beyond restoring the partition.</b> Its rate is the rate
    /// at which the coverage-lag timer spins against leaves it has already
    /// abandoned, which is the direct cost signal for tuning
    /// <c>LeafSnapshotMaxCoverageLagSeconds</c> against the backoff ceiling.
    /// That cost was previously unobservable: the timer fired, did nothing, and
    /// recorded nothing.
    /// </para>
    /// <para>
    /// It is terminal and mutually exclusive with every other terminal arm - the
    /// branch records this and returns, reaching no capture and no other
    /// recording site - which is what lets the terminal arms be read as a
    /// partition rather than as a set that happens to include it.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageRepairBackingOff =
        new(TagOutcome, "backing_off");

    /// <summary>
    /// Every <c>outcome</c> arm of <see cref="LeafSnapshotCoverageRepairs"/>, and
    /// the single source the zero-priming walk iterates.
    /// <para>
    /// This exists so that the instrument's "a zero is a MEASURED zero" claim is
    /// earned rather than asserted. Priming used to be a hand-written run of
    /// <c>Add(0, ...)</c> calls, one per arm, with nothing relating it to the
    /// arms that actually exist: an arm added later and omitted from that run
    /// would publish no zero, and its absence would read as "the path never ran"
    /// on precisely the tree under diagnosis. Issue #3194 is the proof that the
    /// hazard is real rather than theoretical - adding <c>rearmed</c> required a
    /// sixth priming line written by hand, and nothing in the build would have
    /// noticed had it been left out.
    /// </para>
    /// <para>
    /// The sibling <c>blocked_leaf_reactivations_total</c> earns the same claim
    /// by walking an enum through a switch that throws on an unmapped member.
    /// These arms are <see cref="KeyValuePair{TKey,TValue}"/> statics rather than
    /// an enum, so the equivalent guarantee is supplied by a reflection test that
    /// asserts this array holds every <c>CoverageRepair*</c> arm declared on this
    /// class. Add an arm and forget this array, and that test fails.
    /// </para>
    /// <para>
    /// Be exact about where that stops, rather than importing the sibling's
    /// stronger claim. What is closed is the declared-arm case: a new arm
    /// declared the way every existing arm is declared cannot ship unprimed. A
    /// recording site that inlined a tag pair instead of declaring a static
    /// would evade both this array and the reflection test, so the sibling's
    /// "an arm added later CANNOT ship unarmed" is true of an enum walked
    /// through a throwing switch and is NOT true here. The guarantee is
    /// enforced by a test rather than by the compiler.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?>[] CoverageRepairArms =
    [
        CoverageRepairRepaired,
        CoverageRepairUnsatisfied,
        CoverageRepairExhausted,
        CoverageRepairCaptureInFlight,
        CoverageRepairNoUncoveredPartition,
        CoverageRepairBackingOff,
        CoverageRepairRearmed,
    ];

    /// <summary>
    /// Reactivations of a dormant leaf whose unusable durable materialiser pin
    /// was blocking its tree's WAL cursor floor (issue #2710 Limitation 2),
    /// tagged by tree and outcome
    /// (<c>attempted</c>/<c>healed</c>/<c>abandoned</c>).
    /// <para>
    /// All ten outcomes share one instrument so that a zero on
    /// <c>healed</c> is a published series rather than an absent one. That
    /// distinction is load-bearing here: a sweep that reactivates a leaf and
    /// moves on cannot tell "the pin lifted" from "the capture failed again",
    /// and a deployment can sit indefinitely in the second state while the
    /// first is what the sweep was built to produce. Counting only successes
    /// would make those two indistinguishable from outside the process, which
    /// is the same ambiguity that hid the defect this sweep exists to clear.
    /// Note the limit of what a zero buys: it separates a published arm from an
    /// absent one, and does not establish that the arm's recording path is
    /// reachable (issue #2942).
    /// </para>
    /// <para>
    /// <c>abandoned</c> is the alarm condition. It means a leaf stayed blocked
    /// across every permitted attempt of a cycle, so the block is not one
    /// activation away from clearing and something downstream of the touch is
    /// failing - a capture that cannot complete, for instance. The leaf identity
    /// is carried on the paired warning rather than as a tag, because the leaf
    /// population is unbounded and would be an unbounded metric dimension.
    /// </para>
    /// <para>
    /// <c>rearmed</c> is abandonment's counterweight (issue #2783): the budget
    /// was restored after a backoff, because the conditions that make
    /// reactivation futile - memory pressure, replay saturation, an ingest burst
    /// - are transient and a permanent stop outlives them. A tree that never
    /// unblocks therefore shows <c>abandoned</c> and <c>rearmed</c> advancing
    /// together at an ever-slower rate, which is the intended shape; it is
    /// <c>abandoned</c> advancing while <c>rearmed</c> stays flat that means the
    /// sweep has genuinely stopped.
    /// </para>
    /// <para>
    /// Every arm is zero-primed per tree on each GC pass, above every early
    /// return, so an absent series means the scheduler never evaluated that
    /// tree - a statement about the build rather than about the events.
    /// </para>
    /// <para>
    /// A flat zero is weaker than it looks. Two independent conditions must
    /// hold before it can be read as "this outcome did not occur", and
    /// <b>priming establishes neither of them</b>.
    /// </para>
    /// <para>
    /// The first is <b>exhaustive arming</b>, and it was once false here (issue
    /// #2938). The reading holds only while every member of the terminal
    /// outcome enum has an arm. Priming proves a detector exists for the arms
    /// that have one and says nothing whatever about an outcome with no arm at
    /// all, whose structural zero is indistinguishable from a measured one and
    /// passes every priming audit. Three of the four terminal outcomes were
    /// unarmed while this very paragraph promised the reader a measured zero,
    /// and the uncounted outcome fired 234 times in a single window. It is now
    /// enforced by
    /// <c>ReactivationOutcomeTag_arms_every_declared_terminal_outcome</c> and,
    /// at runtime, by the mapping itself, which throws on an unmapped member
    /// rather than folding it into a neighbouring bucket.
    /// </para>
    /// <para>
    /// The second is <b>reachability of the recording path</b> (issue #2942). A
    /// primed series proves the priming path ran; it does not prove that the
    /// code incrementing that arm can ever be reached. An instrument that is
    /// registered, correctly primed and frozen at zero forever is
    /// indistinguishable - on a scrape, and on every enrolment, hygiene,
    /// ordering and doc-coverage gate - from one that is correct and merely
    /// quiet. Only a fixture that drives the scheduler into the outcome and
    /// observes the arm advance separates them. That is what
    /// <c>ExecuteAsync_records_each_terminal_outcome_on_its_own_arm_and_no_other</c>
    /// does for all four terminal arms, as a 4x4 identity matrix: each arm is
    /// shown to advance on its own outcome and to stay at zero on the other
    /// three, so every zero it reports is an earned one.
    /// </para>
    /// <para>
    /// The <b>drive verdict</b> arms (issue #2692 Half B) are held to both
    /// conditions by construction rather than by promise, because they were
    /// added one commit after the two above were established and there was no
    /// reason to re-earn them the slow way. Arming is exhaustive because
    /// priming walks <c>LeafStarvationDriveOutcome</c> itself and
    /// <c>DriveOutcomeTag</c> throws on an unmapped member, gated by
    /// <c>DriveOutcomeTag_arms_every_declared_starvation_drive_verdict</c>.
    /// Reachability is established by
    /// <c>ExecuteAsync_records_each_drive_verdict_on_its_own_arm_and_no_other</c>
    /// as a 6x6 identity matrix on the same principle as the 4x4 above.
    /// </para>
    /// <para>
    /// The check is deliberately one-directional - every terminal outcome must
    /// have an arm, not every arm must be a terminal outcome - because
    /// <c>attempted</c>, <c>healed</c>, <c>abandoned</c> and <c>rearmed</c> are
    /// lifecycle events rather than members of that enum, as are the six drive
    /// verdicts, which belong to a different enum again. A symmetric check would
    /// reject ten legitimate arms. The accepted cost is that the four
    /// lifecycle arms are unguarded in both directions: were <c>healed</c>
    /// dropped from the recording path, no fixture here would catch it. The
    /// general form that would cover them without re-introducing that arity
    /// mismatch is tracked as issue #2939.
    /// </para>
    /// <para>
    /// <b><c>drove_timed_out</c> (issue #3065) is armed here but is not the
    /// instrument to read for the fault it names.</b> It records only when the
    /// scheduler's touch outlives the drive, and the touch abandons at the
    /// Orleans response deadline while the drive's budget is far longer - so in
    /// the wedged case this arm is structurally silent and the grain-side
    /// <see cref="WalReplayStarvationDriveAbandonments"/> is the discriminator.
    /// It is a first-class arm regardless, because folding an abandoned drive
    /// into <c>drove_no_advance</c> would report a timeout as an ordinary
    /// no-op, which is the misfiling the enum's own contract forbids.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcBlockedLeafReactivations =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.blocked_leaf_reactivations", unit: "{reactivation}",
            description: "Reactivations of a dormant leaf whose unusable durable materialiser pin blocked its tree's WAL cursor floor (issue #2710), tagged by tree and outcome. Three disjoint groups of arms share this instrument. The lifecycle arms (attempted/healed/abandoned/rearmed) count what the sweep did. The terminal arms (completed/unresolvable/faulted/undelivered) are the per-touch outcome and partition 'attempted' exactly once each, so they sum to it. The drive-verdict arms (drove_lifted/drove_no_advance/drove_memory_refused/drove_not_driven/drove_already_driving/drove_timed_out, issues #2692 and #3065) are what came of driving a starved leaf's replay forward. All fourteen are zero-primed once per tree per process, latched on the tree's first collection rather than repeated per pass. Read a zero on a terminal or drive arm as measured: both groups are gated for exhaustive arming and each arm is proven to advance by its own positive control (issues #2938, #2942, #2692). Priming alone would not license that reading, since a primed arm whose recording path is unreachable is frozen at zero and looks identical to a quiet one.");

    /// <summary>Canonical name of <see cref="WalGcBlockedLeafReactivations"/>.</summary>
    public const string WalGcBlockedLeafReactivationsName = "orleans.lattice.wal.gc.blocked_leaf_reactivations";

    /// <summary>
    /// Which durable-pin state each <i>absent</i> consumer blocking a WAL GC
    /// pass is in, tagged by tree, partition and
    /// <see cref="WalGcBlockingPinState"/> (issue #3042).
    /// <para>
    /// <c>blocked</c> on <see cref="WalGcPasses"/> says a tree cannot reclaim;
    /// it cannot say whether that is a defect or correct behaviour, because the
    /// leaf publishes the same unusable pin on both routes in. This instrument
    /// resolves that from the leaf's persisted projection checkpoint, read
    /// directly from the storage provider <i>without activating the leaf</i> -
    /// the blocking population is exactly the population that cannot be
    /// activated.
    /// </para>
    /// <para>
    /// Every arm is zero-primed, and primed twice over. Each tree mints all
    /// four arms under the reserved partition value
    /// <see cref="PartitionNone"/> on the first pass that collects it, above
    /// every early return, so an absent series means the classifier is not
    /// running on this silo rather than that nothing was classified. Each classified
    /// <c>(tree, partition)</c> then mints all four of its own arms before
    /// recording the one it resolved, so a zero on a state reads as
    /// measured-and-not-this-state rather than as silence. Absence on this
    /// instrument has been read as evidence three times on the epic that
    /// produced it, and on each occasion "no series" and "never ran" were
    /// byte-identical.
    /// </para>
    /// <para>
    /// <b>It is a cumulative tally of classification events, not a population.</b>
    /// A consumer classified again in a later episode increments it again, so
    /// the total only ever grows and can never answer "how many leaves are
    /// blocked now". On a live estate it was read as a population and quoted as
    /// a blocked-pair count; it climbed from 281 to 337 on a process that never
    /// restarted, which a population on a <c>_total</c> cannot do (issue #3175).
    /// Both inputs are capped as well - the blocked arm at
    /// <c>LatticeWalGc.MaxReportedBlockingConsumers</c>, the floor-holder arm at
    /// the sweep's own classification cap - so even as a tally it is a lower
    /// bound on what was blocking, and nothing in the estate counts distinct
    /// blocked leaves per tree.
    /// </para>
    /// <para>
    /// Diagnostic only - it never changes what a pass is allowed to trim.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcBlockingPinStates =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.blocking_pin_state", unit: "{consumer}",
            description: "Durable-pin state of each absent consumer blocking a WAL GC pass (issue #3042), tagged by tree, partition and status. 'checkpointed_uncovered' is repairable: the leaf durably checkpointed the partition but published an unusable pin because snapshot coverage is absent. The checkpoint half is read through the same guarded accessor the leaf resolves its own pin from, so an unassigned born-0 scalar on partition 0 reads as 'never_checkpointed' rather than being misreported as repairable (issues #2703, #3157). 'never_checkpointed' is correct by design and has no repair: the leaf holds live data it has never checkpointed, so there is no WAL offset it could honestly claim. 'no_durable_state' is a fourth thing and not a flavour of either: the provider answered and reported nothing ever persisted for that leaf. 'unreadable' reports a failure of the classifier itself - an unparseable consumer id, no storage provider on this silo, or a read that threw - and is kept separate so a defect in the measurement is never rendered as a finding about the system. 'orphaned' is the leaf's durable state existing but carrying no bound tree id, so the leaf was reclaimed or purged after publishing the pin (issue #3105); it is the only arm that is actionable without driving anything, and before it existed such a pin classified as 'checkpointed_uncovered' because the checkpoint classifier never read the tree id. 'checkpointed_coverage_unknown' reads the same durable byte as 'checkpointed_uncovered' and differs only in whether the premise licensing the 'uncovered' half was available: that half is inferred and never measured, because coverage is per-activation in-memory state no storage read can reach, and what normally licenses it is the pin being independently known unusable. The blocked arm has that premise by construction; the floor-holder sample (issue #3158) does not, because it runs only when the cursor floor reports usable and samples by lowest durable offset rather than by usability, so a usable pin sampled there is reported under this arm and is NOT driven into the coverage repair (issue #3168). It is driven for a different reason: a pin on this arm whose durable offset equals the tree offset floor is reactivated for LIVENESS, because a scanned-through projection checkpoint advances only during replay and therefore freezes when its leaf deactivates, holding the floor indefinitely on a converged corpus where nothing reactivates it (issue #3178). Since issue #3310 a pin on this arm ABOVE the floor is also driven, but only once a pin at the floor has been admitted on the same sweep and only within the tree's remedy candidate budget: those pins are the next floors in the order they will become the floor, so driving them is prefetch rather than the waste issue #3168 measured - whereas if the floor's own holder is inadmissible the level never drains, nothing above it is ever in the way, and nothing is driven. Classified by a direct storage-provider read that never activates the leaf, once per consumer per blocked episode. It is a cumulative tally of classification events and NOT a population: a consumer classified again in a later episode increments it again, so the total only ever grows and cannot answer how many leaves are blocked now - read as a population on a live estate it climbed from 281 to 337 on a process that never restarted, which a population on a counter cannot do (issue #3175). Both inputs are capped too, the blocked arm at LatticeWalGc.MaxReportedBlockingConsumers and the floor-holder arm at the sweep's classification cap, so even as a tally it is a lower bound on what was blocking; nothing in the estate counts distinct blocked leaves per tree. Its population is disjoint from 'orleans.lattice.leaf.snapshot.coverage_repairs' on the blocked arm by construction - that repair only ever runs inside a live leaf activation, while a consumer holding a live cursor is skipped before its pin is read there - but the two DO intersect on the floor-holder arm, which applies no registry-presence filter and whose 'checkpointed_uncovered' output issue #3164 drives into that repair. All six arms are zero-primed once per tree per process under partition 'none', latched on the tree's first collection rather than repeated per pass, and again per classified partition. That priming establishes that the classifier is wired on this silo and nothing more: Add(0) is idempotent on a counter's exported value, so a primed series can never show that the region ran on any particular pass - see 'orleans.lattice.wal.gc.pass.reach' and 'orleans.lattice.wal.gc.tree.reach' for the advancing layer that can. Diagnostic only: it never changes what a pass is allowed to trim.");

    /// <summary>Canonical name of <see cref="WalGcBlockingPinStates"/>.</summary>
    public const string WalGcBlockingPinStatesName = "orleans.lattice.wal.gc.blocking_pin_state";

    /// <summary>
    /// Reserved <see cref="TagPartition"/> value used by the per-tree
    /// reachability priming of <see cref="WalGcBlockingPinStates"/>.
    /// <para>
    /// A real classification always carries the numeric partition it resolved.
    /// This value is minted once per tree per process - on the first pass that
    /// collects the tree, and again only if the tree is de-registered and
    /// returns - whether or not the tree is blocked, so that the instrument has
    /// a series on a silo where
    /// nothing has ever blocked - which is what makes an absent series a
    /// positive statement ("the classifier is not wired here") instead of an
    /// ambiguous one.
    /// </para>
    /// </summary>
    public const string PartitionNone = "none";

    /// <summary>
    /// Reserved <see cref="TagPartition"/> value for a blocking consumer whose
    /// id could not be parsed back to a leaf grain id and partition, so no
    /// partition can be named. Always recorded against
    /// <see cref="BlockingPinUnreadable"/>.
    /// </summary>
    public const string PartitionUnknown = "unknown";

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcBlockingPinStates"/>
    /// for <see cref="WalGcBlockingPinState.CheckpointedUncovered"/> - the
    /// repairable state.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockingPinCheckpointedUncovered =
        new(TagStatus, "checkpointed_uncovered");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcBlockingPinStates"/>
    /// for <see cref="WalGcBlockingPinState.NeverCheckpointed"/> - the state
    /// that is correct by design and has no repair.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockingPinNeverCheckpointed =
        new(TagStatus, "never_checkpointed");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcBlockingPinStates"/>
    /// for <see cref="WalGcBlockingPinState.NoDurableState"/> - the storage
    /// provider answered and reported nothing persisted for the leaf. Kept
    /// distinct from <see cref="BlockingPinNeverCheckpointed"/> so an absence
    /// is never presented as a claim about live data.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockingPinNoDurableState =
        new(TagStatus, "no_durable_state");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcBlockingPinStates"/>
    /// for <see cref="WalGcBlockingPinState.Unreadable"/> - the classifier
    /// could not answer. Reports a failure of the instrument, not a property of
    /// the leaf, so it is counted separately from every arm that does.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockingPinUnreadable =
        new(TagStatus, "unreadable");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcBlockingPinStates"/>
    /// for <see cref="WalGcBlockingPinState.Orphaned"/> - the leaf's durable
    /// state exists but carries no bound tree id, so the leaf was reclaimed or
    /// purged after publishing the pin and the pin has outlived its publisher
    /// (issue #3105). Distinct from every other arm because it is the only one
    /// that is <i>actionable without driving anything</i>: the sweep retires
    /// such a pin outright rather than spending a reactivation attempt on a
    /// leaf that cannot lift it.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockingPinOrphaned =
        new(TagStatus, "orphaned");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcBlockingPinStates"/>
    /// for <see cref="WalGcBlockingPinState.CheckpointedCoverageUnknown"/> -
    /// the leaf durably checkpointed the partition, but its pin is not known to
    /// be unusable, so whether that checkpoint is covered was never determined
    /// and no claim is made (issue #3168).
    /// <para>
    /// Reads the same byte as <see cref="BlockingPinCheckpointedUncovered"/>
    /// and differs only in whether the premise licensing the <i>uncovered</i>
    /// half was available. It is <b>not</b> repairable and is excluded from the
    /// set issue #3164 drives: no coverage hole was established, so a
    /// reactivation could only spend an activation to be told there is nothing
    /// to repair. A tree sitting on this arm is not defective - its WAL floor
    /// is held by a healthy pin that is merely the oldest, which is a
    /// frontier-advance question and not a coverage one.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockingPinCheckpointedCoverageUnknown =
        new(TagStatus, "checkpointed_coverage_unknown");

    /// <summary>
    /// Outcome of each durable materialiser pin examined by the WAL GC's bulk
    /// orphan sweep (issue #3105), tagged by tree and status.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The status arms <b>partition</b> the population examined, so
    /// <c>sum by (tree)</c> over one sweep is exactly the number of durable
    /// pins that tree holds. That summability is the point: the sweep reads the
    /// pin store directly rather than through the floor's
    /// <c>MaxReportedBlockingConsumers</c>-capped diagnostic report, so this is
    /// the only instrument on which the true pin population is visible at all.
    /// </para>
    /// <para>
    /// <b><c>deferred</c> is the backlog signal, and it is why this is a counter
    /// rather than a gauge.</b> A pin classified orphan but left unretired
    /// because the pass's retirement budget was spent increments
    /// <c>deferred</c>. A sustained non-zero <c>deferred</c> rate therefore
    /// means "orphans remain and the sweep is still draining them", and
    /// <c>deferred</c> falling to zero while <c>retired</c> stops advancing
    /// means the backlog is gone. That reads the operational question - is there
    /// still a backlog? - off an advancing series, with none of the declaration-
    /// order hazards an observable gauge carries.
    /// </para>
    /// <para>
    /// The absence of any backlog signal is what let issue #3105 run for days
    /// undetected: the only orphan series in existence was the reactivation
    /// sweep's monotonically-advancing <c>orphaned</c> counter, on which a 63
    /// pins-per-hour drain against a 9,468-pin backlog is indistinguishable from
    /// steady healthy progress. Every arm is zero-primed per tree so a zero is a
    /// measured zero rather than an absence a reader has to interpret.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> WalGcOrphanPinSweep =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.orphan_pin_sweep", unit: "{pin}",
            description: "Outcome of each durable materialiser pin examined by the WAL GC bulk orphan sweep (issue #3105), tagged by tree and status. The arms partition the examined population, so sum by tree over one sweep is the tree's whole durable pin count - the only place it is visible, since the floor's blocking report is capped. 'retired' is a pin whose leaf state carries no bound tree id and which was removed. 'deferred' is such a pin left in place because the pass's retirement budget was spent, and is the backlog signal: a sustained non-zero rate means orphans remain, and zero while 'retired' stops advancing means the backlog has drained. 'live' is a pin whose leaf still holds a bound tree id, which the sweep never touches. 'unresolved' is a consumer id that does not parse back to a leaf grain id. 'unreadable' is a storage read that failed, and the sweep fails closed on it - an unreadable leaf is never retired. All arms are zero-primed per tree.");

    /// <summary>Canonical name of <see cref="WalGcOrphanPinSweep"/>.</summary>
    public const string WalGcOrphanPinSweepName = "orleans.lattice.wal.gc.orphan_pin_sweep";

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcOrphanPinSweep"/> for a
    /// pin whose leaf state carries no bound tree id and which the sweep
    /// removed.
    /// </summary>
    public static readonly KeyValuePair<string, object?> OrphanPinRetired =
        new(TagStatus, "retired");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcOrphanPinSweep"/> for an
    /// orphaned pin left in place because the pass's retirement budget was
    /// spent. The backlog signal - see the remarks on
    /// <see cref="WalGcOrphanPinSweep"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> OrphanPinDeferred =
        new(TagStatus, "deferred");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcOrphanPinSweep"/> for a
    /// pin whose leaf still carries a bound tree id. The sweep never touches
    /// one: a live leaf's pin is retention the WAL GC is obliged to honour.
    /// </summary>
    public static readonly KeyValuePair<string, object?> OrphanPinLive =
        new(TagStatus, "live");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcOrphanPinSweep"/> for a
    /// consumer id that does not parse back to a leaf grain id, so no leaf can
    /// be read for it.
    /// </summary>
    public static readonly KeyValuePair<string, object?> OrphanPinUnresolved =
        new(TagStatus, "unresolved");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcOrphanPinSweep"/> for a
    /// leaf-state read that failed. The sweep fails closed here: an unreadable
    /// leaf is never retired, because retiring a pin whose leaf might still be
    /// live would authorise a trim over a prefix that leaf has not replayed.
    /// </summary>
    public static readonly KeyValuePair<string, object?> OrphanPinUnreadable =
        new(TagStatus, "unreadable");

    /// <summary>
    /// How much of a tree's durable materialiser pin population the WAL GC
    /// floor-holder classification actually looked at on a sweep, tagged by
    /// tree and status (issue #3158).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The denominator <see cref="WalGcBlockingPinStates"/> never carried.</b>
    /// That instrument reports a pin's state but says nothing about how many
    /// pins were examined to produce it, so a tree showing five zeroes is
    /// indistinguishable from a tree whose classifier was never reached - which
    /// is exactly how issue #3158 stayed invisible: the byte-ceiling tree's arms
    /// existed, at zero, under the reserved partition value
    /// <see cref="PartitionNone"/>, and were read as "no pin is in a notable
    /// state" when they meant "nothing ever looked". The two arms here are that
    /// missing denominator, and they make the difference a measurement rather
    /// than an inference.
    /// </para>
    /// <para>
    /// <b>The arms partition the enumerated population</b>, so
    /// <c>sum by (tree)</c> over one sweep is the tree's whole durable pin count
    /// and <c>classified / sum</c> is the coverage fraction directly. A
    /// deliberately tiny fraction is the expected reading, not an alarm: each
    /// classification is a durable storage read, so the sample is capped per
    /// sweep at a constant that is independent of the population. A tree holding
    /// 52,224 pins is therefore expected to report a handful classified against
    /// tens of thousands unclassified, and <c>unclassified</c> is the series
    /// that says so rather than leaving a reader to assume the sample was
    /// complete.
    /// </para>
    /// <para>
    /// <b>Which pins are sampled is not arbitrary, and issue #3178 corrected
    /// which axis it is taken on.</b> The durable materialiser offset floor is a
    /// minimum over the pins that <i>reported</i> an offset, so the pins
    /// carrying the lowest offset are the ones actually holding it. The sample
    /// is those pins, in ascending offset order with the frontier as the
    /// secondary key, which is why a handful of them answers the operational
    /// question - <i>which</i> pin holds this tree's WAL floor, and what state
    /// is its leaf in - that a census of the other 52,216 would not.
    /// </para>
    /// <para>
    /// It previously ranked on the frontier alone. That is the floor's other
    /// axis, folded into the HLC cursor floor by
    /// <c>ApplyDurableMaterialiserFloorAsync</c> rather than into the offset
    /// floor by <c>ComputeMaterialiserOffsetFloorAsync</c>, and a pin holding
    /// one is not in general the pin holding the other - so on a tree whose
    /// every stop reason was <c>offset_floor</c>, the sample confidently
    /// described pins that were not the blocker. Pins reporting no offset
    /// constrain no offset floor at all, and are sampled into a separate list by
    /// frontier, so the thousands of them a never-trimmed tree carries cannot
    /// evict the pins that do.
    /// </para>
    /// </remarks>
    public static readonly Counter<long> WalGcFloorHolderClassification =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.floor_holder_classification", unit: "{pin}",
            description: "How much of a tree's durable materialiser pin population the WAL GC floor-holder classification examined on a sweep (issue #3158), tagged by tree and status. The two arms partition the enumerated population, so sum by (tree) over one sweep is the tree's whole durable pin count and classified / sum is the coverage fraction. 'classified' is a pin whose leaf state was read and whose result was recorded on 'orleans.lattice.wal.gc.blocking_pin_state'. 'unclassified' is a pin the sample did not reach. This is the denominator that instrument never carried: without it, five zero arms on a tree that was never classified are byte-identical to five measured zeroes on a tree that was, which is how issue #3158 stayed invisible on the one tree - the byte-ceiling tree - the classifier exists to diagnose. A very small classified fraction is the designed behaviour, not an alarm: each classification is a durable storage read, so the sample is capped per sweep by a constant independent of the population, and a tree holding tens of thousands of pins is expected to report a handful classified against the rest unclassified. The sample is not arbitrary, and issue #3178 corrected which axis it is taken on. The durable materialiser offset floor is a minimum over the pins that REPORTED an offset, so the pins carrying the lowest offset are the ones holding it, and those are the ones sampled, in ascending offset order with the frontier as the secondary key. It previously ranked on the frontier alone, which is the other of the floor two axes and is folded into the HLC cursor floor rather than into the offset floor, so on a tree stopping at offset_floor the sample described pins that were not the blocker. Pins reporting no offset constrain no offset floor at all and are sampled separately, by frontier, so the thousands of them a never-trimmed tree carries cannot evict the pins that do. The sample is also deduplicated by leaf: a leaf publishes one pin per WAL partition, all carrying a byte-identical frontier, so an undeduplicated sample of eight on an eight-partition tree describes one leaf eight times. Both arms are zero-primed per tree at the top of the tree's collection, above every early return, so an absent series means the classification is not wired on this silo rather than that it found nothing. Diagnostic only: it never changes what a pass is allowed to trim.");

    /// <summary>Canonical name of <see cref="WalGcFloorHolderClassification"/>.</summary>
    public const string WalGcFloorHolderClassificationName =
        "orleans.lattice.wal.gc.floor_holder_classification";

    /// <summary>
    /// <see cref="TagStatus"/> value on
    /// <see cref="WalGcFloorHolderClassification"/> for a pin whose leaf state
    /// was read and whose result was recorded on
    /// <see cref="WalGcBlockingPinStates"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> FloorHolderClassified =
        new(TagStatus, "classified");

    /// <summary>
    /// <see cref="TagStatus"/> value on
    /// <see cref="WalGcFloorHolderClassification"/> for a pin the bounded sample
    /// did not reach. The complement of <see cref="FloorHolderClassified"/> over
    /// the enumerated population, and the arm that stops a small sample being
    /// mistaken for a complete one.
    /// </summary>
    public static readonly KeyValuePair<string, object?> FloorHolderUnclassified =
        new(TagStatus, "unclassified");

    /// <summary>
    /// Whether a floor-holding pin classified
    /// <see cref="WalGcBlockingPinState.CheckpointedCoverageUnknown"/> carried a
    /// usable durable offset, tagged by tree, partition and status (issue
    /// #3199).
    /// <para>
    /// <b>The question it answers, and why no existing instrument can.</b>
    /// <see cref="WalGcBlockingPinStates"/> records the classifier's verdict and
    /// nothing about the candidate that produced it. Both floor-holder sample
    /// lists - the offset-bearing one and the one holding pins that reported no
    /// offset - are recorded through the same call, so a
    /// <c>checkpointed_coverage_unknown</c> raised by a pin carrying an offset
    /// and one raised by a pin carrying none are indistinguishable in the
    /// metric. That distinction is the whole of issue #3199, because the two
    /// have different remedies: the first is still driven for liveness when its
    /// offset equals the tree offset floor, or since issue #3310 when it sits
    /// above an admitted floor within the candidate budget (issue #3178), and
    /// the second can never satisfy that gate at all.
    /// </para>
    /// <para>
    /// <b>The information was already computed and then discarded.</b> The sweep
    /// routes a candidate by <c>offset &lt; 0</c> when it builds the two sample
    /// lists, so the classifying loop already knows which list it is walking.
    /// This instrument records that bit rather than deriving anything new, which
    /// is why it can be added without touching the gate, the routing, or either
    /// drive set.
    /// </para>
    /// <para>
    /// <b>The arms partition this instrument's own population exactly</b>, since
    /// a candidate's offset is either negative or it is not. The population is
    /// every floor-holder classification that resolved to
    /// <see cref="WalGcBlockingPinState.CheckpointedCoverageUnknown"/> - which
    /// is <i>every</i> observation of that arm anywhere, because the promotion
    /// that mints it exists only on the floor-holder path and the blocked arm
    /// records the classifier's verdict unpromoted. So <c>sum by (tree)</c> here
    /// equals <c>blocking_pin_state{status="checkpointed_coverage_unknown"}</c>
    /// on the same tree, and a divergence is a defect in one of the two.
    /// </para>
    /// <para>
    /// <b>That equality is an assertion about the recording sites, not a query
    /// that can be written naively.</b> It says the two counts agree; it does
    /// not say both sides exist on the same label set, and they do not.
    /// <see cref="WalGcBlockingPinStates"/> is also primed per collected tree at
    /// <see cref="PartitionNone"/> - a partition label this instrument never
    /// mints - so the comparison must sum over <c>partition</c> on both sides or
    /// it mismatches on <i>every</i> collected tree in the estate. And a tree
    /// that never reaches the floor-holder classifier has no series here at all
    /// rather than a zero, so the absent side needs an explicit guard
    /// (<c>or vector(0)</c>, or scoping the comparison to trees where this
    /// instrument is present). Absence here means "never classified a floor
    /// holder", which is not the same claim as the measured zero the priming
    /// above guarantees for a tree that did.
    /// </para>
    /// <para>
    /// Both arms are zero-primed per classified <c>(tree, partition)</c>,
    /// alongside the <see cref="WalGcBlockingPinStates"/> priming and
    /// independently of which state resolved, so <c>offset_absent</c> reading
    /// zero against a large <c>offset_usable</c> is a measured absence rather
    /// than silence. That discrimination is the entire value of the instrument:
    /// issue #3199 turns on whether the <c>offset_absent</c> slice is empty, and
    /// an unprimed zero could not answer it in either direction.
    /// </para>
    /// <para>
    /// It carries no per-tree reachability priming of its own. Whether the
    /// floor-holder classifier is wired and running on this silo is already
    /// answered by <see cref="WalGcFloorHolderClassification"/>, so duplicating
    /// that latch here would add a second thing to keep true without adding a
    /// discrimination.
    /// </para>
    /// <para>
    /// Diagnostic only - it never changes what a pass is allowed to trim.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcCoverageUnknownPinOffset =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.coverage_unknown_pin_offset", unit: "{pin}",
            description: "Whether a floor-holding pin classified 'checkpointed_coverage_unknown' carried a usable durable offset (issue #3199), tagged by tree, partition and status. 'offset_usable' is a candidate whose durable offset is >= 0, which is the pin population the offset floor is a minimum over and the only population issue #3178's liveness drive can admit - it is driven when its offset equals the tree offset floor, or (since issue #3310) when it sits above a floor whose own holder was admitted on the same sweep, within the tree's remedy candidate budget. 'offset_absent' is a candidate that reported no usable offset, which constrains no offset floor and can never satisfy that gate. The distinction is issue #3199: such a pin is excluded from the blocked arm because its frontier is above Zero, from the issue #3164 coverage repair because the frontier gate promoted its state away, and from the issue #3178 liveness drive because it carries no offset - and the pin store merges monotonic-max on both axes, so neither exclusion can be cleared by anything the leaf subsequently does. blocking_pin_state records the verdict and nothing about the candidate, and both floor-holder sample lists are recorded through the same call, so the two cases are indistinguishable there; this instrument records the routing bit the sweep already computed. The arms partition this instrument's population exactly, since an offset is either negative or it is not, and the population is every observation of the 'checkpointed_coverage_unknown' arm anywhere, because the promotion that mints that state exists only on the floor-holder path while the blocked arm records the classifier's verdict unpromoted - so sum by (tree) here equals blocking_pin_state{status='checkpointed_coverage_unknown'} on the same tree and a divergence is a defect in one of the two. That equality is an assertion about the recording sites rather than a query that can be written naively: it says the two counts agree, not that both sides exist on the same label set. blocking_pin_state is also primed per collected tree at partition 'none', a label this instrument never mints, so a comparison must sum over partition on both sides or it mismatches on every collected tree; and a tree that never reaches the floor-holder classifier has no series here at all rather than a zero, so the absent side needs an explicit guard. Absence here means 'never classified a floor holder', which is not the measured zero the priming below guarantees for a tree that did. Both arms are zero-primed per classified (tree, partition) alongside the blocking_pin_state priming and independently of which state resolved, so 'offset_absent' reading zero against a large 'offset_usable' is a measured absence rather than silence - which is the entire value of the instrument, because issue #3199 turns on whether that slice is empty and an unprimed zero could not answer it in either direction. It carries no per-tree reachability priming of its own: whether the floor-holder classifier is wired on this silo is already answered by 'orleans.lattice.wal.gc.floor_holder_classification'. Diagnostic only: it never changes what a pass is allowed to trim.");

    /// <summary>Canonical name of <see cref="WalGcCoverageUnknownPinOffset"/>.</summary>
    public const string WalGcCoverageUnknownPinOffsetName =
        "orleans.lattice.wal.gc.coverage_unknown_pin_offset";

    /// <summary>
    /// <see cref="TagStatus"/> value on
    /// <see cref="WalGcCoverageUnknownPinOffset"/> for a candidate whose durable
    /// offset is <c>&gt;= 0</c>.
    /// <para>
    /// This is the pin population the durable offset floor is a minimum over, so
    /// it is the only one issue #3178's liveness drive can admit - and it is
    /// admitted when its offset equals that floor.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageUnknownOffsetUsable =
        new(TagStatus, "offset_usable");

    /// <summary>
    /// <see cref="TagStatus"/> value on
    /// <see cref="WalGcCoverageUnknownPinOffset"/> for a candidate that reported
    /// no usable durable offset.
    /// <para>
    /// Such a pin constrains no offset floor, so it can never satisfy issue
    /// #3178's equality gate, and its state was promoted out of the issue #3164
    /// coverage repair by the frontier gate. Issue #3199 is the question of
    /// whether this arm is ever non-zero on a live estate.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> CoverageUnknownOffsetAbsent =
        new(TagStatus, "offset_absent");

    /// <summary>
    /// Whether the candidate that <i>defines</i> a tree's durable materialiser
    /// offset floor was admitted to the issue #3178 liveness drive on a
    /// classifying sweep, tagged by tree, tenant and status (issue #3258).
    /// <para>
    /// <b>The question it answers, and why no existing instrument can.</b> The
    /// offset floor is the head of the ascending offset sample, so it is
    /// defined by one of the candidates being classified and every other
    /// candidate is strictly above it by construction. The admission gate takes
    /// a candidate classified
    /// <see cref="WalGcBlockingPinState.CheckpointedUncovered"/>, or one
    /// classified
    /// <see cref="WalGcBlockingPinState.CheckpointedCoverageUnknown"/> whose
    /// offset <i>equals</i> that floor. It follows that when the floor-defining
    /// candidate is itself inadmissible, no candidate can be admitted at all,
    /// the repair set is dropped and the drive is never entered - permanently,
    /// because the pin store merges monotonic-max and nothing the leaf
    /// subsequently does can lower the offset holding the floor. Nothing
    /// recorded today separates that from a tree with no repair to do.
    /// <see cref="WalGcFloorHolderClassification"/> counts how many candidates
    /// were examined but not which one holds the floor;
    /// <see cref="WalGcBlockingPinStates"/> records verdicts without saying
    /// which verdict belongs to the floor; and the drive's own counters record
    /// only attempts that happened, so a tree that can never attempt one reads
    /// exactly like a tree that never needed one.
    /// </para>
    /// <para>
    /// <b>Three readings, which is the point.</b> An absent series means the
    /// floor-holder classifier is not wired on this silo. Both arms present and
    /// static at zero means the classifier ran and the tree constrained no
    /// offset floor, so there was nothing to admit or block - a legitimate
    /// healthy state, not a wedge. A climbing <c>blocked</c> arm means the tree
    /// has an offset floor, it is held by a candidate the gate cannot admit,
    /// and the floor therefore cannot advance by this path. That third reading
    /// is what issue #3258 exists to expose: on
    /// <c>repo-context-vector-payload</c> every reactivation counter sat at
    /// zero beside siblings in the tens, and a zero attempt count is equally
    /// consistent with <i>no repair was needed</i> and <i>no repair was ever
    /// possible</i>. Those two are the same series today and different
    /// operational situations - one is idle, the other has accumulated
    /// permanently unreleasable bytes that no retention or compaction setting
    /// can release. Growth is NOT the discriminator: the wedged tree is flat
    /// whenever it is not being written to, so an operator who looks for
    /// growth and finds none concludes idle - the very inversion this
    /// instrument exists to end.
    /// </para>
    /// <para>
    /// <b>It deliberately does not report whether the repair succeeded.</b> The
    /// <c>admitted</c> arm says a candidate entered the drive, not that the
    /// drive healed anything; the existing reactivation instruments carry that.
    /// Folding outcome in here would merge two independent failures - never
    /// admitted, and admitted but unhealed - into one arm, and it is precisely
    /// the first that has no other witness.
    /// </para>
    /// <para>
    /// <b>Why it is recorded once per sweep rather than once per candidate.</b>
    /// The quantity is a property of the floor, and a tree has one floor per
    /// sweep however many pins hold offsets at it. Counting per candidate would
    /// make the arms scale with the sample size rather than with the number of
    /// sweeps, so a tree whose sample happened to be larger would read as more
    /// blocked than one that was equally stuck.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcFloorHolderAdmission =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.floor_holder_admission", unit: "{sweep}",
            description: "Whether the candidate defining a tree's durable materialiser offset floor was admitted to the issue #3178 blocked-leaf liveness drive, recorded once per classifying sweep and tagged by tree, tenant and status (issue #3258). 'admitted' means the floor-defining candidate cleared the admission gate and the drive was entered. 'blocked' means it did not, which is terminal rather than transient: the offset floor is the head of the ascending offset sample, so it is defined by one of the classified candidates and every other candidate is strictly above it, and the gate admits a 'checkpointed_uncovered' candidate unconditionally, and a 'checkpointed_coverage_unknown' one at the floor or - since issue #3310 - above it, but only once a candidate AT the floor has already been admitted. So if the floor's own holder is inadmissible then nothing can be admitted, the repair set is dropped, and the pin store's monotonic-max merge means nothing the leaf later does can lower the offset that holds it. That property is why issue #3310's widening is gated on the floor having been admitted first: without that precondition a wedged tree would begin driving candidates above a floor that can never drain, and this instrument would decay from a wedge detector into a sweep counter. No existing instrument answers this. floor_holder_classification counts how many candidates were examined but not which holds the floor; blocking_pin_state records verdicts without attributing one to the floor; and the reactivation counters record only attempts that occurred, so a tree that can never attempt one is byte-identical to a tree that never needed one. That was the whole of issue #3258: repo-context-vector-payload sat at zero on every reactivation arm beside siblings in the tens, with zero lifetime trimming and therefore a WAL that is permanently unreleasable, and no series in the process could distinguish 'no repair needed' from 'no repair possible'. Do not use growth, byte count, or growth stopping to tell a wedged tree from an idle one: the wedged tree measured flat for 5.5 minutes of an 8-minute window, so each of those reads benign at exactly the moment it should not. Read three ways. An absent series means the floor-holder classifier is not wired on this silo. Both arms present and static at zero means the classifier ran and the tree constrained no offset floor at all, so there was nothing to admit or block - the offset axis is inert, which is healthy. A climbing 'blocked' arm means the tree has an offset floor, it is held by a candidate the gate cannot admit, and the floor cannot advance by this path. Both arms are zero-primed on every sweep that reaches the classifier, independently of which arm resolves and independently of whether an offset floor exists, so 'blocked' reading zero is a measured absence rather than silence - which is the entire value of the instrument, because a wedge that reads as an unprimed zero is exactly the failure it was built to end. It deliberately says nothing about whether the drive healed anything: the reactivation instruments carry that, and folding outcome in here would merge 'never admitted' with 'admitted but unhealed' into one arm when it is the former that has no other witness. Recorded once per sweep rather than once per candidate, because a tree has one floor per sweep however many pins sit at it, and counting per candidate would scale the arms with sample size rather than with sweeps. Diagnostic only: it never changes what a pass is allowed to trim.");

    /// <summary>Canonical name of <see cref="WalGcFloorHolderAdmission"/>.</summary>
    public const string WalGcFloorHolderAdmissionName =
        "orleans.lattice.wal.gc.floor_holder_admission";

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcFloorHolderAdmission"/>
    /// for a sweep whose floor-defining candidate cleared the admission gate,
    /// so the blocked-leaf liveness drive was entered.
    /// </summary>
    public static readonly KeyValuePair<string, object?> FloorHolderAdmissionAdmitted =
        new(TagStatus, "admitted");

    /// <summary>
    /// <see cref="TagStatus"/> value on <see cref="WalGcFloorHolderAdmission"/>
    /// for a sweep whose floor-defining candidate did not clear the admission
    /// gate. Terminal rather than transient: no other candidate can be admitted
    /// once the floor's own holder is refused, so the tree's offset floor
    /// cannot advance by this path at all.
    /// </summary>
    public static readonly KeyValuePair<string, object?> FloorHolderAdmissionBlocked =
        new(TagStatus, "blocked");

    /// <summary>
    /// How WIDE a tree's floor-holder admission was on the offset axis, split at
    /// the floor itself, so the issue #3310 widening's own contribution is
    /// separable from the admission that predates it.
    /// <para>
    /// <b>The problem it solves.</b>
    /// <see cref="WalGcFloorHolderAdmission"/> answers whether the candidate
    /// DEFINING the floor cleared the gate, and it is deliberately recorded once
    /// per sweep because a tree has one floor however many pins sit on it. That
    /// makes it blind to the quantity issue #3310 turned on: <i>how many</i>
    /// candidates were admitted. Under the equality gate the admitted set was
    /// "every pin sitting on exactly one offset", so its width was set by pin
    /// offset distribution and by nothing else - measured at 166 per sweep on
    /// <c>repo-context-vector-metadata</c>, whose pins share one offset, against
    /// 1 to 3 on <c>repo-context-vector-index</c>, which has nine. Same binary,
    /// same silo, same sweep. No series in the process reported that difference.
    /// </para>
    /// <para>
    /// <b>Read the <c>above_floor</c> arm.</b> It counts precisely the
    /// population the equality gate used to refuse, so it is the discriminator
    /// between a widening that works and one that is inert. A tree with a spread
    /// of pin offsets whose <c>above_floor</c> arm stays at its primed zero has
    /// had the widening applied and is getting nothing from it. A tree whose
    /// pins genuinely share one offset reports zero there legitimately, and
    /// <c>at_floor</c> carries its whole admitted set - that is the healthy
    /// clustered shape, not a failure.
    /// </para>
    /// <para>
    /// <b>It does not report outcome, deliberately.</b> Admission is not
    /// healing. Pair it with the reactivation instruments, whose
    /// <c>no_advance</c> arm is offset-verified (issue #3185): rising
    /// <c>above_floor</c> beside a flat <c>no_advance</c> is the widening
    /// working, while rising <c>above_floor</c> beside rising <c>no_advance</c>
    /// is activations being spent on pins that do not move, which is the
    /// regression this arm exists to make visible.
    /// </para>
    /// <para>
    /// Both arms are zero-primed on every sweep that reaches the classifier, so
    /// a zero is a measured absence rather than silence.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcFloorHolderOffsetAdmission =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.floor_holder_offset_admission", unit: "{candidate}",
            description: "How many floor-holding candidates a classifying sweep admitted to the issue #3178 blocked-leaf liveness drive on the offset axis, tagged by tree, tenant and status (issue #3310). 'at_floor' counts candidates whose pin offset equals the tree's durable materialiser offset floor - the only population the gate admitted before issue #3310. 'above_floor' counts candidates admitted strictly above it, which is exactly the population the old equality gate refused, and is therefore the arm that says whether the widening is doing anything at all. This is a per-candidate WIDTH, and is deliberately distinct from floor_holder_admission, which is a per-sweep yes/no about the candidate DEFINING the floor and cannot see width by construction. Width was the defect: under equality the admitted set was 'every pin sitting on exactly one offset', so it was sized by pin offset distribution and nothing else - repo-context-vector-metadata, whose pins share a single offset, admitted 166 per sweep, while repo-context-vector-index, with nine distinct offsets, admitted 1 to 3 on the same binary in the same silo, and grew its WAL to 120.8% of its ceiling with zero decreasing intervals in sixty minutes. Spread is normal for a tree under continuous ingest, whose leaves checkpoint at their own offsets rather than as a bulk-written cohort, so the gate starved exactly the trees that needed it most. Read 'above_floor' as the discriminator: a tree with spread pin offsets whose above_floor arm sits at its primed zero has the widening applied and is getting nothing from it, whereas a genuinely clustered tree reports zero there legitimately and carries its whole admitted set on at_floor. It reports admission, never outcome - pair it with the reactivation instruments, whose no_advance arm is offset-verified (issue #3185), since rising above_floor beside flat no_advance is the widening working while rising above_floor beside rising no_advance is activations spent on pins that do not move. Admission grants no trim entitlement: the drive replays a dormant leaf to the progress it would have made had it been activated, and the published pin is still resolved as min(checkpoint, covered). Both arms are zero-primed on every sweep that reaches the classifier, so a zero is a measured absence rather than silence. Diagnostic only: it never changes what a pass is allowed to trim.");

    /// <summary>Canonical name of <see cref="WalGcFloorHolderOffsetAdmission"/>.</summary>
    public const string WalGcFloorHolderOffsetAdmissionName =
        "orleans.lattice.wal.gc.floor_holder_offset_admission";

    /// <summary>
    /// <see cref="TagStatus"/> value on
    /// <see cref="WalGcFloorHolderOffsetAdmission"/> for a candidate admitted at
    /// the tree's offset floor - the only population admitted before issue
    /// #3310.
    /// </summary>
    public static readonly KeyValuePair<string, object?> FloorHolderOffsetAdmissionAtFloor =
        new(TagStatus, "at_floor");

    /// <summary>
    /// <see cref="TagStatus"/> value on
    /// <see cref="WalGcFloorHolderOffsetAdmission"/> for a candidate admitted
    /// strictly above the tree's offset floor. This is the population the old
    /// equality gate refused, so it is the arm that distinguishes a working
    /// widening from an inert one (issue #3310).
    /// </summary>
    public static readonly KeyValuePair<string, object?> FloorHolderOffsetAdmissionAboveFloor =
        new(TagStatus, "above_floor");

    /// <summary>
    /// How far each WAL GC scheduling pass actually got: the reachability layer
    /// for every instrument sited inside the region that stops executing when
    /// the scheduler degrades (issue #3075).
    /// <para>
    /// <b>The problem it solves.</b> <see cref="WalGcInterval"/> and
    /// <see cref="WalGcPasses"/> are recorded inside <c>CollectTreeAsync</c>,
    /// which a pass reaches only after clearing several earlier exits. When one
    /// of those exits is taken the instruments simply do not advance, and a
    /// series that exists and does not advance is byte-identical to one being
    /// measured as zero. The usual remedy - site the instrument outside the
    /// failing region - is unavailable, because the quantity being counted only
    /// exists inside it.
    /// </para>
    /// <para>
    /// <b>Why this counter advances rather than priming zeros.</b> A
    /// zero-priming layer cannot answer the question. <c>Add(0)</c> is
    /// idempotent on a counter's exported value, so a series primed once and a
    /// series primed ten thousand times both read <c>0</c>; priming can
    /// establish only that a region was reached <i>at least once</i>, never
    /// that it was reached on this pass. Every arm here is incremented by one,
    /// so a rate is observable: a non-zero rate proves the region is executing
    /// <i>now</i>, a zero rate on a present series proves it <i>stopped</i>,
    /// and an absent series proves the build is not deployed. That is a
    /// three-way discrimination where priming gives two.
    /// </para>
    /// <para>
    /// <b>The completeness invariant, and the bound it actually takes.</b>
    /// Every terminating path out of a pass carries exactly one exit arm, so
    /// <c>pass_entered</c> accounts for the seven exit arms
    /// (<see cref="ReachRegistryCancelled"/>, <see cref="ReachRegistryFailed"/>,
    /// <see cref="ReachRegistryTimedOut"/>,
    /// <see cref="ReachLoopCancelled"/>, <see cref="ReachNoDueTree"/>,
    /// <see cref="ReachPassCompletedImmediate"/> and
    /// <see cref="ReachPassCompletedScheduled"/>). The relation is <b>not</b> an
    /// equality in general:
    /// <code>
    /// 0 &lt;= pass_entered - (sum of exit arms) &lt;= concurrent passes in flight
    /// </code>
    /// and with a single scheduler loop that bound is one. <c>pass_entered</c>
    /// is taken before the work and an exit arm after it, so a pass that is
    /// running right now is legitimately counted in the first and not yet in
    /// the second. Equality holds only at <b>quiescence</b> - after the
    /// scheduler has stopped and its execute task has been awaited. A started
    /// counter exceeding a completed counter by the in-flight set is
    /// information, not a defect; it is a violation only where quiescence has
    /// been independently established. Asserted as an equality anywhere else -
    /// in a test that does not stop the scheduler, or in a production alert -
    /// it flaps once per pass forever and is muted, at which point the guard is
    /// gone and nothing says so.
    /// </para>
    /// <para>
    /// <b>What the invariant guards.</b> It fails when the <i>population</i> of
    /// exits changes, not when a known member changes: an early return added to
    /// a pass without an arm unbalances the sum. That is strictly stronger than
    /// counting occurrences of a pattern, which returns a clean number and
    /// misses the siblings - occurrence-counting is what reported four exits
    /// where the source has eight.
    /// </para>
    /// <para>
    /// <b>Reading it.</b> The pass-level arms carry the reserved tree value
    /// <see cref="TreeNone"/>, so any aggregation over the <see cref="TagTree"/>
    /// dimension must filter <c>tree!="_none_"</c> or every pass is counted
    /// alongside the trees it visited.
    /// </para>
    /// <para>
    /// <b><see cref="ReachTreeSeen"/> and <see cref="ReachTreeCollected"/> are a
    /// pair and neither half is interpretable alone.</b> Their difference is
    /// the set of trees skipped because the adaptive interval had not elapsed.
    /// That set is <i>expected to be large and non-zero</i> in a healthy
    /// steady state - on any given pass most registered trees are not yet due -
    /// so a large gap is the normal reading and not a fault. Read alone,
    /// <c>tree_collected</c> understates coverage and <c>tree_seen</c>
    /// overstates it; record and read both.
    /// </para>
    /// <para>
    /// Diagnostic only - it never changes what a pass is allowed to trim.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcPassReach =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.pass.reach", unit: "{pass}",
            description: "How far each WAL GC scheduling pass got, tagged by stage (issue #3075). Reachability layer for the instruments sited inside the region that stops executing: 'wal.gc.interval' and 'wal.gc.passes' are recorded inside CollectTreeAsync, so an exit taken above them leaves both silent and a non-advancing series is indistinguishable from a measured zero. Every arm advances by one rather than priming a zero, because Add(0) is idempotent on a counter and so priming can only ever establish that a region was reached at least once, never that it was reached on this pass: a non-zero rate here proves the region is executing now, a zero rate on a present series proves it stopped, and an absent series proves the build is not deployed. Every arm ('pass_entered', 'registry_cancelled', 'registry_timed_out', 'registry_failed', 'loop_cancelled', 'no_due_tree', 'pass_completed_immediate', 'pass_completed_scheduled') carries the reserved tree value '_none_' and the platform tenant sentinel, because a pass is a scheduler-wide event that spans the whole registry and no tree id or tenant is knowable at the two registry exits - enumerating the trees is the operation that failed. Query it without a tree or tenant filter; the per-tree half of the layer is the separate 'wal.gc.tree.reach'. 'pass_entered' accounts for the seven exit arms, but it is taken before the work and an exit arm after it, so the correct relation is 0 <= pass_entered - (sum of exit arms) <= passes in flight, which is one for a single scheduler loop; assert equality only at quiescence, and alert on the inequality or it will flap once per pass. That relation is the guard against an early return added without an arm. Diagnostic only: it never changes what a pass is allowed to trim.");

    /// <summary>Canonical name of <see cref="WalGcPassReach"/>.</summary>
    public const string WalGcPassReachName = "orleans.lattice.wal.gc.pass.reach";

    /// <summary>
    /// Per-tree half of the WAL GC reachability layer (issue #3075): how far a
    /// pass got <i>for one tree</i>, tagged by stage.
    /// <para>
    /// <b>Why this is a separate instrument from
    /// <see cref="WalGcPassReach"/> rather than two stages of one.</b> The two
    /// halves have different attribution subjects, and an instrument may only
    /// have one. A pass spans the entire registry, so it belongs to no tenant
    /// and carries <see cref="LatticeTenantLabel.Platform"/>; a tree visit
    /// belongs to exactly the tenant that owns the tree, exactly as its
    /// siblings <see cref="WalGcPasses"/> and <see cref="WalGcInterval"/> do.
    /// Emitting both under one instrument would split its series across two
    /// attribution rules, so a tenant-scoped query would silently return the
    /// per-tree arms and drop the pass-level ones - and dropping
    /// <c>pass_entered</c> is precisely the reading that says whether the
    /// scheduler ran at all. Deriving a tenant for a pass instead would satisfy
    /// the same check by inventing an attribution that does not exist.
    /// </para>
    /// <para>
    /// The split costs no analysis, because neither reading spans the two: the
    /// completeness invariant is entirely pass-level, and the
    /// <see cref="ReachTreeSeen"/>/<see cref="ReachTreeCollected"/> pair is
    /// entirely per-tree.
    /// </para>
    /// <para>
    /// Diagnostic only - it never changes what a pass is allowed to trim.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcTreeReach =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.tree.reach", unit: "{tree}",
            description: "How far each WAL GC scheduling pass got for one tree, tagged by stage and tree (issue #3075). Per-tree half of the reachability layer for the instruments sited inside the region that stops executing: 'wal.gc.interval' and 'wal.gc.passes' are recorded inside CollectTreeAsync, so an exit taken above them leaves both silent and a non-advancing series is indistinguishable from a measured zero. Both arms advance by one rather than priming a zero, because Add(0) is idempotent on a counter and so priming can only ever establish that a region was reached at least once, never that it was reached on this pass. 'tree_seen' and 'tree_collected' are a pair and neither half is interpretable alone: their difference is the set of trees skipped because the adaptive interval had not elapsed, which is expected to be large and non-zero in a healthy steady state, so a large gap is the normal reading and not a fault. 'tree_collected' is the arm that licenses reading a flat 'wal.gc.interval' or 'wal.gc.passes' for that tree as measured rather than as never-executed. This instrument is tenant-derived like its siblings; the scheduler-wide half of the layer, which belongs to no tenant, is the separate 'wal.gc.pass.reach'. Diagnostic only: it never changes what a pass is allowed to trim.");

    /// <summary>Canonical name of <see cref="WalGcTreeReach"/>.</summary>
    public const string WalGcTreeReachName = "orleans.lattice.wal.gc.tree.reach";

    /// <summary>
    /// Reserved <see cref="TagTree"/> value carried by the pass-level arms of
    /// <see cref="WalGcPassReach"/>, which describe a scheduling pass as a whole
    /// rather than any one tree.
    /// <para>
    /// A sentinel is structurally required here rather than merely convenient.
    /// Two of the exits the layer must cover are the <c>catch</c> arms of the
    /// registry enumeration itself, and at those points the tree list is what
    /// failed to be obtained - there is no tree id to label the measurement
    /// with, and there never can be.
    /// </para>
    /// <para>
    /// This is the <see cref="PartitionNone"/> convention applied to a second
    /// dimension, not a second convention. It takes the underscore-delimited
    /// form for a reason that is specific to this dimension: a partition is
    /// numeric, so the bare value <c>none</c> cannot collide with a real one,
    /// whereas a tree id is a caller-supplied string and a tree could
    /// legitimately be named <c>none</c>. The underscore-delimited shape
    /// matches the reserved
    /// <see cref="LatticeTenantLabel.PlatformTenant"/> sentinel and the
    /// <c>_lattice_</c> system tree namespace.
    /// </para>
    /// </summary>
    public const string TreeNone = "_none_";

    /// <summary>
    /// The <see cref="TagTree"/> tag carrying <see cref="TreeNone"/>, frozen so
    /// the pass-level arms allocate nothing per pass.
    /// </summary>
    public static readonly KeyValuePair<string, object?> TreeNoneTag =
        new(TagTree, TreeNone);

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> recorded as the
    /// first statement of a pass, above every exit. It is the denominator the
    /// other pass-level arms are read against.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachPassEntered =
        new(TagStage, "pass_entered");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> for a pass that
    /// ended because enumerating the tree registry was cancelled - host
    /// shutdown, not a fault.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachRegistryCancelled =
        new(TagStage, "registry_cancelled");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> for a pass that
    /// ended because enumerating the tree registry threw. Kept distinct from
    /// <see cref="ReachRegistryCancelled"/>: one is orderly shutdown and the
    /// other is a fault, and a layer that merged them would report a wedged
    /// registry as a clean stop.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachRegistryFailed =
        new(TagStage, "registry_failed");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> for a pass
    /// that ended because the scheduler's own enumeration budget fired.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Kept apart from <see cref="ReachRegistryFailed"/> for the reason the
    /// scheduler keeps the two <c>catch</c> arms apart: a fault is a property
    /// of the registry and a timeout is a property of the bound we chose, so
    /// folding the second into the first would let a decision of ours present
    /// as a finding about the system.
    /// </para>
    /// <para>
    /// This arm was <b>missing</b> from the layer as first shipped, and the
    /// way it was missed is the reason it is documented at length. The
    /// <c>catch (TimeoutException)</c> it accounts for was inserted ahead of
    /// the general fault arm by a later change, and an insertion is invisible
    /// to every detector that reasons about the arms already present: the
    /// balance relation sees only the exits a fixture drives, and no fixture
    /// drove this one. The same insertion silently migrated an unrelated
    /// file's fault tests onto the new arm. An exit added between two covered
    /// exits is the cheapest coverage hole in the file to open and the most
    /// expensive to notice.
    /// </para>
    /// </remarks>
    public static readonly KeyValuePair<string, object?> ReachRegistryTimedOut =
        new(TagStage, "registry_timed_out");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> for a pass
    /// cancelled part-way through its tree loop, so an arbitrary suffix of the
    /// registered trees went uncollected on that pass.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachLoopCancelled =
        new(TagStage, "loop_cancelled");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> for a pass that
    /// completed its loop without finding a single collectable tree.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachNoDueTree =
        new(TagStage, "no_due_tree");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> for a pass that
    /// ran to completion and found its successor already due, so the scheduler
    /// does not sleep. Kept distinct from
    /// <see cref="ReachPassCompletedScheduled"/> because a sustained rate here
    /// is a scheduler running flat out, which is an operational state worth
    /// seeing and costs one string to preserve.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachPassCompletedImmediate =
        new(TagStage, "pass_completed_immediate");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcPassReach"/> for a pass that
    /// ran to completion and scheduled its successor for a future due time -
    /// the ordinary healthy exit.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachPassCompletedScheduled =
        new(TagStage, "pass_completed_scheduled");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcTreeReach"/> for a tree the
    /// pass enumerated and considered. Recorded per tree per pass, whether or
    /// not the tree was due.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachTreeSeen =
        new(TagStage, "tree_seen");

    /// <summary>
    /// <see cref="TagStage"/> value on <see cref="WalGcTreeReach"/> for a tree
    /// whose collection was actually entered, recorded above every exit of that
    /// collection. This is the arm that licenses reading a flat
    /// <see cref="WalGcInterval"/> or <see cref="WalGcPasses"/> as measured
    /// rather than as never-executed.
    /// <para>
    /// <see cref="ReachTreeSeen"/> minus this arm is the population skipped
    /// because its adaptive interval had not elapsed. That is the healthy
    /// steady state, not a fault: on any given pass most registered trees are
    /// not yet due.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReachTreeCollected =
        new(TagStage, "tree_collected");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a touch that was issued
    /// to a blocking leaf. Counts the cost the sweep imposes, independently of
    /// whether it achieved anything.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationAttempted =
        new(TagOutcome, "attempted");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a previously-swept
    /// consumer that stopped blocking its tree. This is the only evidence that
    /// a reactivation accomplished anything, so it is what distinguishes a
    /// working sweep from one that is merely running.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationHealed =
        new(TagOutcome, "healed");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a consumer that stayed
    /// blocked across every permitted attempt of a cycle. Emitted once per
    /// cycle, at the moment the budget is spent, so the series counts stranded
    /// leaves rather than retries.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationAbandoned =
        new(TagOutcome, "abandoned");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for an abandoned consumer
    /// whose attempt budget was restored after a backoff (issue #2783), letting
    /// the sweep try again once the transient pressure that defeated it may have
    /// lifted. Paired with <see cref="BlockedLeafReactivationAbandoned"/>: the
    /// two advancing together is a tree retrying on schedule, whereas
    /// <c>abandoned</c> advancing alone is a sweep that has stopped.
    /// </summary>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationRearmed =
        new(TagOutcome, "rearmed");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a touch whose probe call
    /// did not return before the cluster response timeout (issue #2768).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the arm that separates <i>the sweep could not deliver its
    /// touch</i> from <i>the sweep delivered its touch and the leaf still did
    /// not heal</i>. Without it both land on <c>attempted</c> with no
    /// <c>healed</c>, which reads as "reactivation does not work" when the truth
    /// may be that the leaf was never reached - and those call for opposite
    /// responses.
    /// </para>
    /// <para>
    /// It is emitted alongside <c>attempted</c>, never instead of it, so the
    /// cost series stays complete. A timeout is <b>not</b> evidence that the
    /// touch was wasted: the probe is a request whose only job is to cause
    /// activation, and a caller-side timeout does not cancel the activation the
    /// message already started. It says the sweep stopped waiting, not that
    /// nothing happened.
    /// </para>
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationUndelivered =
        new(TagOutcome, "undelivered");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a touch that reached the
    /// leaf and returned without error (issue #2938).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the arm whose absence did the damage. Paired with a flat
    /// <c>healed</c> it says something no other series can: the sweep reached
    /// the leaf, the leaf answered, and the tree stayed blocked - so
    /// reachability is not sufficient to clear the pin and the remedy's premise
    /// is wrong. Without it that state is indistinguishable from a sweep whose
    /// calls never arrive, and the two call for opposite responses.
    /// </para>
    /// <para>
    /// It was uncounted while it occurred 234 times in a single observation
    /// window, and recovering the fact cost an arithmetic derivation from
    /// source constants plus a log grep. It is a counter now so that the same
    /// question is one query.
    /// </para>
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationCompleted =
        new(TagOutcome, "completed");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a touch whose consumer id
    /// did not resolve to a leaf, so nothing was called (issue #2938).
    /// </summary>
    /// <remarks>
    /// Distinct from every other outcome in that the sweep never left the
    /// process. A tree accumulating this arm has a naming or parsing fault
    /// rather than a stuck leaf, and no amount of reactivation will help it -
    /// which is why the outcome is not refunded against the attempt budget.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationUnresolvable =
        new(TagOutcome, "unresolvable");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a blocking pin whose leaf
    /// reported no bound tree id, so the pin is an orphan left behind by a
    /// reclaimed or purged leaf and the sweep retired it (issue #3101).
    /// </summary>
    /// <remarks>
    /// <para>
    /// The arm that distinguishes a repair from a vacuous success. An orphaned
    /// pin previously reported as <see cref="BlockedLeafReactivationCompleted"/>
    /// - a success arm - so a sweep that could never clear the block was
    /// indistinguishable from one that cleared it, and the tree's WAL was
    /// retained without bound while the counter said the touches were fine.
    /// </para>
    /// <para>
    /// A non-zero rate here is a signal about the <i>leaf lifecycle</i>, not
    /// about the GC: it counts pins that outlived their leaf. The sweep repairs
    /// them, so the series should fall to zero once a deployment has drained the
    /// orphans it accumulated before the retirement seam existed. A rate that
    /// stays non-zero means something is still retiring leaves without retiring
    /// their pins.
    /// </para>
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationOrphaned =
        new(TagOutcome, "orphaned");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a touch whose probe call
    /// threw something other than a timeout (issue #2938).
    /// </summary>
    /// <remarks>
    /// Evidence about the silo rather than about the leaf, so it is refunded
    /// against the attempt budget in the same way as
    /// <see cref="BlockedLeafReactivationUndelivered"/> and reported separately
    /// from it, because a call that failed and a call that has not yet answered
    /// support different conclusions about whether the leaf can heal.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationFaulted =
        new(TagOutcome, "faulted");

    /// <summary>
    /// Canonical name of the WAL GC scheduler's phase-age gauge, declared on
    /// <c>WalGcSchedulerPhaseCensus</c> rather than here because an observable
    /// instrument's registration has to sit beside the callback that tags its
    /// measurements.
    /// <para>
    /// The name is held here so that the marker on <c>WalGcSchedulerPhase</c>,
    /// the documentation gates, and the census all read one string.
    /// </para>
    /// </summary>
    public const string WalGcSchedulerPhaseAgeGaugeName = "orleans.lattice.wal.gc.scheduler.phase_age";

    /// <summary>
    /// Passes the silo's WAL GC scheduler loop has begun, counted before the
    /// pass does anything that can fail.
    /// <para>
    /// <b>This is the heartbeat, and its whole value is where it is emitted.</b>
    /// Issue #3060 is a sweep that stops silo-wide and never resumes, and every
    /// other <c>wal_gc</c> series is written per tree, <i>after</i> the registry
    /// enumeration that opens the pass. A pass that fails at that first await
    /// therefore writes nothing at all, so a scheduler that is alive and failing
    /// every attempt produces a scrape byte-identical to one whose loop has
    /// returned. This counter is incremented above the enumeration's
    /// <c>try</c>, so it advances on exactly the passes that produce no other
    /// evidence, and it separates those two states in a single scrape.
    /// </para>
    /// <para>
    /// <b>Untagged by tree, deliberately.</b> The loop is one per silo and the
    /// question is "did it iterate", not "which tree did it reach" - the second
    /// is already answered by <see cref="WalGcPasses"/>. Tagging by tree would
    /// fragment liveness across every registered tree and turn a read into a sum,
    /// and it would be unanswerable on the failing pass, because a failed
    /// enumeration has no tree ids to tag with.
    /// </para>
    /// <para>
    /// Zero-primed as the first statement of the loop, above the disabled-by-
    /// configuration return and above the startup stagger, so a zero here says
    /// "this silo's scheduler has not begun a pass" rather than "this build has
    /// no such instrument".
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcSchedulerPassesStarted =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.scheduler.passes_started", unit: "{pass}",
            description: "Passes the silo's WAL GC scheduler loop has begun (issue #3060), counted above the registry enumeration so it advances even on a pass that fails at its first await. Untagged by tree, because the loop is one per silo and a failed pass has no tree ids to tag with. Read against the scheduler's phase age: a total that keeps climbing while every per-tree wal_gc series is frozen is a loop that is alive and failing, which needs a different remedy from a loop that has stopped. Zero-primed above every early return, so zero is a reading.");

    /// <summary>Canonical name of <see cref="WalGcSchedulerPassesStarted"/>.</summary>
    public const string WalGcSchedulerPassesStartedName = "orleans.lattice.wal.gc.scheduler.passes_started";

    /// <summary>
    /// What the registry enumeration that opens each WAL GC scheduler pass
    /// produced, tagged by outcome.
    /// <para>
    /// The enumeration is the pass's first await and the single dependency of
    /// every later stage, so it is also its most consequential silent failure.
    /// Before issue #3060 a fault here was swallowed into a <c>LogDebug</c> and
    /// answered with a relaxing quiet wait: no counter, no warning, and a scrape
    /// in which every <c>wal_gc</c> series simply stopped advancing.
    /// </para>
    /// <para>
    /// <b>Six arms, because one silent path already served three conditions.</b>
    /// The scheduler's own quiet-wait documentation names an empty registry, a
    /// faulted registry, and a registry reporting only blank ids, and routes all
    /// three to the same wait through the same silence. They have different
    /// causes and different remedies, so they are separate arms.
    /// <c>timed_out</c> is separate from <c>faulted</c> for a different reason
    /// again: a fault is a property of the registry, whereas a timeout is a
    /// property of the bound this scheduler applies to it, and reporting the
    /// second as the first would render a decision of ours as a finding about the
    /// system.
    /// </para>
    /// <para>
    /// Every arm is zero-primed by walking the outcome enum, so a new arm cannot
    /// ship unprimed and a zero is always a measurement.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcSchedulerEnumerations =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.scheduler.enumerations", unit: "{enumeration}",
            description: "Outcome of the registry enumeration that opens each WAL GC scheduler pass (issue #3060). 'succeeded' saw at least one non-blank tree id. 'faulted' is the registry throwing, which before this instrument existed was swallowed into a debug log and left no trace on any scrape. 'cancelled' is an orderly silo shutdown and is not a fault. 'empty' and 'all_blank' are separated because a registry that answers with ids that are all blank reports success, returns content, and collects nothing, so it presents exactly as an idle silo on every other series. 'timed_out' is a budget-shaped failure, though not necessarily this scheduler's own bound: the awaits are wrapped by Task.WaitAsync, which raises TimeoutException on expiry and also propagates one raised inside the operation, such as an Orleans response timeout at the 30s default. The two are indistinguishable by exception type, so this arm deliberately does not split them - the abandonment log line carries the attribution and the measured elapsed that separate them. It is kept apart from 'faulted' because a fault is a property of the registry and a timeout is a property of a bound, so that a limit of ours is never reported as a failure of the subject. All arms zero-primed by walking the enum.");

    /// <summary>Canonical name of <see cref="WalGcSchedulerEnumerations"/>.</summary>
    public const string WalGcSchedulerEnumerationsName = "orleans.lattice.wal.gc.scheduler.enumerations";

    /// <summary>
    /// Why the silo's WAL GC scheduler loop stopped, tagged by reason.
    /// <para>
    /// A <c>BackgroundService</c> that returns from <c>ExecuteAsync</c> is
    /// finished for the lifetime of the process: nothing restarts it, and the
    /// host neither logs nor reports the return. Every exit therefore ends
    /// silo-wide WAL garbage collection permanently, and before issue #3060 none
    /// of them emitted anything a scrape could see.
    /// </para>
    /// <para>
    /// This is the terminal half of the liveness set and is read with the phase
    /// gauge, never instead of it: a loop wedged inside a phase and a loop that
    /// has returned are indistinguishable on a counter alone, and the gauge
    /// cannot say why a stopped loop stopped.
    /// </para>
    /// <para>
    /// All three arms are zero-primed by walking the termination enum before the
    /// loop can take any of them, so a zero reads as "this silo's scheduler has
    /// not stopped" rather than as an unpublished series - which matters more
    /// here than on most instruments, because the entire finding this instrument
    /// serves is an absence.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalGcSchedulerTerminations =
        Meter.CreateCounter<long>("orleans.lattice.wal.gc.scheduler.terminations", unit: "{termination}",
            description: "Why the silo's WAL GC scheduler loop stopped (issue #3060), tagged by reason. A background service that returns from ExecuteAsync never runs again for the life of the process and the host reports nothing, so every arm here is a permanent end to silo-wide WAL collection. 'disabled' is WalGcInterval <= 0, a correct exit that on every other instrument is indistinguishable from a wedge. 'cancelled' is an orderly host shutdown. 'faulted' is the loop body throwing, which is recorded and then rethrown so the host's configured exception behaviour is unchanged. All arms zero-primed by walking the enum, so a zero is a reading and not a missing wire.");

    /// <summary>Canonical name of <see cref="WalGcSchedulerTerminations"/>.</summary>
    public const string WalGcSchedulerTerminationsName = "orleans.lattice.wal.gc.scheduler.terminations";

    /// <summary>
    /// How long the WAL GC scheduler chose to wait before its next pass, observed
    /// once per pass at the delay site.
    /// <para>
    /// <b>This is the discriminator, not a convenience.</b> When a pass fails
    /// before it reaches any tree, the scheduler answers with a quiet wait that
    /// relaxes geometrically toward the configured ceiling - 30s, 60s, 120s, and
    /// on up to an hour. A loop climbing that ladder is alive and retrying; a
    /// loop that has returned is dead. Both freeze every per-tree series
    /// identically, so the ladder is the <i>only positive signature</i> the
    /// alive-and-retrying case has. Without this histogram the two states are one
    /// reading, separable only by two scrapes an hour apart.
    /// </para>
    /// <para>
    /// <see cref="WalGcInterval"/> cannot supply it. That instrument is written
    /// per tree inside the collection stage, so a pass that fails at the
    /// enumeration writes no observation on it at all - it is absent exactly when
    /// the question is asked.
    /// </para>
    /// <para>
    /// <b>Not zero-primed, and that is the correct treatment.</b> The empty state
    /// of a duration distribution is undefined rather than zero: a scheduler that
    /// has not yet chosen a wait has not waited zero seconds, and a primed
    /// observation would put a fabricated sample in the distribution and pull
    /// every percentile toward it. Its liveness is asserted instead by anchoring
    /// its count to <see cref="WalGcSchedulerPassesStarted"/>, which is primed:
    /// one observation per started pass, checked by fixture.
    /// </para>
    /// <para>
    /// <b>Boundary: this records the wait the scheduler <i>selected</i>, not how
    /// long anything took.</b> The two diverge, and the divergence is itself a
    /// finding this instrument is structurally unable to report: a silo whose
    /// per-tree touches all end at the Orleans default 30s response timeout
    /// selects the 30s floor on every pass while the observed wall period is
    /// 60s, and this histogram reads a flat 30.0 throughout. An instrument sited
    /// on a decision cannot observe a failure that happens after the decision.
    /// <see cref="WalGcSchedulerPassDuration"/> is the counterpart that can, and
    /// the pair is only informative read together.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalGcSchedulerWait =
        Meter.CreateHistogram<double>("orleans.lattice.wal.gc.scheduler.wait", unit: "s",
            description: "How long the silo's WAL GC scheduler chose to wait before its next pass (issue #3060), observed once per pass at the delay site so it is recorded even when the pass reached no tree. A pass that fails early answers with a quiet wait that relaxes geometrically toward the configured ceiling, and that climbing ladder is the only positive signature a loop which is alive and retrying has - every other series it touches is frozen exactly as a stopped loop leaves them. This is the selected wait, not the elapsed pass: see pass_duration for the counterpart that shows selected-versus-actual divergence. Deliberately not zero-primed, because the empty state of a duration distribution is undefined rather than zero; its count is anchored to passes_started instead.");

    /// <summary>Canonical name of <see cref="WalGcSchedulerWait"/>.</summary>
    public const string WalGcSchedulerWaitName = "orleans.lattice.wal.gc.scheduler.wait";

    /// <summary>
    /// How long one WAL GC scheduling pass actually took, in seconds, recorded
    /// once per started pass (issue #3060).
    /// <para>
    /// <b>This is the instrument that shows selected-versus-actual
    /// divergence</b>, which <see cref="WalGcSchedulerWait"/> cannot. A silo
    /// whose per-tree touches are fanned out and awaited together, each ending
    /// at the Orleans default 30s response timeout, selects the 30s adaptive
    /// floor on every pass while the wall period between passes is 60s. The
    /// selected wait reads a flat 30.0 and is not wrong - the scheduler really
    /// did choose 30s - it is simply blind to the other 30. Read together the
    /// pair names the gap; read apart, neither does.
    /// </para>
    /// <para>
    /// <b>Recorded in a finally, so a pass that fails is recorded too.</b> The
    /// failing population is the one whose duration matters most: a pass that
    /// dies at a response timeout has a duration which <i>is</i> the diagnosis.
    /// Siting this on the success path would discard exactly the sample worth
    /// having.
    /// </para>
    /// <para>
    /// <b>No tree dimension, deliberately.</b> Tagging by tree would fragment one
    /// silo fact across every tree the silo hosts and turn "how long is a pass"
    /// into a sum; a pass is a property of the loop, not of any tree it visited,
    /// and a pass that failed before enumerating has no tree to attribute itself
    /// to at all. It carries the constant
    /// <see cref="LatticeTenantLabel.Platform"/> sentinel and nothing else: the
    /// repository requires every emission site to carry the tenant dimension so
    /// that a telemetry query is byte-identical on a tenancy-on and a tenancy-off
    /// cluster, and a genuinely untagged instrument is not an option here.
    /// </para>
    /// <para>
    /// <b>Not zero-primed, for the reason
    /// <see cref="WalGcSchedulerWait"/> is not</b> - the empty state of a
    /// duration distribution is undefined rather than zero. Its liveness is
    /// anchored instead to <see cref="WalGcSchedulerPassesStarted"/>: exactly one
    /// observation per started pass, asserted by fixture. That anchor is what
    /// keeps it from going vacuous, because a frozen count next to a flat
    /// counter that is independently visible cannot be misread as "no passes".
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalGcSchedulerPassDuration =
        Meter.CreateHistogram<double>("orleans.lattice.wal.gc.scheduler.pass_duration", unit: "s",
            description: "How long one WAL GC scheduling pass actually took (issue #3060), recorded in a finally so a pass that faults or is abandoned is measured too - that population's duration is itself the diagnosis. This is the counterpart to the selected wait: a silo whose per-tree touches all end at the Orleans default 30s response timeout selects the 30s floor on every pass while the wall period is 60s, and only this instrument can see the difference. Carries no tree dimension, because a pass is a property of the scheduler loop and a pass that failed before enumerating has no tree to attribute itself to. Not zero-primed (an empty duration distribution is undefined, not zero); its count is anchored to passes_started instead.");

    /// <summary>Canonical name of <see cref="WalGcSchedulerPassDuration"/>.</summary>
    public const string WalGcSchedulerPassDurationName = "orleans.lattice.wal.gc.scheduler.pass_duration";

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="WalGcSchedulerEnumerations"/>
    /// for <see cref="WalGcEnumerationOutcome.Succeeded"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcEnumerationSucceeded =
        new(TagOutcome, "succeeded");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="WalGcSchedulerEnumerations"/>
    /// for <see cref="WalGcEnumerationOutcome.Faulted"/> - the registry threw.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcEnumerationFaulted =
        new(TagOutcome, "faulted");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="WalGcSchedulerEnumerations"/>
    /// for <see cref="WalGcEnumerationOutcome.Cancelled"/> - an orderly shutdown,
    /// kept apart from <see cref="WalGcEnumerationFaulted"/> so that a clean stop
    /// never trains a reader to ignore the fault arm.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcEnumerationCancelled =
        new(TagOutcome, "cancelled");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="WalGcSchedulerEnumerations"/>
    /// for <see cref="WalGcEnumerationOutcome.Empty"/> - no tree ids at all.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcEnumerationEmpty =
        new(TagOutcome, "empty");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="WalGcSchedulerEnumerations"/>
    /// for <see cref="WalGcEnumerationOutcome.AllBlank"/> - ids were returned but
    /// every one of them was blank, which is the arm that cannot be inferred from
    /// any other series.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcEnumerationAllBlank =
        new(TagOutcome, "all_blank");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="WalGcSchedulerEnumerations"/>
    /// for <see cref="WalGcEnumerationOutcome.TimedOut"/> - the scheduler's own
    /// bound fired. Reports a decision of ours rather than a property of the
    /// registry, so it is never summed with
    /// <see cref="WalGcEnumerationFaulted"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcEnumerationTimedOut =
        new(TagOutcome, "timed_out");

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="WalGcSchedulerTerminations"/>
    /// for <see cref="WalGcSchedulerTermination.Disabled"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcSchedulerStoppedDisabled =
        new(TagReason, "disabled");

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="WalGcSchedulerTerminations"/>
    /// for <see cref="WalGcSchedulerTermination.Cancelled"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcSchedulerStoppedCancelled =
        new(TagReason, "cancelled");

    /// <summary>
    /// <see cref="TagReason"/> value on <see cref="WalGcSchedulerTerminations"/>
    /// for <see cref="WalGcSchedulerTermination.Faulted"/>.
    /// </summary>
    public static readonly KeyValuePair<string, object?> WalGcSchedulerStoppedFaulted =
        new(TagReason, "faulted");

    /// <summary>
    /// Terminal states of a leaf's deferred WAL replay (issue #2871), tagged by
    /// tree and outcome (<c>completed</c>/<c>faulted</c>/<c>canceled</c>).
    /// <para>
    /// This instrument exists because issue #2871 moved WAL replay off the leaf
    /// activation critical path, and that move <b>traded a loud failure for a
    /// quiet one</b>. Before it, a replay that could not complete destroyed the
    /// activation: the caller got an error, Orleans logged it, and the failure
    /// was impossible to miss. After it, the activation succeeds regardless -
    /// which is the entire point, because a leaf that answers
    /// <c>GetTreeIdAsync()</c> is a leaf the WAL GC reactivation sweep can reach
    /// - so a replay that fails leaves behind a live, healthy-looking grain whose
    /// data operations fail one at a time. Without this series, the only trace of
    /// a systematically failing replay would be scattered per-request errors
    /// attributed to whatever unlucky caller happened to arrive.
    /// </para>
    /// <para>
    /// All outcomes share one instrument and every outcome is zero-primed per
    /// tree when a barrier is armed, so a zero on <c>faulted</c> is a measured zero
    /// rather than an unpublished series - the same reasoning as
    /// <see cref="WalGcBlockedLeafReactivations"/>, and for the same reason: an
    /// absent series cannot be told from a working one, and here the two mean
    /// opposite things.
    /// </para>
    /// <para>
    /// <c>faulted</c> is the alarm arm. <c>canceled</c> is normal in bounded
    /// quantity - a deactivation cancels an in-flight replay, and a leaf that
    /// activates and deactivates under memory pressure will show it - and is the
    /// arm to read against <c>completed</c> rather than on its own. Neither is
    /// terminal for the grain: a barrier that faults or cancels disarms itself, so
    /// the next data operation or the next WAL GC touch re-arms a fresh replay.
    /// A sustained <c>faulted</c> rate with a flat <c>completed</c> therefore
    /// means a leaf is retrying a replay it can never finish, which is the
    /// condition that used to present as an activation failure.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafReplayBarrierOutcomes =
        Meter.CreateCounter<long>("orleans.lattice.leaf.replay_barrier_outcomes", unit: "{replay}",
            description: "Terminal states of a leaf's deferred WAL replay (issue #2871), tagged by tree and outcome (completed/faulted/canceled). All outcomes share one instrument and each is zero-primed per tree when a barrier is armed, so a zero on 'faulted' is a measured zero and not an unpublished series. Since replay no longer runs on the activation path, a failure here does NOT surface as an activation failure - this series is the signal that replaces it.");

    /// <summary>Canonical name of <see cref="LeafReplayBarrierOutcomes"/>.</summary>
    public const string LeafReplayBarrierOutcomesName = "orleans.lattice.leaf.replay_barrier_outcomes";

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="LeafReplayBarrierOutcomes"/> for
    /// a replay that applied fully. The barrier is satisfied for the remainder of
    /// the activation and every subsequent data operation passes it for free.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReplayBarrierCompleted =
        new(TagOutcome, "completed");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="LeafReplayBarrierOutcomes"/> for
    /// a replay that threw. Any request waiting on the barrier fails with that
    /// exception; the activation stays usable and the barrier re-arms on the next
    /// data operation or WAL GC touch.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReplayBarrierFaulted =
        new(TagOutcome, "faulted");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="LeafReplayBarrierOutcomes"/> for
    /// a replay cancelled before it completed - by deactivation, or by an
    /// operation that discards the state the replay was rebuilding
    /// (<c>ClearGrainStateAsync</c>, <c>RebuildProjectionFromWalAsync</c>).
    /// Expected in bounded quantity and not on its own a defect.
    /// </summary>
    public static readonly KeyValuePair<string, object?> ReplayBarrierCanceled =
        new(TagOutcome, "canceled");

    /// <summary>
    /// Zero-primes every arm of <see cref="LeafReplayBarrierOutcomes"/> for
    /// <paramref name="treeId"/>, at the moment a barrier is armed and so before any
    /// outcome is known.
    /// </summary>
    /// <remarks>
    /// Priming at the arming site rather than at each terminal site is what makes
    /// the absence of a series meaningful: after this call, a flat zero on
    /// <c>faulted</c> means replays were armed on this tree and none failed,
    /// whereas no series at all means no leaf on this tree ever armed a replay.
    /// Those are different facts and the arming site is the only place that can
    /// distinguish them.
    /// </remarks>
    public static void PrimeReplayBarrierOutcomes(string? treeId)
    {
        var treeTag = new KeyValuePair<string, object?>(TagTree, treeId ?? string.Empty);
        var tenantTag = LatticeTenantLabel.ForTree(treeId ?? string.Empty);

        LeafReplayBarrierOutcomes.Add(0, treeTag, ReplayBarrierCompleted, tenantTag);
        LeafReplayBarrierOutcomes.Add(0, treeTag, ReplayBarrierFaulted, tenantTag);
        LeafReplayBarrierOutcomes.Add(0, treeTag, ReplayBarrierCanceled, tenantTag);
    }

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a starvation drive that
    /// advanced the leaf's persisted checkpoint <b>and</b> left no checkpointed
    /// partition uncovered, so its durable materialiser pin resolves to a real
    /// offset (issue #2692 Half B).
    /// </summary>
    /// <remarks>
    /// The only affirmative arm in the drive set, and the one that answers the
    /// question <c>healed</c> could not. <c>healed</c> is credited when a
    /// consumer stops blocking, which is a tree-level reading taken a pass
    /// later; this is credited per leaf at the moment of repair, so a sweep that
    /// repairs leaves while the tree stays blocked for an unrelated reason is
    /// distinguishable from one that repairs nothing. Both halves of the pin
    /// predicate are asserted before this is emitted, because a checkpoint that
    /// advances while its partition stays uncovered leaves the tree blocked
    /// exactly as it was.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationDroveLifted =
        new(TagOutcome, "drove_lifted");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a starvation drive that
    /// ran to completion and left the leaf's pin still unusable (issue #2692
    /// Half B).
    /// </summary>
    /// <remarks>
    /// Not a failure and not an error: the checkpoint advance is clamped behind
    /// any unresolved prepared-saga mutation and is re-asserted monotonic before
    /// it is written, so a drive can execute in full and lift nothing. This arm
    /// is what makes the sweep readable at all. Its predecessor touched the leaf
    /// with a read-only call, reported <c>Completed</c>, and left no series
    /// anywhere separating "drove the leaf and lifted its pin" from "reached the
    /// leaf and achieved nothing" - so the sweep was not failing loudly, it was
    /// succeeding vacuously. Collapsing this into
    /// <see cref="BlockedLeafReactivationDroveLifted"/> reproduces that defect.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationDroveNoAdvance =
        new(TagOutcome, "drove_no_advance");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a starvation drive
    /// abandoned because the process was under heap pressure (issue #2692
    /// Half B).
    /// </summary>
    /// <remarks>
    /// Kept apart from <see cref="BlockedLeafReactivationDroveNoAdvance"/>
    /// because the two call for opposite responses. A leaf that drove and lifted
    /// nothing is blocked on something structural and more attempts will not
    /// help; a leaf refused for heap pressure was never given its chance, and
    /// retrying once pressure lifts is the correct remedy. Folded together they
    /// would report a transient resource stall as a permanent structural block,
    /// which is the reading that would stop anyone looking further.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationDroveMemoryRefused =
        new(TagOutcome, "drove_memory_refused");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a drive that reached a
    /// leaf with no tree id bound, which does no replay and must not consume a
    /// replay permit (issue #2692 Half B).
    /// </summary>
    /// <remarks>
    /// Expected to stay at zero. It is published because the sweep selects
    /// leaves reported as blocking consumers, and one that turns out to have no
    /// tree id would mean the blocking report and the grain disagree - worth
    /// seeing as a number rather than inferring from the absence of the other
    /// arms.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationDroveNotDriven =
        new(TagOutcome, "drove_not_driven");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a drive declined because
    /// one was already in flight on that activation (issue #2692 Half B).
    /// </summary>
    /// <remarks>
    /// Separates sweep contention from leaf starvation. Without it a leaf whose
    /// drives keep colliding is indistinguishable from one that keeps driving
    /// and lifting nothing, and only the second is a reason to look at the leaf.
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationDroveAlreadyDriving =
        new(TagOutcome, "drove_already_driving");

    /// <summary>
    /// <see cref="TagOutcome"/> value on
    /// <see cref="WalGcBlockedLeafReactivations"/> for a drive abandoned at
    /// <see cref="LatticeOptions.StarvationDriveBudget"/>, its replay permit
    /// forcibly released and its in-flight latch cleared (issue #3065).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A first-class arm rather than a fold into
    /// <see cref="BlockedLeafReactivationDroveNoAdvance"/>, because an abandoned
    /// drive and a drive that ran to completion without advancing are different
    /// events with different remedies: the first says storage did not answer
    /// within the budget, the second says there was nothing to absorb. Reporting
    /// the first as the second is the misfiling the verdict enum's contract
    /// forbids, and <c>DriveOutcomeTag</c> throwing on an unmapped member is the
    /// mechanism that forces this arm to exist.
    /// </para>
    /// <para>
    /// <b>Structurally silent in the wedged case, and that is expected.</b> This
    /// arm is recorded scheduler-side, from the value the touch returns; the
    /// touch abandons at the Orleans response deadline, which is well below the
    /// drive budget, so a drive that actually reaches its budget has already
    /// been given up on by the caller that would have recorded this. It counts
    /// abandonments the scheduler stayed to witness. The grain-side
    /// <see cref="WalReplayStarvationDriveAbandonments"/> counts all of them and
    /// is the arm to read when diagnosing a wedged gate.
    /// </para>
    /// </remarks>
    public static readonly KeyValuePair<string, object?> BlockedLeafReactivationDroveTimedOut =
        new(TagOutcome, "drove_timed_out");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="LeafByteOverflows"/> for a
    /// leaf that was over the byte bound and was divided back under it.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafByteOverflowSplit =
        new(TagOutcome, "split");

    /// <summary>
    /// <see cref="TagOutcome"/> value on <see cref="LeafByteOverflows"/> for a
    /// leaf that is over the byte bound and cannot be divided, because it holds
    /// a single entry larger than the bound and a split has no median key to
    /// pivot on. Splitting anyway would move every entry to the sibling and
    /// leave an empty donor, re-triggering forever without making progress, so
    /// the leaf is deliberately left intact and reported here instead.
    /// </summary>
    public static readonly KeyValuePair<string, object?> LeafByteOverflowIrreducible =
        new(TagOutcome, "irreducible");

    /// <summary>
    /// Counter of resident unresolved saga prepares recorded into
    /// <c>LeafNodeState.UnresolvedReplayWork</c> <b>beyond</b> the
    /// <see cref="LatticeOptions.MaxDurableUnresolvedReplayWork"/> cap, emitted
    /// by <c>BPlusLeafGrain.EnsureUnresolvedPrepareRecorded</c> (issue #2183).
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// This exists for a PROVIDER-DEPENDENT hazard, not for the deployment this
    /// repository runs. A resident prepare must never be dropped (dropping it
    /// pins the flush ceiling forever - the #2183 livelock), so past the cap it
    /// is recorded unconditionally and the row is allowed to grow for as long
    /// as a saga leaves a prepare unresolved (registry status InFlight: the
    /// residual population after issue #2190's self-terminalisation, whose
    /// orphan source is tracked as issue #2304). On the default <c>local</c>
    /// durability profile that row is backed by SQLite (~1GB BLOB), so the
    /// growth is a write-amplification cost, not a correctness one. On an
    /// <c>Orleans.Lattice.Storage.AzureTable</c> deployment the 1MB entity cap
    /// makes an unbounded row a genuine persist hazard, and that operator has
    /// no other signal before the write fails. This counter (and the paired
    /// one-shot warning) is that signal. It is observability ONLY: nothing here
    /// caps or drops a prepare - a behavioural cap would reintroduce the exact
    /// drop-and-freeze defect issue #2183 removes. Do not delete it because it
    /// reads as dead weight on SQLite; it is dead weight on SQLite by design.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafUnresolvedPrepareLedgerBeyondCap =
        Meter.CreateCounter<long>("orleans.lattice.leaf.unresolved_prepare_ledger_beyond_cap", unit: "{prepare}",
            description: "Resident unresolved saga prepares recorded beyond the MaxDurableUnresolvedReplayWork cap, tagged by tree and WAL partition. Provider-dependent persist hazard on Azure Table (1MB entity cap); benign on the SQLite local profile.");

    /// <summary>
    /// Deferred terminals (<c>TxCommit</c>, <c>TxAbort</c>, <c>DeleteRange</c>)
    /// that pass 1 of replay could NOT record durably because the leaf's
    /// <c>UnresolvedReplayWork</c> ledger was already at
    /// <c>MaxDurableUnresolvedReplayWork</c>, and which therefore fell back to
    /// the pre-#2165 in-memory clamp (issue #2756). Tagged <c>tree</c> and
    /// <c>partition</c>.
    /// <para>
    /// This is the counterpart of
    /// <see cref="LeafUnresolvedPrepareLedgerBeyondCap"/> for the OTHER ledger
    /// arm, and it is not interchangeable with it. That counter is emitted by
    /// the prepare recorder, which is uncapped by design and records
    /// unconditionally; this one is emitted by the capped deferred-terminal
    /// recorder, at the point where a terminal is DROPPED. Different ledger,
    /// different policy, opposite outcome.
    /// </para>
    /// <para>
    /// It exists because that drop was previously silent - no metric, no log,
    /// no counter - while being able to pin the replay checkpoint and so block
    /// WAL reclamation for the whole tree. On a non-transactional tree (one
    /// that runs no sagas and therefore carries no prepares at all) the
    /// deferred-terminal clamp is the ONLY clamp that can fire, so without this
    /// instrument a frozen tree is indistinguishable between "this clamp is
    /// pinning the checkpoint" and "this clamp never fired and the cause is
    /// elsewhere".
    /// </para>
    /// <para>
    /// Note the neighbouring counter cannot be used as a proxy for it even on a
    /// tree that does run sagas. It fires on <c>work.Count &gt; cap</c>, so a
    /// ledger resting at EXACTLY the cap drops every subsequent terminal
    /// forever while leaving it at zero - it is a near-miss detector that is
    /// blind at precisely the value where this clamp bites. That boundary is
    /// now inclusive for the same reason.
    /// </para>
    /// <para>
    /// Pre-minted at zero per (tree, partition) when a partition enters replay,
    /// so an absent series means the build did not land rather than that the
    /// clamp never fired. Observability only: the drop behaviour is unchanged.
    /// </para>
    /// </summary>
    public static readonly Counter<long> LeafDeferredTerminalsDroppedAtCap =
        Meter.CreateCounter<long>("orleans.lattice.leaf.deferred_terminals_dropped_at_cap", unit: "{terminal}",
            description: "Deferred terminals (TxCommit, TxAbort, DeleteRange) dropped by replay pass 1 because the durable UnresolvedReplayWork ledger was at the MaxDurableUnresolvedReplayWork cap, falling back to the in-memory clamp that can pin the replay checkpoint. Tagged by tree and WAL partition.");

    // --- Storage-usage instruments (byte-accurate retained footprint) ------
    //
    // The four byte gauges and the over-threshold gauge are observable gauges
    // registered lazily by LatticeStorageUsageMetrics (so they cost nothing
    // when no listener is attached and are not created at all when the host
    // never wires the storage-usage singleton). Their canonical names are
    // exposed here as `...Name` constants so the dashboards drift-guard test
    // recognises the PromQL token forms even though the instruments are not
    // statically constructed on this meter. The two policy counters are
    // ordinary counters constructed on the meter below.

    /// <summary>Canonical name of the observable gauge reporting per-tree retained WAL bytes (tagged <see cref="TagTree"/>).</summary>
    public const string StorageWalBytesName = "orleans.lattice.storage.wal_bytes";

    /// <summary>Canonical name of the observable gauge reporting per-tree snapshot blob bytes (tagged <see cref="TagTree"/>).</summary>
    public const string StorageSnapshotBytesName = "orleans.lattice.storage.snapshot_bytes";

    /// <summary>Canonical name of the observable gauge reporting per-tree summed leaf/shard-root state bytes (tagged <see cref="TagTree"/>).</summary>
    public const string StorageLeafStateBytesName = "orleans.lattice.storage.leaf_state_bytes";

    /// <summary>Canonical name of the observable gauge reporting the per-tree sum of the three storage surfaces (tagged <see cref="TagTree"/>).</summary>
    public const string StorageTotalBytesName = "orleans.lattice.storage.total_bytes";

    /// <summary>Canonical name of the observable 0/1 gauge that flags a tree whose retained WAL bytes currently breach the advisory ceiling (tagged <see cref="TagTree"/>).</summary>
    public const string StoragePolicyOverThresholdName = "orleans.lattice.storage.policy.over_threshold";

    /// <summary>
    /// Canonical name of the observable 0/1 gauge reporting whether a tree's
    /// storage usage has ever been measured at <i>depth</i> - that is, whether
    /// a deep report carrying real snapshot and leaf-state byte counts has been
    /// published for it (tagged <see cref="TagTree"/>). Issue #2693.
    /// <para>
    /// <c>1</c> means <see cref="StorageSnapshotBytesName"/>,
    /// <see cref="StorageLeafStateBytesName"/>, and
    /// <see cref="StorageTotalBytesName"/> carry a real measurement for the
    /// tree. <c>0</c> means only the cheap WAL-only refresh path has run, so
    /// those three gauges publish <b>no measurement</b> for that tree and
    /// <see cref="StorageWalBytesName"/> is the only byte surface that has been
    /// sampled. The gauge itself reports no measurement for a tree that has not
    /// been observed at all, so the three states - never seen, seen WAL-only,
    /// and deeply measured - are all distinguishable from
    /// <c>/metrics</c> alone without reading the source.
    /// </para>
    /// </summary>
    public const string StorageUsageDeepPublishedName = "orleans.lattice.storage.usage_deep_published";

    /// <summary>
    /// Counter incremented once per <see cref="ILatticeWalGc.RunOnceAsync"/>
    /// pass that observes a tree's pre-trim retained WAL bytes exceeding the
    /// configured advisory ceiling
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>) and therefore
    /// schedules a byte-pressure trim, tagged with <see cref="TagTree"/> and
    /// <see cref="TagReason"/> (<c>byte_pressure</c>). The trim itself never
    /// crosses the safe frontier; this counter records that the policy acted,
    /// not that the ceiling was met. Not emitted when the policy is disabled
    /// or the WAL provider does not support byte accounting.
    /// </summary>
    public static readonly Counter<long> StoragePolicyTrimTriggered =
        Meter.CreateCounter<long>(StoragePolicyTrimTriggeredName, unit: "{trim}",
            description: "Byte-pressure trim passes scheduled by the advisory storage policy, tagged by tree and reason.");

    /// <summary>Canonical name of <see cref="StoragePolicyTrimTriggered"/>.</summary>
    public const string StoragePolicyTrimTriggeredName = "orleans.lattice.storage.policy.trim_triggered";

    /// <summary>
    /// Counter of WAL bytes freed by a byte-pressure-triggered trim pass
    /// (pre-trim retained bytes minus post-trim retained bytes), tagged with
    /// <see cref="TagTree"/>. Emitted alongside
    /// <see cref="StoragePolicyTrimTriggered"/>; zero-reclaim passes (a
    /// lagging consumer pinned every byte) do not emit so a perpetually
    /// over-ceiling tree with no caught-up consumer produces no reclaim
    /// traffic.
    /// </summary>
    public static readonly Counter<long> StoragePolicyBytesReclaimed =
        Meter.CreateCounter<long>(StoragePolicyBytesReclaimedName, unit: "By",
            description: "WAL bytes freed by byte-pressure-triggered trim passes, tagged by tree.");

    /// <summary>Canonical name of <see cref="StoragePolicyBytesReclaimed"/>.</summary>
    public const string StoragePolicyBytesReclaimedName = "orleans.lattice.storage.policy.bytes_reclaimed";

    /// <summary><see cref="TagReason"/> = <c>byte_pressure</c> (advisory storage-policy trim attribution).</summary>
    public static readonly KeyValuePair<string, object?> ReasonBytePressure = new(TagReason, "byte_pressure");

    // --- Per-tree admission-control instruments ----------------------------
    //
    // Four observable gauges (live_keys, estimated_bytes, over_advisory,
    // utilization) are registered lazily by LatticeAdmissionMetrics - they read
    // the cached per-tree admission aggregate on scrape and cost nothing when no
    // listener is attached, exactly like the storage gauges - so their canonical
    // names are exposed here as `...Name` constants for the dashboard drift
    // guard. The two would_reject / rejected counters are ordinary counters
    // constructed on the meter below. All are tagged with `tree`; the utilisation
    // gauge and the two counters additionally carry the low-cardinality
    // `dimension` = keys | bytes tag.

    /// <summary>Canonical name of the observable gauge reporting a tree's current live (non-tombstone) key count (tagged <see cref="TagTree"/>).</summary>
    public const string AdmissionLiveKeysName = "orleans.lattice.admission.live_keys";

    /// <summary>Canonical name of the observable gauge reporting a tree's current estimated retained bytes (tagged <see cref="TagTree"/>). May alias <see cref="StorageTotalBytesName"/>.</summary>
    public const string AdmissionEstimatedBytesName = "orleans.lattice.admission.estimated_bytes";

    /// <summary>Canonical name of the observable 0/1 gauge that flags a tree currently exceeding its advisory admission ceiling (tagged <see cref="TagTree"/>).</summary>
    public const string AdmissionOverAdvisoryName = "orleans.lattice.admission.over_advisory";

    /// <summary>Canonical name of the observable ratio gauge reporting current / ceiling per <see cref="TagDimension"/> (tagged <see cref="TagTree"/>).</summary>
    public const string AdmissionUtilizationName = "orleans.lattice.admission.utilization";

    /// <summary>
    /// Counter incremented once per write that <i>would</i> have been rejected
    /// at a tree's advisory admission ceiling (the dry-run blast radius of a
    /// candidate cap), tagged with <see cref="TagTree"/> and
    /// <see cref="TagDimension"/>. Never rejects a write; pairs with
    /// <see cref="AdmissionOverAdvisoryName"/> to right-size a cap before
    /// enforcement is enabled.
    /// </summary>
    public static readonly Counter<long> AdmissionWouldReject =
        Meter.CreateCounter<long>(AdmissionWouldRejectName, unit: "{write}",
            description: "Writes that would be rejected at the advisory admission ceiling, tagged by tree and dimension.");

    /// <summary>Canonical name of <see cref="AdmissionWouldReject"/>.</summary>
    public const string AdmissionWouldRejectName = "orleans.lattice.admission.would_reject";

    /// <summary>
    /// Counter incremented once per write actually rejected by an enforced
    /// admission cap (<see cref="LatticeQuotaExceededException"/> thrown), tagged
    /// with <see cref="TagTree"/> and <see cref="TagDimension"/>. Confirms
    /// enforcement is live and surfaces the offending tree(s).
    /// </summary>
    public static readonly Counter<long> AdmissionRejected =
        Meter.CreateCounter<long>(AdmissionRejectedName, unit: "{write}",
            description: "Writes rejected by an enforced admission cap, tagged by tree and dimension.");

    /// <summary>Canonical name of <see cref="AdmissionRejected"/>.</summary>
    public const string AdmissionRejectedName = "orleans.lattice.admission.rejected";

    // --- WAL compression-savings instruments (per-row payload compression) --
    //
    // Three ordinary counters constructed on the meter, emitted once per
    // append batch by a WAL provider that compresses entry payloads (e.g.
    // the Azure Table provider's default-on Zstd path). The savings ratio is
    // derived in the dashboard as 1 - stored/uncompressed; exposing two
    // monotonic byte totals (rather than an observable savings gauge) means
    // no staleness-horizon handling and the totals survive activation churn.

    /// <summary>
    /// Counter of pre-compression encoded WAL payload bytes a provider
    /// attempted to store, summed per append batch and tagged with
    /// <see cref="TagTree"/>. Paired with <see cref="StorageWalStoredBytes"/>:
    /// the compression savings ratio for a tree is
    /// <c>1 - stored_bytes / uncompressed_bytes</c>. Counts the encoded
    /// length regardless of whether compression was applied, so a tree whose
    /// payloads all skip compression reports equal uncompressed and stored
    /// totals.
    /// </summary>
    public static readonly Counter<long> StorageWalUncompressedBytes =
        Meter.CreateCounter<long>(StorageWalUncompressedBytesName, unit: "By",
            description: "Pre-compression encoded WAL payload bytes, summed per append batch and tagged by tree.");

    /// <summary>Canonical name of <see cref="StorageWalUncompressedBytes"/>.</summary>
    public const string StorageWalUncompressedBytesName = "orleans.lattice.storage.wal.uncompressed_bytes";

    /// <summary>
    /// Counter of post-compression WAL payload bytes a provider actually
    /// stored, summed per append batch and tagged with <see cref="TagTree"/>.
    /// When a row skips compression (disabled, below the size threshold, or
    /// caught by the inflation guard) the verbatim length is counted, so this
    /// total never exceeds <see cref="StorageWalUncompressedBytes"/> for the
    /// same tree.
    /// </summary>
    public static readonly Counter<long> StorageWalStoredBytes =
        Meter.CreateCounter<long>(StorageWalStoredBytesName, unit: "By",
            description: "Post-compression stored WAL payload bytes, summed per append batch and tagged by tree.");

    /// <summary>Canonical name of <see cref="StorageWalStoredBytes"/>.</summary>
    public const string StorageWalStoredBytesName = "orleans.lattice.storage.wal.stored_bytes";

    /// <summary>
    /// Counter of WAL rows stored verbatim instead of compressed, tagged with
    /// <see cref="TagTree"/> and <see cref="TagReason"/>
    /// (<c>below_threshold</c>, <c>inflation_guard</c>, or <c>disabled</c>).
    /// Lets a dashboard attribute a low savings ratio to the dominant skip
    /// cause so an operator can tune
    /// <see cref="LatticeOptions"/>-adjacent provider thresholds rather than
    /// guess. Rows that were actually compressed do not increment this
    /// counter.
    /// </summary>
    public static readonly Counter<long> StorageWalCompressionSkipped =
        Meter.CreateCounter<long>(StorageWalCompressionSkippedName, unit: "{row}",
            description: "WAL rows stored verbatim instead of compressed, tagged by tree and skip reason.");

    /// <summary>Canonical name of <see cref="StorageWalCompressionSkipped"/>.</summary>
    public const string StorageWalCompressionSkippedName = "orleans.lattice.storage.wal.compression_skipped";

    /// <summary><see cref="TagReason"/> = <c>below_threshold</c> (payload shorter than the provider's compression size threshold).</summary>
    public static readonly KeyValuePair<string, object?> ReasonBelowThreshold = new(TagReason, "below_threshold");

    /// <summary><see cref="TagReason"/> = <c>inflation_guard</c> (compressing did not shrink the payload, so it was stored verbatim).</summary>
    public static readonly KeyValuePair<string, object?> ReasonInflationGuard = new(TagReason, "inflation_guard");

    /// <summary><see cref="TagReason"/> = <c>disabled</c> (compression is not enabled on the provider).</summary>
    public static readonly KeyValuePair<string, object?> ReasonCompressionDisabled = new(TagReason, "disabled");

    // --- WAL append diagnostic instruments (WalShardGrain) ------------------
    //
    // Phase A horizontal-scaling diagnostics. These instruments are *only*
    // emitted on the WAL append hot path; they attribute caller-visible
    // append latency to grain-side queueing (turn wait), batching depth
    // (batch entries / bytes), and storage-provider time (provider
    // duration). They are intentionally split from the leaf-level
    // <see cref="LeafCommitDuration"/> histogram because the leaf grain
    // measures total commit duration *including* the cross-grain RPC to
    // the WAL shard - this set isolates the WAL grain's own contribution.

    /// <summary>
    /// Histogram of per-flush batch entry counts, observed at the point
    /// <c>WalShardGrain</c> hands a pending batch to the storage
    /// provider. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// A flat distribution near <see cref="LatticeOptions.WalMaxBatchEntries"/>
    /// indicates the WAL is batching effectively; a distribution
    /// concentrated near 1 indicates the per-batch caps are never
    /// reached and the in-flight cap is the actual throughput limit.
    /// </summary>
    public static readonly Histogram<int> WalAppendBatchEntries =
        Meter.CreateHistogram<int>("orleans.lattice.wal.append.batch_entries", unit: "{entry}",
            description: "Entry count per WAL grain flush, observed at provider hand-off.");

    /// <summary>
    /// Histogram of per-flush batch payload bytes, observed at the
    /// point <c>WalShardGrain</c> hands a pending batch to the storage
    /// provider. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// </summary>
    public static readonly Histogram<long> WalAppendBatchBytes =
        Meter.CreateHistogram<long>("orleans.lattice.wal.append.batch_bytes", unit: "By",
            description: "Encoded-payload bytes per WAL grain flush, observed at provider hand-off.");

    /// <summary>
    /// Histogram of the in-flight flush count snapshot taken at the
    /// moment <c>WalShardGrain</c> starts a new flush. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>. A distribution
    /// pinned at <c>0</c> means the WAL is fully serialised under the
    /// configured <see cref="LatticeOptions.WalMaxPendingBatches"/>;
    /// non-zero values prove pipelined provider calls.
    /// </summary>
    public static readonly Histogram<int> WalAppendInFlight =
        Meter.CreateHistogram<int>("orleans.lattice.wal.append.in_flight", unit: "{flush}",
            description: "In-flight flush count snapshot taken at the start of a new WAL flush.");

    /// <summary>
    /// Histogram of <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>
    /// wall-clock duration, observed by <c>WalShardGrain</c>. Tagged
    /// with <see cref="TagTree"/> and <see cref="TagShard"/>. This is
    /// the storage-provider's contribution to caller-visible append
    /// latency; subtracting it from <see cref="WalAppendTurnWait"/>
    /// gives the grain-side queueing tax.
    /// </summary>
    public static readonly Histogram<double> WalAppendProviderDuration =
        Meter.CreateHistogram<double>("orleans.lattice.wal.append.provider.duration", unit: "ms",
            description: "Wall-clock duration of IWalStorageProvider.AppendEncodedBatchAsync, observed by WalShardGrain.");

    /// <summary>
    /// Histogram of caller-visible WAL append latency, measured from
    /// the moment <c>AppendAsync</c> / <c>AppendBatchAsync</c> admits
    /// an entry to the moment the corresponding ack TCS completes.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Includes time spent waiting for the in-flight cap to drain,
    /// time spent in the pending batch before cutover, the
    /// provider's <see cref="WalAppendProviderDuration"/>, and any
    /// grain-turn dispatch overhead.
    /// </summary>
    public static readonly Histogram<double> WalAppendTurnWait =
        Meter.CreateHistogram<double>("orleans.lattice.wal.append.turn_wait", unit: "ms",
            description: "Caller-visible WAL append duration (entry admission to ack), observed by WalShardGrain.");

    /// <summary>
    /// Histogram of the pending-segments queue depth observed at the
    /// moment a per-entry <c>AppendAsync</c> call enqueues its
    /// segment. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagShard"/>. The value is <c>_pendingSegments.Count</c>
    /// *after* the new segment has been added, so a value of 1 means
    /// the entry arrived to an empty pending batch.
    /// </summary>
    public static readonly Histogram<int> WalAppendQueueDepth =
        Meter.CreateHistogram<int>("orleans.lattice.wal.append.queue_depth", unit: "{entry}",
            description: "Pending-batch depth observed at the moment a WAL append enqueues its segment.");

    /// <summary>
    /// Histogram of the cross-grain dispatch duration into
    /// <c>IWalShardGrain.AppendAsync</c> / <c>AppendBatchAsync</c>,
    /// observed by <c>WalCommitLogWriter</c>. Clocked around the
    /// awaited grain RPC on the caller side, so the value includes the
    /// Orleans turn-queue wait on the target <c>WalShardGrain</c>
    /// activation, the RPC serialisation overhead, and the WAL grain's
    /// own body time. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagShard"/> (the WAL partition index, identical to
    /// the <c>WalShardGrain</c>'s own shard tag) plus the Phase A
    /// attribution tags <see cref="TagWalPartitions"/> and
    /// <see cref="TagWalMaxPendingBatches"/>.
    /// <para>
    /// Subtracting <see cref="WalAppendTurnWait"/> (the WAL grain's
    /// own self-clock) from this histogram isolates the Orleans
    /// scheduling tax on the single WAL activation per partition: the
    /// time spent in the activation's turn queue plus the RPC
    /// dispatch overhead. Under <c>WalPartitions = 1</c> every leaf
    /// commit funnels through one activation and any commit-path
    /// throughput regression is expected to show up here first.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalShardDispatchDuration =
        Meter.CreateHistogram<double>("orleans.lattice.wal.shard.dispatch.duration", unit: "ms",
            description: "Wall-clock duration of the cross-grain IWalShardGrain.AppendAsync / AppendBatchAsync RPC, observed by WalCommitLogWriter.");

    /// <summary>
    /// Histogram of the per-dispatch entry count handed to
    /// <c>IWalShardGrain.AppendAsync</c> / <c>AppendBatchAsync</c>,
    /// observed by <c>WalCommitLogWriter</c> at the caller side.
    /// Tagged with <see cref="TagTree"/>, <see cref="TagShard"/>
    /// (the WAL partition index), and the Phase A attribution tags
    /// <see cref="TagWalPartitions"/> and
    /// <see cref="TagWalMaxPendingBatches"/>. The single-entry
    /// overload records <c>1</c>; the batched overload records the
    /// per-partition slice size that <c>AppendForPartitionAsync</c>
    /// forwards as one <c>AppendBatchAsync</c> call.
    /// <para>
    /// Pair with <see cref="WalAppendBatchEntries"/> (the WAL grain's
    /// observed per-flush packing) to detect a missing
    /// cross-AppendBatchAsync coalescing window: if the writer-side
    /// dispatch entry count equals the WAL grain's per-flush packing
    /// under steady-state fan-in, each leaf's dispatch flushes as its
    /// own batch and concurrent leaves never merge into a single
    /// pending batch (the <c>WalShardGrain</c> kick predicate
    /// <c>isLast == true</c> triggers a flush at the end of every
    /// caller's batch).
    /// </para>
    /// </summary>
    public static readonly Histogram<int> WalShardDispatchEntries =
        Meter.CreateHistogram<int>("orleans.lattice.wal.shard.dispatch.entries", unit: "{entry}",
            description: "Per-dispatch entry count handed to IWalShardGrain.AppendAsync / AppendBatchAsync, observed by WalCommitLogWriter.");

    // --- Storage-provider commit instruments --------------------------------

    /// <summary>
    /// Histogram of storage-provider commit phase duration. Tagged
    /// with <see cref="TagPhase"/> = <c>phase1</c> (per-batch
    /// partition transaction) or <c>phase2</c> (manifest partition
    /// transaction). Emitted by the Azure Table WAL provider; other
    /// providers may emit it too. The phase-2 measurement covers a
    /// single coalesced commit transaction, not the per-shard
    /// worker's whole drain loop.
    /// </summary>
    public static readonly Histogram<double> ProviderCommitDuration =
        Meter.CreateHistogram<double>("orleans.lattice.provider.commit.duration", unit: "ms",
            description: "Storage-provider commit-transaction wall-clock duration, tagged by phase.");

    /// <summary>
    /// Histogram of the number of coalesced phase-2 commits the
    /// per-shard provider worker bundled into a single transaction.
    /// A distribution concentrated near 1 means the worker is never
    /// catching up against backed-up arrivals; values closer to the
    /// 49-commit per-transaction cap indicate the worker is the
    /// shard's effective rate limiter.
    /// </summary>
    public static readonly Histogram<int> ProviderPhase2BatchSize =
        Meter.CreateHistogram<int>("orleans.lattice.provider.phase2.batch_size", unit: "{commit}",
            description: "Coalesced phase-2 commits per provider-worker transaction.");

    /// <summary>
    /// Counter incremented once per provider call whose retry budget
    /// was exhausted and surfaced an exception. Tagged with
    /// <see cref="TagPhase"/> and <see cref="TagStatus"/> (the HTTP
    /// status string the SDK observed, or <c>unknown</c>). A non-zero
    /// rate signals the storage backend is throttling the shard at
    /// its ceiling.
    /// </summary>
    public static readonly Counter<long> ProviderRetryExhausted =
        Meter.CreateCounter<long>("orleans.lattice.provider.retry.exhausted", unit: "{call}",
            description: "Provider commit calls that exhausted the SDK retry budget and surfaced an exception.");

    /// <summary>
    /// Counter incremented once per phase-2 manifest commit that
    /// exceeded the per-commit deadline and was abandoned by the
    /// per-shard worker. Tagged with <see cref="TagTree"/> and
    /// <see cref="TagShard"/>. A non-zero rate is the direct signal
    /// that the phase-2 drain loop would otherwise have wedged: the
    /// commit's underlying Azure Tables transaction stopped making
    /// progress (a hung socket, a server-side partition stall, or an
    /// SDK retry storm running past the deadline) and the worker
    /// bounded it instead of blocking every later commit on the same
    /// shard indefinitely. Zero on a healthy shard; the pre-fix
    /// behaviour (no deadline) is recoverable by leaving
    /// <c>PhaseTwoCommitTimeout</c> unset, in which case this counter
    /// never increments because no deadline is enforced.
    /// </summary>
    public static readonly Counter<long> ProviderPhase2CommitTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.provider.phase2.commit.timeouts", unit: "{commit}",
            description: "Phase-2 manifest commits abandoned by the per-shard worker after exceeding the configured per-commit deadline.");

    /// <summary>
    /// Counter incremented once per individual retry attempt the
    /// storage SDK performs on a provider call, regardless of whether
    /// the retry ultimately succeeds. Tagged with <see cref="TagStatus"/>
    /// (the HTTP status string of the response that triggered the
    /// retry, e.g. <c>503</c>, <c>429</c>; <c>0</c> when the trigger
    /// was a transport-level exception with no HTTP status). Phase A
    /// discovered a 5-100x gap between wall
    /// p99 (700-1,700 ms) and Azure Tables server-timing p99
    /// (10-130 ms) on the WAL hot path - the canonical signature of
    /// retry storms whose retries ultimately succeed and therefore
    /// never increment <see cref="ProviderRetryExhausted"/>. This
    /// instrument is the counterpart that captures *attempted*
    /// retries so dashboards can attribute wall-time inflation to
    /// SDK backoff without inferring it from the gap.
    /// <para>
    /// Cardinality is intentionally bounded: only the status tag is
    /// emitted (small bounded set of HTTP status codes), not
    /// <see cref="TagTree"/> / <see cref="TagShard"/>. Per-tree /
    /// per-shard attribution is covered by
    /// <see cref="ProviderRetryExhausted"/>, which fires rarely.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ProviderRetryAttempts =
        Meter.CreateCounter<long>("orleans.lattice.provider.retry.attempts", unit: "{attempt}",
            description: "Individual retry attempts performed by the storage SDK on provider calls, tagged by the HTTP status that triggered each retry.");

    /// <summary>
    /// Counter of SDK retry attempts that
    /// <c>SaturationAwareRetryPolicy</c> short-circuited by stamping a
    /// synthetic 503 response onto the message because the silo-scoped
    /// WAL saturation signal reports
    /// <see cref="WalSaturationState.Saturated"/>. Fires only on the
    /// post-saturation-classifier path; first attempts and retries
    /// under <see cref="WalSaturationState.Healthy"/> /
    /// <see cref="WalSaturationState.Throttled"/> never fire it.
    /// Tagged by the synthetic status (<c>503</c> today; reserved for
    /// future expansion).
    /// <para>
    /// Closes the diagnostic gap that the existing
    /// <see cref="ProviderRetryAttempts"/> counter does not
    /// distinguish SDK-driven retries (a transient transport hiccup,
    /// a 429 throttle) from policy-driven short-circuits (the
    /// saturation signal abandoning the retry). A non-zero rate on
    /// this counter is the operator-visible signal that the
    /// saturation policy is doing its job; a zero rate with a
    /// non-zero <see cref="ProviderRetryExhausted"/> means the SDK
    /// burned its full retry budget without the policy intervening,
    /// which is the pre-policy historical behaviour.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ProviderRetryShortCircuited =
        Meter.CreateCounter<long>("orleans.lattice.provider.retry.short_circuited", unit: "{attempt}",
            description: "SDK retry attempts short-circuited by SaturationAwareRetryPolicy because the WAL saturation signal reports Saturated, tagged by the synthetic HTTP status used to abort the retry chain.");

    /// <summary>
    /// Counter incremented once per provider commit call that surfaced a
    /// <c>409 EntityAlreadyExists</c> conflict which was proven to be an
    /// idempotent replay of an already-durable write and therefore
    /// resolved as a success rather than a failure. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagShard"/> and
    /// <see cref="TagPhase"/>. Fires when the storage SDK's retry
    /// pipeline resends a batch whose first attempt committed
    /// server-side but whose response was lost; the provider reads the
    /// resident rows back, confirms they are byte-identical to the
    /// batch it tried to write, and treats the conflict as a no-op
    /// success. A non-zero rate is the operator-visible signal that
    /// lost-response retries are occurring (typically under CPU /
    /// network pressure); crucially these replays are NOT counted on
    /// <see cref="ProviderRetryExhausted"/>, so they never escalate the
    /// WAL saturation classifier. A 409 that is NOT a clean replay (a
    /// genuine offset collision) still surfaces as a hard failure on
    /// <see cref="ProviderRetryExhausted"/> and never increments this
    /// counter.
    /// </summary>
    public static readonly Counter<long> ProviderIdempotentReplays =
        Meter.CreateCounter<long>("orleans.lattice.provider.idempotent_replays", unit: "{call}",
            description: "Provider commit calls whose 409 EntityAlreadyExists conflict was proven to be an idempotent replay of an already-durable write and resolved as a success.");

    /// <summary>
    /// Counts phase-1 commit attempts that the provider re-issued in place after a
    /// <i>transient</i> fault (a timeout, a 408 / 429 / 5xx, or a network-level cancellation
    /// that is not the silo's own drain token). Each retry resubmits the <b>byte-identical</b>
    /// batch at the same offsets, so it never asks the calling <c>WalShardGrain</c> to fault,
    /// resync, and re-drive divergent content - the positive-feedback 409 conflict storm this
    /// counter's retry path exists to prevent. A retry that lands on an already-durable batch
    /// resolves via the idempotent-replay proof (and increments
    /// <see cref="ProviderIdempotentReplays"/>); one that lands on a never-committed batch
    /// simply commits. A non-zero value means the provider absorbed transient phase-1 turbulence
    /// without escalating it to the shard. When the bounded retry budget is exhausted the fault
    /// surfaces on <see cref="ProviderRetryExhausted"/> as before.
    /// </summary>
    public static readonly Counter<long> ProviderPhaseOneTransientRetries =
        Meter.CreateCounter<long>("orleans.lattice.provider.phase1.transient_retries", unit: "{attempt}",
            description: "Phase-1 commit attempts the provider re-issued in place after a transient fault, resubmitting the byte-identical batch at the same offsets rather than faulting the calling WAL shard.");

    // --- Saga fan-out diagnostic instruments (AtomicWriteGrain) -------------

    /// <summary>
    /// Histogram of <c>SetManyAtomicAsync</c> saga entry counts
    /// observed at the moment the saga enters its execute phase
    /// (i.e. once per saga activation, not once per retry). Tagged
    /// with <see cref="TagTree"/>. Distinct from
    /// <see cref="AtomicWriteBatchSize"/>, which is emitted at
    /// terminal transition: this one is emitted at execute-phase
    /// entry so a diagnostic dashboard can correlate fan-out size
    /// with the per-key duration histogram below regardless of
    /// terminal outcome.
    /// </summary>
    public static readonly Histogram<int> SagaFanoutSize =
        Meter.CreateHistogram<int>("orleans.lattice.saga.fanout.size", unit: "{entry}",
            description: "Entry count per atomic-write saga, observed at execute-phase entry.");

    /// <summary>
    /// Histogram of per-key <c>lattice.SetAsync</c> wall-clock
    /// duration inside an atomic-write saga's execute loop. Tagged
    /// with <see cref="TagTree"/>. One observation per successful
    /// or failing key-level await, regardless of whether the saga
    /// later compensates. The 99th-percentile of this histogram is
    /// the dominant signal for whether the saga's serial fan-out
    /// pattern is the throughput limit: it must be added across
    /// all keys to recover the saga's end-to-end duration, so a
    /// 10-entry saga's duration is bounded below by 10 x p50 of
    /// this histogram.
    /// </summary>
    public static readonly Histogram<double> SagaPerKeyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.perkey.duration", unit: "ms",
            description: "Per-key SetAsync duration inside an atomic-write saga execute loop.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside the saga's prepare
    /// phase: from the start of <c>ExecutePhaseAsync</c>'s parallel
    /// batched <c>lattice.SetManyAsync(slice)</c> dispatch to the
    /// moment every per-shard fan-out completes (post-D1c shape -
    /// a single parallel call rather than a per-key loop). Excludes
    /// the saga checkpoint persist that follows the dispatch.
    /// Tagged with <see cref="TagTree"/> and the per-tree WAL
    /// partition count tag.
    /// <para>
    /// Sums with <see cref="SagaTerminalDecisionDuration"/> and
    /// <see cref="SagaBroadcastDuration"/> to approximate the saga's
    /// end-to-end <c>SetManyAtomicAsync</c> p50 (the residue is
    /// saga-checkpoint persist + grain-RPC framing on the public
    /// surface, both negligible at the c2-iii operating point).
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaPrepareDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.prepare.duration", unit: "ms",
            description: "Wall-clock ms inside the saga's parallel-prepare phase (lattice.SetManyAsync(slice) dispatch through per-shard fan-out completion).");

    /// <summary>
    /// Histogram of wall-clock ms spent inside the saga's terminal
    /// decision write: the per-tree
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.MarkCommittedAsync"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.MarkAbortedAsync"/> call that
    /// records the single tree-wide linearization point before the
    /// per-leaf terminal fan-out. Tagged with <see cref="TagTree"/>
    /// and the per-tree WAL partition count tag.
    /// <para>
    /// Per the c2-xv routing memo this is the lowest-prior candidate
    /// for the saga's binding constraint (one grain RPC per saga) but
    /// is instrumented so the attribution is conclusive rather than
    /// inferred.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaTerminalDecisionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.terminal_decision.duration", unit: "ms",
            description: "Wall-clock ms inside the per-tree TxRegistry MarkCommittedAsync / MarkAbortedAsync call.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside the saga's broadcast
    /// terminal phase: from the start of <c>BroadcastTerminalsAsync</c>'s
    /// per-shard fan-out (one <c>IShardRootGrain.AppendTxTerminalAsync</c>
    /// per touched shard, dispatched via <c>Task.WhenAll</c>) to the
    /// moment every per-shard terminal has been appended and the
    /// leaf-side pending-tx buckets drained into the visible
    /// projection. Tagged with <see cref="TagTree"/> and the per-tree
    /// WAL partition count tag.
    /// <para>
    /// Per the c2-xv routing memo this is the highest-prior candidate
    /// for the saga's binding constraint. Each per-shard
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.AppendTxTerminalAsync"/> appends one
    /// WAL record and drains the leaf-side pending-tx bucket; if
    /// per-shard turn-token contention or per-shard WAL-append
    /// serialisation dominates, the histogram's p50 is the per-saga
    /// floor regardless of how parallel the prepare phase is.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.duration", unit: "ms",
            description: "Wall-clock ms inside BroadcastTerminalsAsync's per-shard AppendTxTerminalAsync fan-out.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single
    /// <c>state.WriteStateAsync</c> call on the saga grain
    /// (<c>AtomicWriteGrain</c>). The per-call <see cref="TagPhase"/>
    /// tag identifies which checkpoint site the observation came from
    /// (e.g. <c>prepare</c>, <c>execute-batch-commit</c>,
    /// <c>complete</c>) so dashboards can decompose the per-saga
    /// checkpoint cost across the grain's ~10 distinct persist sites
    /// without joining across instruments.
    /// <para>
    /// Closes the c2-xvi residual-cost attribution gap: the sum of
    /// the three saga-phase histograms
    /// (<see cref="SagaPrepareDuration"/>,
    /// <see cref="SagaTerminalDecisionDuration"/>,
    /// <see cref="SagaBroadcastDuration"/>) accounted for ~1.4s of
    /// the c2-xi-measured 7.7s per-saga p50; the residual ~6.3s
    /// lives in saga-internal state persists which this histogram
    /// attributes. Tagged with <see cref="TagTree"/>,
    /// <see cref="TagWalPartitions"/>, and <see cref="TagPhase"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaCheckpointDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.checkpoint.duration", unit: "ms",
            description: "Wall-clock ms inside a single state.WriteStateAsync on AtomicWriteGrain, tagged with the call-site phase.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside Orleans reminder
    /// registry calls on the saga grain
    /// (<c>AtomicWriteGrain.RegisterKeepaliveAsync</c> and
    /// <c>UnregisterKeepaliveAsync</c>). Each call is an Azure Tables
    /// transaction against the reminder table (one
    /// <c>RegisterOrUpdateReminder</c> at saga entry, one
    /// <c>GetReminder</c> + <c>UnregisterReminder</c> at saga
    /// completion). The per-call <see cref="TagPhase"/> tag
    /// distinguishes the call site (<c>register</c> /
    /// <c>unregister-get</c> / <c>unregister-drop</c>).
    /// <para>
    /// Closes the c2-xvi/c2-xvii unattributed-residual gap: the
    /// c2-xvi-measured sum of phases (~1.4s) plus the c2-xvii-measured
    /// checkpoint persists (~52ms) left ~6.9s of the c2-xi 7.7s saga
    /// p50 unattributed. Reminder I/O is the most plausible
    /// contributor and was not previously instrumented. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagWalPartitions"/>, and
    /// <see cref="TagPhase"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaReminderDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.reminder.duration", unit: "ms",
            description: "Wall-clock ms inside Orleans reminder registry RPCs on AtomicWriteGrain, tagged with the call-site phase.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single
    /// <c>ShardRootGrain.AppendTxTerminalAsync</c> call: the full
    /// per-shard cost of broadcasting one saga's terminal mark, from
    /// the start of step 1 (affected-leaves resolution) through step 4
    /// (per-leaf <c>ApplyTxTerminalAsync</c> fan-out). Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// <para>
    /// Sums (in parallel-Task.WhenAll-fashion) inside the saga grain's
    /// <see cref="SagaBroadcastDuration"/> - the saga p50 of ~880ms is
    /// the max across ~32 parallel shard calls; this histogram surfaces
    /// the per-shard contribution so the broadcast-cost attribution
    /// gap left open by c2-xvii can be closed. Per the c2-xix routing
    /// memo this is the next instrument target before any structural
    /// optimisation of the broadcast path.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastShardDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.shard.duration", unit: "ms",
            description: "Wall-clock ms inside a single ShardRootGrain.AppendTxTerminalAsync call (per-shard broadcast contribution).");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single per-leaf
    /// <c>IBPlusLeafGrain.ApplyTxTerminalAsync</c> RPC dispatched from
    /// <c>ShardRootGrain.BroadcastTerminalToLeavesAsync</c> (step 4
    /// of the per-shard broadcast). Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>.
    /// <para>
    /// The shard-side broadcast fan-out is a single
    /// <c>Task.WhenAll</c> across ~1-2 affected leaves per shard, so
    /// the shard duration is approximately the max of its per-leaf
    /// durations. The gap between
    /// <see cref="SagaBroadcastShardDuration"/> p50 and
    /// <see cref="SagaBroadcastLeafDuration"/> p50 attributes the
    /// non-leaf cost on the shard (affected-leaves resolution, HLC
    /// compute, optional WAL append, the parallel-dispatch scheduler
    /// overhead). Per the c2-xix routing memo.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastLeafDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.leaf.duration", unit: "ms",
            description: "Wall-clock ms inside a single per-leaf ApplyTxTerminalAsync RPC dispatched from the shard's terminal broadcast.");

    /// <summary>
    /// Histogram of wall-clock ms spent inside a single sub-stage of
    /// <c>ShardRootGrain.AppendTxTerminalAsync</c>. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagShard"/>, and
    /// <see cref="TagStage"/> (<c>resolve</c> | <c>hlc</c> | <c>wal</c>
    /// | <c>fanout</c>).
    /// <para>
    /// Per the c2-xxi memo the c2-xx <see cref="SagaBroadcastShardDuration"/>
    /// p50 of ~143ms could not be attributed to leaf-side turn-token
    /// queueing (<c>[AlwaysInterleave]</c> on <c>GetClockAsync</c> did
    /// not move per-shard p50 down). The four sub-stage spans here
    /// split the per-shard envelope into its constituent pieces so the
    /// dominant cost can be identified before any further structural
    /// attempt.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SagaBroadcastShardStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.saga.broadcast.shard.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (resolve|hlc|wal|fanout) of ShardRootGrain.AppendTxTerminalAsync.");

    // --- Shard-root SetManyAsync split instruments ----------------

    /// <summary>
    /// Histogram of wall-clock ms spent inside the local-apply path
    /// of <c>ShardRootGrain.SetManyAsync</c>: from the moment the
    /// shard-root receives a batch to the moment every per-leaf
    /// <c>IBPlusLeafGrain.SetManyAsync</c> dispatched by
    /// <c>SetManyLocalOnlyAsync</c> has returned. Tagged with
    /// <see cref="TagTree"/>. Includes per-leaf RPC scheduling, leaf
    /// turn-queue wait, leaf commit, WAL append, and the WAL provider's
    /// phase-2 commit. Excludes the lattice-grain's per-shard bucket
    /// build and event publish, and excludes the online-resize
    /// shadow-forward task (measured separately by
    /// <see cref="ShardRootSetManyShadowForwardDuration"/>).
    /// </summary>
    public static readonly Histogram<double> ShardRootSetManyLocalApplyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.shard_root.set_many.local_apply.duration", unit: "ms",
            description: "Wall-clock ms inside ShardRootGrain.SetManyLocalOnlyAsync (per-leaf fan-out, leaf commit, WAL append + phase 2).");

    /// <summary>
    /// Histogram of wall-clock ms spent awaiting the trailing
    /// shadow-forward task in <c>ShardRootGrain.SetManyAsync</c>.
    /// Tagged with <see cref="TagTree"/>. Expected to be near zero in
    /// steady state (no active resize): the shadow-forward task
    /// completes synchronously via <c>TrackShadowForward</c>'s
    /// no-resize fast-path. Material values indicate either an active
    /// online resize or an unexpected wait on the resize tracker.
    /// </summary>
    public static readonly Histogram<double> ShardRootSetManyShadowForwardDuration =
        Meter.CreateHistogram<double>("orleans.lattice.shard_root.set_many.shadow_forward.duration", unit: "ms",
            description: "Wall-clock ms awaiting the shadow-forward task at the tail of ShardRootGrain.SetManyAsync.");

    /// <summary>
    /// Count of outbound shard-to-shard write forwards that were abandoned
    /// because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.ShardForwardTimeout"/>.
    /// Tagged with <see cref="TagTree"/>. A non-zero value indicates a
    /// forward parked against a sibling shard whose ownership was changing
    /// during a reshard swap - the parked forward was faulted as a
    /// <see cref="TimeoutException"/> so the foreground write pipeline could
    /// make forward progress and the operation be retried against refreshed
    /// routing. Expected to be zero in steady state; sustained non-zero
    /// counts during a resize indicate the swap phase is taking longer than
    /// the configured forward deadline.
    /// </summary>
    public static readonly Counter<long> ShardForwardTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.forward.timeouts", unit: "{timeout}",
            description: "Count of outbound shard-to-shard write forwards abandoned after exceeding ShardForwardTimeout.");

    /// <summary>
    /// Count of <c>ShardRootGrain</c> activation-readiness seeds that were
    /// abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.ActivationReadyTimeout"/>.
    /// Tagged with <see cref="TagTree"/>. A non-zero value indicates a
    /// first-activation seed (registry registration or root-leaf
    /// initialization) parked - typically because a startup reshard or
    /// membership change left the target activation not-yet-visible - and
    /// was faulted as a <see cref="TimeoutException"/> so the held
    /// activation gate could release and the foreground write pipeline make
    /// progress, with the seed retried against refreshed routing. Expected
    /// to be zero in steady state; sustained non-zero counts during silo
    /// startup or a reshard indicate the seed envelope is exceeding the
    /// configured deadline.
    /// </summary>
    public static readonly Counter<long> ActivationReadyTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.activation_ready.timeouts", unit: "{timeout}",
            description: "Count of shard-root activation-readiness seeds abandoned after exceeding ActivationReadyTimeout.");

    /// <summary>
    /// Count of <c>ShardRootGrain</c> range-scan page fills abandoned because
    /// they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.MaxScanPageStallDuration"/>.
    /// Tagged with <see cref="TagTree"/>, <see cref="TagShard"/> and
    /// <see cref="TagPhase"/>, the last naming how far the page fill had got -
    /// <c>prologue</c>, <c>descent</c>, or <c>leaf-walk</c> - which is what
    /// distinguishes a slow shard prepare from a single leaf read that never
    /// returned.
    /// <para>
    /// Expected to be flat zero: the cooperative
    /// <see cref="Orleans.Lattice.LatticeOptions.MaxScanPageDuration"/> budget
    /// returns a partial page long before this ceiling, so a non-zero rate
    /// means a page fill was stuck inside a single await and was holding its
    /// deliberately non-reentrant shard root against every other request to
    /// that shard (issue 2002). Treat sustained non-zero as a wedge, and read
    /// the phase tag to place it.
    /// </para>
    /// <para>
    /// <b>This counter records only the fires that faulted, not every ceiling
    /// fire.</b> It is raised from the one site that builds the stall fault, and
    /// a fire whose partial page was banked returns that page instead of
    /// throwing, so it records nothing here and contributes no phase tag. The
    /// phase distribution on this counter is therefore the distribution over
    /// <em>discarded</em> fires alone, which is also why its total equals the
    /// <c>discarded</c> arm of <see cref="ScanPageCeilingOutcomes"/> by
    /// construction. Use that counter's sum, not this one, when the question is
    /// how often the ceiling fired at all.
    /// </para>
    /// <para>
    /// <b>Reading a zero.</b> Because a zero is also the expected value under
    /// health, it carries no information on its own: it is what a clean shard
    /// reports and equally what a deployment that never scanned reports. Pair
    /// it with <see cref="LeafScanDuration"/>'s count before reading zero as
    /// clean, per the fuller note on <see cref="ScanPageCeilingOutcomes"/>.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanPageStalls =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.scan_page.stalls", unit: "{stall}",
            description: "Count of shard-root range-scan page fills abandoned after exceeding MaxScanPageStallDuration.");

    /// <summary>
    /// Count of <c>ShardRootGrain</c> page-fill ceiling fires, tagged with what
    /// the ceiling did with the work the walk had already done:
    /// <see cref="TagOutcome"/> = <c>banked</c> (the walk had completed work to
    /// show, so it was returned as a short page and the caller resumes from it)
    /// or <c>discarded</c> (the fire caught the walk with nothing to bank, so
    /// the call faulted with
    /// <see cref="Orleans.Lattice.ScanPageStalledException"/>). Also tagged
    /// with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// <para>
    /// <b>Reading a zero.</b> Both arms are emitted from the one site a ceiling
    /// fire passes through, so their sum is the ceiling-fire count and neither
    /// arm needs a denominator supplied from elsewhere.
    /// </para>
    /// <para>
    /// <b>A zero on the <c>banked</c> arm has three distinct causes and does not
    /// separate them on its own.</b> In particular it does <i>not</i> indicate a
    /// prologue or descent that parks. An earlier revision of this note said so,
    /// and the phase tag beside it refutes that reading: a fire that parks before
    /// the leaf chain never reaches <c>ScanPagePhase.LeafWalk</c>, so it is
    /// tagged <c>prologue</c> or <c>descent</c> on <see cref="ScanPageStalls"/>
    /// rather than <c>leaf-walk</c>.
    /// </para>
    /// <para>
    /// <b>Cause 1, and the one seen in the field: the fire landed inside the
    /// page's first leaf read.</b> The accumulator is published before the phase
    /// flips to <c>leaf-walk</c>, and a leaf contributes its rows only once its
    /// read returns in full, so a read still in flight has banked nothing. A
    /// <c>leaf-walk</c> stall with <c>banked=0</c> on a paging operation
    /// therefore means no leaf read had completed at all. Banking cannot help
    /// this shape, and where the range fits in a single leaf it never can, since
    /// there is no earlier completed leaf to have contributed rows however small
    /// that leaf is. The mitigation is the read coalescing reported by
    /// <see cref="ScanPageLeafReadOutcomes"/>, not banking: read its
    /// <c>joined</c> arm against <see cref="ScanPageStalls"/> to tell a retry
    /// that attaches to the in-flight read from one that enqueues another behind
    /// it.
    /// </para>
    /// <para>
    /// <b>Cause 2: the operation cannot bank at all.</b> Two stall-guarded
    /// operations still record <c>discarded</c> however many leaves had
    /// completed, and both are deliberate rather than unwired.
    /// <c>CaptureSnapshotBaselineAsync</c> has no meaningful partial - a
    /// baseline covering part of a chain is not a baseline.
    /// <c>DeleteRangeBoundedAsync</c> publishes its replication notification
    /// after the walk, so a banked resume key would carry the caller past a
    /// prefix whose tombstones were applied locally and never published,
    /// orphaning that closure permanently; the fault is retried from the range
    /// start instead, which re-publishes it. On those two series
    /// <c>banked=0</c> is not a statement about the walk at all.
    /// </para>
    /// <para>
    /// Before issue 2807 this cause covered ten of the sixteen guarded
    /// operations, because banking required the core method to publish a row
    /// accumulator through <c>BeginScanPageRows</c> and to return
    /// <c>KeysPage</c> or <c>EntriesPage</c> - the only two shapes
    /// <c>TryBankPartialScanPage</c> could construct - which the counting, any,
    /// diagnostics, storage-usage, projection-rebuild and materialiser-lag
    /// pages all failed. Those eight now publish a finished partial page at
    /// each leaf boundary through <c>PublishScanPagePartial</c>, so their fires
    /// bank. A dashboard or alert written against the older reading - that a
    /// count-heavy or diagnostics-heavy tree necessarily shows a zero
    /// <c>banked</c> arm - is measuring the old behaviour and will now
    /// misreport.
    /// </para>
    /// <para>
    /// <b>Cause 3: every row read so far was filtered out.</b> Moved-away slots
    /// are skipped before they reach the accumulator, so a page that has walked
    /// several completed leaves holding only moved-away entries still banks
    /// nothing. Expect this only while a shard is consolidating.
    /// </para>
    /// <para>
    /// <b>The banked-to-discarded ratio is therefore mix-dependent and is not a
    /// health signal on its own.</b> A shard whose scan traffic is dominated by
    /// range deletes or snapshot baselines reports a zero <c>banked</c> arm
    /// however healthy its leaf reads are, while a shard serving key, entry,
    /// count and diagnostics pages reports a ratio that does track first-leaf
    /// health. Comparing the ratio across trees compares
    /// their operation mixes as much as their leaf latency; separate the two
    /// before attributing a difference to either.
    /// </para>
    /// <para>
    /// The reading this arm is designed to
    /// close off is the one where a series carries no points at all: if this
    /// counter is absent while
    /// <see cref="ScanPageStalls"/> is climbing, the banking path is not wired
    /// up, and that is a broken measurement rather than a clean shard.
    /// <c>banked</c> being zero and <c>discarded</c> also being zero means only
    /// that no ceiling fired. That is the healthy steady state <i>only</i>
    /// beside independent evidence that scans ran at all: on an idle
    /// deployment both arms read zero because nothing reached the fire, which
    /// is an absence of activity and not an absence of defect. This instrument
    /// cannot tell those two zeros apart, because both arms are emitted only
    /// by a fire that a scan has to reach. Supply the activity evidence from
    /// <see cref="LeafScanDuration"/>, whose count series is recorded
    /// immediately before the single return of each leaf key and entry scan.
    /// Read the pair, and note which direction is sound: a non-zero scan count
    /// beside zero on both arms is a measured clean shard, whereas zero on all
    /// three is no evidence either way. That count is biased low, because a
    /// scan that faults before returning never records, so a zero count means
    /// <i>no scan completed</i> rather than <i>no scan was attempted</i> - and
    /// neither of those licenses a clean bill of health.
    /// </para>
    /// <para>
    /// <c>discarded</c> equals <see cref="ScanPageStalls"/> by construction -
    /// the same fire raises both - so a divergence between them is itself a
    /// wiring fault worth alerting on.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanPageCeilingOutcomes =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.scan_page.ceiling_outcomes", unit: "{fire}",
            description: "Count of shard-root page-fill stall-ceiling fires by whether the partial page was banked or discarded.");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>banked</c> (a ceiling fire returned the
    /// rows the walk had already read, as a short page).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanPageBankedTag =
        new(TagOutcome, "banked");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>discarded</c> (a ceiling fire found no
    /// rows to bank, so the call faulted).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanPageDiscardedTag =
        new(TagOutcome, "discarded");

    /// <summary>
    /// Count of bounded leaf reads issued by a stall-guarded shard-root page
    /// fill, tagged with whether the read was issued or attached to one
    /// already in flight (issue 2585).
    /// <para>
    /// This is the convergence counterpart to
    /// <see cref="ScanPageCeilingOutcomes"/>. That counter reports whether a
    /// ceiling fire kept the rows it had; this one reports whether the
    /// <em>next</em> attempt had to pay for them again. A livelocked walk shows
    /// <c>issued</c> climbing in step with
    /// <see cref="ScanPageStalls"/> while <c>joined</c> stays
    /// at zero: every retry re-reading the same leaf from scratch is the
    /// signature of the defect.
    /// </para>
    /// <para>
    /// <b>The third arm, <c>served</c>, is real and is not recency-based
    /// reuse.</b> An earlier revision of this fix did retain settled results for
    /// one ceiling on the reasoning that the window was short, emitted a
    /// <c>served</c> arm for it, and was wrong in kind rather than in degree:
    /// once the leaf's turn ends later writes are ordered after the read, so
    /// replaying its rows returns a scan page that misses committed writes,
    /// which is incorrect at any window length. That clause and its arm were
    /// removed outright. Issue #2786 reinstated a <c>served</c> arm on a
    /// different and sufficient basis - the leaf's activation-fenced revision
    /// cookie, sampled before the read is issued and compared at attach time -
    /// so a settled read is served only when the leaf published no mutation
    /// across a window strictly containing it. The distinction matters when
    /// reading the arm: <c>joined</c> is serialisable by construction, whereas
    /// <c>served</c> rests entirely on that cookie comparison.
    /// </para>
    /// <para>
    /// <b>Reading a zero.</b> All three arms are primed at zero from shard-root
    /// ACTIVATION, through the same recorder the live path uses, so a zero here
    /// is a measured absence rather than an absent measurement. The priming
    /// point is load-bearing and was moved deliberately (issue #2809): it was
    /// once taken on the guarded read path, below <c>!scan.IsStallGuarded</c>,
    /// which made an absent series ambiguous between "the build does not carry
    /// the instrument" and "it does, but no stall-guarded scan-page leaf read
    /// ever ran on that <c>(tree, shard)</c>". Primed from activation the series
    /// exists for every activated <c>(tree, shard)</c> with no scan traffic of
    /// any kind, so absence has exactly one cause again. Note the consequence
    /// for the older reading: a flat zero across all three arms no longer says
    /// the walks on this tree are unguarded - it says no read was coalesced,
    /// which an unguarded tree is only one way of achieving. Take the
    /// guarded-or-not question from <see cref="ScanPageStalls"/> instead, and
    /// note that priming is per activation, so partial presence across
    /// <c>(tree, shard)</c> pairs means only that some shard roots have not
    /// activated.
    /// </para>
    /// <para>
    /// <b>One name is not one cause.</b> <c>issued</c> is the ordinary steady
    /// state of a healthy scan as well as the signature above; it is only
    /// diagnostic read <em>against</em> <see cref="ScanPageStalls"/>, never on
    /// its own.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanPageLeafReadOutcomes =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.scan_page.leaf_read_outcomes", unit: "{read}",
            description: "Count of stall-guarded shard-root page-fill leaf reads by whether the read was issued, joined while still in flight, or served again from a settled read whose leaf revision was unchanged.");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>issued</c> (no identical read was held, so
    /// the read went to the leaf).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanPageLeafReadIssuedTag =
        new(TagOutcome, "issued");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>joined</c> (an identical read was already
    /// in flight, so this walk attached to it instead of enqueueing a duplicate
    /// behind it).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanPageLeafReadJoinedTag =
        new(TagOutcome, "joined");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>served</c> (an identical read had already
    /// settled and the leaf still published the revision cookie it published
    /// before that read was issued, so its rows were served again rather than
    /// re-read).
    /// <para>
    /// Distinct from <c>joined</c> because the two carry different evidence.
    /// <c>joined</c> is serialisable by construction - the read was still in
    /// flight, so no write could have been ordered after it. <c>served</c>
    /// rests on the cookie comparison instead, so it is the arm to read when
    /// asking whether the invalidation basis introduced by issue #2786 is
    /// actually firing, and the arm that would fall to zero were the cookie to
    /// stop being published on some mutation path.
    /// </para>
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanPageLeafReadServedTag =
        new(TagOutcome, "served");

    /// <summary>
    /// Count of shard-root page-fill ceiling fires that made <b>zero</b>
    /// progress - no leaf completed - tagged with whether the leaf they named
    /// is merely slow this once or has now missed the ceiling on enough
    /// consecutive attempts to be classified unreadable (issue #3016). Tagged
    /// with <see cref="TagTree"/>, <see cref="TagShard"/> and
    /// <see cref="TagOutcome"/>.
    /// <para>
    /// <b>This is the counter that separates a wedge from healthy retry, which
    /// no other instrument here can do.</b> <see cref="ScanPageStalls"/> counts
    /// fires and <see cref="ScanPageCeilingOutcomes"/> counts what was done
    /// with the rows, and both report a scan that stalls at leaf 1 on the same
    /// leaf 307 times running exactly as they report 307 stalls spread over 307
    /// different leaves that each recovered. Those are opposite conditions: the
    /// second is a busy tree making progress, the first is a corpus that cannot
    /// converge at all and will not converge on its own however many times it is
    /// retried. Reading them apart needs the <em>consecutiveness</em> of the
    /// zero-progress fires on one leaf identity, which is a per-shard-root
    /// quantity no counter can reconstruct after the fact.
    /// </para>
    /// <list type="bullet">
    ///   <item><c>slow</c> - a zero-progress fire whose leaf has not yet
    ///   reached the consecutive-stall threshold. This is the expected reading
    ///   for a leaf replaying a long WAL window from cold, or queued behind one
    ///   slow call: it is a stall, and it is not evidence of a wedge.</item>
    ///   <item><c>stranded</c> - the same leaf has now missed the ceiling on
    ///   <c>StrandedLeafStallThreshold</c> consecutive attempts with nothing
    ///   read on any of them. The leaf is classified unreadable and the
    ///   recovery in <c>ShardRootGrain.StrandedLeaf.cs</c> has been applied.
    ///   Any non-zero reading on this arm is actionable, and a
    ///   <em>climbing</em> one means the recovery is being applied repeatedly
    ///   and is not taking - the leaf is unreadable for a reason that lives
    ///   inside the leaf rather than in the shard root's coalescing map.</item>
    /// </list>
    /// <para>
    /// <b>Reading a zero.</b> Both arms are primed at zero from shard-root
    /// activation through the same recorder the live path uses, so a zero is a
    /// measured absence rather than an absent measurement. A zero on both arms
    /// beside a climbing <see cref="ScanPageStalls"/> is a wiring fault, not a
    /// clean shard: every discarded fire is classified, so the two totals move
    /// together by construction whenever the fire named an in-flight leaf.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanPageZeroProgressStalls =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.scan_page.zero_progress_stalls", unit: "{stall}",
            description: "Count of shard-root page-fill ceiling fires that completed no leaf, by whether the named leaf is slow this once or has been classified unreadable after consecutive zero-progress stalls.");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>slow</c> (a zero-progress ceiling fire on
    /// a leaf that has not yet reached the consecutive-stall threshold).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanPageLeafSlowTag =
        new(TagOutcome, "slow");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>stranded</c> (the same leaf has missed the
    /// ceiling on enough consecutive zero-progress attempts to be classified
    /// unreadable, and the stranded-leaf recovery has been applied).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanPageLeafStrandedTag =
        new(TagOutcome, "stranded");

    /// <summary>
    /// Count of range-scan chain regressions suppressed by a shard root - rows
    /// dropped from a leaf whose keys sit at or behind the scan's chain
    /// watermark, which proves the leaf is not reachable by descent for the
    /// range it claims to own (issue 3271). Tagged with <see cref="TagTree"/>,
    /// <see cref="TagShard"/>, <see cref="TagOutcome"/> and the tenant label.
    /// <para>
    /// This counter exists because the suppression used to be reported
    /// <em>only</em> as a log warning, and that is not an aggregate anyone can
    /// read. One field burst emitted 110,322 warnings in about eight minutes -
    /// roughly 2,354 lines a second, 99.3% of all warning output - which filled
    /// half of a 100 MB container log ring and collapsed that host's log
    /// retention to about 108 seconds. Nothing on the box could be diagnosed
    /// after the fact while it ran. The log line is now bounded (issue 3341) and
    /// this counter carries the magnitude the log used to carry.
    /// </para>
    /// <list type="bullet">
    ///   <item><c>suppression</c> - one per suppression event, which is once per
    ///   offending leaf per page fill. This is the rate the log used to
    ///   represent line-for-line, so it is the arm to read for how hard the
    ///   condition is firing.</item>
    ///   <item><c>distinct-leaf</c> - one the first time a shard-root activation
    ///   observes a given leaf regress. Summed over an activation it is the
    ///   <em>distinct damaged-leaf count</em>, which is the quantity an operator
    ///   actually needs and the one that used to be obtainable only by
    ///   de-duplicating the warning stream by leaf id. The census is deliberately
    ///   an aggregate rather than a per-leaf tag: one series per B+ leaf grain is
    ///   an unbounded-cardinality defect (issue 2518).</item>
    /// </list>
    /// <para>
    /// <b>Reading a zero.</b> Both arms are primed at zero from shard-root
    /// activation through the same recorder the live path uses, so an absent
    /// series means the build does not carry the instrument rather than that the
    /// shard's chain is intact. Read the two together: <c>suppression</c> far
    /// above <c>distinct-leaf</c> is a small set of damaged leaves re-encountered
    /// on every page, which is the shape the field burst had, whereas the two
    /// moving together is fresh damage spreading across the chain.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanChainRegressions =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.scan_page.chain_regressions", unit: "{regression}",
            description: "Count of range-scan rows suppressed from leaves that regressed the scan's chain watermark, by whether the event is a suppression or the first sighting of a distinct damaged leaf.");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>suppression</c> (one range-scan chain
    /// regression suppressed, counted once per offending leaf per page fill).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanChainRegressionSuppressionTag =
        new(TagOutcome, "suppression");

    /// <summary>
    /// <see cref="TagOutcome"/> = <c>distinct-leaf</c> (the first time this
    /// shard-root activation observed this particular leaf regress, so the arm
    /// sums to the distinct damaged-leaf count).
    /// </summary>
    public static readonly KeyValuePair<string, object?> OutcomeScanChainRegressionDistinctLeafTag =
        new(TagOutcome, "distinct-leaf");


    /// <summary>
    /// Count of client-side resilient scans
    /// (<see cref="Orleans.Lattice.LatticeExtensions.ScanKeysAsync"/> and its
    /// siblings) that met a
    /// <see cref="Orleans.Lattice.ScanPageStalledException"/>, tagged with the
    /// decision the scan took. Tagged with <see cref="TagTree"/>,
    /// <see cref="TagPhase"/> (carried through from the stall) and
    /// <see cref="TagOutcome"/>.
    /// <para>
    /// The outcome tag is the point of the counter, and each value is a
    /// distinct operational statement:
    /// </para>
    /// <list type="bullet">
    /// <item><description><c>resumed</c> - the scan resumed from its last
    /// continuation token and carried on. The scan still completed in full, so
    /// without this counter a scan that succeeded only after N resumptions
    /// would be indistinguishable from one that never stalled, and a worsening
    /// contention trend would be hidden by its own recovery.</description></item>
    /// <item><description><c>budget-exhausted</c> - the scan had already
    /// resumed its permitted number of <em>consecutive</em> non-progressing
    /// times
    /// (<see cref="Orleans.Lattice.LatticeExtensions.DefaultScanStallResumeAttempts"/>,
    /// or a lower caller <c>maxAttempts</c>) without banking a record, and
    /// rethrew. It says the source is genuinely not yielding, so no larger
    /// bound would have helped it. Its meaning is unchanged from builds
    /// predating the lifetime ceiling, so a series spanning that change stays
    /// comparable.
    /// </description></item>
    /// <item><description><c>ceiling-exhausted</c> - the scan was
    /// <em>progressing</em>, banking records between stalls, and exceeded
    /// <see cref="Orleans.Lattice.LatticeExtensions.DefaultScanStallResumeCeiling"/>
    /// stalls over its lifetime. This is the opposite diagnosis to
    /// <c>budget-exhausted</c> despite the identical symptom: it is a statement
    /// about that constant being too small for the workload, not about the
    /// source being dead, and the remedy is to raise it or for the caller to
    /// bank partial progress so a terminated walk resumes rather than restarts.
    /// The two are tagged apart precisely because a single label covering both
    /// would be populated, plausible, and blind to the only distinction an
    /// operator needs to choose between those remedies.
    /// </description></item>
    /// </list>
    /// <para>
    /// A <c>resumed</c> rate that climbs while both terminal outcomes stay at
    /// zero is recovery working. Every terminal outcome is a scan that
    /// failed, and the caller saw the stall: a resumption never truncates, so
    /// the scan either yields its full range or rethrows the last stall
    /// verbatim.
    /// </para>
    /// <para>
    /// Earlier builds emitted a third value, <c>no-progress</c>, when a
    /// progress gate refused a stall that still had resume budget. That gate
    /// refused every stall that occurred in practice, so the resume it guarded
    /// never ran; it has been removed and the label can no longer be recorded.
    /// It is named here only so a reader meeting it in historical data knows
    /// what it meant. See
    /// <see cref="Orleans.Lattice.LatticeExtensions.DefaultScanStallResumeAttempts"/>.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanStallResumptions =
        Meter.CreateCounter<long>("orleans.lattice.scan.stall_resumptions", unit: "{resumption}",
            description: "Count of resilient client scans that met a scan-page stall, tagged by the decision taken (resumed, budget-exhausted, ceiling-exhausted).");

    /// <summary>
    /// What happened to a scan source <em>after</em> a resilient scan gave up on
    /// it for futility - that is, after
    /// <see cref="ScanStallResumptions"/> recorded a
    /// <c>budget-exhausted</c> termination against it. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagPhase"/> (both carried through from
    /// the terminating stall) and <see cref="TagOutcome"/>.
    /// <para>
    /// It answers the one question <c>budget-exhausted</c> cannot. Under load,
    /// "this shard was busy for a while" and "this source is genuinely not
    /// yielding" are the SAME OBSERVATION at the consecutive bound
    /// (<see cref="Orleans.Lattice.LatticeExtensions.DefaultScanStallResumeAttempts"/>),
    /// so a large futility count on its own cannot say whether that bound is
    /// what stopped a job. This counter records whether the abandoned source
    /// served records again shortly afterwards.
    /// </para>
    /// <list type="bullet">
    /// <item><description><c>recovered</c> - a later scan of the same source
    /// yielded a record at or beyond the position the futile walk died on. The
    /// source was recoverable and the consecutive bound cut it off early.
    /// <b>Any sustained non-zero value is the finding</b>: it says the bound is
    /// too tight for this workload.</description></item>
    /// <item><description><c>still-stalled</c> - a later walk reached the same
    /// source and also terminated for futility there. The source was not merely
    /// busy, so the bound was right to give up. A later walk that merely stalls
    /// again and then gets past the abandoned position is deliberately counted
    /// as <c>recovered</c>, not here: that walk demonstrates recoverability and
    /// is precisely the premature-bound case.</description></item>
    /// <item><description><c>unobserved</c> - the observation window closed with
    /// no later scan reaching the source at all, so this termination says
    /// nothing either way.</description></item>
    /// <item><description><c>dropped</c> - the bounded watch table was at
    /// capacity, so the answer is unknown for a reason internal to this
    /// instrument rather than anything about the source.</description></item>
    /// </list>
    /// <para>
    /// READ THE LAST TWO BEFORE CONCLUDING ANYTHING FROM THE FIRST. A zero
    /// <c>recovered</c> is evidence the abandoned sources were dead only when
    /// <c>still-stalled</c> is populated; against a scrape that is mostly
    /// <c>unobserved</c> or <c>dropped</c> it means nobody looked, which is a
    /// fact about the callers or about this table and not about the bound. The
    /// two non-answer arms exist so that distinction cannot be papered over by
    /// whichever complement a reader finds convenient.
    /// </para>
    /// <para>
    /// Every futility termination eventually resolves to exactly one of the four
    /// values, so
    /// <c>sum(stall_futility_outcomes) &lt;= stall_resumptions{outcome="budget-exhausted"}</c>
    /// always holds, with equality once every open watch has resolved. A
    /// persistent shortfall means watches are outliving the scrape window rather
    /// than that terminations went unrecorded.
    /// </para>
    /// <para>
    /// A LOW READING IS NOT A VERDICT UNLESS THE RUN WAS STRAINED. This
    /// instrument can only speak about a bound that came under pressure, so
    /// before reading a small <c>recovered</c> count as "the bound is well
    /// sized", confirm all three of: a non-zero
    /// <c>stall_resumptions{outcome="budget-exhausted"}</c> (with no futility
    /// terminations no watch is ever opened and every arm below reads zero for a
    /// trivial reason); a non-zero <c>recovered + still-stalled</c> (if
    /// <c>unobserved</c> and <c>dropped</c> account for the whole total then
    /// nothing revisited the abandoned sources and the run observed nothing
    /// either way); and a materially non-zero
    /// <see cref="ScanPageStalls"/>, which is the upstream condition
    /// that makes a source look busy at all. Where any of those fails, the
    /// finding is "the bound was not exercised", not "the bound is correct" - a
    /// bound that was never strained is untested, exactly as a
    /// <c>ceiling-exhausted</c> of zero does not validate the lifetime ceiling.
    /// This reading is recorded here in advance of the data precisely so it
    /// cannot be chosen after the numbers arrive.
    /// </para>
    /// <para>
    /// The observation is passive: it issues no grain call, starts no timer, and
    /// never re-drives the abandoned work, so it changes no termination
    /// decision. See <c>ScanStallFutilityWatch</c> for the mechanism, including
    /// why the window is derived from the stall's own reported ceiling and is
    /// structural rather than measured. Shard index is deliberately NOT a tag:
    /// this family must stay small enough to leave on permanently, and the tag
    /// shape is otherwise identical to <see cref="ScanStallResumptions"/> so the
    /// two series join.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ScanStallFutilityOutcomes =
        Meter.CreateCounter<long>("orleans.lattice.scan.stall_futility_outcomes", unit: "{outcome}",
            description: "What happened to a scan source after a resilient scan gave up on it for futility (recovered, still-stalled, unobserved, dropped).");

    /// <summary>
    /// Count of internal-node digest publishes (the upward
    /// <c>ChildDigestSnapshot</c> propagation from a <c>BPlusInternalGrain</c>
    /// to its parent) that were abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.DigestPublishTimeout"/>.
    /// Tagged with <see cref="TagTree"/>. A non-zero value indicates a
    /// publish parked against a parent internal node that was mid-mutation -
    /// the parked publish was faulted as a <see cref="TimeoutException"/> so
    /// the holding turn released the non-reentrant split gate rather than
    /// pinning it indefinitely. The digest is staleness-tolerant, so the next
    /// mutation's publish re-drives convergence; sustained non-zero counts
    /// indicate a contended internal-node chain worth investigating.
    /// </summary>
    public static readonly Counter<long> DigestPublishTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.internal.digest_publish.timeouts", unit: "{timeout}",
            description: "Count of internal-node upward digest publishes abandoned after exceeding DigestPublishTimeout.");

    /// <summary>
    /// Count of outbound <c>IWalShardGrain</c> dispatches
    /// (<c>WalCommitLogWriter.AppendForPartitionAsync</c> /
    /// <c>AppendAsync</c>) that were abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.WalAppendDispatchTimeout"/>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// The dispatch is the writer-side cross-grain RPC into the per-shard
    /// WAL grain; it was historically unbounded on the writer side, so a
    /// wedged shard activation would hold every caller's dispatch parked
    /// until the Orleans response deadline (default 3 minutes) expired.
    /// A non-zero value attributes the wedge to a specific
    /// <c>(tree, shard)</c> pair in O(<see cref="Orleans.Lattice.LatticeOptions.WalAppendDispatchTimeout"/>)
    /// time rather than O(response timeout) time, and the parked dispatch
    /// is faulted as a <see cref="TimeoutException"/> so the request
    /// pipeline releases its slot rather than back-filling behind the
    /// wedge. Sustained non-zero counts on a specific
    /// <c>(tree, shard)</c> identify the wedged shard for follow-up
    /// investigation.
    /// </summary>
    public static readonly Counter<long> WalAppendDispatchTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.wal.append_dispatch.timeouts", unit: "{timeout}",
            description: "Count of writer-side WAL shard dispatches abandoned after exceeding WalAppendDispatchTimeout.");

    /// <summary>
    /// Count of per-shard WAL <c>FlushAsync</c> preflight regions (the
    /// synchronous setup and initial scheduler yield that precede the
    /// bounded provider call) that were abandoned because they exceeded
    /// <see cref="Orleans.Lattice.LatticeOptions.WalFlushPreflightTimeout"/>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// The preflight region is normally microseconds; a non-zero count
    /// indicates the activation's grain scheduler did not resume the
    /// flush's post-yield continuation within the deadline, leaving the
    /// in-flight slot pinned with no provider-call deadline armed (the
    /// existing <see cref="Orleans.Lattice.LatticeOptions.WalFlushTimeout"/>
    /// only covers the provider call itself, which has not yet been
    /// issued). The faulted preflight surfaces as a
    /// <see cref="TimeoutException"/> routed through the normal failure
    /// handler, the slot drains, and this counter attributes the trip to
    /// the affected <c>(tree, shard)</c>. Sustained non-zero counts
    /// indicate the activation's scheduler is being held by a startup
    /// reshard / membership change, a non-cooperative work item, or a
    /// mid-flush activation tear-down.
    /// </summary>
    public static readonly Counter<long> WalFlushPreflightTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.wal.flush.preflight.timeouts", unit: "{timeout}",
            description: "Count of WAL shard FlushAsync preflight regions abandoned after exceeding WalFlushPreflightTimeout.");

    /// <summary>
    /// Histogram of <c>_inFlight.Count</c> observed when a per-shard
    /// <c>WalShardGrain</c> activation is being deactivated. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>. Recorded exactly
    /// once per <c>OnDeactivateAsync</c> call. A zero observation is the
    /// healthy steady-state shape (the grain drained cleanly); a non-zero
    /// observation means the activation was torn down with in-flight
    /// flushes still pending - the slot population that defines the
    /// post-#568 residual phase-1/activation wedge fingerprint. Combined
    /// with <see cref="WalFlushPreflightTimeouts"/>, a deactivation with
    /// non-zero in-flight count immediately followed by a preflight
    /// timeout on a successor activation is the smoking gun for the
    /// "mid-call deactivation orphan" hypothesis.
    /// </summary>
    public static readonly Histogram<long> WalShardDeactivateInFlight =
        Meter.CreateHistogram<long>("orleans.lattice.wal.shard.deactivate.in_flight", unit: "{slot}",
            description: "Per-WAL-shard in-flight slot count observed at OnDeactivateAsync time.");

    /// <summary>
    /// Count of per-shard <c>WalShardGrain</c> deactivation drains that
    /// exceeded <see cref="Orleans.Lattice.LatticeOptions.WalDrainBudget"/>
    /// and had to force-fault one or more in-flight slots so the
    /// activation could finish tearing down. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// <para>
    /// Reliability intent: under a saturating-storage-account wedge,
    /// the provider call's await can park behind an SDK retry loop in
    /// pre-attempt back-off where the per-flush
    /// <see cref="Orleans.Lattice.LatticeOptions.WalFlushTimeout"/>
    /// deadline does not fire promptly (the SDK observes cancellation
    /// only between attempts, not during back-off), so a chain with N
    /// in-flight slots could otherwise hold the deactivation
    /// indefinitely. With the drain budget the deactivation force-faults
    /// any slot that has not unlinked within the deadline; this counter
    /// names the wedged shard so operators can attribute the trip
    /// without source-walking the silo log. Zero on a healthy
    /// drain; any non-zero rate identifies a shard whose provider call
    /// could not be cancelled inside the drain budget.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalShardDrainBudgetExpirations =
        Meter.CreateCounter<long>("orleans.lattice.wal.shard.drain.budget.expirations", unit: "{expiration}",
            description: "Count of WalShardGrain deactivation drains that exceeded WalDrainBudget and force-faulted in-flight slots.");

    /// <summary>
    /// Histogram of in-flight slots force-faulted by a per-shard
    /// <c>WalShardGrain</c> deactivation drain after
    /// <see cref="Orleans.Lattice.LatticeOptions.WalDrainBudget"/>
    /// expired. Tagged with <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// Recorded exactly once per drain that hit the budget; the value is
    /// the number of slots that had not unlinked when the budget fired and
    /// were force-faulted to release the activation. A zero observation
    /// is not recorded - the histogram only fires on the
    /// <see cref="WalShardDrainBudgetExpirations"/> path, so reading the
    /// histogram's count and the counter's count gives the same number.
    /// </summary>
    public static readonly Histogram<long> WalShardDrainBudgetForceFaultedSlots =
        Meter.CreateHistogram<long>("orleans.lattice.wal.shard.drain.budget.force_faulted_slots", unit: "{slot}",
            description: "Per-WAL-shard in-flight slot count force-faulted by a deactivation drain that exceeded WalDrainBudget.");

    /// <summary>
    /// Count of <c>WalShardGrain.StartFlush</c> invocations per
    /// <c>(tree, shard)</c>. Incremented once at the top of every
    /// <c>StartFlush</c> call, including the follow-on flushes a
    /// completing flush kicks off. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/>.
    /// <para>
    /// Diagnostic intent: under the residual phase-1/activation WAL
    /// wedge, the in-flight chain pins at <c>WalMaxPendingBatches</c>
    /// for 120+ seconds with no shipped deadline tripping. This counter
    /// distinguishes two of the three remaining wedge-mechanism classes
    /// in one cohort: if <c>start_flush.calls</c> keeps incrementing
    /// throughout the wedge then new flushes ARE being kicked off, so
    /// the wedge is a slot-leak in the in-flight chain's <c>finally</c>
    /// (slots never removed even after the flush's task settles). If
    /// <c>start_flush.calls</c> stops incrementing during the wedge then
    /// the cap-cutover loop in <c>AppendBatchAsync</c> is itself blocked
    /// and no new flush ever kicks off. Either signal narrows the
    /// remaining investigation to a small handful of code regions.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalShardStartFlushCalls =
        Meter.CreateCounter<long>("orleans.lattice.wal.shard.start_flush.calls", unit: "{call}",
            description: "Count of WalShardGrain.StartFlush invocations per (tree, shard).");

    /// <summary>
    /// Histogram of <c>_pendingSegments.Count</c> observed at every
    /// <c>WalShardGrain.StartFlush</c> entry, sampled <i>before</i> the
    /// pending list is captured into the new in-flight slot. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/>.
    /// <para>
    /// Diagnostic intent: under the wedge, a growing distribution
    /// indicates callers are still arriving and enqueueing into
    /// <c>_pendingSegments</c> even though the chain cannot drain - a
    /// signature of back-pressure absorbing everything but never
    /// releasing. A stuck-at-zero distribution combined with a
    /// <see cref="WalShardStartFlushCalls"/> trickle indicates the
    /// cap-cutover loop blocked itself; combined with a healthy
    /// <c>start_flush.calls</c> rate it indicates the wedge is downstream
    /// of the flush kick-off. Mirrors the existing <c>WalAppendInFlight</c>
    /// histogram's allocation-free emission shape.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalShardPendingSegments =
        Meter.CreateHistogram<long>("orleans.lattice.wal.shard.pending_segments", unit: "{segment}",
            description: "Per-WAL-shard pending-segment count sampled at every StartFlush entry.");

    /// <summary>
    /// Count of <c>TreeReshardGrain.ReshardAsync</c> invocations that
    /// progressed past argument / interlock validation and started a
    /// reshard coordinator. Tagged with <see cref="TagTree"/>.
    /// <para>
    /// Diagnostic intent: the residual WAL wedge is correlated with the
    /// <c>reshard ... REJECTED (Forwarding failed)</c> log storm
    /// (228-540 occurrences per wedged run). This counter is the
    /// Lattice-side initiation signal; pairing it with
    /// <see cref="ShardRootReshardCompleted"/> and
    /// <see cref="ShardRootReshardRejected"/> lets a dashboard correlate
    /// reshard activity with wedge onset directly, without depending on
    /// grep over a rotated silo log. Note: Orleans-side message-routing
    /// rejections ("Forwarding failed") are emitted by Orleans's own
    /// router and are not captured here - they remain log-only until a
    /// separate diagnostic source is added.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardRootReshardInitiated =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.reshard.initiated", unit: "{reshard}",
            description: "Count of TreeReshardGrain.ReshardAsync invocations that started a reshard coordinator.");

    /// <summary>
    /// Count of <c>TreeReshardGrain.ReshardAsync</c> invocations that
    /// were rejected at the Lattice layer before starting a coordinator.
    /// Tagged with <see cref="TagTree"/> and a <c>reason</c> tag
    /// enumerating the rejection cause (e.g. <c>argument_out_of_range</c>,
    /// <c>resize_in_flight</c>, <c>state_write_failed</c>).
    /// <para>
    /// Excludes Orleans-side message-routing rejections, which the
    /// Orleans runtime logs as "Forwarding failed" but does not surface
    /// to <c>TreeReshardGrain</c> as a catchable exception inside
    /// <c>ReshardAsync</c>. See <see cref="ShardRootReshardInitiated"/>.
    /// </para>
    /// </summary>
    public static readonly Counter<long> ShardRootReshardRejected =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.reshard.rejected", unit: "{rejection}",
            description: "Count of TreeReshardGrain.ReshardAsync rejections, tagged by reason.");

    /// <summary>
    /// Count of <c>TreeReshardGrain</c> coordinator completions that
    /// reached the terminal phase successfully. Tagged with
    /// <see cref="TagTree"/>. The difference between this and
    /// <see cref="ShardRootReshardInitiated"/> over a window is the
    /// number of reshards still in flight or that failed mid-coordinator.
    /// </summary>
    public static readonly Counter<long> ShardRootReshardCompleted =
        Meter.CreateCounter<long>("orleans.lattice.shard_root.reshard.completed", unit: "{reshard}",
            description: "Count of TreeReshardGrain coordinator completions.");

    /// <summary>
    /// Histogram observation of the reshard in-flight state for a tree,
    /// emitted at every <c>ReshardAsync</c> entry as either <c>0</c>
    /// (idle) or <c>1</c> (a reshard is already in progress for this
    /// tree). Tagged with <see cref="TagTree"/>. Bridges the gap left
    /// by not registering an <c>ObservableGauge</c>: a non-zero
    /// observation immediately preceding the wedge onset is the same
    /// signal a periodically-polled gauge would provide.
    /// </summary>
    public static readonly Histogram<long> ShardRootReshardInFlight =
        Meter.CreateHistogram<long>("orleans.lattice.shard_root.reshard.in_flight", unit: "{reshard}",
            description: "Per-tree reshard in-flight (0/1) observation, recorded at ReshardAsync entry.");

    /// <summary>
    /// Count of <c>WalCommitLogWriter</c> append dispatches that started
    /// (one increment per pending-append at the <c>Enqueued</c> stamp).
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Wedge-investigation intent: the saturation-rung wedge has a
    /// dominant mode (cohort 2026-06-03, 5/7 wedged cohorts) where every
    /// shard's in-flight chain is empty (<c>head.IsNull=True</c>) yet
    /// 348+ callers are parked at <c>WalShardGrain.AppendBatchAsync</c>
    /// and 375+ at <c>WalCommitLogWriter.AppendForPartitionAsync</c>.
    /// The wedge mechanism for that mode is upstream of in-flight
    /// insertion, inside this writer's per-partition dispatch plumbing.
    /// This counter is the writer-layer kick-off signal: a healthy rate
    /// rules out "writer never gets called"; a collapse to zero during a
    /// wedge tail localises the stall to the writer's own routing /
    /// option-resolver path; a sustained rate combined with stale
    /// <see cref="WalAppendPendingDispatches"/> p99 readings localises it
    /// to the awaited shard-grain RPC.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendDispatched =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.dispatched", unit: "{dispatch}",
            description: "Count of WalCommitLogWriter append dispatches that reached the Enqueued lifecycle stamp.");

    /// <summary>
    /// Histogram of per-partition pending-append depth observed at every
    /// <c>WalCommitLogWriter</c> append entry, sampled <i>before</i> the
    /// new pending stamp is added to the partition's tracker. Tagged
    /// with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Wedge-investigation intent: the writer's per-partition pending
    /// tracker holds one <c>PendingAppend</c> stamp per in-flight
    /// <c>AppendForPartitionAsync</c> caller. A growing distribution
    /// during the wedge confirms the writer is the choke (callers
    /// enqueuing into a tracker that cannot drain); a stuck-at-zero
    /// distribution combined with sustained
    /// <see cref="WalAppendDispatched"/> rules out a writer-layer
    /// dispatch lifecycle stall and points the next bisect downstream of
    /// the <c>SentToShard</c> stage. Mirrors the
    /// <see cref="WalShardPendingSegments"/> shape one layer up.
    /// </para>
    /// </summary>
    public static readonly Histogram<long> WalAppendPendingDispatches =
        Meter.CreateHistogram<long>("orleans.lattice.wal.writer.partition.pending_appends", unit: "{dispatch}",
            description: "Per-WAL-writer-partition pending-append-dispatch count sampled at every append entry.");

    /// <summary>
    /// Count of <c>WalCommitLogWriter</c> append dispatches that failed
    /// to acquire a per-partition admission slot before
    /// <see cref="LatticeOptions.WalAppendDispatchTimeout"/> expired.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Reliability intent: the per-partition admission semaphore caps
    /// <c>PartitionTracker._inFlight</c> depth at
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/>, mirroring the
    /// shard-side ceiling. When the shard cannot drain, callers
    /// awaiting an admission slot are released with a typed
    /// <see cref="TimeoutException"/> at the deadline rather than
    /// silently parking forever in an unbounded writer queue. A
    /// non-zero counter under steady-state operation is the signal that
    /// the offered rate exceeds the shard's drain rate - the
    /// saturation regime previously hidden as a silent wedge. Pair with
    /// <see cref="WalAppendAdmissionWait"/> to distinguish "saturation
    /// hit but absorbed cleanly" (wait p99 elevated, zero timeouts)
    /// from "saturation exceeded the deadline" (non-zero timeouts).
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendAdmissionTimeouts =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.admission_timeouts", unit: "{timeout}",
            description: "Count of WalCommitLogWriter append dispatches whose per-partition admission wait exceeded WalAppendDispatchTimeout.");

    /// <summary>
    /// Histogram of wall-clock ms spent waiting for a per-partition
    /// admission slot before the <c>WalCommitLogWriter</c> dispatch was
    /// allowed to link a new <c>PendingAppend</c> stamp. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Reliability intent: under healthy operation this histogram sits
    /// at the floor (a sub-microsecond uncontended semaphore acquire).
    /// A spreading distribution indicates the per-partition tracker is
    /// approaching its <see cref="LatticeOptions.WalMaxPendingBatches"/>
    /// ceiling, surfacing back-pressure as an honest tail-latency
    /// signal long before any caller hits the
    /// <see cref="WalAppendAdmissionTimeouts"/> deadline. Recorded for
    /// every dispatch that successfully acquired a slot (timed-out
    /// dispatches feed the counter only).
    /// </para>
    /// </summary>
    public static readonly Histogram<double> WalAppendAdmissionWait =
        Meter.CreateHistogram<double>("orleans.lattice.wal.writer.append.admission_wait", unit: "ms",
            description: "Wall-clock ms a WalCommitLogWriter dispatch waited for a per-partition admission slot.");

    /// <summary>
    /// Count of writer-side parked admission callers released by a
    /// silo-drain signal on host shutdown. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagPartition"/>. One sample per parked caller
    /// faulted out of <c>PartitionTracker.AcquireAsync</c> when the
    /// owning <see cref="Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter"/>
    /// drains on host shutdown; zero on a healthy shutdown that has
    /// no parked callers.
    /// <para>
    /// Reliability intent: distinct from
    /// <see cref="WalAppendAdmissionTimeouts"/> (which counts
    /// per-call dispatch-deadline expiries during steady-state
    /// operation) and from <see cref="WalShardDrainBudgetExpirations"/>
    /// (which counts shard-grain deactivation drains that had to
    /// force-fault). This counter names writer-side parked callers
    /// released by the silo's drain on shutdown - the surface that
    /// closes the writer-admission-semaphore-wedged-at-SIGTERM
    /// phenotype documented in
    /// <c>benchmark/azure-throughput/throughput.md</c> section 32.6.
    /// A non-zero rate on shutdown is normal when the silo was under
    /// storage saturation at drain entry; a non-zero rate during
    /// steady-state operation indicates the drain hook fired
    /// spuriously and is a regression signal.
    /// </para>
    /// <para>
    /// Per-silo: each silo process emits its own samples for the
    /// trackers its <see cref="Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter"/>
    /// owns. A dashboard <c>sum by (tree, partition)</c> across the
    /// cluster gives the cumulative drain-released-caller count;
    /// a <c>sum by (silo)</c> isolates which silos saw saturated
    /// shutdowns.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendDrainReleases =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.drain.releases", unit: "{release}",
            description: "Count of writer-side parked admission callers released by a silo-drain signal on host shutdown.");

    /// <summary>
    /// Count of writer-side admission dispatches refused with
    /// <see cref="LatticeSaturatedException"/> because the per-tree
    /// saturation signal reported
    /// <see cref="WalSaturationState.Saturated"/> for longer than
    /// <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagPartition"/>.
    /// <para>
    /// Reliability intent: distinct from
    /// <see cref="WalAppendAdmissionTimeouts"/> (which counts the
    /// per-call <see cref="LatticeOptions.WalAppendDispatchTimeout"/>
    /// deadline) and from <see cref="WalAppendDrainReleases"/> (which
    /// counts host-shutdown drain releases). This counter names
    /// callers refused fast by the
    /// <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/>
    /// gate during steady-state saturation episodes - the surface
    /// that closes the storage-account 409-Conflict-burst phenotype
    /// documented in <c>benchmark/azure-throughput/throughput.md</c>
    /// section 32. A non-zero rate during steady-state operation is
    /// the canonical signal that offered load is exceeding the
    /// storage layer's sustained drain rate; under healthy operation
    /// the counter stays at zero because the saturation signal does
    /// not enter <see cref="WalSaturationState.Saturated"/>.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalAppendAdmissionSaturationRefusals =
        Meter.CreateCounter<long>("orleans.lattice.wal.writer.append.admission_saturation_refusals", unit: "{refusal}",
            description: "Count of writer-side admission dispatches refused with LatticeSaturatedException because the saturation signal stayed Saturated beyond WalAdmissionSaturationWaitBudget.");

    /// <summary>
    /// Tag key for the per-tree saturation state on
    /// <see cref="WalSaturationTransitions"/>. Value is the lowercased
    /// state name (<c>healthy</c>, <c>throttled</c>, <c>saturated</c>).
    /// The <see cref="WalSaturationStateGaugeName"/> observable gauge
    /// deliberately does not carry this tag - its ordinal value already
    /// encodes the regime, so labelling it as well would fragment the
    /// per-tree series on every transition and leave stale elevated
    /// series behind.
    /// </summary>
    public const string TagWalSaturationState = "state";

    /// <summary>
    /// Tag key for the previous saturation state on
    /// <see cref="WalSaturationTransitions"/>. Lets dashboards filter
    /// by direction of transition (e.g. <c>previous_state=healthy</c>
    /// + <c>state=throttled</c> isolates the leading edge of every
    /// saturation episode).
    /// </summary>
    public const string TagWalSaturationPreviousState = "previous_state";

    /// <summary>
    /// Tag naming which sampler input a WAL saturation transition was
    /// attributed to (the lowercased <see cref="WalSaturationCause"/>). Several
    /// inputs map to the same state, so the state tag alone does not identify
    /// the subsystem under pressure. Carried on
    /// <see cref="WalSaturationTransitions"/>.
    /// </summary>
    public const string TagWalSaturationCause = "cause";

    /// <summary>
    /// Counter incremented once per per-tree WAL saturation-state
    /// transition observed by the silo-scoped sampler. Tagged with
    /// <see cref="TagTree"/>, <see cref="TagWalSaturationState"/>
    /// (the new state), and
    /// <see cref="TagWalSaturationPreviousState"/> (the state the tree
    /// was in before the transition). Optional <see cref="TagPartition"/>
    /// and <see cref="TagShard"/> tags are added when the transition
    /// is attributable to a single partition (admission-depth-driven)
    /// or shard (dispatch-timeout-driven).
    /// <para>
    /// Wedge-investigation intent: a healthy silo's series is a flat
    /// zero. A rising rate of <c>state=throttled</c> transitions on a
    /// specific <c>(tree)</c> is the leading edge of the saturation
    /// regime; a rising rate of <c>state=saturated</c> is the regime
    /// itself. Pair with the observable
    /// <see cref="WalSaturationStateGaugeName"/> gauge for "what is
    /// the current regime" and with this counter for "how often is
    /// the regime changing" - flapping between Throttled and
    /// Saturated is a different operational signal from a sustained
    /// Saturated.
    /// </para>
    /// </summary>
    public static readonly Counter<long> WalSaturationTransitions =
        Meter.CreateCounter<long>("orleans.lattice.wal.saturation.transitions", unit: "{transition}",
            description: "Count of per-tree WAL saturation-state transitions observed by the silo-scoped sampler.");

    /// <summary>
    /// Instrument name of the observable gauge that reports the current
    /// per-tree WAL saturation state. Published with
    /// <see cref="TagTree"/> only; the value is the ordinal
    /// of the <see cref="WalSaturationState"/> enum
    /// (<c>0</c> = Healthy, <c>1</c> = Throttled, <c>2</c> = Saturated)
    /// so dashboards can plot the regime as a step function. The regime
    /// is intentionally <b>not</b> also carried as a label: the ordinal
    /// value already encodes it, and adding a redundant state label made
    /// every transition change the series identity, leaving the prior
    /// state's series behind at its last (elevated) value under scrape
    /// staleness - so a recovered tree kept reading as Saturated. The
    /// per-state breakdown lives on <see cref="WalSaturationTransitions"/>.
    /// <para>
    /// Idle cost is zero - the observable callback only runs on scrape
    /// and reads a concurrent-dictionary cache populated by the silo-
    /// scoped sampler. A tree contributes a measurement only after the
    /// sampler has observed at least one signal for it; a tree that
    /// has never been written to does not appear in the gauge series
    /// at all (rather than reporting an incorrect Healthy zero).
    /// </para>
    /// </summary>
    public const string WalSaturationStateGaugeName = "orleans.lattice.wal.saturation.state";

    /// <summary>
    /// Histogram of wall-clock ms for a single per-leaf
    /// <c>IBPlusLeafGrain.SetManyAsync</c> RPC dispatched from
    /// <c>ShardRootGrain.SetManyLocalOnlyAsync</c> via
    /// <c>DispatchLeafBatchWithRetryAsync</c>. Recorded per attempt
    /// (including retries) and per dispatched leaf, so for a single
    /// shard-root <c>SetManyAsync(N)</c> there are up to one
    /// observation per per-leaf bucket. Tagged with <see cref="TagTree"/>.
    /// This is the outbound-call view from the shard-root: it includes
    /// Orleans grain-schedule wait, per-leaf turn-queue wait, leaf
    /// commit, WAL append, and WAL phase-2. Combined with the leaf-side
    /// <c>leaf.commit.duration</c> aggregate, the residual gap localises
    /// pre-turn scheduling cost.
    /// </summary>
    public static readonly Histogram<double> ShardRootSetManyLeafRpcDuration =
        Meter.CreateHistogram<double>("orleans.lattice.shard_root.set_many.leaf_rpc.duration", unit: "ms",
            description: "Wall-clock ms per per-leaf IBPlusLeafGrain.SetManyAsync RPC dispatched from ShardRootGrain.SetManyLocalOnlyAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.SetManyAsync</c>, the user-facing
    /// <see cref="ILattice.SetManyAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// batched write call (includes routing, bucketing, per-shard
    /// parallel fan-out, and event publish). Pair with
    /// <see cref="SetManyStageDuration"/> to attribute the per-call
    /// envelope to one of five sub-spans.
    /// </summary>
    public static readonly Histogram<double> SetManyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set_many.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.SetManyAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.SetManyAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>gate</c> | <c>route</c> |
    /// <c>bucket</c> | <c>fanout</c> | <c>events</c>).
    /// <para>
    /// Mirrors the c2-xxii saga-broadcast sub-stage instrumentation
    /// (<see cref="SagaBroadcastShardStageDuration"/>). Splits the
    /// caller-visible envelope into its constituent spans so the
    /// dominant cost on the set-many path can be identified before any
    /// further structural attempt. Together with the existing
    /// <see cref="ShardRootSetManyLocalApplyDuration"/> /
    /// <see cref="ShardRootSetManyShadowForwardDuration"/> /
    /// <see cref="ShardRootSetManyLeafRpcDuration"/> instruments,
    /// the full envelope from <c>ILattice.SetManyAsync</c> entry down
    /// to the leaf RPC is attributed.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SetManyStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set_many.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (gate|route|bucket|fanout|events) of LatticeGrain.SetManyAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.SetAsync</c>, the user-facing point-write
    /// <see cref="ILattice.SetAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// single-key set call (includes gate, routing, the shard RPC, and
    /// event publish). Pair with <see cref="SetStageDuration"/> to
    /// attribute the per-call envelope to one of four sub-spans.
    /// </summary>
    public static readonly Histogram<double> SetDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.SetAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.SetAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>gate</c> | <c>route</c> |
    /// <c>shard</c> | <c>publish</c>).
    /// <para>
    /// Mirrors <see cref="SetManyStageDuration"/> for the point-write
    /// path. Together with the existing per-leaf instruments
    /// (<c>leaf.commit.duration phase=wal|apply|observer|digest</c>),
    /// <c>wal.shard.dispatch.duration</c>, and <c>wal.append.*</c>
    /// histograms, the full point-write envelope from
    /// <c>ILattice.SetAsync</c> entry down to the Azure provider call is
    /// attributed. The c2-xxvii investigation surfaced this seam.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> SetStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.set.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (gate|route|shard|publish) of LatticeGrain.SetAsync.");

    // --- Foreground read envelopes (LatticeGrain) ----------------------------

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.GetAsync</c>, the user-facing point-read
    /// <see cref="ILattice.GetAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// single-key read (includes routing resolution, the shard RPC, and
    /// any stale-routing retries). Pair with <see cref="GetStageDuration"/>
    /// to attribute the per-call envelope to one of its sub-spans.
    /// <para>
    /// Mirrors <see cref="SetDuration"/> for the point-read path. Closes
    /// the read-side gap in the foreground-call attribution model: the
    /// existing <c>shard.reads</c> counter only counts reads, not their
    /// latency, and <c>leaf.scan.duration</c> covers range scans only.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> GetDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.GetAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.GetAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>route</c> | <c>shard</c>).
    /// <para>
    /// One observation per stage per inner attempt: under a stale-routing
    /// storm a single <c>GetAsync</c> call records multiple <c>route</c> /
    /// <c>shard</c> data points so the histograms attribute the retry
    /// cost. Mirrors the per-attempt accumulation pattern of
    /// <see cref="SetStageDuration"/>.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> GetStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (route|shard) of LatticeGrain.GetAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.GetManyAsync</c>, the user-facing batched-read
    /// <see cref="ILattice.GetManyAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. End-to-end caller-visible latency of one
    /// batched-read call (includes routing, per-key bucketing, per-shard
    /// parallel fan-out, the registry-snapshot double-check, and any
    /// stale-routing retries). Pair with
    /// <see cref="GetManyStageDuration"/> to attribute the envelope to
    /// one of its sub-spans.
    /// </summary>
    public static readonly Histogram<double> GetManyDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get_many.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.GetManyAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one sub-stage of
    /// <c>LatticeGrain.GetManyAsync</c>. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagStage"/> (<c>route</c> | <c>bucket</c> |
    /// <c>fanout</c> | <c>merge</c>).
    /// <para>
    /// Mirrors <see cref="SetManyStageDuration"/> for the batched-read
    /// path. The <c>route</c> stage covers the <c>GetRoutingAsync</c>
    /// fetch; <c>bucket</c> the per-key shard bucketing loop;
    /// <c>fanout</c> the cross-shard <c>Task.WhenAll</c> dispatch;
    /// <c>merge</c> the post-fan-out result merge plus the
    /// snapshot-stability and topology-stability checks. One observation
    /// per stage per inner attempt: under a snapshot retry or
    /// stale-routing storm a single call records multiple data points
    /// per stage so the histogram attributes the retry cost honestly.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> GetManyStageDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get_many.stage.duration", unit: "ms",
            description: "Wall-clock ms inside one sub-stage (route|bucket|fanout|merge) of LatticeGrain.GetManyAsync.");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.ExistsAsync</c>, the user-facing key-existence
    /// <see cref="ILattice.ExistsAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. Lower-traffic than <see cref="GetDuration"/>
    /// in typical workloads but exposed for symmetry with the other
    /// read-side envelopes so a dashboard tile can confirm activity.
    /// </summary>
    public static readonly Histogram<double> ExistsDuration =
        Meter.CreateHistogram<double>("orleans.lattice.exists.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.ExistsAsync call (caller-visible envelope).");

    /// <summary>
    /// Histogram of wall-clock ms inside one call to
    /// <c>LatticeGrain.GetWithVersionAsync</c>, the user-facing versioned-read
    /// <see cref="ILattice.GetWithVersionAsync"/> entry point. Tagged with
    /// <see cref="TagTree"/>. Lower-traffic than <see cref="GetDuration"/>
    /// in typical workloads but exposed for symmetry with the other
    /// read-side envelopes so an operator can verify version-probe
    /// activity.
    /// </summary>
    public static readonly Histogram<double> GetWithVersionDuration =
        Meter.CreateHistogram<double>("orleans.lattice.get_with_version.duration", unit: "ms",
            description: "Wall-clock ms inside one LatticeGrain.GetWithVersionAsync call (caller-visible envelope).");

    // --- Retroactive shard-split sweep instruments ----------------

    /// <summary>
    /// Counter of in-flight prepared mutations retroactively
    /// shadow-forwarded from a source shard's leaf chain to the
    /// destination shard at the start of an adaptive split's
    /// <c>BeginShadowWrite</c> phase. Tagged with <see cref="TagTree"/>
    /// and <see cref="TagShard"/> (the source shard index). One
    /// increment per <see cref="Orleans.Lattice.BPlusTree.PendingMutationSnapshot"/> replayed.
    /// </summary>
    public static readonly Counter<long> SplitRetroactiveForwardEntries =
        Meter.CreateCounter<long>("orleans.lattice.split.retroactive_forward.entries", unit: "{entry}",
            description: "Pending prepared mutations retroactively shadow-forwarded at the start of a shard split.");

    /// <summary>
    /// Histogram of the wall-clock duration the split coordinator
    /// spends inside the retroactive shadow-forward sweep before
    /// transitioning to the <c>Drain</c> phase. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagShard"/> (the source
    /// shard index).
    /// </summary>
    public static readonly Histogram<double> SplitRetroactiveForwardDuration =
        Meter.CreateHistogram<double>("orleans.lattice.split.retroactive_forward.duration", unit: "ms",
            description: "Wall-clock duration of the retroactive prepared-mutation sweep at shard-split BeginShadowWrite entry.");

    // --- Autonomic split admission instruments (HotShardMonitorGrain) -------

    /// <summary>
    /// Histogram sampled once per autonomic monitor pass with the number of
    /// splits currently in flight for that tree (derived from each shard's
    /// authoritative <c>IsSplitting</c> status). Tagged with <see cref="TagTree"/>.
    /// Emitted every pass <em>regardless</em> of whether the cluster-wide split
    /// gate (<see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>) is
    /// enabled, so operators can compute the cluster aggregate as a
    /// <c>sum</c> across the <c>tree</c> tag and decide whether they need the
    /// gate and how to size it.
    /// </summary>
    public static readonly Histogram<long> SplitInFlight =
        Meter.CreateHistogram<long>("orleans.lattice.split.in_flight", unit: "{split}",
            description: "Per-tree autonomic splits in flight, sampled every monitor pass (sum across tree for the cluster total).");

    /// <summary>
    /// Counter of hot, otherwise-eligible shards that could not trigger an
    /// autonomic split this pass because a concurrency cap (the per-tree
    /// <see cref="LatticeOptions.MaxConcurrentAutoSplits"/> or the cluster-wide
    /// <see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>) was already
    /// reached. Tagged with <see cref="TagTree"/>. Emitted regardless of whether
    /// the cluster gate is enabled; a chronically non-zero value across many
    /// trees signals aggregate split pressure the per-tree cap alone cannot see.
    /// </summary>
    public static readonly Counter<long> SplitCandidatesSuppressed =
        Meter.CreateCounter<long>("orleans.lattice.split.candidates_suppressed", unit: "{shard}",
            description: "Hot eligible shards that could not split this pass because a concurrency cap was reached.");

    /// <summary>
    /// Counter incremented only when the <em>cluster-wide</em> admission gate
    /// denied an otherwise-eligible autonomic split (no permit available under
    /// <see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/>). Tagged with
    /// <see cref="TagTree"/> and <see cref="TagReason"/> (<c>cluster_cap</c>).
    /// Flat-zero means the ceiling never binds; sustained non-zero alongside
    /// rising hot-shard latency means the ceiling is set too low.
    /// </summary>
    public static readonly Counter<long> SplitAdmissionDeferred =
        Meter.CreateCounter<long>("orleans.lattice.split.admission.deferred", unit: "{shard}",
            description: "Otherwise-eligible autonomic splits held back by the cluster-wide admission gate.");

    /// <summary><see cref="TagReason"/> = <c>cluster_cap</c> on <see cref="SplitAdmissionDeferred"/>.</summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredClusterCapReasonTag = new(TagReason, "cluster_cap");

    /// <summary>
    /// <see cref="TagReason"/> = <c>uniform_load</c> on <see cref="SplitAdmissionDeferred"/>.
    /// Emitted for a shard that is above the ops/sec threshold but whose tree is
    /// uniformly loaded, so a split would relieve nothing. Sustained non-zero
    /// values are the signature of a bulk ingest, not of a hot spot.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredUniformLoadReasonTag = new(TagReason, "uniform_load");

    /// <summary>
    /// <see cref="TagReason"/> = <c>low_occupancy</c> on <see cref="SplitAdmissionDeferred"/>.
    /// Emitted for a hot, skewed shard that holds too few live entries for a
    /// split to redistribute anything.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredLowOccupancyReasonTag = new(TagReason, "low_occupancy");

    /// <summary>
    /// <see cref="TagReason"/> = <c>shard_ceiling</c> on <see cref="SplitAdmissionDeferred"/>.
    /// Emitted for a hot shard held back because the tree has reached its
    /// per-tree physical shard ceiling. Sustained non-zero means the ceiling is
    /// binding and should be reviewed alongside the tree's shard count.
    /// </summary>
    public static readonly KeyValuePair<string, object?> SplitDeferredShardCeilingReasonTag = new(TagReason, "shard_ceiling");

    // --- Compaction policy instruments (TombstoneCompactionGrain) -----------

    /// <summary>
    /// Histogram of full <c>RunCompactionPassAsync</c> wall-clock duration
    /// recorded by <c>TombstoneCompactionGrain.CompleteCompactionAsync</c>.
    /// Tagged with <see cref="TagTree"/> and <see cref="TagTrigger"/>
    /// (<c>reminder</c> for the periodic reminder, <c>ratio</c> /
    /// <c>size</c> for the corresponding policy triggers, or
    /// <c>operator</c> for an explicit <c>RequestCompactionAsync</c>
    /// call). Distinct from <see cref="LeafCompactionDuration"/>, which
    /// is per-leaf.
    /// </summary>
    public static readonly Histogram<double> CompactionPassDuration =
        Meter.CreateHistogram<double>("orleans.lattice.compaction.pass.duration", unit: "ms",
            description: "Full tombstone-compaction pass duration, tagged by tree and trigger.");

    /// <summary>
    /// Counter of leaves visited by a compaction pass, tagged with
    /// <see cref="TagTree"/> and <see cref="TagOutcome"/> = <c>reaped</c>
    /// (the leaf removed at least one tombstone or expired entry),
    /// <c>noop</c> (the leaf short-circuited because nothing has changed
    /// since its last compaction), or <c>skipped</c> (the leaf threw and
    /// the pass advanced past it). Lets operators distinguish work-done
    /// from work-skipped on a single rate panel.
    /// </summary>
    public static readonly Counter<long> CompactionLeavesVisited =
        Meter.CreateCounter<long>("orleans.lattice.compaction.leaves.visited", unit: "{leaf}",
            description: "Leaves visited by a tombstone-compaction pass, tagged by outcome.");

    /// <summary>
    /// Counter incremented once per per-shard retry inside
    /// <c>TombstoneCompactionGrain.ProcessNextShardAsync</c>. Tagged with
    /// <see cref="TagTree"/>. A non-zero rate means at least one shard's
    /// per-leaf compaction call threw and the pass deferred a fresh
    /// attempt within the same activation.
    /// </summary>
    public static readonly Counter<long> CompactionShardRetries =
        Meter.CreateCounter<long>("orleans.lattice.compaction.shard.retries", unit: "{retry}",
            description: "Per-shard compaction retries inside a single pass.");

    /// <summary>
    /// Counter incremented once per shard whose retry budget was
    /// exhausted and whose cursor advanced without a successful
    /// compaction. Tagged with <see cref="TagTree"/>. A persistent
    /// non-zero rate is the operational alert signal that a shard is
    /// consistently failing past <c>MaxRetriesPerShard</c>.
    /// </summary>
    public static readonly Counter<long> CompactionShardSkipped =
        Meter.CreateCounter<long>("orleans.lattice.compaction.shard.skipped", unit: "{shard}",
            description: "Shards whose per-pass retry budget was exhausted, tagged by tree.");

    /// <summary>
    /// Histogram of the dirty-leaf snapshot size pulled from each shard
    /// root at the start of every compaction shard pass. Tagged with
    /// <see cref="TagTree"/>. A value of <c>0</c> means the shard
    /// activated only its shard-root grain on the pass; a non-zero
    /// value reflects the count of leaves the coordinator activated
    /// via the dirty-leaves fast path. Capacity-planning signal for the
    /// "<c>O(shards + dirty_leaves)</c>" pass-cost target.
    /// </summary>
    public static readonly Histogram<int> CompactionShardDirtyLeaves =
        Meter.CreateHistogram<int>("orleans.lattice.compaction.shard.dirty_leaves", unit: "{leaf}",
            description: "Per-shard dirty-leaf snapshot size at compaction shard-pass start.");

    /// <summary>
    /// Histogram of per-leaf tombstone-to-total ratio
    /// (<c>tombstones / max(liveKeys + tombstones, 1)</c>) sampled
    /// inside a tombstone-compaction pass, just before
    /// <c>CompactTombstonesAsync</c> performs its scan. Tagged with
    /// <see cref="TagTree"/> and <see cref="TagLeaf"/>. Surfaces
    /// space-amplification hot spots without requiring an
    /// <c>ObservableGauge</c> over a registry of live activations -
    /// the histogram is observed lazily inside the pass, so it costs
    /// nothing on the read or write hot paths.
    /// </summary>
    public static readonly Histogram<double> LeafTombstoneRatio =
        Meter.CreateHistogram<double>("orleans.lattice.leaf.tombstone.ratio", unit: "1",
            description: "Per-leaf tombstone-to-total ratio sampled inside compaction passes.");

    // --- Cached constant tag pairs (allocation-free hot-path helpers) -------
    //
    // KeyValuePair<string, object?> is a value type and the corresponding
    // Histogram<T>.Record / Counter<T>.Add overloads with explicit 1/2/3
    // KeyValuePair parameters never allocate. Caching the constant-valued
    // pairs as static-readonly fields elides the per-call struct construction
    // (a handful of CPU cycles) and the dictionary lookup inside the metric
    // handler, and is more readable than inline `new KeyValuePair<...>(name,
    // literal)` repeated dozens of times.

    /// <summary><see cref="TagOutcome"/> = <c>noop</c>.</summary>
    public static readonly KeyValuePair<string, object?> OutcomeNoop = new(TagOutcome, "noop");

    /// <summary><see cref="TagOutcome"/> = <c>reaped</c>.</summary>
    public static readonly KeyValuePair<string, object?> OutcomeReaped = new(TagOutcome, "reaped");

    /// <summary><see cref="TagOutcome"/> = <c>skipped</c>.</summary>
    public static readonly KeyValuePair<string, object?> OutcomeSkipped = new(TagOutcome, "skipped");

    /// <summary><see cref="TagKind"/> = <c>compact</c> (per-leaf WAL-write attribution).</summary>
    public static readonly KeyValuePair<string, object?> KindCompact = new(TagKind, "compact");

    /// <summary><see cref="TagKind"/> = <c>compaction</c> (coordinator-completion attribution).</summary>
    public static readonly KeyValuePair<string, object?> KindCompaction = new(TagKind, "compaction");

    /// <summary><see cref="TagTrigger"/> = <c>reminder</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerReminderTag = new(TagTrigger, "reminder");

    /// <summary><see cref="TagTrigger"/> = <c>ratio</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerRatioTag = new(TagTrigger, "ratio");

    /// <summary><see cref="TagTrigger"/> = <c>size</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerSizeTag = new(TagTrigger, "size");

    /// <summary><see cref="TagTrigger"/> = <c>operator</c>.</summary>
    public static readonly KeyValuePair<string, object?> TriggerOperatorTag = new(TagTrigger, "operator");

    /// <summary><see cref="TagPath"/> = <c>walk</c> (legacy / fallback leaf-chain walk).</summary>
    public static readonly KeyValuePair<string, object?> PathWalkTag = new(TagPath, "walk");

    /// <summary><see cref="TagPath"/> = <c>dirty-set</c> (dirty-leaves fast path).</summary>
    public static readonly KeyValuePair<string, object?> PathDirtySetTag = new(TagPath, "dirty-set");

    /// <summary>String label for the legacy leaf-chain walk path.</summary>
    public const string PathWalk = "walk";

    /// <summary>String label for the shard-root dirty-leaves fast path.</summary>
    public const string PathDirtySet = "dirty-set";

    /// <summary><see cref="TagPhase"/> = <c>phase1</c>.</summary>
    public static readonly KeyValuePair<string, object?> PhasePhase1Tag = new(TagPhase, "phase1");

    /// <summary><see cref="TagPhase"/> = <c>phase2</c>.</summary>
    public static readonly KeyValuePair<string, object?> PhasePhase2Tag = new(TagPhase, "phase2");

    /// <summary>
    /// <see cref="TagPhase"/> = <c>prologue</c> (a range-scan page fill was
    /// still preparing its shard for the operation).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PhaseScanPagePrologueTag = new(TagPhase, "prologue");

    /// <summary>
    /// <see cref="TagPhase"/> = <c>descent</c> (a range-scan page fill was
    /// still traversing down to its start leaf).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PhaseScanPageDescentTag = new(TagPhase, "descent");

    /// <summary>
    /// <see cref="TagPhase"/> = <c>leaf-walk</c> (a range-scan page fill was
    /// reading the leaf chain).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PhaseScanPageLeafWalkTag = new(TagPhase, "leaf-walk");

    /// <summary>
    /// <see cref="TagPhase"/> = <c>baseline-fold</c> (a snapshot baseline
    /// capture was folding the frozen leaves' WAL tails back onto their frozen
    /// caches, the fanned-out second pass of the capture).
    /// </summary>
    public static readonly KeyValuePair<string, object?> PhaseScanPageBaselineFoldTag = new(TagPhase, "baseline-fold");

    /// <summary><see cref="TagStage"/> = <c>resolve</c> (step 1 affected-leaves resolution).</summary>
    public static readonly KeyValuePair<string, object?> StageResolveTag = new(TagStage, "resolve");

    /// <summary><see cref="TagStage"/> = <c>hlc</c> (step 2 ComputeTerminalHlcAsync fan-out + tick).</summary>
    public static readonly KeyValuePair<string, object?> StageHlcTag = new(TagStage, "hlc");

    /// <summary><see cref="TagStage"/> = <c>wal</c> (step 3 commit-log adapter append).</summary>
    public static readonly KeyValuePair<string, object?> StageWalTag = new(TagStage, "wal");

    /// <summary><see cref="TagStage"/> = <c>fanout</c> (step 4 per-leaf ApplyTxTerminalAsync dispatch + shadow-forward).</summary>
    public static readonly KeyValuePair<string, object?> StageFanOutTag = new(TagStage, "fanout");

    /// <summary><see cref="TagStage"/> = <c>gate</c> (LatticeGrain.SetManyAsync pre-flight: compaction reminder + monitor + events-gate probe).</summary>
    public static readonly KeyValuePair<string, object?> StageGateTag = new(TagStage, "gate");

    /// <summary><see cref="TagStage"/> = <c>route</c> (LatticeGrain.SetManyAsync / GetManyAsync routing fetch via GetRoutingAsync; LatticeGrain.GetAsync per-attempt shard resolution).</summary>
    public static readonly KeyValuePair<string, object?> StageRouteTag = new(TagStage, "route");

    /// <summary><see cref="TagStage"/> = <c>bucket</c> (LatticeGrain.SetManyAsync / GetManyAsync per-key shard bucketing loop).</summary>
    public static readonly KeyValuePair<string, object?> StageBucketTag = new(TagStage, "bucket");

    /// <summary><see cref="TagStage"/> = <c>events</c> (LatticeGrain.SetManyAsync trailing per-entry PublishEventAsync foreach).</summary>
    public static readonly KeyValuePair<string, object?> StageEventsTag = new(TagStage, "events");

    /// <summary><see cref="TagStage"/> = <c>shard</c> (LatticeGrain.SetAsync / GetAsync inner shard RPC including stale-routing retries).</summary>
    public static readonly KeyValuePair<string, object?> StageShardTag = new(TagStage, "shard");

    /// <summary><see cref="TagStage"/> = <c>publish</c> (LatticeGrain.SetAsync trailing PublishEventAsync hop).</summary>
    public static readonly KeyValuePair<string, object?> StagePublishTag = new(TagStage, "publish");

    /// <summary><see cref="TagStage"/> = <c>merge</c> (LatticeGrain.GetManyAsync post-fan-out result merge plus snapshot- and topology-stability checks).</summary>
    public static readonly KeyValuePair<string, object?> StageMergeTag = new(TagStage, "merge");

    // --- Auto-trained compression-dictionary instruments -------------------
    //
    // Emitted by AutoTrainingCompressionDictionaryProvider, the opt-in
    // (default-off) provider that samples a bounded reservoir of payloads and
    // periodically trains a Zstandard dictionary off the hot path. When
    // auto-training is disabled the provider emits none of these. The two
    // observable gauges (active_version, reservoir_fill) are registered lazily
    // by the provider instance (so they cost nothing when no provider is
    // constructed and nothing when no listener is attached); their canonical
    // names are exposed as `...Name` constants so the dashboards drift-guard
    // recognises the PromQL token forms even though the instruments are not
    // statically constructed on this meter. The three counters are ordinary
    // counters constructed on the meter below.

    /// <summary>
    /// Counter incremented once per auto-training pass attempt, tagged with
    /// <see cref="TagOutcome"/> = <c>trained</c> (a dictionary was built),
    /// <c>skipped_insufficient_samples</c> (the reservoir held fewer than the
    /// configured minimum, or the underlying builder rejected the corpus), or
    /// <c>skipped_cadence</c> (the minimum training interval had not yet
    /// elapsed since the previous attempt).
    /// </summary>
    public static readonly Counter<long> CompressionDictionaryTrainingRuns =
        Meter.CreateCounter<long>(CompressionDictionaryTrainingRunsName, unit: "{run}",
            description: "Auto-training dictionary pass attempts, tagged by outcome (trained, skipped_insufficient_samples, skipped_cadence).");

    /// <summary>Canonical name of <see cref="CompressionDictionaryTrainingRuns"/>.</summary>
    public const string CompressionDictionaryTrainingRunsName = "orleans.lattice.compress.dictionary.training_runs";

    /// <summary>
    /// Counter of the no-dictionary (plain Zstandard) baseline compressed
    /// bytes of the training probe, summed once per successful training pass.
    /// Paired with <see cref="CompressionDictionaryTrainedBytesOut"/>: the
    /// trained-dictionary compression-ratio delta versus the dictionary-less
    /// baseline is <c>trained_bytes_out / trained_bytes_in</c> (a value below
    /// <c>1</c> means the trained dictionary beats plain Zstandard on the
    /// sampled corpus).
    /// </summary>
    public static readonly Counter<long> CompressionDictionaryTrainedBytesIn =
        Meter.CreateCounter<long>(CompressionDictionaryTrainedBytesInName, unit: "By",
            description: "No-dictionary (plain Zstd) baseline compressed bytes of the training probe, summed per successful auto-training pass.");

    /// <summary>Canonical name of <see cref="CompressionDictionaryTrainedBytesIn"/>.</summary>
    public const string CompressionDictionaryTrainedBytesInName = "orleans.lattice.compress.dictionary.trained_bytes_in";

    /// <summary>
    /// Counter of the trained-dictionary compressed bytes of the training
    /// probe, summed once per successful training pass. See
    /// <see cref="CompressionDictionaryTrainedBytesIn"/> for the ratio
    /// interpretation.
    /// </summary>
    public static readonly Counter<long> CompressionDictionaryTrainedBytesOut =
        Meter.CreateCounter<long>(CompressionDictionaryTrainedBytesOutName, unit: "By",
            description: "Trained-dictionary compressed bytes of the training probe, summed per successful auto-training pass.");

    /// <summary>Canonical name of <see cref="CompressionDictionaryTrainedBytesOut"/>.</summary>
    public const string CompressionDictionaryTrainedBytesOutName = "orleans.lattice.compress.dictionary.trained_bytes_out";

    /// <summary>
    /// Canonical name of the observable gauge reporting the currently active
    /// auto-trained dictionary id (the monotonic version the encoder should
    /// request). <c>0</c> means no dictionary has been trained yet. Registered
    /// lazily by <see cref="AutoTrainingCompressionDictionaryProvider"/>; not
    /// statically constructed on the meter.
    /// </summary>
    public const string CompressionDictionaryActiveVersionName = "orleans.lattice.compress.dictionary.active_version";

    /// <summary>
    /// Canonical name of the observable gauge reporting auto-training reservoir
    /// occupancy. Reports two series tagged with <see cref="TagKind"/>:
    /// <c>samples</c> (retained sample count) and <c>bytes</c> (retained total
    /// bytes). Registered lazily by
    /// <see cref="AutoTrainingCompressionDictionaryProvider"/>; not statically
    /// constructed on the meter.
    /// </summary>
    public const string CompressionDictionaryReservoirFillName = "orleans.lattice.compress.dictionary.reservoir_fill";

    /// <summary><see cref="TagOutcome"/> = <c>trained</c> (an auto-training pass built a dictionary).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeTrained = new(TagOutcome, "trained");

    /// <summary><see cref="TagOutcome"/> = <c>skipped_insufficient_samples</c> (the reservoir held too few samples to train).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeSkippedInsufficientSamples = new(TagOutcome, "skipped_insufficient_samples");

    /// <summary><see cref="TagOutcome"/> = <c>skipped_cadence</c> (the minimum training interval had not elapsed).</summary>
    public static readonly KeyValuePair<string, object?> OutcomeSkippedCadence = new(TagOutcome, "skipped_cadence");

    /// <summary><see cref="TagKind"/> = <c>samples</c> (reservoir-fill sample-count series).</summary>
    public static readonly KeyValuePair<string, object?> ReservoirFillSamplesTag = new(TagKind, "samples");

    /// <summary><see cref="TagKind"/> = <c>bytes</c> (reservoir-fill total-bytes series).</summary>
    public static readonly KeyValuePair<string, object?> ReservoirFillBytesTag = new(TagKind, "bytes");

    // --- Materialised-view instruments (view maintainer) -------------------------

    /// <summary>
    /// Identifies the materialised view a view-maintenance instrument relates to.
    /// Value is the logical view name (the <c>view-{name}</c> tree's name).
    /// </summary>
    public const string TagView = "view";

    /// <summary>
    /// Histogram of view apply lag, recorded each drain pass as the number of
    /// source WAL entries committed but not yet applied to the view at the start
    /// of the pass. Tagged with <see cref="TagView"/>. A persistently high value
    /// indicates the maintainer is falling behind the source write rate.
    /// </summary>
    public static readonly Histogram<long> ViewApplyLag =
        Meter.CreateHistogram<long>("orleans.lattice.view.apply_lag", unit: "{entry}",
            description: "Source WAL entries committed but not yet applied to the view, sampled per drain pass.");

    /// <summary>
    /// Histogram of the per-pass backlog depth: the number of source entries a
    /// single drain pass read before reaching the source head (bounded by the
    /// view's configured batch size). Tagged with <see cref="TagView"/>.
    /// </summary>
    public static readonly Histogram<long> ViewBacklogDepth =
        Meter.CreateHistogram<long>("orleans.lattice.view.backlog_depth", unit: "{entry}",
            description: "Source entries read in a single view drain pass before reaching the source head.");

    /// <summary>
    /// Counter of view writes applied to the view tree (post-coalesce upserts and
    /// deletes). Tagged with <see cref="TagView"/>. Differentiating apply rate from
    /// backlog depth distinguishes coalesce efficiency from raw source throughput.
    /// </summary>
    public static readonly Counter<long> ViewApplied =
        Meter.CreateCounter<long>("orleans.lattice.view.applied", unit: "{write}",
            description: "View writes applied to the view tree after per-batch last-writer-wins coalescing.");

    /// <summary>
    /// Counter of re-key collisions detected in a view drain batch: a view key
    /// produced by two or more distinct source keys under an injective re-map (a
    /// configuration error). Tagged with <see cref="TagView"/>. A non-zero value
    /// means the projection's key re-map is not injective; the maintainer falls
    /// back to source-HLC last-writer-wins so the view stays well-defined, but the
    /// colliding keys' resolution is non-deterministic with respect to intent.
    /// </summary>
    public static readonly Counter<long> ViewKeyCollisions =
        Meter.CreateCounter<long>("orleans.lattice.view.key_collisions", unit: "{collision}",
            description: "View keys produced by more than one distinct source key under an injective re-map, per drain batch.");

    /// <summary>
    /// Counter of aggregation contributions applied to an aggregation view's group
    /// accumulators (folds and retractions). Tagged with <see cref="TagView"/>.
    /// Distinguishes aggregation apply throughput from the filter / re-project
    /// <see cref="ViewApplied"/> upsert/delete counter.
    /// </summary>
    public static readonly Counter<long> ViewAggregationApplied =
        Meter.CreateCounter<long>("orleans.lattice.view.aggregation_applied", unit: "{contribution}",
            description: "Aggregation contributions (folds and retractions) applied to an aggregation view's group accumulators.");

    /// <summary>
    /// Counter of aggregation contributions rejected because the projection's
    /// group-key selector produced a key in the reserved region - empty, or
    /// beginning with the reserved NUL (<c>\u0000</c>) prefix the maintainer uses
    /// for its internal accumulator / inverse / membership rows. Tagged with
    /// <see cref="TagView"/>. The maintainer drops the offending contribution
    /// rather than writing a group value that would be invisible to view reads
    /// (which floor above the reserved region) and could collide with an internal
    /// row; the rejection is deterministic on the key, so every cluster drops the
    /// same members and the view stays convergent. A non-zero value means a
    /// group-key selector is emitting reserved keys and should be corrected.
    /// </summary>
    public static readonly Counter<long> ViewAggregationRejected =
        Meter.CreateCounter<long>("orleans.lattice.view.aggregation_rejected", unit: "{contribution}",
            description: "Aggregation contributions dropped because the group-key selector produced a reserved (empty or NUL-prefixed) key.");

    /// <summary>
    /// Counter of atomic-write staging backstop fall-backs: a drain pass
    /// abandoned incremental atomic-batch staging and forced a rebuild because
    /// the in-flight staging buffer exceeded its configured bound
    /// (<see cref="LatticeViewOptions.MaxStagedTransactions"/> /
    /// <see cref="LatticeViewOptions.MaxStagedBytes"/>) or an un-terminated
    /// batch's blocked-floor pin would sink below the source WAL retention
    /// ceiling. Tagged with <see cref="TagView"/>. A non-zero value means a
    /// saga terminal was lost or the maintainer fell behind the atomic-write
    /// rate; the view still converges via the rebuild, but the
    /// not-visible-until-committed batch was reassembled from current source
    /// state rather than the staged prepares.
    /// </summary>
    public static readonly Counter<long> ViewAtomicStagingBackstop =
        Meter.CreateCounter<long>("orleans.lattice.view.atomic_staging_backstop", unit: "{rebuild}",
            description: "Drain passes that abandoned atomic-batch staging and forced a rebuild because the staging buffer exceeded its bound or its blocked-floor pin would sink below WAL retention.");

    /// <summary>
    /// Counter of cross-tree joint-atomicity-violation degradations: a view
    /// participating in a cross-tree atomic write waited the bounded
    /// <see cref="LatticeViewOptions.CrossTreeReadinessTimeout"/> for every other
    /// participant view to become ready, did not observe a joint flip, and so
    /// degraded to per-tree-slice atomicity - flipping its own slice atomically
    /// into its own view tree and scheduling a reconcile. Tagged with
    /// <see cref="TagView"/>. A non-zero value means a participant view was
    /// permanently unavailable (cluster partition / crashed maintainer) and the
    /// participating views did not flip together for that batch; the views still
    /// converge via the scheduled reconcile, but a reader could momentarily have
    /// observed one view's slice without another's.
    /// </summary>
    public static readonly Counter<long> ViewCrossTreeJointViolation =
        Meter.CreateCounter<long>("orleans.lattice.view.cross_tree_joint_violation", unit: "{degradation}",
            description: "Cross-tree view batches that degraded to per-tree-slice atomicity because a participant view did not become ready within the bounded readiness timeout.");

    /// <summary>
    /// Counter of lag-budget evictions: a view exceeded its per-view
    /// <see cref="LatticeViewOptions.MaxLagBudget"/> (chronically slow, or a crashed
    /// maintainer that only reactivated on a keepalive tick) and was force-evicted -
    /// the maintainer unpinned the source WAL (so a chronically-lagging or dead view
    /// can no longer hold the WAL garbage collector) and re-onboarded the view via a
    /// rebuild from current committed source state, which re-pins at the rebuilt
    /// head. Tagged with <see cref="TagView"/>. A non-zero value means the view fell
    /// further behind than its configured budget at least once; the view still
    /// converges via the rebuild, but the bounded backlog was dropped rather than
    /// tail-replayed.
    /// </summary>
    public static readonly Counter<long> ViewLagBudgetEviction =
        Meter.CreateCounter<long>("orleans.lattice.view.lag_budget_eviction", unit: "{eviction}",
            description: "Views force-evicted (WAL unpinned and rebuilt) because they exceeded their configured MaxLagBudget.");

    /// <summary>
    /// Counter of background drain passes that observed the source tree under WAL
    /// saturation back-pressure and consequently reduced their footprint - a
    /// scaled-down batch size and, for a background timer tick, a deferral of the
    /// next pass - so the asynchronous maintainer hands client concurrency back to
    /// the foreground writer rather than competing with it. Tagged with
    /// <see cref="TagView"/> and <see cref="TagWalSaturationState"/> (the observed
    /// source regime, <c>throttled</c> or <c>saturated</c>). A sustained non-zero
    /// rate means the source tree is hot enough that the view is deliberately
    /// lagging to protect foreground throughput; it converges once the source
    /// recovers. Never recorded while the source is <c>healthy</c> or when
    /// <see cref="LatticeViewOptions.ObeySourceBackpressure"/> is disabled.
    /// </summary>
    public static readonly Counter<long> ViewSourceBackpressure =
        Meter.CreateCounter<long>("orleans.lattice.view.source_backpressure", unit: "{pass}",
            description: "View maintainer drain passes that throttled themselves because the source tree was under WAL saturation back-pressure.");

    /// <summary>
    /// Histogram of the number of calls to a target activation that were already
    /// outstanding from this silo at the instant a further call was dispatched to
    /// it. Tagged with <see cref="TagGrainType"/>. Recorded by
    /// <c>LatticeGrainCallObservationFilter</c>, which a host installs with
    /// <see cref="LatticeServiceCollectionExtensions.AddLatticeGrainCallObservation(Hosting.ISiloBuilder)"/>.
    /// <para>
    /// <b>This instrument exists because the alternative is censored.</b> The
    /// only out-of-the-box description of Orleans' per-activation non-reentrancy
    /// queue is the <c>NonReentrancyQueueSize=</c> clause of the
    /// <c>Response did not arrive on time</c> timeout diagnostic. That clause is
    /// emitted only for a request already approaching the message timeout and
    /// describes only the <em>emitting</em> request's own wait, so a grain type
    /// whose calls are deeply queued but which does not itself trip the timeout
    /// contributes no rows at all, and the rows that do exist are truncated at
    /// the timeout threshold. An extraction from that channel can therefore
    /// report no queueing, be internally consistent, and reproduce exactly - the
    /// sampling frame excluded the phenomenon rather than biasing the estimate.
    /// This histogram records at dispatch on <em>every</em> outgoing call, with
    /// no timeout, fault, or threshold in its emission condition, so its
    /// population is not truncated.
    /// </para>
    /// <para>
    /// <b>Read it as a floor, not a measurement of the queue.</b> Only calls
    /// issued from this silo are counted, so calls to the same activation from
    /// another silo or an external client make the value an under-estimate; and
    /// for a <c>[Reentrant]</c> grain type or an <c>[AlwaysInterleave]</c>
    /// method the outstanding calls interleave rather than queue, so a high
    /// value there means pipelining and not contention. Contrast
    /// <see cref="LeafCommitInFlight"/>, which is measured after the scheduler
    /// has dequeued the request and consequently pins at one on a non-reentrant
    /// grain however deep the real queue is.
    /// </para>
    /// </summary>
    public static readonly Histogram<int> GrainCallOutstandingDepth =
        Meter.CreateHistogram<int>("orleans.lattice.grain.call.outstanding_depth", unit: "{call}",
            description: "Calls to a target activation already outstanding from this silo when a further call was dispatched, by grain type.");

    /// <summary>
    /// Histogram of end-to-end outgoing grain call duration, clocked on the
    /// caller side around the whole call. Tagged with
    /// <see cref="TagGrainType"/> and <see cref="TagOutcome"/>
    /// (<c>completed</c> or <c>faulted</c>). Recorded by
    /// <c>LatticeGrainCallObservationFilter</c>.
    /// <para>
    /// The outcome split is load-bearing rather than decorative. A message
    /// timeout surfaces as a fault after the full timeout has elapsed, so a
    /// single undifferentiated duration series mixes a completion-latency
    /// population with a population pinned at the timeout threshold, and the
    /// second can swamp the first exactly when a cluster is saturated. Selecting
    /// <c>outcome=completed</c> yields request-completion latency that a healthy
    /// run populates; selecting <c>outcome=faulted</c> isolates the timeouts
    /// rather than letting them masquerade as slow completions.
    /// </para>
    /// </summary>
    public static readonly Histogram<double> GrainCallDuration =
        Meter.CreateHistogram<double>("orleans.lattice.grain.call.duration", unit: "ms",
            description: "End-to-end outgoing grain call duration observed by the caller, by grain type and outcome.");
}
