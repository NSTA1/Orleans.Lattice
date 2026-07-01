using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Configuration options for the Azure Table Storage
/// <see cref="IWalStorageProvider"/>. Hosts register the provider via
/// <see cref="LatticeAzureTableServiceCollectionExtensions.AddAzureTableWalStorage"/>
/// and supply a delegate that populates this object.
/// <para>
/// Exactly one of <see cref="ConnectionString"/>,
/// <see cref="ServiceUri"/> + <see cref="TokenCredential"/>,
/// <see cref="ServiceUri"/> + <see cref="SharedKeyCredential"/>, or
/// a pre-built <see cref="ServiceClient"/> must be configured. The
/// provider reads the populated authentication mode at first use and,
/// for the three credential-based modes, constructs a long-lived
/// <see cref="TableServiceClient"/> from it; subsequent edits to these
/// fields are not observed. When <see cref="ServiceClient"/> is set,
/// the supplied instance is returned verbatim and the host owns its
/// lifetime and <see cref="TableClientOptions"/>.
/// </para>
/// </summary>
public sealed class AzureTableWalStorageOptions
{
    /// <summary>
    /// The default <see cref="TableName"/> when none is supplied. Forty
    /// characters or fewer; alphanumeric only; starts with a letter -
    /// matches Azure Table Storage's naming rules.
    /// </summary>
    public const string DefaultTableName = "OrleansLatticeWal";

    /// <summary>
    /// Storage account connection string. When set, the provider builds
    /// the <see cref="TableServiceClient"/> via
    /// <see cref="TableServiceClient(string)"/>. Mutually exclusive with
    /// <see cref="ServiceUri"/>.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Storage account table-service endpoint URI (e.g.
    /// <c>https://{account}.table.core.windows.net</c>). When set,
    /// either <see cref="TokenCredential"/> or
    /// <see cref="SharedKeyCredential"/> must also be supplied.
    /// </summary>
    public Uri? ServiceUri { get; set; }

    /// <summary>
    /// Optional Azure AD credential used in conjunction with
    /// <see cref="ServiceUri"/>. Pair with
    /// <c>new DefaultAzureCredential()</c> for managed-identity
    /// scenarios. Mutually exclusive with
    /// <see cref="SharedKeyCredential"/>.
    /// </summary>
    public TokenCredential? TokenCredential { get; set; }

    /// <summary>
    /// Optional shared-key credential used in conjunction with
    /// <see cref="ServiceUri"/>. Mutually exclusive with
    /// <see cref="TokenCredential"/>.
    /// </summary>
    public TableSharedKeyCredential? SharedKeyCredential { get; set; }

    /// <summary>
    /// Optional pre-built <see cref="TableServiceClient"/> supplied by
    /// the host. When set, the provider uses this instance verbatim
    /// instead of constructing its own from
    /// <see cref="ConnectionString"/> / <see cref="ServiceUri"/> +
    /// credential; <see cref="ConfigureClientOptions"/> is ignored and
    /// the host owns the client's <see cref="TableClientOptions"/> and
    /// lifetime. Mirrors the canonical Orleans wiring shape used by
    /// <c>AddAzureTableGrainStorage</c>'s <c>TableServiceClient</c>
    /// slot so a silo configured with one shared
    /// <see cref="TableServiceClient"/> (typically constructed once
    /// with <c>DefaultAzureCredential</c> for managed-identity
    /// deployments) can route every Azure-backed component through the
    /// same client. Mutually exclusive with <see cref="ConnectionString"/>,
    /// <see cref="ServiceUri"/>, <see cref="TokenCredential"/>, and
    /// <see cref="SharedKeyCredential"/>.
    /// </summary>
    public TableServiceClient? ServiceClient { get; set; }

    /// <summary>
    /// The Azure Table that backs the WAL. Defaults to
    /// <see cref="DefaultTableName"/>. The table is created on first
    /// use (idempotent) so hosts do not need to provision it
    /// out-of-band; specify a non-default name to share an account
    /// across multiple Lattice clusters without WAL collisions.
    /// </summary>
    public string TableName { get; set; } = DefaultTableName;

    /// <summary>
    /// Optional callback invoked when the provider constructs the
    /// <see cref="TableClientOptions"/> for the underlying
    /// <see cref="TableServiceClient"/>. Lets the host attach custom
    /// retry policies, diagnostics, or transport without the provider
    /// having to surface a pass-through option per setting. The default
    /// (null) leaves the options at <c>Azure.Data.Tables</c> defaults.
    /// <para>
    /// Invoked <i>after</i> the provider applies any of the
    /// <see cref="RetryMaxAttempts"/> / <see cref="RetryDelay"/> /
    /// <see cref="RetryMaxDelay"/> / <see cref="RetryNetworkTimeout"/>
    /// / <see cref="RetryMode"/> knobs to
    /// <see cref="ClientOptions.Retry"/>, so anything the host
    /// does inside this callback wins. To attach an additional
    /// per-retry policy without dropping the provider's bundled
    /// <see cref="RetryAttemptTrackingPolicy"/>, call
    /// <see cref="ClientOptions.AddPolicy"/> rather than replacing
    /// <see cref="ClientOptions.Retry"/> wholesale.
    /// </para>
    /// </summary>
    public Action<TableClientOptions>? ConfigureClientOptions { get; set; }

    /// <summary>
    /// When set, overrides <see cref="RetryOptions.MaxRetries"/> on
    /// the constructed <see cref="ClientOptions.Retry"/>. The
    /// value is the number of <i>retries</i> after the initial
    /// attempt (so <c>RetryMaxAttempts = 3</c> yields up to four total
    /// attempts), matching the Azure.Core convention. <c>null</c>
    /// leaves the SDK default (3 retries) in place; <c>0</c>
    /// disables retries entirely. Must be non-negative.
    /// <para>
    /// Phase A observed a 5–100x gap between
    /// wall p99 (700–1,700 ms) and Azure Tables server-timing p99
    /// (10–130 ms) on the WAL hot path, which is consistent with the
    /// SDK's default 4-attempt × 0.8 s base-delay exponential backoff
    /// dominating wall latency. This knob, together with
    /// <see cref="RetryDelay"/> / <see cref="RetryMaxDelay"/> /
    /// <see cref="RetryNetworkTimeout"/>, lets operators tune the
    /// per-call retry budget without surrendering the SDK's transient
    /// fault classification or having to provide a full
    /// <see cref="ConfigureClientOptions"/> delegate.
    /// </para>
    /// <para>
    /// Ignored when <see cref="ServiceClient"/> is set: in pre-built
    /// client mode the host owns the retry policy on the
    /// <see cref="TableClientOptions"/> it constructed.
    /// </para>
    /// </summary>
    public int? RetryMaxAttempts { get; set; }

    /// <summary>
    /// When set, overrides <see cref="RetryOptions.Delay"/> on the
    /// constructed <see cref="ClientOptions.Retry"/> - the base
    /// delay used by the exponential / fixed backoff strategy.
    /// <c>null</c> leaves the SDK default (0.8 s) in place. Must be
    /// non-negative and not exceed <see cref="RetryMaxDelay"/> when
    /// both are set.
    /// <para>
    /// Ignored when <see cref="ServiceClient"/> is set.
    /// </para>
    /// </summary>
    public TimeSpan? RetryDelay { get; set; }

    /// <summary>
    /// When set, overrides <see cref="RetryOptions.MaxDelay"/> on the
    /// constructed <see cref="ClientOptions.Retry"/> - the
    /// per-attempt upper bound on backoff. <c>null</c> leaves the SDK
    /// default (60 s) in place. Must be non-negative and at least as
    /// large as <see cref="RetryDelay"/> when both are set.
    /// <para>
    /// Ignored when <see cref="ServiceClient"/> is set.
    /// </para>
    /// </summary>
    public TimeSpan? RetryMaxDelay { get; set; }

    /// <summary>
    /// Overrides <see cref="RetryOptions.NetworkTimeout"/> on the
    /// constructed <see cref="ClientOptions.Retry"/> - the
    /// per-attempt deadline applied at the transport layer. Defaults to
    /// <see cref="DefaultRetryNetworkTimeout"/> (10 s); set to
    /// <c>null</c> to restore the unbounded SDK default (~100 s). Must
    /// be positive when set.
    /// <para>
    /// Functions as a per-attempt deadline budget: a stuck request
    /// cannot keep a WAL slot occupied longer than this value before
    /// being cancelled and either retried (if attempts remain) or
    /// surfacing a <see cref="LatticeMetrics.ProviderRetryExhausted"/>-tagged failure
    /// to the caller. Ignored when <see cref="ServiceClient"/> is set.
    /// </para>
    /// <para>
    /// <b>Why a finite default.</b> The Azure SDK's retry loop observes
    /// cancellation only <i>between</i> attempts, not while a single
    /// attempt is parked on the transport (the
    /// <c>NetworkTimeout</c> is the only bound on an in-flight attempt).
    /// Under a sustained single-account Azure Tables brown-out a WAL
    /// shard self-bounds each flush at
    /// <c>LatticeOptions.WalFlushTimeout</c> (15 s default) and abandons
    /// its await, but with the SDK's unbounded ~100 s
    /// <c>NetworkTimeout</c> the abandoned HTTP attempt keeps running -
    /// hundreds of these zombie attempts accumulate and self-sustain the
    /// brown-out independently of any conflict-retry path. A finite
    /// default below <c>WalFlushTimeout</c> makes a stuck attempt
    /// surface a real <see cref="Azure.RequestFailedException"/> into the
    /// shard's reconcile-aware failure handler within the flush budget
    /// (so the slot is released and recovered) instead of being
    /// abandoned while the transport zombies on for ~100 s. The
    /// <see cref="SaturationAwareRetryPolicy"/> short-circuits the
    /// <i>retries</i> after the first attempt; this default bounds the
    /// first attempt the policy intentionally never short-circuits.
    /// </para>
    /// </summary>
    public TimeSpan? RetryNetworkTimeout { get; set; } = DefaultRetryNetworkTimeout;

    /// <summary>
    /// Default value for <see cref="RetryNetworkTimeout"/> (10 seconds).
    /// Chosen to sit below the <c>LatticeOptions.WalFlushTimeout</c>
    /// default (15 s) so a hung transport attempt surfaces a fault into
    /// the WAL shard's failure handler within the per-flush budget,
    /// while still leaving 75-1000x headroom over the WAL hot path's
    /// observed server-timing p99 (10-130 ms) so healthy transactions
    /// are never spuriously timed out.
    /// </summary>
    public static readonly TimeSpan DefaultRetryNetworkTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// When set, overrides <see cref="RetryOptions.Mode"/> on the
    /// constructed <see cref="ClientOptions.Retry"/>. Defaults to
    /// <c>null</c>, which leaves the SDK default (<see cref="Azure.Core.RetryMode.Exponential"/>)
    /// in place. Ignored when <see cref="ServiceClient"/> is set.
    /// </summary>
    public RetryMode? RetryMode { get; set; }

    /// <summary>
    /// When <see langword="true"/>, an <c>AppendBatchAsync</c> call
    /// returns as soon as its phase-0 candidate-row and phase-1
    /// entry-row transactions are durable, without waiting for the
    /// per-shard <c>PhaseTwoWorker</c> to commit the manifest row +
    /// <c>TAIL</c> upsert. The next call into the same shard awaits
    /// the previous call's phase-2 task before returning, so failures
    /// remain sticky and a caller can never advance past an
    /// unrecovered phase-2 fault undetected. Default is
    /// <see langword="true"/> - the throughput campaign's measured
    /// Azure-Tables operating-point default and the second-highest-
    /// impact entry on the library-default-flip ladder (synchronous
    /// phase-2 forces every commit to await its own manifest+TAIL
    /// update, halving the steady-state per-shard request-path
    /// latency). Set to <see langword="false"/> to restore the
    /// pre-v6.0 historical behaviour (every <c>AppendBatchAsync</c>
    /// awaits its own phase-2 commit before returning).
    /// <para>
    /// <b>Why pipeline.</b> Each call's request-path latency without
    /// pipelining is <c>max(phase0, phase1) + phase2</c>, and the
    /// per-shard worker's coalescing window (up to 49 phase-2
    /// commits collapsed into one transaction) is wasted because it
    /// never sees more than one pending commit at a time. Pipelining
    /// overlaps phase 2 of batch <c>N</c> with phase 0+1 of batch
    /// <c>N+1</c>, which halves the steady-state request-path
    /// latency per shard and turns the worker's coalescing window
    /// from "never used" to "saturates under burst".
    /// </para>
    /// <para>
    /// <b>What changes.</b> The WAL's all-or-nothing durability
    /// contract is preserved: phase 0 stamps a candidate-row whose
    /// <c>Offset</c> column carries <c>endOffsetInclusive</c>, phase
    /// 1 commits the entry rows atomically, and activation-time
    /// <see cref="IWalStorageProvider.ReconcileAsync"/> rolls forward
    /// any candidate-row whose <c>startOffset</c> contiguously
    /// extends <c>TAIL</c>. A silo crash between phase 1 and phase 2
    /// leaves the batch as a rollforward-eligible orphan whether or
    /// not the caller observed phase 2 returning; the only thing
    /// that moves is when a phase-2 failure surfaces to the caller.
    /// In pipelined mode it surfaces on the <i>next</i>
    /// <c>AppendBatchAsync</c> on the same shard rather than the
    /// failing one. The <c>WalShardGrain</c> sticky-failure resync
    /// path (<see cref="IWalStorageProvider.GetHighestOffsetAsync"/>
    /// + <see cref="IWalStorageProvider.ReconcileAsync"/>) is the
    /// same in both cases because the failed batch's phase 0+1 are
    /// durable and reconciliation rolls them forward.
    /// </para>
    /// <para>
    /// <b>What stays the same.</b> Strict offset-FIFO ordering of
    /// phase-2 commits (the worker's <c>SortedSet</c> invariant);
    /// the worker's "fault the in-flight commit and every later
    /// pending commit" semantics on a phase-2 transaction failure;
    /// the activation-time orphan recovery contract; the
    /// <c>GetHighestOffsetAsync</c> point-read against the
    /// <c>TAIL</c> row. The only observable change is that
    /// <c>GetHighestOffsetAsync</c> issued <i>between</i> an
    /// <c>AppendBatchAsync</c> returning and its phase-2 commit
    /// landing may return the pre-append <c>TAIL</c> rather than the
    /// post-append <c>TAIL</c>. <c>WalShardGrain</c> tracks
    /// <c>_nextOffset</c> in memory and never re-reads <c>TAIL</c>
    /// outside of activation and post-failure resync, so the lag is
    /// invisible to the canonical replication path.
    /// </para>
    /// </summary>
    public bool PipelinePhaseTwoCommits { get; set; } = DefaultPipelinePhaseTwoCommits;

    /// <summary>Default value for <see cref="PipelinePhaseTwoCommits"/> (<c>true</c>; the throughput-campaign operating-point default).</summary>
    public const bool DefaultPipelinePhaseTwoCommits = true;

    /// <summary>
    /// When <see langword="true"/>, <c>AppendBatchAsync</c> and
    /// <c>AppendEncodedBatchAsync</c> skip the phase-0 candidate-row
    /// (C-row) <c>UpsertEntityAsync</c> against the shard's manifest
    /// partition entirely. Activation-time
    /// <see cref="IWalStorageProvider.ReconcileAsync"/> falls back to
    /// a cross-partition discovery scan that enumerates batch
    /// partitions for the shard with a partition-key range filter
    /// anchored at <c>TAIL + 1</c>; the C-row scan still runs first so
    /// any pre-upgrade orphans stamped while the option was off are
    /// still reconciled. Default is <see langword="true"/> - the
    /// throughput campaign's measured Azure-Tables operating-point
    /// default. An A/B against real Azure Tables at the 25k-writer
    /// saturation rung moved the sustained-ingest watermark from
    /// ~58k entries (C-row inline) to ~1.38M entries (C-row elided),
    /// a ~24x capacity gain, because the phase-0 upsert no longer
    /// contends with the per-shard <c>PhaseTwoWorker</c> on the shared
    /// manifest partition. Set to <see langword="false"/> to restore
    /// the pre-v6.0 historical behaviour (C-row written inline on
    /// every batch; reconciliation discovers orphans via the
    /// manifest-partition C-row scan).
    /// <para>
    /// <b>Why elide.</b> The C-row sits in the per-shard manifest
    /// partition, which is the same partition the per-shard
    /// <c>PhaseTwoWorker</c> commits against. Azure Tables (and
    /// Azurite) serialise writes within a PartitionKey server-side, so
    /// the phase-0 upsert contends with the worker's draining queue
    /// and adds a server-side-serialised round-trip to every batch's
    /// inline critical path. v5.1.0's single-transaction equivalent
    /// did not have this contention and ran ~180x faster on the same
    /// Azurite. Eliding phase-0 removes the contended round-trip from
    /// the hot path; the cost is paid at activation time as a single
    /// cross-partition discovery query (bounded below by
    /// <c>TAIL + 1</c>, typically empty in steady state).
    /// </para>
    /// <para>
    /// <b>Soundness.</b> A batch partition whose encoded
    /// <c>startOffset</c> is greater than the persisted <c>TAIL</c>
    /// is necessarily an orphan: phase-2 advances <c>TAIL</c>
    /// atomically with the manifest row, so any
    /// <c>startOffset &gt; TAIL</c> batch partition has phase-1
    /// rows but no phase-2 commit. <c>TrimAsync</c> only deletes
    /// rows below the trim watermark (which never exceeds
    /// <c>TAIL</c>), so committed batches that have not yet been
    /// trimmed do not appear in the discovery scan. The orphan's
    /// <c>endOffsetInclusive</c> is recovered as
    /// <c>max(RowKey)</c> over the orphan's entry rows.
    /// </para>
    /// <para>
    /// <b>Activation-time cost.</b> One cross-partition query against
    /// the WAL table, filtered to partition keys of the form
    /// <c>{BatchPartitionPrefix}|{encoded-treeId}|{shardIndex}|S*</c>
    /// with row-key bound anchored at <c>TAIL + 1</c>. In steady
    /// state with no orphans this returns immediately. The discovery
    /// scan replaces the legacy C-row scan only on shards where every
    /// in-flight batch was appended with the option on; the legacy
    /// scan still runs first so a silo upgraded mid-life finds both
    /// pre- and post-upgrade orphans.
    /// </para>
    /// </summary>
    public bool EliminateCandidateRowOnHotPath { get; set; } = DefaultEliminateCandidateRowOnHotPath;

    /// <summary>Default value for <see cref="EliminateCandidateRowOnHotPath"/> (<c>true</c>; the throughput-campaign operating-point default that elides the contended per-shard manifest-partition C-row write from the hot path).</summary>
    public const bool DefaultEliminateCandidateRowOnHotPath = true;

    /// <summary>
    /// Optional observer invoked when a pipelined phase-2 task faults.
    /// <para>
    /// In pipelined mode (<see cref="PipelinePhaseTwoCommits"/> =
    /// <see langword="true"/>) the per-shard slot holds the previous
    /// batch's phase-2 task and the next <c>AppendBatchAsync</c> is
    /// the canonical observer. If no successor call ever arrives -
    /// e.g. the producer has just appended its last batch and goes
    /// quiescent - the slot's faulted task would otherwise be
    /// observed only by <see cref="AzureTableWalStorageProvider.DisposeAsync"/>,
    /// which intentionally swallows the fault. The data itself is
    /// still recoverable (phase 0+1 are durable and
    /// <see cref="IWalStorageProvider.ReconcileAsync"/> rolls the
    /// batch forward at next activation), but the application
    /// receives no signal that its last phase-2 failed.
    /// </para>
    /// <para>
    /// When set, this delegate is invoked exactly once per faulted
    /// pipelined phase-2 task, on a thread-pool continuation chained
    /// off the slot occupant the moment the fault becomes observable.
    /// The delegate is fired regardless of whether a successor call
    /// later arrives (which would also surface the fault), so
    /// implementations should be idempotent / log-only - typically
    /// a structured-log emit so the operator-side alerting layer
    /// learns about the fault even on a quiescent shard. Exceptions
    /// thrown by the delegate are swallowed; the delegate is not in
    /// the call's request path.
    /// </para>
    /// <para>
    /// Default <see langword="null"/>: pipelined faults on a
    /// quiescent shard are silently dropped (the historical
    /// pre-fix behaviour, retained as the default so the option
    /// stays purely additive).
    /// </para>
    /// </summary>
    public Action<Exception>? PipelinedPhaseTwoFaultHandler { get; set; }

    /// <summary>
    /// Maximum wall-time the per-shard
    /// <c>PhaseTwoWorker</c> drain loop deliberately waits, after the
    /// first arrival but before submitting the coalesced phase-2
    /// transaction, so additional pending commits can accumulate.
    /// Default <see cref="TimeSpan.Zero"/> preserves the historical
    /// drain-on-first-signal behaviour (one phase-2 commit per
    /// transaction whenever per-shard arrival inter-spacing exceeds
    /// the commit's own duration).
    /// <para>
    /// <b>Why a coalescing window.</b> Phase A observations (see
    /// <c></c>) showed <c>provider.phase2.batch_size</c>
    /// pinned at exactly <c>1.00</c> across hundreds of thousands of
    /// samples even when producer-side knobs
    /// (<c>WalMaxPendingBatches</c>, <c>WalPartitions</c>) were swept
    /// to widen the inbound stream. Root cause: under a steady-state
    /// per-partition arrival rate slower than the phase-2 commit's
    /// own latency, the channel is empty at the moment the previous
    /// commit returns, so the next <c>WaitToReadAsync</c> wakes on
    /// the very first arrival and commits a one-item batch. A small,
    /// opt-in window between the first arrival and the commit gives
    /// the worker an opportunity to coalesce additional arrivals
    /// into the same Azure Tables transaction without weakening the
    /// strict offset-FIFO invariant.
    /// </para>
    /// <para>
    /// <b>Soundness.</b> The window only delays the first commit
    /// after a quiet period; the SortedSet ordering primitive is
    /// unchanged, every committed group is still drained in
    /// ascending <c>startOffset</c> order, and the per-transaction
    /// ceiling of 49 coalesced batches still applies. The window is
    /// short-circuited the moment the worker already has 49 pending
    /// commits buffered - there is no point waiting once the
    /// transaction is full.
    /// </para>
    /// <para>
    /// <b>Cost.</b> The window adds up to this much wall-time to the
    /// inline phase-2 latency of an isolated batch (one arrival, no
    /// follow-up). Under burst load the cost is amortised because
    /// multiple commits collapse into one round-trip; under steady
    /// load with no follow-up arrivals the cost is paid every batch.
    /// Pick a value smaller than the observed phase-2 commit
    /// duration p50 so the steady-state-loss case stays bounded.
    /// Must be non-negative.
    /// </para>
    /// <para>
    /// <b>Default.</b> 5 ms - the measured Azure-Tables sweet spot
    /// the throughput campaign settled on as the operating-point
    /// default and the highest-impact entry on the campaign's
    /// library-default-flip ladder (a probe at
    /// <c>PhaseTwoCoalescingWindow = 0</c> measured a -57% throughput
    /// regression vs <c>= 5 ms</c> on the same baseline; a probe at
    /// <c>= 2 ms</c> measured -28% vs <c>= 5 ms</c>). Set to
    /// <see cref="TimeSpan.Zero"/> to restore the pre-v6.0 historical
    /// behaviour (commit-on-first-arrival) when the workload's
    /// arrival shape makes any window-induced delay net-negative.
    /// </para>
    /// </summary>
    public TimeSpan PhaseTwoCoalescingWindow { get; set; } = DefaultPhaseTwoCoalescingWindow;

    /// <summary>Default value for <see cref="PhaseTwoCoalescingWindow"/> (5 ms - the throughput-campaign sweet spot).</summary>
    public static readonly TimeSpan DefaultPhaseTwoCoalescingWindow = TimeSpan.FromMilliseconds(5);

    /// <summary>
    /// When set, bounds the wall-time the per-shard
    /// <c>PhaseTwoWorker</c> waits for any single coalesced phase-2
    /// manifest commit (the Azure Tables
    /// <c>SubmitTransactionAsync</c> round-trip) before abandoning it
    /// and faulting that commit's batch. Defaults to
    /// <see cref="DefaultPhaseTwoCommitTimeout"/> (3 s). Set to
    /// <c>null</c> to disable the deadline and restore the historical
    /// behaviour, in which the commit is bounded only by the worker's
    /// lifetime token and the SDK's own
    /// <see cref="RetryNetworkTimeout"/> per-attempt budget, so a
    /// transaction that never returns - a hung socket, a server-side
    /// partition stall, or an SDK retry loop that keeps re-issuing -
    /// blocks the shard's strict offset-FIFO drain loop indefinitely
    /// and every later pending commit on that shard wedges behind it.
    /// Must be positive when set.
    /// <para>
    /// <b>Why a per-commit deadline.</b> The per-shard worker commits
    /// phase-2 transactions one coalesced group at a time and the next
    /// group cannot start until the current
    /// <c>SubmitTransactionAsync</c> completes (the SortedSet ordering
    /// invariant requires it). The grain-side <c>WalShardGrain</c>
    /// back-pressure and flush deadline bound the <i>foreground</i>
    /// append path, but they do not bound this <i>background</i>
    /// commit: once the worker's await on the Azure call stops making
    /// progress there is no other timer covering it. A finite
    /// per-commit deadline converts an unbounded hang into a bounded
    /// fault that the existing sticky-failure resync path
    /// (<see cref="IWalStorageProvider.GetHighestOffsetAsync"/> +
    /// <see cref="IWalStorageProvider.ReconcileAsync"/>) recovers,
    /// exactly as it already recovers a phase-2 transaction failure.
    /// </para>
    /// <para>
    /// <b>What changes on a timeout.</b> The commit's batch and every
    /// later still-pending commit on the shard fault with a
    /// <see cref="TimeoutException"/> (same fault-the-window semantics
    /// as a transaction error, because their tail offsets are now
    /// stale relative to the recovered <c>TAIL</c>). The worker
    /// continues draining new arrivals. Each timeout increments the
    /// <c>orleans.lattice.provider.phase2.commit.timeouts</c> counter
    /// so operators can prove whether the deadline ever fired.
    /// </para>
    /// <para>
    /// <b>Sizing.</b> Set comfortably above the observed phase-2
    /// commit p99 so healthy commits never trip it, but below the
    /// silo-level activation / request timeout so a wedged shard is
    /// broken before it cascades. The 3 s default
    /// (<see cref="DefaultPhaseTwoCommitTimeout"/>) sits well above the
    /// observed real-Azure phase-2 commit p99 (sub-second) while still
    /// breaking a wedged shard before the silo-level request timeout.
    /// <c>null</c> leaves the seam unbounded (the pre-default
    /// behaviour).
    /// </para>
    /// </summary>
    public TimeSpan? PhaseTwoCommitTimeout { get; set; } = DefaultPhaseTwoCommitTimeout;

    /// <summary>Default value for <see cref="PhaseTwoCommitTimeout"/> (3 s; comfortably above the observed real-Azure phase-2 commit p99 yet below the silo-level request timeout, so a wedged shard is broken before it cascades).</summary>
    public static readonly TimeSpan DefaultPhaseTwoCommitTimeout = TimeSpan.FromSeconds(3);

    /// <summary>
    /// When <see langword="true"/> (the default), the provider attaches
    /// a <see cref="SaturationAwareRetryPolicy"/> to the constructed
    /// <see cref="TableClientOptions"/> so the Azure SDK's internal
    /// retry loop short-circuits on retries whenever the silo-scoped
    /// <see cref="IWalSaturationSignal"/> reports
    /// <see cref="WalSaturationState.Saturated"/>. Has no effect when
    /// no <see cref="IWalSaturationSignal"/> is registered in DI (the
    /// single-node / unit-test deployment shape that does not call
    /// <c>AddLattice</c>; the provider falls back to the historical
    /// unguarded behaviour with no policy attached).
    /// <para>
    /// <b>Why honor the signal.</b> Under the canonical Azure Tables
    /// single-account 409-Conflict regime the <c>Azure.Data.Tables</c>
    /// SDK's internal retry policy ignores the silo's drain
    /// <see cref="System.Threading.CancellationToken"/> once the call
    /// has handed off to the underlying <c>Socket.SendAsync</c>. This
    /// produces a post-producer-stop signature where the saturation
    /// classifier and writer-side admission gate release cleanly but
    /// the SDK's retry queue keeps re-issuing the same in-flight
    /// transactions, polluting the silo's drain wall-clock and the
    /// stall-watchdog with hundreds of parked async frames. The
    /// signal-aware policy abandons each retry as soon as the
    /// classifier escalates the regime, looping the failure back
    /// through the writer's provider-failure-count path on the next
    /// sampler tick instead of waiting out the SDK's full retry
    /// budget.
    /// </para>
    /// <para>
    /// <b>What changes on a saturated retry.</b> The policy stamps a
    /// synthetic 503 <see cref="Azure.Response"/> with a zero
    /// <c>Retry-After</c> onto the in-flight message and returns
    /// without invoking the rest of the pipeline. The SDK's outer
    /// retry policy observes the 503 and exits the retry chain; the
    /// caller's catch site sees the same
    /// <see cref="Azure.RequestFailedException"/> shape it would see
    /// for an SDK-exhausted retry, which the writer's existing
    /// provider-failure-count path attributes to the third Saturated
    /// classifier input. The
    /// <c>orleans.lattice.provider.retry.short_circuited</c> counter
    /// increments once per short-circuited attempt so operators can
    /// prove the policy is doing its job.
    /// </para>
    /// <para>
    /// <b>First attempts are never short-circuited.</b> Only retries
    /// are abandoned; a fresh request always reaches the network at
    /// least once, even under
    /// <see cref="WalSaturationState.Saturated"/>. This preserves
    /// the steady-state hot path exactly and keeps the policy purely
    /// additive on the healthy path.
    /// </para>
    /// <para>
    /// <b>Opt-out.</b> Set to <see langword="false"/> to restore the
    /// historical unguarded retry behaviour: the provider does not
    /// attach the policy regardless of whether
    /// <see cref="IWalSaturationSignal"/> is registered, and the
    /// SDK's internal retry policy runs unmodified. Operators who
    /// front a pre-built <see cref="ServiceClient"/> own the policy
    /// list on the <see cref="TableClientOptions"/> they constructed
    /// and can attach <see cref="SaturationAwareRetryPolicy"/>
    /// directly if needed; this option only affects the provider's
    /// own pipeline construction.
    /// </para>
    /// </summary>
    public bool HonorSaturationSignal { get; set; } = DefaultHonorSaturationSignal;

    /// <summary>Default value for <see cref="HonorSaturationSignal"/> (<see langword="true"/>; the policy is purely additive so opt-in is the safe default).</summary>
    public const bool DefaultHonorSaturationSignal = true;

    /// <summary>
    /// Sticky-window duration the saturation-aware retry policy keeps
    /// short-circuiting SDK retries for after the last observation of
    /// <see cref="WalSaturationState.Saturated"/>. Bridges the gap
    /// between the silo's sample interval (default 200 ms - the
    /// effective lifetime of any single Saturated tick) and the SDK's
    /// exponential retry spacing (default 800 ms - 3.2 s between
    /// attempts). Without the cooldown the next SDK retry almost
    /// always arrives well after the Saturated observation has
    /// decayed to Throttled or Healthy, so the policy would pass the
    /// retry through to the network and burn additional storage-side
    /// capacity that the silo's classifier just told us is exhausted.
    /// <para>
    /// Mechanically: the policy stamps the wall-clock when it
    /// observes <see cref="WalSaturationState.Saturated"/>; on every
    /// subsequent retry attempt it short-circuits if
    /// <c>now - lastSaturated &lt; SaturationShortCircuitCooldown</c>,
    /// independent of the current aggregate state. The wall-clock
    /// source is the standard <see cref="TimeProvider.System"/> in
    /// production; tests inject a fake clock for determinism.
    /// </para>
    /// <para>
    /// Default 2 seconds: large enough to span the worst-case SDK
    /// exponential-backoff retry interval at default settings without
    /// extending into the silo's drain budget. Set to
    /// <see cref="TimeSpan.Zero"/> to disable the sticky window
    /// (policy only fires while the signal is presently Saturated).
    /// </para>
    /// </summary>
    public TimeSpan SaturationShortCircuitCooldown { get; set; } = DefaultSaturationShortCircuitCooldown;

    /// <summary>Default value for <see cref="SaturationShortCircuitCooldown"/> (2 seconds).</summary>
    public static readonly TimeSpan DefaultSaturationShortCircuitCooldown = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Maximum number of <i>additional</i> in-place phase-1 commit attempts the provider makes
    /// after a <b>transient</b> fault before it surfaces the fault to the calling
    /// <c>WalShardGrain</c>. <c>2</c> (the default) means up to three total attempts per phase-1
    /// commit. Set to <c>0</c> to restore the historical behaviour (surface every transient fault
    /// immediately).
    /// <para>
    /// <b>What this fixes.</b> A phase-1 batch commits in a single atomic Azure Table transaction
    /// keyed by its first offset. When that transaction faults <i>transiently</i> - a network
    /// timeout, a 408 / 429 / 500 / 503, or a network-level cancellation that is not the silo's own
    /// drain token - the batch may or may not have become durable, but phase-2 has not run. The
    /// historical path surfaced that fault to the shard, which latched a sticky failure and resynced
    /// <c>_nextOffset</c> to the phase-2 <c>TAIL</c> (blind to the just-written, still-uncommitted
    /// phase-1 rows above it). The shard then re-coalesced different mutations and re-drove
    /// <i>divergent</i> content onto those occupied offsets, producing an <i>unprovable</i>
    /// <c>409 EntityAlreadyExists</c> that faulted again - a positive-feedback conflict storm that
    /// collapses sustained throughput (observed under single-account <c>set-point</c> saturation).
    /// </para>
    /// <para>
    /// <b>Why retrying the identical batch is safe.</b> Each retry resubmits the <b>byte-identical</b>
    /// batch at the <b>same offsets</b>. If the prior attempt was already durable, the retry's
    /// <c>Add</c> returns <c>409</c>, which the existing O(1) idempotent-replay proof
    /// (<see cref="IWalStorageProvider"/> phase-1 path) resolves as a success; if it was not durable,
    /// the retry commits it. Either way the offsets and content never change, so the shard never
    /// faults, never resyncs, and never re-drives divergent content - the storm cannot ignite. An
    /// unprovable <c>409</c> (a genuine collision with a <i>different</i> resident batch) still
    /// surfaces immediately without retry, so this loop cannot spin. Each retry increments
    /// <c>orleans.lattice.provider.phase1.transient_retries</c>.
    /// </para>
    /// <para>
    /// Must be non-negative.
    /// </para>
    /// </summary>
    public int PhaseOneTransientRetryMaxAttempts { get; set; } = DefaultPhaseOneTransientRetryMaxAttempts;

    /// <summary>Default value for <see cref="PhaseOneTransientRetryMaxAttempts"/> (2 additional attempts; three total).</summary>
    public const int DefaultPhaseOneTransientRetryMaxAttempts = 2;

    /// <summary>
    /// Base delay for the jittered backoff applied between in-place phase-1 transient retries
    /// (see <see cref="PhaseOneTransientRetryMaxAttempts"/>). The actual wait before attempt
    /// <c>n</c> (1-based) is a random value in <c>[0, BaseDelay * n)</c>, capped at
    /// <see cref="DefaultPhaseOneTransientRetryMaxDelay"/>. The jitter desynchronises the
    /// re-drive paths of multiple hot shards so they do not retry in lockstep (a touch of
    /// backoff damping complementing the offset-preserving retry). Default 25 ms. Set to
    /// <see cref="TimeSpan.Zero"/> to retry without delay. Must be non-negative.
    /// </summary>
    public TimeSpan PhaseOneTransientRetryBaseDelay { get; set; } = DefaultPhaseOneTransientRetryBaseDelay;

    /// <summary>Default value for <see cref="PhaseOneTransientRetryBaseDelay"/> (25 ms).</summary>
    public static readonly TimeSpan DefaultPhaseOneTransientRetryBaseDelay = TimeSpan.FromMilliseconds(25);

    /// <summary>Upper bound on any single phase-1 transient-retry backoff wait (250 ms).</summary>
    public static readonly TimeSpan DefaultPhaseOneTransientRetryMaxDelay = TimeSpan.FromMilliseconds(250);


    /// <summary>
    /// Per-row WAL payload compression algorithm. Defaults to
    /// <see cref="LatticeCompression.Zstd"/>: each entry's encoded
    /// <c>WalRecord</c> payload at or above
    /// <see cref="CompressionMinPayloadBytes"/> is Zstandard-compressed
    /// before it is written to the row's <c>Payload</c> column, shrinking
    /// the retained on-disk footprint - and the per-append managed
    /// allocations - of larger mutations. The default is on because the
    /// Zstd CPU cost (single-digit microseconds per row) is negligible
    /// beside the Azure Table round-trip it rides on, the
    /// <see cref="CompressionMinPayloadBytes"/> threshold skips payloads
    /// too small to benefit, and an incompressible payload is detected and
    /// stored verbatim, so there is no footprint downside even for binary
    /// values. Set to <see cref="LatticeCompression.None"/> to write every
    /// payload verbatim, or to a host-defined tag in the <c>[0x80, 0xFF]</c>
    /// range to use a custom algorithm.
    /// <para>
    /// The provider resolves a matching <see cref="ILatticeCompressor"/>
    /// from DI at construction; <c>AddAzureTableWalStorage</c> registers the
    /// built-in <see cref="ZstdLatticeCompressor"/> fallback automatically,
    /// so the default works with no extra wiring. A
    /// non-<see cref="LatticeCompression.None"/> value with no registered
    /// compressor throws at construction time.
    /// </para>
    /// <para>
    /// The setting is backwards-compatible in both directions and requires
    /// no migration: rows written by an older silo carry no compression tag
    /// and decode as <see cref="LatticeCompression.None"/>, and compressed
    /// rows are self-describing (the row's compression-tag column selects
    /// the decompressor on read), so enabling, disabling, or changing the
    /// algorithm or Zstd level only affects newly written rows while older
    /// rows continue to decode unchanged and coexist. Granularity is per-row
    /// because the read path supports offset-addressable reads; see
    /// <c>docs/lattice/compression.md</c>.
    /// </para>
    /// </summary>
    public LatticeCompression Compression { get; set; } = DefaultCompression;

    /// <summary>Default value for <see cref="Compression"/> (<see cref="LatticeCompression.Zstd"/>).</summary>
    public const LatticeCompression DefaultCompression = LatticeCompression.Zstd;

    /// <summary>
    /// Minimum encoded payload size, in bytes, at or above which an
    /// entry's payload is compressed when <see cref="Compression"/> is
    /// enabled. Encoded payloads strictly smaller than this threshold
    /// are written verbatim (tagged <see cref="LatticeCompression.None"/>)
    /// to skip the fixed per-row compression CPU cost on payloads too
    /// small for the byte saving to repay it. Defaults to
    /// <see cref="DefaultCompressionMinPayloadBytes"/> (256 bytes): an
    /// in-process sweep of realistic JSON values through the real encode
    /// + Zstd-3 path showed compressed payloads never inflate at any
    /// size (so there is no break-even floor to clear) and that the
    /// reduction crosses ~25% at roughly this encoded size and climbs
    /// steeply above it, while below it the fixed per-call cost dominates
    /// a sub-15% saving. Ignored when <see cref="Compression"/> is
    /// <see cref="LatticeCompression.None"/>. Must be non-negative; a
    /// value of <c>0</c> compresses every payload (best for a
    /// footprint-bound workload where CPU is cheap relative to the
    /// storage round-trip).
    /// </summary>
    public int CompressionMinPayloadBytes { get; set; } = DefaultCompressionMinPayloadBytes;

    /// <summary>Default value for <see cref="CompressionMinPayloadBytes"/> (256 bytes).</summary>
    public const int DefaultCompressionMinPayloadBytes = 256;

    /// <summary>
    /// Validates that exactly one authentication mode is configured and
    /// that <see cref="TableName"/> is non-empty. Called by the provider
    /// at first use.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when zero or more than one authentication mode is configured, or when <see cref="TableName"/> is missing.</exception>
    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(TableName))
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(TableName)} must be a non-empty string.");
        }

        var hasConnectionString = !string.IsNullOrWhiteSpace(ConnectionString);
        var hasServiceUri = ServiceUri is not null;
        var hasTokenCredential = TokenCredential is not null;
        var hasSharedKey = SharedKeyCredential is not null;
        var hasServiceClient = ServiceClient is not null;

        if (hasServiceClient && (hasConnectionString || hasServiceUri || hasTokenCredential || hasSharedKey))
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(ServiceClient)} is mutually exclusive with "
                + $"{nameof(ConnectionString)} / {nameof(ServiceUri)} / {nameof(TokenCredential)} / {nameof(SharedKeyCredential)}. "
                + "Configure exactly one authentication mode.");
        }

        if (hasConnectionString && (hasServiceUri || hasTokenCredential || hasSharedKey))
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(ConnectionString)} is mutually exclusive with "
                + $"{nameof(ServiceUri)} / {nameof(TokenCredential)} / {nameof(SharedKeyCredential)}. Configure exactly one authentication mode.");
        }

        if (!hasConnectionString && !hasServiceUri && !hasServiceClient)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)} requires one of {nameof(ConnectionString)}, "
                + $"{nameof(ServiceUri)} (with a credential), or {nameof(ServiceClient)} to be configured.");
        }

        if (hasServiceUri && hasTokenCredential && hasSharedKey)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(TokenCredential)} and {nameof(SharedKeyCredential)} are mutually exclusive. Configure exactly one credential alongside {nameof(ServiceUri)}.");
        }

        if (hasServiceUri && !hasTokenCredential && !hasSharedKey)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(ServiceUri)} requires either {nameof(TokenCredential)} or {nameof(SharedKeyCredential)} to be configured.");
        }

        if (RetryMaxAttempts is { } maxAttempts && maxAttempts < 0)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(RetryMaxAttempts)} must be non-negative when set.");
        }

        if (RetryDelay is { } delay && delay < TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(RetryDelay)} must be non-negative when set.");
        }

        if (RetryMaxDelay is { } maxDelay && maxDelay < TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(RetryMaxDelay)} must be non-negative when set.");
        }

        if (RetryDelay is { } d && RetryMaxDelay is { } md && d > md)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(RetryDelay)} must not exceed {nameof(RetryMaxDelay)} when both are set.");
        }

        if (RetryNetworkTimeout is { } networkTimeout && networkTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(RetryNetworkTimeout)} must be positive when set.");
        }

        if (PhaseTwoCoalescingWindow < TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(PhaseTwoCoalescingWindow)} must be non-negative.");
        }

        if (PhaseTwoCommitTimeout is { } commitTimeout && commitTimeout <= TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(PhaseTwoCommitTimeout)} must be positive when set.");
        }

        if (SaturationShortCircuitCooldown < TimeSpan.Zero)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(SaturationShortCircuitCooldown)} must be non-negative.");
        }

        if (CompressionMinPayloadBytes < 0)
        {
            throw new InvalidOperationException(
                $"{nameof(AzureTableWalStorageOptions)}.{nameof(CompressionMinPayloadBytes)} must be non-negative.");
        }
    }

    /// <summary>
    /// Builds a fresh <see cref="TableServiceClient"/> from the
    /// configured authentication mode, or returns the host-supplied
    /// <see cref="ServiceClient"/> verbatim when that mode is in use.
    /// Called once per provider instance at first use; the resulting
    /// client is reused for the lifetime of the provider.
    /// <para>
    /// When <paramref name="saturationSignal"/> is non-<c>null</c> and
    /// <see cref="HonorSaturationSignal"/> is <see langword="true"/>,
    /// the provider attaches a
    /// <see cref="SaturationAwareRetryPolicy"/> to the constructed
    /// pipeline at <see cref="HttpPipelinePosition.PerRetry"/>; see
    /// the option's documentation for the regime details. Skipped in
    /// pre-built <see cref="ServiceClient"/> mode (the host owns the
    /// pipeline).
    /// </para>
    /// </summary>
    internal TableServiceClient BuildServiceClient(IWalSaturationSignal? saturationSignal = null)
    {
        Validate();

        if (ServiceClient is not null)
        {
            // Host owns the client's TableClientOptions and lifetime;
            // ConfigureClientOptions is intentionally ignored in this
            // mode because the client is already fully constructed.
            return ServiceClient;
        }

        var clientOptions = new TableClientOptions();

        // Tuning knobs: apply BEFORE the host's ConfigureClientOptions
        // callback so the host has the final word and can override any
        // value (or replace clientOptions.Retry wholesale) if needed.
        // null on a knob means "leave the SDK default in place".
        if (RetryMaxAttempts is { } maxAttempts)
        {
            clientOptions.Retry.MaxRetries = maxAttempts;
        }

        if (RetryDelay is { } delay)
        {
            clientOptions.Retry.Delay = delay;
        }

        if (RetryMaxDelay is { } maxDelay)
        {
            clientOptions.Retry.MaxDelay = maxDelay;
        }

        if (RetryNetworkTimeout is { } networkTimeout)
        {
            clientOptions.Retry.NetworkTimeout = networkTimeout;
        }

        if (RetryMode is { } retryMode)
        {
            clientOptions.Retry.Mode = retryMode;
        }

        ConfigureClientOptions?.Invoke(clientOptions);

        // Layered AFTER the user's ConfigureClientOptions callback so
        // hosts cannot accidentally drop our per-retry observability
        // (e.g. by replacing clientOptions.Transport or rebuilding the
        // policy list). Purely additive: never replaces clientOptions.Retry.
        // Skipped in pre-built ServiceClient mode (the early return
        // above) - hosts using that path attach
        // RetryAttemptTrackingPolicy.Instance themselves if they want
        // the counter populated.
        clientOptions.AddPolicy(RetryAttemptTrackingPolicy.Instance, HttpPipelinePosition.PerRetry);

        // Layered alongside RetryAttemptTrackingPolicy for the same
        // reason: PerRetry attachment AFTER the host's
        // ConfigureClientOptions callback so the host cannot
        // accidentally drop the saturation short-circuit by replacing
        // the policy list wholesale. Purely additive: never replaces
        // clientOptions.Retry. Only attached when the host both
        // registered an IWalSaturationSignal in DI (the AddLattice
        // path) and left HonorSaturationSignal at its default; opt
        // out by setting HonorSaturationSignal = false. Hosts using
        // the pre-built ServiceClient mode (the early return above)
        // attach SaturationAwareRetryPolicy themselves if they want
        // the short-circuit.
        if (HonorSaturationSignal && saturationSignal is not null)
        {
            clientOptions.AddPolicy(
                new SaturationAwareRetryPolicy(saturationSignal, SaturationShortCircuitCooldown, TimeProvider.System),
                HttpPipelinePosition.PerRetry);
        }

        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            return new TableServiceClient(ConnectionString, clientOptions);
        }

        if (TokenCredential is not null)
        {
            return new TableServiceClient(ServiceUri!, TokenCredential, clientOptions);
        }

        return new TableServiceClient(ServiceUri!, SharedKeyCredential!, clientOptions);
    }
}
