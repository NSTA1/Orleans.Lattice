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
    /// <see cref="TableClientOptions.Retry"/>, so anything the host
    /// does inside this callback wins. To attach an additional
    /// per-retry policy without dropping the provider's bundled
    /// <see cref="RetryAttemptTrackingPolicy"/>, call
    /// <see cref="ClientOptions.AddPolicy"/> rather than replacing
    /// <see cref="TableClientOptions.Retry"/> wholesale.
    /// </para>
    /// </summary>
    public Action<TableClientOptions>? ConfigureClientOptions { get; set; }

    /// <summary>
    /// When set, overrides <see cref="RetryOptions.MaxRetries"/> on
    /// the constructed <see cref="TableClientOptions.Retry"/>. The
    /// value is the number of <i>retries</i> after the initial
    /// attempt (so <c>RetryMaxAttempts = 3</c> yields up to four total
    /// attempts), matching the Azure.Core convention. <c>null</c>
    /// leaves the SDK default (3 retries) in place; <c>0</c>
    /// disables retries entirely. Must be non-negative.
    /// <para>
    /// Phase A (see <c>scaling.md</c>) observed a 5–100x gap between
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
    /// constructed <see cref="TableClientOptions.Retry"/> — the base
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
    /// constructed <see cref="TableClientOptions.Retry"/> — the
    /// per-attempt upper bound on backoff. <c>null</c> leaves the SDK
    /// default (60 s) in place. Must be non-negative and at least as
    /// large as <see cref="RetryDelay"/> when both are set.
    /// <para>
    /// Ignored when <see cref="ServiceClient"/> is set.
    /// </para>
    /// </summary>
    public TimeSpan? RetryMaxDelay { get; set; }

    /// <summary>
    /// When set, overrides <see cref="RetryOptions.NetworkTimeout"/>
    /// on the constructed <see cref="TableClientOptions.Retry"/> — the
    /// per-attempt deadline applied at the transport layer. <c>null</c>
    /// leaves the SDK default (100 s) in place. Must be positive.
    /// <para>
    /// Functions as a per-attempt deadline budget: a stuck request
    /// cannot keep a WAL slot occupied longer than this value before
    /// being cancelled and either retried (if attempts remain) or
    /// surfacing a <see cref="ProviderRetryExhausted"/>-tagged failure
    /// to the caller. Ignored when <see cref="ServiceClient"/> is set.
    /// </para>
    /// </summary>
    public TimeSpan? RetryNetworkTimeout { get; set; }

    /// <summary>
    /// When set, overrides <see cref="RetryOptions.Mode"/> on the
    /// constructed <see cref="TableClientOptions.Retry"/>. Defaults to
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
    /// <see langword="false"/> - the safe, simple behaviour where
    /// every <c>AppendBatchAsync</c> awaits its own phase-2 commit
    /// before returning.
    /// <para>
    /// <b>Why pipeline.</b> With the default
    /// <c>LatticeOptions.WalMaxPendingBatches = 1</c> the WAL grain
    /// serialises append calls per shard. Each call's request-path
    /// latency is then
    /// <c>max(phase0, phase1) + phase2</c>, and the per-shard worker's
    /// coalescing window (up to 49 phase-2 commits collapsed into one
    /// transaction) is wasted because it never sees more than one
    /// pending commit at a time. Enabling this option overlaps phase
    /// 2 of batch <c>N</c> with phase 0+1 of batch <c>N+1</c>, which
    /// halves the steady-state request-path latency per shard and
    /// turns the worker's coalescing window from "never used" to
    /// "saturates under burst".
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
    public bool PipelinePhaseTwoCommits { get; set; }

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
    /// still reconciled. Default is <see langword="false"/>.
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
    public bool EliminateCandidateRowOnHotPath { get; set; }

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
    }

    /// <summary>
    /// Builds a fresh <see cref="TableServiceClient"/> from the
    /// configured authentication mode, or returns the host-supplied
    /// <see cref="ServiceClient"/> verbatim when that mode is in use.
    /// Called once per provider instance at first use; the resulting
    /// client is reused for the lifetime of the provider.
    /// </summary>
    internal TableServiceClient BuildServiceClient()
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
