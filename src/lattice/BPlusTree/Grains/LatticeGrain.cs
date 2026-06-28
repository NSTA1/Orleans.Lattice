using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;
namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateless worker that routes requests to the correct <see cref="IShardRootGrain"/>
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="StatelessWorkerAttribute.MaxLocalWorkers"/> is set to 32 so the
/// per-silo activation pool can absorb 32 concurrent in-flight calls before any
/// new caller starts queueing on an existing activation's non-reentrancy queue.
/// The Orleans default (<c>Environment.ProcessorCount</c>) is much smaller on
/// modest hosts (e.g. 4 on a 4-vCPU container) which becomes the visible
/// throughput cap on bulk-write fan-out: the U1b ladder probe (raising the
/// upstream ingest flush-concurrency from 8 to 16 on a 4-vCPU container) drove
/// every <c>SetManyAsync</c> caller onto one of just four activations and the
/// resulting 30 s grain-RPC timeouts surfaced as
/// <c>NonReentrancyQueueSize=7 NumRunning=1</c> in the Orleans timeout
/// diagnostic. 32 is enough headroom that doubling the upstream flush-
/// concurrency knob no longer collides on activation count, while still being
/// bounded so a runaway caller cannot expand the pool without limit. Each
/// activation still serialises its own non-reentrant calls; the per-activation
/// caches (<see cref="_treeIdCache"/>, <see cref="_shardMap"/>,
/// <see cref="_cachedShards"/>, <see cref="_cachedRouting"/>,
/// <see cref="_compactionEnsured"/>, <see cref="_monitorEnsured"/>) are
/// activation-scoped and remain safe under multiple parallel activations.
/// </para>
/// </remarks>
[StatelessWorker(maxLocalWorkers: 32)]
internal sealed partial class LatticeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    IServiceProvider services,
    ILogger<LatticeGrain> logger) : ILattice, ISystemLattice, IReplicationApplyGrain
{
    private string? _treeIdCache;
    private string TreeId => _treeIdCache ??= context.GrainId.Key.ToString()!;

    /// <summary>
    /// Per-activation cached <c>(tag=tree, value=TreeId)</c> KeyValuePair
    /// used as the leading tag on every <c>set.duration</c> /
    /// <c>set.stage.duration</c> / <c>set_many.duration</c> /
    /// <c>set_many.stage.duration</c> / <c>get.duration</c> /
    /// <c>get.stage.duration</c> / <c>get_many.duration</c> /
    /// <c>get_many.stage.duration</c> / <c>exists.duration</c> /
    /// <c>get_with_version.duration</c> Record call.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The pair is invariant for the activation's lifetime
    /// (<c>LatticeMetrics.TagTree</c> is a compile-time constant and
    /// <see cref="TreeId"/> is derived once from the activation's
    /// immutable grain context), so each public-API entry point can
    /// read it as a field instead of re-constructing the KVP on every
    /// call. The KVP itself is a 16-byte value type that already
    /// stack-allocates; the cache primarily removes the per-call
    /// <see cref="TreeId"/> property dispatch + struct construction
    /// from every Record callsite, which adds up to 7+ constructions
    /// per single-key Set/Get call.
    /// </para>
    /// <para>
    /// Initialised behind a <c>bool</c> flag rather than a
    /// <see cref="Nullable{T}"/> wrapper because the BCL's
    /// <see cref="Histogram{T}.Record(T, KeyValuePair{string, object?})"/>
    /// overload set takes the tag by value - wrapping it in a
    /// <see cref="Nullable{T}"/> would force a <c>HasValue</c> check +
    /// a struct copy on every read. The flag-based init is the same
    /// pattern the activation uses for other lazy-cached value-typed
    /// state (see <see cref="_treeIdCache"/> for the reference-typed
    /// equivalent). Safe to lazy-init without a lock under Orleans'
    /// non-reentrant grain scheduling: this activation only ever runs
    /// one turn at a time.
    /// </para>
    /// </remarks>
    private KeyValuePair<string, object?> _stageTagTreeCache;
    private bool _stageTagTreeCached;
    private KeyValuePair<string, object?> StageTagTree
    {
        get
        {
            if (!_stageTagTreeCached)
            {
                _stageTagTreeCache = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId);
                _stageTagTreeCached = true;
            }
            return _stageTagTreeCache;
        }
    }

    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    /// <summary>
    /// Enforces the optional <see cref="LatticeOptions.MaxKeyLength"/> /
    /// <see cref="LatticeOptions.MaxValueSizeBytes"/> write-size bounds at the
    /// public write boundary, so a client cannot drive unbounded heap growth by
    /// writing pathologically large keys or values (memory-exhaustion DoS).
    /// No-op when both bounds are unset (the default). Pass <c>null</c> for
    /// <paramref name="value"/> on a key-only check.
    /// </summary>
    private void ValidateWriteSize(string key, byte[]? value)
    {
        var options = Options;
        if (options.MaxKeyLength is { } maxKeyLength && key.Length > maxKeyLength)
        {
            throw new ArgumentException(
                $"Key length {key.Length} exceeds the configured LatticeOptions.MaxKeyLength of {maxKeyLength}.",
                nameof(key));
        }
        if (value is not null
            && options.MaxValueSizeBytes is { } maxValueSizeBytes
            && value.Length > maxValueSizeBytes)
        {
            throw new ArgumentException(
                $"Value size {value.Length} bytes exceeds the configured LatticeOptions.MaxValueSizeBytes of {maxValueSizeBytes}.",
                nameof(value));
        }
    }

    private bool _compactionEnsured;
    private bool _monitorEnsured;
    private string? _physicalTreeId;
    private ShardMap? _shardMap;
    // Per-activation array-keyed cache of resolved IShardRootGrain references
    // indexed by physical shard index. Replaces the cycle-8 single-slot LRU
    // (cycle 11): the array form removes thrashing under multi-shard fanout
    // (SetManyAsync, KeysAsync, EntriesAsync, DeleteRangeAsync) where
    // consecutive GetGrain calls alternate between distinct shards. Allocated
    // lazily on the first slow-path miss once _shardMap is set, sized to
    // cover the largest physical shard index in the active map. Both
    // invalidation hooks below null this field; the next slow-path call
    // re-allocates against the fresh map.
    private IShardRootGrain?[]? _cachedShards;
    // Per-activation cached RoutingInfo. Populated by GetRoutingSlowAsync once
    // both _physicalTreeId and _shardMap are resolved; nulled by both
    // invalidation hooks alongside the rest of the routing state. Caching the
    // record itself (rather than re-allocating on every GetRoutingAsync call)
    // is what lets GetRoutingAsync degenerate to a non-async sync-fast-path
    // method, which in turn lets GetShardGrainAsync do the same on the
    // shard-cache hit path. Combined: no async state-machine box and no
    // RoutingInfo allocation on the steady-state read/write fast path.
    private RoutingInfo? _cachedRouting;
    private readonly PublishEventsGate _eventsGate = new();

    /// <summary>
    /// Wall-clock budget for every stale-routing retry loop on the public
    /// <see cref="ILattice"/> surface (reads, writes, and replication apply).
    /// A single <see cref="StaleShardRoutingException"/> /
    /// <see cref="StaleTreeRoutingException"/> retry is insufficient because
    /// (a) under cascading mid-saga topology changes (e.g. a 4-to-8 reshard
    /// with adaptive shard splits in flight), multiple sequential ShardMap
    /// swaps can land between the initial fetch and the retry; and
    /// (b) during an adaptive shard split the source enters Reject phase one
    /// RPC before the registry's <c>ShardMap</c> is flipped (see
    /// <c>TreeShardSplitGrain.SwapAsync</c>), so a reader landing in that
    /// window observes one or more stale-routing throws against the
    /// pre-flip map before the post-flip map becomes visible. The wall-clock
    /// budget here mirrors the saga's own per-shard retry pattern in
    /// <c>AtomicWriteGrain.MarkOneShardAsync</c> and
    /// <c>AtomicWriteGrain.CaptureShardAsync</c>: bounded by wall clock
    /// rather than attempt count so any reasonable storm can drain, but
    /// still terminates with the original stale-routing throw when the
    /// topology never quiesces within the budget.
    /// </summary>
    private static readonly TimeSpan StaleRoutingWriteRetryBudget = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Rejects any public <see cref="ILattice"/> call targeting a reserved
    /// system-tree name (any id starting with
    /// <see cref="LatticeConstants.SystemTreePrefix"/>, which includes the
    /// registry tree and the replog prefix). Internal library code that
    /// legitimately addresses system trees resolves
    /// <see cref="ISystemLattice"/> instead and bypasses this guard via
    /// explicit interface implementation.
    /// </summary>
    private void ThrowIfSystemTree()
    {
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Tree ID '{TreeId}' is reserved for internal Lattice system trees and cannot be addressed via the public ILattice surface. Choose a tree name that does not start with '{LatticeConstants.SystemTreePrefix}'.");
    }

    /// <summary>
    /// Rejects a direct public <see cref="ILattice"/> <em>write</em> to a
    /// materialised-view tree (any id starting with
    /// <see cref="LatticeConstants.ViewTreePrefix"/>) that does not originate from
    /// the view maintainer. A view tree is derived state owned by its maintainer;
    /// a direct user write would corrupt the view's drift digest and trigger a
    /// spurious rebuild, so only writes carrying the maintainer's view-write
    /// capability (<see cref="ViewWriteContext"/>) are admitted. Direct user
    /// <em>reads</em> are rejected separately by
    /// <see cref="ThrowIfProtectedViewRead"/>, and the replication apply path
    /// (<see cref="IReplicationApplyGrain"/>) bypasses it via explicit interface
    /// implementation so a ShipView consumer still receives its replicated view
    /// tree.
    /// </summary>
    private void ThrowIfProtectedView()
    {
        if (TreeId.StartsWith(LatticeConstants.ViewTreePrefix, StringComparison.Ordinal)
            && !ViewWriteContext.IsAuthorised)
            throw new InvalidOperationException(
                $"Tree ID '{TreeId}' is a materialised view and is read-only through the public ILattice surface. A view's contents are maintained from its source tree; write to the source instead. Choose a tree name that does not start with '{LatticeConstants.ViewTreePrefix}' for directly-writable trees.");
    }

    /// <summary>
    /// Rejects a direct public <see cref="ILattice"/> <em>content read</em>
    /// (<c>GetAsync</c>, <c>GetWithVersionAsync</c>, <c>ExistsAsync</c>,
    /// <c>GetManyAsync</c>, <c>CountAsync</c>, <c>CountPerShardAsync</c>,
    /// <c>KeysAsync</c>, <c>EntriesAsync</c>, and their predicate variants) of a
    /// materialised-view tree (any id starting with
    /// <see cref="LatticeConstants.ViewTreePrefix"/>) that does not originate from
    /// an authorised view scope. A shadow-swap rebuild can swap the active
    /// view-tree generation underneath a fixed <c>view-{name}</c> bind, so a raw
    /// read may observe a stale or empty generation; callers must read through an
    /// <see cref="ILatticeView"/> handle (resolved via
    /// <c>ILatticeViewFactory.GetAsync</c> or <c>ILatticeViewFactory.Create</c>),
    /// which resolves the active generation and opens a
    /// <see cref="ViewReadContext"/> scope. The maintainer's own view-tree reads
    /// run under either that read scope or its <see cref="ViewWriteContext"/> write
    /// scope, so both are admitted. Structural reads (projection digests used by
    /// replication anti-entropy) and the replication apply path are not affected by
    /// this guard.
    /// </summary>
    private void ThrowIfProtectedViewRead()
    {
        if (TreeId.StartsWith(LatticeConstants.ViewTreePrefix, StringComparison.Ordinal)
            && !ViewWriteContext.IsAuthorised
            && !ViewReadContext.IsAuthorised)
            throw new InvalidOperationException(
                $"Tree ID '{TreeId}' is a materialised view and cannot be read directly through the public ILattice surface; a rebuild can swap the active view-tree generation underneath a raw bind, so a direct read may observe a stale or empty generation. Resolve an ILatticeView handle via ILatticeViewFactory.GetAsync(...) (or Create) and read through it instead.");
    }

    private IHostApplicationLifetime? _lifetime;
    private bool _lifetimeResolved;

    /// <summary>
    /// Resolves the optional <see cref="IHostApplicationLifetime"/> from the
    /// activation's service provider. Cached after first lookup. Returns
    /// <see langword="null"/> on non-hosted test activations (which do not
    /// register the lifetime), in which case <see cref="ThrowIfShuttingDown"/>
    /// is a no-op. Mirrors the lazy-resolve pattern the atomic-write saga
    /// coordinator established.
    /// </summary>
    private IHostApplicationLifetime? ResolveLifetime()
    {
        if (_lifetimeResolved) return _lifetime;
        _lifetimeResolved = true;
        _lifetime = services.GetService<IHostApplicationLifetime>();
        return _lifetime;
    }

    /// <summary>
    /// Fast-fails a public write entry point with
    /// <see cref="LatticeShuttingDownException"/> when the host has begun
    /// shutting down, before the call touches the activation directory or
    /// dispatches to the write-ahead-log writer. A no-op on a healthy host
    /// or a non-hosted test activation. Every public mutation method calls
    /// this first so a single <c>is LatticeShuttingDownException</c> check on
    /// the caller covers every write path, not just the atomic-write saga.
    /// The steady-state healthy path is a cached field read plus a struct
    /// token check - no allocation.
    /// </summary>
    private void ThrowIfShuttingDown()
    {
        if (ResolveLifetime() is { } lifetime && lifetime.ApplicationStopping.IsCancellationRequested)
            throw new LatticeShuttingDownException(
                $"Write to tree '{TreeId}' refused: the silo is shutting down (ApplicationStopping is signalled); "
                + "the write was not dispatched to the write-ahead-log writer.");
    }


    private async Task PublishEventAsync(LatticeTreeEventKind kind, string? key = null, int? shardIndex = null)
    {
        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(kind, TreeId, key, shardIndex);
        await LatticeEventPublisher.PublishAsync(services, opts, evt, logger);
    }

    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        return GetAsyncCore(key, cancellationToken);
    }

    async Task<byte[]?> ISystemLattice.GetAsync(string key, CancellationToken cancellationToken)
    {
        using var _ = LatticeSystemTreeBoundary.Enter();
        return await GetAsyncCore(key, cancellationToken);
    }

    private async Task<byte[]?> GetAsyncCore(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        // Caller-visible per-call envelope + per-attempt sub-stage attribution.
        // GetDuration tracks the end-to-end wall-clock cost of one
        // ILattice.GetAsync; GetStageDuration tagged stage=(route|shard)
        // splits the envelope into routing-resolution vs the inner shard
        // RPC so a stale-routing storm shows up as multiple route+shard
        // observations per envelope rather than one inflated envelope cell.
        //
        // The retry loop is inlined (rather than delegated to
        // RetryOnStaleRoutingAsync) so the per-stage measurements live on
        // the method's own async state machine instead of a per-call
        // closure capturing `this`, `key`, and `stageTagTree`. Mirrors the
        // hot-path inlining choice in SetAsyncCore - the cost is a
        // duplicated retry-loop shape, the benefit is one fewer
        // allocation per call on the busiest public read entry point.
        var stageTagTree = StageTagTree;
        var envelopeStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var deadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
            var invalidOpRetried = false;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    IShardRootGrain shard;
                    var routeStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
                    try
                    {
                        shard = await GetShardGrainAsync(key);
                    }
                    finally
                    {
                        LatticeMetrics.GetStageDuration.Record(
                            System.Diagnostics.Stopwatch.GetElapsedTime(routeStartTicks).TotalMilliseconds,
                            stageTagTree, LatticeMetrics.StageRouteTag);
                    }
                    var shardStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
                    try
                    {
                        return await shard.GetAsync(key);
                    }
                    finally
                    {
                        LatticeMetrics.GetStageDuration.Record(
                            System.Diagnostics.Stopwatch.GetElapsedTime(shardStartTicks).TotalMilliseconds,
                            stageTagTree, LatticeMetrics.StageShardTag);
                    }
                }
                catch (StaleShardRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    InvalidateShardMap();
                }
                catch (StaleTreeRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                }
                catch (ShardActivationTimeoutException)
                {
                    // Seed-timeout is retriable by construction; absorb
                    // into the wall-clock budget with a per-attempt
                    // backoff (mirrors RetryOnStaleRoutingAsync).
                    if (DateTime.UtcNow >= deadline) throw;
                    await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                }
                catch (Exception churnEx) when (ShardActivationRetry.IsTransientSiloChurn(churnEx))
                {
                    // Target activation's host is restarting, draining, or
                    // has just left the cluster (SiloUnavailableException, or
                    // a forward-to-deactivating rejection); the directory
                    // re-places it on retry, so absorb within the budget.
                    if (DateTime.UtcNow >= deadline) throw;
                    await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                }
                catch (InvalidOperationException)
                {
                    if (invalidOpRetried) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                    invalidOpRetried = true;
                }
            }
        }
        finally
        {
            LatticeMetrics.GetDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(envelopeStartTicks).TotalMilliseconds,
                stageTagTree);
        }
    }

    public async Task<VersionedValue> GetWithVersionAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        // Caller-visible per-call envelope. GetWithVersionDuration is the
        // single-observation envelope histogram; no per-stage decomposition
        // is published here today because the path is structurally identical
        // to GetAsync (one routing + one shard RPC) and the stage split on
        // GetAsync covers the same diagnostic surface. Operators triaging a
        // versioned-read latency tail can pivot on get.stage.duration.
        //
        // The retry loop is inlined (rather than delegated to
        // RetryOnStaleRoutingAsync) so the per-call success path takes no
        // closure / delegate allocation. Mirrors the hot-path inlining
        // choice in GetAsyncCore - the cost is a duplicated retry-loop
        // shape, the benefit is one closure object + one Func<Task<T>>
        // delegate + one inner async state machine box eliminated per call
        // on a busy public read entry point. The retry semantics are
        // bit-identical to RetryOnStaleRoutingAsync (same deadline, same
        // four catch arms in the same order).
        var stageTagTree = StageTagTree;
        var envelopeStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var deadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
            var invalidOpRetried = false;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var shard = await GetShardGrainAsync(key);
                    return await shard.GetWithVersionAsync(key);
                }
                catch (StaleShardRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    InvalidateShardMap();
                }
                catch (StaleTreeRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                }
                catch (ShardActivationTimeoutException)
                {
                    // Seed-timeout is retriable by construction; absorb
                    // into the wall-clock budget with a per-attempt
                    // backoff (mirrors RetryOnStaleRoutingAsync).
                    if (DateTime.UtcNow >= deadline) throw;
                    await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                }
                catch (Exception churnEx) when (ShardActivationRetry.IsTransientSiloChurn(churnEx))
                {
                    // Target activation's host is restarting, draining, or
                    // has just left the cluster (SiloUnavailableException, or
                    // a forward-to-deactivating rejection); the directory
                    // re-places it on retry, so absorb within the budget.
                    if (DateTime.UtcNow >= deadline) throw;
                    await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                }
                catch (InvalidOperationException)
                {
                    if (invalidOpRetried) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                    invalidOpRetried = true;
                }
            }
        }
        finally
        {
            LatticeMetrics.GetWithVersionDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(envelopeStartTicks).TotalMilliseconds,
                stageTagTree);
        }
    }

    public Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        return ExistsAsyncCore(key, cancellationToken);
    }

    async Task<bool> ISystemLattice.ExistsAsync(string key, CancellationToken cancellationToken)
    {
        using var _ = LatticeSystemTreeBoundary.Enter();
        return await ExistsAsyncCore(key, cancellationToken);
    }

    private async Task<bool> ExistsAsyncCore(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        // Caller-visible per-call envelope. ExistsDuration is the
        // single-observation envelope histogram; no per-stage decomposition
        // is published here today (structurally identical to GetAsync at
        // the LatticeGrain layer; operators triaging an existence-probe
        // latency tail can pivot on get.stage.duration).
        //
        // The retry loop is inlined (rather than delegated to
        // RetryOnStaleRoutingAsync) so the per-call success path takes no
        // closure / delegate allocation. Mirrors the hot-path inlining
        // choice in GetAsyncCore - the cost is a duplicated retry-loop
        // shape, the benefit is one closure object + one Func<Task<T>>
        // delegate + one inner async state machine box eliminated per call
        // on a busy public read entry point. The retry semantics are
        // bit-identical to RetryOnStaleRoutingAsync (same deadline, same
        // four catch arms in the same order).
        var stageTagTree = StageTagTree;
        var envelopeStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var deadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
            var invalidOpRetried = false;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var shard = await GetShardGrainAsync(key);
                    return await shard.ExistsAsync(key);
                }
                catch (StaleShardRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    InvalidateShardMap();
                }
                catch (StaleTreeRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                }
                catch (ShardActivationTimeoutException)
                {
                    // Seed-timeout is retriable by construction; absorb
                    // into the wall-clock budget with a per-attempt
                    // backoff (mirrors RetryOnStaleRoutingAsync).
                    if (DateTime.UtcNow >= deadline) throw;
                    await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                }
                catch (Exception churnEx) when (ShardActivationRetry.IsTransientSiloChurn(churnEx))
                {
                    // Target activation's host is restarting, draining, or
                    // has just left the cluster (SiloUnavailableException, or
                    // a forward-to-deactivating rejection); the directory
                    // re-places it on retry, so absorb within the budget.
                    if (DateTime.UtcNow >= deadline) throw;
                    await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                }
                catch (InvalidOperationException)
                {
                    if (invalidOpRetried) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                    invalidOpRetried = true;
                }
            }
        }
        finally
        {
            LatticeMetrics.ExistsDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(envelopeStartTicks).TotalMilliseconds,
                stageTagTree);
        }
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        ArgumentNullException.ThrowIfNull(keys);
        cancellationToken.ThrowIfCancellationRequested();

        // Caller-visible per-call envelope. GetManyDuration tracks the
        // wall-clock cost of one ILattice.GetManyAsync (including the
        // outer stale-routing retry loop below and the inner
        // snapshot-retry loop in GetManyAsyncCore). GetManyStageDuration
        // tagged stage=(route|bucket|fanout|merge) is recorded per inner
        // attempt inside GetManyAsyncCore so the dominant sub-cost is
        // attributable without a profile. Mirrors the per-attempt
        // accumulation pattern on SetManyAsync.
        var stageTagTree = StageTagTree;
        var envelopeStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            // Deadline-bounded stale-routing retry loop, symmetric with
            // SetAsyncCore. Under a cascading mid-saga reshard the previous
            // single-shot catch-and-retry shape exhausted on the second
            // stale-routing throw and the exception propagated out of a
            // continuous read - which a strict-atomic-visibility caller
            // counts as a violation. The wall-clock budget mirrors
            // SetAsyncCore's so reads and writes absorb the same topology
            // storm in lockstep. Asymmetric InvalidOperationException
            // handling preserves tree-deletion semantics (single retry
            // only, so a deleted tree surfaces in <2s rather than after
            // 60s).
            var deadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
            var invalidOpRetried = false;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    return await GetManyAsyncCore(keys, stageTagTree);
                }
                catch (StaleShardRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    InvalidateShardMap();
                }
                catch (StaleTreeRoutingException)
                {
                    if (DateTime.UtcNow >= deadline) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                }
                catch (InvalidOperationException)
                {
                    if (invalidOpRetried) throw;
                    if (!TryInvalidateStaleAlias()) throw;
                    invalidOpRetried = true;
                }
            }
        }
        finally
        {
            LatticeMetrics.GetManyDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(envelopeStartTicks).TotalMilliseconds,
                stageTagTree);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Dictionary<string, byte[]>> GetManyAsyncCore(List<string> keys, KeyValuePair<string, object?> stageTagTree)
    {
        string physicalTreeId;
        ShardMap shardMap;
        var initialRouteStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            (physicalTreeId, shardMap) = await GetRoutingAsync();
        }
        finally
        {
            LatticeMetrics.GetManyStageDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(initialRouteStartTicks).TotalMilliseconds,
                stageTagTree, LatticeMetrics.StageRouteTag);
        }
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Double-checked snapshot retry: pre-fetch a TxRegistry snapshot
        // (snap1), fan out under that ambient view, then re-fetch
        // (snap2) and confirm no saga transitioned InFlight->Committed
        // during the fan-out. The single-shot snapshot pattern is
        // insufficient because per-leaf drain into Entries on
        // ApplyTxCommit is irreversible: a reader whose snap1 was
        // taken before MarkCommittedAsync but whose fan-out reaches
        // some leaves after their drain will see drained leaves return
        // post-saga Entries (no pending entry to gate visibility) while
        // sibling undrained leaves consult snap1.InFlight and fall
        // through to pre-saga Entries - split-observation that defeats
        // strict per-tree atomic visibility. Retrying with snap2 (now
        // reflecting the committed transition) makes the next fan-out
        // observe the saga as Committed everywhere, so undrained
        // leaves surface pending.value (post) and drained leaves'
        // Entries=post are consistent.
        //
        // A topology change is the second source of split observation:
        // grouping keys by a shard map that is then swapped mid-fan-out
        // routes some keys to a source shard that has already
        // shadow-forwarded its slots to the new owner (post) while
        // sibling keys still resolve to undrained owners (pre). Like the
        // CountAsync path, we therefore capture the map version at the
        // start of each attempt, re-group under the current map, and
        // discard-and-retry if the version moves while the fan-out is in
        // flight, re-resolving routing under the fresh map.
        //
        // This map-version guard is the reader-side half of the
        // mid-reshard atomic-visibility defense. It catches the case
        // where the shard map is swapped while this fan-out is in flight,
        // forcing a re-fan under the fresh map. The authoritative half
        // lives in the split coordinator's Swap phase
        // (TreeShardSplitGrain.SwapAsync), which performs a final
        // moved-slot drain after the source freezes its writes and BEFORE
        // the registry map flips, so the destination provably holds every
        // committed value for a migrating slot by the time any reader can
        // route to it. Together they close the window in which a reader
        // could observe a migrating slot's drained historical value
        // (IsMigrated=true, no shadow marker) for some keys while other
        // keys show the post-saga value within a single observed map
        // version.
        var maxRetries = Math.Max(1, Options.MaxScanRetries);
        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            if (attempt > 0)
            {
                InvalidateShardMap();
                var retryRouteStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
                try
                {
                    (physicalTreeId, shardMap) = await GetRoutingAsync();
                }
                finally
                {
                    LatticeMetrics.GetManyStageDuration.Record(
                        System.Diagnostics.Stopwatch.GetElapsedTime(retryRouteStartTicks).TotalMilliseconds,
                        stageTagTree, LatticeMetrics.StageRouteTag);
                }
            }

            var versionAtStart = shardMap.Version;

            // Group keys by shard under the map snapshot captured for
            // this attempt.
            //
            // Presize discipline (see also the ConcurrentDictionary
            // presize below): the outer shardBuckets dictionary holds
            // at most one entry per distinct physical shard that owns
            // at least one of the requested keys - bounded above by
            // min(keys.Count, distinct-physical-shard-count). The
            // ShardMap caches its physical-shard set on first call so
            // the .Count lookup is O(1) after the first resolve. Each
            // per-shard bucket list holds at most keys.Count entries
            // (worst case: all keys hash to the same shard); presizing
            // to keys.Count over-allocates by N x 8 B per list in the
            // perfectly-distributed case but eliminates the geometric
            // 0->4->8->...->keys.Count grow chain that an un-presized
            // List<string>() would walk on the dominant single-shard
            // hot path. This is the locus called out as carry-forward
            // item #3 in POSTMORTEM-2026-06-09-retry-on-stale-routing-tstate.
            var bucketStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            Dictionary<int, List<string>> shardBuckets;
            try
            {
                var physicalShardCount = shardMap.GetPhysicalShardIndices().Count;
                shardBuckets = new Dictionary<int, List<string>>(
                    capacity: Math.Min(keys.Count, physicalShardCount));
                foreach (var key in keys)
                {
                    var idx = shardMap.Resolve(key);
                    if (!shardBuckets.TryGetValue(idx, out var bucket))
                    {
                        bucket = new List<string>(capacity: keys.Count);
                        shardBuckets[idx] = bucket;
                    }
                    bucket.Add(key);
                }
            }
            finally
            {
                LatticeMetrics.GetManyStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(bucketStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StageBucketTag);
            }

#if LATTICE_DIAG
            DiagSink.Write($"[DIAG reader-batch-enter] tree={physicalTreeId} attempt={attempt} keyCount={keys.Count} bucketCount={shardBuckets.Count} buckets=[{string.Join(';', shardBuckets.Select(kv => $"s{kv.Key}:[{string.Join(',', kv.Value)}]"))}]");
#endif

            // Fan-out stage: registry-snapshot probe + per-shard parallel
            // fetch. The snapshot probe is part of the fan-out (it sets up
            // the ambient visibility view every per-shard task reads under)
            // so attributing it to a separate "snapshot" stage would
            // fragment the fan-out's wall-clock without buying
            // diagnostic value at the per-call granularity.
            ConcurrentDictionary<string, byte[]> concurrent;
            RegistrySnapshotPair snap1Pair;
            var fanoutStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                snap1Pair = await FetchRegistrySnapshotAsync();
                var snap1 = snap1Pair.Snap;
                // Presize the per-call merge target: the per-shard
                // FetchFromShardAsync workers TryAdd one entry per
                // returned key, so the steady-state final size is
                // bounded by keys.Count (less when some keys are
                // absent on the leaf). concurrencyLevel is the
                // distinct-shard count - one writer per shard task -
                // which matches the actual parallel-TryAdd fan-in
                // without over-segmenting the bucket array. Both
                // arguments must be >= 1 (ConcurrentDictionary's
                // constructor rejects 0); the keys.Count == 0 entry
                // point (legal per the public ILattice.GetManyAsync
                // surface, exercised by GetManyAsync_returns_empty_for_no_keys)
                // makes both shardBuckets.Count and keys.Count zero,
                // so a Math.Max(1, ...) floor on each preserves the
                // presize semantics on the common path while keeping
                // the empty-batch path well-defined.
                concurrent = new ConcurrentDictionary<string, byte[]>(
                    concurrencyLevel: Math.Max(1, shardBuckets.Count),
                    capacity: Math.Max(1, keys.Count));
                using (LatticeRegistrySnapshotContext.BeginScope(snap1))
                {
                    var tasks = new List<Task>(shardBuckets.Count);
                    foreach (var (shardIdx, bucket) in shardBuckets)
                    {
                        var shard = GetShardGrainByIndex(physicalTreeId, shardIdx);
                        tasks.Add(FetchFromShardAsync(shard, shardIdx, bucket, concurrent, attempt));
                    }
                    await Task.WhenAll(tasks);
                }
            }
            finally
            {
                LatticeMetrics.GetManyStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(fanoutStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StageFanOutTag);
            }

            // Merge stage: topology-stability check + snap2 stability
            // check + final ConcurrentDictionary -> Dictionary
            // materialise on the happy path. Recorded once per attempt
            // even when a topology-version drift or snap2 mismatch
            // forces a continue, so operators see the wasted-attempt
            // cost.
            var mergeStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            bool returning = false;
            Dictionary<string, byte[]>? result = null;
            try
            {
                // Unconditional topology-stability check: if the shard-map
                // version moved while the fan-out was in flight, the per-shard
                // reads may have spanned an inconsistent snapshot (some keys
                // routed to a shadow-forwarded source owner, others to the new
                // owner). Discard and retry against the fresh map.
                var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap;
                if (shardMapNow.Version != versionAtStart)
                {
#if LATTICE_DIAG
                    DiagSink.Write($"[DIAG reader-topology-retry] tree={physicalTreeId} attempt={attempt} versionAtStart={versionAtStart} versionNow={shardMapNow.Version}");
#endif
                    continue;
                }

                if (await IsSnap2StableAsync(snap1Pair.Snap, snap1Pair.Revision))
                {
#if LATTICE_DIAG
                    DiagSink.Write($"[DIAG reader-batch-exit] tree={physicalTreeId} attempt={attempt} keyCount={keys.Count} returnedCount={concurrent.Count} rounds=[{string.Join(',', concurrent.Select(kv => $"{kv.Key}=r{DiagSink.DecodeRound(kv.Value)}"))}]");
#endif
                    result = new Dictionary<string, byte[]>(concurrent);
                    returning = true;
                }
                // else: a saga's InFlight->Committed transition raced our
                // fan-out; retry with the fresh snapshot in scope.
#if LATTICE_DIAG
                if (!returning) DiagSink.Write($"[DIAG reader-snapshot-retry] tree={physicalTreeId} attempt={attempt} returnedSoFar={concurrent.Count}");
#endif
            }
            finally
            {
                LatticeMetrics.GetManyStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(mergeStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StageMergeTag);
            }
            if (returning) return result!;
        }

        throw new InvalidOperationException(
            $"GetManyAsync exceeded {Options.MaxScanRetries} retries while the TxRegistry " +
            "kept committing sagas faster than the fan-out could complete. Increase " +
            "LatticeOptions.MaxScanRetries or reduce concurrent saga rate.");

        static async Task FetchFromShardAsync(
            IShardRootGrain shard,
            int shardIdx,
            List<string> keys,
            ConcurrentDictionary<string, byte[]> result,
            int attempt)
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG reader-shard-fanout-enter] shard={shardIdx} attempt={attempt} keys=[{string.Join(',', keys)}]");
#endif
            // Per-shard ShardActivationRetry wrap: a single shard's cold-start
            // seed-timeout retries only that shard, not the whole fan-out.
            // Healthy sibling tasks continue uninterrupted.
            var values = await ShardActivationRetry.RunAsync(
                () => shard.GetManyAsync(keys));
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG reader-shard-fanout-exit] shard={shardIdx} attempt={attempt} returnedCount={values.Count} rounds=[{string.Join(',', values.Select(kv => $"{kv.Key}=r{DiagSink.DecodeRound(kv.Value)}"))}]");
#endif
            foreach (var (key, value) in values)
            {
                result[key] = value;
            }
        }
    }

    public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(key);
        ValidateWriteSize(key, value);
        return LatticeIdempotencyContext.IsActive
            ? RunMutationAsync(ct => SetAsyncCore(key, value, ct), cancellationToken)
            : SetAsyncCore(key, value, cancellationToken);
    }

    async Task ISystemLattice.SetAsync(string key, byte[] value, CancellationToken cancellationToken)
    {
        using var _ = LatticeSystemTreeBoundary.Enter();
        await SetAsyncCore(key, value, cancellationToken);
    }

    private async Task SetAsyncCore(string key, byte[] value, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();

        // c2-xxvii envelope + sub-stage attribution. SetDuration tracks the caller-visible
        // wall-clock; SetStageDuration tagged with stage=
        // (gate|route|shard|publish) splits the envelope into its
        // four sub-spans so the LatticeGrain-side overhead of one
        // single-key write is attributed alongside the existing
        // leaf-side / WAL-side instruments. Mirrors c2-xxiv on the
        // set-many path.
        var stageTagTree = StageTagTree;
        var setStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var gateStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await EnsureCompactionReminderAsync();
                await EnsureMonitorAsync();
            }
            finally
            {
                LatticeMetrics.SetStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(gateStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StageGateTag);
            }
            cancellationToken.ThrowIfCancellationRequested();

            // Deadline-bounded stale-routing retry loop. Under a cascading
            // mid-saga reshard or online resize, a single ShardMap swap is
            // not the worst case - chains of adaptive shard splits during
            // a 4-to-8 reshard can produce multiple sequential stale-routing
            // throws on the same per-key write. The previous one-shot
            // catch-and-retry shape exhausted on the second throw and the
            // exception propagated into the saga, which then pivoted into
            // compensation on a write that would have succeeded with one
            // more refresh. The wall-clock budget here mirrors the saga's
            // own per-shard retry pattern (AtomicWriteGrain.MarkOneShardAsync
            // and CaptureShardAsync) and absorbs any reasonable topology
            // storm; if the topology genuinely never quiesces within the
            // budget, the original stale-routing throw still surfaces to
            // the caller.
            //
            // Important asymmetry: only the two topology-change exceptions
            // (StaleShardRoutingException, StaleTreeRoutingException)
            // benefit from the deadline-budget retry. InvalidOperationException
            // is typically permanent (e.g. tree deleted, alias removed) and
            // looping on it would mask deletion semantics and trip Orleans'
            // default response timeout on the caller side. Preserve the
            // pre-existing single-retry shape for that catch so a deleted
            // tree surfaces on the second throw, not after 60 seconds.
            var deadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
            var invalidOpRetried = false;
#if LATTICE_DIAG
            var swSetCore = System.Diagnostics.Stopwatch.StartNew();
            var setCoreAttempts = 0;
            DiagSink.Write($"[DIAG setcore-enter] tree={TreeId} key={key} round=r{DiagSink.DecodeRound(value)} deadlineMs={StaleRoutingWriteRetryBudget.TotalMilliseconds:F0}");
#endif
            // The shard stage covers route + shard.SetAsync + stale-
            // routing retries together because those three sub-spans
            // interleave inside the retry loop and a fresh route is
            // part of every retry iteration; splitting them further
            // would over-attribute the route stage on the no-retry
            // happy path which is the only path that matters for
            // steady-state attribution.
            var shardStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var shard = await GetShardGrainAsync(key);
#if LATTICE_DIAG
                    setCoreAttempts++;
#endif
                    try
                    {
                        await shard.SetAsync(key, value);
                        break;
                    }
                    catch (StaleShardRoutingException)
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG setcore-stale-shard] tree={TreeId} key={key} attempt={setCoreAttempts} elapsedMs={swSetCore.Elapsed.TotalMilliseconds:F0} deadlineRemainMs={(deadline - DateTime.UtcNow).TotalMilliseconds:F0}");
#endif
                        if (DateTime.UtcNow >= deadline) throw;
                        InvalidateShardMap();
                    }
                    catch (StaleTreeRoutingException)
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG setcore-stale-tree] tree={TreeId} key={key} attempt={setCoreAttempts} elapsedMs={swSetCore.Elapsed.TotalMilliseconds:F0} deadlineRemainMs={(deadline - DateTime.UtcNow).TotalMilliseconds:F0}");
#endif
                        if (DateTime.UtcNow >= deadline) throw;
                        if (!TryInvalidateStaleAlias()) throw;
                    }
                    catch (ShardActivationTimeoutException)
                    {
                        // Seed-timeout is retriable by construction; absorb
                        // into the existing wall-clock budget with a per-
                        // attempt backoff (mirrors RetryOnStaleRoutingAsync).
                        if (DateTime.UtcNow >= deadline) throw;
                        await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                    }
                    catch (Exception churnEx) when (ShardActivationRetry.IsTransientSiloChurn(churnEx))
                    {
                        // Target activation's host is restarting, draining,
                        // or has just left the cluster (SiloUnavailableException,
                        // or a forward-to-deactivating rejection); the
                        // directory re-places it on retry, so absorb within
                        // the existing wall-clock budget with a backoff.
                        if (DateTime.UtcNow >= deadline) throw;
                        await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
                    }
                    catch (InvalidOperationException)
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG setcore-invalid-op] tree={TreeId} key={key} attempt={setCoreAttempts} retried={invalidOpRetried}");
#endif
                        if (invalidOpRetried) throw;
                        if (!TryInvalidateStaleAlias()) throw;
                        invalidOpRetried = true;
                    }
                }
            }
            finally
            {
                LatticeMetrics.SetStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(shardStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StageShardTag);
            }
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG setcore-exit] tree={TreeId} key={key} attempts={setCoreAttempts} elapsedMs={swSetCore.Elapsed.TotalMilliseconds:F0}");
#endif

            var publishStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await PublishEventAsync(LatticeTreeEventKind.Set, key);
            }
            finally
            {
                LatticeMetrics.SetStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(publishStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StagePublishTag);
            }
        }
        finally
        {
            LatticeMetrics.SetDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(setStartTicks).TotalMilliseconds,
                stageTagTree);
        }
    }

    /// <inheritdoc />
    public async Task SetAsync(string key, byte[] value, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ValidateWriteSize(key, value);
        if (ttl <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be positive.");
        var nowUtc = DateTimeOffset.UtcNow;
        if (ttl > DateTimeOffset.MaxValue - nowUtc)
            throw new ArgumentOutOfRangeException(nameof(ttl),
                "TTL is too large - absolute expiry would exceed DateTimeOffset.MaxValue.");
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Resolve the absolute expiry on the silo handling this call so
        // per-entry lifetimes are not shifted by client-clock skew.
        var expiresAtTicks = nowUtc.Add(ttl).UtcTicks;

        // Cold path: no idempotency scope, no closure, no helper state-machine.
        if (!LatticeIdempotencyContext.IsActive)
        {
            await SetAsyncTtlCore(key, value, expiresAtTicks, cancellationToken);
        }
        else
        {
            await RunMutationAsync(ct => SetAsyncTtlCore(key, value, expiresAtTicks, ct), cancellationToken);
        }
        await PublishEventAsync(LatticeTreeEventKind.Set, key);
    }

    private Task SetAsyncTtlCore(string key, byte[] value, long expiresAtTicks, CancellationToken cancellationToken)
    {
        return RetryOnStaleRoutingAsync(
            (self: this, key, value, expiresAtTicks),
            static async args =>
            {
                var shard = await args.self.GetShardGrainAsync(args.key);
                await shard.SetAsync(args.key, args.value, args.expiresAtTicks);
            },
            cancellationToken);
    }

    public async Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ValidateWriteSize(key, value);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var applied = LatticeIdempotencyContext.IsActive
            ? await RunMutationAsync(ct => SetIfVersionAsyncCore(key, value, expectedVersion, ct), cancellationToken)
            : await SetIfVersionAsyncCore(key, value, expectedVersion, cancellationToken);
        if (applied) await PublishEventAsync(LatticeTreeEventKind.Set, key);
        return applied;
    }

    private Task<bool> SetIfVersionAsyncCore(string key, byte[] value, HybridLogicalClock expectedVersion, CancellationToken cancellationToken)
    {
        return RetryOnStaleRoutingAsync(
            (self: this, key, value, expectedVersion),
            static async args =>
            {
                var shard = await args.self.GetShardGrainAsync(args.key);
                return await shard.SetIfVersionAsync(args.key, args.value, args.expectedVersion);
            },
            cancellationToken);
    }

    public async Task<HybridLogicalClock> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(deltaBytes);
        ValidateWriteSize(key, deltaBytes);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var version = LatticeIdempotencyContext.IsActive
            ? await RunMutationAsync(ct => ApplyCrdtDeltaAsyncCore(key, mode, deltaBytes, ct), cancellationToken)
            : await ApplyCrdtDeltaAsyncCore(key, mode, deltaBytes, cancellationToken);
        await PublishEventAsync(LatticeTreeEventKind.Set, key);
        return version;
    }

    private Task<HybridLogicalClock> ApplyCrdtDeltaAsyncCore(string key, LatticeMergeMode mode, byte[] deltaBytes, CancellationToken cancellationToken)
    {
        return RetryOnStaleRoutingAsync(
            (self: this, key, mode, deltaBytes),
            static async args =>
            {
                var shard = await args.self.GetShardGrainAsync(args.key);
                return await shard.ApplyCrdtDeltaAsync(args.key, args.mode, args.deltaBytes);
            },
            cancellationToken);
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ValidateWriteSize(key, value);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();
        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var existing = LatticeIdempotencyContext.IsActive
            ? await RunMutationAsync(ct => GetOrSetAsyncCore(key, value, ct), cancellationToken)
            : await GetOrSetAsyncCore(key, value, cancellationToken);
        // Publish only when a new value was actually written (existing was null).
        if (existing is null) await PublishEventAsync(LatticeTreeEventKind.Set, key);
        return existing;
    }

    private Task<byte[]?> GetOrSetAsyncCore(string key, byte[] value, CancellationToken cancellationToken)
    {
        return RetryOnStaleRoutingAsync(
            (self: this, key, value),
            static async args =>
            {
                var shard = await args.self.GetShardGrainAsync(args.key);
                return await shard.GetOrSetAsync(args.key, args.value);
            },
            cancellationToken);
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(entries);
        {
            var sizeOptions = Options;
            if (sizeOptions.MaxKeyLength is not null || sizeOptions.MaxValueSizeBytes is not null)
            {
                foreach (var entry in entries)
                {
                    if (entry.Key is not null)
                        ValidateWriteSize(entry.Key, entry.Value);
                }
            }
        }
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();

        // c2-xxiv envelope + sub-stage attribution: SetManyDuration tracks the caller-visible
        // wall-clock; SetManyStageDuration tagged with stage=
        // (gate|route|bucket|fanout|events) splits the envelope into
        // its constituent spans so the dominant cost on the set-many
        // path can be identified before any structural attempt. The
        // existing shard-side instruments (ShardRootSetManyLocalApply /
        // ShardRootSetManyShadowForward / ShardRootSetManyLeafRpc)
        // continue to attribute the per-shard half; this fills the
        // missing LatticeGrain-side half of the envelope.
        var stageTagTree = StageTagTree;
        var setManyStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var gateStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                await EnsureCompactionReminderAsync();
                await EnsureMonitorAsync();
            }
            finally
            {
                LatticeMetrics.SetManyStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(gateStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StageGateTag);
            }
            cancellationToken.ThrowIfCancellationRequested();
            await RetryOnStaleRoutingAsync(
                (self: this, entries, stageTagTree),
                static args => args.self.SetManyAsyncCore(args.entries, args.stageTagTree),
                cancellationToken);

            // Publish one Set event per entry. Emitted only after all shard writes
            // have committed so subscribers never observe a Set for a key that
            // failed to persist (we'd have thrown above). Skipped entirely when
            // publishing is disabled to avoid walking the entry list.
            var eventsStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            try
            {
                if (entries.Count > 0 && await _eventsGate.IsEnabledAsync(grainFactory, TreeId, Options))
                {
                    foreach (var entry in entries)
                    {
                        await PublishEventAsync(LatticeTreeEventKind.Set, entry.Key);
                    }
                }
            }
            finally
            {
                LatticeMetrics.SetManyStageDuration.Record(
                    System.Diagnostics.Stopwatch.GetElapsedTime(eventsStartTicks).TotalMilliseconds,
                    stageTagTree, LatticeMetrics.StageEventsTag);
            }
        }
        finally
        {
            LatticeMetrics.SetManyDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(setManyStartTicks).TotalMilliseconds,
                stageTagTree);
        }
    }

    private async Task SetManyAsyncCore(List<KeyValuePair<string, byte[]>> entries, KeyValuePair<string, object?> stageTagTree)
    {
        var routeStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        string physicalTreeId;
        ShardMap shardMap;
        try
        {
            (physicalTreeId, shardMap) = await GetRoutingAsync();
        }
        finally
        {
            LatticeMetrics.SetManyStageDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(routeStartTicks).TotalMilliseconds,
                stageTagTree, LatticeMetrics.StageRouteTag);
        }

        // Group entries by shard. Pre-size each bucket to the expected
        // shard-fair fraction of the batch, capped at 256 to bound
        // over-allocation for tiny shards / huge batches (see PR #210 for
        // the canonical bounded pre-size shape). Eliminates the 0→N
        // AddWithResize cascade that previously dominated bulk-write
        // allocations.

        var bucketStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        Dictionary<int, List<KeyValuePair<string, byte[]>>> shardBuckets;
        try
        {
            var physicalShardCount = Math.Max(1, shardMap.GetPhysicalShardIndices().Count);
            var expectedPerShard = Math.Max(4, entries.Count / physicalShardCount);
            var bucketCapacity = Math.Min(expectedPerShard, 256);
            shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>(physicalShardCount);
            foreach (var entry in entries)
            {
                var idx = shardMap.Resolve(entry.Key);
                if (!shardBuckets.TryGetValue(idx, out var bucket))
                {
                    bucket = new List<KeyValuePair<string, byte[]>>(bucketCapacity);
                    shardBuckets[idx] = bucket;
                }
                bucket.Add(entry);
            }
        }
        finally
        {
            LatticeMetrics.SetManyStageDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(bucketStartTicks).TotalMilliseconds,
                stageTagTree, LatticeMetrics.StageBucketTag);
        }

        // Fan out writes in parallel per shard.
        var fanoutStartTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        try
        {
            var tasks = new List<Task>(shardBuckets.Count);
            foreach (var (shardIdx, bucket) in shardBuckets)
            {
                var shard = GetShardGrainByIndex(physicalTreeId, shardIdx);
                tasks.Add(WriteToShardAsync(shard, bucket));
            }

            await Task.WhenAll(tasks);
        }
        finally
        {
            LatticeMetrics.SetManyStageDuration.Record(
                System.Diagnostics.Stopwatch.GetElapsedTime(fanoutStartTicks).TotalMilliseconds,
                stageTagTree, LatticeMetrics.StageFanOutTag);
        }

        static async Task WriteToShardAsync(
            IShardRootGrain shard,
            List<KeyValuePair<string, byte[]>> entries)
        {
            // Per-shard ShardActivationRetry wrap: a single shard's cold-start
            // seed-timeout retries only that shard, not the whole fan-out.
            await ShardActivationRetry.RunAsync(
                () => shard.SetManyAsync(entries));
        }
    }

    public async Task<IReadOnlyList<string>> SetManyWherePredicateAsync(
        List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();

        await EnsureCompactionReminderAsync();
        await EnsureMonitorAsync();
        cancellationToken.ThrowIfCancellationRequested();

        if (entries.Count == 0) return Array.Empty<string>();

        var written = await RetryOnStaleRoutingAsync(
            (self: this, entries, predicate),
            static args => args.self.SetManyWhereAsyncCore(args.entries, args.predicate),
            cancellationToken);

        // Publish one Set event per actually-written key, after all shard
        // writes have committed, mirroring SetManyAsync's post-commit
        // publication but scoped to the guarded-in subset.
        if (written.Count > 0 && await _eventsGate.IsEnabledAsync(grainFactory, TreeId, Options))
        {
            for (int i = 0; i < written.Count; i++)
            {
                await PublishEventAsync(LatticeTreeEventKind.Set, written[i]);
            }
        }

        return written;
    }

    private async Task<IReadOnlyList<string>> SetManyWhereAsyncCore(
        List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();

        // Group entries by shard, pre-sizing each bucket to the shard-fair
        // fraction of the batch (capped at 256) - same bounded pre-size shape
        // as SetManyAsyncCore.
        var physicalShardCount = Math.Max(1, shardMap.GetPhysicalShardIndices().Count);
        var expectedPerShard = Math.Max(4, entries.Count / physicalShardCount);
        var bucketCapacity = Math.Min(expectedPerShard, 256);
        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>(physicalShardCount);
        foreach (var entry in entries)
        {
            var idx = shardMap.Resolve(entry.Key);
            if (!shardBuckets.TryGetValue(idx, out var bucket))
            {
                bucket = new List<KeyValuePair<string, byte[]>>(bucketCapacity);
                shardBuckets[idx] = bucket;
            }
            bucket.Add(entry);
        }

        // Fan out the conditional writes per shard and collect each shard's
        // written-key set.
        var tasks = new Task<IReadOnlyList<string>>[shardBuckets.Count];
        var t = 0;
        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            var shard = GetShardGrainByIndex(physicalTreeId, shardIdx);
            tasks[t++] = WriteToShardAsync(shard, bucket, predicate);
        }
        var perShard = await Task.WhenAll(tasks);

        var total = 0;
        for (int i = 0; i < perShard.Length; i++)
            total += perShard[i].Count;
        if (total == 0)
            return Array.Empty<string>();

        var aggregated = new List<string>(total);
        for (int i = 0; i < perShard.Length; i++)
        {
            var keys = perShard[i];
            for (int k = 0; k < keys.Count; k++)
                aggregated.Add(keys[k]);
        }
        return aggregated;

        static async Task<IReadOnlyList<string>> WriteToShardAsync(
            IShardRootGrain shard,
            List<KeyValuePair<string, byte[]>> entries,
            LatticePredicateNode predicate)
        {
            return await ShardActivationRetry.RunAsync(
                () => shard.SetManyWherePredicateAsync(entries, predicate));
        }
    }

    /// <summary>
    /// Atomic multi-key write. Activates a dedicated
    /// <see cref="IAtomicWriteGrain"/> keyed by <c>{treeId}/{operationId}</c>
    /// and awaits saga completion. Duplicate-key and null-value validation is
    /// done inside the saga grain; no routing is needed here because the saga
    /// itself calls back through <see cref="ILattice"/> for each write.
    /// </summary>
    public async Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0) return;

        var operationId = Guid.NewGuid().ToString("N");
        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
#if LATTICE_DIAG
        var swSetMany = System.Diagnostics.Stopwatch.StartNew();
        DiagSink.Write($"[DIAG setmanyatomic-enter] tree={TreeId} op={operationId} entriesCount={entries.Count}");
        try
        {
            await ShardActivationRetry.RunAsync(
                () => saga.ExecuteAsync(TreeId, entries),
                cancellationToken);
            DiagSink.Write($"[DIAG setmanyatomic-exit] tree={TreeId} op={operationId} entriesCount={entries.Count} elapsedMs={swSetMany.Elapsed.TotalMilliseconds:F0}");
        }
        catch (Exception ex)
        {
            DiagSink.Write($"[DIAG setmanyatomic-throw] tree={TreeId} op={operationId} entriesCount={entries.Count} elapsedMs={swSetMany.Elapsed.TotalMilliseconds:F0} ex={ex.GetType().Name} msg={ex.Message.Replace(System.Environment.NewLine, " | ")}");
            throw;
        }
#else
        await ShardActivationRetry.RunAsync(
            () => saga.ExecuteAsync(TreeId, entries),
            cancellationToken);
#endif
    }

    /// <summary>
    /// Caller-supplied idempotency-key overload. Validates the
    /// <paramref name="operationId"/> shape, then dispatches to the saga
    /// grain keyed by <c>{TreeId}/{operationId}</c>. Resubmissions with
    /// the same id re-attach to the original saga and inherit its
    /// completion outcome.
    /// </summary>
    public async Task SetManyAtomicAsync(
        List<KeyValuePair<string, byte[]>> entries,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(entries);
        ValidateOperationId(operationId);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0) return;

        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
#if LATTICE_DIAG
        var swSetMany = System.Diagnostics.Stopwatch.StartNew();
        DiagSink.Write($"[DIAG setmanyatomic-enter] tree={TreeId} op={operationId} entriesCount={entries.Count} idempotent=true");
        try
        {
            await ShardActivationRetry.RunAsync(
                () => saga.ExecuteAsync(TreeId, entries),
                cancellationToken);
            DiagSink.Write($"[DIAG setmanyatomic-exit] tree={TreeId} op={operationId} entriesCount={entries.Count} elapsedMs={swSetMany.Elapsed.TotalMilliseconds:F0}");
        }
        catch (Exception ex)
        {
            DiagSink.Write($"[DIAG setmanyatomic-throw] tree={TreeId} op={operationId} entriesCount={entries.Count} elapsedMs={swSetMany.Elapsed.TotalMilliseconds:F0} ex={ex.GetType().Name} msg={ex.Message.Replace(System.Environment.NewLine, " | ")}");
            throw;
        }
#else
        await ShardActivationRetry.RunAsync(
            () => saga.ExecuteAsync(TreeId, entries),
            cancellationToken);
#endif
    }

    private static void ValidateOperationId(string operationId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        if (operationId.Contains('/'))
            throw new ArgumentException(
                "operationId must not contain '/' (reserved as the grain-key separator).",
                nameof(operationId));
    }

    /// <summary>
    /// Mixed atomic bulk write: unions <paramref name="upserts"/> and
    /// <paramref name="deletes"/> into a single atomic batch carrying a
    /// parallel per-entry delete (tombstone) channel, then dispatches to the
    /// saga grain keyed by <c>{TreeId}/{operationId}</c>. The delete keys ride
    /// the same saga terminal as the upserts, so a re-key retraction (upsert at
    /// the new key, delete at the old key) flips atomically.
    /// </summary>
    public async Task SetManyAtomicAsync(
        List<KeyValuePair<string, byte[]>> upserts,
        IReadOnlyList<string> deletes,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(upserts);
        ArgumentNullException.ThrowIfNull(deletes);
        ValidateOperationId(operationId);
        cancellationToken.ThrowIfCancellationRequested();
        if (upserts.Count == 0 && deletes.Count == 0) return;

        // Union the upserts and deletes into a single batch with a parallel
        // per-entry delete flag. Upserts keep their flag false; deletes carry
        // an empty (non-null) value buffer and a true flag so the leaf stages a
        // tombstone. When there are no deletes the delete channel stays null,
        // keeping the dispatch byte-identical to the upsert-only overload.
        var entries = new List<KeyValuePair<string, byte[]>>(upserts.Count + deletes.Count);
        entries.AddRange(upserts);
        List<bool>? entryDeletes = null;
        if (deletes.Count > 0)
        {
            entryDeletes = new List<bool>(entries.Capacity);
            for (var i = 0; i < upserts.Count; i++) entryDeletes.Add(false);
            foreach (var key in deletes)
            {
                entries.Add(new KeyValuePair<string, byte[]>(key, Array.Empty<byte>()));
                entryDeletes.Add(true);
            }
        }

        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
        await ShardActivationRetry.RunAsync(
            () => saga.ExecuteAsync(TreeId, entries, entryDeletes),
            cancellationToken);
    }

    /// <summary>
    /// Guarded atomic multi-key write. Activates a dedicated
    /// <see cref="IAtomicWriteGrain"/> and awaits its guarded saga, which
    /// commits all-or-nothing only if every key's pre-saga value satisfies
    /// <paramref name="predicate"/>. Returns the terminal outcome rather than
    /// throwing on a precondition miss.
    /// </summary>
    public async Task<AtomicWriteOutcome> SetManyAtomicWhereAsync(
        List<KeyValuePair<string, byte[]>> entries,
        LatticePredicateNode predicate,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0) return AtomicWriteOutcome.Committed;

        var operationId = Guid.NewGuid().ToString("N");
        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
        return await ShardActivationRetry.RunAsync(
            () => saga.ExecuteGuardedAsync(TreeId, entries, predicate),
            cancellationToken);
    }

    /// <summary>
    /// Caller-supplied idempotency-key overload of the guarded atomic write.
    /// Re-submitting with the same <paramref name="operationId"/> re-attaches
    /// to the original saga and returns its memoized outcome.
    /// </summary>
    public async Task<AtomicWriteOutcome> SetManyAtomicWhereAsync(
        List<KeyValuePair<string, byte[]>> entries,
        LatticePredicateNode predicate,
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(entries);
        ValidateOperationId(operationId);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Count == 0) return AtomicWriteOutcome.Committed;

        var saga = grainFactory.GetGrain<IAtomicWriteGrain>($"{TreeId}/{operationId}");
        return await ShardActivationRetry.RunAsync(
            () => saga.ExecuteGuardedAsync(TreeId, entries, predicate),
            cancellationToken);
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        return LatticeIdempotencyContext.IsActive
            ? RunMutationAsync(ct => DeleteAsyncCore(key, ct), cancellationToken)
            : DeleteAsyncCore(key, cancellationToken);
    }

    async Task<bool> ISystemLattice.DeleteAsync(string key, CancellationToken cancellationToken)
    {
        using var _ = LatticeSystemTreeBoundary.Enter();
        return await DeleteAsyncCore(key, cancellationToken);
    }

    private async Task<bool> DeleteAsyncCore(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();
        await EnsureCompactionReminderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var existed = await RetryOnStaleRoutingAsync(
            (self: this, key),
            static async args =>
            {
                var shard = await args.self.GetShardGrainAsync(args.key);
                return await shard.DeleteAsync(args.key);
            },
            cancellationToken);
        if (existed) await PublishEventAsync(LatticeTreeEventKind.Delete, key);
        return existed;
    }

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();
        await EnsureCompactionReminderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var deleted = LatticeIdempotencyContext.IsActive
            ? await RunMutationAsync(ct => DeleteRangeAsyncOuter(startInclusive, endExclusive, null, ct), cancellationToken)
            : await DeleteRangeAsyncOuter(startInclusive, endExclusive, null, cancellationToken);
        if (deleted > 0)
            await PublishEventAsync(LatticeTreeEventKind.DeleteRange, $"{startInclusive}..{endExclusive}");
        return deleted;
    }

    public async Task<int> DeleteRangeWherePredicateAsync(LatticePredicateNode predicate, string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfShuttingDown();
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        cancellationToken.ThrowIfCancellationRequested();
        LatticeTransactionContext.EnsureCurrent();
        await EnsureCompactionReminderAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var deleted = LatticeIdempotencyContext.IsActive
            ? await RunMutationAsync(ct => DeleteRangeAsyncOuter(startInclusive, endExclusive, predicate, ct), cancellationToken)
            : await DeleteRangeAsyncOuter(startInclusive, endExclusive, predicate, cancellationToken);
        if (deleted > 0)
            await PublishEventAsync(LatticeTreeEventKind.DeleteRange, $"{startInclusive}..{endExclusive}");
        return deleted;
    }

    private Task<int> DeleteRangeAsyncOuter(string startInclusive, string endExclusive, LatticePredicateNode? predicate, CancellationToken cancellationToken)
    {
        return RetryOnStaleRoutingAsync(
            (self: this, startInclusive, endExclusive, predicate, cancellationToken),
            static args => args.self.DeleteRangeAsyncCore(args.startInclusive, args.endExclusive, args.predicate, args.cancellationToken),
            cancellationToken);
    }

    private async Task<int> DeleteRangeAsyncCore(string startInclusive, string endExclusive, LatticePredicateNode? predicate, CancellationToken cancellationToken)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // Generate a single producer-side issue HLC for the entire range
        // delete and pin every per-leaf tombstone (across every shard in
        // the fan-out) to that HLC via LatticeHlcOverrideContext. This
        // preserves the cross-origin LWW invariant on the receiver side:
        // a DeleteRange authored at frontier T must not overwrite a
        // foreign-origin write whose HLC is strictly greater than T. The
        // single-HLC stamping also makes the producer-side and
        // receiver-side stamps bit-identical, so leaf-level LWW
        // resolution at the receiver agrees with the producer for every
        // key in the range.
        //
        // Nested DeleteRange (a user-level DeleteRange invoked from
        // inside a saga or split coordinator that already pinned an
        // override) keeps the outer override - the producer's authoring
        // frontier dominates and the inner walk inherits it.
        var existingOverride = LatticeHlcOverrideContext.Current;
        var issueHlc = existingOverride ?? HybridLogicalClock.Tick(default);
        using var hlcScope = LatticeHlcOverrideContext.With(issueHlc);

        // Fan out to all physical shards in parallel - any may contain keys in the range.
        // Per-shard ShardActivationRetry wrap: a single shard's cold-start
        // seed-timeout retries only that shard, not the whole fan-out.
        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = GetShardGrainByIndex(physicalTreeId, physicalShards[i]);
            tasks[i] = ShardActivationRetry.RunAsync(
                () => shard.DeleteRangeAsync(startInclusive, endExclusive, predicate));
        }

        await Task.WhenAll(tasks);
        cancellationToken.ThrowIfCancellationRequested();

        var total = 0;
        for (int i = 0; i < tasks.Length; i++)
            total += tasks[i].Result;
        return total;
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            return await CountAsyncCore(cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountAsyncCore(cancellationToken);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountAsyncCore(cancellationToken);
        }
    }

    public async Task<int> CountAsync(string? startInclusive, string? endExclusive, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            return await RangedCountAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await RangedCountAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await RangedCountAsyncCore(startInclusive, endExclusive, cancellationToken);
        }
    }

    /// <summary>
    /// Whole-tree strongly-consistent count. Delegates to
    /// <see cref="RangedCountAsyncCore"/> with unbounded range
    /// <c>(null, null)</c> so the intricate split-reconciliation machinery
    /// lives in exactly one place.
    /// </summary>
    private Task<int> CountAsyncCore(CancellationToken cancellationToken) =>
        RangedCountAsyncCore(null, null, cancellationToken);

    /// <summary>
    /// Strongly-consistent live key count over the half-open key range
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)
    /// across all physical shards of this tree (a <see langword="null"/>
    /// bound is unbounded on that side). Tolerates concurrent adaptive shard
    /// splits by asking every physical shard to count only the virtual slots
    /// it currently owns per the authoritative <see cref="ShardMap"/>, then
    /// re-reading the map after the fan-out and retrying on any version
    /// change. Fully-covered leaves contribute their full count; only
    /// boundary leaf(s) are partial-counted, and no keys are materialised
    /// across the wire on either path.
    /// <para>
    /// This supersedes an earlier design that fanned out via
    /// <see cref="IShardRootGrain.CountWithMovedAwayAsync"/> and relied on
    /// each source shard filtering its moved-slot keys via
    /// <c>SplitInProgress.Phase</c>. That protocol had a gap: the split
    /// coordinator publishes the new <see cref="ShardMap"/> (including the
    /// target shard) before advancing the source shard's persisted phase to
    /// <c>Reject</c>. During that window the source did not filter
    /// moved-slot keys while the target already held them, and a count
    /// arriving in the window double-counted each migrating-slot key.
    /// Routing per-slot through the current map closes that window by
    /// construction: each virtual slot is counted exactly once, against the
    /// shard the map identifies as its current owner.
    /// </para>
    /// <para>
    /// Bounded by <see cref="LatticeOptions.MaxScanRetries"/>; throws
    /// <see cref="InvalidOperationException"/> when topology changes faster
    /// than the orchestrator can converge. System trees skip reconciliation
    /// (they never participate in adaptive splits and the registry would
    /// deadlock on itself).
    /// </para>
    /// </summary>
    private async Task<int> RangedCountAsyncCore(string? startInclusive, string? endExclusive, CancellationToken cancellationToken)
    {
        var (physicalTreeId, shardMap0) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var physicalShards = shardMap0.GetPhysicalShardIndices();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var maxRetries = Math.Max(1, Options.MaxScanRetries);

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (attempt > 0)
            {
                InvalidateShardMap();
                (physicalTreeId, shardMap0) = await GetRoutingAsync();
                physicalShards = shardMap0.GetPhysicalShardIndices();
            }

            var versionAtStart = shardMap0.Version;
            var virtualShardCount = shardMap0.Slots.Length;

            // Per-attempt double-checked TxRegistry snapshot. snap1 is
            // stamped onto the ambient for the lifetime of the fan-out
            // so every leaf applies the same registry decision view (a
            // linearizable scan over the InFlight->Committed
            // transition). snap2, taken after the fan-out, is checked
            // against snap1 alongside the shard-map version stability
            // check below - either an InFlight->Committed transition or
            // a topology change forces a retry. The single-shot
            // snapshot pattern (snap1 only, fixed for the lifetime of
            // the call) is insufficient because per-leaf drain into
            // Entries on TxCommit is irreversible: a reader whose snap1
            // was taken before MarkCommittedAsync but whose fan-out
            // reaches some leaves after their drain observes drained
            // leaves returning post-saga Entries while sibling
            // undrained leaves consult snap1.InFlight and fall through
            // to pre-saga Entries - split observation that defeats
            // strict per-tree atomic visibility.
            var snap1Pair = await FetchRegistrySnapshotAsync();
            var snap1 = snap1Pair.Snap;

            // Fast path: Version == 0 means the default identity map is in
            // effect - no split has ever been persisted for this tree.
            // ShardMap.Version is monotonically incremented on every persist,
            // so if it is still 0 at the end of the call, no split can have
            // started during our fan-out. Use the cheap leaf.CountAsync()
            // path (full leaf count, in-range bound applied at the leaf) and
            // avoid BuildOwnedSlotMap / per-key slot hashing / binary-search
            // entirely.
            if (versionAtStart == 0L)
            {
                int simple;
                using (LatticeRegistrySnapshotContext.BeginScope(snap1))
                {
                    simple = await SimpleSumCountAsync(physicalTreeId, physicalShards, startInclusive, endExclusive);
                }
                var mapAfter = await registry.GetShardMapAsync(TreeId) ?? shardMap0;
                if (mapAfter.Version != 0L) { shardMap0 = mapAfter; continue; }
                if (!await IsSnap2StableAsync(snap1, snap1Pair.Revision)) continue;
                return simple;
            }

            // Partition virtual slots by current owner per the
            // authoritative map and ask each physical shard to count only
            // its owned slots (additionally bounded to the range). This
            // makes the result topology-consistent with the observed map
            // snapshot regardless of where each shard is in its per-split
            // phase machine.
            var ownedByShard = BuildOwnedSlotMap(shardMap0);
            var pass1Tasks = new Task<int>[physicalShards.Count];
            using (LatticeRegistrySnapshotContext.BeginScope(snap1))
            {
                for (int i = 0; i < physicalShards.Count; i++)
                {
                    var physicalIdx = physicalShards[i];
                    var shard = GetShardGrainByIndex(physicalTreeId, physicalIdx);
                    if (!ownedByShard.TryGetValue(physicalIdx, out var owned) || owned.Length == 0)
                    {
                        // Shard referenced by the map but owning no slots (pathological).
                        pass1Tasks[i] = Task.FromResult(0);
                        continue;
                    }
                    // Per-shard ShardActivationRetry wrap: a single shard's
                    // cold-start seed-timeout retries only that shard, not
                    // the whole fan-out.
                    pass1Tasks[i] = ShardActivationRetry.RunAsync(
                        () => shard.CountForSlotsAsync(owned, virtualShardCount, startInclusive, endExclusive));
                }
                await Task.WhenAll(pass1Tasks);
            }

            var total = 0;
            for (int i = 0; i < pass1Tasks.Length; i++)
                total += pass1Tasks[i].Result;

            // Unconditional stability check: if the shard-map version moved
            // while pass1 was in flight, the per-shard counts may have
            // spanned an inconsistent snapshot. Discard and retry against
            // the fresh map.
            var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap0;
            if (shardMapNow.Version != versionAtStart) continue;

            // TxRegistry stability check: an InFlight->Committed
            // transition during the fan-out forces a retry under a
            // fresh snapshot.
            if (!await IsSnap2StableAsync(snap1, snap1Pair.Revision)) continue;

            return total;
        }

        throw new InvalidOperationException(
            $"CountAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
            "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
    }

    private async Task<int> SimpleSumCountAsync(string physicalTreeId, IReadOnlyList<int> physicalShards, string? startInclusive, string? endExclusive)
    {
        // Per-shard ShardActivationRetry wrap: a single shard's cold-start
        // seed-timeout retries only that shard, not the whole fan-out.
        var tasks = new Task<int>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = GetShardGrainByIndex(physicalTreeId, physicalShards[i]);
            tasks[i] = ShardActivationRetry.RunAsync(
                () => shard.CountAsync(startInclusive, endExclusive));
        }
        await Task.WhenAll(tasks);
        var total = 0;
        for (int i = 0; i < tasks.Length; i++) total += tasks[i].Result;
        return total;
    }

    /// <summary>
    /// Partitions the virtual slots of <paramref name="map"/> by their
    /// currently-owning physical shard, returning a dictionary mapping
    /// physical shard index to its sorted owned-slot array. Used by
    /// <see cref="CountAsyncCore"/> and <see cref="CountPerShardAsyncCore"/>
    /// to route per-slot count requests through the authoritative map.
    /// </summary>
    internal static Dictionary<int, int[]> BuildOwnedSlotMap(ShardMap map)
    {
        // Single-pass build. Slots are iterated in ascending order, so each
        // per-owner array is naturally sorted without a secondary Array.Sort.
        // First pass: count slots per owner so we can allocate each int[]
        // at its final size (no List<int> growth cycle, no copy). Second
        // pass: fill via per-owner write cursors.
        var slots = map.Slots;
        var counts = new Dictionary<int, int>();
        for (int s = 0; s < slots.Length; s++)
        {
            var owner = slots[s];
            counts.TryGetValue(owner, out var c);
            counts[owner] = c + 1;
        }

        var result = new Dictionary<int, int[]>(counts.Count);
        var cursors = new Dictionary<int, int>(counts.Count);
        foreach (var kv in counts)
        {
            result[kv.Key] = new int[kv.Value];
            cursors[kv.Key] = 0;
        }

        for (int s = 0; s < slots.Length; s++)
        {
            var owner = slots[s];
            var pos = cursors[owner];
            result[owner][pos] = s;
            cursors[owner] = pos + 1;
        }
        return result;
    }

    /// <summary>
    /// Groups <paramref name="slots"/> by their owning physical shard per
    /// <paramref name="map"/>. Out-of-range slots are silently dropped.
    /// </summary>
    internal static Dictionary<int, List<int>> GroupSlotsByOwner(HashSet<int> slots, ShardMap map)
    {
        var byOwner = new Dictionary<int, List<int>>();
        foreach (var s in slots)
        {
            if ((uint)s >= (uint)map.Slots.Length) continue;
            var owner = map.Slots[s];
            if (!byOwner.TryGetValue(owner, out var list))
            {
                list = [];
                byOwner[owner] = list;
            }
            list.Add(s);
        }
        return byOwner;
    }

    /// <summary>Copies <paramref name="list"/> into a new sorted array.</summary>
    internal static int[] ToSortedArray(List<int> list)
    {
        var arr = list.ToArray();
        Array.Sort(arr);
        return arr;
    }

    /// <summary>
    /// Computes the set of virtual slots whose owning physical shard differs
    /// between <paramref name="oldMap"/> and <paramref name="newMap"/>.
    /// Used by strongly-consistent scans to detect topology changes that
    /// happened between the start and end of a scan pass.
    /// </summary>
    internal static HashSet<int>? ComputeOwnerDiff(ShardMap oldMap, ShardMap newMap)
    {
        if (newMap.Version == oldMap.Version) return null;
        HashSet<int>? diff = null;
        var n = Math.Min(oldMap.Slots.Length, newMap.Slots.Length);
        for (int s = 0; s < n; s++)
        {
            if (oldMap.Slots[s] != newMap.Slots[s])
                (diff ??= []).Add(s);
        }
        return diff;
    }

    public async Task<IReadOnlyList<int>> CountPerShardAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedViewRead();
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            return await CountPerShardAsyncCore(cancellationToken);
        }
        catch (StaleTreeRoutingException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountPerShardAsyncCore(cancellationToken);
        }
        catch (InvalidOperationException) when (TryInvalidateStaleAlias())
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await CountPerShardAsyncCore(cancellationToken);
        }
    }

    private async Task<IReadOnlyList<int>> CountPerShardAsyncCore(CancellationToken cancellationToken)
    {
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        var physicalShards = shardMap.GetPhysicalShardIndices();

        // Mirror CountAsyncCore's per-slot routing so per-shard
        // counts are also topology-consistent with the observed map. Without
        // this, a split mid-call would surface as a target-shard count
        // inflated by double-counted migrating slots.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var maxRetries = Math.Max(1, Options.MaxScanRetries);

        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (attempt > 0)
            {
                InvalidateShardMap();
                (physicalTreeId, shardMap) = await GetRoutingAsync();
                physicalShards = shardMap.GetPhysicalShardIndices();
            }

            var versionAtStart = shardMap.Version;
            var virtualShardCount = shardMap.Slots.Length;

            // Per-attempt double-checked TxRegistry snapshot - same
            // rationale as CountAsyncCore. Per-shard counts must apply
            // a single registry decision view across every shard so
            // the totals reconcile against CountAsync; snap1/snap2
            // validation prevents the InFlight->Committed transition
            // race that would otherwise produce a split observation
            // across drained vs undrained leaves.
            var snap1Pair = await FetchRegistrySnapshotAsync();
            var snap1 = snap1Pair.Snap;

            // Fast path: Version == 0 means no split has ever been persisted
            // for this tree. Use the cheap per-shard CountAsync() path and
            // confirm the map is still at Version 0 after fan-out.
            if (versionAtStart == 0L)
            {
                var fastTasks = new Task<int>[physicalShards.Count];
                using (LatticeRegistrySnapshotContext.BeginScope(snap1))
                {
                    for (int i = 0; i < physicalShards.Count; i++)
                    {
                        var sh = GetShardGrainByIndex(physicalTreeId, physicalShards[i]);
                        // Per-shard ShardActivationRetry wrap: see
                        // SimpleSumCountAsync for the rationale.
                        fastTasks[i] = ShardActivationRetry.RunAsync(
                            () => sh.CountAsync());
                    }
                    await Task.WhenAll(fastTasks);
                }
                var mapAfter = await registry.GetShardMapAsync(TreeId) ?? shardMap;
                if (mapAfter.Version != 0L) { shardMap = mapAfter; continue; }
                if (!await IsSnap2StableAsync(snap1, snap1Pair.Revision)) continue;
                var fastCounts = new int[physicalShards.Count];
                for (int i = 0; i < physicalShards.Count; i++) fastCounts[i] = fastTasks[i].Result;
                return fastCounts;
            }

            var ownedByShard = BuildOwnedSlotMap(shardMap);
            var tasks = new Task<int>[physicalShards.Count];
            using (LatticeRegistrySnapshotContext.BeginScope(snap1))
            {
                for (int i = 0; i < physicalShards.Count; i++)
                {
                    var physicalIdx = physicalShards[i];
                    var shard = GetShardGrainByIndex(physicalTreeId, physicalIdx);
                    if (!ownedByShard.TryGetValue(physicalIdx, out var owned) || owned.Length == 0)
                    {
                        tasks[i] = Task.FromResult(0);
                        continue;
                    }
                    // Per-shard ShardActivationRetry wrap: see the pass-1
                    // fan-out above for the rationale.
                    tasks[i] = ShardActivationRetry.RunAsync(
                        () => shard.CountForSlotsAsync(owned, virtualShardCount));
                }
                await Task.WhenAll(tasks);
            }

            var shardMapNow = await registry.GetShardMapAsync(TreeId) ?? shardMap;
            if (shardMapNow.Version != versionAtStart) continue;

            if (!await IsSnap2StableAsync(snap1, snap1Pair.Revision)) continue;

            var counts = new int[physicalShards.Count];
            for (int i = 0; i < physicalShards.Count; i++)
                counts[i] = tasks[i].Result;
            return counts;
        }

        throw new InvalidOperationException(
            $"CountPerShardAsync exceeded {Options.MaxScanRetries} retries while topology kept changing. " +
            "Increase LatticeOptions.MaxScanRetries or reduce concurrent split activity.");
    }

    /// <summary>
    /// Lazily ensures the tree's <c>TombstoneCompactionGrain</c> has a
    /// registered reminder, on the first write to this tree. Subsequent
    /// writes are no-ops.
    /// </summary>
    private async Task EnsureCompactionReminderAsync()
    {
        if (_compactionEnsured) return;
        if (Options.TombstoneGracePeriod == Timeout.InfiniteTimeSpan) return;

        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        try
        {
            await compaction.EnsureReminderAsync();
        }
        catch (Exception ex) when (ReminderServiceReadiness.IsStillInitializing(ex))
        {
            // Registering the tombstone-compaction keepalive reminder is a
            // best-effort background-GC bootstrap that runs on the first write to
            // a tree. The Orleans reminder service initialises asynchronously
            // after the silo reaches Active, so a write in that startup window
            // throws a transient "still initializing" - which must never fail the
            // user's write. Defer: leave _compactionEnsured false so a later write
            // (once the service is up, within seconds of silo start) registers it.
            logger.LogDebug(
                "Deferred tombstone-compaction reminder registration for tree {TreeId}: reminder service still initializing; will retry on a subsequent write.",
                TreeId);
            return;
        }
        _compactionEnsured = true;
    }

    /// <summary>
    /// Lazily activates the per-tree autonomic <c>HotShardMonitorGrain</c> on
    /// the first write to this tree. Subsequent writes are no-ops. The monitor
    /// itself is a no-op when <see cref="LatticeOptions.AutoSplitEnabled"/> is
    /// <c>false</c>.
    /// </summary>
    private async Task EnsureMonitorAsync()
    {
        if (_monitorEnsured) return;
        if (!Options.AutoSplitEnabled) { _monitorEnsured = true; return; }
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            _monitorEnsured = true;
            return;
        }

        var monitor = grainFactory.GetGrain<IHotShardMonitorGrain>(TreeId);
        try
        {
            await monitor.EnsureRunningAsync();
        }
        catch (Exception ex) when (ReminderServiceReadiness.IsStillInitializing(ex))
        {
            // Same best-effort first-write bootstrap as the compaction reminder:
            // the hot-shard monitor registers its keepalive reminder, which can
            // race the reminder service's async startup init. Defer rather than
            // fail the user's write; a later write re-attempts once the service
            // is initialised.
            logger.LogDebug(
                "Deferred hot-shard-monitor activation for tree {TreeId}: reminder service still initializing; will retry on a subsequent write.",
                TreeId);
            return;
        }
        _monitorEnsured = true;
    }

    private async ValueTask<string> GetPhysicalTreeIdAsync()
    {
        if (_physicalTreeId is not null) return _physicalTreeId;

        // System trees (e.g. _lattice_trees) must not resolve aliases - the
        // registry itself is backed by an ILattice tree, so calling ResolveAsync
        // here would create a circular call chain and deadlock.
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            _physicalTreeId = TreeId;
            return _physicalTreeId;
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        _physicalTreeId = await registry.ResolveAsync(TreeId);
        return _physicalTreeId;
    }

    private ValueTask<IShardRootGrain> GetShardGrainAsync(string key)
    {
        // Sync fast path: if routing is already cached, resolve the shard
        // index and look up the per-activation array cache synchronously.
        // Skips both the async state-machine box for this method AND the
        // RoutingInfo allocation that the async wrapper used to take through
        // GetRoutingAsync on every call.
        var routing = _cachedRouting;
        if (routing is not null)
        {
            var shardIndex = routing.Map.Resolve(key);
            var shardCache = _cachedShards;
            if (shardCache is not null && (uint)shardIndex < (uint)shardCache.Length && shardCache[shardIndex] is { } existing)
                return new ValueTask<IShardRootGrain>(existing);
            return new ValueTask<IShardRootGrain>(ResolveShardSlow(routing.PhysicalTreeId, shardIndex));
        }
        return GetShardGrainSlowAsync(key);
    }

    private async ValueTask<IShardRootGrain> GetShardGrainSlowAsync(string key)
    {
        var routing = await GetRoutingAsync();
        var shardIndex = routing.Map.Resolve(key);
        var cache = _cachedShards;
        if (cache is not null && (uint)shardIndex < (uint)cache.Length && cache[shardIndex] is { } existing)
            return existing;
        return ResolveShardSlow(routing.PhysicalTreeId, shardIndex);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private IShardRootGrain ResolveShardSlow(string physicalTreeId, int shardIndex)
    {
        var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
        var cache = _cachedShards;
        if (cache is null)
        {
            // First miss: lazily size the cache to cover the largest physical
            // shard index in the active map. Every caller path reaches here
            // only after GetRoutingAsync has populated _shardMap (single-key
            // path: GetShardGrainAsync awaits routing first; fanout path:
            // GetShardGrainByIndex is invoked from inside loops keyed by
            // physicalShards, which itself comes from _shardMap). _shardMap
            // therefore must be non-null on this path. The defensive
            // `shardIndex + 1` floor is kept for crash-safety only - a
            // null _shardMap would indicate a programming error elsewhere.
            var map = _shardMap;
            int size = shardIndex + 1;
            if (map is not null)
            {
                var indices = map.GetPhysicalShardIndices();
                if (indices.Count > 0)
                {
                    var top = indices[indices.Count - 1] + 1;
                    if (top > size) size = top;
                }
            }
            cache = new IShardRootGrain?[size];
            _cachedShards = cache;
        }
        else if (shardIndex >= cache.Length)
        {
            // Unreachable in steady state: the initial allocation is sized to
            // cover every valid physical shard index in _shardMap, and the
            // map is invariant per-activation (both invalidation hooks null
            // _cachedShards alongside _shardMap, so a fresh map gets a fresh
            // cache). A larger shardIndex therefore means the map has been
            // mutated from under us - copy old entries forward into a grown
            // array so we do not silently drop previously cached references.
            var grown = new IShardRootGrain?[shardIndex + 1];
            Array.Copy(cache, grown, cache.Length);
            cache = grown;
            _cachedShards = cache;
        }
        cache[shardIndex] = shard;
        return shard;
    }

    /// <summary>
    /// Returns an <see cref="IShardRootGrain"/> reference for the given shard
    /// index against the resolved physical tree id. Reuses the array-keyed
    /// per-activation cache populated by <see cref="GetShardGrainAsync"/>
    /// (cycle 11) so multi-shard fanout sites - bulk batch, cursor, range
    /// scan, k-way-merge - that already have <c>physicalTreeId</c> and
    /// <c>shardIndex</c> in hand do not pay the
    /// <c>GetGrain&lt;IShardRootGrain&gt;(string)</c> materialisation cost on
    /// any repeat-shard hit, even when consecutive calls alternate across
    /// distinct shards. Cache invalidation is shared with
    /// <see cref="GetShardGrainAsync"/>: <see cref="TryInvalidateStaleAlias"/>
    /// and <see cref="InvalidateShardMap"/> both null the array.
    /// </summary>
    private IShardRootGrain GetShardGrainByIndex(string physicalTreeId, int shardIndex)
    {
        var cache = _cachedShards;
        if (cache is not null && (uint)shardIndex < (uint)cache.Length && cache[shardIndex] is { } existing)
            return existing;
        return ResolveShardSlow(physicalTreeId, shardIndex);
    }

    /// <summary>
    /// Returns the routing context for this tree: the resolved physical tree
    /// ID and the effective <see cref="ShardMap"/>. Both are cached for the
    /// lifetime of this activation and invalidated together by
    /// <see cref="TryInvalidateStaleAlias"/> when a downstream shard reports
    /// the tree as deleted.
    /// </summary>
    public ValueTask<RoutingInfo> GetRoutingAsync(CancellationToken cancellationToken = default)
    {
        // NOTE: intentionally NOT guarded - `GetRoutingAsync` is called by the
        // library's own internal coordinator grains (saga compensation, stats,
        // cursor) which sometimes resolve routing for their owning tree before
        // dispatching further internal calls. It does not read or mutate user
        // data; the shard grains enforce the real boundary on reads/writes.
        cancellationToken.ThrowIfCancellationRequested();
        var cached = _cachedRouting;
        if (cached is not null) return new ValueTask<RoutingInfo>(cached);
        return GetRoutingSlowAsync(cancellationToken);
    }

    /// <summary>
    /// Force-refresh overload. When <paramref name="forceRefresh"/> is
    /// <see langword="true"/>, invalidates the cached
    /// <see cref="ShardMap"/>, the cached resolved physical tree id
    /// (alias), and the <see cref="RoutingInfo"/> snapshot on this
    /// activation before re-resolving. Clearing the alias too is
    /// essential for callers using this hook to escape a
    /// <see cref="StaleTreeRoutingException"/> retry loop after an
    /// online resize / reshard swapped the alias - if only the shard
    /// map were invalidated, the next resolve would still hand back
    /// the same stale physical tree id and the caller would spin
    /// against the same throw indefinitely. External saga coordinators
    /// use this overload to break out of stale-routing retry loops
    /// that the <see cref="StatelessWorker"/> activation's per-instance
    /// cache would otherwise sustain.
    /// </summary>
    public ValueTask<RoutingInfo> GetRoutingAsync(bool forceRefresh, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (forceRefresh)
        {
            // Clear the alias too - a caller invoking forceRefresh:true is
            // by definition trying to escape a StaleTreeRoutingException
            // retry loop, which is an alias-level concern. Without this
            // the next resolve hands back the cached alias and the
            // caller spins against the same stale physical tree id.
            TryInvalidateStaleAlias();
            InvalidateShardMap();
        }
        return GetRoutingAsync(cancellationToken);
    }

    private async ValueTask<RoutingInfo> GetRoutingSlowAsync(CancellationToken cancellationToken)
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        if (_shardMap is null)
        {
            var resolved = await optionsResolver.ResolveAsync(TreeId);
            if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            {
                // System trees never have a custom shard map; using the default
                // also avoids a circular registry call.
                _shardMap = ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
            }
            else
            {
                var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
                _shardMap = await registry.GetShardMapAsync(TreeId)
                    ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
            }
        }
        var routing = new RoutingInfo(physicalTreeId, _shardMap);
        _cachedRouting = routing;
        return routing;
    }

    /// <inheritdoc />
    public Task<TreeDiagnosticReport> DiagnoseAsync(bool deep = false, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var stats = grainFactory.GetGrain<ILatticeStats>(TreeId);
        return stats.GetReportAsync(deep, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TreeStorageUsageReport> GetStorageUsageAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        var usage = grainFactory.GetGrain<ILatticeStorageUsage>(TreeId);
        return usage.GetReportAsync(forceRefresh: false, cancellationToken);
    }

    /// <summary>
    /// Linear backoff applied inside <see cref="RetryOnStaleRoutingAsync{TResult, TState}(TState, Func{TState, Task{TResult}}, CancellationToken)"/>
    /// (and its void overload) between consecutive
    /// <see cref="ShardActivationTimeoutException"/> retries. The seed
    /// timeout itself is the dominant per-attempt cost (~15 s default);
    /// the backoff exists so the next attempt does not slam into the same
    /// not-yet-visible activation immediately. Aligned with the per-attempt
    /// shape used by the standalone <see cref="ShardActivationRetry"/>
    /// helper consumed by operators (e.g. <see cref="ReshardAsync"/>) that
    /// do not route through the stale-routing envelope.
    /// </summary>
    private static readonly TimeSpan ShardActivationRetryBackoff = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Returns <c>true</c> if the cached alias was stale and has been invalidated,
    /// allowing a retry with a fresh resolution. Returns <c>false</c> if no alias
    /// was cached (meaning the tree is genuinely deleted, not a stale alias).
    /// Used as a <c>when</c> filter in catch clauses.
    /// </summary>
    private bool TryInvalidateStaleAlias()
    {
        if (_physicalTreeId is null) return false;
        _physicalTreeId = null;
        _shardMap = null;
        _cachedShards = null;
        _cachedRouting = null;
        return true;
    }

    /// <summary>
    /// Invalidates the cached <see cref="ShardMap"/> only (preserves the
    /// resolved physical tree ID). Used by <see cref="StaleShardRoutingException"/>
    /// catch clauses to force a fresh map fetch on retry after an adaptive
    /// shard split has remapped virtual slots to a new physical shard.
    /// Always returns <c>true</c> so it can be used as a <c>when</c> filter.
    /// </summary>
    private bool InvalidateShardMap()
    {
        _shardMap = null;
        _cachedShards = null;
        _cachedRouting = null;
        return true;
    }

    /// <summary>
    /// Deadline-bounded stale-routing retry loop used by every per-key
    /// public surface method (<see cref="GetAsync(string, CancellationToken)"/>,
    /// <see cref="SetAsync(string, byte[], CancellationToken)"/>, etc.) and
    /// by the replication apply surface. A single retry is insufficient
    /// because an adaptive shard split intentionally orders the source-side
    /// Reject transition before the registry's <c>ShardMap</c> flip (see
    /// <c>TreeShardSplitGrain.SwapAsync</c>), opening a one-RPC window in
    /// which a fresh map fetch still returns the pre-flip map and the
    /// retry hits the same Reject-phase source. The wall-clock budget
    /// (<see cref="StaleRoutingWriteRetryBudget"/>) absorbs that window
    /// and any cascading reshard storm, while still surfacing the original
    /// throw if the topology never quiesces.
    /// <para>
    /// The <see cref="InvalidOperationException"/> catch keeps the
    /// pre-existing single-retry semantics from <c>SetAsyncCore</c>: it is
    /// typically permanent (tree deleted, alias removed) and looping on it
    /// would mask deletion semantics and trip Orleans' default response
    /// timeout. A deleted tree therefore surfaces on the second throw, not
    /// after the full budget elapses.
    /// </para>
    /// <para>
    /// The <typeparamref name="TState"/> parameter is the BCL anti-closure
    /// pattern (mirrors <see cref="System.Threading.LazyInitializer"/> and
    /// <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey, TValue}.GetOrAdd{TArg}(TKey, Func{TKey, TArg, TValue}, TArg)"/>):
    /// callers pass per-invocation data through <paramref name="state"/>
    /// (typically a value-tuple capturing <c>this</c> plus the method's
    /// parameters) so the lambda passed as <paramref name="operation"/>
    /// can be declared <c>static</c> and Roslyn cache its
    /// <see cref="Func{T, TResult}"/> instance as a singleton on first
    /// use. Compared to the historic <c>async () =&gt; { ... }</c> shape
    /// this eliminates one <see cref="Func{T}"/> delegate object and one
    /// <c>&lt;&gt;c__DisplayClass*</c> closure object per call on the
    /// success path; the inner lambda's async state machine box is the
    /// only residual heap allocation imposed by the retry envelope, and
    /// callers on the hottest paths
    /// (<see cref="GetAsyncCore"/> / <see cref="SetAsyncCore"/> /
    /// <see cref="ExistsAsyncCore"/> / <see cref="GetWithVersionAsync"/>)
    /// inline the loop directly to elide that residual too.
    /// </para>
    /// </summary>
    private async Task<TResult> RetryOnStaleRoutingAsync<TResult, TState>(
        TState state,
        Func<TState, Task<TResult>> operation,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
        var invalidOpRetried = false;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await operation(state);
            }
            catch (StaleShardRoutingException)
            {
                if (DateTime.UtcNow >= deadline) throw;
                InvalidateShardMap();
            }
            catch (StaleTreeRoutingException)
            {
                if (DateTime.UtcNow >= deadline) throw;
                if (!TryInvalidateStaleAlias()) throw;
            }
            catch (ShardActivationTimeoutException)
            {
                // Seed-timeout is by design retriable - every cross-grain
                // step in the shard-root activation seed is idempotent on
                // retry. Absorb it into the existing wall-clock budget so
                // operators do not observe the typed surface during normal
                // cold-start races. A backoff between attempts keeps the
                // retry from slamming into the same not-yet-visible
                // activation immediately.
                if (DateTime.UtcNow >= deadline) throw;
                await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
            }
            catch (Exception ex) when (ShardActivationRetry.IsTransientSiloChurn(ex))
            {
                // The target activation's host is restarting, draining, or
                // has just left the cluster (SiloUnavailableException, or a
                // forward-to-deactivating rejection). The Orleans directory
                // re-places the activation on retry, so absorb the
                // membership-convergence artifact within the same wall-clock
                // budget with a backoff rather than surfacing it.
                if (DateTime.UtcNow >= deadline) throw;
                await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
            }
            catch (InvalidOperationException)
            {
                if (invalidOpRetried) throw;
                if (!TryInvalidateStaleAlias()) throw;
                invalidOpRetried = true;
            }
        }
    }

    /// <summary>
    /// Non-generic overload of <see cref="RetryOnStaleRoutingAsync{TResult, TState}(TState, Func{TState, Task{TResult}}, CancellationToken)"/>
    /// for operations that do not produce a value. Duplicates the loop
    /// body rather than wrapping the generic version in a sentinel
    /// closure so the void path takes no extra closure allocation. See
    /// the generic overload for the <typeparamref name="TState"/>
    /// anti-closure rationale.
    /// </summary>
    private async Task RetryOnStaleRoutingAsync<TState>(
        TState state,
        Func<TState, Task> operation,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + StaleRoutingWriteRetryBudget;
        var invalidOpRetried = false;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await operation(state);
                return;
            }
            catch (StaleShardRoutingException)
            {
                if (DateTime.UtcNow >= deadline) throw;
                InvalidateShardMap();
            }
            catch (StaleTreeRoutingException)
            {
                if (DateTime.UtcNow >= deadline) throw;
                if (!TryInvalidateStaleAlias()) throw;
            }
            catch (ShardActivationTimeoutException)
            {
                // See the generic overload for the rationale: seed-timeout
                // is retriable by construction; absorb within the wall-clock
                // budget with a per-attempt backoff.
                if (DateTime.UtcNow >= deadline) throw;
                await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
            }
            catch (Exception ex) when (ShardActivationRetry.IsTransientSiloChurn(ex))
            {
                // The target activation's host is restarting, draining, or
                // has just left the cluster (SiloUnavailableException, or a
                // forward-to-deactivating rejection). The Orleans directory
                // re-places the activation on retry, so absorb the
                // membership-convergence artifact within the same wall-clock
                // budget with a backoff rather than surfacing it.
                if (DateTime.UtcNow >= deadline) throw;
                await Task.Delay(ShardActivationRetryBackoff, cancellationToken);
            }
            catch (InvalidOperationException)
            {
                if (invalidOpRetried) throw;
                if (!TryInvalidateStaleAlias()) throw;
                invalidOpRetried = true;
            }
        }
    }

    /// <summary>
    /// Computes a stable shard index for the given key using XxHash32.
    /// Provided for backward compatibility; new routing should go through
    /// the per-tree <see cref="ShardMap"/> via <see cref="GetRoutingAsync"/>.
    /// </summary>
    internal static int GetShardIndex(string key, int shardCount) =>
        LatticeSharding.GetShardIndex(key, shardCount);

    /// <summary>
    /// Pair of <see cref="ITxRegistryGrain.SnapshotAsync"/> result and
    /// the matching <see cref="ITxRegistryGrain.GetDecisionsRevisionAsync"/>
    /// reading captured in the same call window. The revision lets the
    /// double-checked retry replace its snap2 dictionary fetch with the
    /// cheap revision probe (see
    /// <see cref="IsSnap2StableAsync"/>): when the registry's
    /// <em>post</em>-fan-out revision equals the snap1 revision, no
    /// decision mutation occurred and snap1 is provably authoritative.
    /// </summary>
    private readonly record struct RegistrySnapshotPair(
        Dictionary<Guid, TxStatus>? Snap,
        long Revision);

    /// <summary>
    /// Fetches a single per-tree <see cref="ITxRegistryGrain"/> decision
    /// snapshot. Used by multi-shard read fan-outs in concert with
    /// <see cref="IsSnapshotStable"/> to implement a double-checked
    /// snapshot retry: pre-fetch (snap1) is stamped onto the ambient
    /// <see cref="LatticeRegistrySnapshotContext"/> for the lifetime of
    /// the fan-out so every leaf applies the same registry decision
    /// view; post-fetch (snap2) is compared against snap1 to detect any
    /// <see cref="TxStatus.InFlight"/>-&gt;<see cref="TxStatus.Committed"/>
    /// transition that raced the fan-out and would otherwise produce
    /// a split observation across drained vs undrained leaves.
    /// <para>
    /// Defensive: if the registry RPC fails the call returns
    /// <c>null</c> so the scan still proceeds - leaves fall back to
    /// their per-leaf <c>GetStatusManyAsync</c> RPC, which reintroduces
    /// the original non-linearizable-scan race but keeps reads
    /// available. The matching <see cref="IsSnapshotStable"/> check
    /// treats a <c>null</c> snap2 as stable for the same reason.
    /// </para>
    /// <para>
    /// Returns a <see cref="RegistrySnapshotPair"/> carrying both the
    /// decision dictionary and the registry's monotonic decisions
    /// revision; the revision feeds the post-fan-out
    /// <see cref="IsSnap2StableAsync"/> cheap-probe stability check.
    /// On RPC failure both fields decay to defaults
    /// (<c>Snap = null</c>, <c>Revision = 0</c>); the stability check
    /// treats this as stable (same back-compat as the original
    /// snap1-null path).
    /// </para>
    /// </summary>
    private async ValueTask<RegistrySnapshotPair> FetchRegistrySnapshotAsync()
    {
        var registry = grainFactory.GetGrain<ITxRegistryGrain>(TreeId);
        try
        {
            // Single atomic RPC returns both the dict and the revision
            // captured in the same registry turn, so the (Snap,
            // Revision) pair is guaranteed self-consistent. A sequential
            // (SnapshotAsync, GetDecisionsRevisionAsync) pair would
            // admit a writer-interleave skew: SnapshotAsync runs under
            // the turn token (no AlwaysInterleave), but a writer's
            // turn could complete fully between the two calls and
            // produce a snapshot reflecting revision N alongside a
            // probe reading N+1 - a "false stable" hazard the
            // post-fan-out probe cannot detect.
            var pair = await registry.SnapshotWithRevisionAsync();
            return new RegistrySnapshotPair(pair.Decisions, pair.Revision);
        }
        catch
        {
            return new RegistrySnapshotPair(null, 0L);
        }
    }

    /// <summary>
    /// Cheap-probe replacement for the post-fan-out snap2 dictionary
    /// fetch. Issues a single
    /// <see cref="ITxRegistryGrain.GetDecisionsRevisionAsync"/> RPC and
    /// returns <c>true</c> when the returned revision equals the
    /// captured <paramref name="snap1Revision"/> - in that case the
    /// registry's Decisions map provably did not mutate during the
    /// fan-out, so <paramref name="snap1"/> is still authoritative and
    /// no second dictionary serialization is required. On revision
    /// mismatch the method falls through to a full
    /// <see cref="ITxRegistryGrain.SnapshotAsync"/> fetch and applies
    /// the existing <see cref="IsSnapshotStable"/> rule, preserving
    /// every legacy correctness guarantee.
    /// <para>
    /// Multi-silo safe: the probe is still a grain RPC to the
    /// single-activation registry; the saving is the elided dictionary
    /// payload on the steady-state happy path, not the RPC turn itself.
    /// </para>
    /// </summary>
    private async ValueTask<bool> IsSnap2StableAsync(
        Dictionary<Guid, TxStatus>? snap1,
        long snap1Revision)
    {
        var registry = grainFactory.GetGrain<ITxRegistryGrain>(TreeId);
        long revision2;
        try
        {
            revision2 = await registry.GetDecisionsRevisionAsync();
        }
        catch
        {
            // RPC failure: treat as stable (legacy snap1-null /
            // snap2-null handling - the reader proceeds and falls back
            // to per-leaf GetStatusManyAsync if a pending entry is
            // observed).
            return true;
        }
        if (revision2 == snap1Revision)
        {
            return true;
        }

        // Revision changed during the fan-out. A Committed transition
        // would invalidate snap1; an Aborted-only or pure-Forget
        // transition would not. Issue a fresh atomic
        // SnapshotWithRevisionAsync to disambiguate (using the
        // self-consistent shape avoids re-introducing the snap1
        // skew the optimisation closed), then run the existing
        // IsSnapshotStable rule.
        Dictionary<Guid, TxStatus>? snap2;
        try
        {
            var snap2Pair = await registry.SnapshotWithRevisionAsync();
            snap2 = snap2Pair.Decisions;
        }
        catch
        {
            // RPC failure on the disambiguation path: same defensive
            // treatment as FetchRegistrySnapshotAsync above.
            return true;
        }
        return IsSnapshotStable(snap1, snap2);
    }

    /// <summary>
    /// Returns <c>true</c> when a fan-out result computed under
    /// <paramref name="snap1"/> is still consistent given a fresh
    /// <paramref name="snap2"/> taken after the fan-out - that is, no
    /// saga transitioned <see cref="TxStatus.InFlight"/>-&gt;
    /// <see cref="TxStatus.Committed"/> during the fan-out window. The
    /// check is asymmetric: every <see cref="TxStatus.Committed"/>
    /// entry in snap2 must already be Committed in snap1.
    /// <para>
    /// The asymmetry is the whole point. Per-leaf drain into the
    /// runtime entry cache on
    /// <see cref="MutationKind.TxCommit"/> is irreversible - once a
    /// leaf has flipped a saga's prepared keys into the cache it has
    /// no record they came from a saga, so a stale
    /// <see cref="TxStatus.InFlight"/> snapshot can no longer gate
    /// visibility on that leaf. A reader whose snap1 was taken before
    /// <c>MarkCommittedAsync</c> but whose fan-out reaches some leaves
    /// after their drain therefore observes drained leaves serving
    /// post-saga entries while sibling undrained leaves consult
    /// snap1.InFlight and fall through to pre-saga entries - split
    /// observation. snap2.Committed reveals the transition; the
    /// caller retries with the fresh snapshot in scope so the next
    /// fan-out observes the saga as Committed everywhere (drained
    /// leaves return cached-post AND undrained leaves surface
    /// pending.value=post via <see cref="TxStatus.Committed"/>).
    /// </para>
    /// <para>
    /// <see cref="TxStatus.Aborted"/> transitions and registry forgets
    /// (snap1 has the txid, snap2 does not) are atomic-safe by
    /// construction and do not invalidate snap1: an Aborted saga
    /// drains by removing pending entries everywhere (no Entries
    /// change), and a forget implies every leaf has already drained
    /// the terminal mark, so snap1.Committed already gates undrained
    /// pending consistently. A <c>null</c> snap2 (registry RPC
    /// failure) is treated as stable so the scan completes rather
    /// than retrying indefinitely.
    /// </para>
    /// </summary>
    private static bool IsSnapshotStable(
        Dictionary<Guid, TxStatus>? snap1,
        Dictionary<Guid, TxStatus>? snap2)
    {
        if (snap2 is null) return true;
        foreach (var (txid, status) in snap2)
        {
            if (status == TxStatus.Committed)
            {
                if (snap1 is null
                    || !snap1.TryGetValue(txid, out var s1)
                    || s1 != TxStatus.Committed)
                {
                    return false;
                }
            }
        }
        return true;
    }
}
