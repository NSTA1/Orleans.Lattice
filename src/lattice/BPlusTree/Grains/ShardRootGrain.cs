using System.Diagnostics;
using System.IO;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The root grain for a single shard. Lazily creates the first leaf and
/// handles root splits by creating a new internal root above the old one.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// </summary>
internal sealed partial class ShardRootGrain(
    IGrainContext context,
    [PersistentState("shardroot", LatticeOptions.StorageProviderName)] IPersistentState<ShardRootState> state,
    IGrainFactory grainFactory,
    LatticeOptionsResolver optionsResolver,
    ILogger<ShardRootGrain> logger,
    MutationObserverDispatcher mutationObservers) : IShardRootGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    async Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // Coalesced dirty-leaf flush on clean shutdown: ensure any pending
        // in-memory marks reach storage so the next activation observes
        // the same dirty-set the compaction coordinator already expects.
        // Best-effort - a failure here is logged inside the helper and
        // the next routed Delete will re-mark the leaf.
        await FlushPendingDirtyMarksOnDeactivateAsync(cancellationToken);
    }

    private string? _treeId;
    private string TreeId => _treeId ??= ComputeTreeId();

    private bool? _internalOriginEnforced;

    /// <summary>
    /// Defense-in-depth internal-origin assertion (issue #1103): refuses a direct
    /// external grain call to this internal shard grain that would bypass the
    /// <c>ILattice</c> facade's access gate. A no-op unless the authorization
    /// layer's capability-stripping filter is registered (signalled by the
    /// <see cref="LatticeInternalOriginEnforcementMarker"/> sentinel); a no-auth
    /// cluster, or one with a custom gate but no filter, pays nothing. When active,
    /// every legitimate caller (the facade, replication-apply, structural
    /// maintenance, the atomic-write saga, and bulk-load) is silo-sourced and
    /// carries the re-derived internal-origin marker, so only a direct external
    /// client call is rejected.
    /// </summary>
    private void EnsureInternalOrigin(LatticeOperation operation)
    {
        _internalOriginEnforced ??=
            context.ActivationServices.GetService<LatticeInternalOriginEnforcementMarker>() is not null;
        if (_internalOriginEnforced is true)
        {
            LatticeInternalOriginContext.EnsureInternalGrainOrigin(TreeId, operation);
        }
    }
    private string ComputeTreeId()
    {
        var key = context.GrainId.Key.ToString()!;
        var (slash, _) = ParseShardGrainKey(key);
        return key[..slash];
    }

    /// <summary>
    /// Validates the activation key and returns the separator position and
    /// parsed shard index. Throws <see cref="InvalidOperationException"/> for
    /// any malformed shape (empty key, missing separator, leading slash,
    /// trailing slash, non-integer suffix, negative shard index) so callers
    /// see a typed validation failure rather than a low-level
    /// <see cref="ArgumentOutOfRangeException"/> /
    /// <see cref="FormatException"/> or a silently misparsed value that
    /// mis-tags metrics and corrupts routing. Mirrors the contract already
    /// enforced by <c>WalShardGrain</c> and
    /// <c>LeafReplayCoordinatorGrain</c>.
    /// </summary>
    /// <remarks>
    /// Returns the slash position rather than allocating the
    /// <c>{treeId}</c> substring so callers that only need the shard index
    /// (<c>ShardIndex</c>, <c>MyShardIndex</c>) pay zero string allocations.
    /// <c>ComputeTreeId</c> slices once and caches the result.
    /// </remarks>
    private static (int slash, int shardIndex) ParseShardGrainKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            throw new InvalidOperationException(
                $"{nameof(ShardRootGrain)} activation key is empty; expected '{{treeId}}/{{shardIndex}}'.");
        var slash = key.LastIndexOf('/');
        if (slash <= 0 || slash >= key.Length - 1)
            throw new InvalidOperationException(
                $"{nameof(ShardRootGrain)} activation key '{key}' is not in the expected '{{treeId}}/{{shardIndex}}' format.");
        if (!int.TryParse(
                key.AsSpan(slash + 1),
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture,
                out var shardIndex)
            || shardIndex < 0)
        {
            throw new InvalidOperationException(
                $"{nameof(ShardRootGrain)} activation key '{key}' has a non-integer or negative shard index suffix.");
        }
        return (slash, shardIndex);
    }

    private ResolvedLatticeOptions? _cachedOptions;

    /// <summary>
    /// Returns the effective options for this tree. Cached for the grain's
    /// lifetime. Structural sizing is sourced from the tree registry pin;
    /// non-structural fields flow through from <see cref="LatticeOptions"/>.
    /// </summary>
    private ValueTask<ResolvedLatticeOptions> GetOptionsAsync() =>
        _cachedOptions is not null
            ? new ValueTask<ResolvedLatticeOptions>(_cachedOptions)
            : ResolveOptionsSlowAsync();

    private async ValueTask<ResolvedLatticeOptions> ResolveOptionsSlowAsync() =>
        _cachedOptions = await optionsResolver.ResolveAsync(TreeId);

    private static readonly ObjectPool<Stack<GrainId>> StackPool =
        new DefaultObjectPoolProvider().Create(new StackPoolPolicy());

    private sealed class StackPoolPolicy : PooledObjectPolicy<Stack<GrainId>>
    {
        public override Stack<GrainId> Create() => new();
        public override bool Return(Stack<GrainId> obj) { obj.Clear(); return true; }
    }

    private const int MaxRetries = 2;

    /// <summary>
    /// Per-activation gate that serialises every shard-root
    /// <c>state.WriteStateAsync()</c> call. <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.SetManyAsync"/>
    /// is annotated <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> for throughput, which
    /// allows two concurrent <c>SetManyAsync</c> turns on the same activation to
    /// race the underlying <c>WriteStateAsync</c>
    /// call. The second writer observes a stale etag and the storage provider
    /// throws <see cref="Orleans.Storage.InconsistentStateException"/> -
    /// the exact "Etag mismatch during Update" signal the U9g real-Azure
    /// ladder captured against the shard-root state. The gate guards
    /// storage only, not compute: callers continue to do their sort /
    /// route / lookup work in parallel, only the single storage write
    /// is serialised. Every shard-root persistence site routes through
    /// <see cref="WriteShardStateAsync"/> so admin paths (split,
    /// shadow-forward, bulk-load, lifecycle, dirty-leaves) cannot
    /// collide with a concurrent <c>SetManyAsync</c> turn either.
    /// </summary>
    private readonly SemaphoreSlim _stateWriteGate = new(1, 1);

    /// <summary>
    /// Per-activation gate that serialises the full root-promotion
    /// sequence (Phase 1 persist of <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.PendingPromotion"/>
    /// + cross-grain <c>InitializeAsync</c> on the new root + Phase 2
    /// persist that clears the pending intent). Two interleaved
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.SetManyAsync"/> turns can both invoke
    /// the promotion path on the same activation; without this gate,
    /// turn B's assignment to <c>state.State.PendingPromotion</c> can
    /// overwrite turn A's still-in-flight intent before A's
    /// <c>CompletePromotionAsync</c> observes it, silently dropping
    /// A's promotion and leaving a dangling sibling grain. The
    /// existing <see cref="_stateWriteGate"/> only serialises
    /// individual storage writes; promotion is a multi-await
    /// sequence with cross-grain calls between two persistence
    /// sites and so needs its own gate.
    /// </summary>
    private readonly SemaphoreSlim _promotionGate = new(1, 1);

    /// <summary>
    /// Per-activation gate that serialises the lazy single-leaf root
    /// seed in <see cref="EnsureRootAsync"/>. Public operations are
    /// annotated <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>, so two turns
    /// can both observe a null in-memory <c>RootNodeId</c> on a
    /// freshly-activated shard and race into the seed path. Without this
    /// gate, both would re-read storage, both would find it empty, and
    /// both would create-and-persist a single-leaf root - the second
    /// write overwriting the first turn's already-published leaf id and
    /// orphaning its leaf. The gate guarantees only one turn performs the
    /// storage re-read + seed sequence; the loser re-checks the (now
    /// non-null) <c>RootNodeId</c> under the gate and returns without
    /// seeding. This is the activation-local complement to the
    /// cross-cluster defence: the re-read closes the "reactivated against
    /// stale empty state" window, and the gate closes the "two
    /// interleaved turns both seed" window.
    /// </summary>
    private readonly SemaphoreSlim _ensureRootGate = new(1, 1);

    /// <summary>
    /// Serialised replacement for <c>state.WriteStateAsync()</c>. All
    /// shard-root <c>WriteStateAsync</c>
    /// call sites must route through this helper so the per-activation
    /// <see cref="_stateWriteGate"/> serialises storage writes across
    /// interleaved turns.
    /// </summary>
    private async Task WriteShardStateAsync()
    {
        await _stateWriteGate.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            await state.WriteStateAsync();
        }
        finally
        {
            _stateWriteGate.Release();
        }
    }

    private IHostApplicationLifetime? _lifetime;
    private bool _lifetimeResolved;

    /// <summary>
    /// Resolves the optional <see cref="IHostApplicationLifetime"/> from the
    /// activation's service provider. Cached after first lookup; returns
    /// <see langword="null"/> on non-hosted test activations. Mirrors the
    /// lazy-resolve pattern the atomic-write saga coordinator established.
    /// </summary>
    private IHostApplicationLifetime? ResolveLifetime()
    {
        if (_lifetimeResolved) return _lifetime;
        _lifetimeResolved = true;
        _lifetime = context.ActivationServices?.GetService<IHostApplicationLifetime>();
        return _lifetime;
    }

    /// <summary>
    /// Fast-fails a shard-root write entry point with
    /// <see cref="LatticeShuttingDownException"/> when the host has begun
    /// shutting down, before the write touches the leaf grains or the
    /// write-ahead-log writer. A no-op on a healthy host or a non-hosted
    /// test activation. The steady-state healthy path is a cached field read
    /// plus a struct token check - no allocation.
    /// </summary>
    private void ThrowIfShuttingDown()
    {
        if (ResolveLifetime() is { } lifetime && lifetime.ApplicationStopping.IsCancellationRequested)
            throw new LatticeShuttingDownException(
                $"Write to shard '{context.GrainId.Key}' refused: the silo is shutting down (ApplicationStopping is signalled); "
                + "the write was not dispatched to the write-ahead-log writer.");
    }


    public async Task<byte[]?> GetAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfMovedAwayForReadKey(key);
        RecordRead();
        return await TraverseForReadAsync(key);
    }

    public async Task<VersionedValue> GetWithVersionAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfMovedAwayForReadKey(key);
        RecordRead();
        return await TraverseForReadWithVersionAsync(key);
    }

    /// <inheritdoc />
    public async Task<LwwEntry?> GetRawEntryAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfMovedAwayForReadKey(key);
        RecordRead();

        if (state.State.RootNodeId is null) return null;

        // Decide leaf-vs-internal by node TYPE so a corrupt RootIsLeaf flag
        // over an internal root (issue 899) descends to the real leaf rather
        // than blind-casting the internal root to IBPlusLeafGrain.
        var leafId = RootIsLeafTyped
            ? state.State.RootNodeId!.Value
            : await TraverseToLeafAsync(key);
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var raw = await leaf.GetRawEntryAsync(key);
        if (raw is null) return null;
        if (raw.Value.IsTombstone) return null;
        return raw;
    }

    /// <inheritdoc />
    public async Task<List<LwwEntry?>> GetRawEntriesAsync(List<string> keys)
    {
        await PrepareForOperationAsync();
        ThrowIfMovedAwayForReadAnyKey(keys);
        RecordRead();

        var result = new List<LwwEntry?>(keys.Count);
        for (int i = 0; i < keys.Count; i++) result.Add(null);

        if (state.State.RootNodeId is null) return result;

        // Group keys by their target leaf. Mirrors TraverseForBatchReadAsync
        // but addresses the raw leaf grain directly (bypassing LeafCacheGrain)
        // so tombstones and TTL metadata survive - matching single-key
        // GetRawEntryAsync semantics. The bucket value carries the
        // (key, inputIndex) pair so the per-leaf batched response can be
        // scattered back into the index-aligned result list.
        var leafBuckets = new Dictionary<GrainId, List<(string Key, int Index)>>();
        // Type-correcting flat-tree fast path: a corrupt RootIsLeaf flag left
        // true over an internal root (issue 899) falls through to per-key
        // routing rather than bucketing the whole batch onto the internal root.
        if (RootIsLeafTyped)
        {
            var rootLeaf = state.State.RootNodeId!.Value;
            var bucket = new List<(string, int)>(keys.Count);
            for (int i = 0; i < keys.Count; i++) bucket.Add((keys[i], i));
            leafBuckets[rootLeaf] = bucket;
        }
        else
        {
            for (int i = 0; i < keys.Count; i++)
            {
                var leafId = await TraverseToLeafAsync(keys[i]);
                if (!leafBuckets.TryGetValue(leafId, out var bucket))
                {
                    bucket = new List<(string, int)>();
                    leafBuckets[leafId] = bucket;
                }
                bucket.Add((keys[i], i));
            }
        }

        // One batched RPC per distinct leaf. Sequential rather than
        // parallel: each leaf grain serialises its incoming calls anyway
        // (Orleans single-threaded reentrancy model), and the saga's
        // microbench workload has all keys in a single leaf, so the
        // sequential vs WhenAll distinction is observably identical for
        // n=1 leaf and trades one Task.WhenAll allocation per call
        // otherwise. Keep sequential to mirror the existing
        // TraverseForBatchReadAsync shape.
        foreach (var (leafId, bucket) in leafBuckets)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var leafKeys = new List<string>(bucket.Count);
            foreach (var (key, _) in bucket) leafKeys.Add(key);

            var leafResult = await leaf.GetRawEntriesAsync(leafKeys);

            for (int i = 0; i < bucket.Count; i++)
            {
                var raw = leafResult[i];
                // Tombstones are surfaced as null to match the single-key
                // GetRawEntryAsync semantics. Already-expired entries are
                // returned (callers filter via LwwValue.IsExpired) for
                // parity with the single-key variant.
                if (raw is null || raw.Value.IsTombstone)
                {
                    // result slot already initialised to null above.
                    continue;
                }
                result[bucket[i].Index] = raw;
            }
        }
        return result;
    }

    public async Task<bool> ExistsAsync(string key)
    {
        await PrepareForOperationAsync();
        ThrowIfMovedAwayForReadKey(key);
        RecordRead();
        return await TraverseForExistsAsync(key);
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        await PrepareForOperationAsync();
        ThrowIfMovedAwayForReadAnyKey(keys);
        RecordRead();
        return await TraverseForBatchReadAsync(keys);
    }

    public async Task SetAsync(string key, byte[] value)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var forwardTask = TrackShadowForward((key, value), static (t, s) => t.SetAsync(s.key, s.value));
                var splitResult = await TraverseForWriteAsync(key, value);

                // If the root node split, we need to create a new internal root.
                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                // shadow-forward the write to the split target if applicable.
                await ForwardLocalWriteToShadowIfNeededAsync(key, value);
                await forwardTask;
                return;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                // The failed grain will be deactivated by Orleans. On retry, a fresh
                // activation loads clean state and the recovery guards resume any
                // interrupted split.
            }
        }
    }

    /// <inheritdoc />
    public async Task SetAsync(string key, byte[] value, long expiresAtTicks)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var forwardTask = TrackShadowForward((key, value, expiresAtTicks), static (t, s) => t.SetAsync(s.key, s.value, s.expiresAtTicks));
                var splitResult = await TraverseForWriteWithExpiryAsync(key, value, expiresAtTicks);

                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                // shadow-forward the write to the split target if applicable.
                // The target fetches the authoritative entry via the normal merge
                // path so expiry is preserved end-to-end.
                await ForwardLocalWriteToShadowIfNeededAsync(key, value, expiresAtTicks);
                await forwardTask;
                return;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    public async Task<byte[]?> GetOrSetAsync(string key, byte[] value)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                // Shadow-forward the same semantic operation so the destination tree
                // observes GetOrSet semantics too. LWW on the destination absorbs
                // the interleaving between drain reads and this forward.
                var forwardTask = TrackShadowForward((key, value), static (t, s) => t.GetOrSetAsync(s.key, s.value));
                var result = await TraverseForGetOrSetAsync(key, value);

                // If the key was already live, no write occurred - return existing value.
                if (result.ExistingValue is not null)
                {
                    await forwardTask;
                    return result.ExistingValue;
                }

                // A write occurred - propagate any split.
                var splitResult = result.Split;
                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                // shadow-forward the write to the split target if applicable.
                await ForwardLocalWriteToShadowIfNeededAsync(key, value);
                await forwardTask;
                return null;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                // The failed grain will be deactivated by Orleans. On retry, a fresh
                // activation loads clean state and the recovery guards resume any
                // interrupted split.
            }
        }
    }

    public async Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                // Forward the unconditional Set on success. Rationale: SetIfVersion's
                // precondition is on the source's HLC; the destination holds its own
                // drained copy plus any preceding forwards. Issuing an unconditional
                // Set on the destination ensures the same post-condition value
                // lands there regardless of the destination's own version state,
                // with LWW resolving ordering against concurrent forwards. We only
                // forward once the local CAS has succeeded so a rejected CAS does
                // not leak a phantom value onto the destination.
                var result = await TraverseForSetIfVersionAsync(key, value, expectedVersion);

                if (!result.Success)
                {
                    return false;
                }

                // A write occurred - propagate any split.
                var splitResult = result.Split;
                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                // shadow-forward the write to the split target if applicable.
                await ForwardLocalWriteToShadowIfNeededAsync(key, value);
                await TrackShadowForward((key, value), static (t, s) => t.SetAsync(s.key, s.value));
                return true;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    public Task<HybridLogicalClock> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes) =>
        ApplyCrdtDeltaAsync(key, mode, deltaBytes, expiresAtTicks: 0);

    public async Task<HybridLogicalClock> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes, long expiresAtTicks)
    {
        EnsureInternalOrigin(LatticeOperation.CrdtApply);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(deltaBytes);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var result = await TraverseForCrdtApplyAsync(key, mode, deltaBytes, expiresAtTicks);

                var splitResult = result.Split;
                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                return result.Version;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        // Reject-check up-front so the batch fails fast rather than partially applying.
        ThrowIfRejectedForAnyKey(entries.Select(e => e.Key));
        RecordWrite();

        if (entries.Count == 0) return;

        // Online-resize shadow-forward: forward the whole batch once in parallel
        // with the local apply. Without batched forward, a single SetManyAsync
        // of N entries would pay N sequential shadow-forward RTTs. Mirrors
        // MergeManyAsync's pattern. LWW on the destination absorbs any
        // interleaving with the drain reader.
        var forwardTask = TrackShadowForward(entries, static (t, s) => t.SetManyAsync(s));

        // Preserve the local exception as the primary diagnostic. The
        // older shape (try { local } finally { await forwardTask; }) would
        // replace a local failure with a forward-path failure if both
        // happened to fail on the same call. The tracker's fault-logger
        // continuation already observes any forward fault asynchronously,
        // so when the local loop throws we rethrow its exception and let
        // the continuation log the forward side separately.
        System.Runtime.ExceptionServices.ExceptionDispatchInfo? localFailure = null;
        var localApplyTs = Stopwatch.GetTimestamp();
        try
        {
            await SetManyLocalOnlyAsync(entries);
        }
        catch (Exception ex)
        {
            localFailure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex);
        }
        LatticeMetrics.ShardRootSetManyLocalApplyDuration.Record(
            Stopwatch.GetElapsedTime(localApplyTs).TotalMilliseconds,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));

        if (localFailure is null)
        {
            // Local succeeded - surface any forward failure to the caller.
            var forwardTs = Stopwatch.GetTimestamp();
            try
            {
                await forwardTask;
            }
            finally
            {
                LatticeMetrics.ShardRootSetManyShadowForwardDuration.Record(
                    Stopwatch.GetElapsedTime(forwardTs).TotalMilliseconds,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
            }
            return;
        }

        // Local failed - the continuation will observe / log any forward fault.
        localFailure.Throw();
    }

    /// <summary>
    /// Local apply path for <see cref="SetManyAsync"/>. Routes each input
    /// entry to its target leaf, groups the input into per-leaf slices,
    /// and dispatches one <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/> call
    /// per leaf so the batched commit-log seam
    /// (<see cref="Orleans.Lattice.BPlusTree.Grains.ICommitLogWriter.AppendManyAsync"/>) collapses the
    /// per-key WAL grain hops into a single batched dispatch per leaf.
    /// The pre-batched shape called <c>leaf.SetAsync(key, value)</c> once
    /// per entry, which routed through the per-key WAL append path and
    /// paid one WAL round-trip per key - the exact regression that
    /// suppressed the batched-WAL-append throughput win on the
    /// foreground bulk-write path.
    /// <c>TraverseForWriteAsync</c> shape: the parent path captured on
    /// the first key of each leaf bucket is walked back up if the leaf
    /// returns a non-null <see cref="SplitResult"/>, and any residual
    /// split at the top of the path is promoted to a new root via
    /// <see cref="PromoteRootAsync"/>.
    /// <para>
    /// Per-leaf dispatch runs in parallel via <see cref="Task.WhenAll{TResult}(Task{TResult}[])"/>
    /// (mirroring the per-shard fan-out <c>LatticeGrain.SetManyAsyncCore</c>
    /// already does across shards). The shard-root grain is
    /// single-activation so the parallel awaits all resume on the same
    /// grain turn; the leaf-grain and routing caches are only mutated
    /// in the sequential resolve and split-promotion passes bracketing
    /// the <c>Task.WhenAll</c>. Split promotion is walked sequentially
    /// per leaf because <see cref="IBPlusInternalGrain.AcceptSplitAsync"/>
    /// and <see cref="PromoteRootAsync"/> mutate shared routing tables
    /// and this shard's root respectively.
    /// </para>
    /// <para>
    /// After the batched leaf apply completes, every input key is fed
    /// through <see cref="ForwardLocalWriteToShadowIfNeededAsync"/>. This
    /// is the per-key adaptive-split shadow-forward path (distinct from
    /// the online-resize <c>TrackShadowForward</c> the caller dispatches
    /// in <see cref="SetManyAsync"/>) and is required so writes for slots
    /// that have already moved to a sibling shard reach the destination.
    /// The single-key <c>SetAsync</c> path runs the same per-key forward
    /// inside its retry envelope; the batched path mirrors it once per
    /// input key, post-apply, so semantics are unchanged.
    /// </para>
    /// </summary>
    private async Task SetManyLocalOnlyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        // Flat-tree shortcut: every entry routes to the root leaf, so
        // the whole batch lands in one leaf.SetManyAsync call. Guarded by
        // node type so a corrupt RootIsLeaf flag over an internal root
        // (issue 899) falls through to the routed non-flat path instead of
        // blind-casting the internal root to a leaf.
        if (state.State.RootIsLeaf && IsLeafGrainId(state.State.RootNodeId!.Value))
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var leaf = ResolveLeafGrain(rootLeafId);
            await RecordAffectedLeafIfPreparedAsync(rootLeafId);
            var split = await DispatchLeafBatchWithRetryAsync(leaf, entries);
            while (split is not null)
            {
                split = await PromoteRootAsync(split);
            }
            await ForwardLocalWritesToShadowIfNeededAsync(entries);
            return;
        }

        // Non-flat tree: group entries by routed leaf. Capture the
        // root-to-immediate-parent path the first time each leaf is
        // seen so split promotion has the same shape as the
        // single-key TraverseForWriteAsync path-pop loop.
        var buckets = new Dictionary<GrainId, LeafBucket>(capacity: 4);
        foreach (var entry in entries)
        {
            var leafId = await TraverseToLeafAsync(entry.Key);
            if (!IsLeafGrainId(leafId))
            {
                leafId = await DescendToLeafForKeyAsync(leafId, entry.Key);
            }
            if (!buckets.TryGetValue(leafId, out var bucket))
            {
                var parents = await CaptureLeafParentPathAsync(entry.Key);
                bucket = new LeafBucket(new List<KeyValuePair<string, byte[]>>(), parents);
                buckets[leafId] = bucket;
            }
            bucket.Slice.Add(entry);
        }

        // Resolve each leaf grain reference and (when the saga's prepared
        // context is active) register the affected leaf with the per-tree
        // tx registry. Done in one sequential pass so the per-tx dedup
        // gate inside RecordAffectedLeafIfPreparedAsync is observed in
        // deterministic order before any concurrent leaf dispatch begins.
        // The bucket order is also frozen here so the parallel-dispatch
        // index and the split-promotion index agree on which parent path
        // belongs to which leaf result.
        var orderedBuckets = new List<(GrainId LeafId, LeafBucket Bucket)>(buckets.Count);
        foreach (var (leafId, bucket) in buckets)
        {
            bucket.Leaf = ResolveLeafGrain(leafId);
            await RecordAffectedLeafIfPreparedAsync(leafId);
            orderedBuckets.Add((leafId, bucket));
        }

        // Dispatch the per-leaf batched RPCs in parallel. SetManyAsync is
        // marked [AlwaysInterleave], so this shard-root activation can
        // have multiple in-flight turns concurrently; the per-activation
        // grain-reference caches are ConcurrentDictionary instances so
        // those interleaved turns cannot corrupt them. Within a single
        // turn, the cache is only written by the sequential resolve loop
        // above and consulted (read-only) by the split-promotion loop
        // below.
        // The original shape awaited each leaf sequentially, which
        // collapsed N leaves of useful concurrency per shard turn into
        // one. Provider commit p50 ~16 ms means that with B buckets
        // per shard, the wall-clock cost drops from B x commit_p50 to
        // ~commit_p50, multiplying effective shard-root throughput by
        // B on the bulk-write path.
        var leafDispatchTasks = new Task<SplitResult?>[orderedBuckets.Count];
        for (int i = 0; i < orderedBuckets.Count; i++)
        {
            var bucket = orderedBuckets[i].Bucket;
            leafDispatchTasks[i] = DispatchLeafBatchWithRetryAsync(bucket.Leaf!, bucket.Slice);
        }
        var splitResults = await Task.WhenAll(leafDispatchTasks);

        // Walk split promotion sequentially per leaf. Parent
        // AcceptSplitAsync calls mutate shared internal-node routing
        // tables (and PromoteRootAsync rewrites this shard's root),
        // both of which must be serialised. Each leaf's parent path
        // was captured before dispatch, so the order is well-defined.
        for (int i = 0; i < orderedBuckets.Count; i++)
        {
            var split = splitResults[i];
            if (split is null) continue;

            var parents = orderedBuckets[i].Bucket.Parents;
            var parentCursor = parents.Count;
            while (split is not null && parentCursor > 0)
            {
                var parentId = parents[--parentCursor];
                var parentGrain = ResolveInternalGrain(parentId);
                split = await parentGrain.AcceptSplitAsync(split.PromotedKey, split.NewSiblingId);
                InvalidateRoutingTable(parentId);
            }

            // Any residual split at the root must be promoted into a
            // new root grain - matches the single-key path's
            // post-loop PromoteRootAsync.
            while (split is not null)
            {
                split = await PromoteRootAsync(split);
            }
        }

        await ForwardLocalWritesToShadowIfNeededAsync(entries);
    }

    /// <summary>
    /// Mutable per-leaf record used by <see cref="SetManyLocalOnlyAsync"/>
    /// to group routed entries, the captured parent path for split
    /// promotion, and the resolved leaf grain reference. Reference type
    /// (not <c>struct</c>) because <see cref="Leaf"/> is filled in by a
    /// second pass after the dictionary is populated, and a value-type
    /// dictionary value would require a get-modify-set update per entry.
    /// </summary>
    private sealed class LeafBucket(List<KeyValuePair<string, byte[]>> slice, List<GrainId> parents)
    {
        public List<KeyValuePair<string, byte[]>> Slice { get; } = slice;
        public List<GrainId> Parents { get; } = parents;
        public IBPlusLeafGrain? Leaf { get; set; }
    }

    /// <summary>
    /// Per-key adaptive-split shadow forward applied to every entry after
    /// the batched local leaf apply succeeds. Each call is a cheap no-op
    /// when no adaptive split is in progress or when the key's virtual
    /// slot has not moved, so the steady-state cost is dictionary lookups
    /// only. During an active split the call forwards the per-key write
    /// to the post-split owner shard, preserving the same per-entry
    /// semantics the single-key <c>SetAsync</c> path provides.
    /// </summary>
    private Task ForwardLocalWritesToShadowIfNeededAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        // Steady-state fast path: a per-entry forward only ever has work to do
        // when an adaptive split is in progress (SplitInProgress non-null) or
        // when this shard has already moved slots away (MovedAwaySlots
        // populated). When neither holds - the dominant single-silo,
        // no-resize case - every per-entry ForwardLocalWriteToShadowIfNeededAsync
        // resolves to a null target and returns synchronously, so the whole
        // batch loop is dead work. Skip it entirely (and its async state
        // machine) by returning a completed task. The steady-state caller
        // already resumed synchronously here, so this preserves the existing
        // yield behaviour while eliminating the per-batch async machinery.
        if (state.State.SplitInProgress is null && state.State.MovedAwaySlots.Count == 0)
        {
            return Task.CompletedTask;
        }

        return ForwardLocalWritesToShadowSlowAsync(entries);
    }

    private async Task ForwardLocalWritesToShadowSlowAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        foreach (var entry in entries)
        {
            await ForwardLocalWriteToShadowIfNeededAsync(entry.Key, entry.Value);
        }
    }

    /// <summary>
    /// Dispatches a single per-leaf batched <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyAsync"/>
    /// call with the same transient-exception retry envelope the per-key
    /// <c>SetAsync</c> path uses. The retry is idempotent under LWW: each
    /// retry advances the leaf's HLC, but the dominant per-key value the
    /// leaf would have committed on a successful first try is the same
    /// value the retry commits, so a partial-leaf-failure recovery
    /// converges to the same projection state.
    /// </summary>
    private async Task<SplitResult?> DispatchLeafBatchWithRetryAsync(
        IBPlusLeafGrain leaf,
        List<KeyValuePair<string, byte[]>> slice)
    {
        for (int attempt = 0; ; attempt++)
        {
            var rpcTs = Stopwatch.GetTimestamp();
            try
            {
                var result = await leaf.SetManyAsync(slice);
                LatticeMetrics.ShardRootSetManyLeafRpcDuration.Record(
                    Stopwatch.GetElapsedTime(rpcTs).TotalMilliseconds,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
                return result;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                LatticeMetrics.ShardRootSetManyLeafRpcDuration.Record(
                    Stopwatch.GetElapsedTime(rpcTs).TotalMilliseconds,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
            }
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> SetManyWherePredicateAsync(
        List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        ArgumentNullException.ThrowIfNull(entries);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        ThrowIfRejectedForAnyKey(entries.Select(e => e.Key));
        RecordWrite();

        if (entries.Count == 0) return Array.Empty<string>();

        // Online-resize shadow-forward of the whole conditional batch in
        // parallel with the local apply. The destination shard re-evaluates
        // the guard against its own copy; LWW reconciles any interleaving with
        // the drain reader. The forwarded written set is discarded - this
        // shard's local apply is authoritative for the returned set.
        var forwardTask = TrackShadowForward(
            (entries, predicate),
            static (t, s) => t.SetManyWherePredicateAsync(s.entries, s.predicate));

        System.Runtime.ExceptionServices.ExceptionDispatchInfo? localFailure = null;
        IReadOnlyList<string> written = Array.Empty<string>();
        var localApplyTs = Stopwatch.GetTimestamp();
        try
        {
            written = await SetManyWhereLocalOnlyAsync(entries, predicate);
        }
        catch (Exception ex)
        {
            localFailure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex);
        }
        LatticeMetrics.ShardRootSetManyLocalApplyDuration.Record(
            Stopwatch.GetElapsedTime(localApplyTs).TotalMilliseconds,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));

        if (localFailure is null)
        {
            var forwardTs = Stopwatch.GetTimestamp();
            try
            {
                await forwardTask;
            }
            finally
            {
                LatticeMetrics.ShardRootSetManyShadowForwardDuration.Record(
                    Stopwatch.GetElapsedTime(forwardTs).TotalMilliseconds,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
            }
            return written;
        }

        localFailure.Throw();
        return written; // unreachable - Throw() always throws.
    }

    /// <summary>
    /// Conditional sibling of <see cref="SetManyLocalOnlyAsync"/>: routes each
    /// entry to its owning leaf, dispatches one
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyWherePredicateAsync"/> per leaf, walks
    /// the resulting split promotions, and forwards only the actually-written
    /// entries (those that passed the guard) through the per-key adaptive-split
    /// shadow path. Returns the aggregated written-key set across this shard's
    /// leaves.
    /// </summary>
    private async Task<IReadOnlyList<string>> SetManyWhereLocalOnlyAsync(
        List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate)
    {
        // Flat-tree shortcut: every entry routes to the root leaf. Guarded by
        // node type so a corrupt RootIsLeaf flag over an internal root
        // (issue 899) falls through to the routed non-flat path instead of
        // blind-casting the internal root to a leaf.
        if (state.State.RootIsLeaf && IsLeafGrainId(state.State.RootNodeId!.Value))
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var leaf = ResolveLeafGrain(rootLeafId);
            await RecordAffectedLeafIfPreparedAsync(rootLeafId);
            var result = await DispatchConditionalLeafBatchWithRetryAsync(leaf, entries, predicate);
            var split = result.Split;
            while (split is not null)
            {
                split = await PromoteRootAsync(split);
            }
            await ForwardWrittenEntriesToShadowIfNeededAsync(entries, result.WrittenKeys);
            return result.WrittenKeys;
        }

        // Non-flat tree: group entries by routed leaf, capturing each leaf's
        // parent path for split promotion (mirrors SetManyLocalOnlyAsync).
        var buckets = new Dictionary<GrainId, LeafBucket>(capacity: 4);
        foreach (var entry in entries)
        {
            var leafId = await TraverseToLeafAsync(entry.Key);
            if (!IsLeafGrainId(leafId))
            {
                leafId = await DescendToLeafForKeyAsync(leafId, entry.Key);
            }
            if (!buckets.TryGetValue(leafId, out var bucket))
            {
                var parents = await CaptureLeafParentPathAsync(entry.Key);
                bucket = new LeafBucket(new List<KeyValuePair<string, byte[]>>(), parents);
                buckets[leafId] = bucket;
            }
            bucket.Slice.Add(entry);
        }

        var orderedBuckets = new List<(GrainId LeafId, LeafBucket Bucket)>(buckets.Count);
        foreach (var (leafId, bucket) in buckets)
        {
            bucket.Leaf = ResolveLeafGrain(leafId);
            await RecordAffectedLeafIfPreparedAsync(leafId);
            orderedBuckets.Add((leafId, bucket));
        }

        var leafDispatchTasks = new Task<ConditionalSetManyResult>[orderedBuckets.Count];
        for (int i = 0; i < orderedBuckets.Count; i++)
        {
            var bucket = orderedBuckets[i].Bucket;
            leafDispatchTasks[i] = DispatchConditionalLeafBatchWithRetryAsync(bucket.Leaf!, bucket.Slice, predicate);
        }
        var leafResults = await Task.WhenAll(leafDispatchTasks);

        // Walk split promotion sequentially per leaf (parent routing tables
        // and the shard root must be mutated serially).
        for (int i = 0; i < orderedBuckets.Count; i++)
        {
            var split = leafResults[i].Split;
            if (split is null) continue;

            var parents = orderedBuckets[i].Bucket.Parents;
            var parentCursor = parents.Count;
            while (split is not null && parentCursor > 0)
            {
                var parentId = parents[--parentCursor];
                var parentGrain = ResolveInternalGrain(parentId);
                split = await parentGrain.AcceptSplitAsync(split.PromotedKey, split.NewSiblingId);
                InvalidateRoutingTable(parentId);
            }

            while (split is not null)
            {
                split = await PromoteRootAsync(split);
            }
        }

        // Forward only the written entries per leaf, and aggregate the written
        // set. Each leaf's WrittenKeys is an in-order subsequence of its slice,
        // so the forward uses an alloc-free two-pointer walk.
        var totalWritten = 0;
        for (int i = 0; i < leafResults.Length; i++)
            totalWritten += leafResults[i].WrittenKeys.Count;

        if (totalWritten == 0)
            return Array.Empty<string>();

        var aggregated = new List<string>(totalWritten);
        for (int i = 0; i < orderedBuckets.Count; i++)
        {
            var slice = orderedBuckets[i].Bucket.Slice;
            var writtenKeys = leafResults[i].WrittenKeys;
            await ForwardWrittenEntriesToShadowIfNeededAsync(slice, writtenKeys);
            for (int k = 0; k < writtenKeys.Count; k++)
                aggregated.Add(writtenKeys[k]);
        }
        return aggregated;
    }

    /// <summary>
    /// Forwards only the written entries of <paramref name="slice"/> through
    /// the per-key adaptive-split shadow path. <paramref name="writtenKeys"/>
    /// is an in-order subsequence of <paramref name="slice"/>'s keys (the leaf
    /// appends matches while iterating the slice in order), so a two-pointer
    /// walk pairs each written key with its value without an intermediate set.
    /// </summary>
    private async Task ForwardWrittenEntriesToShadowIfNeededAsync(
        List<KeyValuePair<string, byte[]>> slice, IReadOnlyList<string> writtenKeys)
    {
        if (writtenKeys.Count == 0) return;
        var w = 0;
        for (int i = 0; i < slice.Count && w < writtenKeys.Count; i++)
        {
            if (string.Equals(slice[i].Key, writtenKeys[w], StringComparison.Ordinal))
            {
                await ForwardLocalWriteToShadowIfNeededAsync(slice[i].Key, slice[i].Value);
                w++;
            }
        }
    }

    /// <summary>
    /// Conditional sibling of <see cref="DispatchLeafBatchWithRetryAsync"/>:
    /// dispatches a single per-leaf
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.SetManyWherePredicateAsync"/> call under the
    /// same transient-exception retry envelope. Idempotent under LWW: a retry
    /// re-evaluates the guard against the same committed values and converges
    /// to the same projection state.
    /// </summary>
    private async Task<ConditionalSetManyResult> DispatchConditionalLeafBatchWithRetryAsync(
        IBPlusLeafGrain leaf,
        List<KeyValuePair<string, byte[]>> slice,
        LatticePredicateNode predicate)
    {
        for (int attempt = 0; ; attempt++)
        {
            var rpcTs = Stopwatch.GetTimestamp();
            try
            {
                var result = await leaf.SetManyWherePredicateAsync(slice, predicate);
                LatticeMetrics.ShardRootSetManyLeafRpcDuration.Record(
                    Stopwatch.GetElapsedTime(rpcTs).TotalMilliseconds,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
                return result;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                LatticeMetrics.ShardRootSetManyLeafRpcDuration.Record(
                    Stopwatch.GetElapsedTime(rpcTs).TotalMilliseconds,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
            }
        }
    }

    /// <summary>
    /// Walks root -> ... -> immediate-parent for <paramref name="key"/>,
    /// returning the list of internal ancestors in root-to-immediate-parent
    /// order. Excludes the leaf itself. Reuses the same cached routing
    /// snapshots <c>TraverseToLeafAsync</c> does, so steady-state cost is
    /// dominated by dictionary lookups - no additional grain RPCs in the
    /// hot path.
    /// </summary>
    private async Task<List<GrainId>> CaptureLeafParentPathAsync(string key)
    {
        var parents = new List<GrainId>(capacity: 4);
        var currentId = state.State.RootNodeId!.Value;

        // Flag-trusting walk that records each ancestor. Skipped for a
        // single-leaf tree; the type-extension below corrects a corrupt
        // RootIsLeaf / ChildrenAreLeaves flag that stopped the walk on an
        // internal node (issue 899) so the captured path still reaches the
        // real leaf's parent. For a healthy tree (or a non-runtime test
        // factory where IsLeafGrainId is always true) the extension loop never
        // runs and the captured path is identical to the pre-guard walk.
        if (!state.State.RootIsLeaf)
        {
            while (true)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var (childId, childrenAreLeaves) = snapshot.Route(key);
                parents.Add(currentId);
                currentId = childId;
                if (childrenAreLeaves)
                {
                    break;
                }
            }
        }

        while (!IsLeafGrainId(currentId))
        {
            parents.Add(currentId);
            var snapshot = await GetRoutingTableSnapshotAsync(currentId);
            var (childId, _) = snapshot.Route(key);
            currentId = childId;
        }

        return parents;
    }

    public async Task<bool> DeleteAsync(string key)
    {
        EnsureInternalOrigin(LatticeOperation.Delete);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        ThrowIfRejectedForKey(key);
        RecordWrite();

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                // For online resize, tombstones MUST be forwarded - the destination
                // tree becomes authoritative at swap, so a tombstone that never
                // reached T' would leave the key alive post-swap. LWW on the
                // destination resolves any interleaving with the drain reader.
                // This differs from the adaptive-split path, where post-swap
                // cleanup restores convergence within one tree.
                var forwardTask = TrackShadowForward(key, static (t, s) => t.DeleteAsync(s));

                bool result;
                GrainId leafId;
                if (state.State.RootIsLeaf && IsLeafGrainId(state.State.RootNodeId!.Value))
                {
                    leafId = state.State.RootNodeId!.Value;
                }
                else
                {
                    // Traverse to the leaf.
                    leafId = await TraverseToLeafAsync(key);
                    // Guard: route to a real leaf even if a corrupt RootIsLeaf /
                    // ChildrenAreLeaves flag resolved an internal node (issue 899).
                    if (!IsLeafGrainId(leafId))
                    {
                        leafId = await DescendToLeafForKeyAsync(leafId, key);
                    }
                }

                var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
                await RecordAffectedLeafIfPreparedAsync(leafId);
                await MarkLeafDirtyAsync(leafId);
                result = await leafGrain.DeleteAsync(key);

                // Shadow-forward the prepared tombstone to the split
                // destination and install the destination-side shadow marker
                // (issue 1117 - the delete flavour of the PR 1115 write fix).
                // Non-prepared deletes remain deferred to the split
                // coordinator's cleanup phase; only a saga prepare needs the
                // marker to close the mid-saga atomic-visibility torn read.
                await ForwardLocalDeleteToShadowIfNeededAsync(key);
                await forwardTask;
                return result;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
                // Retry - same rationale as SetAsync.
            }
        }
    }

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive, LatticePredicateNode? predicate = null)
    {
        EnsureInternalOrigin(LatticeOperation.RangeDelete);
        ThrowIfShuttingDown();
        await PrepareForOperationAsync();
        // range deletes do not currently shadow-forward tombstones - see
        // ForwardLocalWriteToShadowIfNeededAsync XML doc. The cleanup phase of
        // the split coordinator restores convergence by re-tombstoning moved-slot entries on T after the swap. No explicit reject check is performed
        // here because the LatticeGrain has already routed the range delete
        // to the correct shard via the current ShardMap.
        RecordWrite();

        // For online resize, forward the same range delete to the destination
        // in parallel - LWW on the destination shard absorbs any interleaving
        // with drain and live forwards. The predicate rides along so the
        // destination filters identically.
        var forwardTask = TrackShadowForward((startInclusive, endExclusive, predicate), static (t, s) => t.DeleteRangeAsync(s.startInclusive, s.endExclusive, s.predicate));

        // Find the starting leaf for the range.
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else
        {
            leafId = await TraverseToLeafAsync(startInclusive);
        }

        // Guard: route to a real leaf even if a corrupt RootIsLeaf /
        // ChildrenAreLeaves flag resolved an internal node (issue 899).
        if (!IsLeafGrainId(leafId))
        {
            leafId = await DescendToLeafForKeyAsync(leafId, startInclusive);
        }

        // Walk the leaf chain, tombstoning matching entries in each leaf.
        // Terminate on the first leaf that reports PastRange=true:
        // deleting zero is NOT a valid termination signal on multi-shard trees,
        // where early leaves can be sparse yet later leaves contain range-matching
        // entries.
        //
        // For a predicate-filtered delete, accumulate every leaf's matched
        // key set so the single per-shard DeleteRange notification carries the
        // exact tombstone closure - replication apply then reproduces it
        // without re-evaluating the predicate.
        var totalDeleted = 0;
        List<string>? matchedKeys = predicate is null ? null : [];
        while (true)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var result = await leafGrain.DeleteRangeAsync(startInclusive, endExclusive, predicate);
            totalDeleted += result.Deleted;
            if (result.Deleted > 0)
                await MarkLeafDirtyAsync(leafId);
            if (matchedKeys is not null && result.MatchedKeys is { Count: > 0 })
                matchedKeys.AddRange(result.MatchedKeys);

            if (result.PastRange)
                break;

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                break;

            leafId = nextSibling.Value;
        }

        await forwardTask;
        await PublishDeleteRangeAsync(startInclusive, endExclusive, matchedKeys);
        return totalDeleted;
    }

    public Task<int> CountAsync() => CountAsync(null, null);

    public async Task<int> CountAsync(string? startInclusive, string? endExclusive)
    {
        await PrepareForOperationAsync();
        RecordRead();

        if (state.State.RootNodeId is null)
            return 0;

        // Find the leftmost leaf and walk the chain.
        var leafId = await GetLeftmostLeafIdAsync();
        if (leafId is null) return 0;

        // if any virtual slots have been split away, we cannot trust
        // the leaf-level count (it includes orphan moved-slot entries). Walk
        // the keys and filter. The fast path (no splits) is preserved when
        // MovedAwaySlots is empty. The [startInclusive, endExclusive) bounds
        // are pushed all the way to the leaf so counting stays server-side:
        // a fully-covered leaf returns its full count and a boundary leaf
        // returns only the in-range subset, never materialising keys across
        // the lattice->caller wire.
        var hasMovedAway = state.State.MovedAwaySlots.Count > 0
            && state.State.MovedAwayVirtualShardCount is not null;

        var total = 0;
        var currentId = leafId.Value;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            if (hasMovedAway)
            {
                var keys = await leaf.GetKeysAsync(startInclusive, endExclusive);
                for (int i = 0; i < keys.Count; i++)
                {
                    if (!IsSlotMovedAway(keys[i])) total++;
                }
            }
            else
            {
                total += await leaf.CountAsync(startInclusive, endExclusive);
            }

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return total;
    }

    /// <inheritdoc />
    public async Task<ShardCountResult> CountWithMovedAwayAsync()
    {
        await PrepareForOperationAsync();
        RecordRead();

        if (state.State.RootNodeId is null)
            return new ShardCountResult { Count = 0 };

        var leafId = await GetLeftmostLeafIdAsync();
        if (leafId is null) return new ShardCountResult { Count = 0 };

        var hasActiveSplit = state.State.SplitInProgress is { } sip
            && (sip.Phase == ShardSplitPhase.Swap
                || sip.Phase == ShardSplitPhase.Reject
                || sip.Phase == ShardSplitPhase.Complete);
        var hasMovedAway = state.State.MovedAwaySlots.Count > 0
            && state.State.MovedAwayVirtualShardCount is not null;

        var total = 0;
        HashSet<int>? movedSet = null;
        var currentId = leafId.Value;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            if (hasActiveSplit || hasMovedAway)
            {
                var keys = await leaf.GetKeysAsync(null, null);
                for (int i = 0; i < keys.Count; i++)
                {
                    if (TryGetMovedAwaySlot(keys[i], out var movedSlot))
                    {
                        (movedSet ??= []).Add(movedSlot);
                        continue;
                    }
                    total++;
                }
            }
            else
            {
                total += await leaf.CountAsync();
            }

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return new ShardCountResult
        {
            Count = total,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    /// <inheritdoc />
    public Task<int> CountForSlotsAsync(int[] sortedSlots, int virtualShardCount) =>
        CountForSlotsAsync(sortedSlots, virtualShardCount, null, null);

    /// <inheritdoc />
    public async Task<int> CountForSlotsAsync(int[] sortedSlots, int virtualShardCount, string? startInclusive, string? endExclusive)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        await PrepareForOperationAsync();
        RecordRead();

        if (sortedSlots.Length == 0 || state.State.RootNodeId is null)
            return 0;

        var leafId = await GetLeftmostLeafIdAsync();
        if (leafId is null) return 0;

        var total = 0;
        var currentId = leafId.Value;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(currentId);
            // Push the [startInclusive, endExclusive) bound to the leaf so it
            // returns only the in-range keys; the per-slot ownership filter is
            // then applied to that already-bounded set. Both constraints must
            // hold for a key to count, keeping the post-split ranged count
            // exact against the authoritative ShardMap.
            var keys = await leaf.GetKeysAsync(startInclusive, endExclusive);
            for (int i = 0; i < keys.Count; i++)
            {
                var slot = ShardMap.GetVirtualSlot(keys[i], virtualShardCount);
                if (Array.BinarySearch(sortedSlots, slot) >= 0)
                    total++;
            }

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            currentId = next.Value;
        }

        return total;
    }

    private static int[] SortedSlotsArray(HashSet<int> set)
    {
        var arr = new int[set.Count];
        var i = 0;
        foreach (var v in set) arr[i++] = v;
        Array.Sort(arr);
        return arr;
    }

    private async Task EnsureRootAsync()
    {
        // Steady-state fast path: once a root exists, short-circuit with
        // zero storage I/O and no gate acquisition. This is the only path
        // every hot-path read/write pays after the shard is initialised.
        if (state.State.RootNodeId is not null) return;

        // Slow path: this activation believes the shard is brand new.
        // Serialise the re-read + seed behind the init gate so two
        // interleaved turns cannot both seed a single-leaf root.
        await _ensureRootGate.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            await EnsureRootSlowWithDeadlineAsync();
        }
        finally
        {
            _ensureRootGate.Release();
        }
    }

    /// <summary>
    /// Bounds the one-time activation-readiness seed
    /// (<see cref="EnsureRootSlowAsync"/>) with the per-tree
    /// <see cref="LatticeOptions.ActivationReadyTimeout"/> deadline.
    /// <para>
    /// The seed runs a chain of cross-grain awaits the first time a
    /// brand-new or freshly-reactivated shard prepares for an operation:
    /// the defensive <c>state.ReadStateAsync</c> re-read, the tree-registry
    /// <c>RegisterAsync</c>, the deterministic root-leaf init pair, and the
    /// initial <c>WriteShardStateAsync</c>. During a startup reshard or a
    /// membership change Orleans can reject or park one of those messages
    /// (the target registry / leaf activation is not yet visible) and leave
    /// the caller-side <c>await</c> neither completing nor faulting. Because
    /// this seed runs while <c>_ensureRootGate</c> is held, a parked seed
    /// pins the gate, every interleaved read/write turn on the activation
    /// stalls behind it, the lattice grain's per-shard fan-out saturates at
    /// its in-flight limit, and the whole write pipeline wedges with no
    /// fault and no activation recycle until the caller-side Orleans
    /// response deadline (default 3 minutes) expires. The deadline abandons
    /// the parked seed (its eventual completion is harmlessly unobserved)
    /// and faults the turn with a <see cref="TimeoutException"/>, which the
    /// existing transient-exception retry envelope on every mutation path
    /// catches and re-runs against refreshed routing / registration once
    /// the startup reshard has settled.
    /// </para>
    /// <para>
    /// Abandoning a parked seed never loses data or double-registers: every
    /// cross-grain step in <see cref="EnsureRootSlowAsync"/> is idempotent
    /// on retry (the registry registration by contract; the leaf-init pair
    /// by its own cycle-1 guard; the shard-state write by the
    /// re-read-and-recheck at the top of the slow path), and a failed
    /// <c>WriteShardStateAsync</c> reverts the in-memory seed so a retry
    /// re-runs cleanly.
    /// </para>
    /// <para>
    /// When the configured timeout is <see cref="Timeout.InfiniteTimeSpan"/>
    /// the seed is awaited unbounded, restoring the historical behaviour.
    /// </para>
    /// </summary>
    private async Task EnsureRootSlowWithDeadlineAsync()
    {
        var timeout = (await GetOptionsAsync()).ActivationReadyTimeout;
        if (timeout == Timeout.InfiniteTimeSpan)
        {
            await EnsureRootSlowAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            return;
        }

        using var deadline = new CancellationTokenSource(timeout);
        try
        {
            await EnsureRootSlowAsync().WaitAsync(deadline.Token)
                .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        }
        catch (OperationCanceledException oce) when (deadline.IsCancellationRequested)
        {
            LatticeMetrics.ActivationReadyTimeouts.Add(
                1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId));
            throw new ShardActivationTimeoutException(
                $"Activation-readiness seed for shard {MyShardIndex} of tree '{TreeId}' "
                + $"exceeded the {timeout} seed deadline "
                + $"({nameof(LatticeOptions.ActivationReadyTimeout)}); a registry or "
                + "root-leaf RPC is likely parked because the target activation is not "
                + "yet visible during a startup reshard or membership change. The seed is "
                + "abandoned and the operation will be retried against refreshed routing.", oce)
            {
                TreeId = TreeId ?? string.Empty,
                ShardIndex = MyShardIndex,
                TimeoutSeconds = timeout.TotalSeconds,
            };
        }
    }

    private async Task EnsureRootSlowAsync()
    {
        // Re-check under the gate: a turn that lost the race to the gate
        // observes the winner's published RootNodeId and returns without
        // seeding.
        if (state.State.RootNodeId is not null) return;

        // Defensive re-read before seeding a fresh single-leaf root.
        //
        // We only reach here when this activation believes the shard is
        // brand new (RootNodeId is null in memory). That belief can be
        // WRONG on a reactivation that raced a concurrent write or
        // activated against not-yet-visible state during a membership
        // change / silo restart: storage already holds a live topology
        // (a promoted internal root and a populated leaf chain) for this
        // shard while the freshly-activated grain's in-memory copy is
        // still empty. Seeding a single-leaf root in that window would
        // overwrite the persisted topology and silently drop every key
        // that lived under the rest of the tree. Re-read once from
        // storage and re-check; if storage already has a root we adopt it
        // and return without seeding. The re-read only ever ADDS
        // information here - a newer in-memory write would already have
        // set RootNodeId and tripped the fast-path guard above - so it
        // cannot clobber a pending in-memory mutation.
        await state.ReadStateAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        if (state.State.RootNodeId is not null) return;

        // Register the tree in the registry before creating the root node.
        // This ensures the tree is discoverable before any data is written.
        // System trees (e.g. the registry itself) skip self-registration.
        var prevIsRegistered = state.State.IsRegistered;
        if (!state.State.IsRegistered &&
            !TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.RegisterAsync(TreeId);
            state.State.IsRegistered = true;
        }

        // Use a deterministic GrainId derived from this shard's own identity
        // so that a crash-retry reuses the same leaf instead of creating an orphan.
        var shardKey = context.GrainId.Key.ToString()!;
        var deterministicId = DeterministicGuid(shardKey);
        var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(deterministicId);
        await leafGrain.SetTreeIdAsync(TreeId);
        await leafGrain.SetShardIndexAsync(MyShardIndex);
        var prevRootNodeId = state.State.RootNodeId;
        var prevRootIsLeaf = state.State.RootIsLeaf;
        state.State.RootNodeId = leafGrain.GetGrainId();
        state.State.RootIsLeaf = true;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            // Class B revert: a thrown WriteStateAsync leaves the
            // in-memory IsRegistered / RootNodeId / RootIsLeaf set
            // while storage stays at the pre-mutation values. The
            // `if (state.State.RootNodeId is not null) return;` guard
            // at the top of this method would then short-circuit every
            // retry on this activation, permanently routing against a
            // root id storage never accepted (or skipping the registry
            // registration even though the registry-side write succeeded
            // - which is itself idempotent, so re-running it on the
            // next retry is safe).
            //
            // The cross-grain calls above (registry.RegisterAsync,
            // leafGrain.SetTreeIdAsync, leafGrain.SetShardIndexAsync)
            // are NOT reverted - each is idempotent on retry by its own
            // idempotency guard (cycle 1 for the leaf-init pair, by
            // contract for RegisterAsync), so leaving their side effects
            // in place across a failed WriteStateAsync is correct.
            state.State.IsRegistered = prevIsRegistered;
            state.State.RootNodeId = prevRootNodeId;
            state.State.RootIsLeaf = prevRootIsLeaf;
            throw;
        }
    }

    /// <summary>
    /// If a previous root promotion was interrupted (Phase 1 persisted but
    /// Phase 2 did not complete), resume it now.
    /// </summary>
    private async Task ResumePendingPromotionAsync()
    {
        if (state.State.PendingPromotion is null) return;

        // Re-check under the promotion gate. Two interleaved
        // PrepareForOperationSlowAsync turns could both observe a
        // non-null PendingPromotion on the unguarded peek above; the
        // first one through the gate clears it and the second one
        // would otherwise replay CompletePromotionAsync against a
        // null pending intent (NRE) or, worse, against a fresh
        // pending intent published by an unrelated mid-flight
        // PromoteRootAsync sequence. The gate guarantees only one
        // CompletePromotionAsync call observes any given pending
        // intent.
        await _promotionGate.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            if (state.State.PendingPromotion is null) return;
            await CompletePromotionAsync();
        }
        finally
        {
            _promotionGate.Release();
        }
    }

    /// <summary>
    /// Produces a deterministic <see cref="Guid"/> from <paramref name="input"/>
    /// using a SHA-256 hash truncated to 16 bytes. This ensures crash-retries
    /// in <see cref="EnsureRootAsync"/> reuse the same grain identity.
    /// </summary>
    private static Guid DeterministicGuid(string input)
    {
        var hash = System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(input));
        return new Guid(hash.AsSpan(0, 16));
    }

    private void ThrowIfDeleted()
    {
        if (state.State.IsDeleted)
            throw new InvalidOperationException("This tree has been deleted and is no longer accessible.");
    }

    private Task PrepareForOperationAsync()
    {
        // Order matters: a shard that participated as the *source* of an
        // online resize transitions Reject -> Cleanup, which sets BOTH
        // ShadowForward.Phase = Rejecting AND IsDeleted = true on the old
        // physical tree's shards. A stale in-flight read iterator that was
        // routed against the old physical tree before the alias swap must
        // surface as the recoverable StaleTreeRoutingException so the
        // calling LatticeGrain refreshes its alias and retries against the
        // new physical tree -- not as the terminal "tree has been deleted"
        // InvalidOperationException. A user-initiated DeleteTreeAsync never
        // touches ShadowForward state, so for that case ThrowIfTreeRejecting
        // is a no-op and ThrowIfDeleted still fires correctly.
        ThrowIfTreeRejecting();
        ThrowIfRetainedRedirect();
        ThrowIfDeleted();

        // Steady-state sync fast path: on the read hot path each `await`
        // below resolves synchronously - `EnsureRootAsync` short-circuits
        // when `RootNodeId is not null`, and the two `ResumePending*` helpers
        // short-circuit when their state pointers are null. The original
        // `async Task` wrapper still ran three nested `MoveNext` traversals
        // for every read; this peek lets the whole prepare chain compile to
        // a `Task.CompletedTask` return when nothing is owed, deferring the
        // async machinery to `PrepareForOperationSlowAsync` only when at
        // least one helper actually has work. The cycle 36 lesson
        // (synchronous-completion async methods are zero-alloc) still
        // applies - this is a CPU optimisation, not an allocation one.
        if (state.State.RootNodeId is not null
            && state.State.PendingPromotion is null
            && state.State.PendingBulkGraft is null)
        {
            return Task.CompletedTask;
        }

        return PrepareForOperationSlowAsync();
    }

    private async Task PrepareForOperationSlowAsync()
    {
        await EnsureRootAsync();
        await ResumePendingPromotionAsync();
        await ResumePendingBulkGraftAsync();
    }

    public async Task MergeManyAsync(Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration = false)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        await PrepareForOperationAsync();
        RecordWrite();

        if (entries.Count == 0)
        {
            return;
        }

        // Online-resize shadow-forward: forward the full merge batch to the
        // destination tree in parallel. LWW preserves the original HLCs end-to-end,
        // so the destination converges whether this forward wins, loses, or races
        // with the background drain reader.
        //
        // The shadow-forward target is a different physical tree (the online-resize
        // destination), not a different shard within the same tree. From the
        // destination tree's perspective the forwarded write is a normal merge,
        // NOT a cross-shard migration, so isCrossShardMigration is deliberately
        // not threaded through TrackShadowForward.
        var forwardTask = TrackShadowForward(entries, static (t, s) => t.MergeManyAsync(s));

        // Root-is-leaf fast path: route the entire batch to the single leaf
        // in one grain call and one WriteStateAsync. Decided by node TYPE so a
        // corrupt RootIsLeaf flag over an internal root (issue 899) takes the
        // grouped internal-routing path below instead of blind-casting.
        if (RootIsLeafTyped)
        {
            await MergeGroupAsync(entries, isCrossShardMigration);
            await forwardTask;
            return;
        }

        // Group entries by target leaf so each leaf is called exactly once.
        // Per-leaf WriteStateAsync collapses from O(entries) to O(leaves) -
        // the dominant storage-I/O win. Internal-node routing RPCs are still
        // paid during grouping and re-paid during apply (the apply phase
        // re-traverses so that a split produced by an earlier group is
        // observed by later groups) - that cost is O((N+L)·D) lightweight
        // in-memory reads against the persisted internal nodes, dominated
        // in practice by the O(L) storage writes.
        //
        // Distinct groups target distinct leaves, so a split produced by one
        // group cannot re-route another group's keys (internal splits create
        // sibling internals but preserve child GrainIds; leaf splits only
        // affect the one leaf being written to).
        var groups = new Dictionary<GrainId, Dictionary<string, LwwValue<byte[]>>>();
        foreach (var (key, lww) in entries)
        {
            var leafId = await TraverseToLeafWithRetryAsync(key);
            if (!groups.TryGetValue(leafId, out var group))
            {
                // Pre-size conservatively: if entries spread evenly across
                // leaves the per-group size is entries.Count / expected-leaves;
                // we cap at the incoming count to avoid over-allocating on
                // tiny batches.
                group = new Dictionary<string, LwwValue<byte[]>>(
                    capacity: Math.Min(entries.Count, 16));
                groups[leafId] = group;
            }
            group[key] = lww;
        }

        foreach (var group in groups.Values)
        {
            await MergeGroupAsync(group, isCrossShardMigration);
        }

        // Await forwardTask at the end of the grouped path - matches the
        // root-is-leaf fast path and surfaces any forward failure to the
        // caller. Without this await, the forward would only be observed
        // asynchronously by the tracker continuation.
        await forwardTask;
    }

    /// <summary>
    /// Traverses to the leaf owning <paramref name="key"/> with transient-exception retry.
    /// Mirrors the resilience the per-entry path had before leaf-grouped routing.
    /// </summary>
    private async Task<GrainId> TraverseToLeafWithRetryAsync(string key)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return await TraverseToLeafAsync(key);
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    /// <summary>
    /// Applies a pre-grouped batch of entries to a single leaf, re-traversing
    /// against the current topology and propagating any resulting split up to
    /// the root. Retries on transient Orleans / storage exceptions; the leaf's
    /// <c>MergeManyAsync</c> is LWW-idempotent, so replay is safe.
    /// </summary>
    private async Task MergeGroupAsync(Dictionary<string, LwwValue<byte[]>> group, bool isCrossShardMigration)
    {
        // Any key in the group routes to the same leaf under the current
        // topology; pick one via foreach-break to avoid the LINQ enumerator
        // boxing allocation of entries.Keys.First().
        string? pivotKey = null;
        foreach (var k in group.Keys) { pivotKey = k; break; }
        // group is never empty here (callers only invoke with non-empty groups).

        for (int attempt = 0; ; attempt++)
        {
            try
            {
                var splitResult = await TraverseForMergeAsync(pivotKey!, group, isCrossShardMigration);

                while (splitResult is not null)
                {
                    splitResult = await PromoteRootAsync(splitResult);
                }

                return;
            }
            catch (Exception ex) when (ex is OrleansException or TimeoutException or IOException && attempt < MaxRetries)
            {
            }
        }
    }

    private async Task<SplitResult?> TraverseForMergeAsync(string key, Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration)
    {
        if (RootIsLeafTyped)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.RootNodeId!.Value);
            return await leaf.MergeManyAsync(entries, isCrossShardMigration);
        }

        var path = StackPool.Get();
        try
        {
            var currentId = state.State.RootNodeId!.Value;

            while (true)
            {
                var snapshot = await GetRoutingTableSnapshotAsync(currentId);
                var (childId, childrenAreLeaves) = snapshot.Route(key);

                if (childrenAreLeaves)
                {
                    path.Push(currentId);
                    path.Push(childId);
                    break;
                }

                path.Push(currentId);
                currentId = childId;
            }

            var leafId = path.Pop();
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var splitResult = await leafGrain.MergeManyAsync(entries, isCrossShardMigration);

            while (splitResult is not null && path.Count > 0)
            {
                var parentId = path.Pop();
                var parentGrain = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
                splitResult = await parentGrain.AcceptSplitAsync(splitResult.PromotedKey, splitResult.NewSiblingId);
                InvalidateRoutingTable(parentId);
            }

            return splitResult;
        }
        finally
        {
            StackPool.Return(path);
        }
    }

    // Terminate a forward paged range-scan sibling walk as soon as it provably
    // leaves the [startInclusive, endExclusive) range. A leaf owns exactly
    // [LowKeyInclusive, HighKeyExclusive), and HighKeyExclusive equals the next
    // sibling's LowKeyInclusive, so once the current leaf's HighKeyExclusive is
    // at/after endExclusive every subsequent leaf is entirely out of range and
    // need not be read. Bounds are predicate-independent, so unlike an empty
    // result this is a safe stop signal even when a predicate filtered every
    // in-range row on a leaf (issue 1046). A null bound (outermost leaf, or
    // legacy state that pre-dates the slot) means "no constraint" so the walk
    // falls back to its prior end-of-tree behaviour.
    private async Task<bool> ForwardWalkLeftRangeAsync(IBPlusLeafGrain leafGrain, string? endExclusive)
    {
        if (endExclusive is null)
            return false;
        var bounds = await leafGrain.GetKeyRangeAsync();
        return bounds.HighKeyExclusive is not null
            && string.CompareOrdinal(bounds.HighKeyExclusive, endExclusive) >= 0;
    }

    // Reverse counterpart of <see cref="ForwardWalkLeftRangeAsync"/>: a backward
    // walk has left the range once the current leaf's LowKeyInclusive is
    // at/before startInclusive, since every previous sibling holds only keys
    // strictly below that bound (issue 1046).
    private async Task<bool> ReverseWalkLeftRangeAsync(IBPlusLeafGrain leafGrain, string? startInclusive)
    {
        if (startInclusive is null)
            return false;
        var bounds = await leafGrain.GetKeyRangeAsync();
        return bounds.LowKeyInclusive is not null
            && string.CompareOrdinal(bounds.LowKeyInclusive, startInclusive) <= 0;
    }

    public async Task<KeysPage> GetSortedKeysBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null,
        LatticePredicateNode? predicate = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

        // Determine the starting leaf.
        var seekKey = continuationToken ?? startInclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        // Walk the sibling chain, collecting keys until the page is full.
        // Guard: the start node must be a leaf. A corrupt ChildrenAreLeaves
        // flag could have steered the traversal onto an internal node; if so
        // re-descend to the leftmost leaf rather than blind-casting it.
        leafId = await DescendToLeafAsync(leafId, rightmost: false);
        var keys = new List<string>(pageSize);
        HashSet<int>? movedSet = null;
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as afterExclusive so the leaf filters
            // at the source - avoids transferring keys that would be
            // discarded here. The optional predicate is evaluated inside the
            // leaf so non-matching values never cross the wire.
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive, afterExclusive: continuationToken, predicate: predicate);

            foreach (var key in leafKeys)
            {
                if (TryGetMovedAwaySlot(key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                keys.Add(key);
                if (keys.Count >= pageSize)
                    break;
            }

            if (keys.Count >= pageSize)
                break;

            if (await ForwardWalkLeftRangeAsync(leafGrain, endExclusive))
                return new KeysPage
                {
                    Keys = keys,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new KeysPage
                {
                    Keys = keys,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            // Guard: a next-sibling pointer must stay at leaf level. If it
            // crosses onto an internal node, re-descend to that subtree's
            // leftmost leaf rather than blind-casting it (issue 899).
            leafId = await DescendToLeafAsync(nextSibling.Value, rightmost: false);
        }

        return new KeysPage
        {
            Keys = keys,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    public async Task<KeysPage> GetSortedKeysBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null,
        LatticePredicateNode? predicate = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

        // Determine the starting leaf (rightmost, or the leaf for the seek key).
        var seekKey = continuationToken ?? endExclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToRightmostLeafAsync();
        }

        // Walk the sibling chain backward, collecting keys in reverse until the page is full.
        // Guard: the start node must be a leaf. If a corrupt ChildrenAreLeaves
        // flag steered the traversal onto an internal node, re-descend to the
        // rightmost leaf rather than blind-casting it (issue 899).
        leafId = await DescendToLeafAsync(leafId, rightmost: true);
        var keys = new List<string>(pageSize);
        HashSet<int>? movedSet = null;
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as beforeExclusive so the leaf filters
            // at the source - avoids transferring keys that would be
            // discarded here.
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive, beforeExclusive: continuationToken, predicate: predicate);

            // Walk the leaf's keys in reverse order.
            for (int i = leafKeys.Count - 1; i >= 0; i--)
            {
                var key = leafKeys[i];
                if (TryGetMovedAwaySlot(key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                keys.Add(key);
                if (keys.Count >= pageSize)
                    break;
            }

            if (keys.Count >= pageSize)
                break;

            if (await ReverseWalkLeftRangeAsync(leafGrain, startInclusive))
                return new KeysPage
                {
                    Keys = keys,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            var prevSibling = await leafGrain.GetPrevSiblingAsync();
            if (prevSibling is null)
                return new KeysPage
                {
                    Keys = keys,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            // Guard: a prev-sibling pointer must stay at leaf level. If it
            // crosses onto an internal node, re-descend to that subtree's
            // rightmost leaf rather than blind-casting it (issue 899).
            leafId = await DescendToLeafAsync(prevSibling.Value, rightmost: true);
        }

        return new KeysPage
        {
            Keys = keys,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    public async Task<EntriesPage> GetSortedEntriesBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null,
        LatticePredicateNode? predicate = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

        var seekKey = continuationToken ?? startInclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        var entries = new List<KeyValuePair<string, byte[]>>(pageSize);
        HashSet<int>? movedSet = null;
        // Guard: the start node must be a leaf; re-descend to the leftmost
        // leaf if a corrupt ChildrenAreLeaves flag returned an internal node
        // rather than blind-casting it (issue 899).
        leafId = await DescendToLeafAsync(leafId, rightmost: false);
        while (entries.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as afterExclusive so the leaf filters
            // at the source - avoids serializing byte[] values that would be
            // discarded here.
            var leafEntries = await leafGrain.GetEntriesAsync(startInclusive, endExclusive, continuationToken, predicate: predicate);

            foreach (var entry in leafEntries)
            {
                if (TryGetMovedAwaySlot(entry.Key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                entries.Add(entry);
                if (entries.Count >= pageSize)
                    break;
            }

            if (entries.Count >= pageSize)
                break;

            if (await ForwardWalkLeftRangeAsync(leafGrain, endExclusive))
                return new EntriesPage
                {
                    Entries = entries,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new EntriesPage
                {
                    Entries = entries,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            // Guard: keep the forward walk at leaf level across internal
            // boundaries (issue 899).
            leafId = await DescendToLeafAsync(nextSibling.Value, rightmost: false);
        }

        return new EntriesPage
        {
            Entries = entries,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    public async Task<EntriesPage> GetSortedEntriesBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null,
        LatticePredicateNode? predicate = null)
    {
        await PrepareForOperationAsync();
        RecordRead();

        var seekKey = continuationToken ?? endExclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToRightmostLeafAsync();
        }

        var entries = new List<KeyValuePair<string, byte[]>>(pageSize);
        HashSet<int>? movedSet = null;
        // Guard: the start node must be a leaf; re-descend to the rightmost
        // leaf if a corrupt ChildrenAreLeaves flag returned an internal node
        // rather than blind-casting it (issue 899).
        leafId = await DescendToLeafAsync(leafId, rightmost: true);
        while (entries.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            // Pass continuationToken as beforeExclusive so the leaf filters
            // at the source - avoids serializing byte[] values that would be
            // discarded here.
            var leafEntries = await leafGrain.GetEntriesAsync(startInclusive, endExclusive, beforeExclusive: continuationToken, predicate: predicate);

            for (int i = leafEntries.Count - 1; i >= 0; i--)
            {
                var entry = leafEntries[i];
                if (TryGetMovedAwaySlot(entry.Key, out var movedSlot))
                {
                    (movedSet ??= []).Add(movedSlot);
                    continue;
                }
                entries.Add(entry);
                if (entries.Count >= pageSize)
                    break;
            }

            if (entries.Count >= pageSize)
                break;

            if (await ReverseWalkLeftRangeAsync(leafGrain, startInclusive))
                return new EntriesPage
                {
                    Entries = entries,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            var prevSibling = await leafGrain.GetPrevSiblingAsync();
            if (prevSibling is null)
                return new EntriesPage
                {
                    Entries = entries,
                    HasMore = false,
                    MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
                };

            // Guard: keep the reverse walk at leaf level across internal
            // boundaries (issue 899).
            leafId = await DescendToLeafAsync(prevSibling.Value, rightmost: true);
        }

        return new EntriesPage
        {
            Entries = entries,
            HasMore = true,
            MovedAwaySlots = movedSet is null ? null : SortedSlotsArray(movedSet),
        };
    }

    /// <inheritdoc />
    public async Task<KeysPage> GetSortedKeysBatchForSlotsAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken,
        int[] sortedSlots,
        int virtualShardCount,
        LatticePredicateNode? predicate = null)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        await PrepareForOperationAsync();
        RecordRead();

        if (sortedSlots.Length == 0 || state.State.RootNodeId is null)
            return new KeysPage { Keys = [], HasMore = false };

        var seekKey = continuationToken ?? startInclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        var keys = new List<string>(pageSize);
        // Guard: re-descend to a real leaf if the start node is internal
        // (issue 899).
        leafId = await DescendToLeafAsync(leafId, rightmost: false);
        while (keys.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var leafKeys = await leafGrain.GetKeysAsync(startInclusive, endExclusive, afterExclusive: continuationToken, predicate: predicate);

            foreach (var key in leafKeys)
            {
                var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
                if (Array.BinarySearch(sortedSlots, slot) < 0) continue;
                keys.Add(key);
                if (keys.Count >= pageSize) break;
            }

            if (keys.Count >= pageSize) break;

            if (await ForwardWalkLeftRangeAsync(leafGrain, endExclusive))
                return new KeysPage { Keys = keys, HasMore = false };

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new KeysPage { Keys = keys, HasMore = false };

            // Guard: keep the walk at leaf level across internal boundaries
            // (issue 899).
            leafId = await DescendToLeafAsync(nextSibling.Value, rightmost: false);
        }

        return new KeysPage { Keys = keys, HasMore = true };
    }

    /// <inheritdoc />
    public async Task<EntriesPage> GetSortedEntriesBatchForSlotsAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken,
        int[] sortedSlots,
        int virtualShardCount,
        LatticePredicateNode? predicate = null)
    {
        ArgumentNullException.ThrowIfNull(sortedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        await PrepareForOperationAsync();
        RecordRead();

        if (sortedSlots.Length == 0 || state.State.RootNodeId is null)
            return new EntriesPage { Entries = [], HasMore = false };

        var seekKey = continuationToken ?? startInclusive;
        GrainId leafId;
        if (state.State.RootIsLeaf)
        {
            leafId = state.State.RootNodeId!.Value;
        }
        else if (seekKey is not null)
        {
            leafId = await TraverseToLeafAsync(seekKey);
        }
        else
        {
            leafId = await TraverseToLeftmostLeafAsync();
        }

        var entries = new List<KeyValuePair<string, byte[]>>(pageSize);
        // Guard: re-descend to a real leaf if the start node is internal
        // (issue 899).
        leafId = await DescendToLeafAsync(leafId, rightmost: false);
        while (entries.Count < pageSize)
        {
            var leafGrain = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            var leafEntries = await leafGrain.GetEntriesAsync(startInclusive, endExclusive, continuationToken, predicate: predicate);

            foreach (var entry in leafEntries)
            {
                var slot = ShardMap.GetVirtualSlot(entry.Key, virtualShardCount);
                if (Array.BinarySearch(sortedSlots, slot) < 0) continue;
                entries.Add(entry);
                if (entries.Count >= pageSize) break;
            }

            if (entries.Count >= pageSize) break;

            if (await ForwardWalkLeftRangeAsync(leafGrain, endExclusive))
                return new EntriesPage { Entries = entries, HasMore = false };

            var nextSibling = await leafGrain.GetNextSiblingAsync();
            if (nextSibling is null)
                return new EntriesPage { Entries = entries, HasMore = false };

            // Guard: keep the walk at leaf level across internal boundaries
            // (issue 899).
            leafId = await DescendToLeafAsync(nextSibling.Value, rightmost: false);
        }

        return new EntriesPage { Entries = entries, HasMore = true };
    }

    public async Task<GrainId?> GetLeftmostLeafIdAsync()
    {
        // Resolve the leftmost leaf by node TYPE rather than trusting the
        // persisted RootIsLeaf flag: a baked-inconsistent flag left true over an
        // internal root (issue 899) would otherwise return the internal root id
        // to a caller that casts it to IBPlusLeafGrain (the replication snapshot
        // producer, compaction / merge / split leaf-chain walkers). The guarded
        // traversal returns a real leaf id or null for an empty shard.
        return state.State.RootNodeId is null
            ? null
            : await TraverseToLeftmostLeafAsync();
    }
}
