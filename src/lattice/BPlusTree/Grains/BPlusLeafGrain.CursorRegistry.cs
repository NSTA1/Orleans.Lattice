using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cursor-registry integration partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>.
/// Reports the leaf's highest applied <see cref="HybridLogicalClock"/> to
/// the silo-scoped <see cref="ILeafCursorReporter"/> after every successful
/// projection-checkpoint persist so the per-shard WAL GC pins its trim
/// point under the slowest local consumer (the leaf-as-materialiser).
/// <para>
/// Lazy and zero-cost when nothing drives the projection: the helper is a
/// branch check + early return when <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.Clock"/> is
/// still <see cref="HybridLogicalClock.Zero"/>. The
/// <see cref="ILeafCursorReporter"/> is registered by default (an in-memory
/// reporter wired by <c>AddLattice</c>), so the leaf reports its applied
/// frontier into the always-on cursor registry on every host and the WAL
/// saturation sampler's drain-lag back-pressure is live for every write
/// workload. The durable-pin mirror on
/// <see cref="ILeafCursorReporter.NoteDurableMaterialiserFrontier"/> is a
/// no-op until the host opts into <c>AddWalCursorRegistry</c> (directly or
/// via the WAL GC / views / replication / storage packages), which swaps in
/// the durable-pin-aware reporter; until then the in-memory report still
/// flows and the WAL GC behaves identically to its pre-promotion baseline.
/// </para>
/// <para>
/// Failures to advance the cursor are logged-and-swallowed at warning
/// level: the cursor is monotonic by construction so the next successful
/// flush catches up, and a transient registry hiccup must not stall the
/// foreground write path or block the leaf's own checkpoint advance.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Cached <see cref="ILeafCursorReporter"/> resolved from
    /// <see cref="IGrainContext.ActivationServices"/> on first use.
    /// Normally non-<c>null</c>: <c>AddLattice</c> registers an in-memory
    /// reporter by default. <c>null</c> only on a host that has stripped
    /// even that default registration, in which case the cursor-report
    /// path is a no-op.
    /// </summary>
    private ILeafCursorReporter? _cursorReporter;

    /// <summary>
    /// <see langword="true"/> once the lazy resolution of
    /// <see cref="_cursorReporter"/> has run. The resolution is a single
    /// dictionary lookup; caching the outcome (including the
    /// <c>null</c> result) avoids paying it on every successful flush.
    /// </summary>
    private bool _cursorReporterResolved;

    /// <summary>
    /// Cached consumer id template of the form
    /// <c>_lattice_materialiser_{treeId}_{leafGrainId}</c> for the
    /// single-partition shape, or the partition-suffixed form
    /// <c>_lattice_materialiser_{treeId}_{leafGrainId}_{partition}</c>
    /// when <see cref="LatticeOptions.WalPartitions"/> > 1. Computed
    /// once on first use; <c>null</c> when <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.TreeId"/>
    /// is unset (system-tree leaves and tests that bypass tree
    /// initialisation), in which case the cursor-report path is a
    /// no-op.
    /// </summary>
    private string? _cachedConsumerIdBase;

    /// <summary>
    /// <see langword="true"/> once this activation has published its first real
    /// (non-Zero) checkpoint frontier through
    /// <see cref="ILeafCursorReporter.FlushDurableMaterialiserFrontierAsync"/>.
    /// The first crossing from the seeded <see cref="HybridLogicalClock.Zero"/>
    /// block pin to a real frontier goes through the batched flush - one
    /// round-trip per routed pin shard - rather than the per-consumer debounced
    /// mirror, so the durable retention floor leaves Zero promptly instead of
    /// waiting on the debounce window. Every subsequent checkpoint advance falls
    /// back to the cheap debounced fire-and-forget mirror, so the steady-state
    /// checkpoint path stays allocation- and round-trip-light.
    /// </summary>
    private bool _durableFrontierBarriered;

    /// <summary>
    /// Reports the leaf's current projection HLC to the registered
    /// <see cref="ILeafCursorReporter"/>, lazy-gated on
    /// <c>state.State.Clock &gt; HybridLogicalClock.Zero</c>. Called from
    /// <see cref="FlushPendingCheckpointAsync"/> after every successful
    /// persist; never throws. Under multi-partition WAL the leaf reports
    /// one cursor per partition so the per-shard WAL GC trims each
    /// partition independently against its own slowest consumer.
    /// </summary>
    private async Task ReportCursorIfActiveAsync()
    {
        var clock = state.State.Clock;
        if (clock <= HybridLogicalClock.Zero)
        {
            return;
        }

        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return;
        }

        var treeId = state.State.TreeId!;
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);

        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);
            try
            {
                await reporter.ReportAsync(treeId, consumerId, clock, CancellationToken.None);
            }
            catch (Exception ex)
            {
                var logger = context.ActivationServices?
                    .GetService<ILoggerFactory>()?
                    .CreateLogger<BPlusLeafGrain>();
                logger?.LogWarning(
                    ex,
                    "Failed to report leaf cursor for tree {TreeId} consumer {ConsumerId} at HLC {Cursor}; will retry on next checkpoint flush.",
                    treeId,
                    consumerId,
                    clock);
            }
        }

        // Durable pin mirror. The first time this activation crosses from the
        // seeded Zero block pin to a real frontier we publish through the
        // batched flush (one round-trip per routed pin shard) so the durable
        // retention floor leaves Zero promptly; every subsequent advance uses
        // the cheap per-consumer debounced mirror. The pin store coalesces the
        // durable write itself in both cases - a pin that lags the leaf's true
        // frontier only retains more WAL, which is always GC-safe. Never throws
        // (the flush swallows transient failures).
        if (!_durableFrontierBarriered)
        {
            await FlushDurableMaterialiserFrontierAsync();
            _durableFrontierBarriered = true;
        }
        else
        {
            var partitionsWithLiveData = ComputePartitionsWithLiveData(partitionCount);
            var partitionsWithEmptyWal = await ComputeEmptyWalPartitionsAsync(
                partitionCount, partitionsWithLiveData, treeId);
            for (var partition = 0; partition < partitionCount; partition++)
            {
                var consumerId = BuildConsumerId(idBase, partition, partitionCount);
                var (frontier, offset) = ResolveDurablePinForPartition(
                    partition, clock, partitionsWithLiveData, partitionsWithEmptyWal);
                reporter.NoteDurableMaterialiserFrontier(
                    treeId, consumerId, frontier, offset);
            }
        }
    }

    /// <summary>
    /// Publishes this leaf's current real checkpoint frontier as a WAL
    /// retention pin (one per WAL partition) via
    /// <see cref="ILeafCursorReporter.FlushDurableMaterialiserFrontierAsync"/>,
    /// gated on <c>Clock &gt; Zero</c> except for the never-written release
    /// described below. Unlike the per-consumer debounced mirror
    /// on <see cref="ReportCursorIfActiveAsync"/>, the whole partition set goes
    /// in one batched round-trip per routed pin shard and is merged into the
    /// durable store's monotonic-max state immediately, so the trim floor
    /// reflects this leaf's checkpoint without waiting on a debounce window.
    /// <para>
    /// For a never-written leaf (<c>Clock == Zero</c>) the flush publishes only
    /// when at least one partition resolves to the <c>(Zero, X)</c> release of
    /// issue #3453 - a scanned-through <b>persisted</b> checkpoint <c>X &gt;= 0</c>
    /// on a partition with no live row - and is otherwise a no-op exactly as
    /// before. The in-memory registry is untouched on this path;
    /// <see cref="ReportCursorIfActiveAsync"/> keeps its <c>Clock &gt; Zero</c>
    /// guard, so the registry still never receives a Zero cursor.
    /// </para>
    /// The store's own durable write is coalesced (see
    /// <see cref="ILeafCursorReporter.FlushDurableMaterialiserFrontierAsync"/>);
    /// a pin that lags this leaf's true frontier only retains more WAL and is
    /// always GC-safe. Called on the first real-frontier checkpoint of an
    /// activation and again on graceful deactivation (after the final checkpoint
    /// flush) so a leaf that goes dormant leaves its frontier behind for the
    /// WAL GC. Idempotent (the pin store's monotonic-max merge no-ops a
    /// stale/equal frontier) and never throws; a no-op when the host has no
    /// cursor reporter (pre-WAL), the tree id is unset, or the leaf has not
    /// checkpointed yet.
    /// </summary>
    /// <param name="cancellationToken">
    /// Deadline for the flush. This runs on the deactivation path, where
    /// Orleans awaits <c>OnDeactivateAsync</c> under a deactivation deadline
    /// and reports a <c>TaskCanceledException</c> from its own frame when the
    /// hook overruns - so a flush that ignores the token cannot be interrupted,
    /// the grain never returns in time, and the caller's exception handling
    /// never gets the chance to swallow it (issue 1965). Passing the token
    /// through lets the flush abandon promptly, which is safe because the pin
    /// is best-effort: the WAL is the durability boundary and the next
    /// activation re-reports the frontier.
    /// </param>
    private async Task<int> FlushDurableMaterialiserFrontierAsync(CancellationToken cancellationToken = default)
    {
        // Issue #3453: a never-written leaf (Clock == Zero) is no longer turned
        // away here unconditionally. Its release branches resolve to
        // (Zero, -1), which is byte-identical to the block pin, so for it the
        // only expressible release is (Zero, X) with X its PERSISTED
        // checkpoint - see the never-written arm of
        // ResolveDurablePinForPartition. The flush therefore still returns
        // without publishing unless at least one partition resolves through
        // that arm; every other Zero-clock outcome is exactly the pin the
        // activation seed already published, so skipping it keeps this path
        // byte-identical to before for every leaf the fix does not release.
        var clock = state.State.Clock;
        var neverWritten = clock <= HybridLogicalClock.Zero;

        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return 0;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return 0;
        }

        var treeId = state.State.TreeId!;
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        var partitionsWithLiveData = ComputePartitionsWithLiveData(partitionCount);
        if (neverWritten && !HasNeverWrittenScannedThroughPartition(partitionCount, partitionsWithLiveData))
        {
            return 0;
        }

        var partitionsWithEmptyWal = await ComputeEmptyWalPartitionsAsync(
            partitionCount, partitionsWithLiveData, treeId);
        var reports = new MaterialiserPinReport[partitionCount];
        var releases = 0;
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);
            var (frontier, offset) = ResolveDurablePinForPartition(
                partition, clock, partitionsWithLiveData, partitionsWithEmptyWal,
                releaseNeverWrittenScannedThrough: true);
            reports[partition] = new MaterialiserPinReport(consumerId, frontier, offset);

            if (neverWritten)
            {
                // Issue #3453: count the partitions released as (Zero, X), for
                // the same reason the #3103 releases below are counted - the
                // drive's replay-based predicate cannot score a release whose
                // leaf has no applied data. The #3103 arm is NOT counted for a
                // never-written leaf: it resolves to (Zero, -1), the block pin.
                if (IsNeverWrittenScannedThroughPartition(partition, partitionsWithLiveData))
                {
                    releases++;
                }

                continue;
            }

            // Count the partitions this flush released under the #3103 rule -
            // ones that held live rows and no checkpoint, and so would have
            // published a permanent block pin, but whose WAL turned out to be
            // empty. The starvation drive needs this to answer honestly whether
            // it lifted anything, because its replay-based predicate cannot
            // see a release it did not reach through a replay.
            if (partitionsWithEmptyWal is not null
                && partition < partitionsWithEmptyWal.Length
                && partitionsWithEmptyWal[partition]
                && partitionsWithLiveData[partition]
                && GetCurrentCheckpointForPartition(partition) < 0)
            {
                releases++;
            }
        }

        await reporter.FlushDurableMaterialiserFrontierAsync(
            treeId, reports, cancellationToken);
        return releases;
    }

    /// <summary>
    /// Whether <paramref name="partition"/> of a never-written leaf
    /// (<c>Clock == Zero</c>) has a durable scanned-through checkpoint it may
    /// publish as a <c>(Zero, X)</c> release (issue #3453): the partition holds
    /// no live cache row and its <b>persisted</b> checkpoint is <c>&gt;= 0</c>.
    /// </summary>
    /// <remarks>
    /// The persisted checkpoint, never the pending one
    /// (<see cref="GetCurrentCheckpointForPartition"/>): published offsets merge
    /// by monotonic maximum, so an over-report can never be lowered, and every
    /// replay starts from the persisted offset (issue #3476).
    /// </remarks>
    private bool IsNeverWrittenScannedThroughPartition(int partition, bool[] partitionsWithLiveData)
        => !partitionsWithLiveData[partition]
            && GetPersistedCheckpointForPartition(partition) >= 0;

    private bool HasNeverWrittenScannedThroughPartition(int partitionCount, bool[] partitionsWithLiveData)
    {
        for (var partition = 0; partition < partitionCount; partition++)
        {
            if (IsNeverWrittenScannedThroughPartition(partition, partitionsWithLiveData))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Seeds the durable WAL materialiser pin store with this leaf's
    /// persisted checkpoint frontier (which may be
    /// <see cref="HybridLogicalClock.Zero"/> for a leaf that has activated
    /// but never checkpointed). Unlike <see cref="ReportCursorIfActiveAsync"/>
    /// this is <b>not</b> gated on <c>Clock &gt; Zero</c>: a Zero seed is a
    /// deliberate "block" pin that keeps the WAL head retained for a
    /// never-checkpointed leaf across a restart, closing the edge where a
    /// leaf has applied and shipped a write but never produced a checkpoint.
    /// Reports to the in-memory registry are skipped for the Zero case (a
    /// Zero cursor would either be rejected or pin the trim point at offset
    /// zero); only the durable, GC-floor-only pin is seeded. The seed uses
    /// the same per-partition consumer-id shape as
    /// <see cref="ReportCursorIfActiveAsync"/> so a later real-frontier
    /// report advances the same key rather than orphaning the Zero seed.
    /// Never throws.
    /// <para>
    /// The offset half of the seed is coverage-gated through
    /// <see cref="ResolveDurablePinForPartition"/>, exactly as
    /// <see cref="ReportCursorIfActiveAsync"/> and
    /// <see cref="FlushDurableMaterialiserFrontierAsync"/> are, and for the same
    /// reason. The two dimensions of the pin merge <b>independently</b> and both
    /// monotonically (<c>WalMaterialiserPinGrain.Merge</c>), so a Zero frontier
    /// does <em>not</em> neutralise an over-reported offset: the offset is
    /// recorded permanently and no later, correctly-gated report can lower it.
    /// A leaf can hold a checkpoint at or past a WAL offset its durable snapshot
    /// does not cover while its clock is still Zero - replay bumps the
    /// per-partition checkpoint for entries it <em>skips</em> because they route
    /// to another leaf's key range, which never advances this leaf's clock - so
    /// seeding the raw checkpoint publishes an offset floor beyond durable
    /// coverage. That floor outlives the Zero block: once the leaf takes real
    /// data and its first gated flush releases the frontier, the stale
    /// over-advanced offset is still the merged maximum, and the shared-shard
    /// WAL GC is authorised to trim a prefix no cold rebuild can replay
    /// (<c>LeafProjectionStaleException</c>).
    /// </para>
    /// <para>
    /// Note that the frontier half is inert at this seam - the sole caller
    /// (<c>OnActivateAsync</c>) invokes this only when
    /// <c>Clock &lt;= HybridLogicalClock.Zero</c>, and every branch of
    /// <see cref="ResolveDurablePinForPartition"/> returns either that clock or
    /// the literal Zero, so the seeded frontier is Zero either way. The gate is
    /// therefore doing offset work only, which is precisely why its absence here
    /// was survivable rather than harmless.
    /// </para>
    /// <para>
    /// The seed deliberately does <b>not</b> opt in to the never-written
    /// <c>(Zero, X)</c> release of issue #3453, although it runs only for a
    /// Zero-clock leaf. That release publishes the <b>persisted</b>
    /// scanned-through checkpoint and is reserved for the flush paths - the
    /// starvation drive, which persists the replay's advance before it
    /// publishes, and the deactivation barrier. At activation the seed has no
    /// such ordering to rely on, and the hazard described above is precisely a
    /// Zero-clock offset published from this seam; keeping the seed on the
    /// coverage-gated arms leaves its behaviour byte-identical to before.
    /// </para>
    /// </summary>
    private async Task SeedDurableMaterialiserFrontierAsync()
    {
        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return;
        }

        var treeId = state.State.TreeId!;
        var clock = state.State.Clock;
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        var partitionsWithLiveData = ComputePartitionsWithLiveData(partitionCount);
        var partitionsWithEmptyWal = await ComputeEmptyWalPartitionsAsync(
            partitionCount, partitionsWithLiveData, treeId);
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);
            var (frontier, offset) = ResolveDurablePinForPartition(
                partition, clock, partitionsWithLiveData, partitionsWithEmptyWal);
            reporter.NoteDurableMaterialiserFrontier(
                treeId, consumerId, frontier, offset);
        }
    }

    /// <summary>
    /// Durably seeds a <see cref="HybridLogicalClock.Zero"/> "block" pin for
    /// this leaf and <b>awaits</b> the write, closing the window in which a
    /// freshly-born data-capable leaf can have its WAL trimmed past its
    /// un-materialised frontier before it registers any pin. Called at the two
    /// tree-id birth seams - a split sibling's
    /// <see cref="InitializeSiblingAsync"/> and a root/bulk-load leaf's
    /// <see cref="SetTreeIdAsync"/> - <em>before</em> the inherited/routed
    /// writes that follow (<see cref="MergeEntriesAsync"/>) make the leaf's data
    /// reachable in the WAL. Seeds <see cref="HybridLogicalClock.Zero"/> (not the
    /// current clock) because the leaf has checkpointed none of its data yet, so
    /// its entire range is un-materialised; a Zero pin disables the WAL GC's
    /// cursor-trim branch for the tree until the leaf produces its first durable
    /// checkpoint and advances the pin past Zero. Idempotent and never throws
    /// (the awaited seam swallows transient failures); a no-op when the host has
    /// no cursor reporter (pre-WAL) or the tree id is unset.
    /// <para>
    /// <b>The offset half is not a sentinel when the caller knows the birth
    /// frontier (issue #3094).</b> A pin reporting the "-1" no-offset sentinel
    /// is deliberately excluded from the WAL GC's offset coverage set, so it is
    /// protected <i>only</i> by the Zero-HLC block-pin branch - and that branch
    /// does not block one partition, it disables the cursor trim for the whole
    /// tree. Leaf keys hash across every WAL partition
    /// (<see cref="WalPartitionHash"/> is FNV-1a over the entire key), so one
    /// newborn leaf blocks all of them; and splits admit newborns continuously,
    /// so a growing tree never stops having one. The measured consequence is a
    /// WAL that grows monotonically while every trim pass short-circuits before
    /// a single shard is scanned.
    /// </para>
    /// <para>
    /// When <paramref name="walHeadsAtBirth"/> is supplied - the split path,
    /// where the donor captured the per-partition heads before handing the rows
    /// over - each partition's pin carries that head as a real checkpoint
    /// offset instead. This is <b>not</b> a relaxation of the retention
    /// guarantee: the sibling's rows are appended at or above the captured head,
    /// and the WAL GC's offset floor refuses to trim any entry ABOVE the floor
    /// even when it is HLC-eligible, so the rows the seed exists to protect stay
    /// protected on the offset axis. What changes is that the pin now sits
    /// inside the offset coverage set, so it no longer has to disable the cursor
    /// branch tree-wide to be safe.
    /// </para>
    /// <para>
    /// A head of <c>0</c> or less is not a usable checkpoint offset and falls
    /// back to the sentinel, mirroring the <c>donorHead &gt; 0</c> guard
    /// <c>CompleteSplitAsync</c> applies to the same array. The root/bulk-load
    /// seam passes nothing and keeps the sentinel: it is a one-time bounded
    /// event at tree creation rather than a continuous admission source.
    /// </para>
    /// </summary>
    private async Task SeedDurableMaterialiserBlockPinAsync(long[]? walHeadsAtBirth = null)
    {
        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return;
        }

        var treeId = state.State.TreeId!;
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        var reports = new MaterialiserPinReport[partitionCount];
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);

            // Only a head the donor actually captured for THIS partition is
            // usable. A short array (a donor configured with fewer partitions
            // than this leaf resolves) contributes nothing rather than reading
            // another partition's offset space.
            var offset = walHeadsAtBirth is { } heads
                && partition < heads.Length
                && heads[partition] > 0
                    ? heads[partition]
                    : -1;

            reports[partition] = new MaterialiserPinReport(consumerId, HybridLogicalClock.Zero, offset);
        }

        await reporter.SeedDurableMaterialiserBlockManyAsync(
            treeId, reports, CancellationToken.None);
    }

    /// <summary>
    /// Deregisters every one of this leaf's materialiser cursors - the death
    /// seam that mirrors <see cref="SeedDurableMaterialiserBlockPinAsync"/>'s
    /// birth seam (issue #3101).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why this exists.</b> A leaf's materialiser pin is a WAL retention
    /// <i>floor</i>: until it resolves to a real offset, the GC cannot trim past
    /// it. Registration is birth-gated - <see cref="ResolveConsumerIdBase"/>
    /// returns <see langword="null"/> while the tree id is unset, so only a leaf
    /// with a persisted tree id can ever hold a pin - but nothing retired the
    /// pin at death. A reclaimed leaf therefore left its pin behind, and the pin
    /// pinned the WAL forever.
    /// </para>
    /// <para>
    /// <b>Why it must run before the state clear.</b> The consumer ids are
    /// derived from <c>state.State.TreeId</c>, which
    /// <c>state.ClearStateAsync()</c> nulls. Deregistering afterwards is not
    /// merely late, it is impossible: the ids can no longer be computed. The
    /// ordering is the whole correctness of the fix, so
    /// <see cref="ClearGrainStateAsync"/> calls this first.
    /// </para>
    /// <para>
    /// <b>Why it is safe.</b> This runs only from
    /// <see cref="ClearGrainStateAsync"/>, which is terminal by construction -
    /// it clears durable state and deactivates. That is exactly the "leaf
    /// eviction during a purge" case
    /// <see cref="ILeafCursorReporter.UnregisterAsync"/> reserves itself for.
    /// Routine deactivation does not come here and must never deregister, or the
    /// GC could trim entries the next activation still needs to replay.
    /// </para>
    /// <para>
    /// <b>Failures are swallowed.</b> The caller has already committed the fold
    /// that removed this leaf from the chain, so throwing here would report a
    /// completed structural change as failed. A missed deregistration degrades
    /// to the pre-fix behaviour - a retained WAL prefix - rather than to
    /// corruption, and the GC's own orphan sweep retires the pin independently.
    /// </para>
    /// </remarks>
    private async Task UnregisterMaterialiserPinsAsync()
    {
        var reporter = ResolveCursorReporter();
        if (reporter is null)
        {
            return;
        }

        var idBase = ResolveConsumerIdBase();
        if (idBase is null)
        {
            return;
        }

        var treeId = state.State.TreeId!;

        int partitionCount;
        try
        {
            var options = await GetOptionsAsync();
            partitionCount = Math.Max(1, options.WalPartitions);
        }
        catch (Exception ex)
        {
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {Leaf} of tree '{TreeId}' could not resolve its WAL partition count while retiring its materialiser pins; the pins are left registered and the WAL GC's orphan sweep will retire them instead.",
                context.GrainId,
                treeId);
            return;
        }

        for (var partition = 0; partition < partitionCount; partition++)
        {
            var consumerId = BuildConsumerId(idBase, partition, partitionCount);
            try
            {
                await reporter.UnregisterAsync(treeId, consumerId, CancellationToken.None);
            }
            catch (Exception ex)
            {
                ResolveLogger()?.LogWarning(
                    ex,
                    "Leaf {Leaf} of tree '{TreeId}' failed to retire materialiser pin {Consumer}; the WAL prefix behind it stays retained until the WAL GC's orphan sweep retires it.",
                    context.GrainId,
                    treeId,
                    consumerId);
            }
        }
    }

    private ILeafCursorReporter? ResolveCursorReporter()
    {
        if (_cursorReporterResolved)
            return _cursorReporter;

        _cursorReporterResolved = true;
        _cursorReporter = context.ActivationServices?.GetService<ILeafCursorReporter>();
        return _cursorReporter;
    }

    private string? ResolveConsumerIdBase()
    {
        if (_cachedConsumerIdBase is not null)
            return _cachedConsumerIdBase;

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return null;

        _cachedConsumerIdBase = $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_{context.GrainId}";
        return _cachedConsumerIdBase;
    }

    /// <summary>
    /// Builds the per-partition consumer id. The single-partition shape
    /// (<paramref name="partitionCount"/> == 1) returns the legacy
    /// unsuffixed form for wire compatibility with hosts that have
    /// never enabled multi-partition WAL replay; the multi-partition
    /// shape suffixes <c>_{partition}</c> so each partition's cursor
    /// is tracked independently.
    /// </summary>
    private static string BuildConsumerId(string idBase, int partition, int partitionCount)
        => partitionCount == 1 ? idBase : $"{idBase}_{partition}";

    /// <summary>
    /// Computes, in a single cache pass, which WAL partitions this leaf holds
    /// live data for - any cache row (a live value or a not-yet-reaped
    /// tombstone) whose key routes to the partition via
    /// <see cref="WalPartitionHash.Compute"/>. Used by
    /// <see cref="ResolveDurablePinForPartition"/> to distinguish a genuinely
    /// empty partition (safe to release its WAL trim block) from one that holds
    /// committed-but-not-yet-checkpointed data (which still depends on the whole
    /// WAL and must keep its block).
    /// <para>
    /// Walks keys only. The answer depends on nothing but the keys, so pulling
    /// the payload of a lazily hydrated snapshot through this - on a path that
    /// runs on every checkpoint flush and at deactivation - would force a leaf
    /// to materialise itself in full to answer a question about routing, which
    /// is exactly the cost bounded hydration exists to avoid.
    /// </para>
    /// </summary>
    private bool[] ComputePartitionsWithLiveData(int partitionCount)
    {
        var hasData = new bool[partitionCount];
        var remaining = partitionCount;
        foreach (var key in Cache.EnumerateKeysUnordered())
        {
            var partition = WalPartitionHash.Compute(key, partitionCount);
            if (!hasData[partition])
            {
                hasData[partition] = true;
                if (--remaining == 0)
                    break;
            }
        }
        return hasData;
    }

    /// <summary>
    /// Per-activation memo of WAL partitions observed to hold at least one
    /// entry. A partition's head offset is the next sequence number to be
    /// assigned, so it is monotonic and <c>head &gt; 0</c> is a one-way
    /// transition: once a partition is known non-empty it can never become
    /// empty again, and re-probing it is pure cost. Lazily allocated because
    /// the probe itself only runs for the rare state that needs it.
    /// </summary>
    private bool[]? _partitionWalKnownNonEmpty;

    /// <summary>
    /// Resolves which WAL partitions are <b>empty</b> (head offset <c>0</c>,
    /// i.e. no entry was ever appended) among those that would otherwise
    /// publish a permanent block pin, or <c>null</c> when no partition is in
    /// that state and the probe can be skipped entirely.
    /// <para>
    /// This exists for issue #3103. A partition that holds live cache rows but
    /// has never durably checkpointed (<c>checkpoint &lt; 0</c>) publishes the
    /// <see cref="HybridLogicalClock.Zero"/> block pin, and that pin is
    /// <b>permanent</b> when the partition's WAL is empty: there is nothing to
    /// replay, so the starved-checkpoint drive returns <c>NoAdvance</c> for
    /// ever, the checkpoint never advances, and the coverage repair's
    /// "checkpointed WITHOUT coverage" predicate stays unreachable. Because a
    /// block pin is tree-wide, one such partition strands every other leaf in
    /// the tree and its WAL can never be trimmed again. The state is reached by
    /// a WAL reset that preserves snapshots: the leaf rehydrates rows from the
    /// surviving blob while the partition's WAL is empty.
    /// </para>
    /// <para>
    /// An empty WAL holds no committed prefix, so the block protects nothing
    /// and costs only liveness - which is exactly the reasoning the existing
    /// genuinely-empty-partition branch already applies. This probe is what
    /// lets that branch tell "empty WAL" from "unapplied prefix". It is
    /// deliberately <b>not</b> a claim of coverage: a partition whose WAL holds
    /// unapplied entries still keeps its block, preserving the #1535 no-loss
    /// invariant and the #945 fall-off guard.
    /// </para>
    /// <para>
    /// Fails closed. A probe that throws returns <c>null</c>, which keeps every
    /// block pin exactly as it was - retaining more WAL is always safe, so a
    /// transient read failure can never authorise a trim.
    /// </para>
    /// </summary>
    private async Task<bool[]?> ComputeEmptyWalPartitionsAsync(
        int partitionCount, bool[] partitionsWithLiveData, string treeId)
    {
        var known = _partitionWalKnownNonEmpty;
        var needsProbe = false;
        for (var p = 0; p < partitionCount; p++)
        {
            if (!partitionsWithLiveData[p] || GetCurrentCheckpointForPartition(p) >= 0)
                continue;
            if (known is not null && p < known.Length && known[p])
                continue;
            needsProbe = true;
            break;
        }
        if (!needsProbe)
            return null;

        long[] heads;
        try
        {
            heads = await CaptureWalHeadsByPartitionAsync(treeId);
        }
        catch (Exception)
        {
            // Fail closed: keep every block pin. Retaining WAL is always safe.
            return null;
        }

        if (known is null || known.Length < partitionCount)
        {
            var grown = new bool[partitionCount];
            if (known is not null)
                Array.Copy(known, grown, Math.Min(known.Length, partitionCount));
            _partitionWalKnownNonEmpty = known = grown;
        }

        var empty = new bool[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            if (p >= heads.Length)
                continue;
            if (heads[p] > 0)
                known[p] = true;
            else
                empty[p] = true;
        }
        return empty;
    }

    /// <summary>
    /// Resolves the durable materialiser pin (frontier HLC and checkpoint
    /// offset) this leaf reports for <paramref name="partition"/>. This is the
    /// coverage-gated trim floor: the pin may only authorise the shared-shard
    /// WAL GC to trim as far as the checkpointed prefix is durably
    /// recoverable, which for a data-bearing partition means as far as a
    /// durable leaf snapshot actually covers.
    /// <para>
    /// The decision folds two block regimes into one coherent rule so the
    /// WAL GC never trims a prefix a cold rebuild could not replay:
    /// </para>
    /// <list type="bullet">
    /// <item><description>
    /// <b>Genuinely empty partition</b> (no cache key routes to it <em>and</em>
    /// it has never durably checkpointed, so <c>checkpoint &lt; 0</c>): nothing
    /// to lose, so it releases any block and reports the real
    /// <c>(clock, checkpoint)</c> frontier exactly as before - keeping WAL trim
    /// live for the ubiquitous empty-partition pins a multi-partition leaf
    /// produces. The <c>checkpoint &lt; 0</c> guard is essential: emptiness is
    /// read from the transient in-memory cache, which does not reflect durable
    /// data before the cache is hydrated (a cold reactivation with no snapshot,
    /// or after tombstone reaping / compaction), so a durably-checkpointed
    /// partition is never released on the strength of a momentarily empty cache.
    /// </description></item>
    /// <item><description>
    /// <b>Data-bearing with an empty WAL</b> (issue #3103): the partition holds
    /// live cache rows and has never checkpointed, but its WAL head is
    /// <c>0</c>, so no entry was ever appended and the block protects nothing.
    /// Reached by a WAL reset that preserves snapshots - the leaf rehydrates
    /// rows from the surviving blob while the WAL behind them is gone. Left
    /// blocking, the pin is <em>permanent</em>: there is nothing to replay, so
    /// the checkpoint can never advance and the coverage repair can never
    /// become reachable, and because a block pin is tree-wide one such
    /// partition strands the whole tree's WAL for ever. Released, exactly as
    /// the genuinely-empty-partition case is released and on the same
    /// reasoning. See <see cref="ComputeEmptyWalPartitionsAsync"/>.
    /// </description></item>
    /// <item><description>
    /// <b>Never-written, scanned through</b> (issue #3453, only when
    /// <paramref name="releaseNeverWrittenScannedThrough"/> is set): the leaf's
    /// clock is <see cref="HybridLogicalClock.Zero"/>, the partition holds no
    /// live row, and its <b>persisted</b> checkpoint <c>X &gt;= 0</c> records
    /// only entries the replay skipped as another leaf's work. Resolves to
    /// <c>(Zero, X)</c>: for such a leaf every release above collapses to
    /// <c>(Zero, -1)</c>, the block pin itself, so this is the only release its
    /// encoding can express. Only the flush paths opt in; the activation seed
    /// does not (see <see cref="SeedDurableMaterialiserFrontierAsync"/>).
    /// </description></item>
    /// <item><description>
    /// <b>Data-bearing, not durably recoverable</b>: the partition holds live
    /// data whose only durable copy is the WAL prefix from offset 0, because
    /// it either never durably checkpointed (offset <c>&lt; 0</c>, the
    /// original #1490 un-checkpointed-data case) or checkpointed but has no
    /// snapshot covering the checkpointed prefix (the residual cold-restart
    /// prefix-loss case). Both retain the <see cref="HybridLogicalClock.Zero"/>
    /// block pin so the whole prefix stays replayable.
    /// </description></item>
    /// <item><description>
    /// <b>Data-bearing and snapshot-covered</b>: a durable snapshot covers the
    /// checkpointed prefix, so the pin authorises trimming up to
    /// <c>min(persistedCheckpoint, coveredOffset)</c> - never past what the
    /// snapshot durably holds. The guaranteed cadence capture (see
    /// <c>MaybeRunPeriodicSnapshotRecheckAsync</c>) ensures a block pin always
    /// has a bounded path to coverage and trim, so retention stays bounded.
    /// </description></item>
    /// </list>
    /// <para>
    /// <b>Invariant (issue #3476): a published offset never exceeds the
    /// partition's persisted projection checkpoint.</b> The <c>checkpoint &lt; 0</c>
    /// release tests above read the current checkpoint (persisted or pending),
    /// but the offset a data-bearing partition publishes is bounded by the
    /// PERSISTED checkpoint alone. A pending advance lives only in this
    /// activation's memory, while every replay starts from the persisted offset
    /// and faults when the WAL tail has passed persisted + 1, so a pin above it
    /// authorises exactly the trim that latches the leaf. Snapshot coverage
    /// above the persisted checkpoint does not relax the bound: a live
    /// activation's replay does not rehydrate from the blob, and the pin
    /// store's monotonic-max merge means an over-reported offset can never be
    /// withdrawn.
    /// </para>
    /// </summary>
    private (HybridLogicalClock Frontier, long Offset) ResolveDurablePinForPartition(
        int partition, HybridLogicalClock clock, bool[] partitionsWithLiveData,
        bool[]? partitionsWithEmptyWal = null,
        bool releaseNeverWrittenScannedThrough = false)
    {
        var checkpoint = GetCurrentCheckpointForPartition(partition);

        // Genuinely empty partition: it has applied NOTHING durably
        // (checkpoint < 0) AND holds no live cache row, so there is no
        // committed prefix to lose. Release any block and report the real
        // frontier so the ubiquitous empty-partition pins keep WAL trim live
        // (preserves #1490's empty-partition narrowness).
        //
        // The `checkpoint < 0` clause is load-bearing. Emptiness is decided
        // from the transient per-activation in-memory cache
        // (ComputePartitionsWithLiveData -> Cache.EnumerateRows), which does
        // NOT reflect this leaf's durable data in the window between activation
        // and cache hydration: a leaf can reactivate cold, find no snapshot to
        // rehydrate from, and report/flush its durable pin while its cache is
        // still empty even though the persisted projection checkpoint says the
        // prefix [0, checkpoint] was durably applied (tombstone reaping and
        // compaction can also empty the cache for a checkpointed partition
        // while the WAL prefix still has to replay). Trusting that empty cache
        // to RELEASE the block for a partition whose checkpoint is >= 0 is the
        // "fall off the log" hole: it authorises the shared-shard WAL GC to
        // trim a checkpointed, un-snapshotted prefix, after which the next cold
        // rebuild replays from offset 0 over a WAL whose prefix is gone and the
        // leaf comes up with its checkpoint below the WAL trim floor
        // (LeafProjectionStaleException). A partition with a durable checkpoint
        // must therefore be coverage-gated exactly like a cache-populated one,
        // regardless of whether the cache momentarily shows it empty.
        if (!partitionsWithLiveData[partition] && checkpoint < 0)
        {
            return (clock, checkpoint);
        }

        // Issue #3103: data-bearing, never checkpointed, and its WAL is EMPTY
        // (head offset 0 - no entry was ever appended). This is the same
        // "nothing to lose" case as the branch above, reached the other way
        // round: the rows are real but the WAL behind them is not, because a
        // WAL reset preserved the snapshot the leaf rehydrated them from.
        // Blocking here is not conservative, it is terminal - an empty WAL has
        // nothing to replay, so the starved-checkpoint drive returns NoAdvance
        // for ever, the checkpoint never leaves -1, and the coverage repair's
        // "checkpointed WITHOUT coverage" predicate stays permanently
        // unreachable. Since a block pin is tree-wide, one such partition
        // strands every leaf in the tree. Release it: an empty WAL holds no
        // committed prefix, so the block protects nothing at all.
        //
        // Narrow by construction. This releases ONLY on a proven-empty WAL; a
        // partition whose WAL holds unapplied entries keeps its block exactly
        // as before, so neither the #1535 no-loss invariant nor the #945
        // fall-off guard is weakened. The probe fails closed (see
        // ComputeEmptyWalPartitionsAsync), so an unreadable head keeps the
        // block too.
        if (checkpoint < 0
            && partitionsWithEmptyWal is not null
            && partition < partitionsWithEmptyWal.Length
            && partitionsWithEmptyWal[partition])
        {
            return (clock, checkpoint);
        }

        // Issue #3453: a never-written leaf (Clock == Zero) that has scanned
        // this partition through a PERSISTED checkpoint X >= 0 over entries it
        // skipped as another leaf's work, and holds no live row here. For such
        // a leaf the release branches above resolve to (Zero, -1), which is
        // byte-identical to the block pin, so no drive could ever lift it and
        // the WAL GC scheduler looped on NoAdvance. (Zero, X) is the only
        // release its encoding can express: an offset >= 0 puts the consumer in
        // the GC's offset coverage set, which already exempts a Zero frontier
        // from blocking (#3094), and the offset floor then retains everything
        // above X - including any later write that routes to this leaf.
        //
        // X is the PERSISTED checkpoint, never `checkpoint` above (which is
        // max(persisted, pending)). The pin store merges offsets by monotonic
        // max, so an over-report can never be withdrawn, and every replay -
        // including a cold rebuild - starts from the persisted offset (#3476).
        //
        // Opt-in, and only FlushDurableMaterialiserFrontierAsync opts in.
        // SeedDurableMaterialiserFrontierAsync deliberately does not: its
        // issue-2150 hazard is a Zero-clock leaf publishing a RAW checkpoint
        // beyond durable coverage from the activation seed. This arm publishes
        // the persisted checkpoint only, and only from the flush paths (the
        // starvation drive, which persists before it publishes, and the
        // deactivation barrier). A persisted scanned-through checkpoint is safe
        // for a never-written leaf because it owns no row whose only durable
        // copy is the WAL prefix at or below X: a cold activation with no
        // snapshot and an empty cache replays under the -1 sentinel, which the
        // fall-off detector exempts, and the #945 guard compares the WAL tail
        // against this same persisted checkpoint.
        if (releaseNeverWrittenScannedThrough
            && clock <= HybridLogicalClock.Zero
            && IsNeverWrittenScannedThroughPartition(partition, partitionsWithLiveData))
        {
            return (HybridLogicalClock.Zero, GetPersistedCheckpointForPartition(partition));
        }

        // Data-bearing partition (live cache rows) OR a durably-checkpointed
        // partition whose in-memory cache is momentarily empty. Either way the
        // checkpointed prefix's only durable copy - absent a snapshot - is the
        // WAL, so authorise trimming only as far as a durable snapshot covers.
        //
        // Issue #3476: and never past the PERSISTED checkpoint. `checkpoint`
        // above is max(persisted, pending), and the pending half is an advance
        // held only in this activation's memory. Every replay this activation
        // (or a crash-recovered successor that does not rehydrate a snapshot)
        // runs starts from the persisted offset, and the #945 guard and the
        // fall-off-log detector fault it when the WAL tail has passed
        // persisted + 1. A pin above the persisted checkpoint licenses exactly
        // that trim: the #3224 drive recheck restamps coverage up to the
        // pending checkpoint, so both arms of min(checkpoint, covered) could
        // sit above it, the GC trimmed to the pin, and the next replay latched
        // the leaf. Because the pin store merges by monotonic max, an
        // over-reported offset can never be taken back, so the clamp has to
        // hold at publication. The pending advance reaches the pin as soon as
        // it persists: FlushPendingCheckpointAsync republishes through
        // ReportCursorIfActiveAsync, and the starvation drive persists before
        // it republishes.
        var covered = DurableSnapshotCoverageForPartition(partition);
        var safeOffset = Math.Min(GetPersistedCheckpointForPartition(partition), covered);
        if (safeOffset < 0)
        {
            // Never checkpointed (checkpoint < 0, #1490), checkpointed only in
            // memory so far (a pending first checkpoint over a persisted -1,
            // #3476), OR checkpointed but uncovered (covered < 0, the residual
            // cold-restart prefix loss and the empty-cache misclassification):
            // the whole WAL from offset 0 is the only durable copy - retain the
            // Zero block pin.
            return (HybridLogicalClock.Zero, -1L);
        }

        return (clock, safeOffset);
    }
}