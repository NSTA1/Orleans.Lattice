using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO.Hashing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-index background reconciliation coordinator for a tag index, keyed by
/// <c>{indexName}</c>. Reuses <see cref="CoordinatorGrain{TSelf}"/> for the
/// keepalive-reminder + phase-timer work-pump, and adds a recurring schedule
/// reminder that starts a digest-gated sweep on a configurable cadence.
/// <para>
/// Each sweep has two phases. <b>Probe</b> folds every covered tree's per-shard
/// <see cref="LeafProjectionDigest"/> into a compact fingerprint and compares it
/// against the baseline captured at the last successful reconcile; an equal
/// fingerprint means the tree is clean (orphan membership rows arise only from
/// subject-tree key deletions, which change the subject digest), so it is
/// skipped with no scans or writes. <b>Repair</b> deep-scans and repairs only
/// the divergent trees through the on-demand reconcile path, then advances each
/// repaired tree's baseline. A clean index therefore incurs only digest-probe
/// cost per cycle.
/// </para>
/// </summary>
internal sealed class TagIndexReconcileGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeTagIndexReconciliationOptions> optionsMonitor,
    ILatticeMergeModeResolver mergeModeResolver,
    ILatticeOriginClusterIdResolver originClusterIdResolver,
    ILogger<TagIndexReconcileGrain> logger,
    [PersistentState("tag-index-reconcile", LatticeOptions.StorageProviderName)]
    IPersistentState<TagIndexReconcileState> state)
    : CoordinatorGrain<TagIndexReconcileGrain>(context, reminderRegistry, logger), ITagIndexReconcileGrain
{
    private const string ScheduleReminderName = "tag-index-reconcile-schedule";
    private const string TagIndexDimension = "index";
    private const string IndexTreeIdPrefix = "tag-";

    private static readonly Counter<long> SweepsCounter =
        LatticeMetrics.Meter.CreateCounter<long>("orleans.lattice.tag_index.reconcile.sweeps", unit: "{sweep}",
            description: "Background tag-index reconciliation sweeps, tagged by outcome (clean, repaired, probe_only).");

    private static readonly Counter<long> TreesProbedCounter =
        LatticeMetrics.Meter.CreateCounter<long>("orleans.lattice.tag_index.reconcile.trees.probed", unit: "{tree}",
            description: "Covered trees whose digest fingerprint a reconciliation sweep probed.");

    private static readonly Counter<long> TreesMismatchedCounter =
        LatticeMetrics.Meter.CreateCounter<long>("orleans.lattice.tag_index.reconcile.trees.mismatched", unit: "{tree}",
            description: "Covered trees a reconciliation sweep found divergent from their digest baseline.");

    private static readonly Counter<long> OrphanRowsRemovedCounter =
        LatticeMetrics.Meter.CreateCounter<long>("orleans.lattice.tag_index.reconcile.orphan_rows.removed", unit: "{row}",
            description: "Orphan membership rows removed by background tag-index reconciliation.");

    private static readonly Histogram<double> SweepDurationHistogram =
        LatticeMetrics.Meter.CreateHistogram<double>("orleans.lattice.tag_index.reconcile.duration", unit: "ms",
            description: "Wall-clock duration of a background tag-index reconciliation sweep.");

    private long _sweepStartTimestamp;

    private string IndexName => Context.GrainId.Key.ToString()!;
    private LatticeTagIndexReconciliationOptions Options => optionsMonitor.Get(IndexName);

    // Resolves the membership convergence mode for the sibling index tree
    // (`tag-{indexName}`) and the dot-authoring replica id, so the coordinator's
    // orphan cleanup authors flag disables - never plain deletes - whenever the
    // operator declared the index tree under a flag merge mode. The mode comes
    // from the same per-tree resolver the commit path uses; the replica id comes
    // from the local origin-cluster-id seam (the index name is a stable
    // non-empty fallback if no cluster id is configured). In LwwRegister mode
    // the cleanup stays on the original plain-delete path.
    private LatticeTagIndexContext CreateCoordinatorContext()
    {
        var indexTreeId = string.Concat(IndexTreeIdPrefix, IndexName);
        var mode = mergeModeResolver.Resolve(indexTreeId);
        if (mode is LatticeMergeMode.OrFlag or LatticeMergeMode.RwFlag)
        {
            var replicaId = originClusterIdResolver.Resolve(indexTreeId);
            if (string.IsNullOrEmpty(replicaId))
            {
                replicaId = IndexName;
            }
            return LatticeTagIndexContext.CreateForCoordinator(grainFactory, IndexName, mode.Value, replicaId);
        }
        return LatticeTagIndexContext.CreateForCoordinator(grainFactory, IndexName);
    }

    protected override string KeepaliveReminderName => "tag-index-reconcile-keepalive";
    protected override bool InProgress => state.State.InProgress;
    protected override string LogContext => $"tag index {IndexName}";

    public async Task EnsureScheduleAsync()
    {
        var opts = Options;
        if (!opts.Enabled)
        {
            await UnregisterScheduleAsync();
            return;
        }
        var period = ClampInterval(opts.Interval);
        await ReminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: Context.GrainId,
            reminderName: ScheduleReminderName,
            dueTime: period,
            period: period);
    }

    public Task<bool> IsIdleAsync() => Task.FromResult(!state.State.InProgress);

    public async Task<TagReconcileReport> RunSweepAsync()
    {
        await InitSweepStateAsync();
        while (state.State.InProgress)
        {
            await ProcessNextPhaseAsync();
        }
        return CurrentReport();
    }

    /// <summary>
    /// Handles the recurring schedule reminder (start a sweep, drift-correct the
    /// period, or unregister when disabled) and delegates the keepalive reminder
    /// to the base coordinator.
    /// </summary>
    public override async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == ScheduleReminderName)
        {
            var opts = Options;
            if (!opts.Enabled)
            {
                await UnregisterScheduleAsync();
                return;
            }
            var desired = ClampInterval(opts.Interval);
            if (status.Period != desired)
            {
                await ReminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: Context.GrainId,
                    reminderName: ScheduleReminderName,
                    dueTime: desired,
                    period: desired);
            }
            if (!state.State.InProgress)
            {
                await BeginSweepAsync();
            }
            return;
        }
        await base.ReceiveReminder(reminderName, status);
    }

    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress)
        {
            return;
        }
        switch (state.State.Phase)
        {
            case TagIndexReconcilePhase.Probe:
                await ProcessProbeChunkAsync();
                break;
            case TagIndexReconcilePhase.Repair:
                await ProcessRepairChunkAsync();
                break;
            default:
                await FinishSweepAsync();
                break;
        }
    }

    private async Task BeginSweepAsync()
    {
        await InitSweepStateAsync();
        await StartCoordinatorAsync();
    }

    private async Task InitSweepStateAsync()
    {
        _sweepStartTimestamp = Stopwatch.GetTimestamp();
        var ctx = CreateCoordinatorContext();
        var covered = await ctx.GetCoveredTreesAsync(CancellationToken.None);

        PruneStaleBaselines(covered);

        state.State.InProgress = true;
        state.State.Phase = TagIndexReconcilePhase.Probe;
        state.State.CoveredTrees = covered.ToList();
        state.State.NextProbeIndex = 0;
        state.State.DirtyTrees = [];
        state.State.NextRepairIndex = 0;
        state.State.PendingBaselines.Clear();
        state.State.ProbeOnlySweep = Options.ProbeOnly;
        state.State.TreesProbed = 0;
        state.State.TreesMismatched = 0;
        state.State.KeysScanned = 0;
        state.State.MembershipRowsScanned = 0;
        state.State.OrphanRowsRemoved = 0;
        await state.WriteStateAsync();
    }

    private void PruneStaleBaselines(IReadOnlyList<string> covered)
    {
        if (state.State.Baselines.Count == 0)
        {
            return;
        }
        var keep = new HashSet<string>(covered, StringComparer.Ordinal);
        if (keep.Count == state.State.Baselines.Count
            && state.State.Baselines.Keys.All(keep.Contains))
        {
            return;
        }
        var stale = state.State.Baselines.Keys.Where(t => !keep.Contains(t)).ToList();
        foreach (var treeId in stale)
        {
            state.State.Baselines.Remove(treeId);
        }
    }

    private async Task ProcessProbeChunkAsync()
    {
        var chunk = Math.Max(1, Options.ChunkSize);
        var processed = 0;
        while (processed < chunk && state.State.NextProbeIndex < state.State.CoveredTrees.Count)
        {
            var treeId = state.State.CoveredTrees[state.State.NextProbeIndex];
            state.State.NextProbeIndex++;
            state.State.TreesProbed++;
            processed++;

            var fingerprint = await ComputeFingerprintAsync(treeId, CancellationToken.None);
            var clean = fingerprint is not null
                && state.State.Baselines.TryGetValue(treeId, out var baseline)
                && baseline.AsSpan().SequenceEqual(fingerprint);
            if (clean)
            {
                continue;
            }

            state.State.TreesMismatched++;
            state.State.DirtyTrees.Add(treeId);
            if (fingerprint is not null)
            {
                state.State.PendingBaselines[treeId] = fingerprint;
            }
            else
            {
                // Digest unavailable: cannot establish a baseline, so drop any
                // stale one and keep reconciling this tree every sweep.
                state.State.PendingBaselines.Remove(treeId);
            }
        }

        if (state.State.NextProbeIndex >= state.State.CoveredTrees.Count)
        {
            if (state.State.ProbeOnlySweep || state.State.DirtyTrees.Count == 0)
            {
                await FinishSweepAsync();
                return;
            }
            state.State.Phase = TagIndexReconcilePhase.Repair;
            state.State.NextRepairIndex = 0;
        }
        await state.WriteStateAsync();
    }

    private async Task ProcessRepairChunkAsync()
    {
        if (state.State.NextRepairIndex >= state.State.DirtyTrees.Count)
        {
            await FinishSweepAsync();
            return;
        }

        var treeId = state.State.DirtyTrees[state.State.NextRepairIndex];
        state.State.NextRepairIndex++;

        var ctx = CreateCoordinatorContext();
        var report = await ctx.ReconcileSubjectAsync(treeId, null, null, CancellationToken.None);
        state.State.KeysScanned += report.KeysScanned;
        state.State.MembershipRowsScanned += report.MembershipRowsScanned;
        state.State.OrphanRowsRemoved += report.OrphanRowsRemoved;

        if (state.State.PendingBaselines.TryGetValue(treeId, out var fingerprint))
        {
            state.State.Baselines[treeId] = fingerprint;
        }

        if (state.State.NextRepairIndex >= state.State.DirtyTrees.Count)
        {
            await FinishSweepAsync();
            return;
        }
        await state.WriteStateAsync();
    }

    private async Task FinishSweepAsync()
    {
        var report = CurrentReport();
        EmitMetrics(report);

        state.State.InProgress = false;
        state.State.Phase = TagIndexReconcilePhase.Idle;
        state.State.CoveredTrees = [];
        state.State.DirtyTrees = [];
        state.State.PendingBaselines.Clear();
        state.State.NextProbeIndex = 0;
        state.State.NextRepairIndex = 0;
        await state.WriteStateAsync();

        await CompleteCoordinatorAsync();
    }

    private TagReconcileReport CurrentReport() => new(
        state.State.TreesProbed,
        state.State.KeysScanned,
        state.State.MembershipRowsScanned,
        state.State.OrphanRowsRemoved);

    private void EmitMetrics(TagReconcileReport report)
    {
        var outcome = state.State.ProbeOnlySweep
            ? "probe_only"
            : state.State.TreesMismatched == 0 ? "clean" : "repaired";

        var indexTags = new KeyValuePair<string, object?>[] { new(TagIndexDimension, IndexName) };
        var outcomeTags = new KeyValuePair<string, object?>[]
        {
            new(TagIndexDimension, IndexName),
            new(LatticeMetrics.TagOutcome, outcome),
        };

        SweepsCounter.Add(1, outcomeTags);
        TreesProbedCounter.Add(state.State.TreesProbed, indexTags);
        TreesMismatchedCounter.Add(state.State.TreesMismatched, indexTags);
        OrphanRowsRemovedCounter.Add(report.OrphanRowsRemoved, indexTags);
        var elapsedMs = (Stopwatch.GetTimestamp() - _sweepStartTimestamp) * 1000.0 / Stopwatch.Frequency;
        SweepDurationHistogram.Record(elapsedMs, indexTags);

        Logger.LogInformation(
            "Tag-index reconcile sweep for {Index} completed: outcome={Outcome} probed={Probed} mismatched={Mismatched} keysScanned={Keys} rowsScanned={Rows} orphansRemoved={Orphans}",
            IndexName, outcome, state.State.TreesProbed, state.State.TreesMismatched,
            report.KeysScanned, report.MembershipRowsScanned, report.OrphanRowsRemoved);
    }

    /// <summary>
    /// Folds every physical shard's <see cref="LeafProjectionDigest"/> of the
    /// subject tree into a 16-byte fingerprint. Returns <see langword="null"/>
    /// when the tree is unresolvable or its projection digest is disabled, in
    /// which case the caller treats the tree as divergent (it cannot be gated).
    /// </summary>
    private async Task<byte[]?> ComputeFingerprintAsync(string treeId, CancellationToken cancellationToken)
    {
        var tree = grainFactory.GetGrain<ILattice>(treeId);
        IReadOnlyList<int> shards;
        try
        {
            var routing = await tree.GetRoutingAsync(cancellationToken);
            shards = routing.Map.GetPhysicalShardIndices();
        }
        catch
        {
            return null;
        }

        var hash = new XxHash128();
        var scratch = new byte[4];
        foreach (var shardIndex in shards)
        {
            LeafProjectionDigest digest;
            try
            {
                digest = await tree.GetLeafProjectionDigestAsync(shardIndex, cancellationToken);
            }
            catch
            {
                return null;
            }

            // The per-shard digest Hash already folds in EntryCount and
            // CheckpointOffset (it is XxHash128(running_xor || entryCount ||
            // checkpointOffset)), so folding the canonical Hash - qualified by
            // the shard index and the contribution-function Version - is
            // sufficient to detect any subject-tree change without re-hashing
            // the count and offset a second time.
            BinaryPrimitives.WriteInt32LittleEndian(scratch, shardIndex);
            hash.Append(scratch);
            hash.Append(digest.Hash ?? []);
            BinaryPrimitives.WriteInt32LittleEndian(scratch, digest.Version);
            hash.Append(scratch);
        }
        return hash.GetHashAndReset();
    }

    private async Task UnregisterScheduleAsync()
    {
        try
        {
            var reminder = await ReminderRegistry.GetReminder(Context.GrainId, ScheduleReminderName);
            if (reminder is not null)
            {
                await ReminderRegistry.UnregisterReminder(Context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Failed to unregister tag-index reconcile schedule for {Context}", LogContext);
        }
    }

    private static TimeSpan ClampInterval(TimeSpan interval) =>
        interval < LatticeTagIndexReconciliationOptions.MinimumInterval
            ? LatticeTagIndexReconciliationOptions.MinimumInterval
            : interval;
}
