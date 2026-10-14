using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the registry round trips several routing reads paid for projections
/// of a single registry entry. <c>ILatticeRegistry.ResolveAsync(id)</c> is
/// <c>GetEntryAsync(id)?.PhysicalTreeId ?? id</c> and <c>GetShardMapAsync(id)</c>
/// is <c>GetEntryAsync(id)?.ShardMap</c>, so a caller that awaited them beside,
/// or instead of, an entry read paid extra serial registry hops for data the
/// entry already carries. The shipped callers read the entry once and derive
/// both.
/// <list type="bullet">
/// <item><see cref="Site.StateObserverOpen"/> - <c>LatticeStateObserver</c>
/// opening a change subscription: Resolve + GetEntry -> GetEntry.</item>
/// <item><see cref="Site.BootstrapFenceResolve"/> - <c>TreeBootstrapReadFence.ResolveAsync</c>:
/// Resolve + GetEntry -> GetEntry.</item>
/// <item><see cref="Site.SnapshotExport"/> - one <c>LatticeSnapshotProvider</c>
/// export: the open and close generation captures (GetEntry + Resolve +
/// GetShardMap each -> GetEntry) and the tombstone and prepared passes
/// (Resolve + GetShardMap each -> GetEntry), 10 serial reads -> 4.</item>
/// <item><see cref="Site.ResizeInitiate"/> - <c>TreeResizeGrain</c> initiating a
/// resize (and its bootstrap-fence probe): Resolve + GetEntry -> GetEntry.</item>
/// <item><see cref="Site.CompactionTopology"/> - <c>TombstoneCompactionGrain</c>
/// resolving a pass topology: Resolve + options GetEntry + GetShardMap -> GetEntry.</item>
/// <item><see cref="Site.MergeInitiate"/> - <c>TreeMergeGrain</c> initiating a
/// merge: source Resolve + options GetEntry + GetShardMap, plus the target
/// Resolve -> source GetEntry plus the target Resolve, 4 serial reads -> 2.</item>
/// <item><see cref="Site.SplitInitiate"/> - <c>TreeShardSplitGrain</c> initiating a
/// shard split: alias Resolve + options GetEntry + GetShardMap + fence GetEntry
/// -> one GetEntry carrying map, alias and fence, 4 serial reads -> 1.</item>
/// <item><see cref="Site.ConsolidationInitiate"/> - <c>TreeShardConsolidationGrain</c>
/// initiating a fold: options GetEntry + GetShardMap + alias Resolve + fence
/// GetEntry -> one GetEntry, 4 serial reads -> 1.</item>
/// <item><see cref="Site.ReshardSplitTick"/> - one <c>TreeReshardGrain</c> grow
/// tick: options GetEntry + GetShardMap + Resolve -> options GetEntry + GetEntry,
/// 3 serial reads -> 2, paid on every tick of a reshard.</item>
/// <item><see cref="Site.ReshardConsolidateTick"/> - one <c>TreeReshardGrain</c>
/// shrink tick: options GetEntry + Resolve + GetShardMap -> options GetEntry +
/// GetEntry, 3 serial reads -> 2, paid on every tick of a shrink.</item>
/// <item><see cref="Site.StalePendingSettle"/> - one <c>StalePendingClearer</c>
/// pass (the pre-export capture or the post-drain clear): Resolve + GetShardMap
/// -> GetEntry, 2 serial reads -> 1.</item>
/// <item><see cref="Site.BootstrapReceiverScan"/> - one
/// <c>LatticeBootstrapCoordinatorGrain</c> receiver scan (pre-capture,
/// foreign-delete reconcile, absent-source-row probe, poisoned-saga discard):
/// Resolve + GetShardMap -> GetEntry, 2 serial reads -> 1.</item>
/// <item><see cref="Site.SnapshotInitiate"/> - <c>TreeSnapshotGrain</c>
/// initiating a standalone snapshot: routing GetEntry + source Resolve ->
/// GetEntry, 2 serial reads -> 1.</item>
/// <item><see cref="Site.RestoreShadowValidate"/> - <c>LatticeBackupRestoreService</c>
/// validating a restore shadow on commit or revert: shadow GetEntry + target
/// Resolve -> shadow GetEntry, the alias read only when the shadow's provenance
/// stamp does not decide it, 2 serial reads -> 1.</item>
/// <item><see cref="Site.CutoverPrepare"/> - a shadow-restore commit or a schema
/// remediation cutover preparing its alias swap: target Resolve + the prepare's
/// logical and destination GetEntry -> the two GetEntry, the replaced physical
/// tree derived from the logical entry, 3 serial reads -> 2.</item>
/// <item><see cref="Site.ResizeEmptyProbe"/> - resize emptiness routing:
/// options GetEntry + GetShardMap + Resolve -> one fresh GetEntry.</item>
/// <item><see cref="Site.ResizeIntentOptions"/> - resize undo capture:
/// options GetEntry + GetEntry -> one fresh GetEntry.</item>
/// <item><see cref="Site.DeletionShardCensus"/> - allocated-shard census:
/// options GetEntry + GetEntry -> one fresh GetEntry, excluding the unchanged
/// owner read for an aliased copy.</item>
/// <item><see cref="Site.StatsCacheHit"/> - cached diagnostics: options
/// GetEntry -> configured TTL, 1 serial read -> 0.</item>
/// <item><see cref="Site.WalByteSizes"/> - one WAL partition's accounting:
/// physical + retained grain calls -> combined pair, 2 serial calls -> 1.
/// This models grain dispatch only, not the unchanged provider I/O.</item>
/// <item><see cref="Site.WalUsageMiss"/> - a complete-pin WAL report miss:
/// initial options + topology + final policy -> topology/options + final
/// policy, 3 serial reads -> 2. The final fresh policy read stays.</item>
/// <item><see cref="Site.MergeFrontDoor"/> - merge existence, options and intent
/// reuse one source entry, plus the unchanged target read: 4 calls -> 2.</item>
/// <item><see cref="Site.ResizeUndoFence"/> - an unaliased complete-pin resize
/// undo resolves its bootstrap-fence topology and options from one entry:
/// 2 calls -> 1. Aliased owners and incomplete pins retain their reads.</item>
/// <item><see cref="Site.HotShardTick"/> - one <c>HotShardMonitorGrain</c>
/// sampling tick: the start-of-tick registration GetEntry, then Resolve +
/// GetShardMap -> the registration GetEntry plus one GetEntry, 3 serial reads
/// -> 2, paid on every tick of every autonomic tree.</item>
/// <item><see cref="Site.HealingSweep"/> - one <c>ShardHealingOrchestratorGrain</c>
/// sweep: options GetEntry + GetShardMap + Resolve -> one GetEntry, 3 serial
/// reads -> 1, paid on every sweep of every autonomic tree.</item>
/// <item><see cref="Site.AbsentParticipantDiscard"/> - a replicated cross-tree
/// terminal dropped for a de-enrolled tree discarding its pending bucket:
/// Exists + Resolve + GetShardMap -> one GetEntry, 3 serial reads -> 1.</item>
/// </list>
/// <para>
/// <b>Read the hop model.</b> <see cref="HopModel.Yield"/> prices only the
/// dispatch overhead of each awaited call. <see cref="HopModel.Delay1Ms"/>
/// models a remote round trip with <c>Task.Delay(1)</c>, which the OS timer
/// rounds up (about 15.6 ms on Windows, about 1 ms on Linux), so its absolute
/// times are platform-dependent and its <b>ratio</b> is the evidence: it tracks
/// the serial registry hops removed from each path.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=registryprojection</c> (or
/// <c>--suite registryprojection</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency. <c>--suite registryoptions</c> selects just the
/// three complete-pin resize/deletion lanes; missing or incomplete pins retain
/// the seeding fallback and are not represented by these timing lanes.
/// <c>--suite registrymaintenance</c> selects just the hot-shard tick, healing
/// sweep and absent-participant discard lanes.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class RegistryProjectionElisionBenchmarks
{
    private const string TreeId = "tree";
    private const string TargetTreeId = "target";
    private const string ShadowTreeId = "shadow";

    private RegistryStore _registry = null!;

    /// <summary>The routing read being measured; see the class summary.</summary>
    [Params(Site.StateObserverOpen, Site.BootstrapFenceResolve, Site.SnapshotExport, Site.ResizeInitiate, Site.CompactionTopology, Site.MergeInitiate, Site.SplitInitiate, Site.ConsolidationInitiate, Site.ReshardSplitTick, Site.ReshardConsolidateTick, Site.StalePendingSettle, Site.BootstrapReceiverScan, Site.SnapshotInitiate, Site.RestoreShadowValidate, Site.CutoverPrepare, Site.ResizeEmptyProbe, Site.ResizeIntentOptions, Site.DeletionShardCensus, Site.StatsCacheHit, Site.WalByteSizes, Site.WalUsageMiss, Site.MergeFrontDoor, Site.ResizeUndoFence, Site.HotShardTick, Site.HealingSweep, Site.AbsentParticipantDiscard)]
    public Site Path { get; set; }

    /// <summary>How each simulated registry call completes; see the class summary.</summary>
    [Params(HopModel.Yield, HopModel.Delay1Ms)]
    public HopModel Hop { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _registry = new RegistryStore(Hop, new Entry("tree-copy", ShardMapVersion: 7, RestoreShadowOf: TargetTreeId));
        var before = Separate_Projection_Reads_Async().GetAwaiter().GetResult();
        var after = Single_Entry_Read_Async().GetAwaiter().GetResult();
        if (before != after)
        {
            throw new InvalidOperationException($"Registry projection lanes disagree: before={before}, after={after}.");
        }
    }

    [Benchmark(Baseline = true, Description = "separate Resolve/GetShardMap registry reads")]
    public async Task<long> Separate_Projection_Reads_Async()
    {
        switch (Path)
        {
            case Site.HotShardTick:
            {
                var registered = await _registry.GetEntryAsync(TreeId);
                var physical = await _registry.ResolveAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                return Fold(physical, (map ?? registered?.ShardMapVersion) ?? 0);
            }

            case Site.HealingSweep:
            {
                var options = await _registry.GetEntryAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                var physical = await _registry.ResolveAsync(TreeId);
                return Fold(physical, (map ?? options?.ShardMapVersion) ?? 0);
            }

            case Site.AbsentParticipantDiscard:
            {
                if (!await _registry.ExistsAsync(TreeId))
                {
                    return 0;
                }

                var physical = await _registry.ResolveAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                return Fold(physical, map ?? 0);
            }

            case Site.MergeFrontDoor:
            {
                await _registry.GetEntryAsync(TreeId); // existence
                var options = await _registry.GetEntryAsync(TreeId);
                var source = await _registry.GetEntryAsync(TreeId);
                var target = await _registry.ResolveAsync(TargetTreeId);
                return Fold(source?.PhysicalTreeId ?? TreeId, source?.ShardMapVersion ?? options?.ShardMapVersion ?? 0) + target.Length;
            }

            case Site.ResizeUndoFence:
            {
                var entry = await _registry.GetEntryAsync(TreeId);
                var options = await _registry.GetEntryAsync(TreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, options?.ShardMapVersion ?? 0);
            }

            case Site.StatsCacheHit:
            {
                await _registry.GetEntryAsync(TreeId);
                return Fold("cached-report", 7);
            }

            case Site.WalByteSizes:
            {
                var physical = await _registry.GetEntryAsync(TreeId);
                var retained = await _registry.GetEntryAsync(TreeId);
                return Fold(physical?.PhysicalTreeId ?? TreeId, retained?.ShardMapVersion ?? 0);
            }

            case Site.WalUsageMiss:
            {
                await _registry.GetEntryAsync(TreeId);
                var topology = await _registry.GetEntryAsync(TreeId);
                var policy = await _registry.GetEntryAsync(TreeId);
                return Fold(topology?.PhysicalTreeId ?? TreeId, policy?.ShardMapVersion ?? 0);
            }

            case Site.ResizeEmptyProbe:
            {
                var options = await _registry.GetEntryAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                var physical = await _registry.ResolveAsync(TreeId);
                return Fold(physical, (map ?? options?.ShardMapVersion) ?? 0);
            }

            case Site.ResizeIntentOptions:
            case Site.DeletionShardCensus:
            {
                var options = await _registry.GetEntryAsync(TreeId);
                var entry = await _registry.GetEntryAsync(TreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, (entry?.ShardMapVersion ?? options?.ShardMapVersion) ?? 0);
            }

            case Site.StateObserverOpen:
            case Site.BootstrapFenceResolve:
            case Site.ResizeInitiate:
            {
                var physical = await _registry.ResolveAsync(TreeId);
                var entry = await _registry.GetEntryAsync(TreeId);
                return Fold(physical, entry?.ShardMapVersion ?? 0);
            }

            case Site.CompactionTopology:
            {
                var physical = await _registry.ResolveAsync(TreeId);
                var options = await _registry.GetEntryAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                return Fold(physical, (map ?? options?.ShardMapVersion) ?? 0);
            }

            case Site.MergeInitiate:
            {
                var source = await _registry.ResolveAsync(TreeId);
                var target = await _registry.ResolveAsync(TargetTreeId);
                var options = await _registry.GetEntryAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                return Fold(source, (map ?? options?.ShardMapVersion) ?? 0) + target.Length;
            }

            case Site.SplitInitiate:
            case Site.ConsolidationInitiate:
            {
                var physical = await _registry.ResolveAsync(TreeId);
                var options = await _registry.GetEntryAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                var fence = await _registry.GetEntryAsync(TreeId);
                var admitted = (fence?.PhysicalTreeId ?? TreeId) == physical;
                return Fold(physical, (map ?? options?.ShardMapVersion) ?? 0) + (admitted ? 1 : 0);
            }

            case Site.ReshardSplitTick:
            case Site.ReshardConsolidateTick:
            {
                var options = await _registry.GetEntryAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                var physical = await _registry.ResolveAsync(TreeId);
                return Fold(physical, (map ?? options?.ShardMapVersion) ?? 0);
            }

            case Site.StalePendingSettle:
            case Site.BootstrapReceiverScan:
            {
                var physical = await _registry.ResolveAsync(TreeId);
                var map = await _registry.GetShardMapAsync(TreeId);
                return Fold(physical, map ?? 0);
            }

            case Site.SnapshotInitiate:
            {
                var routing = await _registry.GetEntryAsync(TreeId);
                var physical = await _registry.ResolveAsync(TreeId);
                return Fold(physical, routing?.ShardMapVersion ?? 0);
            }

            case Site.RestoreShadowValidate:
            {
                var shadow = await _registry.GetEntryAsync(ShadowTreeId);
                var physical = await _registry.ResolveAsync(TargetTreeId);
                var admitted = shadow?.RestoreShadowOf == TargetTreeId || physical == ShadowTreeId;
                return Fold(ShadowTreeId, shadow?.ShardMapVersion ?? 0) + (admitted ? 1 : 0);
            }

            case Site.CutoverPrepare:
            {
                var physical = await _registry.ResolveAsync(TreeId);
                var logical = await _registry.GetEntryAsync(TreeId);
                var destination = await _registry.GetEntryAsync(ShadowTreeId);
                return Fold(physical, (destination?.ShardMapVersion ?? 0) + (logical?.ShardMapVersion ?? 0));
            }

            default:
            {
                var total = 0L;
                for (var capture = 0; capture < 2; capture++)
                {
                    var entry = await _registry.GetEntryAsync(TreeId);
                    var physical = await _registry.ResolveAsync(TreeId);
                    var map = await _registry.GetShardMapAsync(TreeId);
                    total += Fold(physical, (map ?? entry?.ShardMapVersion) ?? 0);
                }

                for (var pass = 0; pass < 2; pass++)
                {
                    var physical = await _registry.ResolveAsync(TreeId);
                    var map = await _registry.GetShardMapAsync(TreeId);
                    total += Fold(physical, map ?? 0);
                }

                return total;
            }
        }
    }

    [Benchmark(Description = "one GetEntry read, projections derived")]
    public async Task<long> Single_Entry_Read_Async()
    {
        switch (Path)
        {
            case Site.StatsCacheHit:
                return Fold("cached-report", 7);

            case Site.HotShardTick:
            {
                await _registry.GetEntryAsync(TreeId); // registration
                var entry = await _registry.GetEntryAsync(TreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, entry?.ShardMapVersion ?? 0);
            }

            case Site.AbsentParticipantDiscard:
            {
                var entry = await _registry.GetEntryAsync(TreeId);
                if (entry is null)
                {
                    return 0;
                }

                return Fold(entry.PhysicalTreeId ?? TreeId, entry.ShardMapVersion);
            }

            case Site.WalByteSizes:
            {
                var sizes = await _registry.GetEntryAsync(TreeId);
                return Fold(sizes?.PhysicalTreeId ?? TreeId, sizes?.ShardMapVersion ?? 0);
            }

            case Site.WalUsageMiss:
            {
                var topology = await _registry.GetEntryAsync(TreeId);
                var policy = await _registry.GetEntryAsync(TreeId);
                return Fold(topology?.PhysicalTreeId ?? TreeId, policy?.ShardMapVersion ?? 0);
            }

            case Site.StateObserverOpen:
            case Site.BootstrapFenceResolve:
            case Site.ResizeInitiate:
            case Site.CompactionTopology:
            case Site.StalePendingSettle:
            case Site.BootstrapReceiverScan:
            case Site.SnapshotInitiate:
            case Site.ResizeEmptyProbe:
            case Site.ResizeIntentOptions:
            case Site.DeletionShardCensus:
            case Site.ResizeUndoFence:
            case Site.HealingSweep:
            {
                var entry = await _registry.GetEntryAsync(TreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, entry?.ShardMapVersion ?? 0);
            }

            case Site.MergeInitiate:
            case Site.MergeFrontDoor:
            {
                var entry = await _registry.GetEntryAsync(TreeId);
                var target = await _registry.ResolveAsync(TargetTreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, entry?.ShardMapVersion ?? 0) + target.Length;
            }

            case Site.RestoreShadowValidate:
            {
                var shadow = await _registry.GetEntryAsync(ShadowTreeId);
                var admitted = shadow?.RestoreShadowOf == TargetTreeId
                    || await _registry.ResolveAsync(TargetTreeId) == ShadowTreeId;
                return Fold(ShadowTreeId, shadow?.ShardMapVersion ?? 0) + (admitted ? 1 : 0);
            }

            case Site.CutoverPrepare:
            {
                var logical = await _registry.GetEntryAsync(TreeId);
                var destination = await _registry.GetEntryAsync(ShadowTreeId);
                return Fold(
                    logical?.PhysicalTreeId ?? TreeId,
                    (destination?.ShardMapVersion ?? 0) + (logical?.ShardMapVersion ?? 0));
            }

            case Site.SplitInitiate:
            case Site.ConsolidationInitiate:
            {
                var entry = await _registry.GetEntryAsync(TreeId);
                var physical = entry?.PhysicalTreeId ?? TreeId;
                var admitted = (entry?.PhysicalTreeId ?? TreeId) == physical;
                return Fold(physical, entry?.ShardMapVersion ?? 0) + (admitted ? 1 : 0);
            }

            case Site.ReshardSplitTick:
            case Site.ReshardConsolidateTick:
            {
                var options = await _registry.GetEntryAsync(TreeId);
                var entry = await _registry.GetEntryAsync(TreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, (entry?.ShardMapVersion ?? options?.ShardMapVersion) ?? 0);
            }

            default:
            {
                var total = 0L;
                for (var read = 0; read < 4; read++)
                {
                    var entry = await _registry.GetEntryAsync(TreeId);
                    total += Fold(entry?.PhysicalTreeId ?? TreeId, entry?.ShardMapVersion ?? 0);
                }

                return total;
            }
        }
    }

    private static long Fold(string physical, long shardMapVersion) => physical.Length + shardMapVersion;

    /// <summary>The routing read being measured.</summary>
    public enum Site
    {
        /// <summary>A change-observation subscription opening.</summary>
        StateObserverOpen,

        /// <summary>A bootstrap read fence resolving the routed shards.</summary>
        BootstrapFenceResolve,

        /// <summary>One snapshot export's registry routing reads.</summary>
        SnapshotExport,

        /// <summary>A resize initiation capturing the old alias and entry.</summary>
        ResizeInitiate,

        /// <summary>A compaction pass resolving its shard topology.</summary>
        CompactionTopology,

        /// <summary>A merge initiation resolving source and target topology.</summary>
        MergeInitiate,

        /// <summary>A shard split initiation reading its map, alias and commit fence.</summary>
        SplitInitiate,

        /// <summary>A shard consolidation initiation reading its map, alias and commit fence.</summary>
        ConsolidationInitiate,

        /// <summary>One reshard grow tick reading its options, map and physical tree.</summary>
        ReshardSplitTick,

        /// <summary>One reshard shrink tick reading its options, map and physical tree.</summary>
        ReshardConsolidateTick,

        /// <summary>One stale-pending clearer pass resolving the receiver's physical shards.</summary>
        StalePendingSettle,

        /// <summary>One bootstrap coordinator receiver scan resolving its physical shards.</summary>
        BootstrapReceiverScan,

        /// <summary>A standalone snapshot initiation reading its routing entry and physical source.</summary>
        SnapshotInitiate,

        /// <summary>A restore commit or revert validating a shadow it was handed against its target.</summary>
        RestoreShadowValidate,

        /// <summary>A restore commit or remediation cutover preparing its alias swap.</summary>
        CutoverPrepare,

        /// <summary>A resize checking emptiness before choosing its fast path.</summary>
        ResizeEmptyProbe,

        /// <summary>A resize capturing options and its predecessor for undo.</summary>
        ResizeIntentOptions,

        /// <summary>A deletion resolving its contiguous allocated-shard range.</summary>
        DeletionShardCensus,

        /// <summary>A cache hit in the per-tree diagnostics aggregator.</summary>
        StatsCacheHit,

        /// <summary>One WAL partition's physical and retained accounting calls.</summary>
        WalByteSizes,

        /// <summary>A complete-pin WAL report refresh, including final policy.</summary>
        WalUsageMiss,

        /// <summary>A merge front-door existence/options/intent pipeline.</summary>
        MergeFrontDoor,

        /// <summary>A resize undo bootstrap-fence topology read on a complete unaliased owner.</summary>
        ResizeUndoFence,

        /// <summary>One hot-shard monitor sampling tick reading registration, physical tree and map.</summary>
        HotShardTick,

        /// <summary>One shard-healing sweep reading options, map and physical tree.</summary>
        HealingSweep,

        /// <summary>A de-enrolled tree's dropped cross-tree terminal discarding its pending bucket.</summary>
        AbsentParticipantDiscard,
    }

    /// <summary>How a simulated grain call completes.</summary>
    public enum HopModel
    {
        /// <summary>One yield: prices dispatch overhead only.</summary>
        Yield,

        /// <summary>One <c>Task.Delay(1)</c>: prices a remote round trip at the OS timer floor.</summary>
        Delay1Ms,
    }

    private sealed record Entry(string? PhysicalTreeId, long ShardMapVersion, string? RestoreShadowOf = null);

    /// <summary>
    /// Stands in for the registry grain: every call completes asynchronously per
    /// the hop model, and the projections are computed from the entry exactly as
    /// the registry grain computes them.
    /// </summary>
    private sealed class RegistryStore(HopModel hop, Entry entry)
    {
        public async Task<Entry?> GetEntryAsync(string id)
        {
            await HopAsync();
            return entry;
        }

        public async Task<bool> ExistsAsync(string id)
        {
            await HopAsync();
            return entry is not null;
        }

        public async Task<string> ResolveAsync(string id)
        {
            await HopAsync();
            return entry.PhysicalTreeId ?? id;
        }

        public async Task<long?> GetShardMapAsync(string id)
        {
            await HopAsync();
            return entry.ShardMapVersion;
        }

        private async Task HopAsync()
        {
            if (hop == HopModel.Delay1Ms)
            {
                await Task.Delay(1);
            }
            else
            {
                await Task.Yield();
            }
        }
    }
}
