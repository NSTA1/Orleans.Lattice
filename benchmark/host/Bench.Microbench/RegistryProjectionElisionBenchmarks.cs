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
/// Orleans silo dependency.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class RegistryProjectionElisionBenchmarks
{
    private const string TreeId = "tree";
    private const string TargetTreeId = "target";

    private RegistryStore _registry = null!;

    /// <summary>The routing read being measured; see the class summary.</summary>
    [Params(Site.StateObserverOpen, Site.BootstrapFenceResolve, Site.SnapshotExport, Site.ResizeInitiate, Site.CompactionTopology, Site.MergeInitiate)]
    public Site Path { get; set; }

    /// <summary>How each simulated registry call completes; see the class summary.</summary>
    [Params(HopModel.Yield, HopModel.Delay1Ms)]
    public HopModel Hop { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _registry = new RegistryStore(Hop, new Entry("tree-copy", ShardMapVersion: 7));
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
            case Site.StateObserverOpen:
            case Site.BootstrapFenceResolve:
            case Site.ResizeInitiate:
            case Site.CompactionTopology:
            {
                var entry = await _registry.GetEntryAsync(TreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, entry?.ShardMapVersion ?? 0);
            }

            case Site.MergeInitiate:
            {
                var entry = await _registry.GetEntryAsync(TreeId);
                var target = await _registry.ResolveAsync(TargetTreeId);
                return Fold(entry?.PhysicalTreeId ?? TreeId, entry?.ShardMapVersion ?? 0) + target.Length;
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
    }

    /// <summary>How a simulated grain call completes.</summary>
    public enum HopModel
    {
        /// <summary>One yield: prices dispatch overhead only.</summary>
        Yield,

        /// <summary>One <c>Task.Delay(1)</c>: prices a remote round trip at the OS timer floor.</summary>
        Delay1Ms,
    }

    private sealed record Entry(string? PhysicalTreeId, long ShardMapVersion);

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
