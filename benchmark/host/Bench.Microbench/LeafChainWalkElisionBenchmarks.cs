using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the leaf round trips a leaf-chain walk paid for reading a leaf's
/// payload and its next-sibling pointer as two separate grain calls. The shipped
/// callers read both from one combined leaf call.
/// <list type="bullet">
/// <item><see cref="Site.LiveRawWalk"/> - the replication local version-vector
/// seeder and the bootstrap coordinator's receiver scans:
/// <c>GetLiveRawEntriesAsync</c> + <c>GetNextSiblingAsync</c> ->
/// <c>GetLiveRawEntriesAndNextSiblingAsync</c>, 2 calls per leaf -> 1.</item>
/// <item><see cref="Site.PendingWalk"/> - the stale-pending clearer and the
/// bootstrap coordinator's pending capture: <c>GetPendingMutationsForSlotsAsync</c>
/// + <c>GetNextSiblingAsync</c> -> <c>GetPendingMutationsForSlotsAndNextSiblingAsync</c>,
/// 2 calls per leaf -> 1.</item>
/// <item><see cref="Site.DiscardWalk"/> - the atomic-write abandoned-copy cleanup,
/// the replication absent-participant discard and the bootstrap poisoned-saga
/// discard: one <c>DiscardPendingTransactionAsync</c> per transaction id plus
/// <c>GetNextSiblingAsync</c> -> <c>DiscardPendingTransactionsAndGetNextSiblingAsync</c>,
/// k + 1 calls per leaf -> 1 (and one state write per leaf instead of up to k).</item>
/// <item><see cref="Site.KeyRangeWalk"/> - the bounded leaf walk every
/// maintenance drain advances through (compaction, merge, split and
/// consolidation drains, snapshot copy, bounded diagnostics):
/// <c>GetNextSiblingAsync</c> + <c>GetKeyRangeAsync</c> on every non-tail leaf ->
/// <c>GetKeyRangeAndNextSiblingAsync</c>, 2 calls per leaf -> 1.</item>
/// <item><see cref="Site.DeltaWalk"/> - the replication snapshot provider's
/// tombstone capture: <c>GetDeltaSinceAsync</c> + <c>GetNextSiblingAsync</c> ->
/// <c>GetDeltaSinceAndNextSiblingAsync</c>, 2 calls per leaf -> 1. (Its
/// prepared-pending capture is the <see cref="Site.PendingWalk"/> shape.)</item>
/// <item><see cref="Site.ShardScanWalk"/> - the shard root's bounded count,
/// emptiness-probe, range-clock, projection-rebuild and range-bounded page-fill
/// walks, which read a per-leaf payload and then the leaf's key range and
/// next-sibling pointer: payload + <c>GetKeyRangeAsync</c> +
/// <c>GetNextSiblingAsync</c> -> payload + <c>GetKeyRangeAndNextSiblingAsync</c>,
/// 3 calls per leaf -> 2.</item>
/// <item><see cref="Site.PayloadDrainWalk"/> - the tree merge drain and the
/// snapshot copy, which read a per-leaf payload and then advanced the bounded
/// leaf walk with its own <c>GetKeyRangeAndNextSiblingAsync</c>: payload +
/// <c>GetKeyRangeAndNextSiblingAsync</c> -> one combined payload call whose page
/// also carries the leaf's key range and next sibling
/// (<c>GetDeltaSinceAndNextSiblingAsync</c> / <c>GetLiveRawEntriesAndNextSiblingAsync</c>),
/// 2 calls per leaf -> 1.</item>
/// <item><see cref="Site.ReverseScanWalk"/> - the shard root's reverse
/// range-bounded page fills, which read a per-leaf payload, then the leaf's key
/// range, then its previous-sibling pointer: payload + <c>GetKeyRangeAsync</c> +
/// <c>GetPrevSiblingAsync</c> -> payload + <c>GetKeyRangeAndPrevSiblingAsync</c>,
/// 3 calls per leaf -> 2.</item>
/// </list>
/// <para>
/// <b>Read the hop model.</b> <see cref="HopModel.Yield"/> prices only the
/// dispatch overhead of each awaited call. <see cref="HopModel.Delay1Ms"/>
/// models a remote round trip with <c>Task.Delay(1)</c>, which the OS timer
/// rounds up (about 15.6 ms on Windows, about 1 ms on Linux), so its absolute
/// times are platform-dependent and its <b>ratio</b> is the evidence: it tracks
/// the serial leaf hops removed from each walk.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=leafchainwalk</c> (or
/// <c>--suite leafchainwalk</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LeafChainWalkElisionBenchmarks
{
    private const int TransactionIds = 4;

    private LeafChain _chain = null!;
    private Guid[] _transactionIds = null!;

    /// <summary>The leaf-chain walk being measured; see the class summary.</summary>
    [Params(Site.LiveRawWalk, Site.PendingWalk, Site.DiscardWalk, Site.KeyRangeWalk, Site.DeltaWalk, Site.ShardScanWalk, Site.PayloadDrainWalk, Site.ReverseScanWalk)]
    public Site Path { get; set; }

    /// <summary>The number of leaves in the walked chain.</summary>
    [Params(4)]
    public int Leaves { get; set; }

    /// <summary>How each simulated leaf call completes; see the class summary.</summary>
    [Params(HopModel.Yield, HopModel.Delay1Ms)]
    public HopModel Hop { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _chain = new LeafChain(Hop, Leaves);
        _transactionIds = new Guid[TransactionIds];
        for (var i = 0; i < _transactionIds.Length; i++)
        {
            _transactionIds[i] = Guid.NewGuid();
        }

        var before = Separate_Payload_And_Sibling_Reads_Async().GetAwaiter().GetResult();
        var after = Combined_Leaf_Read_Async().GetAwaiter().GetResult();
        if (before != after)
        {
            throw new InvalidOperationException($"Leaf chain walk lanes disagree: before={before}, after={after}.");
        }
    }

    [Benchmark(Baseline = true, Description = "payload call + GetNextSibling per leaf")]
    public async Task<long> Separate_Payload_And_Sibling_Reads_Async()
    {
        var total = 0L;
        int? leaf = 0;
        while (leaf is int current)
        {
            switch (Path)
            {
                case Site.LiveRawWalk:
                    total += (await _chain.GetLiveRawEntriesAsync(current)).Count;
                    break;
                case Site.PendingWalk:
                    total += (await _chain.GetPendingMutationsAsync(current)).Count;
                    break;
                case Site.DeltaWalk:
                    total += (await _chain.GetDeltaSinceAsync(current)).Count;
                    break;
                case Site.ShardScanWalk:
                    total += (await _chain.GetLiveRawEntriesAsync(current)).Count;
                    total += await _chain.GetKeyRangeAsync(current);
                    break;
                case Site.ReverseScanWalk:
                    total += (await _chain.GetLiveRawEntriesAsync(current)).Count;
                    total += await _chain.GetKeyRangeAsync(current);
                    leaf = await _chain.GetPrevSiblingAsync(current);
                    continue;
                case Site.PayloadDrainWalk:
                {
                    total += (await _chain.GetDeltaSinceAsync(current)).Count;
                    var (drainRange, drainNext) = await _chain.GetKeyRangeAndNextSiblingAsync(current);
                    total += drainNext is null ? 0 : drainRange;
                    leaf = drainNext;
                    continue;
                }

                case Site.KeyRangeWalk:
                {
                    leaf = await _chain.GetNextSiblingAsync(current);
                    if (leaf is not null)
                    {
                        total += await _chain.GetKeyRangeAsync(current);
                    }

                    continue;
                }

                default:
                    foreach (var id in _transactionIds)
                    {
                        total += await _chain.DiscardAsync(current, id) ? 1 : 0;
                    }

                    break;
            }

            leaf = await _chain.GetNextSiblingAsync(current);
        }

        return total;
    }

    [Benchmark(Description = "one combined payload + next-sibling call per leaf")]
    public async Task<long> Combined_Leaf_Read_Async()
    {
        var total = 0L;
        int? leaf = 0;
        while (leaf is int current)
        {
            switch (Path)
            {
                case Site.LiveRawWalk:
                {
                    var (entries, next) = await _chain.GetLiveRawEntriesAndNextSiblingAsync(current);
                    total += entries.Count;
                    leaf = next;
                    break;
                }

                case Site.PendingWalk:
                {
                    var (mutations, next) = await _chain.GetPendingMutationsAndNextSiblingAsync(current);
                    total += mutations.Count;
                    leaf = next;
                    break;
                }

                case Site.DeltaWalk:
                {
                    var (delta, next) = await _chain.GetDeltaSinceAndNextSiblingAsync(current);
                    total += delta.Count;
                    leaf = next;
                    break;
                }

                case Site.ShardScanWalk:
                {
                    total += (await _chain.GetLiveRawEntriesAsync(current)).Count;
                    var (range, next) = await _chain.GetKeyRangeAndNextSiblingAsync(current);
                    total += range;
                    leaf = next;
                    break;
                }

                case Site.ReverseScanWalk:
                {
                    total += (await _chain.GetLiveRawEntriesAsync(current)).Count;
                    var (range, prev) = await _chain.GetKeyRangeAndPrevSiblingAsync(current);
                    total += range;
                    leaf = prev;
                    break;
                }

                case Site.PayloadDrainWalk:
                {
                    var (delta, range, next) = await _chain.GetDeltaSinceWithLinkAsync(current);
                    total += delta.Count;
                    total += next is null ? 0 : range;
                    leaf = next;
                    break;
                }

                case Site.KeyRangeWalk:
                {
                    var (range, next) = await _chain.GetKeyRangeAndNextSiblingAsync(current);
                    total += next is null ? 0 : range;
                    leaf = next;
                    break;
                }

                default:
                {
                    var (discarded, next) = await _chain.DiscardManyAndGetNextSiblingAsync(current, _transactionIds);
                    total += discarded;
                    leaf = next;
                    break;
                }
            }
        }

        return total;
    }

    /// <summary>The leaf-chain walk being measured.</summary>
    public enum Site
    {
        /// <summary>A live raw-entry walk (local VV seed, bootstrap receiver scans).</summary>
        LiveRawWalk,

        /// <summary>A pending-mutation walk (stale-pending clear, bootstrap pending capture).</summary>
        PendingWalk,

        /// <summary>A multi-transaction pending discard walk (abandoned-copy and poisoned-saga cleanup).</summary>
        DiscardWalk,

        /// <summary>The bounded leaf walk's key-range + next-sibling advance (maintenance drains).</summary>
        KeyRangeWalk,

        /// <summary>A tombstone delta walk (replication snapshot provider).</summary>
        DeltaWalk,

        /// <summary>The shard root's count, probe, range-clock, projection-rebuild and range-bounded page-fill walks.</summary>
        ShardScanWalk,

        /// <summary>A payload drain whose page carries the walk's link (tree merge drain, snapshot copy).</summary>
        PayloadDrainWalk,

        /// <summary>The shard root's reverse range-bounded page fills.</summary>
        ReverseScanWalk,
    }

    /// <summary>How a simulated grain call completes.</summary>
    public enum HopModel
    {
        /// <summary>One yield: prices dispatch overhead only.</summary>
        Yield,

        /// <summary>One <c>Task.Delay(1)</c>: prices a remote round trip at the OS timer floor.</summary>
        Delay1Ms,
    }

    /// <summary>
    /// Stands in for a chain of leaf grains: every call completes asynchronously
    /// per the hop model, and the combined calls return exactly what the separate
    /// calls return.
    /// </summary>
    private sealed class LeafChain(HopModel hop, int leaves)
    {
        private readonly List<int> _payload = [1, 2, 3, 4, 5, 6, 7, 8];

        public async Task<List<int>> GetLiveRawEntriesAsync(int leaf)
        {
            await HopAsync();
            return _payload;
        }

        public async Task<List<int>> GetPendingMutationsAsync(int leaf)
        {
            await HopAsync();
            return _payload;
        }

        public async Task<bool> DiscardAsync(int leaf, Guid transactionId)
        {
            await HopAsync();
            return true;
        }

        public async Task<List<int>> GetDeltaSinceAsync(int leaf)
        {
            await HopAsync();
            return _payload;
        }

        public async Task<int> GetKeyRangeAsync(int leaf)
        {
            await HopAsync();
            return leaf;
        }

        public async Task<(List<int> Delta, int? Next)> GetDeltaSinceAndNextSiblingAsync(int leaf)
        {
            await HopAsync();
            return (_payload, Next(leaf));
        }

        public async Task<(int KeyRange, int? Next)> GetKeyRangeAndNextSiblingAsync(int leaf)
        {
            await HopAsync();
            return (leaf, Next(leaf));
        }

        public async Task<(List<int> Delta, int KeyRange, int? Next)> GetDeltaSinceWithLinkAsync(int leaf)
        {
            await HopAsync();
            return (_payload, leaf, Next(leaf));
        }

        public async Task<(int KeyRange, int? Prev)> GetKeyRangeAndPrevSiblingAsync(int leaf)
        {
            await HopAsync();
            return (leaf, Next(leaf));
        }

        public async Task<int?> GetPrevSiblingAsync(int leaf)
        {
            await HopAsync();
            return Next(leaf);
        }

        public async Task<int?> GetNextSiblingAsync(int leaf)
        {
            await HopAsync();
            return Next(leaf);
        }

        public async Task<(List<int> Entries, int? Next)> GetLiveRawEntriesAndNextSiblingAsync(int leaf)
        {
            await HopAsync();
            return (_payload, Next(leaf));
        }

        public async Task<(List<int> Mutations, int? Next)> GetPendingMutationsAndNextSiblingAsync(int leaf)
        {
            await HopAsync();
            return (_payload, Next(leaf));
        }

        public async Task<(int Discarded, int? Next)> DiscardManyAndGetNextSiblingAsync(int leaf, Guid[] transactionIds)
        {
            await HopAsync();
            return (transactionIds.Length, Next(leaf));
        }

        private int? Next(int leaf) => leaf + 1 < leaves ? leaf + 1 : null;

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
