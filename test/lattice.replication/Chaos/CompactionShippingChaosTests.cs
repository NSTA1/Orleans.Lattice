using System.Collections.Concurrent;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos coverage of the compaction + replication shipping
/// interleaving. Drives sustained write + delete churn against site A
/// (so tombstones accumulate and the per-leaf
/// <c>CompactTombstonesAsync</c> path fires reap envelopes -
/// <see cref="MutationKind.Tombstone"/>, tagged
/// <see cref="MutationCategory.Maintenance"/> via the producer-side
/// <c>LatticeMaintenanceContext</c> scope). Asserts the producer-side
/// <c>ReplicationShipperGrain.ShouldShip</c> filter keeps every
/// maintenance-tagged tombstone-reap envelope off the wire: each peer
/// cluster runs its own compaction independently, so reap envelopes
/// have no defined cross-cluster semantics and must be filtered at
/// the producer boundary.
/// </summary>
/// <remarks>
/// The test reads every entry that crossed the loopback transport
/// (via the <c>LoopbackReplicationTransport.OnBatchObserved</c> hook)
/// and asserts every observed entry has <c>Op</c> != Tombstone.
/// Convergence on the live key set is also pinned so the test fails
/// loudly if a regression of the producer-side filter starts shipping
/// reap envelopes that silently delete keys on the receiver.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CompactionShippingChaosTests
{
    private const string TreeName = "chaos-compaction";

    [Test]
    public async Task Tombstone_reap_envelopes_are_filtered_at_producer_under_concurrent_compaction_and_shipping()
    {
        await using var fixture = new ProductionShipperFixture(TreeName, siteCount: 2);
        await fixture.InitializeAsync();

        var siteBId = fixture.ClusterIds[1];
        var aLattice = fixture.ClientOf(0).GetGrain<ILattice>(TreeName);
        var bLattice = fixture.ClientOf(1).GetGrain<ILattice>(TreeName);

        var observed = new ConcurrentBag<WalRecord>();
        fixture.TransportOf(0).OnBatchObserved = batch =>
        {
            foreach (var entry in batch)
            {
                observed.Add(entry);
            }
        };

        // ---- Phase 1: write + delete churn. Author 40 keys; delete
        // the odd-indexed half. The even-indexed remain live.
        var liveKeys = new List<string>();
        for (var i = 0; i < 40; i++)
        {
            var key = $"k-{i:D3}";
            await aLattice.SetAsync(key, Encoding.UTF8.GetBytes($"v-{i}"));
            if (i % 2 == 0) liveKeys.Add(key);
        }
        for (var i = 1; i < 40; i += 2)
        {
            await aLattice.DeleteAsync($"k-{i:D3}");
        }

        // ---- Phase 2: force compaction. The per-leaf path reaps
        // tombstones whose wall-clock age exceeds the configured grace.
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        var compaction = fixture.ClientOf(0).GetGrain<ITombstoneCompactionGrain>(TreeName);
        await compaction.RunCompactionPassAsync();

        // ---- Phase 3: more writes / deletes / another compaction pass
        // so the chaos shape alternates rather than being a single
        // quiescent compaction window.
        for (var i = 40; i < 70; i++)
        {
            var key = $"k-{i:D3}";
            await aLattice.SetAsync(key, Encoding.UTF8.GetBytes($"v-{i}"));
            if (i % 3 == 0) liveKeys.Add(key);
        }
        for (var i = 41; i < 70; i++)
        {
            if (i % 3 != 0) await aLattice.DeleteAsync($"k-{i:D3}");
        }
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        await compaction.RunCompactionPassAsync();

        // ---- Phase 4: drain. Site B must converge on the live key set.
        await WaitForConvergenceAsync(bLattice, liveKeys, TimeSpan.FromSeconds(30));

        // ---- Invariant: no Tombstone-op entry must have crossed the wire.
        var tombstoneReapEntries = observed
            .Where(e => e.Op == MutationKind.Tombstone)
            .ToArray();

        Assert.That(tombstoneReapEntries, Is.Empty,
            $"Producer-side ShouldShip filter leaked {tombstoneReapEntries.Length} tombstone-reap envelope(s) onto the wire. " +
            $"Reap envelopes are local structural cleanup records and must never cross the producer boundary. " +
            $"First leaked keys: {string.Join(",", tombstoneReapEntries.Take(5).Select(e => e.Key))}.");

        Assert.That(observed.Count, Is.GreaterThan(0),
            "Loopback transport observed zero entries - workload didn't actually ship anything (test is vacuous).");

        TestContext.Out.WriteLine(
            $"Compaction+ship chaos: entries observed on wire = {observed.Count}, " +
            $"Set = {observed.Count(e => e.Op == MutationKind.Set)}, " +
            $"Delete = {observed.Count(e => e.Op == MutationKind.Delete)}, " +
            $"DeleteRange = {observed.Count(e => e.Op == MutationKind.DeleteRange)}, " +
            $"Tombstone-reap = {tombstoneReapEntries.Length} (must be 0), " +
            $"final live keys = {liveKeys.Count}.");
    }

    private static async Task WaitForConvergenceAsync(ILattice peer, IEnumerable<string> keys, TimeSpan timeout)
    {
        var keysArr = keys.ToArray();
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var allPresent = true;
            foreach (var k in keysArr)
            {
                var v = await peer.GetAsync(k);
                if (v is null) { allPresent = false; break; }
            }
            if (allPresent) return;
            await Task.Delay(100);
        }
        Assert.Fail($"Peer did not converge on {keysArr.Length} live keys within {timeout.TotalSeconds}s.");
    }
}
