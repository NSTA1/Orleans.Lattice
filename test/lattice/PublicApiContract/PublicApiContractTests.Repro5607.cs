using System.Linq;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

// THROWAWAY reproduction harness for issue #5607. Never merged.
public partial class PublicApiContractTests
{
    private const int Repro5607Iterations = 150;

    [Test]
    public async Task Repro_5607_cold_restart_scan_loses_no_key()
    {
        var expected = Enumerable.Range(0, DeepScanKeyCount).Select(DeepScanKey).ToArray();
        for (var iteration = 0; iteration < Repro5607Iterations; iteration++)
        {
            var treeId = "pac-repro5607-" + Guid.NewGuid().ToString("N")[..8];
            await CreateDepthTwoTreeAsync(treeId);
            await _fixture.RestartClusterAsync();
            var tree = await _fixture.CreateSmallTreeAsync(
                treeId,
                shardCount: 1,
                maxLeafKeys: DeepScanLeafKeys,
                maxInternalChildren: DeepScanInternalChildren);

            var keys = new List<string>();
            await foreach (var k in tree.ScanKeysAsync())
            {
                keys.Add(k);
            }

            if (keys.SequenceEqual(expected))
            {
                continue;
            }

            var report = new StringBuilder();
            report.AppendLine($"REPRO5607 iteration {iteration} tree {treeId}: scan returned {keys.Count} of {expected.Length}.");
            var missing = expected.Except(keys).ToList();
            report.AppendLine($"missing: {string.Join(",", missing)}");
            var extra = keys.Except(expected).ToList();
            report.AppendLine($"extra: {string.Join(",", extra)}");

            foreach (var k in missing)
            {
                var value = await tree.GetAsync(k);
                report.AppendLine($"point read {k}: {(value is null ? "MISS" : "HIT")}");
            }

            var rescan = new List<string>();
            await foreach (var k in tree.ScanKeysAsync())
            {
                rescan.Add(k);
            }

            report.AppendLine($"rescan returned {rescan.Count}; still missing: {string.Join(",", expected.Except(rescan))}");

            var routing = await tree.GetRoutingAsync();
            var shard = Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");
            foreach (var k in missing)
            {
                var owner = await shard.GetLeafIdForKeyAsync(k);
                if (owner is { } ownerId)
                {
                    var ownerLeaf = Client.GetGrain<IBPlusLeafGrain>(ownerId);
                    var (ownerKeys, ownerLink) = await ownerLeaf.GetKeysAndNextSiblingAsync(null, null);
                    var ckpt = await ownerLeaf.GetProjectionCheckpointAndNextSiblingAsync();
                    report.AppendLine(
                        $"route {k} -> leaf {ownerId}: keys [{string.Join(",", ownerKeys)}] range "
                        + $"[{ownerLink.KeyRange}] next {ownerLink.NextSibling} checkpoint {ckpt.CheckpointOffset}");
                }
                else
                {
                    report.AppendLine($"route {k} -> no leaf");
                }
            }

            var leafId = await shard.GetLeftmostLeafIdAsync();
            var hops = 0;
            while (leafId is { } id && hops++ < 200)
            {
                var leaf = Client.GetGrain<IBPlusLeafGrain>(id);
                var (leafKeys, link) = await leaf.GetKeysAndNextSiblingAsync(null, null);
                report.AppendLine(
                    $"chain {hops}: leaf {id} keys [{string.Join(",", leafKeys)}] range [{link.KeyRange}] next {link.NextSibling}");
                leafId = link.NextSibling;
            }

            for (var p = 0; p < 8; p++)
            {
                try
                {
                    var wal = Client.GetGrain<IWalShardGrain>($"{routing.PhysicalTreeId}/{p}");
                    var trim = await wal.GetTrimWatermarkAsync(CancellationToken.None);
                    var head = await wal.GetReadableHeadAsync(CancellationToken.None);
                    report.AppendLine($"wal partition {p}: trim {trim?.ToString() ?? "none"} head {head}");
                }
                catch (Exception ex)
                {
                    report.AppendLine($"wal partition {p}: {ex.GetType().Name}");
                }
            }

            TestContext.Progress.WriteLine(report.ToString());
            Assert.Fail(report.ToString());
        }
    }
}
