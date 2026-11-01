namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

// THROWAWAY probe for issue #5607. Never merged. Drives the miss report
// against a healthy tree with k00016..k00023 removed from the scanned list,
// so CI shows that every section of the report collects.
public partial class PublicApiContractTests
{
    [Test]
    public async Task Repro_5607_diagnostic_probe()
    {
        var treeId = "pac-probe5607-" + Guid.NewGuid().ToString("N")[..8];
        await CreateDepthTwoTreeAsync(treeId);
        await _fixture.RestartClusterAsync();
        var tree = await _fixture.CreateSmallTreeAsync(
            treeId,
            shardCount: 1,
            maxLeafKeys: DeepScanLeafKeys,
            maxInternalChildren: DeepScanInternalChildren);

        using var signals = new DeepScanSignals(treeId);
        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync())
        {
            keys.Add(k);
        }

        var expected = Enumerable.Range(0, DeepScanKeyCount).Select(DeepScanKey).ToArray();
        var faked = keys
            .Where(k => string.CompareOrdinal(k, "k00016") < 0 || string.CompareOrdinal(k, "k00024") >= 0)
            .ToList();
        Assert.Fail("REPRO5607-PROBE " + await DescribeDeepScanMissAsync(tree, faked, expected, "probe", signals));
    }
}
