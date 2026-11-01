using System.Linq;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

// THROWAWAY reproduction harness for issue #5607. Never merged.
// Every scan page yields after one leaf, so every leaf boundary takes the
// resume-by-bounds path a slow cold restart reaches only when the page's
// time budget runs out.
[TestFixture]
[Category("Integration")]
[Category("API")]
[NonParallelizable]
public sealed class Repro5607BudgetTests
{
    private static readonly InMemoryWalStorageProvider WalProvider = new();
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUpAsync() => _cluster = await DeployAsync();

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static async Task<TestCluster> DeployAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private async Task RestartAsync()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
        _cluster = await DeployAsync();
    }

    private async Task<ILattice> RegisterAsync(string treeId)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            MaxInternalChildren = 4,
            ShardCount = 1,
        });
        return _cluster.Client.GetGrain<ILattice>(treeId);
    }

    private static string Key(int i) => $"k{i:D5}";

    private async Task BuildAsync(ILattice tree)
    {
        for (var start = 0; start < 256; start += 32)
        {
            var batch = Enumerable.Range(start, 32)
                .Select(i => new KeyValuePair<string, byte[]>(Key(i), Encoding.UTF8.GetBytes(i.ToString())))
                .ToList();
            await tree.SetManyAsync(batch);
        }
    }

    private static async Task<List<string>> ScanAsync(ILattice tree)
    {
        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync())
        {
            keys.Add(k);
        }

        return keys;
    }

    private async Task<string> DescribeAsync(ILattice tree, string treeId, IReadOnlyCollection<string> missing)
    {
        var report = new StringBuilder();
        report.AppendLine($"missing: {string.Join(",", missing)}");
        foreach (var k in missing)
        {
            report.AppendLine($"point read {k}: {(await tree.GetAsync(k) is null ? "MISS" : "HIT")}");
        }

        var routing = await tree.GetRoutingAsync();
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        var hops = 0;
        while (leafId is { } id && hops++ < 200)
        {
            var (leafKeys, link) = await _cluster.Client.GetGrain<IBPlusLeafGrain>(id)
                .GetKeysAndNextSiblingAsync(null, null);
            report.AppendLine($"chain {hops}: [{link.KeyRange}] keys [{string.Join(",", leafKeys)}]");
            leafId = link.NextSibling;
        }

        foreach (var k in missing)
        {
            if (await shard.GetLeafIdForKeyAsync(k) is { } owner)
            {
                var (ownerKeys, ownerLink) = await _cluster.Client.GetGrain<IBPlusLeafGrain>(owner)
                    .GetKeysAndNextSiblingAsync(null, null);
                report.AppendLine($"route {k}: [{ownerLink.KeyRange}] keys [{string.Join(",", ownerKeys)}]");
            }
        }

        return report.ToString();
    }

    [Test]
    public async Task Repro_5607_one_leaf_pages_after_a_cold_restart()
    {
        var expected = Enumerable.Range(0, 256).Select(Key).ToArray();
        var failures = new StringBuilder();
        for (var iteration = 0; iteration < 20; iteration++)
        {
            var treeId = "repro5607-budget-" + Guid.NewGuid().ToString("N")[..8];
            var tree = await RegisterAsync(treeId);
            await BuildAsync(tree);

            var warm = await ScanAsync(tree);
            if (!warm.SequenceEqual(expected))
            {
                var missing = expected.Except(warm).ToList();
                failures.AppendLine($"WARM iteration {iteration}: {warm.Count} of 256");
                failures.AppendLine(await DescribeAsync(tree, treeId, missing));
                break;
            }

            await RestartAsync();
            tree = await RegisterAsync(treeId);
            var cold = await ScanAsync(tree);
            if (!cold.SequenceEqual(expected))
            {
                var missing = expected.Except(cold).ToList();
                failures.AppendLine($"COLD iteration {iteration}: {cold.Count} of 256");
                failures.AppendLine(await DescribeAsync(tree, treeId, missing));
                var rescan = await ScanAsync(tree);
                failures.AppendLine($"rescan: {rescan.Count} of 256");
                break;
            }
        }

        Assert.Fail(failures.Length == 0
            ? "REPRO5607-BUDGET no miss in 20 iterations with one-leaf pages."
            : "REPRO5607-BUDGET " + failures);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddWalStorage(_ => WalProvider);
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<Orleans.Storage.IGrainStorage>(
                    name,
                    (_, _) => new ProcessScopeMemoryGrainStorage()));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(opts =>
            {
                opts.DigestCoalescingWindowMs = 0;
                opts.StorageUsagePollInterval = TimeSpan.Zero;
                opts.MaxLeavesPerScanPage = 1;
            });
        }
    }
}
