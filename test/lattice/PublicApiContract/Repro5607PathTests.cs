using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

// THROWAWAY targeted reproduction for issue #5607. Never merged. Forces the
// scan paths a slow (18 s) cold run takes and a 1.3 s run never does: the
// cooperative time-budget resume, the stall-ceiling bank with abandoned walks
// still running, and a split interrupted by the restart.
[TestFixture]
[Category("Integration")]
[Category("API")]
[NonParallelizable]
public sealed class Repro5607PathTests
{
    private static readonly InMemoryWalStorageProvider WalProvider = new();
    private static volatile int s_leafReadDelayMaxMs;
    private static string s_variant = "";
    private TestCluster _cluster = null!;

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

    private static KeyValuePair<string, byte[]> Kvp(string key) => new(key, Encoding.UTF8.GetBytes(key));

    private static async Task BuildAsync(ILattice tree)
    {
        for (var start = 0; start < 256; start += 32)
        {
            await tree.SetManyAsync(Enumerable.Range(start, 32).Select(i => Kvp(Key(i))).ToList());
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

    private async Task<string> DescribeAsync(ILattice tree, IReadOnlyCollection<string> missing)
    {
        var report = new StringBuilder();
        report.AppendLine($"missing: {string.Join(",", missing)}");
        try
        {
            s_leafReadDelayMaxMs = 0;
            foreach (var k in missing)
            {
                report.AppendLine($"point read {k}: {(await tree.GetAsync(k) is null ? "MISS" : "HIT")}");
            }

            var routing = await tree.GetRoutingAsync();
            var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/0");
            foreach (var k in missing.Take(8))
            {
                if (await shard.GetLeafIdForKeyAsync(k) is { } owner)
                {
                    var (ownerKeys, ownerLink) = await _cluster.Client.GetGrain<IBPlusLeafGrain>(owner)
                        .GetKeysAndNextSiblingAsync(null, null);
                    report.AppendLine($"route {k}: {owner} {ownerLink.KeyRange} keys [{string.Join(",", ownerKeys)}]");
                }
            }

            var leafId = await shard.GetLeftmostLeafIdAsync();
            for (var hop = 0; leafId is { } id && hop < 300; hop++)
            {
                var (leafKeys, link) = await _cluster.Client.GetGrain<IBPlusLeafGrain>(id)
                    .GetKeysAndNextSiblingAsync(null, null);
                report.AppendLine($"chain {hop}: {id} {link.KeyRange} keys [{string.Join(",", leafKeys)}]");
                leafId = link.NextSibling;
            }

            var rescan = await ScanAsync(tree);
            report.AppendLine($"rescan (no delay): {rescan.Count}");
        }
        catch (Exception ex)
        {
            report.AppendLine($"describe failed: {ex.GetType().Name}: {ex.Message}");
        }

        return report.ToString();
    }

    private static ConcurrentDictionary<string, long> ListenScanSignals(out MeterListener listener)
    {
        var counts = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
        listener = MeterListening.StartForMeter(LatticeMetrics.Meter, l =>
        {
            l.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                if (value == 0 || !instrument.Name.Contains(".scan", StringComparison.Ordinal))
                {
                    return;
                }

                var label = new StringBuilder(instrument.Name);
                foreach (var tag in tags)
                {
                    if (tag.Key is LatticeMetrics.TagOutcome or "phase" or "reason")
                    {
                        label.Append(' ').Append(tag.Key).Append('=').Append(tag.Value);
                    }
                }

                counts.AddOrUpdate(label.ToString(), value, (_, c) => c + value);
            });
        });
        return counts;
    }

    // variant: budget | ceiling | split
    [TestCase("budget")]
    [TestCase("ceiling")]
    [TestCase("split")]
    public async Task Repro_5607_forced_scan_paths(string variant)
    {
        s_variant = variant;
        s_leafReadDelayMaxMs = 0;
        _cluster = await DeployAsync();
        var signals = ListenScanSignals(out var listener);
        var failures = new StringBuilder();
        var iterations = variant == "split" ? 25 : 12;
        var started = DateTime.UtcNow;
        try
        {
            for (var iteration = 0; iteration < iterations && failures.Length == 0; iteration++)
            {
                s_leafReadDelayMaxMs = 0;
                var treeId = $"repro5607-{variant}-" + Guid.NewGuid().ToString("N")[..8];
                var tree = await RegisterAsync(treeId);
                await BuildAsync(tree);
                var expected = Enumerable.Range(0, 256).Select(Key).ToList();

                if (variant == "split")
                {
                    // Insert 32 keys between k00016 and k00024 and restart while
                    // that batch is in flight, so the leaves holding the
                    // acknowledged k00016..k00023 can be mid-division when the
                    // silo stops.
                    var extra = Enumerable.Range(0, 32).Select(i => Kvp($"k000{16 + (i / 4):D2}x{i:D2}")).ToList();
                    var inFlight = tree.SetManyAsync(extra);
                    await Task.Delay(Random.Shared.Next(0, 40));
                    await RestartAsync();
                    try
                    {
                        await inFlight.WaitAsync(TimeSpan.FromSeconds(30));
                        expected.AddRange(extra.Select(e => e.Key));
                    }
                    catch
                    {
                        // Unacknowledged: its keys may or may not be present.
                    }
                }
                else
                {
                    await RestartAsync();
                }

                tree = await RegisterAsync(treeId);
                s_leafReadDelayMaxMs = variant == "ceiling" ? 60 : 8;
                var scanned = await ScanAsync(tree);
                s_leafReadDelayMaxMs = 0;
                var missing = expected.Except(scanned, StringComparer.Ordinal).ToList();
                if (missing.Count > 0)
                {
                    failures.AppendLine($"{variant} iteration {iteration}: scanned {scanned.Count}, expected >= {expected.Count}");
                    failures.AppendLine(await DescribeAsync(tree, missing));
                }
            }
        }
        finally
        {
            s_leafReadDelayMaxMs = 0;
            listener.Dispose();
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        var signalText = string.Join("; ", signals.OrderBy(s => s.Key).Select(s => $"{s.Key} x{s.Value}"));
        Assert.Fail(failures.Length == 0
            ? $"REPRO5607-PATH {variant}: no miss in {iterations} iterations ({(DateTime.UtcNow - started).TotalSeconds:F0}s). signals: {signalText}"
            : $"REPRO5607-PATH {variant} MISS. signals: {signalText}\n{failures}");
    }

    private sealed class LeafReadDelayFilter : IIncomingGrainCallFilter
    {
        public async Task Invoke(IIncomingGrainCallContext context)
        {
            var max = s_leafReadDelayMaxMs;
            if (max > 0
                && context.InterfaceMethod?.DeclaringType == typeof(IBPlusLeafGrain)
                && context.InterfaceMethod.Name.StartsWith("GetKeys", StringComparison.Ordinal))
            {
                await Task.Delay(Random.Shared.Next(0, max + 1));
            }

            await context.Invoke();
        }
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
            siloBuilder.AddIncomingGrainCallFilter<LeafReadDelayFilter>();
            var variant = s_variant;
            siloBuilder.ConfigureLattice(opts =>
            {
                opts.DigestCoalescingWindowMs = 0;
                opts.StorageUsagePollInterval = TimeSpan.Zero;
                switch (variant)
                {
                    case "budget":
                        opts.MaxScanPageDuration = TimeSpan.FromMilliseconds(3);
                        break;
                    case "ceiling":
                        opts.MaxScanPageDuration = TimeSpan.Zero;
                        opts.MaxScanPageStallDuration = TimeSpan.FromMilliseconds(40);
                        break;
                    case "split":
                        opts.MaxScanPageDuration = TimeSpan.FromMilliseconds(3);
                        break;
                }
            });
        }
    }
}
