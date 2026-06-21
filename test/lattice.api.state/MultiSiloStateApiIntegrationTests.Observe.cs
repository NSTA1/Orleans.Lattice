using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Cross-silo coverage for the change-observation surface. A subscription is
/// served by a silo that did not originate the writes, and the tree's WAL
/// partitions are spread across the cluster, so delivery proves the observer
/// tails every partition through real cross-silo grain fan-out rather than only
/// the partitions local to the serving silo.
/// </summary>
public sealed partial class MultiSiloStateApiIntegrationTests
{
    [Test]
    public async Task ObserveChanges_served_by_non_originating_silo_tails_writes_across_silos()
    {
        const string treeId = "multisilo-observe";
        var registry = _fixture.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new Orleans.Lattice.BPlusTree.State.TreeRegistryEntry
        {
            MaxLeafKeys = MultiSiloStateApiClusterFixture.MaxLeafKeys,
            ShardCount = MultiSiloStateApiClusterFixture.ShardCount,
            WalPartitions = MultiSiloStateApiClusterFixture.WalPartitions,
        });
        var tree = _fixture.Client.GetGrain<ILattice>(treeId);

        var expectedKeys = Enumerable.Range(0, 12).Select(i => $"obs-key-{i:D5}").ToArray();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var collected = new HashSet<string>();

        // Subscribe from a non-originating silo, then drive the writes through the
        // cluster client once the subscription has seeded its tail cursor. WAL
        // delivery is at-least-once, so dedupe before counting.
        var collectTask = Task.Run(async () =>
        {
            await foreach (var change in _fixture.ObserverFromOtherSilo().ObserveAsync(
                new StateObserveRequest { TreeId = treeId }, cts.Token))
            {
                collected.Add(change.Key);
                if (collected.Count >= expectedKeys.Length)
                {
                    break;
                }
            }
        }, cts.Token);

        await Task.Delay(500, cts.Token);
        foreach (var key in expectedKeys)
        {
            await tree.SetAsync(key, Encoding.UTF8.GetBytes("v"));
        }

        try
        {
            await collectTask.WaitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Fall through to the assertion, which reports what was actually seen.
        }

        Assert.That(collected, Is.EquivalentTo(expectedKeys),
            "a subscription on a non-originating silo must observe writes from every WAL partition across the cluster");
    }
}
