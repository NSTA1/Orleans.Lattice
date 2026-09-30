using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The shrink half of the reshard chaos fixture: the same dense workload of
/// point reads, point writes, scans and counts runs while
/// <see cref="ILattice.ReshardAsync(int, CancellationToken)"/> folds the tree
/// from 4 physical shards down to 2.
/// <para>
/// Exercises what the grow run cannot: the coordinator's fold planning and
/// concurrency under sustained traffic, every fold's drain, freeze, swap and
/// finalise racing live writes, the release of each retired donor's storage
/// while scans and counts may still hold a pre-fold shard map, and the
/// retired shard's stale-routing and empty-page answers to those callers.
/// </para>
/// </summary>
public partial class ChaosReshardIntegrationTests
{
    private const int ShrinkTarget = 2;

    [Test]
    public async Task Chaos_shrinking_reshard_under_concurrent_load_preserves_all_data_and_retires_the_donors()
    {
        var treeId = $"reshard-shrink-chaos-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);

        await SeedAsync(tree);

        var failures = new ConcurrentBag<string>();
        var stats = new ConcurrentDictionary<string, int>();
        static int Bump(ConcurrentDictionary<string, int> s, string k)
            => s.AddOrUpdate(k, 1, (_, v) => v + 1);

        // Warm cold activations and start the shrink outside the timed window,
        // for the same reason the grow run does.
        _ = await tree.GetRoutingAsync();
        _ = await tree.CountAsync();
        _ = await reshard.IsIdleAsync();

        await tree.ReshardAsync(ShrinkTarget);
        Bump(stats, "reshard-kicked");

        using var cts = new CancellationTokenSource(ChaosDuration);
        var ct = cts.Token;

        var workers = StartWorkload(tree, ct, failures, stats);

        // ---- Shrink driver: pump the coordinator and every fold it starts.
        workers.Add(Task.Run(async () =>
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    if (await reshard.IsIdleAsync()) { Bump(stats, "reshard-complete"); break; }

                    await reshard.RunReshardPassAsync();
                    Bump(stats, "reshard-passes");
                    if (await DriveFoldsAsync(treeId, ct) > 0) Bump(stats, "fold-passes");

                    await Task.Delay(100, ct);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) when (IsTransient(ex)) { Bump(stats, "transient-reshard"); }
            catch (Exception ex)
            {
                failures.Add($"shrink-driver threw: {ex.GetType().Name}: {ex.Message}");
            }
        }, ct));

        await Task.WhenAll(workers);

        // Post-chaos drain against a quiescent system, so the final invariants
        // are evaluated cleanly.
        using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!drainCts.IsCancellationRequested && !await reshard.IsIdleAsync())
        {
            await reshard.RunReshardPassAsync();
            await DriveFoldsAsync(treeId, CancellationToken.None);
            await Task.Delay(100);
        }

        var finalCount = await tree.CountAsync();
        var finalKeys = await DrainKeysWithRetryAsync(tree, maxAttempts: 5);
        var live = (await registry.GetShardMapAsync(treeId))?.GetPhysicalShardIndices() ?? [];
        var reshardDone = await reshard.IsIdleAsync();

        var retiredStates = new List<(int Index, bool Retired, bool HasLeaves)>();
        foreach (var idx in Enumerable.Range(0, FourShardClusterFixture.TestShardCount).Except(live))
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{idx}");
            retiredStates.Add((idx, await shard.IsRetiredAsync(), await shard.GetLeftmostLeafIdAsync() is not null));
        }

        Assert.Multiple(() =>
        {
            Assert.That(failures, Is.Empty,
                $"Chaos observed {failures.Count} invariant violations (first 20):\n " +
                string.Join("\n ", failures.Take(20)));

            Assert.That(finalCount, Is.EqualTo(UniverseSize),
                "Post-chaos CountAsync must match the pinned universe size.");
            Assert.That(finalKeys.Count, Is.EqualTo(UniverseSize),
                "Post-chaos KeysAsync must yield exactly the pinned universe.");
            Assert.That(reshardDone, Is.True,
                "The shrink must complete within the post-chaos drain window.");
            Assert.That(live, Has.Count.EqualTo(ShrinkTarget),
                $"The final ShardMap must hold exactly {ShrinkTarget} distinct physical shards.");

            Assert.That(retiredStates, Has.Count.EqualTo(FourShardClusterFixture.TestShardCount - ShrinkTarget));
            foreach (var (index, retired, hasLeaves) in retiredStates)
            {
                Assert.That(retired, Is.True, $"shard {index} left the map and must be retired");
                Assert.That(hasLeaves, Is.False, $"retired shard {index} must hold no leaves");
            }

            foreach (var op in new[] { "point-writes", "point-reads", "keys-scans", "counts" })
            {
                Assert.That(stats.GetValueOrDefault(op, 0), Is.GreaterThan(0),
                    $"Workload category '{op}' must have performed at least one operation.");
            }

            Assert.That(stats.GetValueOrDefault("reshard-passes", 0), Is.GreaterThan(0),
                "The shrink coordinator must have run at least one pass under load.");
        });
    }

    /// <summary>
    /// Runs one pass of every in-flight fold on the tree's original shard
    /// indices - a shrink only ever retires indices it started with - and
    /// returns how many folds it drove.
    /// </summary>
    private async Task<int> DriveFoldsAsync(string treeId, CancellationToken ct)
    {
        var driven = 0;
        for (var idx = 0; idx < FourShardClusterFixture.TestShardCount; idx++)
        {
            if (ct.IsCancellationRequested) break;
            var fold = _cluster.GrainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{idx}");
            if (await fold.IsIdleAsync()) continue;
            await fold.RunConsolidationPassAsync();
            driven++;
        }

        return driven;
    }
}
