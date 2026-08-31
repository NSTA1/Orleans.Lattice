using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Collections.Concurrent;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end coverage for online shard consolidation - folding one physical
/// donor shard back onto an adjacent survivor and retiring it from the routing
/// map, without taking any shard offline or quiescing the tree.
/// <para>
/// Adaptive split is otherwise a one-way door: a tree the splitter shattered
/// has no way back. These tests exercise the inverse against a real cluster,
/// with the emphasis on the two properties that make a fold safe to run on a
/// busy, already-damaged deployment - no key is ever unreachable at any
/// instant, and no write is lost - plus the durability-coherence claim that a
/// fold can never license a WAL trim over data the survivor has not absorbed.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardConsolidationIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ITreeShardConsolidationGrain Consolidator(string treeId, int donorShardIndex)
        => _cluster.GrainFactory.GetGrain<ITreeShardConsolidationGrain>($"{treeId}/{donorShardIndex}");

    private async Task<ShardMap> GetMapAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var map = await registry.GetShardMapAsync(treeId);
        return map ?? ShardMap.CreateDefault(
            LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);
    }

    private async Task<int> PhysicalShardCountAsync(string treeId)
        => (await GetMapAsync(treeId)).GetPhysicalShardIndices().Count;

    private async Task RunFoldAsync(string treeId, int donor, int survivor)
    {
        var coordinator = Consolidator(treeId, donor);
        await coordinator.StartAsync(survivor);
        for (var i = 0; i < 64 && !await coordinator.IsIdleAsync(); i++)
            await coordinator.RunConsolidationPassAsync();
        Assert.That(await coordinator.IsIdleAsync(), Is.True, "The fold must reach a terminal state.");
    }

    private static async Task<Dictionary<string, byte[]>> PopulateAsync(ILattice tree, string prefix, int count)
    {
        var expected = new Dictionary<string, byte[]>(count);
        for (var i = 0; i < count; i++)
        {
            var key = $"{prefix}-{i:D5}";
            var value = Encoding.UTF8.GetBytes($"value-{i}");
            await tree.SetAsync(key, value);
            expected[key] = value;
        }
        return expected;
    }

    private static async Task AssertAllReadableAsync(ILattice tree, Dictionary<string, byte[]> expected, string when)
    {
        foreach (var (key, value) in expected)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Key '{key}' unreachable {when}.");
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Key '{key}' has the wrong value {when}.");
        }
    }

    // --- The core promise ---

    [Test]
    public async Task Consolidation_reduces_the_physical_shard_count_with_no_data_loss()
    {
        var treeId = $"cons-basic-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "k", 300);

        var before = await PhysicalShardCountAsync(treeId);
        Assert.That(before, Is.EqualTo(FourShardClusterFixture.TestShardCount));

        await RunFoldAsync(treeId, donor: 3, survivor: 2);

        var after = await PhysicalShardCountAsync(treeId);
        Assert.That(after, Is.EqualTo(before - 1),
            "A fold must retire exactly one physical shard from the routing map.");

        var map = await GetMapAsync(treeId);
        Assert.That(map.GetPhysicalShardIndices(), Does.Not.Contain(3),
            "The retired donor must no longer appear in the routing map.");

        await AssertAllReadableAsync(tree, expected, "after the fold");
        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count),
            "A fold must neither lose nor duplicate a key in the tree's count.");
    }

    [Test]
    public async Task Consolidating_an_already_consolidated_pair_is_a_no_op()
    {
        var treeId = $"cons-idem-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await PopulateAsync(tree, "k", 40);

        await RunFoldAsync(treeId, donor: 3, survivor: 2);
        var mapAfterFirst = (await GetMapAsync(treeId)).Slots;

        var coordinator = Consolidator(treeId, 3);
        await coordinator.StartAsync(2);

        Assert.That(await coordinator.IsIdleAsync(), Is.True);
        Assert.That((await GetMapAsync(treeId)).Slots, Is.EqualTo(mapAfterFirst).AsCollection,
            "Re-folding a retired donor must not change routing at all.");
    }

    [Test]
    public async Task Repeated_consolidation_folds_an_over_split_tree_back_down()
    {
        // The healing shape a driver runs: keep folding the cheapest adjacent
        // pair until the shard count is back where it belongs.
        var treeId = $"cons-heal-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "k", 200);

        await RunFoldAsync(treeId, donor: 3, survivor: 2);
        await RunFoldAsync(treeId, donor: 2, survivor: 1);
        await RunFoldAsync(treeId, donor: 1, survivor: 0);

        Assert.That(await PhysicalShardCountAsync(treeId), Is.EqualTo(1));
        await AssertAllReadableAsync(tree, expected, "after folding the tree down to one shard");
        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count));
    }

    // --- Fully online: concurrent readers and writers ---

    [Test]
    public async Task A_concurrent_reader_observes_no_missing_key_at_any_instant_of_a_fold()
    {
        var treeId = $"cons-read-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "rk", 400);

        var failures = new ConcurrentBag<string>();
        var reads = 0;
        using var cts = new CancellationTokenSource();

        var readers = Enumerable.Range(0, 4).Select(worker => Task.Run(async () =>
        {
            var rng = new Random(unchecked(worker * 7919 + 13));
            var keys = expected.Keys.ToArray();
            while (!cts.IsCancellationRequested)
            {
                var key = keys[rng.Next(keys.Length)];
                try
                {
                    var actual = await tree.GetAsync(key);
                    if (actual is null)
                        failures.Add($"Key '{key}' was unreachable mid-fold.");
                    else if (!actual.AsSpan().SequenceEqual(expected[key]))
                        failures.Add($"Key '{key}' returned a stale or wrong value mid-fold.");
                    Interlocked.Increment(ref reads);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    failures.Add($"Reader faulted on '{key}': {ex.GetType().Name}: {ex.Message}");
                }
            }
        })).ToArray();

        await RunFoldAsync(treeId, donor: 3, survivor: 2);

        await cts.CancelAsync();
        await Task.WhenAll(readers);

        Assert.That(reads, Is.GreaterThan(0), "The reader workers must actually have run during the fold.");
        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures.Take(10)));
        await AssertAllReadableAsync(tree, expected, "after the fold");
    }

    [Test]
    public async Task A_concurrent_writer_loses_no_write_across_a_fold()
    {
        var treeId = $"cons-write-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await PopulateAsync(tree, "seed", 100);

        var written = new ConcurrentDictionary<string, byte[]>();
        var failures = new ConcurrentBag<string>();
        using var cts = new CancellationTokenSource();

        var writers = Enumerable.Range(0, 3).Select(worker => Task.Run(async () =>
        {
            var i = 0;
            while (!cts.IsCancellationRequested)
            {
                var key = $"w{worker}-{i:D6}";
                var value = Encoding.UTF8.GetBytes($"w{worker}-v{i}");
                try
                {
                    await tree.SetAsync(key, value);
                    written[key] = value;
                    i++;
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    failures.Add($"Writer faulted on '{key}': {ex.GetType().Name}: {ex.Message}");
                }
            }
        })).ToArray();

        await RunFoldAsync(treeId, donor: 3, survivor: 2);

        await cts.CancelAsync();
        await Task.WhenAll(writers);

        Assert.That(failures, Is.Empty, string.Join(Environment.NewLine, failures.Take(10)));
        Assert.That(written, Is.Not.Empty, "The writer workers must actually have run during the fold.");

        // Every write acknowledged during the fold - including any that raced
        // the slot re-point - must still be readable afterwards, exactly once.
        foreach (var (key, value) in written)
        {
            var actual = await tree.GetAsync(key);
            Assert.That(actual, Is.Not.Null, $"Write '{key}' acknowledged during the fold was lost.");
            Assert.That(actual, Is.EqualTo(value).AsCollection, $"Write '{key}' came back with the wrong value.");
        }
    }

    // --- Value shapes: tombstones, expiries, CRDTs ---

    [Test]
    public async Task Tombstones_and_expiring_entries_survive_a_fold()
    {
        var treeId = $"cons-shapes-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        var live = await PopulateAsync(tree, "live", 60);
        var deleted = await PopulateAsync(tree, "gone", 60);
        foreach (var key in deleted.Keys)
            Assert.That(await tree.DeleteAsync(key), Is.True);

        var ttlValue = Encoding.UTF8.GetBytes("expires-later");
        await tree.SetAsync("ttl-key", ttlValue, TimeSpan.FromHours(12));

        await RunFoldAsync(treeId, donor: 3, survivor: 2);

        await AssertAllReadableAsync(tree, live, "after the fold");
        foreach (var key in deleted.Keys)
        {
            Assert.That(await tree.GetAsync(key), Is.Null,
                $"Tombstone for '{key}' was lost across the fold and the key came back to life.");
            Assert.That(await tree.ExistsAsync(key), Is.False);
        }

        var ttlAfter = await tree.GetAsync("ttl-key");
        Assert.That(ttlAfter, Is.Not.Null, "An unexpired TTL entry must survive the fold.");
        Assert.That(ttlAfter, Is.EqualTo(ttlValue).AsCollection);
    }

    [Test]
    public async Task Crdt_values_converge_across_a_fold()
    {
        var treeId = $"cons-crdt-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Spread the shapes across many keys so at least some land on the
        // donor's virtual slots whichever way the hash falls.
        const int fanOut = 24;
        for (var i = 0; i < fanOut; i++)
        {
            await tree.GCounter($"gc-{i}").IncrementAsync("r1", 5);
            await tree.GCounter($"gc-{i}").IncrementAsync("r2", 7);

            await tree.PnCounter($"pn-{i}").IncrementAsync("r1", 10);
            await tree.PnCounter($"pn-{i}").DecrementAsync("r1", 3);

            await tree.GSet($"gs-{i}").AddAsync(Encoding.UTF8.GetBytes("alpha"));
            await tree.GSet($"gs-{i}").AddAsync(Encoding.UTF8.GetBytes("beta"));

            await tree.OrSet($"os-{i}").AddAsync(Encoding.UTF8.GetBytes("kept"), "r1");
            await tree.OrSet($"os-{i}").AddAsync(Encoding.UTF8.GetBytes("dropped"), "r1");
            await tree.OrSet($"os-{i}").RemoveAsync(Encoding.UTF8.GetBytes("dropped"));

            await tree.OrFlag($"of-{i}").EnableAsync("r1");
            await tree.RwFlag($"rf-{i}").EnableAsync("r1");
        }

        await RunFoldAsync(treeId, donor: 3, survivor: 2);

        for (var i = 0; i < fanOut; i++)
        {
            Assert.That(await tree.GCounter($"gc-{i}").ValueAsync(), Is.EqualTo(12),
                $"GCounter 'gc-{i}' lost a replica's contribution across the fold.");
            Assert.That(await tree.PnCounter($"pn-{i}").ValueAsync(), Is.EqualTo(7),
                $"PnCounter 'pn-{i}' did not converge across the fold.");

            Assert.That(await tree.GSet($"gs-{i}").ContainsAsync(Encoding.UTF8.GetBytes("alpha")), Is.True);
            Assert.That(await tree.GSet($"gs-{i}").ContainsAsync(Encoding.UTF8.GetBytes("beta")), Is.True);

            Assert.That(await tree.OrSet($"os-{i}").ContainsAsync(Encoding.UTF8.GetBytes("kept")), Is.True);
            Assert.That(await tree.OrSet($"os-{i}").ContainsAsync(Encoding.UTF8.GetBytes("dropped")), Is.False,
                $"OrSet 'os-{i}' resurrected a removed element - its causality metadata did not survive.");

            Assert.That(await tree.OrFlag($"of-{i}").IsEnabledAsync(), Is.True);
            Assert.That(await tree.RwFlag($"rf-{i}").IsEnabledAsync(), Is.True);
        }
    }

    [Test]
    public async Task Crdt_writes_that_race_a_fold_still_converge()
    {
        var treeId = $"cons-crdt-race-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        const int keys = 12;
        for (var i = 0; i < keys; i++)
            await tree.GCounter($"race-{i}").IncrementAsync("seed", 1);

        var increments = new int[keys];
        using var cts = new CancellationTokenSource();
        var writer = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                for (var i = 0; i < keys && !cts.IsCancellationRequested; i++)
                {
                    await tree.GCounter($"race-{i}").IncrementAsync("racer", 1);
                    Interlocked.Increment(ref increments[i]);
                }
            }
        });

        await RunFoldAsync(treeId, donor: 3, survivor: 2);
        await cts.CancelAsync();
        await writer;

        for (var i = 0; i < keys; i++)
        {
            Assert.That(await tree.GCounter($"race-{i}").ValueAsync(),
                Is.EqualTo(1 + Volatile.Read(ref increments[i])),
                $"GCounter 'race-{i}' lost an increment that raced the fold.");
        }
    }

    // --- The unreachable-key hazard, end to end ---

    [Test]
    public async Task A_shard_split_away_and_then_folded_back_is_fully_readable_again()
    {
        // The sharpest failure mode a fold can have: the survivor is the shard
        // the donor was split out of, so it still refuses those slots. Without
        // the survivor-side reclaim the routing map would send readers to a
        // shard that sends them straight back to the retired donor.
        var treeId = $"cons-roundtrip-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "rt", 250);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/0");
        await split.SplitAsync(sourceShardIndex: 0);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);

        var afterSplit = await GetMapAsync(treeId);
        Assert.That(afterSplit.GetPhysicalShardIndices(), Does.Contain(4),
            "Precondition: the split must have allocated a new physical shard.");
        await AssertAllReadableAsync(tree, expected, "after the split");

        // Fold the freshly-created shard 4 back onto its adjacent neighbour 3,
        // then fold the whole chain back so the original shard 0 reclaims its
        // own split-away slots.
        await RunFoldAsync(treeId, donor: 4, survivor: 3);
        await AssertAllReadableAsync(tree, expected, "after folding the split shard back");

        await RunFoldAsync(treeId, donor: 3, survivor: 2);
        await RunFoldAsync(treeId, donor: 2, survivor: 1);
        await RunFoldAsync(treeId, donor: 1, survivor: 0);

        Assert.That(await PhysicalShardCountAsync(treeId), Is.EqualTo(1),
            "The tree must fold all the way back onto the shard that originally donated.");
        await AssertAllReadableAsync(tree, expected, "after folding every slot back onto shard 0");
        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count));
    }

    // --- Durability coherence ---

    [Test]
    public async Task A_fold_leaves_the_donor_state_and_wal_retention_intact()
    {
        // The highest-severity invariant: a retired donor must not license a
        // WAL trim over data the survivor has not absorbed. Consolidation makes
        // that structural by never deleting donor leaf state and never
        // releasing a pin - the donor is retired from routing only.
        var treeId = $"cons-durability-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "dk", 200);

        var walUsage = _cluster.GrainFactory.GetGrain<ILatticeWalUsage>(treeId);
        var retainedBefore = (await walUsage.GetWalUsageAsync(CancellationToken.None)).WalRetainedBytes;

        var physicalTreeId = await _cluster.GrainFactory
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeId);
        var donor = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/3");
        var donorLeafBefore = await donor.GetLeftmostLeafIdAsync();

        await RunFoldAsync(treeId, donor: 3, survivor: 2);

        Assert.That(await donor.IsDeletedAsync(), Is.False,
            "A retired donor must never be soft-deleted; its leaves and their pins have to survive.");
        Assert.That(await donor.GetLeftmostLeafIdAsync(), Is.EqualTo(donorLeafBefore),
            "The donor's leaf chain must be intact after the fold, so its durable checkpoints stand.");

        var retainedAfter = (await walUsage.GetWalUsageAsync(CancellationToken.None)).WalRetainedBytes;
        Assert.That(retainedAfter, Is.GreaterThanOrEqualTo(retainedBefore),
            "A fold must never make a WAL prefix trimmable that was not trimmable before it ran.");

        await AssertAllReadableAsync(tree, expected, "after the fold");
    }

    [Test]
    public async Task A_folded_tree_survives_a_shard_reactivation()
    {
        var treeId = $"cons-reactivate-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "ra", 150);

        await RunFoldAsync(treeId, donor: 3, survivor: 2);

        var physicalTreeId = await _cluster.GrainFactory
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeId);
        foreach (var index in (await GetMapAsync(treeId)).GetPhysicalShardIndices())
            await _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{index}").ForceDeactivateAsync();

        await AssertAllReadableAsync(tree, expected, "after reactivating every surviving shard");
        Assert.That(await tree.CountAsync(), Is.EqualTo(expected.Count));
    }

    // --- Cancellation and progress ---

    [Test]
    public async Task A_cancelled_fold_leaves_the_tree_readable_writable_and_foldable_again()
    {
        var treeId = $"cons-cancel-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var expected = await PopulateAsync(tree, "ck", 120);

        var coordinator = Consolidator(treeId, 3);
        await coordinator.StartAsync(2);
        Assert.That(await coordinator.CancelAsync(), Is.True);
        await coordinator.RunConsolidationPassAsync();

        Assert.That(await coordinator.IsIdleAsync(), Is.True);
        var progress = await coordinator.GetProgressAsync();
        Assert.That(progress.Cancelled, Is.True);
        Assert.That(progress.Complete, Is.False);
        Assert.That(await PhysicalShardCountAsync(treeId), Is.EqualTo(FourShardClusterFixture.TestShardCount),
            "An abandoned fold must leave the tree's physical topology exactly as it was.");

        await AssertAllReadableAsync(tree, expected, "after the fold was abandoned");
        await tree.SetAsync("post-cancel", Encoding.UTF8.GetBytes("ok"));
        Assert.That(await tree.GetAsync("post-cancel"), Is.Not.Null);

        // The abandoned fold must not have poisoned the pair.
        await RunFoldAsync(treeId, donor: 3, survivor: 2);
        Assert.That(await PhysicalShardCountAsync(treeId),
            Is.EqualTo(FourShardClusterFixture.TestShardCount - 1));
        await AssertAllReadableAsync(tree, expected, "after re-folding the previously abandoned pair");
    }

    [Test]
    public async Task Progress_reports_the_fold_advancing_and_then_completing()
    {
        var treeId = $"cons-progress-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await PopulateAsync(tree, "pk", 150);

        var coordinator = Consolidator(treeId, 3);
        await coordinator.StartAsync(2);

        var started = await coordinator.GetProgressAsync();
        Assert.That(started.InProgress, Is.True);
        Assert.That(started.DonorShardIndex, Is.EqualTo(3));
        Assert.That(started.SurvivorShardIndex, Is.EqualTo(2));
        Assert.That(started.SlotsToFold, Is.GreaterThan(0));

        for (var i = 0; i < 64 && !await coordinator.IsIdleAsync(); i++)
            await coordinator.RunConsolidationPassAsync();

        var finished = await coordinator.GetProgressAsync();
        Assert.That(finished.Complete, Is.True);
        Assert.That(finished.InProgress, Is.False);
        Assert.That(finished.UpdatedAtTicks, Is.GreaterThanOrEqualTo(started.UpdatedAtTicks));
        Assert.That(finished.OperationId, Is.EqualTo(started.OperationId));
    }

    [Test]
    public async Task Consolidation_is_refused_while_a_split_is_in_flight_on_the_same_shard()
    {
        var treeId = $"cons-vs-split-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await PopulateAsync(tree, "sk", 60);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/3");
        await split.SplitAsync(sourceShardIndex: 3);

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Consolidator(treeId, 3).StartAsync(2),
            "A fold and a split contending for one shard's migration record would strand slots.");

        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);
    }
}
