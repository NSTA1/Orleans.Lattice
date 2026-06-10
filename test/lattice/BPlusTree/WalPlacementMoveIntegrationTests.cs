using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for the <see cref="ILatticeAdmin"/> WAL placement managed
/// move surface against a live single-silo cluster: a partition's WAL is moved
/// from the baseline ("default") provider to a named ("secondary") provider,
/// post-move appends land on the target, the moved tail is readable at its
/// original offsets, the source is retained until an explicit reclaim, and a
/// move can be reverted by moving the partition back.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalPlacementMoveIntegrationTests
{
    private WalPlacementMoveClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new WalPlacementMoveClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeAdmin Admin =>
        _fixture.Cluster.Client.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);

    private static async Task<long> WaitForHighestAsync(
        InMemoryWalStorageProvider provider, string physicalTreeId, int partition, long atLeast)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var highest = await provider.GetHighestOffsetAsync(physicalTreeId, partition, CancellationToken.None);
            if (highest >= atLeast)
            {
                return highest;
            }
            await Task.Delay(50);
        }
        return await provider.GetHighestOffsetAsync(physicalTreeId, partition, CancellationToken.None);
    }

    private static async Task WriteKeysAsync(ILattice tree, string prefix, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await tree.SetAsync($"{prefix}-{i}", Encoding.UTF8.GetBytes($"value-{prefix}-{i}"));
        }
    }

    [Test]
    public async Task ExecuteWalMove_copies_tail_routes_new_writes_to_target_and_retains_source()
    {
        var treeId = $"move-fwd-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var routing = await tree.GetRoutingAsync();
        var physical = routing.PhysicalTreeId;

        await WriteKeysAsync(tree, "pre", 8);
        var srcHighestBefore = await WaitForHighestAsync(WalMoveProviders.Baseline, physical, 0, 0);
        Assert.That(srcHighestBefore, Is.GreaterThanOrEqualTo(0), "pre-move writes must land on the baseline WAL");

        // Plan reports a real copy to a resolvable target.
        var plan = await Admin.PlanWalMoveAsync(treeId, 0, "secondary");
        Assert.Multiple(() =>
        {
            Assert.That(plan.FromProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
            Assert.That(plan.ToProviderKey, Is.EqualTo("secondary"));
            Assert.That(plan.TargetResolvableOnThisSilo, Is.True);
            Assert.That(plan.AlreadyAtTarget, Is.False);
            Assert.That(plan.EntriesToCopy, Is.GreaterThan(0));
        });

        // Execute the move.
        var receipt = await Admin.ExecuteWalMoveAsync(treeId, 0, "secondary");
        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(receipt.PreviousPlacementVersion, Is.EqualTo(0));
            Assert.That(receipt.NewPlacementVersion, Is.EqualTo(1));
            Assert.That(receipt.SourceRetained, Is.True);
        });

        // The target now holds the moved tail at the original highest offset...
        var dstHighest = await WalMoveProviders.Secondary.GetHighestOffsetAsync(physical, 0, CancellationToken.None);
        Assert.That(dstHighest, Is.EqualTo(srcHighestBefore), "target tail must match the source tail copied across");

        // ...and the moved entries are readable from the target at their offsets.
        var copied = new List<long>();
        await foreach (var entry in WalMoveProviders.Secondary.ReadAsync(physical, 0, -1, 1000, CancellationToken.None))
        {
            copied.Add(entry.Offset);
        }
        Assert.That(copied, Is.Not.Empty);
        Assert.That(copied[^1], Is.EqualTo(srcHighestBefore));

        // The source is NOT auto-trimmed: it still holds its tail.
        var srcHighestAfter = await WalMoveProviders.Baseline.GetHighestOffsetAsync(physical, 0, CancellationToken.None);
        Assert.That(srcHighestAfter, Is.EqualTo(srcHighestBefore), "a move must never trim the source");

        // Placement now routes partition 0 to the target.
        var placement = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Version, Is.EqualTo(1));
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo("secondary"));

        // New writes after the move land on the target, not the source.
        await WriteKeysAsync(tree, "post", 6);
        var dstAfterPost = await WaitForHighestAsync(WalMoveProviders.Secondary, physical, 0, dstHighest + 1);
        Assert.That(dstAfterPost, Is.GreaterThan(dstHighest), "post-move writes must extend the target");
        var srcAfterPost = await WalMoveProviders.Baseline.GetHighestOffsetAsync(physical, 0, CancellationToken.None);
        Assert.That(srcAfterPost, Is.EqualTo(srcHighestBefore), "post-move writes must not touch the source");

        // Explicit reclaim discards the orphaned source tail.
        var reclaim = await Admin.ReclaimMovedWalSourceAsync(treeId, 0, IWalStorageProviderCatalog.DefaultProviderKey);
        Assert.That(reclaim.Outcome, Is.EqualTo(WalMoveOutcome.SourceReclaimed));
        var srcAfterReclaim = await WalMoveProviders.Baseline.GetHighestOffsetAsync(physical, 0, CancellationToken.None);
        Assert.That(srcAfterReclaim, Is.EqualTo(-1), "reclaim must trim the orphaned source");
    }

    [Test]
    public async Task ExecuteWalMove_is_reversible_by_moving_the_partition_back()
    {
        var treeId = $"move-rev-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var routing = await tree.GetRoutingAsync();
        var physical = routing.PhysicalTreeId;

        await WriteKeysAsync(tree, "k", 5);
        await WaitForHighestAsync(WalMoveProviders.Baseline, physical, 0, 0);

        var forward = await Admin.ExecuteWalMoveAsync(treeId, 0, "secondary");
        Assert.That(forward.NewPlacementVersion, Is.EqualTo(1));

        // Revert: move the partition back to the default key.
        var back = await Admin.ExecuteWalMoveAsync(treeId, 0, IWalStorageProviderCatalog.DefaultProviderKey);
        Assert.That(back.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
        Assert.That(back.NewPlacementVersion, Is.EqualTo(2));

        var placement = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));

        // Data remains readable through the tree after the round trip.
        var value = await tree.GetAsync("k-0");
        Assert.That(value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("value-k-0"));
    }

    [Test]
    public async Task ExecuteWalMove_to_unregistered_provider_fails_closed()
    {
        var treeId = $"move-bad-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await WriteKeysAsync(tree, "k", 2);

        Assert.That(
            async () => await Admin.ExecuteWalMoveAsync(treeId, 0, "no-such-account"),
            Throws.TypeOf<LatticeWalProviderMissingException>());
    }

    [Test]
    public async Task ExecuteWalMove_to_current_provider_is_idempotent_no_op()
    {
        var treeId = $"move-noop-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await WriteKeysAsync(tree, "k", 3);

        var receipt = await Admin.ExecuteWalMoveAsync(treeId, 0, IWalStorageProviderCatalog.DefaultProviderKey);

        Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.AlreadyAtTarget));
        Assert.That(receipt.NewPlacementVersion, Is.EqualTo(0));
    }

    [Test]
    public async Task BatchExecuteWalMove_flips_every_partition_under_one_placement_version()
    {
        var treeId = $"move-batch-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await WriteKeysAsync(tree, "k", 16);

        var moves = new (int Partition, string TargetProviderKey)[] { (0, "secondary"), (1, "secondary"), (2, "secondary") };

        // Plan previews one entry per requested partition.
        var plan = await Admin.PlanWalMoveAsync(treeId, moves);
        Assert.Multiple(() =>
        {
            Assert.That(plan.PlacementVersion, Is.EqualTo(0));
            Assert.That(plan.Moves, Has.Length.EqualTo(3));
            Assert.That(plan.AllTargetsResolvableOnThisSilo, Is.True);
            Assert.That(plan.Moves.Select(m => m.Partition), Is.EqualTo(new[] { 0, 1, 2 }));
            Assert.That(plan.Moves.All(m => m.ToProviderKey == "secondary"), Is.True);
        });

        // Execute: a single placement bump (0 -> 1) covers all three partitions.
        var receipt = await Admin.ExecuteWalMoveAsync(treeId, moves);
        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(receipt.PreviousPlacementVersion, Is.EqualTo(0));
            Assert.That(receipt.NewPlacementVersion, Is.EqualTo(1));
            Assert.That(receipt.Moves, Has.Length.EqualTo(3));
            Assert.That(receipt.Moves.All(m => m.Outcome == WalMoveOutcome.Moved), Is.True);
            Assert.That(receipt.Moves.All(m => m.NewPlacementVersion == 1), Is.True);
            Assert.That(receipt.Moves.All(m => m.SourceRetained), Is.True);
        });

        // Placement reflects all three partitions on the target at version 1.
        var placement = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Version, Is.EqualTo(1));
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo("secondary"));
        Assert.That(placement.Partitions[1].ProviderKey, Is.EqualTo("secondary"));
        Assert.That(placement.Partitions[2].ProviderKey, Is.EqualTo("secondary"));
        Assert.That(placement.Partitions[3].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));

        // The batch is reversible: move all three back in one batch.
        var back = await Admin.ExecuteWalMoveAsync(treeId, new (int, string)[]
        {
            (0, IWalStorageProviderCatalog.DefaultProviderKey),
            (1, IWalStorageProviderCatalog.DefaultProviderKey),
            (2, IWalStorageProviderCatalog.DefaultProviderKey),
        });
        Assert.That(back.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
        Assert.That(back.NewPlacementVersion, Is.EqualTo(2));
        var reverted = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(reverted.Partitions[0].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(reverted.Partitions[1].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(reverted.Partitions[2].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }

    [Test]
    public async Task BatchExecuteWalMove_all_already_at_target_is_idempotent_no_op()
    {
        var treeId = $"move-batch-noop-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await WriteKeysAsync(tree, "k", 4);

        var receipt = await Admin.ExecuteWalMoveAsync(treeId, new (int, string)[]
        {
            (0, IWalStorageProviderCatalog.DefaultProviderKey),
            (1, IWalStorageProviderCatalog.DefaultProviderKey),
        });

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.AlreadyAtTarget));
            Assert.That(receipt.PreviousPlacementVersion, Is.EqualTo(0));
            Assert.That(receipt.NewPlacementVersion, Is.EqualTo(0));
            Assert.That(receipt.Moves.All(m => m.Outcome == WalMoveOutcome.AlreadyAtTarget), Is.True);
        });

        var placement = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Version, Is.EqualTo(0));
    }

    [Test]
    public async Task BatchExecuteWalMove_fails_closed_and_leaves_placement_unchanged_when_any_target_unresolvable()
    {
        var treeId = $"move-batch-bad-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await WriteKeysAsync(tree, "k", 4);

        Assert.That(
            async () => await Admin.ExecuteWalMoveAsync(treeId, new (int, string)[] { (0, "secondary"), (1, "no-such-account") }),
            Throws.TypeOf<LatticeWalProviderMissingException>());

        // The whole batch was refused before any pin flip - placement is untouched.
        var placement = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Version, Is.EqualTo(0));
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }

    [Test]
    public async Task BatchExecuteWalMove_rejects_a_partition_named_more_than_once()
    {
        var treeId = $"move-batch-dup-{Guid.NewGuid():N}";
        await _fixture.CreateTreeAsync(treeId);

        Assert.That(
            async () => await Admin.ExecuteWalMoveAsync(treeId, new (int, string)[] { (0, "secondary"), (0, "secondary") }),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task BatchExecuteWalMove_rejects_an_empty_batch()
    {
        var treeId = $"move-batch-empty-{Guid.NewGuid():N}";
        await _fixture.CreateTreeAsync(treeId);

        Assert.That(
            async () => await Admin.ExecuteWalMoveAsync(treeId, Array.Empty<(int, string)>()),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public async Task BatchExecuteWalMove_mixes_real_moves_and_already_at_target_repairs_in_request_order()
    {
        var treeId = $"move-batch-mixed-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await WriteKeysAsync(tree, "k", 16);

        // Pre-move partition 0 alone so the next batch sees it already at target.
        var first = await Admin.ExecuteWalMoveAsync(treeId, 0, "secondary");
        Assert.That(first.NewPlacementVersion, Is.EqualTo(1));

        // Batch with a no-op (partition 0, already on secondary) followed by a
        // real move (partition 1). Only the real move flips the pin, but it
        // flips once for the whole batch (1 -> 2).
        var receipt = await Admin.ExecuteWalMoveAsync(treeId, new (int, string)[]
        {
            (0, "secondary"),
            (1, "secondary"),
        });

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(receipt.PreviousPlacementVersion, Is.EqualTo(1));
            Assert.That(receipt.NewPlacementVersion, Is.EqualTo(2));
            Assert.That(receipt.Moves, Has.Length.EqualTo(2));
            // Receipts preserve request order with per-partition outcomes.
            Assert.That(receipt.Moves[0].Partition, Is.EqualTo(0));
            Assert.That(receipt.Moves[0].Outcome, Is.EqualTo(WalMoveOutcome.AlreadyAtTarget));
            Assert.That(receipt.Moves[1].Partition, Is.EqualTo(1));
            Assert.That(receipt.Moves[1].Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            // Every receipt reports the same post-batch version.
            Assert.That(receipt.Moves.All(m => m.NewPlacementVersion == 2), Is.True);
        });

        var placement = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Version, Is.EqualTo(2));
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo("secondary"));
        Assert.That(placement.Partitions[1].ProviderKey, Is.EqualTo("secondary"));

        // Data written before the moves remains readable through the tree.
        var value = await tree.GetAsync("k-0");
        Assert.That(value, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("value-k-0"));
    }

    [Test]
    public async Task BatchExecuteWalMove_with_parallel_concurrency_flips_atomically_and_preserves_data()
    {
        var treeId = $"move-batch-par-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await WriteKeysAsync(tree, "k", 24);

        // A concurrency ceiling at or above the batch size runs every partition's
        // copy phases in parallel (the unbounded Task.WhenAll path).
        var options = WalMoveOptions.Default with { MaxConcurrentPartitionMoves = 4 };
        var moves = new (int Partition, string TargetProviderKey)[] { (0, "secondary"), (1, "secondary"), (2, "secondary") };

        var receipt = await Admin.ExecuteWalMoveAsync(treeId, moves, options);
        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(receipt.PreviousPlacementVersion, Is.EqualTo(0));
            Assert.That(receipt.NewPlacementVersion, Is.EqualTo(1));
            Assert.That(receipt.Moves, Has.Length.EqualTo(3));
            Assert.That(receipt.Moves.All(m => m.Outcome == WalMoveOutcome.Moved), Is.True);
        });

        // One atomic version bump for the whole parallel batch.
        var placement = await Admin.GetWalPlacementAsync(treeId);
        Assert.That(placement.Version, Is.EqualTo(1));
        Assert.That(placement.Partitions[0].ProviderKey, Is.EqualTo("secondary"));
        Assert.That(placement.Partitions[1].ProviderKey, Is.EqualTo("secondary"));
        Assert.That(placement.Partitions[2].ProviderKey, Is.EqualTo("secondary"));

        // All pre-move data survives the parallel copy and remains readable.
        for (var i = 0; i < 24; i++)
        {
            var value = await tree.GetAsync($"k-{i}");
            Assert.That(value, Is.Not.Null, $"key k-{i} must survive the parallel batch move");
            Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo($"value-k-{i}"));
        }
    }
}
