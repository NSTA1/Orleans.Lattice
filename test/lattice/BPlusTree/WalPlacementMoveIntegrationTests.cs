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
}
