using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers the <b>driver</b> half of the work-bounded projection-admin verbs on
/// <see cref="LatticeGrain"/> - the loops in <c>LatticeGrain.ProjectionAdmin.cs</c>
/// that pump <see cref="IShardRootGrain.RebuildShardProjectionBoundedAsync"/> and
/// <see cref="IShardRootGrain.GetShardMaterialiserLagBoundedAsync"/> batch by batch
/// until the shard reports no resume position.
/// <para>
/// The shard-root half of these walks (that a batch stops within its budget and
/// yields a resume key) is pinned by <c>ShardRootGrainPartialWalkWorkBoundTests</c>.
/// What had no fixture at all was the caller that <em>drives</em> those batches:
/// every existing test returned a terminal page on the first call, so the
/// continuation arms - re-issuing with the returned cursor, keeping the first
/// batch's WAL heads, and <c>min</c>-reducing the per-batch checkpoints - were
/// never executed. That is the machinery issue 1972 added to stop an operator
/// rebuild or lag query holding a non-reentrant shard for the length of the whole
/// leaf chain, so it is exactly the part worth pinning.
/// </para>
/// </summary>
[TestFixture]
public class LatticeGrainProjectionAdminDriverTests
{
    private const string TreeId = "projection-admin-driver-tree";

    private static (LatticeGrain grain, IShardRootGrain shard, ILatticeRegistry registry) CreateGrain(
        int shardCount = 1,
        ShardMap? shardMap = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult(shardMap));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = shardCount }));

        var shard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shard);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, shard, registry);
    }

    /// <summary>
    /// Builds a shard map whose slots all address physical shard 0, so a
    /// single-shard tree routes every fan-out to the one substituted shard root.
    /// </summary>
    private static ShardMap SingleShardMap() => new() { Slots = new int[8], Version = 1 };

    // ---------------------------------------------------------------- rebuild

    /// <summary>
    /// The rebuild driver must re-issue the bounded batch with the cursor the
    /// previous batch returned, and keep going until a batch reports no resume
    /// position. A single-batch stub would leave the continuation arm dead.
    /// </summary>
    [Test]
    public async Task RebuildLeafProjectionAsync_drives_every_batch_until_the_shard_stops_resuming()
    {
        var (grain, shard, _) = CreateGrain(shardMap: SingleShardMap());

        var cursors = new List<string?>();
        var batches = 0;
        shard.RebuildShardProjectionBoundedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                cursors.Add(ci.Arg<string?>());
                batches++;
                return new ShardProjectionRebuildPage
                {
                    LeavesRebuilt = 4,
                    // Three bounded batches, then a terminal page.
                    ResumeFromInclusive = batches < 3 ? $"k{batches}" : null,
                };
            });

        await grain.RebuildLeafProjectionAsync(0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(batches, Is.EqualTo(3), "the driver must pump batches until one reports no resume position");
            Assert.That(cursors, Is.EqualTo(new string?[] { null, "k1", "k2" }),
                "each batch must be re-issued with the cursor the previous batch returned, "
                + "starting from null; re-sending null would re-walk the chain forever");
        });
    }

    /// <summary>
    /// The negative control for the loop above: a shard whose very first batch is
    /// terminal must be called exactly once, so the driver cannot be "passing" the
    /// multi-batch test by unconditionally re-issuing.
    /// </summary>
    [Test]
    public async Task RebuildLeafProjectionAsync_issues_a_single_batch_when_the_first_page_is_terminal()
    {
        var (grain, shard, _) = CreateGrain(shardMap: SingleShardMap());
        shard.RebuildShardProjectionBoundedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ShardProjectionRebuildPage { LeavesRebuilt = 2, ResumeFromInclusive = null });

        await grain.RebuildLeafProjectionAsync(0, CancellationToken.None);

        await shard.Received(1).RebuildShardProjectionBoundedAsync(null, Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Cancellation is honoured at the batch boundary, which is the granularity
    /// the verb documents ("cancelling already stops the fan-out before the next
    /// leaf"). Cancelling after the first batch must abort the walk rather than
    /// run it to completion.
    /// </summary>
    [Test]
    public void RebuildLeafProjectionAsync_stops_at_a_batch_boundary_when_cancelled()
    {
        var (grain, shard, _) = CreateGrain(shardMap: SingleShardMap());
        using var cts = new CancellationTokenSource();

        var batches = 0;
        shard.RebuildShardProjectionBoundedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                batches++;
                cts.Cancel();
                // Never terminal: only cancellation can end this walk.
                return new ShardProjectionRebuildPage { LeavesRebuilt = 1, ResumeFromInclusive = $"k{batches}" };
            });

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await grain.RebuildLeafProjectionAsync(0, cts.Token));
        Assert.That(batches, Is.EqualTo(1), "the walk must abort at the first batch boundary after cancellation");
    }

    /// <summary>
    /// A shard index outside the tree's physical shard set is rejected before any
    /// shard grain is addressed, so an off-by-one operator mistake cannot silently
    /// activate an empty shard.
    /// </summary>
    [Test]
    public void RebuildLeafProjectionAsync_rejects_a_shard_index_outside_the_map()
    {
        var (grain, shard, _) = CreateGrain(shardMap: SingleShardMap());

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await grain.RebuildLeafProjectionAsync(7, CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("is not a physical shard"));
        Assert.That(shard.ReceivedCalls(), Is.Empty);
    }

    // -------------------------------------------------------------------- lag

    /// <summary>
    /// The lag driver must keep the WAL heads captured on the <em>first</em>
    /// batch and <c>min</c>-reduce the per-batch checkpoints. A later batch's
    /// heads must never replace the first batch's, otherwise a tree committing
    /// mid-walk inflates the reported lag - the false alarm
    /// <see cref="ShardMaterialiserLagPage.WalHeadOffsets"/> is documented to
    /// prevent.
    /// </summary>
    [Test]
    public async Task GetMaterialiserLagAsync_keeps_the_first_batch_heads_and_min_reduces_the_checkpoints()
    {
        var (grain, shard, _) = CreateGrain(shardMap: SingleShardMap());

        var pages = new Queue<ShardMaterialiserLagPage>(
        [
            // Heads pinned here: 100. Checkpoint 90.
            new ShardMaterialiserLagPage
            {
                WalHeadOffsets = [100],
                MinCheckpointOffset = 90,
                ResumeFromInclusive = "k1",
            },
            // The lowest checkpoint of the walk - this is the one that must win.
            new ShardMaterialiserLagPage
            {
                WalHeadOffsets = [100_000],
                MinCheckpointOffset = 40,
                ResumeFromInclusive = "k2",
            },
            // Higher again, and terminal: min must not regress back up.
            new ShardMaterialiserLagPage
            {
                WalHeadOffsets = [100_000],
                MinCheckpointOffset = 70,
                ResumeFromInclusive = null,
            },
        ]);

        var cursors = new List<string?>();
        shard.GetShardMaterialiserLagBoundedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                cursors.Add(ci.Arg<string?>());
                return pages.Dequeue();
            });

        var lag = await grain.GetMaterialiserLagAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(lag, Is.EqualTo(60),
                "lag must be the first batch's head (100) minus the min checkpoint across every "
                + "batch (40); using a later batch's head or a later checkpoint would inflate it");
            Assert.That(cursors, Is.EqualTo(new string?[] { null, "k1", "k2" }),
                "each lag batch must resume from the cursor the previous batch returned");
            Assert.That(pages, Is.Empty, "every batch must be consumed");
        });
    }

    /// <summary>
    /// A tree whose shard map addresses no physical shard at all short-circuits to
    /// zero without addressing a shard grain. This is the degenerate arm that
    /// keeps the fan-out's <c>Task[]</c> allocation off an empty walk.
    /// </summary>
    [Test]
    public async Task GetMaterialiserLagAsync_returns_zero_for_a_tree_with_no_physical_shards()
    {
        var (grain, shard, _) = CreateGrain(shardMap: new ShardMap { Slots = [], Version = 1 });

        var lag = await grain.GetMaterialiserLagAsync(CancellationToken.None);

        Assert.That(lag, Is.Zero);
        Assert.That(shard.ReceivedCalls(), Is.Empty, "no shard may be addressed when the map is empty");
    }

    /// <summary>
    /// The fan-out reduces with <c>max</c>, not mean or sum: back-pressure is
    /// dominated by the slowest shard. Two shards with different lags must report
    /// the larger.
    /// </summary>
    [Test]
    public async Task GetMaterialiserLagAsync_reduces_across_shards_with_max()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        // Slots alternate between physical shard 0 and 1, so the fan-out addresses both.
        registry.GetShardMapAsync(Arg.Any<string>())
            .Returns(Task.FromResult<ShardMap?>(new ShardMap { Slots = [0, 1, 0, 1], Version = 1 }));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 2 }));

        // Route each physical shard id to its own substitute so the two lags differ.
        static IShardRootGrain ShardWithLag(long head, long checkpoint)
        {
            var s = Substitute.For<IShardRootGrain>();
            s.GetShardMaterialiserLagBoundedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
                .Returns(new ShardMaterialiserLagPage
                {
                    WalHeadOffsets = [head],
                    MinCheckpointOffset = checkpoint,
                    ResumeFromInclusive = null,
                });
            return s;
        }

        var slow = ShardWithLag(500, 100);   // lag 400
        var fast = ShardWithLag(500, 480);   // lag 20
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>())
            .Returns(ci => ci.ArgAt<string>(0).EndsWith("/0", StringComparison.Ordinal) ? slow : fast);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(
            context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);

        var lag = await grain.GetMaterialiserLagAsync(CancellationToken.None);

        Assert.That(lag, Is.EqualTo(400), "the reported lag must be the slowest shard's, not the mean or the sum");
    }

    /// <summary>
    /// A shard that visited no leaf leaves <see cref="long.MaxValue"/> standing as
    /// its minimum, which the reduction reads as "no projection state exists" and
    /// reports the heads themselves as the lag.
    /// </summary>
    [Test]
    public async Task GetMaterialiserLagAsync_reports_the_heads_when_a_shard_has_no_projection_state()
    {
        var (grain, shard, _) = CreateGrain(shardMap: SingleShardMap());
        shard.GetShardMaterialiserLagBoundedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(new ShardMaterialiserLagPage
            {
                WalHeadOffsets = [7, 11],
                MinCheckpointOffset = long.MaxValue,
                ResumeFromInclusive = null,
            });

        var lag = await grain.GetMaterialiserLagAsync(CancellationToken.None);

        Assert.That(lag, Is.EqualTo(18), "an unprojected shard reports the sum of its WAL heads as the lag");
    }

    /// <summary>
    /// Cancelling between two lag batches aborts the walk at that boundary rather
    /// than draining the rest of the chain.
    /// </summary>
    [Test]
    public void GetMaterialiserLagAsync_stops_at_a_batch_boundary_when_cancelled()
    {
        var (grain, shard, _) = CreateGrain(shardMap: SingleShardMap());
        using var cts = new CancellationTokenSource();

        var batches = 0;
        shard.GetShardMaterialiserLagBoundedAsync(Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                batches++;
                if (batches == 2) cts.Cancel();
                return new ShardMaterialiserLagPage
                {
                    WalHeadOffsets = batches == 1 ? [100] : [],
                    MinCheckpointOffset = 10,
                    ResumeFromInclusive = $"k{batches}",
                };
            });

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await grain.GetMaterialiserLagAsync(cts.Token));
        Assert.That(batches, Is.EqualTo(2), "the walk must abort at the batch boundary after cancellation");
    }
}
