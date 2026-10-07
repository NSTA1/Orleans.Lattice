using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the durable WAL move fence on
/// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeRegistryGrain"/>
/// (issue #4525): <c>RaiseWalMoveFencesAsync</c>, <c>ReleaseWalMoveFenceAsync</c>
/// and the fenced flip <c>FlipFencedWalPlacementAsync</c>. Every test runs
/// against the JSON-backed in-memory store, so each fence also round-trips
/// through the persisted registry entry.
/// </summary>
public partial class LatticeRegistryGrainTests
{
    private static readonly TimeSpan LongLease = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan LapsedLease = TimeSpan.FromTicks(1);

    private static async Task<Orleans.Lattice.BPlusTree.Grains.LatticeRegistryGrain> RegisteredGrainAsync(string treeId)
    {
        var (grain, tree) = CreateGrain();
        BackWithInMemoryStore(tree);
        await grain.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1 });
        return grain;
    }

    private static async Task LetLapsedLeasePassAsync() => await Task.Delay(TimeSpan.FromMilliseconds(20));

    [Test]
    public async Task RaiseWalMoveFencesAsync_persists_the_fence_without_bumping_the_placement_version()
    {
        var grain = await RegisteredGrainAsync("fence-raise");

        await grain.RaiseWalMoveFencesAsync("fence-raise", 0, [0, 1], "move-a", LongLease, renew: false);

        var reread = await grain.GetWalPlacementAsync("fence-raise");
        Assert.Multiple(() =>
        {
            Assert.That(reread.Version, Is.EqualTo(0), "a fence is not a placement change");
            Assert.That(reread.ResolveFence(0)?.MoveId, Is.EqualTo("move-a"));
            Assert.That(reread.ResolveFence(1)?.MoveId, Is.EqualTo("move-a"));
            Assert.That(reread.ResolveFence(0)?.SourceProviderKey, Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
            Assert.That(reread.ResolveFence(0)!.LeaseExpiresUtcTicks, Is.GreaterThan(DateTime.UtcNow.Ticks));
        });
    }

    [Test]
    public async Task RaiseWalMoveFencesAsync_refuses_a_partition_another_move_holds_a_live_fence_on()
    {
        var grain = await RegisteredGrainAsync("fence-held");
        await grain.RaiseWalMoveFencesAsync("fence-held", 0, [0], "move-a", LongLease, renew: false);

        Assert.That(
            async () => await grain.RaiseWalMoveFencesAsync("fence-held", 0, [0], "move-b", LongLease, renew: false),
            Throws.InvalidOperationException.With.Message.Contains("another placement move"));
        Assert.That((await grain.GetWalPlacementAsync("fence-held")).ResolveFence(0)?.MoveId, Is.EqualTo("move-a"));
    }

    [Test]
    public async Task RaiseWalMoveFencesAsync_takes_over_another_moves_lapsed_fence()
    {
        var grain = await RegisteredGrainAsync("fence-takeover");
        await grain.RaiseWalMoveFencesAsync("fence-takeover", 0, [0], "move-a", LapsedLease, renew: false);
        await LetLapsedLeasePassAsync();

        await grain.RaiseWalMoveFencesAsync("fence-takeover", 0, [0], "move-b", LongLease, renew: false);

        Assert.That((await grain.GetWalPlacementAsync("fence-takeover")).ResolveFence(0)?.MoveId, Is.EqualTo("move-b"));
    }

    [Test]
    public async Task RaiseWalMoveFencesAsync_renewal_refuses_once_the_fence_was_released()
    {
        var grain = await RegisteredGrainAsync("fence-renew");
        await grain.RaiseWalMoveFencesAsync("fence-renew", 0, [0], "move-a", LapsedLease, renew: false);
        await LetLapsedLeasePassAsync();
        await grain.ReleaseWalMoveFenceAsync("fence-renew", 0, "move-a", onlyIfExpired: true);

        Assert.That(
            async () => await grain.RaiseWalMoveFencesAsync("fence-renew", 0, [0], "move-a", LongLease, renew: true),
            Throws.InvalidOperationException.With.Message.Contains("no longer holds its fence"));
        Assert.That((await grain.GetWalPlacementAsync("fence-renew")).ResolveFence(0), Is.Null,
            "a renewal must never re-create a fence that was released");
    }

    [Test]
    public async Task RaiseWalMoveFencesAsync_renewal_extends_the_lease_of_the_moves_own_fence()
    {
        var grain = await RegisteredGrainAsync("fence-extend");
        await grain.RaiseWalMoveFencesAsync("fence-extend", 0, [0], "move-a", TimeSpan.FromSeconds(1), renew: false);
        var before = (await grain.GetWalPlacementAsync("fence-extend")).ResolveFence(0)!.LeaseExpiresUtcTicks;

        await grain.RaiseWalMoveFencesAsync("fence-extend", 0, [0], "move-a", LongLease, renew: true);

        Assert.That((await grain.GetWalPlacementAsync("fence-extend")).ResolveFence(0)!.LeaseExpiresUtcTicks,
            Is.GreaterThan(before));
    }

    [Test]
    public async Task RaiseWalMoveFencesAsync_refuses_a_stale_placement_version()
    {
        var grain = await RegisteredGrainAsync("fence-stale");
        await grain.UpdateWalPlacementAsync("fence-stale", 0, 1, "secondary");

        Assert.That(
            async () => await grain.RaiseWalMoveFencesAsync("fence-stale", 0, [0], "move-a", LongLease, renew: false),
            Throws.InvalidOperationException.With.Message.Contains("changed concurrently"));
    }

    [Test]
    public async Task ReleaseWalMoveFenceAsync_only_if_expired_keeps_a_live_fence_and_releases_a_lapsed_one()
    {
        var grain = await RegisteredGrainAsync("fence-release");
        await grain.RaiseWalMoveFencesAsync("fence-release", 0, [0], "live", LongLease, renew: false);
        await grain.RaiseWalMoveFencesAsync("fence-release", 0, [1], "lapsed", LapsedLease, renew: false);
        await LetLapsedLeasePassAsync();

        var afterLive = await grain.ReleaseWalMoveFenceAsync("fence-release", 0, "live", onlyIfExpired: true);
        var afterLapsed = await grain.ReleaseWalMoveFenceAsync("fence-release", 1, "lapsed", onlyIfExpired: true);

        Assert.Multiple(() =>
        {
            Assert.That(afterLive.ResolveFence(0)?.MoveId, Is.EqualTo("live"));
            Assert.That(afterLapsed.ResolveFence(1), Is.Null);
            Assert.That(afterLapsed.Version, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task ReleaseWalMoveFenceAsync_ignores_a_fence_held_by_another_move()
    {
        var grain = await RegisteredGrainAsync("fence-foreign");
        await grain.RaiseWalMoveFencesAsync("fence-foreign", 0, [0], "move-a", LongLease, renew: false);

        var after = await grain.ReleaseWalMoveFenceAsync("fence-foreign", 0, "move-b", onlyIfExpired: false);

        Assert.That(after.ResolveFence(0)?.MoveId, Is.EqualTo("move-a"));
    }

    [Test]
    public async Task FlipFencedWalPlacementAsync_flips_and_clears_the_moves_fence_in_one_write()
    {
        var grain = await RegisteredGrainAsync("fence-flip");
        await grain.RaiseWalMoveFencesAsync("fence-flip", 0, [0, 1], "move-a", LongLease, renew: false);

        var flipped = await grain.FlipFencedWalPlacementAsync("fence-flip", 0, [(0, "secondary")], "move-a");

        var reread = await grain.GetWalPlacementAsync("fence-flip");
        Assert.Multiple(() =>
        {
            Assert.That(flipped.Version, Is.EqualTo(1));
            Assert.That(reread.ResolveKey(0), Is.EqualTo("secondary"));
            Assert.That(reread.ResolveFence(0), Is.Null, "the flip clears the moved partition's fence");
            Assert.That(reread.ResolveFence(1)?.MoveId, Is.EqualTo("move-a"), "an unmoved partition keeps its fence");
        });
    }

    /// <summary>
    /// The registry half of issue #4525: once the move's fence lapsed and an
    /// activation of the source released it, the source may have acknowledged
    /// appends the copy never saw, so the flip must be refused.
    /// </summary>
    [Test]
    public async Task FlipFencedWalPlacementAsync_refuses_a_flip_whose_fence_was_released()
    {
        var grain = await RegisteredGrainAsync("fence-flip-released");
        await grain.RaiseWalMoveFencesAsync("fence-flip-released", 0, [0], "move-a", LapsedLease, renew: false);
        await LetLapsedLeasePassAsync();
        await grain.ReleaseWalMoveFenceAsync("fence-flip-released", 0, "move-a", onlyIfExpired: true);

        Assert.That(
            async () => await grain.FlipFencedWalPlacementAsync("fence-flip-released", 0, [(0, "secondary")], "move-a"),
            Throws.InvalidOperationException.With.Message.Contains("refused to flip"));
        var reread = await grain.GetWalPlacementAsync("fence-flip-released");
        Assert.That(reread.Version, Is.EqualTo(0));
        Assert.That(reread.ResolveKey(0), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }

    [Test]
    public async Task FlipFencedWalPlacementAsync_refuses_a_flip_whose_fence_another_move_took_over()
    {
        var grain = await RegisteredGrainAsync("fence-flip-taken");
        await grain.RaiseWalMoveFencesAsync("fence-flip-taken", 0, [0], "move-a", LapsedLease, renew: false);
        await LetLapsedLeasePassAsync();
        await grain.RaiseWalMoveFencesAsync("fence-flip-taken", 0, [0], "move-b", LongLease, renew: false);

        Assert.That(
            async () => await grain.FlipFencedWalPlacementAsync("fence-flip-taken", 0, [(0, "secondary")], "move-a"),
            Throws.InvalidOperationException.With.Message.Contains("refused to flip"));
        Assert.That((await grain.GetWalPlacementAsync("fence-flip-taken")).Version, Is.EqualTo(0));
    }

    [Test]
    public async Task FlipFencedWalPlacementAsync_flips_a_fence_that_lapsed_but_was_never_released()
    {
        // A lapsed fence nobody released still guards: an activation must release
        // it before it serves an append, so the flip is still safe.
        var grain = await RegisteredGrainAsync("fence-flip-lapsed");
        await grain.RaiseWalMoveFencesAsync("fence-flip-lapsed", 0, [0], "move-a", LapsedLease, renew: false);
        await LetLapsedLeasePassAsync();

        var flipped = await grain.FlipFencedWalPlacementAsync("fence-flip-lapsed", 0, [(0, "secondary")], "move-a");

        Assert.That(flipped.ResolveKey(0), Is.EqualTo("secondary"));
    }

    [Test]
    public void WalMoveFenceLeaseTicks_saturates_an_extreme_lease()
    {
        var now = DateTime.UtcNow.Ticks;
        Assert.Multiple(() =>
        {
            Assert.That(
                Orleans.Lattice.BPlusTree.Grains.LatticeRegistryGrain.WalMoveFenceLeaseTicks(now, TimeSpan.MaxValue),
                Is.EqualTo(DateTime.MaxValue.Ticks));
            Assert.That(
                Orleans.Lattice.BPlusTree.Grains.LatticeRegistryGrain.WalMoveFenceLeaseTicks(now, TimeSpan.FromSeconds(1)),
                Is.EqualTo(now + TimeSpan.TicksPerSecond));
        });
    }
}
