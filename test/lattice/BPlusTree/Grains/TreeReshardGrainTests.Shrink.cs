using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The shrink half of the online reshard: a target below the current physical
/// shard count folds adjacent shards together through
/// <see cref="ITreeShardConsolidationGrain"/>, bounded by
/// <see cref="LatticeOptions.MaxConcurrentMigrations"/>, and completes only once
/// every fold it started - including the release of its donor's storage - has
/// finished.
/// </summary>
public partial class TreeReshardGrainTests
{
    private sealed class ShrinkHarness
    {
        public required Orleans.Lattice.BPlusTree.Grains.TreeReshardGrain Grain { get; init; }
        public required Fakes.FakePersistentState<TreeReshardState> State { get; init; }
        public required Dictionary<int, ITreeShardConsolidationGrain> Folds { get; init; }
        public required IShardHealingOrchestratorGrain Healer { get; init; }
        public required IGrainFactory Factory { get; init; }
    }

    private static ShrinkHarness CreateShrinkGrain(
        ShardMap map, int target, int maxConcurrentMigrations = 4, params int[] trackedDonors)
    {
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: map.Slots.Length,
            physicalShardCount: map.GetPhysicalShardIndices().Count,
            existingMap: map,
            maxConcurrentMigrations: maxConcurrentMigrations);
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.Shrinking = true;
        state.State.TargetShardCount = target;
        state.State.ConsolidationDonorShardIndices = [.. trackedDonors];

        var folds = new Dictionary<int, ITreeShardConsolidationGrain>();
        for (var i = 0; i < 64; i++)
        {
            var fold = Substitute.For<ITreeShardConsolidationGrain>();
            fold.IsIdleAsync().Returns(true);
            fold.GetProgressAsync().Returns(new ShardConsolidationProgress { InProgress = false });
            folds[i] = fold;
            grainFactory.GetGrain<ITreeShardConsolidationGrain>($"{TreeId}/{i}").Returns(fold);
        }

        var healer = Substitute.For<IShardHealingOrchestratorGrain>();
        healer.GetInFlightDonorShardIndicesAsync().Returns(Array.Empty<int>());
        grainFactory.GetGrain<IShardHealingOrchestratorGrain>(TreeId).Returns(healer);

        return new ShrinkHarness
        {
            Grain = grain,
            State = state,
            Folds = folds,
            Healer = healer,
            Factory = grainFactory,
        };
    }

    private static int StartCalls(ITreeShardConsolidationGrain fold)
        => fold.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ITreeShardConsolidationGrain.StartAsync));

    [Test]
    public async Task A_shrink_starts_folds_on_the_cheapest_adjacent_pairs_up_to_the_shards_still_needed()
    {
        // Four equal shards, target two: two folds are needed. The cheapest pair
        // is (0,1) - ties retire the higher index - and once shard 1 is projected
        // onto 0, the cheapest remaining pair is (2,3).
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2);

        await h.Grain.MigrateAsync();

        await h.Folds[1].Received(1).StartAsync(0);
        await h.Folds[3].Received(1).StartAsync(2);
        Assert.That(StartCalls(h.Folds[0]) + StartCalls(h.Folds[2]), Is.Zero,
            "Survivors must never be started as donors in the same tick.");
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.EquivalentTo(new[] { 1, 3 }));
        Assert.That(h.State.State.Phase, Is.EqualTo(ReshardPhase.Migrating));
    }

    [TestCase("snapshot")]
    [TestCase("merge")]
    public async Task A_shrink_starts_no_fold_while_a_snapshot_or_merge_reads_the_tree(string running)
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2);
        var lattice = h.Factory.GetGrain<ILattice>(TreeId);
        if (running == "snapshot") lattice.IsSnapshotCompleteAsync().Returns(false);
        else lattice.IsMergeCompleteAsync().Returns(false);

        await h.Grain.MigrateAsync();

        Assert.That(h.Folds.Values.Sum(StartCalls), Is.Zero);
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.Empty);
    }

    [Test]
    public async Task An_empty_tree_reshard_revives_retired_shards_before_publishing_the_identity_map()
    {
        // The identity map routes to indices 0..n-1, which may be shards an
        // earlier shrink retired; unrevived, they would refuse every operation.
        var (grain, _, grainFactory, registry) = CreateGrain(virtualShardCount: 16, physicalShardCount: 4);
        var shard = Substitute.For<IShardRootGrain>();
        shard.AnyBoundedAsync(Arg.Any<string?>()).Returns(Task.FromResult(new ShardAnyPage { Found = false }));
        var order = new List<string>();
        shard.ReviveAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(_ => { order.Add("revive"); return Task.CompletedTask; });
        registry.SetShardMapAsync(TreeId, Arg.Any<ShardMap>())
            .Returns(_ => { order.Add("map"); return Task.CompletedTask; });
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);

        await grain.ReshardAsync(8);

        await shard.Received(8).ReviveAsync(Arg.Is<int[]>(s => s.Length == 2), 16);
        Assert.That(order.LastIndexOf("revive"), Is.LessThan(order.IndexOf("map")),
            "every shard the new map routes to must be in service before the map is published");
    }

    [Test]
    public async Task A_shrink_never_dispatches_splits()
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2);
        var split = Substitute.For<ITreeShardSplitGrain>();
        h.Factory.GetGrain<ITreeShardSplitGrain>(Arg.Any<string>()).Returns(split);

        await h.Grain.MigrateAsync();

        await split.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task A_shrink_starts_no_more_folds_than_MaxConcurrentMigrations_allows()
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 8), target: 2, maxConcurrentMigrations: 1);

        await h.Grain.MigrateAsync();

        var started = h.Folds.Values.Sum(StartCalls);
        Assert.That(started, Is.EqualTo(1));
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_shrink_counts_its_running_folds_against_the_concurrency_budget()
    {
        // Shard 3's fold is still running (pre-swap: the map still references
        // it) and the budget is one, so nothing new may start.
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2, maxConcurrentMigrations: 1, 3);
        h.Folds[3].GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = true, DonorShardIndex = 3, SurvivorShardIndex = 2,
        });

        await h.Grain.MigrateAsync();

        Assert.That(h.Folds.Values.Sum(StartCalls), Is.Zero);
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public async Task A_shrink_does_not_complete_while_a_fold_it_started_is_still_running()
    {
        // The map already holds the target - the fold swapped - but it is still
        // releasing the donor's storage, so the shrink must stay open.
        var map = ShardMap.CreateDefault(16, 2);
        var h = CreateShrinkGrain(map, target: 2, maxConcurrentMigrations: 4, 3);
        h.Folds[3].GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = true, DonorShardIndex = 3, SurvivorShardIndex = 1,
            Phase = ShardConsolidationPhase.Complete,
        });

        await h.Grain.MigrateAsync();

        Assert.That(h.State.State.Phase, Is.EqualTo(ReshardPhase.Migrating));
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.EqualTo(new[] { 3 }));
    }

    [Test]
    public async Task A_shrink_completes_once_the_map_holds_the_target_and_every_fold_has_finished()
    {
        var map = ShardMap.CreateDefault(16, 2);
        var h = CreateShrinkGrain(map, target: 2, maxConcurrentMigrations: 4, 3);
        h.Folds[3].GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = false, Complete = true, DonorShardIndex = 3, SurvivorShardIndex = 1,
        });

        await h.Grain.MigrateAsync();

        Assert.That(h.State.State.Phase, Is.EqualTo(ReshardPhase.Complete));
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.Empty);
    }

    [Test]
    public async Task A_shrink_waits_for_folds_automatic_healing_admitted_before_it_began()
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2);
        h.Healer.GetInFlightDonorShardIndicesAsync().Returns(new[] { 7 });
        h.Folds[7].IsIdleAsync().Returns(false);

        await h.Grain.MigrateAsync();

        Assert.That(h.Folds.Values.Sum(StartCalls), Is.Zero);
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.Empty);
    }

    [Test]
    public async Task A_shrink_ignores_a_healing_record_whose_fold_has_already_finished()
    {
        // The orchestrator's record can over-count; an idle coordinator is not
        // a fold in flight.
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 3);
        h.Healer.GetInFlightDonorShardIndicesAsync().Returns(new[] { 7 });

        await h.Grain.MigrateAsync();

        Assert.That(h.Folds.Values.Sum(StartCalls), Is.EqualTo(1));
    }

    [Test]
    public async Task A_refused_fold_start_is_untracked_so_the_next_tick_can_re_plan()
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 3);
        h.Folds[1].StartAsync(Arg.Any<int>()).ThrowsAsync(new InvalidOperationException("split in flight"));

        await h.Grain.MigrateAsync();

        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.Empty);
        Assert.That(h.State.State.Phase, Is.EqualTo(ReshardPhase.Migrating));
    }

    [Test]
    public async Task A_shrink_plans_nothing_while_a_tracked_fold_cannot_be_read()
    {
        var h = CreateShrinkGrain(ShardMap.CreateDefault(16, 4), target: 2, maxConcurrentMigrations: 4, 3);
        h.Folds[3].GetProgressAsync().ThrowsAsync(new TimeoutException());

        await h.Grain.MigrateAsync();

        Assert.That(h.Folds.Values.Sum(StartCalls), Is.Zero,
            "An unreadable fold's survivor is unknown, so no pair can be proven safe to fold.");
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.EqualTo(new[] { 3 }),
            "An unreachable fold is not evidence that it finished.");
    }

    [Test]
    public async Task A_post_swap_fold_does_not_hide_a_reduction_still_needed()
    {
        // Shard 3 already swapped out (the map routes nothing to it) but its
        // fold is still finishing; the map still holds three shards against a
        // target of two, so one more fold must start now. Shard 2 is its
        // survivor and heavy, so the cheapest pair, (0,1), avoids it.
        var slots = new int[16];
        slots[0] = 0; slots[1] = 0; slots[2] = 1; slots[3] = 1;
        for (var i = 4; i < 16; i++) slots[i] = 2;
        var h = CreateShrinkGrain(new ShardMap { Slots = slots, Version = 1 }, target: 2, maxConcurrentMigrations: 4, 3);
        h.Folds[3].GetProgressAsync().Returns(new ShardConsolidationProgress
        {
            InProgress = true, DonorShardIndex = 3, SurvivorShardIndex = 2,
            Phase = ShardConsolidationPhase.Complete,
        });

        await h.Grain.MigrateAsync();

        await h.Folds[1].Received(1).StartAsync(0);
        Assert.That(h.Folds.Values.Sum(StartCalls), Is.EqualTo(1));
        Assert.That(h.State.State.ConsolidationDonorShardIndices, Is.EquivalentTo(new[] { 3, 1 }));
    }
}
