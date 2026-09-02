using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Contention coverage for a fold whose donor an adaptive split tries to take
/// at the same time.
/// <para>
/// A shard root carries exactly one migration record
/// (<c>ShardRootState.SplitInProgress</c>), and both an adaptive split and an
/// online consolidation open their shadow-write window through the same
/// <see cref="IShardRootGrain.BeginSplitAsync"/> primitive. The two therefore
/// contend for a single slot, and the loser of that contention silently loses
/// acknowledged writes rather than failing loudly: the re-aimed record stops
/// shadow-forwarding the fold's slots to the survivor, the fold's freeze then
/// fences the split's slots instead of its own, and
/// <see cref="IShardRootGrain.CompleteSplitAsync"/> promotes the wrong slot set
/// into the permanent moved-away seal - leaving the retired donor accepting and
/// serving writes on slots the routing map has already handed to the survivor.
/// </para>
/// <para>
/// These are the deterministic regression tests for that defect (issue #1885).
/// They drive the interleaving directly instead of racing for it, so neither
/// depends on winning a chaos schedule.
/// </para>
/// </summary>
public partial class ShardConsolidationIntegrationTests
{
    /// <summary>
    /// Finds a key that hashes into <paramref name="slot"/>, so a test can
    /// probe a specific virtual slot's gate without depending on the corpus
    /// happening to contain one.
    /// </summary>
    private static string KeyForSlot(int slot, int virtualShardCount)
    {
        for (var i = 0; i < 500_000; i++)
        {
            var candidate = $"slot-probe-{i}";
            if (ShardMap.GetVirtualSlot(candidate, virtualShardCount) == slot) return candidate;
        }

        throw new InvalidOperationException(
            $"No probe key hashes to virtual slot {slot} of {virtualShardCount}.");
    }

    [Test]
    public async Task SplitAsync_when_the_source_shard_is_the_donor_of_an_in_flight_fold_is_refused()
    {
        var treeId = $"cons-split-refuse-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await PopulateAsync(tree, "k", 200);

        var map = await GetMapAsync(treeId);
        Assert.That(ShardConsolidationPlanner.TryPlanNext(map, out var plan), Is.True,
            "The four-shard fixture must offer an adjacent pair to fold.");

        var consolidator = Consolidator(treeId, plan.DonorShardIndex);
        await consolidator.StartAsync(plan.SurvivorShardIndex);

        var split = _cluster.GrainFactory
            .GetGrain<ITreeShardSplitGrain>($"{treeId}/{plan.DonorShardIndex}");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await split.SplitAsync(plan.DonorShardIndex),
            "A split must not be admitted on a shard that is already the donor of an in-flight fold: "
            + "the two share one migration record, so admitting the split re-aims the fold's shadow-write window.");

        // The refusal must leave the fold able to finish. A split that unwound
        // cleanly leaves no half-committed intent behind, so the donor is
        // splittable again once the fold retires it.
        for (var i = 0; i < 64 && !await consolidator.IsIdleAsync(); i++)
            await consolidator.RunConsolidationPassAsync();
        Assert.That(await consolidator.IsIdleAsync(), Is.True,
            "The fold must still reach a terminal state after refusing the contending split.");
    }

    [Test]
    public async Task Consolidation_when_a_split_contends_for_the_donor_still_seals_every_folded_slot()
    {
        var treeId = $"cons-split-seal-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var physicalTreeId = await _cluster.GrainFactory
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeId);
        await PopulateAsync(tree, "k", 400);

        var map = await GetMapAsync(treeId);
        Assert.That(ShardConsolidationPlanner.TryPlanNext(map, out var plan), Is.True,
            "The four-shard fixture must offer an adjacent pair to fold.");

        var donorIndex = plan.DonorShardIndex;
        var foldedSlots = plan.DonorSlots;
        var virtualShardCount = map.Slots.Length;

        var consolidator = Consolidator(treeId, donorIndex);
        await consolidator.StartAsync(plan.SurvivorShardIndex);

        // The interleaving under test: an adaptive split coordinator reaches
        // for the very shard the fold is draining.
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{donorIndex}");
        try
        {
            await split.SplitAsync(donorIndex);
        }
        catch (InvalidOperationException)
        {
            // The expected refusal. Swallowed rather than asserted here so
            // this test fails on the write-loss invariant below rather than on
            // the mechanism that happens to enforce it.
        }

        for (var i = 0; i < 128 && !await consolidator.IsIdleAsync(); i++)
            await consolidator.RunConsolidationPassAsync();
        Assert.That(await consolidator.IsIdleAsync(), Is.True, "The fold must reach a terminal state.");

        // The invariant. Every slot the fold moved must now be fenced on the
        // retired donor. The seal is what makes a stale-routed operation
        // self-heal onto the survivor; an unsealed folded slot means the donor
        // silently accepts and serves writes for a slot the routing map has
        // given away, and every such write is acknowledged and then
        // unreachable.
        var donor = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{donorIndex}");
        var unsealed = new List<int>();
        foreach (var slot in foldedSlots)
        {
            try
            {
                await donor.GetRawEntryAsync(KeyForSlot(slot, virtualShardCount));
                unsealed.Add(slot);
            }
            catch (StaleShardRoutingException)
            {
            }
        }

        Assert.That(unsealed, Is.Empty,
            $"{unsealed.Count} of {foldedSlots.Length} folded virtual slot(s) were left unsealed on retired donor "
            + $"shard {donorIndex}, so it still serves and accepts writes for slots the routing map has handed to "
            + $"shard {plan.SurvivorShardIndex}. Unsealed sample: [{string.Join(", ", unsealed.Take(16))}].");
    }

    [Test]
    public async Task BeginSplitAsync_when_a_differently_aimed_migration_is_in_flight_refuses_instead_of_re_aiming()
    {
        var treeId = $"cons-reaim-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        var physicalTreeId = await _cluster.GrainFactory
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeId);
        await PopulateAsync(tree, "k", 100);

        var map = await GetMapAsync(treeId);
        var shards = map.GetPhysicalShardIndices();
        Assert.That(shards.Count, Is.GreaterThanOrEqualTo(3),
            "This test needs three distinct physical shards to aim a record two different ways.");

        var subject = shards[0];
        var firstTarget = shards[1];
        var secondTarget = shards[2];
        var virtualShardCount = map.Slots.Length;
        var ownedSlots = Enumerable.Range(0, virtualShardCount)
            .Where(s => map.Slots[s] == subject).ToArray();
        Assert.That(ownedSlots, Is.Not.Empty);

        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{subject}");
        await shard.BeginSplitAsync(firstTarget, ownedSlots, virtualShardCount);

        // Re-asserting the same aim is the crash-recovery path and stays a
        // no-op, so the refusal below is about re-aiming rather than about
        // any second call.
        Assert.DoesNotThrowAsync(
            async () => await shard.BeginSplitAsync(firstTarget, ownedSlots, virtualShardCount),
            "Re-asserting an identical migration window is the idempotent crash-recovery path.");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await shard.BeginSplitAsync(
                secondTarget, ownedSlots.Take(ownedSlots.Length / 2).ToArray(), virtualShardCount),
            "A shard holds one migration record; re-aiming it at a second target strands the first migration's slots.");

        // The original aim must survive the refusal: the shard must still fence
        // its in-flight migration's slots towards the original target, rather
        // than towards the one it just refused.
        await shard.EnterRejectPhaseAsync();
        var probeKey = KeyForSlot(ownedSlots[0], virtualShardCount);
        var ex = Assert.ThrowsAsync<StaleShardRoutingException>(
            async () => await shard.GetRawEntryAsync(probeKey),
            "The in-flight migration's slots must still be fenced by its own record.");
        Assert.That(ex!.TargetShardIndex, Is.EqualTo(firstTarget),
            "The record must still point at the original migration's target, not the refused one.");
    }
}
