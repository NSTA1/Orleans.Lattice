using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Concurrency tests for the registry's shard-map reassignment path.
/// <para>
/// Both topology coordinators - <c>TreeShardSplitGrain.SwapAsync</c> and
/// <c>TreeShardConsolidationGrain.SwapAsync</c> - need to apply their own slot
/// diff onto the live map while the other may be doing the same. Each holds an
/// <c>OriginalShardMap</c> captured when its saga started, which is stale the
/// moment the other coordinator commits.
/// </para>
/// <para>
/// The composition is only safe when the read of the live map and the persist
/// of the reassigned copy happen inside a single grain call. Non-reentrancy
/// makes each individual call atomic; it does not make a sequence of two calls
/// atomic, because the grain is free to serve the other coordinator in the gap
/// between them.
/// </para>
/// </summary>
public class LatticeRegistryGrainShardMapConcurrencyTests
{
    private const string TreeId = "shard-map-concurrency";

    /// <summary>
    /// Builds the registry grain over a real dictionary-backed store, so a
    /// persisted entry is observable by a later read. The bytes are whatever
    /// the grain itself serialises, so the round trip needs no knowledge of the
    /// entry format.
    /// </summary>
    private static LatticeRegistryGrain CreateGrainOverBackingStore()
    {
        var store = new Dictionary<string, byte[]>(StringComparer.Ordinal);

        var registryTree = Substitute.For<ISystemLattice>();
        registryTree.GetAsync(Arg.Any<string>())
            .Returns(ci => Task.FromResult(
                store.TryGetValue(ci.ArgAt<string>(0), out var bytes) ? bytes : null));
        registryTree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(ci =>
            {
                store[ci.ArgAt<string>(0)] = ci.ArgAt<byte[]>(1);
                return Task.CompletedTask;
            });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId).Returns(registryTree);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        return new LatticeRegistryGrain(grainFactory, optionsMonitor);
    }

    /// <summary>
    /// A split and a fold each apply their own slot diff while holding a view
    /// of the map captured before the other committed. Neither reassignment may
    /// be erased.
    /// <para>
    /// This is not a hypothetical interleaving: the chaos fixture
    /// <c>ShardConsolidationChaosTests</c> drives a splitter and a folder
    /// against one tree concurrently by design, which is the pair modelled
    /// here. A reassignment erased this way leaves the slot routing to the
    /// shard the other coordinator has already drained, so every key in it
    /// becomes unreachable and any acknowledged write on it is lost.
    /// </para>
    /// </summary>
    [Test]
    public async Task Split_then_fold_each_holding_a_stale_view_both_survive()
    {
        var grain = CreateGrainOverBackingStore();
        var atSagaStart = new ShardMap { Slots = [0, 0, 0, 0] };
        await grain.SetShardMapAsync(TreeId, new ShardMap { Slots = [0, 0, 0, 0] });

        // The split coordinator commits first: slot 1 moves to new shard 7.
        await grain.ReassignSlotsAsync(TreeId, [1], 7, atSagaStart);

        // The fold coordinator then commits its own diff. Its fallback view is
        // the map as it stood when its saga started, which no longer reflects
        // the split above.
        await grain.ReassignSlotsAsync(TreeId, [2], 5, atSagaStart);

        var final = await grain.GetShardMapAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(final!.Slots[2], Is.EqualTo(5),
                "the fold's own slot diff must be applied.");
            Assert.That(final.Slots[1], Is.EqualTo(7),
                "the split's earlier reassignment of slot 1 to shard 7 must survive the "
                + "fold's later persist.");
        });
    }

    /// <summary>
    /// The same composition in the other direction, so neither coordinator is
    /// privileged by the ordering. Enumerated rather than assumed symmetric.
    /// </summary>
    [Test]
    public async Task Fold_then_split_each_holding_a_stale_view_both_survive()
    {
        var grain = CreateGrainOverBackingStore();
        var atSagaStart = new ShardMap { Slots = [0, 0, 0, 0] };
        await grain.SetShardMapAsync(TreeId, new ShardMap { Slots = [0, 0, 0, 0] });

        await grain.ReassignSlotsAsync(TreeId, [2], 5, atSagaStart);
        await grain.ReassignSlotsAsync(TreeId, [1], 7, atSagaStart);

        var final = await grain.GetShardMapAsync(TreeId);

        Assert.Multiple(() =>
        {
            Assert.That(final!.Slots[1], Is.EqualTo(7),
                "the split's own slot diff must be applied.");
            Assert.That(final.Slots[2], Is.EqualTo(5),
                "the fold's earlier reassignment of slot 2 to survivor 5 must survive the "
                + "split's later persist.");
        });
    }

    /// <summary>
    /// Many coordinators composing in sequence, each carrying the same stale
    /// saga-start view. Every reassignment must be present at the end.
    /// </summary>
    [Test]
    public async Task Every_reassignment_in_a_sequence_of_stale_view_coordinators_survives()
    {
        var grain = CreateGrainOverBackingStore();
        var atSagaStart = ShardMap.CreateDefault(8, 1);
        await grain.SetShardMapAsync(TreeId, ShardMap.CreateDefault(8, 1));

        for (var slot = 0; slot < 8; slot++)
            await grain.ReassignSlotsAsync(TreeId, [slot], slot + 10, atSagaStart);

        var final = await grain.GetShardMapAsync(TreeId);

        Assert.Multiple(() =>
        {
            for (var slot = 0; slot < 8; slot++)
            {
                Assert.That(final!.Slots[slot], Is.EqualTo(slot + 10),
                    $"slot {slot} must retain its reassignment.");
            }
        });
    }

    /// <summary>
    /// The fallback map is consulted only when the tree has no persisted map,
    /// which is the case the callers' <c>OriginalShardMap</c> fallback covers.
    /// </summary>
    [Test]
    public async Task Fallback_map_is_used_when_no_map_is_persisted_yet()
    {
        var grain = CreateGrainOverBackingStore();

        var result = await grain.ReassignSlotsAsync(TreeId, [3], 4, ShardMap.CreateDefault(8, 2));

        Assert.Multiple(() =>
        {
            Assert.That(result.Slots[3], Is.EqualTo(4));
            Assert.That(result.Slots, Has.Length.EqualTo(8),
                "the fallback map's shape must be adopted when nothing is persisted.");
        });
    }

    /// <summary>
    /// The persisted map wins over the fallback whenever one exists, which is
    /// the property that makes concurrent coordinators compose.
    /// </summary>
    [Test]
    public async Task Persisted_map_takes_precedence_over_the_fallback()
    {
        var grain = CreateGrainOverBackingStore();
        await grain.SetShardMapAsync(TreeId, new ShardMap { Slots = [9, 9, 9, 9] });

        var result = await grain.ReassignSlotsAsync(
            TreeId, [0], 1, new ShardMap { Slots = [0, 0, 0, 0] });

        Assert.Multiple(() =>
        {
            Assert.That(result.Slots[0], Is.EqualTo(1), "the caller's own diff must be applied.");
            Assert.That(result.Slots[3], Is.EqualTo(9),
                "untouched slots must come from the persisted map, not the stale fallback.");
        });
    }

    /// <summary>
    /// Version must advance on a reassignment, because strongly-consistent
    /// scans detect topology changes by comparing it.
    /// </summary>
    [Test]
    public async Task Reassignment_increments_the_map_version()
    {
        var grain = CreateGrainOverBackingStore();
        await grain.SetShardMapAsync(TreeId, ShardMap.CreateDefault(8, 2));
        var before = (await grain.GetShardMapAsync(TreeId))!.Version;

        var result = await grain.ReassignSlotsAsync(TreeId, [1], 3, ShardMap.CreateDefault(8, 2));

        Assert.That(result.Version, Is.EqualTo(before + 1));
    }

    /// <summary>
    /// An empty diff still composes: it must not disturb any other
    /// coordinator's reassignment.
    /// </summary>
    [Test]
    public async Task Empty_slot_set_leaves_every_assignment_intact()
    {
        var grain = CreateGrainOverBackingStore();
        await grain.SetShardMapAsync(TreeId, new ShardMap { Slots = [0, 7, 0, 5] });

        var result = await grain.ReassignSlotsAsync(
            TreeId, [], 2, new ShardMap { Slots = [0, 0, 0, 0] });

        Assert.That(result.Slots, Is.EqualTo(new[] { 0, 7, 0, 5 }));
    }

    /// <summary>
    /// A slot outside the map is a programming error and must be rejected
    /// rather than silently widening or corrupting the map.
    /// </summary>
    [Test]
    public void Reassignment_rejects_a_slot_outside_the_map()
    {
        var grain = CreateGrainOverBackingStore();

        Assert.That(
            async () => await grain.ReassignSlotsAsync(TreeId, [99], 1, ShardMap.CreateDefault(8, 2)),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Reassignment_throws_when_tree_id_null()
    {
        var grain = CreateGrainOverBackingStore();

        Assert.That(
            async () => await grain.ReassignSlotsAsync(null!, [0], 1, ShardMap.CreateDefault(8, 2)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Reassignment_throws_when_slots_null()
    {
        var grain = CreateGrainOverBackingStore();

        Assert.That(
            async () => await grain.ReassignSlotsAsync(TreeId, null!, 1, ShardMap.CreateDefault(8, 2)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Reassignment_throws_when_fallback_map_null()
    {
        var grain = CreateGrainOverBackingStore();

        Assert.That(
            async () => await grain.ReassignSlotsAsync(TreeId, [0], 1, null!),
            Throws.ArgumentNullException);
    }
}
