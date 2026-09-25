using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Leaf-side coverage for the replay filter push-down (issue #3565): a leaf
/// hands its ownership to the replay coordinator with every slice, so storage
/// can drop the partition's records that belong to sibling leaves before their
/// payloads are decoded.
/// <para>
/// The coordinators here are served by <see cref="ReplaySliceStub"/>, whose
/// filtered overload is an independent reference implementation of the rule, so
/// these tests prove the leaf's side of the contract - what it pushes down, and
/// that it converges on a filtered window - without trusting the production
/// rule they rely on.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static LatticeMutation PushdownSet(string key) => new()
    {
        TreeId = MaterialiserTreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = Encoding.UTF8.GetBytes("v-" + key),
        Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
    };

    [Test]
    public async Task Replay_pushes_a_bounded_leafs_key_range_down_with_every_slice()
    {
        var coord = BuildCoordinator(
            head: 4,
            new CommitLogSliceEntry(1, PushdownSet("a1")),
            new CommitLogSliceEntry(2, PushdownSet("m2")),
            new CommitLogSliceEntry(3, PushdownSet("z3")));
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord, seedState: OwnsOnlyTheMRange());

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest.Keys, Is.EqualTo(new[] { "m2" }));
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3));
        });
        await coord.Received().ReadSliceAsync(
            Arg.Any<long>(),
            Arg.Any<long>(),
            Arg.Any<int>(),
            Arg.Is<WalKeyFilter>(f => f.LowKeyInclusive == "m" && f.HighKeyExclusive == "n" && !f.HasShardConstraint),
            Arg.Any<CancellationToken>());
        await coord.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Replay_of_a_leaf_that_owns_every_key_reads_unfiltered_slices()
    {
        // A leaf with no bounds and no shard map has nothing to push down, so it
        // must keep the unfiltered read exactly as before.
        var coord = BuildCoordinator(head: 2, new CommitLogSliceEntry(1, PushdownSet("a1")));
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(grain.EntriesForTest.ContainsKey("a1"), Is.True);
        await coord.Received().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        await coord.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Replay_pushes_the_shard_axis_down_when_the_registry_publishes_a_map()
    {
        // The shard axis is the larger saving: a partition interleaves every
        // shard's writes, and a leaf's own shard is a small fraction of them.
        const int leafShard = 1;
        var map = ShardMap.CreateDefault(64, 4);
        var keys = Enumerable.Range(0, 40).Select(i => $"k{i:D2}").ToArray();
        var entries = keys.Select((key, i) => new CommitLogSliceEntry(i + 1, PushdownSet(key))).ToArray();
        var coord = BuildCoordinator(head: entries.Length + 1, entries);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s => s.ShardIndex = leafShard,
            registryShardMap: map);

        await ActivateAsync(grain);

        var owned = keys.Where(k => map.Resolve(k) == leafShard).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(owned, Is.Not.Empty.And.Length.LessThan(keys.Length),
                "The fixture must mix owned and foreign keys, or it proves nothing about the filter.");
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(owned),
                "Exactly the keys the map routes to this leaf's shard are applied.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(entries.Length),
                "The checkpoint still reaches the last record, dropped or not.");
        });
        await coord.Received().ReadSliceAsync(
            Arg.Any<long>(),
            Arg.Any<long>(),
            Arg.Any<int>(),
            Arg.Is<WalKeyFilter>(f => f.HasShardConstraint && f.VirtualShardCount == 64),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Replay_checkpoint_reaches_the_head_across_slices_that_hold_only_other_leaves_records()
    {
        // More foreign records than one slice examines: every slice the
        // coordinator returns is just its trailing routing-only record. The leaf
        // must still walk the whole gap and bank the head, or it would re-scan
        // the partition on every activation and pin the WAL GC floor (#2270).
        var entries = Enumerable.Range(1, 600)
            .Select(i => new CommitLogSliceEntry(i, PushdownSet($"a{i:D4}")))
            .ToArray();
        var coord = BuildCoordinator(head: 601, entries);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord, seedState: OwnsOnlyTheMRange());

        await ActivateAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(grain.EntriesForTest, Is.Empty);
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(600));
        });

        // Each slice resumes from the previous slice's routing-only record - the
        // last record it examined - which is the whole mechanism under test.
        // Pinned by start offset rather than by a total count, because the loop
        // also issues one trailing empty read at the head (#3489).
        foreach (var resumedFrom in new long[] { 256, 512, 600 })
        {
            await coord.Received(1).ReadSliceAsync(
                resumedFrom, Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>());
        }
    }

    [Test]
    public async Task FoldTail_pushes_the_leafs_ownership_down_with_every_slice()
    {
        var coord = BuildCoordinator(
            head: 3,
            new CommitLogSliceEntry(0, FrozenCommittedSet("b-owned", Encoding.UTF8.GetBytes("mine"))),
            new CommitLogSliceEntry(1, FrozenCommittedSet("z-foreign", Encoding.UTF8.GetBytes("theirs"))),
            new CommitLogSliceEntry(2, FrozenCommittedSet("c-owned", Encoding.UTF8.GetBytes("mine"))));
        var (grain, _) = CreateFrozenBaselineGrain(
            explicitCoordinator: coord,
            seedState: s =>
            {
                s.LowKeyInclusive = "b";
                s.HighKeyExclusive = "d";
            });

        var rows = await grain.FoldTailOntoFrozenAsync(Freeze(), capturedHead: [3], CancellationToken.None);

        Assert.That(rows.Select(r => r.Key), Is.EqualTo(new[] { "b-owned", "c-owned" }));
        await coord.Received().ReadSliceAsync(
            Arg.Any<long>(),
            Arg.Any<long>(),
            Arg.Any<int>(),
            Arg.Is<WalKeyFilter>(f => f.LowKeyInclusive == "b" && f.HighKeyExclusive == "d"),
            Arg.Any<CancellationToken>());
        await coord.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// A warm activation that replays its window undisturbed proves its cache
    /// covers it, and a later warm stale-leaf rescue rests on that proof. An
    /// unfiltered replay proves it by offset density, but a bounded leaf's
    /// filtered slices skip other owners' records by design - this leaf owns
    /// only <c>k1</c>, so it is served offsets 1 and 3 and never 2 - and density
    /// would fail every filtered replay. The retained floor proves it instead.
    /// </summary>
    [Test]
    public async Task Filtered_warm_replay_still_proves_its_cache_by_the_retained_floor()
    {
        var (grain, wal) = await ActivateWarmBoundedLeafAsync(oldestReadableOffset: 1);

        Assert.Multiple(() =>
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3));
            Assert.That(WarmRescueField("_warmCacheReplayFailed").GetValue(grain), Is.False);
            Assert.That(WarmRescueField("_warmCacheProvenOffsets").GetValue(grain), Is.EqualTo(new[] { 3L }));
        });
        await wal.Coordinator.Received().ReadSliceAsync(
            Arg.Any<long>(),
            Arg.Any<long>(),
            Arg.Any<int>(),
            Arg.Is<WalKeyFilter>(f => f.HighKeyExclusive == "k2"),
            Arg.Any<CancellationToken>());
        await wal.Coordinator.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// The failing arm of that proof: once the replay has read its window, the
    /// oldest readable offset sits past the first offset the window needed, so a
    /// trim may have removed records the leaf never saw, and the cache must not
    /// be certified.
    /// </summary>
    [Test]
    public async Task Filtered_warm_replay_is_not_proven_when_a_trim_overtook_its_window()
    {
        var (grain, _) = await ActivateWarmBoundedLeafAsync(oldestReadableOffset: 3);

        Assert.Multiple(() =>
        {
            Assert.That(WarmRescueField("_warmCacheReplayFailed").GetValue(grain), Is.True);
            Assert.That(WarmRescueField("_warmCacheProvenOffsets").GetValue(grain), Is.Null);
        });
    }

    /// <summary>
    /// Activates a warm leaf owning <c>[null, k2)</c> over a partition holding
    /// <c>k1</c>, <c>k2</c> and <c>k3</c> at offsets 1 to 3, with the partition's
    /// oldest readable offset reported as <paramref name="oldestReadableOffset"/>.
    /// </summary>
    private async Task<(BPlusLeafGrain Grain, GrowingWal Wal)> ActivateWarmBoundedLeafAsync(long oldestReadableOffset)
    {
        var wal = new GrowingWal();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.TailReplay));
        var (grain, state, _, _) = CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector);
        state.State.HighKeyExclusive = "k2";
        wal.GrowTo(3);
        wal.Coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(oldestReadableOffset));

        await ActivateAsync(grain);
        return (grain, wal);
    }
}
