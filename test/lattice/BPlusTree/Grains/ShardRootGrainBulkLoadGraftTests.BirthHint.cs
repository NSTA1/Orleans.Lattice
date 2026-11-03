using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The bulk switch's birth hint and marker (epic #5605, member 3; design note section
/// 3). A bulk operation stores its hint and opens <c>bulk:{operationId}</c> on the
/// shard's marker before it creates its first leaf; once the switch has relocated
/// every row its leaves own below the hint, the hint becomes their read position and
/// the marker closes by fence with the hint removed. The hint is never applied when a
/// row could not be relocated (review N2 of #5646).
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    private const long BirthHead = 40;
    private const long CloseHead = 90;

    /// <summary>
    /// The hint a bulk operation stores from <see cref="BirthHead"/>: one below it, as a head
    /// is exclusive and a hint an inclusive checkpoint (issue #5760).
    /// </summary>
    private const long BirthHint = BirthHead - 1;

    private sealed record HintRig(Harness H, EnumerableMemoryGrainStorage Storage, IWalShardGrain Wal, ILeafReplayCoordinatorGrain Coordinator);

    /// <summary>
    /// A harness whose append-fence gate is open, whose marker row lives in memory, whose
    /// WAL head reads as <see cref="BirthHead"/>, and whose WAL partitions accept a routing
    /// generation claim with head <see cref="CloseHead"/>.
    /// </summary>
    private static HintRig CreateHintRig(int maxLeafKeys = 2, bool gateOpen = true, bool advancingHeads = false)
    {
        var gate = Substitute.For<IWalAppendFenceGate>();
        gate.IsOpen.Returns(gateOpen);
        var h = CreateHarness(maxLeafKeys, gate);
        var storage = new EnumerableMemoryGrainStorage();
        h.Grain.UseHandOffStorageForTesting(storage);

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        // Advancing heads model a log that keeps growing: each read is later than the last.
        var reads = 0L;
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(advancingHeads ? BirthHead + Interlocked.Increment(ref reads) : BirthHead));
        h.Factory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(coordinator);

        var wal = Substitute.For<IWalShardGrain>();
        wal.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(WalAppendFenceClaimResult.Accept(CloseHead)));
        wal.GetNextSequenceAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<long>(CloseHead));
        h.Factory.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(wal);
        return new HintRig(h, storage, wal, coordinator);
    }

    private static async Task<ShardHandOffState> ShardMarkerAsync(HintRig rig) =>
        (await HandOffMarkerRows.ReadShardAsync(rig.Storage, TreeId, 0)).State;

    private static long[]? HintAppliedTo(IBPlusLeafGrain leaf) =>
        leaf.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.SetCheckpointOffsetHintsAsync))
            .Select(c => (long[])c.GetArguments()[0]!)
            .LastOrDefault();

    private static GrainId SeedAppendTail(Harness h)
    {
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        h.Leaf(existing).GetAppendTailAsync().Returns((2, "a"));
        return existing;
    }

    [Test]
    public async Task A_bulk_append_stores_its_hint_and_opens_its_marker_before_its_first_appended_leaf_is_born()
    {
        var rig = CreateHintRig();
        SeedAppendTail(rig.H);

        // Read the marker row at the moment each appended leaf is born.
        var atBirth = new List<ShardHandOffState>();
        rig.H.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var key = call.ArgAt<Guid>(0);
            var fresh = !rig.H.Leaves.ContainsKey(key);
            var leaf = ResolveLeaf(rig.H.Leaves, key);
            if (fresh)
            {
                leaf.InitializeSiblingAsync(Arg.Any<SiblingInitialization>())
                    .Returns(async _ => atBirth.Add(await ShardMarkerAsync(rig)));
            }

            return leaf;
        });

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        Assert.That(atBirth, Is.Not.Empty, "precondition: the append created leaves");
        Assert.Multiple(() =>
        {
            Assert.That(atBirth.All(m => m.BulkBirthHints.TryGetValue("op-hint", out var hint) && hint.Length > 0 && hint.All(x => x == BirthHint)), Is.True,
                "the hint is durable before any appended leaf is born");
            Assert.That(atBirth.All(m => m.Open.TryGetValue("bulk:op-hint", out var range) && range.LowInclusive == "k0000" && range.HighExclusive is null), Is.True,
                "the marker is open over the appended range before any appended leaf is born");
        });
    }

    [Test]
    public async Task A_bulk_append_applies_its_hint_to_every_appended_leaf_and_the_runs_close_closes_its_marker_by_fence()
    {
        var rig = CreateHintRig();
        var existing = SeedAppendTail(rig.H);

        // How many leaves read from the hint when the fence's first re-claim is made.
        var hintedAtClose = -1;
        rig.Wal.ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (hintedAtClose < 0)
                    hintedAtClose = rig.H.Leaves.Values.Count(l => HintAppliedTo(l) is not null);
                return Task.FromResult(WalAppendFenceClaimResult.Accept(CloseHead));
            });

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        // Review C1 of #5702: the append leaves its switch for the run's fence.
        var beforeClose = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(beforeClose.Open, Does.ContainKey("bulk:op-hint"), "an append does not close its own switch");
            Assert.That(hintedAtClose, Is.EqualTo(-1), "no fence ran during the append");
        });

        await rig.H.Grain.CloseBulkSwitchesForTestingAsync();

        var appended = LeavesByBirthLow(rig.H);
        Assert.That(appended, Has.Count.EqualTo(3), "precondition: three appended leaves");
        foreach (var (low, leaf) in appended)
        {
            Assert.That(HintAppliedTo(leaf), Is.Not.Empty.And.All.EqualTo(BirthHint), $"the appended leaf at {low} reads from one below the head it captured (#5760)");
        }

        Assert.That(HintAppliedTo(rig.H.Leaf(existing)), Is.Null, "the old rightmost leaf keeps its read position");

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Does.Not.ContainKey("bulk:op-hint"));
            Assert.That(marker.Closes.Single(c => c.Token == "bulk:op-hint").Heads, Is.Not.Empty.And.All.EqualTo(CloseHead),
                "the close records the heads the routing-generation re-claim returned");
            Assert.That(marker.BulkBirthHints, Does.Not.ContainKey("op-hint"), "the hint is removed with the close");
            Assert.That(marker.RoutingGeneration, Is.GreaterThan(0), "the close raised the routing generation");
        });

        Assert.That(hintedAtClose, Is.EqualTo(appended.Count), "the hint is applied before the fence closes the switch");
    }

    [Test]
    public async Task The_hint_applied_is_the_one_stored_before_the_first_appended_leaf_was_born()
    {
        // A hint read at the switch's end would lie above the appended leaves' own
        // records, so a replay from it would skip them.
        var rig = CreateHintRig(advancingHeads: true);
        SeedAppendTail(rig.H);
        long[]? storedAtBirth = null;
        rig.H.Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(call =>
        {
            var key = call.ArgAt<Guid>(0);
            var fresh = !rig.H.Leaves.ContainsKey(key);
            var leaf = ResolveLeaf(rig.H.Leaves, key);
            if (fresh)
            {
                leaf.InitializeSiblingAsync(Arg.Any<SiblingInitialization>()).Returns(async _ =>
                    storedAtBirth ??= (await ShardMarkerAsync(rig)).BulkBirthHints["op-hint"]);
            }

            return leaf;
        });

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        Assert.That(storedAtBirth, Is.Not.Null, "precondition: the hint was stored before the first birth");
        foreach (var (low, leaf) in LeavesByBirthLow(rig.H))
        {
            Assert.That(HintAppliedTo(leaf), Is.EqualTo(storedAtBirth), $"the appended leaf at {low} reads from the hint stored before its birth");
        }
    }

    [Test]
    public async Task A_resumed_append_reuses_the_stored_hint_rather_than_capturing_a_later_one()
    {
        // A crash between the capture and the switch: the resume must start the leaves
        // from the hint the first attempt stored, not from a head read after it.
        var rig = CreateHintRig();
        SeedAppendTail(rig.H);
        await HandOffMarkerRows.UpdateShardAsync(rig.Storage, TreeId, 0, m => m.BulkBirthHints.TryAdd("op-hint", [7]));

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        foreach (var (low, leaf) in LeavesByBirthLow(rig.H))
        {
            Assert.That(HintAppliedTo(leaf), Is.EqualTo(new long[] { 7 }), $"the appended leaf at {low} reads from the stored hint");
        }
    }

    [Test]
    public async Task A_graft_whose_old_leaf_could_not_be_narrowed_does_not_apply_the_hint()
    {
        // Review N2 of #5646: an older silo hosts the old rightmost leaf, which may
        // already be narrowed with rows stranded on it whose records predate the hint.
        // The appended leaves must keep reading from the log's start, which reaches them.
        var rig = CreateHintRig();
        var existing = SeedAppendTail(rig.H);
        var unknown = new TypeLoadException(WalAppendFenceMethodProbe.UnresolvedAliasPrefix + " 'inv.lattice.leaf.narrow'");
        rig.H.Leaf(existing).NarrowHighKeyAsync(Arg.Any<string>(), Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>())
            .Returns(Task.FromException<LeafNarrowResult?>(unknown));

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));
        await rig.H.Grain.CloseBulkSwitchesForTestingAsync();

        foreach (var (low, leaf) in LeavesByBirthLow(rig.H))
        {
            Assert.That(HintAppliedTo(leaf), Is.Null, $"the appended leaf at {low} keeps reading from the log's start");
        }

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Does.Not.ContainKey("bulk:op-hint"), "the switch still closes");
            Assert.That(marker.BulkBirthHints, Is.Empty);
        });
    }

    [Test]
    public async Task A_graft_whose_old_leaf_could_not_drain_in_time_succeeds_links_without_narrowing_and_applies_no_hint()
    {
        // Review B1 of #5702: the old leaf's drain missed its deadline, so it narrowed
        // nothing and returned null. The chunk must still succeed, on the 10.2.0 path.
        var rig = CreateHintRig();
        var existing = SeedAppendTail(rig.H);
        rig.H.Leaf(existing).NarrowHighKeyAsync(Arg.Any<string>(), Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>())
            .Returns(Task.FromResult<LeafNarrowResult?>(null));

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));
        await rig.H.Grain.CloseBulkSwitchesForTestingAsync();

        var appended = LeavesByBirthLow(rig.H);
        Assert.That(appended, Is.Not.Empty, "precondition: the append created leaves");
        foreach (var (low, leaf) in appended)
        {
            Assert.That(HintAppliedTo(leaf), Is.Null, $"the appended leaf at {low} keeps reading from the log's start");
        }

        await rig.H.Leaf(existing).Received().SetNextSiblingAsync(Arg.Any<GrainId?>());
        await rig.H.Leaf(existing).DidNotReceive().DropRelocatedRowsAsync(Arg.Any<Dictionary<string, HybridLogicalClock>>());
        Assert.That(rig.H.State.State.PendingBulkGraft, Is.Null, "the graft completed");
    }

    [Test]
    public async Task A_resumed_graft_with_the_gate_closed_and_the_old_leaf_unnarrowed_does_not_apply_the_hint()
    {
        // Review N2 of #5646, the gate variant: the graft resumes with the gate closed
        // and the old leaf still declares the appended range, so its rows were never
        // relocated.
        var rig = CreateHintRig(gateOpen: false);
        var existing = SeedPendingGraft(rig.H, "op-hint", rootWasLeaf: false);
        await HandOffMarkerRows.UpdateShardAsync(rig.Storage, TreeId, 0, m =>
            m.BulkBirthHints.TryAdd("op-hint", [BirthHead]) | HandOffMarkerCore.Open(m, "bulk:op-hint", "k", null));
        rig.H.Leaf(existing).GetKeyRangeAndNextSiblingAsync().Returns(Task.FromResult(new LeafChainLink()));

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(1));
        await rig.H.Grain.CloseBulkSwitchesForTestingAsync();

        foreach (var entry in rig.H.State.State.PendingBulkGraft?.NewLeaves ?? [])
        {
            Assert.That(HintAppliedTo(rig.H.Leaf(entry.LeafId)), Is.Null);
        }

        foreach (var leaf in rig.H.Leaves.Values)
        {
            Assert.That(HintAppliedTo(leaf), Is.Null, "no leaf reads from the hint");
        }

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Closes.Single(c => c.Token == "bulk:op-hint").Heads, Is.Not.Empty.And.All.EqualTo(CloseHead),
                "with the gate closed the switch closes on the allocator heads");
            Assert.That(marker.BulkBirthHints, Is.Empty);
        });
        await rig.Wal.DidNotReceive().ClaimRoutingGenerationAsync(Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task With_the_gate_closed_a_bulk_append_stores_no_hint_and_opens_no_marker()
    {
        var rig = CreateHintRig(gateOpen: false);
        SeedAppendTail(rig.H);
        rig.H.Leaves.Values.Single().CountAsync().Returns(2);

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        var marker = await ShardMarkerAsync(rig);
        Assert.That(marker.Sequence, Is.Zero, "no marker is opened or closed");
        Assert.That(marker.BulkBirthHints, Is.Empty, "no hint is stored");
        foreach (var leaf in rig.H.Leaves.Values)
        {
            Assert.That(HintAppliedTo(leaf), Is.Null);
        }
    }

    [Test]
    public async Task A_bulk_load_applies_its_hint_to_every_leaf_after_draining_the_replaced_root()
    {
        var rig = CreateHintRig();
        var seeded = rig.H.AddLeaf();
        var seededLeaf = rig.H.Leaf(seeded);
        rig.H.State.State.RootNodeId = seeded;
        rig.H.State.State.RootIsLeaf = true;
        seededLeaf.GetStatsAsync().Returns(
            Task.FromResult(new LeafStats()),
            Task.FromResult(new LeafStats { LiveKeys = 1 }));
        seededLeaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(Task.FromResult(new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["k0001"] = LwwValue<byte[]>.Create([9], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
            },
            Version = new VersionVector(),
        }));

        await rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));

        var loaded = rig.H.Leaves.Values.Where(l => l != seededLeaf && HintAppliedTo(l) is not null).ToList();
        Assert.That(loaded, Has.Count.EqualTo(3), "every loaded leaf reads from the hint");
        Assert.That(loaded.SelectMany(l => HintAppliedTo(l)!), Is.All.EqualTo(BirthHint));

        // The replaced root's rows are merged into the loaded tree before any loaded
        // leaf's read position moves past them.
        Received.InOrder(() =>
        {
            seededLeaf.GetDeltaSinceAsync(Arg.Any<VersionVector>());
            loaded[0].SetCheckpointOffsetHintsAsync(Arg.Any<long[]>());
        });

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Closes.Single(c => c.Token == "bulk:load").LowInclusive, Is.Null, "a load's switch covers the shard");
            Assert.That(marker.Open, Is.Empty);
            Assert.That(marker.BulkBirthHints, Is.Empty);
        });
    }

    [Test]
    public async Task A_retried_completed_load_closes_a_switch_a_crash_left_open_without_applying_the_hint()
    {
        // A crash after the load published its root and before its switch closed: the
        // replaced root is no longer known, so its rows can be reached only by a replay
        // from the log's start. The retry closes the switch and applies nothing.
        var rig = CreateHintRig();
        rig.H.State.State.LastCompletedBulkOperationId = "load";
        rig.H.State.State.RootNodeId = rig.H.AddLeaf();
        await HandOffMarkerRows.UpdateShardAsync(rig.Storage, TreeId, 0, m =>
            m.BulkBirthHints.TryAdd("load", [BirthHead]) | HandOffMarkerCore.Open(m, "bulk:load", null, null));

        await rig.H.Grain.BulkLoadRawAsync("load", RawEntries(2));

        foreach (var leaf in rig.H.Leaves.Values)
        {
            Assert.That(HintAppliedTo(leaf), Is.Null);
        }

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Is.Empty, "the abandoned switch is closed");
            Assert.That(marker.Closes.Select(c => c.Token), Has.Member("bulk:load"));
            Assert.That(marker.BulkBirthHints, Is.Empty);
        });
    }

    [Test]
    public async Task The_next_bulk_operation_closes_a_switch_an_abandoned_append_left_open_without_applying_its_hint()
    {
        // Review N1 of #5702: an append that opened its switch and stopped before it
        // recorded its graft, and was never re-driven, would hold its range on the
        // replay-based advance for good. The next bulk operation on the shard closes it.
        var rig = CreateHintRig();
        SeedAppendTail(rig.H);
        await HandOffMarkerRows.UpdateShardAsync(rig.Storage, TreeId, 0, m =>
            m.BulkBirthHints.TryAdd("abandoned", [BirthHead]) | HandOffMarkerCore.Open(m, "bulk:abandoned", "j", null));

        await rig.H.Grain.BulkAppendAsync("op-next", Pairs(5));

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open.Keys, Is.EqualTo(new[] { "bulk:op-next" }), "the abandoned switch is closed; the new one waits for its run's fence");
            Assert.That(marker.Closes.Single(c => c.Token == "bulk:abandoned").Heads, Is.Not.Empty.And.All.EqualTo(CloseHead));
            Assert.That(marker.BulkBirthHints.Keys, Is.EqualTo(new[] { "op-next" }), "the abandoned hint is removed with its close");
            Assert.That(marker.BulkBirthHints["op-next"], Is.All.EqualTo(CloseHead - 1),
                "the close is folded into the open: the new switch's hint is the fence's heads, read no later than its first leaf's birth");
        });
    }

    [Test]
    public async Task The_next_bulk_operation_leaves_the_switch_of_a_graft_still_pending_open()
    {
        var rig = CreateHintRig();
        SeedAppendTail(rig.H);
        rig.H.State.State.PendingBulkGraft = new PendingBulkGraft
        {
            OperationId = "pending",
            ExistingRightmostLeafId = rig.H.State.State.RootNodeId!.Value,
            NewLeaves = [],
            RootWasLeaf = true,
        };
        await HandOffMarkerRows.UpdateShardAsync(rig.Storage, TreeId, 0, m =>
            m.BulkBirthHints.TryAdd("pending", [BirthHead]) | HandOffMarkerCore.Open(m, "bulk:pending", "j", null));

        await rig.H.Grain.BeginBulkSwitchForTestingAsync("op-next", "k");

        var marker = await ShardMarkerAsync(rig);
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Does.ContainKey("bulk:pending"), "a pending graft still owns its switch");
            Assert.That(marker.BulkBirthHints, Does.ContainKey("pending"));
        });
    }

    private static int HeadReads(HintRig rig) =>
        rig.Coordinator.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(ILeafReplayCoordinatorGrain.GetHeadOffsetAsync));

    private static int Claims(HintRig rig) =>
        rig.Wal.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IWalShardGrain.ClaimRoutingGenerationAsync));

    [Test]
    public async Task The_next_chunk_of_a_run_reuses_the_runs_hint_and_reads_no_heads()
    {
        // Review C1 of #5702. The reused hint was read before this chunk began, so it is
        // no higher than the heads at its first leaf's birth: it never lies above a record
        // the chunk's leaves have not applied, it only lengthens their replay.
        var rig = CreateHintRig(advancingHeads: true);
        var first = await rig.H.Grain.BeginBulkSwitchForTestingAsync("chunk-1", "a");
        var readsAfterFirst = HeadReads(rig);

        var second = await rig.H.Grain.BeginBulkSwitchForTestingAsync("chunk-2", "m");

        var headNow = await rig.Coordinator.GetHeadOffsetAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(readsAfterFirst, Is.GreaterThan(0), "precondition: the run's first chunk read the heads");
            Assert.That(HeadReads(rig) - readsAfterFirst, Is.EqualTo(1), "the second chunk read no heads (one read is this test's own)");
            Assert.That(second, Is.EqualTo(first), "the second chunk reuses the run's hint");
            Assert.That(second, Is.All.LessThan(headNow), "the reused hint is below the heads at the chunk's begin");
            Assert.That(Claims(rig), Is.Zero, "no fence ran");
        });
    }

    [Test]
    public async Task The_chunk_after_a_full_run_closes_it_with_one_fence_folded_into_its_open_and_takes_the_fences_heads()
    {
        var rig = CreateHintRig();
        for (var i = 0; i < ShardRootGrain.BulkSwitchesPerFence; i++)
        {
            await rig.H.Grain.BeginBulkSwitchForTestingAsync($"chunk-{i}", $"k{i}");
        }

        Assert.That(Claims(rig), Is.Zero, "precondition: no fence ran while the run filled");
        var writesBefore = rig.Storage.Writes;

        var hint = await rig.H.Grain.BeginBulkSwitchForTestingAsync("chunk-next", "z");

        var marker = await ShardMarkerAsync(rig);
        var partitions = marker.Closes.First().Heads.Length;
        Assert.Multiple(() =>
        {
            Assert.That(Claims(rig), Is.EqualTo(partitions), "one claim per partition closes the whole run");
            Assert.That(marker.Open.Keys, Is.EqualTo(new[] { "bulk:chunk-next" }));
            Assert.That(marker.Closes.Count(c => c.Token.StartsWith("bulk:chunk-", StringComparison.Ordinal)), Is.EqualTo(ShardRootGrain.BulkSwitchesPerFence));
            Assert.That(marker.Closes.SelectMany(c => c.Heads), Is.All.EqualTo(CloseHead));
            Assert.That(marker.BulkBirthHints.Keys, Is.EqualTo(new[] { "chunk-next" }), "the closed switches' hints are removed");
            Assert.That(hint, Is.All.EqualTo(CloseHead - 1), "the new chunk takes one below the fence's heads as its hint (#5760)");
            Assert.That(rig.Storage.Writes - writesBefore, Is.EqualTo(2), "the generation raise, then one write that closes the run and opens the chunk");
        });
    }

    [Test]
    public async Task The_idle_close_closes_every_completed_switch_with_one_fence_and_ends_the_run()
    {
        var rig = CreateHintRig();
        await rig.H.Grain.BeginBulkSwitchForTestingAsync("chunk-1", "a");
        await rig.H.Grain.BeginBulkSwitchForTestingAsync("chunk-2", "m");
        var readsBefore = HeadReads(rig);

        await rig.H.Grain.CloseBulkSwitchesForTestingAsync();

        var marker = await ShardMarkerAsync(rig);
        var partitions = marker.Closes.First().Heads.Length;
        Assert.Multiple(() =>
        {
            Assert.That(marker.Open, Is.Empty);
            Assert.That(marker.BulkBirthHints, Is.Empty);
            Assert.That(marker.Closes.Select(c => c.Token), Is.SupersetOf(new[] { "bulk:chunk-1", "bulk:chunk-2" }));
            Assert.That(Claims(rig), Is.EqualTo(partitions), "one fence for the run");
        });

        // The run has ended, so the next chunk does not reuse a hint from before it.
        await rig.H.Grain.BeginBulkSwitchForTestingAsync("chunk-3", "t");
        Assert.That(HeadReads(rig), Is.GreaterThan(readsBefore), "a new run reads the heads afresh");
    }

    [Test]
    public async Task The_idle_close_with_no_switch_open_runs_no_fence()
    {
        var rig = CreateHintRig();

        await rig.H.Grain.CloseBulkSwitchesForTestingAsync();

        Assert.That(Claims(rig), Is.Zero);
    }

    [Test]
    public async Task A_resumed_chunk_keeps_its_stored_hint_when_it_folds_a_fence()
    {
        // S7: a resume never takes a later hint than its first attempt stored, even when
        // it closes a full run and holds the fence's later heads.
        var rig = CreateHintRig();
        await HandOffMarkerRows.UpdateShardAsync(rig.Storage, TreeId, 0, m => m.BulkBirthHints.TryAdd("resumed", [7]));
        for (var i = 0; i < ShardRootGrain.BulkSwitchesPerFence; i++)
        {
            await rig.H.Grain.BeginBulkSwitchForTestingAsync($"chunk-{i}", $"k{i}");
        }

        var hint = await rig.H.Grain.BeginBulkSwitchForTestingAsync("resumed", "z");

        Assert.Multiple(() =>
        {
            Assert.That(Claims(rig), Is.GreaterThan(0), "precondition: the resume folded the run's fence");
            Assert.That(hint, Is.EqualTo(new long[] { 7 }), "the resume keeps the hint its first attempt stored");
        });
    }

    [Test]
    public async Task A_bulk_append_applies_its_hint_capped_per_partition_at_the_old_leafs_replay_frontier()
    {
        // Issue #5752: a record of the appended range that another author appended
        // below the hint and the old leaf never applied lies above the old leaf's
        // frontier; an appended leaf starting at the hint would skip it, and the old
        // leaf, narrowed, would skip it too.
        var rig = CreateHintRig();
        var existing = SeedAppendTail(rig.H);
        var frontier = Enumerable.Repeat(BirthHead + 5, 64).ToArray();
        frontier[0] = BirthHead - 10;
        frontier[1] = -1;
        rig.H.Leaf(existing).NarrowHighKeyAsync(Arg.Any<string>(), Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>())
            .Returns(Task.FromResult<LeafNarrowResult?>(new LeafNarrowResult { ReadFrontier = frontier }));

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        var appended = LeavesByBirthLow(rig.H);
        Assert.That(appended, Is.Not.Empty, "precondition: the append created leaves");
        foreach (var (low, leaf) in appended)
        {
            var applied = HintAppliedTo(leaf);
            Assert.That(applied, Is.Not.Null, $"the appended leaf at {low} reads from the capped hint");
            Assert.Multiple(() =>
            {
                Assert.That(applied![0], Is.EqualTo(BirthHead - 10), "capped at the frontier, not at the frontier plus one");
                Assert.That(applied[1], Is.EqualTo(-1), "a partition the old leaf never replayed is replayed from the start");
                Assert.That(applied.Skip(2), Is.All.EqualTo(BirthHint), "the hint, one below the head, where it is the lower");
            });
        }
    }

    [Test]
    public async Task A_graft_whose_old_leaf_reports_no_frontier_applies_no_hint()
    {
        // An old leaf an earlier attempt narrowed cannot say how far it had replayed
        // when it narrowed, so the resumed graft's leaves replay from the log's start.
        var rig = CreateHintRig();
        var existing = SeedAppendTail(rig.H);
        rig.H.Leaf(existing).NarrowHighKeyAsync(Arg.Any<string>(), Arg.Any<GrainId?>(), Arg.Any<List<GraftEntry>?>())
            .Returns(Task.FromResult<LeafNarrowResult?>(new LeafNarrowResult { ReadFrontier = null }));

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        foreach (var (low, leaf) in LeavesByBirthLow(rig.H))
        {
            Assert.That(HintAppliedTo(leaf), Is.Null, $"the appended leaf at {low} keeps reading from the log's start");
        }
    }

    [Test]
    public async Task A_graft_hands_the_old_leaf_every_appended_leaf_so_it_can_move_their_read_gates()
    {
        // Issue #5755: the old leaf moves the shadow markers of the rows it hands back
        // to the appended leaf each will be merged into, so it needs all of them.
        var rig = CreateHintRig();
        var existing = SeedAppendTail(rig.H);

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

        var appended = LeavesByBirthLow(rig.H);
        var expected = appended.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => (kv.Key, ((IGrainBase)kv.Value).GrainContext.GrainId)).ToList();
        var call = rig.H.Leaf(existing).ReceivedCalls()
            .Single(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.NarrowHighKeyAsync));
        var targets = (List<GraftEntry>?)call.GetArguments()[2];
        Assert.That(targets?.Select(e => (e.SeparatorKey, e.LeafId)), Is.EqualTo(expected),
            "every appended leaf, by separator, in order");
    }
}
