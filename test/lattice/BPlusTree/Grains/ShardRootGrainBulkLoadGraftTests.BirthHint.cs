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

    private sealed record HintRig(Harness H, EnumerableMemoryGrainStorage Storage, IWalShardGrain Wal);

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
        return new HintRig(h, storage, wal);
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
            Assert.That(atBirth.All(m => m.BulkBirthHints.TryGetValue("op-hint", out var hint) && hint.Length > 0 && hint.All(x => x == BirthHead)), Is.True,
                "the hint is durable before any appended leaf is born");
            Assert.That(atBirth.All(m => m.Open.TryGetValue("bulk:op-hint", out var range) && range.LowInclusive == "k0000" && range.HighExclusive is null), Is.True,
                "the marker is open over the appended range before any appended leaf is born");
        });
    }

    [Test]
    public async Task A_bulk_append_applies_its_hint_to_every_appended_leaf_then_closes_its_marker_by_fence()
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

        var appended = LeavesByBirthLow(rig.H);
        Assert.That(appended, Has.Count.EqualTo(3), "precondition: three appended leaves");
        foreach (var (low, leaf) in appended)
        {
            Assert.That(HintAppliedTo(leaf), Is.Not.Empty.And.All.EqualTo(BirthHead), $"the appended leaf at {low} reads from the hint");
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
        rig.H.Leaf(existing).NarrowHighKeyAsync(Arg.Any<string>(), Arg.Any<GrainId?>())
            .Returns(Task.FromException<List<LwwEntry>>(unknown));

        await rig.H.Grain.BulkAppendAsync("op-hint", Pairs(5));

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
        Assert.That(loaded.SelectMany(l => HintAppliedTo(l)!), Is.All.EqualTo(BirthHead));

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
}
