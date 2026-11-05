using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The leaf's detectors for #5837's claimless advance, each named for the model mutation
/// it refutes (<c>spec/wal/LiveAdvanceCoreRefinement.md</c>): the free advance's target
/// and guard, the split's consistent cut and its marker view, the donor's close head, the
/// split transfer's class, the persisted narrow tag, the birth proof, and the latch
/// exemption with its three conditions. Every one runs under the pre-arm gate.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly Guid ClaimlessDonor = new("d0000000-0000-0000-0000-00000000c0de");
    private static readonly Guid ClaimlessDonorToken = new("c0000000-0000-0000-0000-00000000c0de");

    private static LiveAdvanceRig PreArmRig()
    {
        var rig = new LiveAdvanceRig();
        rig.FenceGate.IsPreArmOpen = true;
        return rig;
    }

    private static Task InitialiseOwnMarkerAsync(LiveAdvanceRig rig) =>
        rig.SetLeafMarkerAsync(static m =>
        {
            m.Initialised = true;
            return true;
        });

    /// <summary>Records a donor close with its bound pending on the rig leaf's own marker.</summary>
    private static Task PendingDonorCloseAsync(LiveAdvanceRig rig) =>
        rig.SetLeafMarkerAsync(m =>
        {
            m.Initialised = true;
            HandOffMarkerCore.Open(m, "split:donor", null, null);
            return HandOffMarkerCore.CloseWithDeferredBound(m, "split:donor", [0], ClaimlessDonor, ClaimlessDonorToken, 0, 1);
        });

    [Test]
    public async Task FreeAdvanceIgnoresForeignHigh()
    {
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        var foreign = rig.Writer.ForeignAppend();
        rig.ReportedWatermark = foreign;
        await SetStampedAsync(grain, "a", "1");

        var held = !await grain.TryAdvanceLiveCheckpointAsync();
        await grain.SetCheckpointOffsetHintsAsync([foreign]);
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True, "the claimless advance passed a reported foreign record it had not read");
            Assert.That(advanced, Is.True, "once read past, the reported bound no longer holds the advance");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(foreign + 1));
        });
    }

    [Test]
    public async Task FreeAdvanceIgnoresCloseHeads()
    {
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await rig.SetLeafMarkerAsync(static m =>
        {
            m.Initialised = true;
            HandOffMarkerCore.Open(m, "reclaim:x", null, null);
            return HandOffMarkerCore.Close(m, "reclaim:x", [5]);
        });
        for (var i = 0; i < 6; i++)
        {
            rig.Writer.ForeignAppend();
        }

        await SetStampedAsync(grain, "a", "1");

        var held = !await grain.TryAdvanceLiveCheckpointAsync();
        await grain.SetCheckpointOffsetHintsAsync([4]);
        var advanced = await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True, "the claimless advance passed a close head it had not read to");
            Assert.That(advanced, Is.True);
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(6L));
        });
    }

    [Test]
    public async Task FreeAdvanceViewBeforeHeads()
    {
        // FreeTarget is fixed before the marker view is read: an own append landing
        // during the read, above a record the leaf has not read, is not passed.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        long foreign = -1;
        rig.AfterNextLeafMarkerRead = async () =>
        {
            foreign = rig.Writer.ForeignAppend();
            await SetStampedAsync(grain, "b", "2");
        };

        await grain.TryAdvanceLiveCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(foreign, Is.EqualTo(1L), "precondition: a record was allocated during the read");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.LessThan(foreign),
                "the target was taken after the view, so the advance passed an unread record");
        });
    }

    private static async Task<(BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, IBPlusLeafGrain Sibling, LiveAdvanceRig Rig)>
        PreArmSplitAsync(WalSplitCut cut, long frontier, Action<LiveAdvanceRig>? configure = null)
    {
        var rig = PreArmRig();
        rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(cut));
        configure?.Invoke(rig);
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        sibling.MergeEntriesAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>()).Returns(Task.FromResult<SplitResult?>(null));
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(cut.Head));
        var state = new FakePersistentState<LeafNodeState>();
        if (frontier >= 0)
        {
            state.State.ProjectionCheckpointOffset = frontier;
            state.State.ProjectionCheckpointOffsetAssigned = true;
        }

        var grain = CreateGrain(
            state,
            siblingStub: sibling,
            maxLeafKeys: 3,
            liveAdvance: rig,
            configureGrainFactory: f =>
            {
                f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard);
                f.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(coordinator);
            });
        await InitialiseOwnMarkerAsync(rig);
        for (var i = 0; i < 4; i++)
        {
            await SetStampedAsync(grain, $"k{i}", "v");
        }

        return (grain, state, sibling, rig);
    }

    [Test]
    public async Task SplitHintWithoutBound()
    {
        // A bound the donor has not read past keeps the sibling's hint at its frontier.
        var (_, _, sibling, _) = await PreArmSplitAsync(new WalSplitCut(Head: 42, ForeignHigh: 35, UnstampedWatermark: -1), frontier: 30);

        await sibling.Received(1).SetCheckpointOffsetHintsAsync(Arg.Is<long[]>(h => h.Length == 1 && h[0] == 30L));
    }

    [Test]
    public async Task SplitHintAboveHeads()
    {
        // A donor past every bound hands the sibling one below the cut's head, never more.
        var (_, _, sibling, _) = await PreArmSplitAsync(new WalSplitCut(Head: 42, ForeignHigh: 20, UnstampedWatermark: 25), frontier: 30);

        await sibling.Received(1).SetCheckpointOffsetHintsAsync(Arg.Is<long[]>(h => h.Length == 1 && h[0] == 41L));
    }

    [Test]
    public async Task SplitViewBeforeHeads()
    {
        // V3: the split reads its marker view after the cut, never before it.
        var order = new List<string>();
        var (_, _, sibling, _) = await PreArmSplitAsync(
            new WalSplitCut(Head: 42, ForeignHigh: 20, UnstampedWatermark: 25),
            frontier: 30,
            configure: rig =>
            {
                rig.Shard.When(s => s.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())).Do(_ => order.Add("cut"));
                rig.ShardRoot.When(s => s.GetLiveAdvanceViewAsync(Arg.Any<GrainId>(), Arg.Any<string?>(), Arg.Any<string?>())).Do(_ => order.Add("view"));
            });

        Assert.Multiple(() =>
        {
            Assert.That(order, Does.Contain("cut"), "precondition: the split read its cut");
            Assert.That(order.IndexOf("view"), Is.GreaterThan(order.IndexOf("cut")), "the marker view was read before the cut");
        });
        await sibling.Received(1).SetCheckpointOffsetHintsAsync(Arg.Is<long[]>(h => h[0] == 41L));
    }

    [Test]
    public async Task ResolveAtPartitionHead()
    {
        // DeferredHeadCostWitness: with no own moved-key write since the heads, the donor
        // closes its sibling's window at the split's heads, not at the partition's head.
        var (_, state, _, rig) = await PreArmSplitAsync(new WalSplitCut(Head: 2, ForeignHigh: -1, UnstampedWatermark: -1), frontier: -1);
        Assert.That(state.State.UnlinkedSplitSiblingId, Is.Not.Null, "precondition: the donor split");
        var marker = (await HandOffMarkerRows.ReadLeafAsync(rig.Storage, state.State.UnlinkedSplitSiblingId!.Value.GetGuidKey())).State;

        var closes = marker.Closes.Where(c => c.DonorLeaf == rig.LeafId).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(closes, Has.Count.EqualTo(1), "precondition: the donor closed its sibling's window with its bound");
            Assert.That(closes.Single().Heads, Is.EqualTo(new long[] { 2 }), "the close head is not the split's head");
            Assert.That(rig.Writer.NextOffset(0), Is.GreaterThan(2L), "precondition: the partition's head is above the split's");
        });
    }

    [Test]
    public async Task TransferNotInForeignHigh()
    {
        // T1/T3: a split's transfer into its sibling is appended under the span's routing
        // generation, so it is attributed to the sibling and never raises the shared
        // watermark; a row outside the span, or a merge that is not a transfer, is not.
        var rig = PreArmRig();
        var grain = CreateGrain(liveAdvance: rig);
        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "live-advance-tree",
            ShardIndex = 0,
            LowKeyInclusive = "m",
            HighKeyExclusive = null,
            SplitBirth = true,
            SpanShard = 0,
            SpanGeneration = 3,
        });

        static Dictionary<string, LwwValue<byte[]>> Rows(string key) =>
            new() { [key] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v"), HybridLogicalClock.Tick(HybridLogicalClock.Zero)) };

        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(Rows("n")));
        var transfer = rig.Writer.LastRouting;
        await SplitTransferScope.RunAsync(() => grain.MergeEntriesAsync(Rows("a")));
        var outOfSpan = rig.Writer.LastRouting;
        await grain.MergeEntriesAsync(Rows("o"));
        var notTransfer = rig.Writer.LastRouting;

        Assert.Multiple(() =>
        {
            Assert.That(transfer, Is.EqualTo(WalRoutingStamp.ForShardRoot(0, 3)), "the transfer was not appended under the span's generation");
            Assert.That(outOfSpan.IsSet, Is.False, "a row outside the span rode the transfer class");
            Assert.That(notTransfer.IsSet, Is.False, "a merge that is not a transfer rode the transfer class");
        });
    }

    [Test]
    public async Task BirthProofOnReinit()
    {
        // P1: only the call that seeds the write-once slots proves; a re-init does not.
        var born = PreArmRig();
        var bornGrain = CreateGrain(liveAdvance: born);
        await bornGrain.InitializeSiblingAsync(new SiblingInitialization { TreeId = "live-advance-tree", ShardIndex = 0, LowKeyInclusive = "m", SplitBirth = true });

        var reinit = PreArmRig();
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "m";
        var reinitGrain = CreateGrain(state, liveAdvance: reinit);
        await reinitGrain.InitializeSiblingAsync(new SiblingInitialization { TreeId = "live-advance-tree", ShardIndex = 0, LowKeyInclusive = "m", SplitBirth = true });

        var unflagged = PreArmRig();
        var unflaggedGrain = CreateGrain(liveAdvance: unflagged);
        await unflaggedGrain.InitializeSiblingAsync(new SiblingInitialization { TreeId = "live-advance-tree", ShardIndex = 0, LowKeyInclusive = "m" });

        Assert.Multiple(() =>
        {
            Assert.That(bornGrain.AppendFenceForTesting?.IsReachabilityProvenForTesting, Is.True, "a split's own birth did not prove");
            Assert.That(reinitGrain.AppendFenceForTesting?.IsReachabilityProvenForTesting ?? false, Is.False, "a re-init proved");
            Assert.That(unflaggedGrain.AppendFenceForTesting?.IsReachabilityProvenForTesting ?? false, Is.False, "a birth that is not a split's proved (P2)");
        });
    }

    private static async Task<BPlusLeafGrain> UnreachableExemptLeafAsync(LiveAdvanceRig rig)
    {
        rig.Reachable = false;
        rig.ShardMarker.RoutingGeneration = 1;
        var grain = CreateGrain(liveAdvance: rig);
        await PendingDonorCloseAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        return grain;
    }

    [Test]
    public async Task PendingDonorCloseMarksProven()
    {
        // An unreachable sibling with its donor's bound pending is deferred, never proven.
        var rig = PreArmRig();
        var grain = await UnreachableExemptLeafAsync(rig);

        Assert.That(grain.AppendFenceForTesting!.IsReachabilityProvenForTesting, Is.False, "a pending donor close marked the leaf proven");
    }

    [Test]
    public async Task LatchUnreachableWithPendingDonorClose()
    {
        // The exemption's witness: the same sibling is not latched unstamped either, so it
        // stamps once its link resolves the bound.
        var rig = PreArmRig();
        var grain = await UnreachableExemptLeafAsync(rig);
        await grain.TryAdvanceLiveCheckpointAsync();

        Assert.That(grain.AppendFenceForTesting!.IsUnreachable, Is.False, "an unreachable sibling with a pending donor close was latched");
    }

    [Test]
    public async Task ExemptLeafArmsUnreachable()
    {
        // The exemption defers the decision; it never lets an unreachable leaf arm.
        var rig = PreArmRig();
        var grain = await UnreachableExemptLeafAsync(rig);

        await grain.TryAdvanceLiveCheckpointAsync(withinDrive: true);

        await rig.Shard.DidNotReceiveWithAnyArgs().ArmAppendEpochAsync(default, default, default, default, default, default);
        Assert.That(grain.AppendFenceForTesting!.HasClaimedAnywhere, Is.False, "an exempt, unreachable leaf claimed");
    }

    [Test]
    public async Task PendingExemptionOverridesOrphanRepair()
    {
        // Orphan repair wins: the pending donor close does not exempt the leaf.
        var rig = PreArmRig();
        rig.OrphanRepairInProgress = true;
        var grain = await UnreachableExemptLeafAsync(rig);
        await grain.TryAdvanceLiveCheckpointAsync();

        Assert.That(grain.AppendFenceForTesting!.IsUnreachable, Is.True, "a pending donor close exempted a leaf under orphan repair");
    }

    [Test]
    public async Task ForwardExcludedWithoutObligation()
    {
        // X4/X7: the narrow's forwards are excluded only once the narrow persisted with its
        // obligation; a persist that fails leaves the activation's stamps untagged.
        var (failed, failedSibling) = await ResumedPreArmSplitAsync(failNarrow: true);
        var (persisted, persistedSibling) = await ResumedPreArmSplitAsync(failNarrow: false);

        Assert.Multiple(() =>
        {
            Assert.That(failed.AppendFenceForTesting!.PersistedNarrowTag, Is.EqualTo(Guid.Empty), "a narrow whose persist failed tagged the stamps");
            Assert.That(persisted.AppendFenceForTesting!.PersistedNarrowTag, Is.EqualTo(persistedSibling.GetGuidKey()), "a persisted narrow did not tag the stamps");
            Assert.That(failedSibling, Is.Not.EqualTo(default(GrainId)));
        });
    }

    private static async Task<(BPlusLeafGrain Grain, GrainId Sibling)> ResumedPreArmSplitAsync(bool failNarrow)
    {
        var rig = PreArmRig();
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(
            state,
            liveAdvance: rig,
            configureGrainFactory: f => f.GetGrain<IWalShardGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(rig.Shard));
        await SetStampedAsync(grain, "a", "v");
        var sibling = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var clock = HybridLogicalClock.Zero;
        for (var i = 0; i < 127; i++)
        {
            clock = HybridLogicalClock.Tick(clock);
            grain.EntriesForTest[$"key-{i:D4}"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes($"v-{i}"), clock);
        }

        state.State.SplitState = SplitState.SplitInProgress;
        state.State.SplitKey = "key-0064";
        state.State.SplitSiblingId = sibling;
        state.State.NextSibling = sibling;
        state.OnWriteState = written =>
        {
            if (failNarrow && written.HighKeyExclusive == "key-0064")
            {
                throw new InvalidOperationException("injected: the narrow's persist failed");
            }
        };

        clock = HybridLogicalClock.Tick(clock);
        try
        {
            await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
            {
                ["new-key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), clock),
            });
        }
        catch (InvalidOperationException) when (failNarrow)
        {
            // The injected failure; what matters is the tag it left.
        }

        return (grain, sibling);
    }
}
