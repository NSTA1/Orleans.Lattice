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
        PreArmSplitAsync(WalSplitCut cut, long frontier, Action<LiveAdvanceRig>? configure = null, Action<FakePersistentState<LeafNodeState>, LiveAdvanceRig>? onState = null, Action<FakePersistentState<LeafNodeState>>? beforeLastWrite = null)
    {
        var rig = PreArmRig();
        rig.Shard.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(cut));
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

        onState?.Invoke(state, rig);
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
            if (i == 3)
            {
                beforeLastWrite?.Invoke(state);
            }

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
                rig.Shard.When(s => s.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())).Do(_ => order.Add("cut"));
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

    [Test]
    public async Task A_fresh_split_closes_from_its_own_open_row_and_an_intervening_write_forces_the_read()
    {
        // #5837, E6 (a): a marker write between the open and the close misses the open's
        // ETag; the close re-reads, still lands, and keeps the intervening write.
        var narrowed = false;
        var leafMarkerReadsAfterNarrow = 0;
        var (_, state, _, rig) = await PreArmSplitAsync(
            new WalSplitCut(Head: 2, ForeignHigh: -1, UnstampedWatermark: -1),
            frontier: -1,
            onState: (s, r) =>
            {
                r.OnStorageRead = stateName =>
                {
                    if (narrowed && stateName == HandOffMarkerRows.LeafGrainType)
                    {
                        leafMarkerReadsAfterNarrow++;
                    }
                };
                s.OnWriteState = written =>
                {
                    if (!narrowed && written.UnlinkedSplitSiblingId is { } sibling)
                    {
                        narrowed = true;
                        HandOffMarkerRows.UpdateLeafAsync(r.Storage, sibling.GetGuidKey(), m =>
                        {
                            m.CompactedHeads = [7];
                            return true;
                        }).GetAwaiter().GetResult();
                    }
                };
            });

        var marker = (await HandOffMarkerRows.ReadLeafAsync(rig.Storage, state.State.UnlinkedSplitSiblingId!.Value.GetGuidKey())).State;
        Assert.Multiple(() =>
        {
            Assert.That(narrowed, Is.True, "precondition: the split narrowed");
            Assert.That(marker.Open, Is.Empty, "the close did not land after its ETag missed");
            Assert.That(marker.Closes.Count(c => c.DonorLeaf == rig.LeafId), Is.EqualTo(1), "the close did not record its bound");
            Assert.That(marker.CompactedHeads, Is.EqualTo(new long[] { 7 }), "the close overwrote a write made after the open");
            Assert.That(leafMarkerReadsAfterNarrow, Is.GreaterThan(0), "the ETag miss did not fall back to a read");
        });
    }

    [Test]
    public async Task A_fresh_split_closes_with_no_marker_read_after_its_narrow()
    {
        // #5837, E1: the close writes from the open's own row on its ETag, with no read.
        var narrowed = false;
        var leafMarkerReadsAfterNarrow = 0;
        var (_, state, _, rig) = await PreArmSplitAsync(
            new WalSplitCut(Head: 2, ForeignHigh: -1, UnstampedWatermark: -1),
            frontier: -1,
            onState: (s, r) =>
            {
                r.OnStorageRead = stateName =>
                {
                    if (narrowed && stateName == HandOffMarkerRows.LeafGrainType)
                    {
                        leafMarkerReadsAfterNarrow++;
                    }
                };
                s.OnWriteState = written => narrowed |= written.UnlinkedSplitSiblingId is not null;
            });

        var marker = (await HandOffMarkerRows.ReadLeafAsync(rig.Storage, state.State.UnlinkedSplitSiblingId!.Value.GetGuidKey())).State;
        Assert.Multiple(() =>
        {
            Assert.That(marker.Closes.Count(c => c.DonorLeaf == rig.LeafId), Is.EqualTo(1), "precondition: the close landed");
            Assert.That(leafMarkerReadsAfterNarrow, Is.Zero, "the close read the marker its own open wrote");
        });
    }

    [Test]
    public async Task A_resumed_split_closes_on_the_read_path()
    {
        // #5837, E6 (b): a resumed split has no open row of its own; its close reads.
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
        var narrowed = false;
        var leafMarkerReadsAfterNarrow = 0;
        state.OnWriteState = written => narrowed |= written.HighKeyExclusive == "key-0064";
        rig.OnStorageRead = stateName =>
        {
            if (narrowed && stateName == HandOffMarkerRows.LeafGrainType)
            {
                leafMarkerReadsAfterNarrow++;
            }
        };

        clock = HybridLogicalClock.Tick(clock);
        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["new-key"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("new"), clock),
        });
        var marker = (await HandOffMarkerRows.ReadLeafAsync(rig.Storage, sibling.GetGuidKey())).State;

        Assert.Multiple(() =>
        {
            Assert.That(marker.Closes.Count(c => c.DonorLeaf == rig.LeafId), Is.EqualTo(1), "precondition: the resumed close landed");
            Assert.That(leafMarkerReadsAfterNarrow, Is.GreaterThan(0), "a resumed split closed without reading the marker");
        });
    }

    [Test]
    public void A_marker_copy_carries_every_property()
    {
        // #5837, E3: the close mutates a copy of its open's row, so the copy must be whole.
        var close = new HandOffClose { Token = "t", LowInclusive = "a", HighExclusive = "z", Heads = [1, 2], Sequence = 3, DeferredShard = 4, DeferredGeneration = 5, DonorLeaf = Guid.NewGuid(), DonorToken = Guid.NewGuid(), AdmittedShard = 6, AdmittedGeneration = 7, LinkedGeneration = 10, LinkedShard = 11, CarryResolvedAt = [true, false] };
        var marker = new LeafHandOffState { Initialised = true, CompactedHeads = [8], Sequence = 9, SiblingLinkedGeneration = 12, SiblingLinkedShard = 13 };
        marker.Open["o"] = new HandOffMarkerRange { Token = "o", LowInclusive = "b", HighExclusive = "y" };
        marker.Closes.Add(close);

        var copy = HandOffMarkerRows.Copy(marker);

        static IEnumerable<(string Name, object? Value)> Props(object o) =>
            o.GetType().GetProperties().Select(p => (p.Name, p.GetValue(o)));

        Assert.Multiple(() =>
        {
            Assert.That(typeof(LeafHandOffState).GetProperties().Select(p => p.Name),
                Is.EquivalentTo(new[] { "Initialised", "Open", "Closes", "CompactedHeads", "Sequence", "SiblingLinkedGeneration", "SiblingLinkedShard" }), "a marker property the copy does not know of");
            Assert.That(typeof(HandOffClose).GetProperties(), Has.Length.EqualTo(14), "a close property the copy does not know of");
            Assert.That(typeof(HandOffMarkerRange).GetProperties(), Has.Length.EqualTo(3), "a range property the copy does not know of");
            Assert.That(Props(copy.Closes.Single()).Where(p => p.Name is not ("Heads" or "CarryResolvedAt")), Is.EqualTo(Props(close).Where(p => p.Name is not ("Heads" or "CarryResolvedAt"))));
            Assert.That(copy.Closes.Single().Heads, Is.EqualTo(close.Heads).And.Not.SameAs(close.Heads));
            Assert.That(copy.Closes.Single().CarryResolvedAt, Is.EqualTo(close.CarryResolvedAt).And.Not.SameAs(close.CarryResolvedAt));
            Assert.That(copy.Closes.Single(), Is.Not.SameAs(close));
            Assert.That((copy.Initialised, copy.Sequence, copy.SiblingLinkedGeneration, copy.SiblingLinkedShard), Is.EqualTo((true, 9L, 12L, 13)));
            Assert.That(copy.CompactedHeads, Is.EqualTo(marker.CompactedHeads).And.Not.SameAs(marker.CompactedHeads));
            Assert.That(copy.Open["o"].HighExclusive, Is.EqualTo("y"));
            Assert.That(copy.Open["o"], Is.Not.SameAs(marker.Open["o"]));
        });
    }
}
