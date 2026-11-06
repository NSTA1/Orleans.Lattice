using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The lazy carry re-claim and the carrying cut reader (#5851; a5e2e2ef's A4 amendment
/// on #5866, LA1-LA4). The backstop re-claims a witnessed carried close only while it is
/// binding - an own applied append above the checkpoint that the advance did not pass -
/// judged on the read that triggers it; a carriage that binds nothing is delayed, never
/// ended, so its stamps keep the close, and no arm creates the epoch row a later split's
/// duplicate fence would claim every partition on. A split's first cut reader carries the
/// closes its marker carries; a close recorded after the reader still caps the cut.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static int ShardCalls(LiveAdvanceRig rig, string method) =>
        rig.Shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == method);

    /// <summary>
    /// A one-partition split-born sibling - a birth checkpoint, so never starved - with a
    /// witnessed carried close and an own write it has since read past.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, LiveAdvanceRig Rig)> NonBindingCarriedSiblingAsync()
    {
        var rig = new LiveAdvanceRig(partitions: 1, firstOffset: 1);
        rig.FenceGate.IsPreArmOpen = true;
        var state = new Orleans.Lattice.Tests.Fakes.FakePersistentState<LeafNodeState>();
        state.State.ProjectionCheckpointOffset = 0;
        state.State.ProjectionCheckpointOffsetAssigned = true;
        var grain = CreateGrain(state, liveAdvance: rig);
        await WitnessedCarriedCloseAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        await grain.TryAdvanceLiveCheckpointAsync();
        AcceptCarryArms(rig);
        await grain.SetCheckpointOffsetHintsAsync([rig.Writer.NextOffset(0) - 1]);
        return (grain, rig);
    }

    [Test]
    public async Task LazyCarry_a_non_binding_carried_close_is_never_armed_and_stays_carried_until_a_raised_stale_high_binds_it()
    {
        // LA1 (ReadPositionHonestCarryDroppedWhenNotBinding): below every checkpoint the
        // carriage is not armed, and not ended; its stamps keep the close. A later
        // old-generation write the close's stale high reports holds the advance, which
        // binds the carriage, and only then does the backstop re-claim.
        var (grain, rig) = await NonBindingCarriedSiblingAsync();
        var fence = grain.AppendFenceForTesting!;
        var armsBefore = await CarryArmCallsAsync(rig);

        for (var tick = 0; tick < 3; tick++)
        {
            await grain.TryBackstopCarriedClosesAsync();
        }

        var armsWhileIdle = await CarryArmCallsAsync(rig);
        var carriedWhileIdle = fence.IsCarriedAtForTesting(0);
        var readerCarries = fence.TryGetCutReader(out var reader) && reader.Close1.IsSet;

        var stale = rig.Writer.ForeignAppend();
        rig.ReportedWatermark = stale;
        await SetStampedAsync(grain, "b", "2");
        var held = !await grain.TryAdvanceLiveCheckpointAsync();
        var armedOnceBinding = await grain.TryBackstopCarriedClosesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(armsWhileIdle, Is.EqualTo(armsBefore), "a carriage that binds nothing was re-claimed");
            Assert.That(carriedWhileIdle, Is.True, "a carriage that binds nothing was ended without the re-claim");
            Assert.That(readerCarries, Is.True, "the sibling's stamps dropped a close no arm resolved");
            Assert.That(held, Is.True, "the advance passed a stale high the carried close reported");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.LessThan(stale), "the leaf read past the old-generation write it never applied");
            Assert.That(armedOnceBinding, Is.True, "a binding carriage was not re-claimed");
        });
    }

    [Test]
    public async Task LazyCarry_a_non_binding_sibling_reads_nothing_on_the_tick()
    {
        // LA1: the binding check is in memory first, so a sibling it does not bind costs the
        // tick no marker read.
        var (grain, rig) = await NonBindingCarriedSiblingAsync();
        var markerReads = 0;
        rig.OnStorageRead = name =>
        {
            if (name == HandOffMarkerRows.LeafGrainType)
            {
                markerReads++;
            }
        };

        for (var tick = 0; tick < 3; tick++)
        {
            await grain.TryBackstopCarriedClosesAsync();
        }

        Assert.That(markerReads, Is.Zero, "a carriage that binds nothing read the marker on every tick");
    }

    [Test]
    public async Task LazyCarry_binding_is_judged_on_the_read_that_would_trigger_the_re_claim()
    {
        // LA2: binding when the tick starts, the partition is read past while the backstop's
        // own view read is in flight; the re-claim is skipped, and the carriage stays.
        var rig = CarryRig(partitions: 1);
        var grain = CreateGrain(liveAdvance: rig);
        await WitnessedCarriedCloseAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        await grain.TryAdvanceLiveCheckpointAsync();
        AcceptCarryArms(rig);
        var armsBefore = await CarryArmCallsAsync(rig);
        var readPast = rig.Writer.NextOffset(0) - 1;
        rig.AfterNextLeafMarkerRead = () => grain.SetCheckpointOffsetHintsAsync([readPast]);

        var armed = await grain.TryBackstopCarriedClosesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(readPast), "precondition: the read moved the checkpoint past the own append");
            Assert.That(armed, Is.False, "the backstop re-claimed on a binding verdict its own read had overtaken");
            Assert.That(grain.AppendFenceForTesting!.IsCarriedAtForTesting(0), Is.True, "skipping the re-claim ended the carriage");
        });
        Assert.That(await CarryArmCallsAsync(rig), Is.EqualTo(armsBefore), "the backstop re-claimed a partition no longer binding");
    }

    [Test]
    public async Task LazyCarry_an_unarmed_siblings_split_claims_nothing_at_its_duplicate_fence()
    {
        // The claim cascade: an arm writes the leaf's epoch row, and a split's duplicate
        // fence then claims every partition. A sibling the carriage never bound has no arm,
        // so no row, and its split's fence claims nothing.
        var (grain, rig) = await NonBindingCarriedSiblingAsync();
        for (var tick = 0; tick < 3; tick++)
        {
            await grain.TryBackstopCarriedClosesAsync();
        }

        var claimsBefore = ShardCalls(rig, nameof(IWalShardGrain.ClaimAppendEpochAsync));
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(rig.Storage, rig.LeafId);
        var fenced = await LeafSplitDuplicateFence.FenceAsync(rig.Storage, _ => rig.Shard, rig.Partitions, rig.LeafId, null, null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(row.RecordExists, Is.False, "the unbound carriage wrote an epoch row");
            Assert.That(fenced, Is.Null, "the split's duplicate fence found an activation to fence");
            Assert.That(ShardCalls(rig, nameof(IWalShardGrain.ClaimAppendEpochAsync)), Is.EqualTo(claimsBefore), "the split's duplicate fence claimed");
        });
    }

    private static void WitnessedCarriedCloseNow(LiveAdvanceRig rig) =>
        rig.SetLeafMarkerAsync(m =>
        {
            m.Initialised = true;
            HandOffMarkerCore.Open(m, "split:donor", null, null);
            HandOffMarkerCore.CloseWithDeferredBound(m, "split:donor", new long[rig.Partitions], ClaimlessDonor, ClaimlessDonorToken, 0, 1);
            return HandOffMarkerCore.WitnessLink(m, ClaimlessDonor, 0, CarryWitness);
        }).GetAwaiter().GetResult();

    [Test]
    public async Task CarryingReader_a_siblings_first_split_takes_a_reader_carrying_its_closes_and_uplifts()
    {
        // LA4: the sibling never read its view before splitting; its reader still carries
        // the witnessed close its marker carries, so the cut is not capped on a mismatch.
        var (_, _, sibling, _) = await PreArmSplitAsync(
            new WalSplitCut(Head: 42, ForeignHigh: 20, UnstampedWatermark: 25),
            frontier: 30,
            onState: (_, rig) => WitnessedCarriedCloseNow(rig));

        await sibling.Received(1).SetCheckpointOffsetHintsAsync(Arg.Is<long[]>(h => h.Length == 1 && h[0] == 41L));
    }

    [Test]
    public async Task CarryingReader_a_close_recorded_after_the_reader_still_caps_the_cut()
    {
        // LA4: a mismatched cut is never accepted. The close lands between the cut and the
        // view the split evaluates it against; the reader did not carry it, so the hint stays
        // at the donor's frontier.
        var (_, _, sibling, _) = await PreArmSplitAsync(
            new WalSplitCut(Head: 42, ForeignHigh: 20, UnstampedWatermark: 25),
            frontier: 30,
            configure: rig => rig.Shard
                .When(s => s.GetSplitCutAsync(Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>()))
                .Do(_ => WitnessedCarriedCloseNow(rig)));

        await sibling.Received(1).SetCheckpointOffsetHintsAsync(Arg.Is<long[]>(h => h.Length == 1 && h[0] == 30L));
    }
}
