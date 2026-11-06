using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A witnessed donor close's carriage on its sibling (a5e2e2ef's G4 and A1-A6 on #5866):
/// the witness ends the bound but not the carriage; the sibling's stamps keep the close at
/// each WAL partition until an arm there re-claims the close's shard's generation at or
/// above the witness and records the re-claim head with the resolved bit in one marker
/// write. The tick's backstop arms an idle carried partition once per (partition, witness),
/// re-arms after a failed write, and never loops on a close it cannot resolve.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const long CarryWitness = 10;

    private const long CarryReclaimHead = 40;

    private static LiveAdvanceRig CarryRig(int partitions)
    {
        var rig = new LiveAdvanceRig(partitions);
        rig.FenceGate.IsPreArmOpen = true;
        return rig;
    }

    /// <summary>
    /// From here on, every arm is accepted and a re-claim returns <see cref="CarryReclaimHead"/>;
    /// until then the substitute refuses, so the advance's own arm cannot resolve the
    /// carriage before the backstop under test runs.
    /// </summary>
    private static void AcceptCarryArms(LiveAdvanceRig rig) =>
        rig.Shard.ArmAppendEpochAsync(rig.LeafId, Arg.Any<long>(), Arg.Any<Guid>(), Arg.Any<WalRoutingInitiator>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var initiator = call.ArgAt<WalRoutingInitiator>(3);
                return Task.FromResult(new WalAppendFenceArmResult(
                    true, 5, 0, -1, initiator.IsSet ? CarryReclaimHead : -1, initiator.IsSet ? call.ArgAt<long>(4) : 0));
            });

    /// <summary>Records on the rig leaf's own marker a donor close of shard 0 whose link shard <paramref name="linkedShard"/> witnessed.</summary>
    private static Task WitnessedCarriedCloseAsync(LiveAdvanceRig rig, int linkedShard = 0) =>
        rig.SetLeafMarkerAsync(m =>
        {
            m.Initialised = true;
            HandOffMarkerCore.Open(m, "split:donor", null, null);
            HandOffMarkerCore.CloseWithDeferredBound(m, "split:donor", new long[rig.Partitions], ClaimlessDonor, ClaimlessDonorToken, 0, 1);
            return HandOffMarkerCore.WitnessLink(m, ClaimlessDonor, linkedShard, CarryWitness);
        });

    private static async Task<int> CarryArmCallsAsync(LiveAdvanceRig rig)
    {
        await Task.Yield();
        return rig.Shard.ReceivedCalls().Count(c =>
            c.GetMethodInfo().Name == nameof(IWalShardGrain.ArmAppendEpochAsync)
            && c.GetArguments()[3] is WalRoutingInitiator { IsSet: true });
    }

    [Test]
    public async Task A_witnessed_close_unarmed_at_a_partition_is_still_carried_by_its_stamps()
    {
        // G4 detector 1 (ReadPositionHonestWitnessEndsDeferredHead): the witness ends the
        // bound only; until the sibling's arm at the partition, its stamps carry the close.
        var rig = CarryRig(partitions: 1);
        var grain = CreateGrain(liveAdvance: rig);
        await WitnessedCarriedCloseAsync(rig);
        await SetStampedAsync(grain, "a", "1");

        await grain.TryAdvanceLiveCheckpointAsync();
        await SetStampedAsync(grain, "b", "2");

        var fence = grain.AppendFenceForTesting!;
        Assert.Multiple(() =>
        {
            Assert.That(fence.HasPendingCloses, Is.False, "a witnessed bound stayed pending");
            Assert.That(fence.IsCarriedAtForTesting(0), Is.True, "the witness ended the carriage before the sibling's arm");
            Assert.That(fence.TryGetCutReader(out var reader) && reader.Close1.IsSet, Is.True, "the sibling's stamps dropped a close its arm never resolved");
        });
    }

    [Test]
    public async Task The_backstop_arms_a_binding_carried_partition_once_and_resolves_it_with_the_re_claim_head()
    {
        // G4 detector 4 (A2, A4 as amended by LA1): partition 1 carries the close and holds
        // an own write the advance cannot pass, held by a raised bound its covering report
        // carried. The first tick arms it, re-claiming the witness; the marker records that
        // re-claim's head and the resolved bit. A later tick does not arm again.
        var rig = CarryRig(partitions: 2);
        var grain = CreateGrain(liveAdvance: rig);
        await WitnessedCarriedCloseAsync(rig);
        await SetStampedAsync(grain, KeyOnPartition(0, 2, "p0-"), "1");
        await grain.TryAdvanceLiveCheckpointAsync();
        rig.ReportedWatermark = rig.Writer.ForeignAppend(1);
        await SetStampedAsync(grain, KeyOnPartition(1, 2, "p1-"), "1");
        await grain.TryAdvanceLiveCheckpointAsync();
        AcceptCarryArms(rig);
        var baseline = await CarryArmCallsAsync(rig);

        var armedFirst = await grain.TryBackstopCarriedClosesAsync();
        var marker = await rig.LeafMarkerAsync();
        var callsAfterFirst = await CarryArmCallsAsync(rig);
        var armedAgain = await grain.TryBackstopCarriedClosesAsync();
        var callsAfterSecond = await CarryArmCallsAsync(rig);

        var close = marker.Closes.Single();
        Assert.Multiple(() =>
        {
            Assert.That(armedFirst, Is.True, "the first tick after the witness did not arm the carried partition");
            Assert.That(callsAfterFirst, Is.GreaterThan(baseline), "the backstop made no re-claim");
            Assert.That(close.CarryResolvedAt.Length, Is.GreaterThanOrEqualTo(2));
            Assert.That(close.CarryResolvedAt.ElementAtOrDefault(1), Is.True, "the backstop's arm did not resolve the binding partition");
            Assert.That(close.Heads[1], Is.EqualTo(CarryReclaimHead), "the resolved partition's head is not the arm's own re-claim head");
            Assert.That(armedAgain, Is.False, "the backstop armed again for the same witness");
            Assert.That(callsAfterSecond, Is.EqualTo(callsAfterFirst), "a later tick re-claimed a resolved partition");
        });
        await rig.Shard.Received().ArmAppendEpochAsync(
            rig.LeafId, Arg.Any<long>(), Arg.Any<Guid>(), WalRoutingInitiator.ShardRoot(0), Arg.Is<long>(g => g >= CarryWitness), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_failed_resolve_write_leaves_the_partition_carried_and_the_backstop_arms_it_again()
    {
        // G4 detector 5 (A3): the arm's re-claim lands but the marker write recording its
        // head and resolved bit fails; the partition stays carried, and the next tick re-arms.
        var rig = CarryRig(partitions: 1);
        var grain = CreateGrain(liveAdvance: rig);
        await WitnessedCarriedCloseAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        await grain.TryAdvanceLiveCheckpointAsync();
        rig.ReportedWatermark = rig.Writer.ForeignAppend();
        await SetStampedAsync(grain, "b", "2");
        await grain.TryAdvanceLiveCheckpointAsync();
        AcceptCarryArms(rig);
        var baseline = await CarryArmCallsAsync(rig);
        var failWrites = true;
        rig.OnStorageWrite = (stateName, _) =>
        {
            if (failWrites && stateName == HandOffMarkerRows.LeafGrainType)
            {
                throw new InvalidOperationException("injected marker write failure");
            }
        };

        var armedFailing = await grain.TryBackstopCarriedClosesAsync();
        var afterFailure = (await rig.LeafMarkerAsync()).Closes.Single();
        var resolvedAfterFailure = afterFailure.CarryResolvedAt.ElementAtOrDefault(0);
        var headAfterFailure = afterFailure.Heads[0];
        var callsAfterFailure = await CarryArmCallsAsync(rig);
        failWrites = false;
        var armedRetry = await grain.TryBackstopCarriedClosesAsync();
        var afterRetry = (await rig.LeafMarkerAsync()).Closes.Single();

        Assert.Multiple(() =>
        {
            Assert.That(callsAfterFailure, Is.GreaterThan(baseline), "precondition: the backstop's arm re-claimed");
            Assert.That(armedFailing, Is.False, "a failed resolve write was reported as landed");
            Assert.That(resolvedAfterFailure, Is.False, "a failed resolve write resolved the carriage");
            Assert.That(headAfterFailure, Is.Zero, "a failed resolve write raised the head");
            Assert.That(armedRetry, Is.True, "the backstop did not re-arm after a failed write");
            Assert.That(afterRetry.CarryResolvedAt.ElementAtOrDefault(0), Is.True, "the retry did not resolve the carriage");
            Assert.That(afterRetry.Heads[0], Is.EqualTo(CarryReclaimHead));
        });
    }

    [Test]
    public async Task A_close_witnessed_by_another_shard_root_stays_carried_and_the_backstop_does_not_loop_on_it()
    {
        // A1: the witness's shard is not the close's admitted shard. The close stays carried,
        // and no tick arms for it or re-reads the marker for it.
        var rig = CarryRig(partitions: 1);
        var grain = CreateGrain(liveAdvance: rig);
        await WitnessedCarriedCloseAsync(rig, linkedShard: 1);
        await SetStampedAsync(grain, "a", "1");
        await grain.TryAdvanceLiveCheckpointAsync();
        AcceptCarryArms(rig);
        var baseline = await CarryArmCallsAsync(rig);
        var markerReads = 0;
        rig.OnStorageRead = stateName =>
        {
            if (stateName == HandOffMarkerRows.LeafGrainType)
            {
                markerReads++;
            }
        };

        var armed = false;
        for (var tick = 0; tick < 3; tick++)
        {
            armed |= await grain.TryBackstopCarriedClosesAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(grain.AppendFenceForTesting!.IsCarriedAtForTesting(0), Is.True, "a shard mismatch ended the carriage");
            Assert.That(armed, Is.False, "the backstop armed for a close it cannot resolve");
            Assert.That(markerReads, Is.Zero, "the backstop looped on a close it cannot resolve");
        });
        Assert.That(await CarryArmCallsAsync(rig), Is.EqualTo(baseline), "the backstop re-claimed for a shard-mismatched witness");
    }
}
