using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for member 2's guards on the WAL shard's routing-generation table (epic
/// #5605): a generation re-claim waits out every multi-entry batch stamped with that
/// initiator's generation (the design owner's answer Q4), so a batch straddling a
/// hand-off close is either wholly below the heads the close records or rejected
/// entry by entry, never allocated above the heads under the old generation.
/// </summary>
public partial class WalShardGrainTests
{
    [TestCase(1)]
    [TestCase(2)]
    public async Task A_generation_re_claim_never_splits_a_generation_stamped_batch(int kind)
    {
        // One entry per batch and one flush in flight: the batch's third entry waits
        // for the gated first flush, leaving a window between two of its entries.
        var gated = new GatedWalStorageProvider(new InMemoryWalStorageProvider());
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateGrainAsync(gated, new LatticeOptions { WalMaxBatchEntries = 1, WalMaxPendingBatches = 1 });
        grain.UseFenceStorageForTesting(storage);
        var initiator = new WalRoutingInitiator((WalRoutingInitiatorKind)kind, 0);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(initiator, 1, CancellationToken.None);
        var stamp = new WalAppendStamp(FenceLeaf, 1, initiator, 1);

        var batch = grain.AppendBatchStampedAsync(
            new[] { MakeEntry("a"), MakeEntry("b"), MakeEntry("c") }, stamp, CancellationToken.None);
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (await grain.GetNextSequenceAsync(CancellationToken.None) < 2 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5);
        }

        // The close's re-claim: everything allocated before its raise lies below the head.
        var reclaim = grain.ClaimRoutingGenerationAsync(initiator, 2, CancellationToken.None);
        var raisedEarly = await Task.WhenAny(reclaim, Task.Delay(TimeSpan.FromMilliseconds(250))) == reclaim;
        gated.Open();
        var result = await batch;
        var close = await reclaim;
        var late = await grain.AppendBatchStampedAsync(new[] { MakeEntry("d") }, stamp, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(raisedEarly, Is.False, "the re-claim waits until every entry of the in-flight batch is assigned");
            Assert.That(result.AdmittedCount, Is.EqualTo(3));
            Assert.That(close.Accepted, Is.True);
            Assert.That(result.Offsets, Is.All.LessThan(close.Head),
                "the straddling batch lies wholly below the heads the close records");
            Assert.That(late.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration),
                "a write admitted under the old generation and arriving after the close is rejected");
        });
    }

    [Test]
    public async Task A_generation_entry_is_unloaded_until_re_presented_after_a_restart()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var initiator = WalRoutingInitiator.TreeRangeDelete;
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(initiator, 3, CancellationToken.None);

        var restarted = await CreateFencedGrainAsync(storage);
        await restarted.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var unloaded = await restarted.AppendStampedAsync(MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, initiator, 3), CancellationToken.None);
        var staleRepresent = await restarted.ClaimRoutingGenerationAsync(initiator, 2, CancellationToken.None);
        var represent = await restarted.ClaimRoutingGenerationAsync(initiator, 3, CancellationToken.None);
        var admitted = await restarted.AppendStampedAsync(MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, initiator, 3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unloaded.Outcome, Is.EqualTo(WalAppendFenceOutcome.UnloadedGeneration));
            Assert.That(staleRepresent.Accepted, Is.False, "a stale generation cannot reload the entry");
            Assert.That(represent.Accepted, Is.True, "the current generation, already durable, reloads it");
            Assert.That(admitted.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
        });
    }
}
