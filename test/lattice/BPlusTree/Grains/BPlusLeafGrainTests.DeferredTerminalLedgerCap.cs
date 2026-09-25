using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    public void Deferred_terminal_cap_description_distinguishes_ledger_records_from_retained_terminals()
    {
        var instrument = LatticeMetrics.LeafDeferredTerminalsDroppedAtCap;
        Assert.Multiple(() =>
        {
            Assert.That(instrument.Name, Is.EqualTo("orleans.lattice.leaf.deferred_terminals_dropped_at_cap"));
            Assert.That(instrument.Unit, Is.EqualTo("{terminal}"));
            Assert.That(instrument.Description, Does.StartWith("Durable ledger records"));
            Assert.That(instrument.Description, Does.Contain("terminals themselves are retained"));
            Assert.That(instrument.Description, Does.Contain("pass 2"));
        });
    }

    [TestCase(1)]
    [TestCase(LatticeOptions.DefaultMaxDurableUnresolvedReplayWork)]
    public async Task Deferred_terminal_at_cap_is_retained_and_drained_by_pass_two(int cap)
    {
        var state = NewFlushCeilingState();
        var p0Entries = new CommitLogSliceEntry[cap + 2];
        for (var i = 0; i < cap; i++)
        {
            p0Entries[i] = new CommitLogSliceEntry(i + 1,
                BuildDeleteRange("a0", "a9", hlcPhysical: 500, treeId: FlushCeilingTreeId));
        }
        // Only the refused terminal targets m5. Applying the ledger alone cannot pass this test.
        var refusedOffset = cap + 1;
        p0Entries[cap] = FlushDeleteRange(refusedOffset);
        p0Entries[cap + 1] = FlushSet(cap + 2, "z0");

        var p1Entries = new CommitLogSliceEntry[cap + 3];
        p1Entries[0] = FlushSet(1, "m5");
        for (var i = 1; i < p1Entries.Length; i++)
            p1Entries[i] = FlushSet(i + 1, "z1");

        var sawSaturatedLedger = false;
        var p0 = BuildObservableCoordinator(
            head: p0Entries.Length + 1, sliceSize: 128, tail: 0, onRead: null, p0Entries);
        var p1 = BuildObservableCoordinator(
            head: p1Entries.Length + 1, sliceSize: 128, tail: 0,
            onRead: read =>
            {
                if (read != 1)
                    return;

                Assert.Multiple(() =>
                {
                    Assert.That(state.State.UnresolvedReplayWork, Has.Count.EqualTo(cap));
                    Assert.That(state.State.UnresolvedReplayWork!.Any(
                        e => e.Partition == 0 && e.Offset == refusedOffset), Is.False);
                    Assert.That(state.State.ProjectionCheckpointOffset, Is.LessThan(refusedOffset));
                });
                sawSaturatedLedger = true;
            }, p1Entries);
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(state, [p0, p1], store.Stub, maxDurableUnresolvedReplayWork: cap);

        var measurements = await RecordMeasurementsAsync(
            LatticeMetrics.LeafDeferredTerminalsDroppedAtCap,
            () => LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(sawSaturatedLedger, Is.True, "Pass 1 must reach the positive, saturated cap.");
            Assert.That(measurements
                .Where(m => Equals(m.Tags[LatticeMetrics.TagTree], FlushCeilingTreeId)
                    && Equals(m.Tags[LatticeMetrics.TagPartition], 0))
                .Sum(m => m.Value), Is.EqualTo(1L), "Exactly one durable record was refused.");
            Assert.That(state.State.UnresolvedReplayWork ?? [], Is.Empty,
                "Pass 2 drains the admitted ledger records, not just the in-memory refusal.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(p0Entries.Length));
        });
        Assert.That(await grain.GetAsync("m5"), Is.Null,
            "The terminal refused by the ledger must remain in deferredTerminals and delete the later partition's key.");
        Assert.That(await grain.GetAsync("z1"), Is.Not.Null, "The later partition really was applied.");
    }
}
