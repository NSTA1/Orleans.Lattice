using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the <see cref="LeafSnapshotSaveOutcome"/> that
/// <see cref="LeafSnapshotStorageGrain.SaveAsync"/> reports (issue #3421).
/// <para>
/// The leaf records an offered blob's coverage as durable - the figure that
/// sets its WAL materialiser pin and so licenses the coverage-gated WAL GC to
/// trim - only when this outcome is <see cref="LeafSnapshotSaveOutcome.Kept"/>.
/// So <c>Kept</c> must mean "the durable blob now covers the offer in every
/// partition", and every refusal must say <see cref="LeafSnapshotSaveOutcome.Declined"/>.
/// A refusal reported as <c>Kept</c> is the defect itself; an acceptance
/// reported as <c>Declined</c> freezes the leaf's WAL pin forever. Both
/// directions are pinned here.
/// </para>
/// </summary>
public sealed partial class LeafSnapshotStorageGrainTests
{
    [Test]
    public async Task SaveAsync_reports_kept_for_the_first_snapshot()
    {
        var (grain, state) = CreateGrain();

        var outcome = await grain.SaveAsync(CompactionBlob(10L, 5L, CompactionLiveRow("k", [1], 100L)), CancellationToken.None);

        Assert.That(outcome, Is.EqualTo(LeafSnapshotSaveOutcome.Kept),
            "a first snapshot is persisted verbatim, so the leaf may record its coverage");
        Assert.That(state.WriteCount, Is.EqualTo(1), "and it really was written");
    }

    [Test]
    public async Task SaveAsync_reports_kept_for_a_capture_that_advances_every_partition()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(CompactionBlob(10L, 5L, CompactionLiveRow("k", [1], 100L)), CancellationToken.None);

        var outcome = await grain.SaveAsync(CompactionBlob(20L, 7L, CompactionLiveRow("k", [2], 200L)), CancellationToken.None);

        Assert.That(outcome, Is.EqualTo(LeafSnapshotSaveOutcome.Kept),
            "the non-regressing fast path persists the offer verbatim");
        Assert.That(state.State.SnapshotOffsetsByPartition, Is.EqualTo(new long[] { 20L, 7L }),
            "and the durable coverage is exactly the offer's");
    }

    [Test]
    public async Task SaveAsync_reports_kept_for_a_regressing_capture_that_merges()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(CompactionBlob(100L, 50L, CompactionLiveRow("k", [1], 900L)), CancellationToken.None);

        var outcome = await grain.SaveAsync(CompactionBlob(200L, 40L, CompactionLiveRow("k", [4], 200L)), CancellationToken.None);

        Assert.That(outcome, Is.EqualTo(LeafSnapshotSaveOutcome.Kept),
            "a regressing capture that carries every stored key merges element-wise; the merged "
            + "coverage is >= the offer in every partition, so recording the offer is conservative");
        Assert.That(state.State.SnapshotOffsetsByPartition, Is.EqualTo(new long[] { 200L, 50L }),
            "precondition: the merge really took the element-wise max");
    }

    [Test]
    public async Task SaveAsync_reports_declined_when_a_regressing_capture_omits_a_stored_key()
    {
        var (grain, state) = CreateGrain();
        var stored = CompactionBlob(100L, 50L, CompactionLiveRow("k", [1], 900L));
        await grain.SaveAsync(stored, CancellationToken.None);
        var writesBefore = state.WriteCount;

        var outcome = await grain.SaveAsync(CompactionBlob(200L, 40L), CancellationToken.None);

        Assert.That(outcome, Is.EqualTo(LeafSnapshotSaveOutcome.Declined),
            "the store kept the stored blob verbatim, which covers partition 0 only to 100 - reporting "
            + "Kept would let the leaf claim 200 as durable and the WAL GC trim a prefix nothing holds");
        Assert.Multiple(() =>
        {
            Assert.That(state.State, Is.SameAs(stored), "precondition: the stored blob was kept verbatim");
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore), "and nothing was written");
        });
    }

    [Test]
    public async Task SaveAsync_reports_declined_for_an_unreadable_payload()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(CompactionBlob(10L, 5L, CompactionLiveRow("k", [1], 100L)), CancellationToken.None);
        var writesBefore = state.WriteCount;

        var outcome = await grain.SaveAsync(
            CompactionBlob(20L, 7L, new LeafSnapshotRow(null!, CompactionLiveRow("x", [1], 1L).Value)),
            CancellationToken.None);

        Assert.That(outcome, Is.EqualTo(LeafSnapshotSaveOutcome.Declined),
            "an offer whose row payload does not read back is refused, and must say so");
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore), "nothing was written");
            Assert.That(state.State.SnapshotOffsetsByPartition, Is.EqualTo(new long[] { 10L, 5L }),
                "and the durable coverage is still the earlier blob's");
        });
    }

    [Test]
    public async Task SaveAsync_reports_declined_for_an_unreadable_first_snapshot()
    {
        var (grain, state) = CreateGrain();

        var outcome = await grain.SaveAsync(
            CompactionBlob(20L, 7L, new LeafSnapshotRow(null!, CompactionLiveRow("x", [1], 1L).Value)),
            CancellationToken.None);

        Assert.That(outcome, Is.EqualTo(LeafSnapshotSaveOutcome.Declined),
            "there is no snapshot at all after a refused first save, so nothing may be recorded as durable");
        Assert.That(state.WriteCount, Is.Zero, "and nothing was written");
    }

    [Test]
    public async Task SaveAsync_reports_kept_when_the_stored_instance_is_re_offered()
    {
        var (grain, state) = CreateGrain();
        var stored = CompactionBlob(10L, 5L, CompactionLiveRow("k", [1], 100L));
        await grain.SaveAsync(stored, CancellationToken.None);
        Assert.That(state.State, Is.SameAs(stored), "precondition: the grain holds the very instance saved");
        var writesBefore = state.WriteCount;

        var outcome = await grain.SaveAsync(stored, CancellationToken.None);

        Assert.That(outcome, Is.EqualTo(LeafSnapshotSaveOutcome.Kept),
            "re-offering the instance already held is not a refusal: the durable blob IS the offer, so "
            + "reporting Declined would freeze the caller's pin for no reason");
        Assert.That(state.WriteCount, Is.EqualTo(writesBefore), "and there is nothing to write");
    }
}
