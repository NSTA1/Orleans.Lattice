using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2207 on the orphan-repair path: once an orphan is unspliced it is on
/// neither the chain nor any routing table, so a clear that fails there cannot
/// be rediscovered by a later audit. It must be recorded as owed and retried by
/// the next repair.
/// </summary>
public sealed partial class ShardRootGrainOrphanRepairTests
{
    [Test]
    public async Task An_unspliced_orphan_whose_clear_failed_is_retried_by_the_next_repair()
    {
        var h = CreateHarness();
        h.B.ClearGrainStateAsync().Returns(
            _ => Task.FromException(new InvalidOperationException("storage unavailable")),
            _ => Task.CompletedTask);

        var first = await RepairAsync(h);

        Assert.That(first.Findings.Single().Disposition, Is.EqualTo(OrphanedLeafDisposition.Repaired),
            "the unsplice committed, so it is reported as repaired even though the clear failed");
        Assert.That(h.State.State.PendingLeafClears, Is.EqualTo(new[] { h.LeafB }));

        // B is off the chain now, so a second audit cannot find it.
        Assert.That(h.Probes[h.LeafA].NextSibling, Is.EqualTo(h.LeafC));

        var second = await RepairAsync(h);

        Assert.That(second.Findings, Is.Empty, "the retry is bookkeeping, not a new finding");
        await h.B.Received(2).ClearGrainStateAsync();
        Assert.That(h.State.State.PendingLeafClears, Is.Empty);
    }

    [Test]
    public async Task A_dry_run_does_not_retry_an_owed_clear()
    {
        // Inspection is read-only by contract, owed clears included.
        var h = CreateHarness();
        h.B.ClearGrainStateAsync().Returns(
            _ => Task.FromException(new InvalidOperationException("storage unavailable")),
            _ => Task.CompletedTask);
        await RepairAsync(h);

        await InspectAsync(h);

        await h.B.Received(1).ClearGrainStateAsync();
        Assert.That(h.State.State.PendingLeafClears, Is.EqualTo(new[] { h.LeafB }));
    }

    [Test]
    public async Task A_repair_retries_a_clear_owed_by_an_empty_leaf_fold()
    {
        // The record is shared: a clear owed by the reclaim path is equally
        // retried by an operator repair, which is the faster lever in an outage.
        var h = CreateHarness();
        var folded = GrainId.Create("leaf", "orphan-leaf-folded");
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.ClearGrainStateAsync().Returns(Task.CompletedTask);
        h.Leaves[folded] = leaf;
        h.State.State.PendingLeafClears.Add(folded);

        await RepairAsync(h);

        await leaf.Received(1).ClearGrainStateAsync();
        Assert.That(h.State.State.PendingLeafClears, Is.Empty);
    }
}
