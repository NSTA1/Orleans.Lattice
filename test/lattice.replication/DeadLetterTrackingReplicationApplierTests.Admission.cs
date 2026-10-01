using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for issue #4021: the decorator's single-entry and per-entry paths recorded
/// inbound contact for a tree the receiver does not admit. The inner applier drops such an entry
/// with Applied=false, which counted as success, so a peer's chosen <c>(tree, origin)</c> pair
/// landed in <see cref="ReplicationPeerStats"/> and in the peer-status report. Contact is now
/// recorded only for an admitted run.
/// </summary>
public partial class DeadLetterTrackingReplicationApplierTests
{
    private static ApplyResult Dropped() => new() { Applied = false, HighWaterMark = HybridLogicalClock.Zero };

    private static readonly Dictionary<string, LatticeMergeMode> OtherTreeOnly = new() { ["other-tree"] = LatticeMergeMode.LwwRegister };

    [Test]
    public async Task ApplyBatchAsync_single_entry_does_not_record_contact_for_a_tree_not_enrolled_here()
    {
        var (decorator, inner, stats) = BuildWithStats(replicatedTrees: OtherTreeOnly);
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(Dropped());

        await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None);

        Assert.That(stats.Snapshot(), Is.Empty);
    }

    [Test]
    public async Task ApplyBatchAsync_slow_path_does_not_record_contact_for_a_tree_not_enrolled_here()
    {
        var (decorator, inner, stats) = BuildWithStats(replicatedTrees: OtherTreeOnly);
        inner.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns<Task<ApplyResult>>(_ => throw new InvalidOperationException("batch boom"));
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Dropped());

        await decorator.ApplyBatchAsync(new[] { MakeEntry("a"), MakeEntry("b") }, CancellationToken.None);

        Assert.That(stats.Snapshot(), Is.Empty);
    }

    [Test]
    public async Task ApplyBatchAsync_does_not_record_contact_for_many_peer_chosen_tree_ids()
    {
        var (decorator, inner, stats) = BuildWithStats(replicatedTrees: OtherTreeOnly);
        inner.ApplyAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Dropped());

        for (var i = 0; i < 50; i++)
        {
            await decorator.ApplyBatchAsync(new[] { MakeEntry("a") with { TreeId = "planted-" + i } }, CancellationToken.None);
        }

        Assert.That(stats.Snapshot(), Is.Empty);
        Assert.That(stats.InboundRowCount, Is.Zero);
    }

    [Test]
    public async Task ApplyBatchAsync_does_not_record_contact_for_a_wire_mode_the_gate_rejects()
    {
        var mismatched = new Dictionary<string, LatticeMergeMode> { [TreeId] = LatticeMergeMode.OrSet };
        var (decorator, inner, stats) = BuildWithStats(replicatedTrees: mismatched);
        var entry = MakeEntry("a");
        Assume.That(entry.Mode, Is.Not.EqualTo(LatticeMergeMode.OrSet));
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(Dropped());

        await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None);

        Assert.That(stats.Snapshot(), Is.Empty);
    }

    [Test]
    public async Task ApplyBatchAsync_without_an_enrollment_source_records_no_contact()
    {
        var (decorator, inner, stats) = BuildWithStats(enroll: false);
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(Applied(entry));

        await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None);

        Assert.That(stats.Snapshot(), Is.Empty, "fail closed: with no enrollment source nothing is admitted");
    }

    [Test]
    public async Task ApplyBatchAsync_resolves_enrollment_through_the_replication_context()
    {
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode(TreeId).Returns(MakeEntry().Mode);
        var (decorator, inner, stats) = BuildWithStats(enroll: false, context: context);
        var entry = MakeEntry("a");
        inner.ApplyAsync(entry, Arg.Any<CancellationToken>()).Returns(Applied(entry));

        await decorator.ApplyBatchAsync(new[] { entry }, CancellationToken.None);

        Assert.That(Inbound(stats), Is.Not.Null, "a tree the context enrolls is admitted and attributed");
    }
}
