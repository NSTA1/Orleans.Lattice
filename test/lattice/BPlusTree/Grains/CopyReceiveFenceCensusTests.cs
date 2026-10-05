using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="CopyReceiveFenceCensus"/> (issue #4593), the
/// per-silo census behind the closed-copy age gauge, and for
/// <see cref="ReplicationApplyScope"/>, the flow mark that makes routing check a
/// copy's receive fence.
/// </summary>
[TestFixture]
public sealed class CopyReceiveFenceCensusTests
{
    [Test]
    public void A_closed_copy_reports_its_age_tagged_by_physical_tree()
    {
        var copy = $"census-copy-{Guid.NewGuid():N}";
        var closedAt = DateTime.UtcNow.AddMinutes(-5).Ticks;
        CopyReceiveFenceCensus.Enrol(copy, closedAt);
        try
        {
            var measurement = CopyReceiveFenceCensus.Observe()
                .Single(m => m.Tags.ToArray().Any(t => t.Key == LatticeMetrics.TagTree && Equals(t.Value, copy)));

            Assert.That(measurement.Value, Is.GreaterThanOrEqualTo(300).And.LessThan(400));
            Assert.That(CopyReceiveFenceCensus.Gauge.Name, Is.EqualTo(LatticeMetrics.CopyReceiveClosedAgeGaugeName));
        }
        finally
        {
            CopyReceiveFenceCensus.Withdraw(copy, closedAt);
        }
    }

    [Test]
    public void A_withdrawn_copy_is_no_longer_reported()
    {
        var copy = $"census-copy-{Guid.NewGuid():N}";
        var closedAt = DateTime.UtcNow.Ticks;
        CopyReceiveFenceCensus.Enrol(copy, closedAt);

        CopyReceiveFenceCensus.Withdraw(copy, closedAt);

        Assert.That(CopyReceiveFenceCensus.IsEnrolled(copy), Is.False);
        Assert.That(
            CopyReceiveFenceCensus.Observe().Any(m => m.Tags.ToArray().Any(t => Equals(t.Value, copy))),
            Is.False);
    }

    [Test]
    public void A_stale_withdrawal_does_not_remove_a_newer_enrolment()
    {
        var copy = $"census-copy-{Guid.NewGuid():N}";
        CopyReceiveFenceCensus.Enrol(copy, 100);
        CopyReceiveFenceCensus.Enrol(copy, 200);
        try
        {
            CopyReceiveFenceCensus.Withdraw(copy, 100);

            Assert.That(CopyReceiveFenceCensus.IsEnrolled(copy), Is.True);
        }
        finally
        {
            CopyReceiveFenceCensus.Withdraw(copy, 200);
        }
    }

    [Test]
    public async Task The_replication_apply_mark_is_scoped_to_the_async_method_that_sets_it()
    {
        Assert.That(ReplicationApplyScope.IsActive, Is.False);

        var seenInside = await MarkAndObserveAsync();

        Assert.Multiple(() =>
        {
            Assert.That(seenInside, Is.True, "the mark is visible inside the marking method");
            Assert.That(ReplicationApplyScope.IsActive, Is.False, "the mark never leaks into the caller");
        });
    }

    private static async Task<bool> MarkAndObserveAsync()
    {
        ReplicationApplyScope.Enter();
        await Task.Yield();
        return await ObserveAsync();
    }

    private static async Task<bool> ObserveAsync()
    {
        await Task.Yield();
        return ReplicationApplyScope.IsActive;
    }

    [Test]
    public void The_refusal_names_the_tree_and_the_closed_copy()
    {
        var ex = new CopyReceiveFencedException("orders", "orders-shadow");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("orders"));
            Assert.That(ex.PhysicalTreeId, Is.EqualTo("orders-shadow"));
            Assert.That(ex.Message, Does.Contain("orders-shadow"));
            Assert.That(ex.AdmittedBeforeRestore, Is.False);
            Assert.That(new CopyReceiveFencedException("orders", "orders-shadow", admittedBeforeRestore: true).AdmittedBeforeRestore, Is.True);
            Assert.That(new CopyReceiveFencedException().TreeId, Is.Empty);
        });
    }

    [Test]
    public async Task The_admission_stamp_names_the_admitting_tree_and_epoch_and_is_flow_scoped()
    {
        Assert.That(Orleans.Lattice.BPlusTree.ReplicationAdmissionEpoch.TryGet(out _, out _), Is.False);

        var (tree, epoch) = await StampAndReadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tree, Is.EqualTo("orders"));
            Assert.That(epoch, Is.EqualTo(4));
            Assert.That(Orleans.Lattice.BPlusTree.ReplicationAdmissionEpoch.TryGet(out _, out _), Is.False,
                "the stamp never leaks into the caller");
        });
    }

    private static async Task<(string Tree, long Epoch)> StampAndReadAsync()
    {
        Orleans.Lattice.BPlusTree.ReplicationAdmissionEpoch.Stamp("orders", 4);
        await Task.Yield();
        Orleans.Lattice.BPlusTree.ReplicationAdmissionEpoch.TryGet(out var tree, out var epoch);
        return (tree, epoch);
    }
}
