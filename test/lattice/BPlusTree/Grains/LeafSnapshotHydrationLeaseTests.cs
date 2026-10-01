using NUnit.Framework;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LeafSnapshotHydrationLease"/>, the reservation a
/// leaf holds for as long as a snapshot hydration is materialising bytes.
/// <para>
/// The lease's whole job is to hand a reservation back exactly once, and the
/// three arms that enforce "exactly once" were cold: the ownerless
/// <see cref="LeafSnapshotHydrationLease.None"/> sentinel, the second
/// <c>Dispose</c>, and a <c>Reconcile</c> arriving after disposal. Each of them
/// fails silently rather than loudly - a double release drives the gate's
/// in-flight total negative, which reads as extra headroom and lets the gate
/// admit past its budget, which is precisely the condition it exists to
/// prevent.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotHydrationLeaseTests
{
    private const long BudgetBytes = 1_000L;
    private const long StoredBytes = 100L;

    [Test]
    public void None_reserves_nothing_and_reports_neither_queued_nor_exclusive()
    {
        var none = LeafSnapshotHydrationLease.None;

        Assert.That(none, Is.Not.Null);
        Assert.That(none.HeldBytes, Is.Zero);
        Assert.That(none.Queued, Is.False);
        Assert.That(none.Exclusive, Is.False);
        Assert.That(none.ContiguousBytes, Is.Zero);
    }

    [Test]
    public void None_is_a_singleton_so_the_no_gate_paths_allocate_nothing()
    {
        Assert.That(LeafSnapshotHydrationLease.None, Is.SameAs(LeafSnapshotHydrationLease.None));
    }

    [Test]
    public void Disposing_None_is_a_no_op_so_callers_need_no_null_checks()
    {
        // The sentinel is handed to paths that never reserve - a leaf with no
        // tree id, or one that cannot address a snapshot grain - and those
        // paths dispose it unconditionally.
        Assert.That(
            () =>
            {
                LeafSnapshotHydrationLease.None.Dispose();
                LeafSnapshotHydrationLease.None.Dispose();
            },
            Throws.Nothing);

        Assert.That(LeafSnapshotHydrationLease.None.HeldBytes, Is.Zero);
    }

    [Test]
    public void Reconciling_None_is_a_no_op()
    {
        LeafSnapshotHydrationLease.None.Reconcile(999_999L);

        Assert.That(
            LeafSnapshotHydrationLease.None.HeldBytes,
            Is.Zero,
            "an ownerless lease has no gate to correct, so a measurement must be dropped");
    }

    [Test]
    public async Task A_lease_releases_its_reservation_on_dispose()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        var lease = await admission.AcquireAsync(StoredBytes, CancellationToken.None);

        Assert.That(lease.HeldBytes, Is.EqualTo(LeafSnapshotHydrationAdmission.ToHeapCostBytes(StoredBytes)));
        Assert.That(admission.InFlightBytes, Is.EqualTo(lease.HeldBytes));

        lease.Dispose();

        Assert.That(admission.InFlightBytes, Is.Zero);
        Assert.That(admission.AdmittedCount, Is.Zero);
    }

    [Test]
    public async Task A_second_dispose_does_not_release_the_reservation_twice()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        var lease = await admission.AcquireAsync(StoredBytes, CancellationToken.None);

        lease.Dispose();
        lease.Dispose();

        Assert.That(
            admission.InFlightBytes,
            Is.Zero,
            "a double release would drive the in-flight total negative, which reads as extra headroom");
        Assert.That(
            admission.AdmittedCount,
            Is.Zero,
            "a double release would drive the admitted count negative and defeat the sole-occupant rule");
    }

    [Test]
    public async Task A_second_dispose_cannot_release_a_concurrent_lease_reservation()
    {
        // The negative-accounting failure is only visible once another lease is
        // in flight: a second release subtracts that lease's budget rather than
        // its own, so the gate reports headroom it does not have.
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        var first = await admission.AcquireAsync(StoredBytes, CancellationToken.None);
        using var second = await admission.AcquireAsync(StoredBytes, CancellationToken.None);

        var expected = second.HeldBytes;

        first.Dispose();
        first.Dispose();

        Assert.That(admission.InFlightBytes, Is.EqualTo(expected));
        Assert.That(admission.AdmittedCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Reconcile_corrects_the_reservation_to_the_measured_size()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        using var lease = await admission.AcquireAsync(StoredBytes, CancellationToken.None);

        lease.Reconcile(StoredBytes * 2);

        var expected = LeafSnapshotHydrationAdmission.ToHeapCostBytes(StoredBytes * 2);
        Assert.That(lease.HeldBytes, Is.EqualTo(expected));
        Assert.That(admission.InFlightBytes, Is.EqualTo(expected));
    }

    [Test]
    public async Task Reconcile_updates_the_contiguous_figure_it_reports()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        using var lease = await admission.AcquireAsync(StoredBytes, CancellationToken.None);

        lease.Reconcile(StoredBytes * 3);

        Assert.That(
            lease.ContiguousBytes,
            Is.EqualTo(LeafSnapshotHydrationAdmission.ToContiguousBytes(StoredBytes * 3)),
            "the figure reported on an out-of-memory failure must describe the allocation that actually ran");
    }

    [Test]
    public async Task Reconcile_after_dispose_is_dropped()
    {
        // The hydration path reconciles when the true size is known and
        // disposes when it finishes; a fault between the two can invert that
        // order. A reconcile applied after the release would re-add bytes to a
        // gate that has already given them back, leaking the difference for the
        // life of the silo.
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        var lease = await admission.AcquireAsync(StoredBytes, CancellationToken.None);

        lease.Dispose();
        Assert.That(admission.InFlightBytes, Is.Zero);

        lease.Reconcile(StoredBytes * 10);

        Assert.That(
            admission.InFlightBytes,
            Is.Zero,
            "a measurement that arrives after the lease was released must be dropped");
        Assert.That(admission.AdmittedCount, Is.Zero);
    }

    [Test]
    public async Task A_lease_taken_without_waiting_reports_Queued_false()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        using var lease = await admission.AcquireAsync(StoredBytes, CancellationToken.None);

        Assert.That(
            lease.Queued,
            Is.False,
            "'the gate is deployed and nothing queued' must stay distinguishable from 'the gate is not deployed'");
    }
}
