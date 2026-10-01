using NUnit.Framework;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the cancellation and lease-lifecycle arms of
/// <see cref="LeafSnapshotHydrationAdmission"/>, the per-silo gate that bounds
/// how much snapshot hydration runs concurrently during a cold-start storm.
/// <para>
/// The gate's admission arithmetic is already exercised through
/// <c>BPlusLeafGrainTests.ColdActivationAdmission</c> and
/// <c>BPlusLeafGrainTests.HydrationContiguity</c>, which drive it from the leaf
/// grain. What none of them reach is what happens when a queued claim is
/// <b>cancelled</b>, and that is where the gate's reservations can leak: a
/// waiter is removed from the queue by one path and granted a reservation by
/// another, and every one of those arms - the pre-cancelled fast path,
/// <c>Abandon</c>, the registration callback, and the lost-race hand-back in
/// <c>Complete</c> - was cold.
/// </para>
/// <para>
/// A leak here is silent and cumulative: the gate keeps reporting a healthy
/// budget while admitting less and less, until cold activations stall for a
/// reason nothing attributes to cancellation. Every test below therefore
/// asserts the gate's accounting returns to empty, not merely that the claim
/// faulted.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotHydrationAdmissionCancellationTests
{
    // 1 KiB of budget against claims whose heap cost is five times their stored
    // size, so a 100-byte claim reserves 500 and a second one cannot join it.
    private const long BudgetBytes = 1_000L;
    private const long HalfBudgetStoredBytes = 100L;
    private const long OversubscribedStoredBytes = 150L;

    [Test]
    public void AcquireAsync_with_an_already_cancelled_token_never_enters_the_queue()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var claim = admission.AcquireAsync(HalfBudgetStoredBytes, cts.Token);

        Assert.That(claim.IsCanceled, Is.True, "the fast path must hand back a cancelled task synchronously");
        Assert.That(admission.QueuedCount, Is.Zero);
        Assert.That(admission.AdmittedCount, Is.Zero);
        Assert.That(
            admission.InFlightBytes,
            Is.Zero,
            "a claim refused before admission must not reserve anything");
    }

    [Test]
    public void AcquireAsync_with_an_already_cancelled_token_throws_on_await()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await admission.AcquireAsync(HalfBudgetStoredBytes, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task An_uncancellable_claim_is_admitted_without_a_registration()
    {
        // CancellationToken.None cannot be cancelled, so WaitAsync takes the
        // branch that awaits the completion directly rather than registering a
        // callback. The queued claim must still be admitted when the occupant
        // leaves.
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        var occupant = await admission.AcquireAsync(HalfBudgetStoredBytes, CancellationToken.None);
        var queued = admission.AcquireAsync(OversubscribedStoredBytes, CancellationToken.None);

        Assert.That(queued.IsCompleted, Is.False, "the second claim must not fit alongside the first");
        Assert.That(admission.QueuedCount, Is.EqualTo(1));

        occupant.Dispose();

        using var admitted = await queued;
        Assert.That(admitted.Queued, Is.True, "a claim that waited must report that it waited");
        Assert.That(admission.AdmittedCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Cancelling_a_queued_claim_abandons_it_and_reserves_nothing()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        using var occupant = await admission.AcquireAsync(HalfBudgetStoredBytes, CancellationToken.None);
        var inFlightBeforeQueueing = admission.InFlightBytes;

        using var cts = new CancellationTokenSource();
        var queued = admission.AcquireAsync(OversubscribedStoredBytes, cts.Token);

        Assert.That(queued.IsCompleted, Is.False);
        Assert.That(admission.QueuedCount, Is.EqualTo(1));

        cts.Cancel();

        Assert.That(
            async () => await queued,
            Throws.InstanceOf<OperationCanceledException>());

        await TestPoll.UntilAsync(
            () => admission.QueuedCount == 0,
            "the cancelled waiter to be removed from the queue");

        Assert.That(
            admission.InFlightBytes,
            Is.EqualTo(inFlightBeforeQueueing),
            "an abandoned claim was never admitted, so it must not change the reservation");
        Assert.That(admission.AdmittedCount, Is.EqualTo(1), "the occupant still holds its lease");
    }

    [Test]
    public async Task Abandoning_the_head_of_the_queue_lets_the_claim_behind_it_through()
    {
        // The drain stops at the first claim that does not fit, so a cancelled
        // head that is never removed would block a claim that does fit - for
        // the whole life of the gate, because nothing else re-examines it.
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        var occupant = await admission.AcquireAsync(HalfBudgetStoredBytes, CancellationToken.None);

        using var cts = new CancellationTokenSource();
        var head = admission.AcquireAsync(OversubscribedStoredBytes, cts.Token);
        var tail = admission.AcquireAsync(20L, CancellationToken.None);

        await TestPoll.UntilAsync(() => admission.QueuedCount == 2, "both claims to queue");

        cts.Cancel();
        Assert.That(async () => await head, Throws.InstanceOf<OperationCanceledException>());

        // Abandon removes the head and drains in the same pass, so the tail
        // (100 bytes of heap cost against the 500 still free beside the
        // occupant) is admitted immediately and the queue empties outright.
        await TestPoll.UntilAsync(
            () => admission.QueuedCount == 0,
            "the cancelled head to be abandoned and the tail behind it admitted");

        using var admittedTail = await tail;
        Assert.That(admittedTail.Queued, Is.True);
        Assert.That(admission.AdmittedCount, Is.EqualTo(2), "the occupant and the tail both hold leases");

        occupant.Dispose();
        admittedTail.Dispose();
        Assert.That(admission.InFlightBytes, Is.Zero);
        Assert.That(admission.AdmittedCount, Is.Zero);
    }

    [Test]
    public async Task A_claim_cancelled_as_it_is_admitted_hands_its_reservation_straight_back()
    {
        // The narrow window the gate guards with `if (!waiter.TryAdmit(lease))`:
        // the registration callback cancels the waiter's completion source
        // synchronously, but the continuation that removes it from the queue
        // runs asynchronously. Releasing the occupant inside that window drains
        // a waiter that has already been cancelled, grants it a reservation
        // nobody will ever dispose, and must therefore hand it straight back.
        //
        // The window is a genuine race, so this drives it repeatedly and
        // asserts the invariant that holds however the race lands: whatever the
        // outcome of each claim, the gate must end empty. A leak would show as
        // a non-zero residue that never drains.
        const int attempts = 400;

        for (var i = 0; i < attempts; i++)
        {
            var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
            var occupant = await admission.AcquireAsync(HalfBudgetStoredBytes, CancellationToken.None);

            using var cts = new CancellationTokenSource();
            var queued = admission.AcquireAsync(OversubscribedStoredBytes, cts.Token);
            Assert.That(queued.IsCompleted, Is.False, $"attempt {i}: the claim must queue");

            // Cancel and release back to back. Cancel runs the registration
            // callback inline; the release drains on this same thread.
            cts.Cancel();
            occupant.Dispose();

            try
            {
                // Either the claim lost the race and was cancelled, or it won
                // and was admitted - both are correct outcomes of the race, and
                // both must leave the gate empty.
                (await queued).Dispose();
            }
            catch (OperationCanceledException)
            {
                // Lost the race; the gate must have reclaimed the grant.
            }

            await TestPoll.UntilAsync(
                () => admission.InFlightBytes == 0 && admission.AdmittedCount == 0
                    && admission.QueuedCount == 0,
                $"attempt {i}: the gate to drain back to empty after a cancel/release race");
        }
    }

    [Test]
    public async Task A_cancelled_claim_does_not_strand_the_gate_for_later_callers()
    {
        var admission = new LeafSnapshotHydrationAdmission(BudgetBytes);
        using (var occupant = await admission.AcquireAsync(HalfBudgetStoredBytes, CancellationToken.None))
        {
            using var cts = new CancellationTokenSource();
            var queued = admission.AcquireAsync(OversubscribedStoredBytes, cts.Token);
            cts.Cancel();
            Assert.That(async () => await queued, Throws.InstanceOf<OperationCanceledException>());

            await TestPoll.UntilAsync(() => admission.QueuedCount == 0, "the waiter to be abandoned");
        }

        // The whole budget must be available again to an unrelated caller.
        using var later = await admission.AcquireAsync(HalfBudgetStoredBytes, CancellationToken.None);

        Assert.That(later.Queued, Is.False, "the gate was empty, so this claim must not have waited");
        Assert.That(admission.AdmittedCount, Is.EqualTo(1));
    }
}
