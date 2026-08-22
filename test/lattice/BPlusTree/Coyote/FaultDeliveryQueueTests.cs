using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Unit tests for <see cref="FaultDeliveryQueue{T}"/>, the bounded fault-injecting
/// transport the Coyote liveness models broadcast terminals through. These are
/// plain deterministic unit tests (no Coyote engine): the nondeterministic
/// decision source is a scripted delegate, so drop / duplicate / reorder outcomes
/// are exercised deterministically.
/// </summary>
[TestFixture]
public sealed class FaultDeliveryQueueTests
{
    private static Func<bool> Always(bool value) => () => value;

    private static Func<bool> Script(params bool[] values)
    {
        var queue = new Queue<bool>(values);
        return () => queue.Count > 0 && queue.Dequeue();
    }

    [Test]
    public void Constructor_rejects_a_null_budget()
    {
        Assert.That(() => new FaultDeliveryQueue<int>(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Enqueue_makes_a_message_pending()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(0, 0, 0));

        Assert.That(queue.HasPending, Is.False);

        queue.Enqueue(7);

        Assert.Multiple(() =>
        {
            Assert.That(queue.HasPending, Is.True);
            Assert.That(queue.PendingCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void Try_deliver_next_on_an_empty_queue_returns_false()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(1, 1, 1));

        var delivered = queue.TryDeliverNext(Always(true), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(delivered, Is.False);
            Assert.That(message, Is.EqualTo(0));
        });
    }

    [Test]
    public void Try_deliver_next_with_no_fault_budget_delivers_the_message()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(0, 0, 0));
        queue.Enqueue(42);

        var delivered = queue.TryDeliverNext(Always(true), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(delivered, Is.True);
            Assert.That(message, Is.EqualTo(42));
            Assert.That(queue.HasPending, Is.False);
        });
    }

    [Test]
    public void Try_deliver_next_drops_the_chosen_message_when_the_drop_budget_permits()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(drops: 1, duplicates: 0, restarts: 0));
        queue.Enqueue(5);

        // Always-true: selection takes the head, then the drop is injected.
        var delivered = queue.TryDeliverNext(Always(true), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(delivered, Is.False, "a dropped message is not delivered");
            Assert.That(message, Is.EqualTo(0));
            Assert.That(queue.HasPending, Is.False, "the dropped message is removed from the queue");
        });
    }

    [Test]
    public void Try_deliver_next_duplicates_the_message_when_the_duplicate_budget_permits()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(drops: 0, duplicates: 1, restarts: 0));
        queue.Enqueue(9);

        // Drops are exhausted, so only the duplicate choice is consulted (accepted).
        var delivered = queue.TryDeliverNext(Always(true), out var first);

        Assert.Multiple(() =>
        {
            Assert.That(delivered, Is.True);
            Assert.That(first, Is.EqualTo(9));
            Assert.That(queue.PendingCount, Is.EqualTo(1), "a duplicate copy is re-enqueued for later delivery");
        });

        // The re-enqueued duplicate is delivered on the next step (budget now spent).
        var redelivered = queue.TryDeliverNext(Always(true), out var second);

        Assert.Multiple(() =>
        {
            Assert.That(redelivered, Is.True);
            Assert.That(second, Is.EqualTo(9));
            Assert.That(queue.HasPending, Is.False);
        });
    }

    [Test]
    public void Try_deliver_next_reorders_by_deferring_earlier_candidates()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(0, 0, 0));
        queue.Enqueue(100);
        queue.Enqueue(200);

        // Defer the head (false), take the second candidate (true).
        var delivered = queue.TryDeliverNext(Script(false, true), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(delivered, Is.True);
            Assert.That(message, Is.EqualTo(200), "the reorder selected the deferred-to candidate");
            Assert.That(queue.PendingCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void Remove_all_drops_every_matching_message_and_reports_the_count()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(0, 0, 0));
        queue.Enqueue(0);
        queue.Enqueue(1);
        queue.Enqueue(0);

        var removed = queue.RemoveAll(m => m == 0);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.EqualTo(2));
            Assert.That(queue.PendingCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void Remove_all_rejects_a_null_predicate()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(0, 0, 0));

        Assert.That(() => queue.RemoveAll(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Try_deliver_next_rejects_a_null_decision()
    {
        var queue = new FaultDeliveryQueue<int>(new FaultBudget(0, 0, 0));
        queue.Enqueue(1);

        Assert.That(() => queue.TryDeliverNext(null!, out _), Throws.ArgumentNullException);
    }
}
