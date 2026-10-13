using System.Reflection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerPhysicalStagingTests
{
    [Test]
    public async Task Drain_rebind_resolved_eviction_queue_does_not_evict_new_log_duplicate_ownership()
        => await AssertResolvedQueueAsync(retainOldQueue: false);

    [Test]
    public void Drain_resolved_queue_detector_rejects_retained_old_queue_only_control()
    {
        var failure = Assert.ThrowsAsync<AssertionException>(async () =>
            await AssertResolvedQueueAsync(retainOldQueue: true));
        Assert.That(failure!.Message, Does.Contain("new-duplicate"));
    }

    private static async Task AssertResolvedQueueAsync(bool retainOldQueue)
    {
        var capacity = (int)(typeof(ViewMaintainerGrain)
            .GetField("ResolvedCapacity", BindingFlags.NonPublic | BindingFlags.Static)?.GetRawConstantValue()
            ?? throw new InvalidOperationException("Resolved eviction capacity anchor disappeared"));
        Assert.That(capacity, Is.GreaterThan(1));
        var h = await Harness.CreateAsync();
        h.Append(Old, 7, Prepare(Old, "old-complete", 11));
        h.Append(Old, 8, Terminal(Old));
        await h.Grain.DrainAsync();
        var oldQueue = ((Queue<Guid>)Field("_resolvedOrder").GetValue(h.Grain)!).ToArray();
        Assert.That(oldQueue, Is.EqualTo(new[] { Transaction }));
        h.Alias = New;
        await h.Grain.NotifySourceIdentityChangedAsync(New);
        await h.Grain.DrainAsync();
        if (retainOldQueue)
        {
            h.BeforeTailEntries = tree =>
            {
                Assert.That(tree, Is.EqualTo(New));
                Assert.That((HashSet<Guid>)Field("_resolved").GetValue(h.Grain)!, Is.Empty,
                    "the control leaves the resolved set correctly scoped to the newly selected log");
                var queue = (Queue<Guid>)Field("_resolvedOrder").GetValue(h.Grain)!;
                foreach (var id in oldQueue) queue.Enqueue(id);
                h.BeforeTailEntries = null;
            };
        }
        h.Append(New, 2, Prepare(New, "new-complete", 21));
        h.Append(New, 3, Terminal(New));
        long offset = 4;
        for (var i = 1; i < capacity; i++)
        {
            var id = new Guid(i, 0, 0, new byte[8]);
            h.Append(New, offset++, Terminal(New) with { TransactionId = id, Kind = MutationKind.TxAbort });
        }
        await h.Grain.DrainAsync();
        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key),
            Is.EqualTo(new[] { "old-complete", "new-complete" }));
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(offset - 1));
        Assert.That(((Queue<Guid>)Field("_resolvedOrder").GetValue(h.Grain)!).Count, Is.EqualTo(capacity),
            "actual production resolution reached its bounded eviction boundary");

        h.Append(New, offset++, Prepare(New, "new-duplicate", 22));
        h.Append(New, offset++, Terminal(New));
        await h.Grain.DrainAsync();

        Assert.That(h.AtomicWrites.SelectMany(x => x.Writes).Select(x => x.Key),
            Is.EqualTo(new[] { "old-complete", "new-complete" }),
            "new-duplicate must remain suppressed: an old log's queue entry cannot evict the same id owned by the new log");
        Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(offset - 1));
    }
}
