using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression tests for issue #4465: a duplicate of an entry whose first
/// delivery is still in flight must not be acknowledged as applied. If it is,
/// the sender's cursor moves past the entry, and an abort of the first
/// delivery (a receiver restart mid-call, or a failure whose transport-level
/// response the sender has already moved past) loses the entry. Each test
/// holds the first delivery mid-flight, delivers the duplicate and asserts it
/// is deferred (a not-accepted, cursor-preserving ack); then aborts the first
/// delivery and asserts the re-delivered entry is applied. Both windows the
/// receiver can be caught in - the dependency check before a park (the window
/// the TLC trace exhibits) and the apply itself - and both the single-entry and
/// the batch paths are covered.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_defers_a_duplicate_while_the_first_apply_is_in_flight_and_applies_it_after_an_abort()
    {
        var (applier, _, apply, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10));
        var firstApply = new TaskCompletionSource();
        apply.ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, Arg.Any<long>())
            .Returns(firstApply.Task, Task.CompletedTask);

        var first = applier.ApplyAsync(entry);
        Assert.That(first.IsCompleted, Is.False, "The first delivery must be held mid-apply.");

        var duplicate = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(duplicate.Applied, Is.False);
            Assert.That(duplicate.Deferred, Is.True,
                "A duplicate of an in-flight delivery must be deferred so the sender keeps its cursor.");
        });

        // The first delivery aborts; nothing acknowledged the entry as applied.
        firstApply.SetException(new TimeoutException("simulated abort"));
        Assert.ThrowsAsync<TimeoutException>(async () => await first);

        // The sender re-delivers the entry it never got an accepted ack for.
        var redelivered = await applier.ApplyAsync(entry);

        Assert.That(redelivered.Applied, Is.True);
        await apply.Received(2).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_defers_a_duplicate_while_the_first_delivery_is_between_its_dependency_check_and_park()
    {
        var (applier, _, apply, hwm) = CreateApplier();
        var entry = SetEntry("k", Hlc(10)) with
        {
            VectorClock = DependencyOn(RemoteCluster, Hlc(10), "site-c", Hlc(5)),
        };
        var firstVectorRead = new TaskCompletionSource<VersionVector>();
        hwm.GetVectorAsync(Arg.Any<CancellationToken>())
            .Returns(firstVectorRead.Task, Task.FromResult(DependencyOn("site-c", Hlc(5))));

        var first = applier.ApplyAsync(entry);
        Assert.That(first.IsCompleted, Is.False, "The first delivery must be held at its dependency check.");

        var duplicate = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(duplicate.Applied, Is.False);
            Assert.That(duplicate.Deferred, Is.True,
                "A duplicate of a delivery still deciding whether to park must be deferred.");
        });

        firstVectorRead.SetException(new TimeoutException("simulated abort"));
        Assert.ThrowsAsync<TimeoutException>(async () => await first);

        var redelivered = await applier.ApplyAsync(entry);

        Assert.That(redelivered.Applied, Is.True);
        await apply.Received(1).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_still_acknowledges_a_duplicate_of_a_completed_apply()
    {
        var (applier, _, _, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10));

        await applier.ApplyAsync(entry);
        var duplicate = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(duplicate.Applied, Is.False);
            Assert.That(duplicate.Deferred, Is.False,
                "A re-delivery of a completed apply is a genuine duplicate and is acknowledged.");
        });
    }

    [Test]
    public async Task ApplyAsync_still_acknowledges_a_duplicate_of_a_completed_park()
    {
        var (applier, _, _, hwm) = CreateApplier();
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        var entry = SetEntry("k", Hlc(10)) with
        {
            VectorClock = DependencyOn(RemoteCluster, Hlc(10), "site-c", Hlc(5)),
        };

        var parked = await applier.ApplyAsync(entry);
        var duplicate = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(parked.Applied, Is.False);
            Assert.That(parked.Deferred, Is.False);
            Assert.That(duplicate.Deferred, Is.False);
        });
    }

    [Test]
    public async Task ApplyBatchAsync_defers_the_run_when_it_duplicates_an_in_flight_delivery_and_applies_it_after_an_abort()
    {
        var (applier, _, apply, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10));
        var other = SetEntry("other", Hlc(11));
        var firstApply = new TaskCompletionSource();
        apply.ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, Arg.Any<long>())
            .Returns(firstApply.Task);

        var first = applier.ApplyAsync(entry);
        Assert.That(first.IsCompleted, Is.False);

        var batch = await applier.ApplyBatchAsync(new[] { entry, other });

        Assert.Multiple(() =>
        {
            Assert.That(batch.Deferred, Is.True,
                "A run carrying a duplicate of an in-flight delivery must be deferred.");
            Assert.That(batch.Applied, Is.True, "The rest of the run still applies.");
        });
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 1 && items[0].Key == "other"));

        firstApply.SetException(new TimeoutException("simulated abort"));
        Assert.ThrowsAsync<TimeoutException>(async () => await first);

        // The sender re-ships the whole batch it got a not-accepted ack for.
        var resent = await applier.ApplyBatchAsync(new[] { entry, other });

        Assert.Multiple(() =>
        {
            Assert.That(resent.Deferred, Is.False);
            Assert.That(resent.Applied, Is.True);
        });
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 1 && items[0].Key == "k"));
    }

    [Test]
    public async Task ApplyBatchAsync_does_not_defer_a_duplicate_emit_pair_within_its_own_run()
    {
        // A structural rewrite's duplicate-emit pair can ride in one batch.
        // The second copy collides with the first copy's reservation, which
        // this same run holds in its pending batch; deferring the run would
        // re-collide on every re-send and never make progress.
        var (applier, _, apply, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10));

        var batch = await applier.ApplyBatchAsync(new[] { entry, SetEntry("x", Hlc(12)), entry });

        Assert.Multiple(() =>
        {
            Assert.That(batch.Deferred, Is.False);
            Assert.That(batch.Applied, Is.True);
        });
        await apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 2 && items[0].Key == "k" && items[1].Key == "x"));
    }

    [Test]
    public async Task ApplyAsync_releases_the_reservation_when_the_first_delivery_is_cancelled_so_the_reship_applies()
    {
        var (applier, _, apply, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10));
        var firstApply = new TaskCompletionSource();
        apply.ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, Arg.Any<long>())
            .Returns(firstApply.Task, Task.CompletedTask);

        var first = applier.ApplyAsync(entry);
        Assert.That((await applier.ApplyAsync(entry)).Deferred, Is.True);

        // The receive call is cancelled mid-apply: the reservation must be
        // released (not completed, not left in flight).
        firstApply.SetCanceled();
        Assert.CatchAsync<OperationCanceledException>(async () => await first);

        var reshipped = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(reshipped.Applied, Is.True);
            Assert.That(reshipped.Deferred, Is.False);
        });
        await apply.Received(2).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_a_hung_first_delivery_defers_duplicates_only_until_its_grain_call_times_out()
    {
        // A stuck grain call is bounded by Orleans' response timeout, which
        // surfaces as a TimeoutException on the awaited call. Duplicates are
        // deferred while it hangs and the re-ship applies once it times out.
        var (applier, _, apply, _) = CreateApplier();
        var entry = SetEntry("k", Hlc(10));
        var hung = new TaskCompletionSource();
        apply.ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(10), RemoteCluster, null, Arg.Any<long>())
            .Returns(hung.Task, Task.CompletedTask);

        var first = applier.ApplyAsync(entry);
        for (var attempt = 0; attempt < 3; attempt++)
        {
            Assert.That((await applier.ApplyAsync(entry)).Deferred, Is.True, $"Duplicate {attempt} while hung.");
        }

        hung.SetException(new TimeoutException("Response did not arrive on time"));
        Assert.ThrowsAsync<TimeoutException>(async () => await first);

        Assert.That((await applier.ApplyAsync(entry)).Applied, Is.True);
    }

    [Test]
    public async Task ApplyBatchAsync_releases_every_reservation_when_the_run_flush_fails_so_the_reship_applies()
    {
        var (applier, _, apply, _) = CreateApplier();
        var batch = new[] { SetEntry("a", Hlc(10)), SetEntry("b", Hlc(11)) };
        apply.ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>())
            .Returns(Task.FromException(new TimeoutException("simulated abort")), Task.CompletedTask);

        Assert.ThrowsAsync<TimeoutException>(async () => await applier.ApplyBatchAsync(batch));

        var reshipped = await applier.ApplyBatchAsync(batch);

        Assert.Multiple(() =>
        {
            Assert.That(reshipped.Applied, Is.True);
            Assert.That(reshipped.Deferred, Is.False);
        });
        await apply.Received(2).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items => items.Count == 2));
    }

    private static VersionVector DependencyOn(params object[] originClockPairs)
    {
        var v = new VersionVector();
        for (var i = 0; i < originClockPairs.Length; i += 2)
        {
            v.Entries[(string)originClockPairs[i]] = (HybridLogicalClock)originClockPairs[i + 1];
        }
        return v;
    }
}
