using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="BoundedFanOut"/>, the ordered, concurrency-bounded
/// fan-out that replaced the unbounded <c>Task.WhenAll</c> levels in the
/// storage-usage roll-up (issue #1728). Pins the three properties its callers
/// rely on: the bound is real, slot ordering survives it, and no launched task
/// is ever abandoned unobserved.
/// </summary>
[TestFixture]
public sealed class BoundedFanOutTests
{
    [Test]
    public async Task RunAsync_generic_returns_results_in_slot_order()
    {
        // Later slots finish first, so completion order is the reverse of slot
        // order and an append-as-they-finish implementation would fail here.
        var results = await BoundedFanOut.RunAsync(8, 3, async slot =>
        {
            await Task.Delay((8 - slot) * 4, CancellationToken.None);
            return slot;
        });

        Assert.That(results, Is.EqualTo(new[] { 0, 1, 2, 3, 4, 5, 6, 7 }).AsCollection);
    }

    [Test]
    public async Task RunAsync_never_exceeds_the_bound()
    {
        const int Bound = 4;
        var inFlight = 0;
        var peak = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await BoundedFanOut.RunAsync(32, Bound, async _ =>
        {
            var current = Interlocked.Increment(ref inFlight);
            RecordPeak(ref peak, current);
            if (current >= Bound) release.TrySetResult();
            await release.Task;
            Interlocked.Decrement(ref inFlight);
        });

        Assert.That(peak, Is.EqualTo(Bound));
    }

    [Test]
    public async Task RunAsync_bound_at_or_above_count_runs_every_slot()
    {
        var ran = 0;
        await BoundedFanOut.RunAsync(5, 1000, _ =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        });

        Assert.That(ran, Is.EqualTo(5));
    }

    [TestCase(0)]
    [TestCase(-7)]
    public async Task RunAsync_bound_below_one_is_clamped_rather_than_deadlocking(int bound)
    {
        var ran = 0;
        var run = BoundedFanOut.RunAsync(4, bound, _ =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        });

        var finished = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(10)));

        Assert.That(finished, Is.SameAs(run), "a non-positive bound must clamp to 1, not stall forever");
        await run;
        Assert.That(ran, Is.EqualTo(4));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public async Task RunAsync_non_positive_count_is_a_noop(int count)
    {
        var ran = 0;
        await BoundedFanOut.RunAsync(count, 4, _ =>
        {
            Interlocked.Increment(ref ran);
            return Task.CompletedTask;
        });

        var results = await BoundedFanOut.RunAsync(count, 4, _ => Task.FromResult(1));

        Assert.Multiple(() =>
        {
            Assert.That(ran, Is.Zero);
            Assert.That(results, Is.Empty);
        });
    }

    [Test]
    public void RunAsync_null_body_throws_argument_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await BoundedFanOut.RunAsync(1, 1, (Func<int, Task>)null!),
                Throws.ArgumentNullException);
            Assert.That(async () => await BoundedFanOut.RunAsync(1, 1, (Func<int, Task<int>>)null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task RunAsync_settles_every_slot_before_surfacing_a_fault()
    {
        var completed = 0;

        try
        {
            await BoundedFanOut.RunAsync(10, 3, async slot =>
            {
                await Task.Delay(5, CancellationToken.None);
                Interlocked.Increment(ref completed);
                if (slot == 1) throw new InvalidOperationException("slot 1 down");
            });
            Assert.Fail("the fan-out should have surfaced the slot fault");
        }
        catch (InvalidOperationException)
        {
            // Expected.
        }

        // Every slot ran to completion before the aggregate threw, so a caller's
        // catch acts on a fully-quiesced batch.
        Assert.That(completed, Is.EqualTo(10));
    }

    /// <summary>
    /// Sentinel fault thrown only by this fixture's fan-out bodies. The
    /// <see cref="TaskScheduler.UnobservedTaskException"/> hook is process-global,
    /// so in a full-suite run it also catches faults abandoned by fixtures running
    /// in parallel; matching on this type scopes the assertion to the claim this
    /// test can actually own - that <b>this</b> fan-out observed <b>its own</b>
    /// children.
    /// </summary>
    private sealed class FanOutProbeException(string message) : Exception(message);

    [Test]
    public async Task RunAsync_multiple_faults_leave_no_unobserved_task_exceptions()
    {
        var unobserved = new List<Exception>();
        void Handler(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            if (e.Exception.Flatten().InnerExceptions.Any(x => x is FanOutProbeException))
            {
                lock (unobserved) unobserved.Add(e.Exception);
            }

            e.SetObserved();
        }

        TaskScheduler.UnobservedTaskException += Handler;
        try
        {
            await RunFaultingFanOutAsync();

            for (var i = 0; i < 3; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            lock (unobserved)
            {
                Assert.That(unobserved, Is.Empty);
            }
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= Handler;
        }
    }

    /// <summary>
    /// Kept in its own frame so every task it creates becomes collectable once
    /// it returns, letting the caller's forced GC surface any fault that was
    /// never observed.
    /// </summary>
    private static async Task RunFaultingFanOutAsync()
    {
        try
        {
            await BoundedFanOut.RunAsync(16, 4, async slot =>
            {
                await Task.Delay(5, CancellationToken.None);
                throw new FanOutProbeException($"slot {slot} down");
            });
        }
        catch (FanOutProbeException)
        {
            // Only the first fault surfaces; the rest must still be observed by
            // the aggregate rather than left to the finaliser.
        }
    }

    [Test]
    public async Task RunAsync_cancellation_stops_dispatching_queued_slots()
    {
        using var cts = new CancellationTokenSource();
        const int Count = 20;
        const int Bound = 2;

        // The bodies park on a TCS that is deliberately NOT linked to the token,
        // so a slot that acquired the gate keeps its permit until the test hands
        // it back. With zero permits left, a queued waiter can only ever leave
        // the queue by observing cancellation - it can never be handed the gate -
        // so the dispatched count is stable across the assertion with no timing
        // race. (Releasing the gate first would let a release beat a waiter's
        // cancellation registration and dispatch another slot, which is correct
        // behaviour but not something a test can pin to an exact number.)
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var saturated = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatched = 0;

        var run = BoundedFanOut.RunAsync(Count, Bound, async _ =>
        {
            if (Interlocked.Increment(ref dispatched) >= Bound)
            {
                saturated.TrySetResult();
            }

            await hold.Task;
        }, cts.Token);

        await saturated.Task;

        // Cancel runs every registered callback before returning, and every slot
        // called WaitAsync synchronously during the launch loop, so all 18 queued
        // waiters are cancelled by the time this completes.
        await cts.CancelAsync();

        Assert.That(Volatile.Read(ref dispatched), Is.EqualTo(Bound),
            "cancellation must stop queued slots from ever being dispatched");

        // Releasing the gate lets a still-queued waiter win the handoff against
        // its own cancellation registration and dispatch one more slot. That is
        // correct behaviour, not a leak, so there is deliberately no post-release
        // count assertion here - the invariant worth pinning is the one above,
        // taken while the gate is fully held and therefore race-free.
        hold.SetResult();
        Assert.That(async () => await run, Throws.InstanceOf<OperationCanceledException>());
    }

    private static void RecordPeak(ref int peak, int candidate)
    {
        var observed = Volatile.Read(ref peak);
        while (candidate > observed)
        {
            var prior = Interlocked.CompareExchange(ref peak, candidate, observed);
            if (prior == observed) return;
            observed = prior;
        }
    }
}
