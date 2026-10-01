using NUnit.Framework;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="ShareBoundedTouchRunner"/>, the share-bounded
/// driver of one WAL GC reactivation pass's touches (issue #3761 item 1(a)).
/// </summary>
[TestFixture]
public class ShareBoundedTouchRunnerTests
{
    /// <summary>A scripted pass: records every start and lets the test settle touches by hand.</summary>
    private sealed class Script
    {
        private readonly object _gate = new();
        private readonly List<(int Index, TaskCompletionSource<bool> Completion)> _pending = [];

        public List<int> Starts { get; } = [];

        public int InFlight { get; private set; }

        public int MaxInFlight { get; private set; }

        public bool MayLaunch { get; set; } = true;

        public Task<bool> Touch(int index)
        {
            var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_gate)
            {
                Starts.Add(index);
                _pending.Add((index, completion));
                InFlight++;
                MaxInFlight = Math.Max(MaxInFlight, InFlight);
            }

            return completion.Task;
        }

        public int[] Pending()
        {
            lock (_gate)
            {
                return _pending.Select(p => p.Index).ToArray();
            }
        }

        public void Settle(int index, bool refused)
        {
            TaskCompletionSource<bool> completion;
            lock (_gate)
            {
                var at = _pending.FindIndex(p => p.Index == index);
                completion = _pending[at].Completion;
                _pending.RemoveAt(at);
                InFlight--;
            }

            completion.SetResult(refused);
        }

        public void Fault(int index, Exception fault)
        {
            TaskCompletionSource<bool> completion;
            lock (_gate)
            {
                var at = _pending.FindIndex(p => p.Index == index);
                completion = _pending[at].Completion;
                _pending.RemoveAt(at);
                InFlight--;
            }

            completion.SetException(fault);
        }
    }

    private static Task RunAsync(Script script, int count, int concurrency, bool leadAlone = false) =>
        ShareBoundedTouchRunner.RunAsync(
            count,
            concurrency,
            leadAlone,
            script,
            static (s, i) => s.Touch(i),
            static s => s.MayLaunch);

    /// <summary>Waits until the script shows exactly <paramref name="expected"/> touches pending.</summary>
    /// <remarks>
    /// The barrier fails by name at the wait itself. Returning the short array it
    /// happened to observe would surface a timeout as whatever the caller asserted
    /// next - a length mismatch, or an <see cref="InvalidOperationException"/> out of
    /// <c>Single()</c> on an empty array - neither of which names the wait that failed.
    /// </remarks>
    private static async Task<int[]> PendingAsync(Script script, int expected)
    {
        await TestPoll.UntilAsync(
            () => script.Pending().Length == expected,
            $"the script must settle at exactly {expected} touch(es) pending",
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMilliseconds(1));

        return script.Pending();
    }

    [Test]
    public async Task RunAsync_never_holds_more_touches_in_flight_than_the_concurrency()
    {
        var script = new Script();
        var run = RunAsync(script, count: 7, concurrency: 2);

        for (var settled = 0; settled < 7; settled++)
        {
            var pending = await PendingAsync(script, Math.Min(2, 7 - settled));
            Assert.That(pending, Has.Length.EqualTo(Math.Min(2, 7 - settled)));
            script.Settle(pending.Min(), refused: false);
        }

        await run;
        Assert.Multiple(() =>
        {
            Assert.That(script.MaxInFlight, Is.EqualTo(2));
            Assert.That(script.Starts, Is.EqualTo(new[] { 0, 1, 2, 3, 4, 5, 6 }),
                "touches must start lowest index first, so the caller's ranking decides who takes a freed slot.");
        });
    }

    [Test]
    public async Task RunAsync_clamps_the_concurrency_to_at_least_one()
    {
        var script = new Script();
        var run = RunAsync(script, count: 3, concurrency: 0);

        for (var settled = 0; settled < 3; settled++)
        {
            var pending = await PendingAsync(script, 1);
            script.Settle(pending.Single(), refused: false);
        }

        await run;
        Assert.That(script.MaxInFlight, Is.EqualTo(1));
    }

    [Test]
    public async Task RunAsync_clamps_the_concurrency_to_the_count()
    {
        var script = new Script();
        var run = RunAsync(script, count: 2, concurrency: 50);

        var pending = await PendingAsync(script, 2);
        foreach (var index in pending)
        {
            script.Settle(index, refused: false);
        }

        await run;
        Assert.That(script.MaxInFlight, Is.EqualTo(2));
    }

    [Test]
    public async Task RunAsync_with_a_zero_count_starts_nothing()
    {
        var script = new Script();
        await RunAsync(script, count: 0, concurrency: 4);
        Assert.That(script.Starts, Is.Empty);
    }

    [Test]
    public async Task RunAsync_leading_alone_starts_nothing_else_until_touch_zero_finishes()
    {
        var script = new Script();
        var run = RunAsync(script, count: 3, concurrency: 3, leadAlone: true);

        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 0 }));
        await Task.Delay(20);
        Assert.That(script.Pending(), Is.EqualTo(new[] { 0 }),
            "no other touch may start while the lead touch is still in flight.");

        script.Settle(0, refused: false);
        var rest = await PendingAsync(script, 2);
        Assert.That(rest, Is.EquivalentTo(new[] { 1, 2 }));
        foreach (var index in rest)
        {
            script.Settle(index, refused: false);
        }

        await run;
    }

    [Test]
    public async Task RunAsync_leading_alone_with_a_single_touch_runs_it_once()
    {
        var script = new Script();
        var run = RunAsync(script, count: 1, concurrency: 1, leadAlone: true);

        script.Settle((await PendingAsync(script, 1)).Single(), refused: false);
        await run;
        Assert.That(script.Starts, Is.EqualTo(new[] { 0 }));
    }

    [Test]
    public async Task RunAsync_retries_a_refused_touch_once_a_sibling_has_finished()
    {
        var script = new Script();
        var run = RunAsync(script, count: 3, concurrency: 1);

        script.Settle((await PendingAsync(script, 1)).Single(), refused: true);

        // Touch 0 was refused with nothing finished since, so touch 1 takes the
        // slot; once it finishes, 0 is eligible again and outranks 2.
        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 1 }));
        script.Settle(1, refused: false);
        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 0 }));
        script.Settle(0, refused: false);
        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 2 }));
        script.Settle(2, refused: false);

        await run;
        Assert.That(script.Starts, Is.EqualTo(new[] { 0, 1, 0, 2 }));
    }

    [Test]
    public async Task RunAsync_does_not_retry_a_refusal_with_no_sibling_left_to_finish()
    {
        var script = new Script();
        var run = RunAsync(script, count: 1, concurrency: 1);

        script.Settle((await PendingAsync(script, 1)).Single(), refused: true);

        await run;
        Assert.That(script.Starts, Is.EqualTo(new[] { 0 }),
            "a refusal the pass cannot see a freed slot for is left to the scheduler's cross-pass retry.");
    }

    [Test]
    public async Task RunAsync_retries_one_touch_at_most_MaxInPassRetries_times()
    {
        var script = new Script();
        const int Count = 12;
        var run = RunAsync(script, count: Count, concurrency: 1);

        // Touch 0 is refused every time; the others all succeed and so each
        // finishes as a sibling, making 0 eligible again after each one.
        const int ExpectedStarts = Count + ShareBoundedTouchRunner.MaxInPassRetries;
        for (var settled = 0; settled < ExpectedStarts; settled++)
        {
            var pending = (await PendingAsync(script, 1)).Single();
            script.Settle(pending, refused: pending == 0);
        }

        await run;
        Assert.Multiple(() =>
        {
            Assert.That(script.Starts.Count(i => i == 0), Is.EqualTo(1 + ShareBoundedTouchRunner.MaxInPassRetries));
            Assert.That(script.Starts.Where(i => i != 0), Is.EqualTo(Enumerable.Range(1, Count - 1)),
                "every other touch must run exactly once.");
        });
    }

    [Test]
    public async Task RunAsync_starts_nothing_further_once_mayLaunch_is_false()
    {
        var script = new Script();
        var run = RunAsync(script, count: 5, concurrency: 2);

        var first = await PendingAsync(script, 2);
        script.MayLaunch = false;
        foreach (var index in first)
        {
            script.Settle(index, refused: false);
        }

        await run;
        Assert.That(script.Starts, Is.EqualTo(new[] { 0, 1 }),
            "touches never started are the caller's to restore.");
    }

    [Test]
    public async Task RunAsync_starts_the_first_touch_even_when_mayLaunch_is_false()
    {
        var script = new Script { MayLaunch = false };
        var run = RunAsync(script, count: 3, concurrency: 3);

        script.Settle((await PendingAsync(script, 1)).Single(), refused: false);
        await run;
        Assert.That(script.Starts, Is.EqualTo(new[] { 0 }));
    }

    [Test]
    public async Task RunAsync_drains_the_touches_in_flight_and_rethrows_a_fault()
    {
        var script = new Script();
        var run = RunAsync(script, count: 4, concurrency: 3);

        await PendingAsync(script, 3);
        script.Fault(1, new OperationCanceledException("shutdown"));
        await Task.Delay(20);
        Assert.That(run.IsCompleted, Is.False,
            "the run must wait for the other touches in flight before it rethrows.");

        script.Fault(0, new InvalidOperationException("suppressed"));
        script.Settle(2, refused: false);

        Assert.That(async () => await run, Throws.TypeOf<OperationCanceledException>());
        Assert.That(script.Starts, Is.EqualTo(new[] { 0, 1, 2 }),
            "nothing further may start once a touch has faulted.");
    }

    [Test]
    public async Task RunAsync_runs_touches_that_complete_synchronously_without_parking()
    {
        var starts = new List<int>();
        var run = ShareBoundedTouchRunner.RunAsync(
            5,
            2,
            false,
            starts,
            static (s, i) =>
            {
                s.Add(i);
                return Task.FromResult(false);
            },
            static _ => true);

        Assert.That(run.IsCompleted, Is.True,
            "a pass whose touches are all already complete must never park on the wake.");
        await run;
        Assert.That(starts, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task RunAsync_is_woken_by_every_completion_racing_on_the_thread_pool()
    {
        // Completions land on pool threads while the pass arms its wake, so a
        // lost wake-up hangs the pass and a double one over-launches it.
        for (var round = 0; round < 200; round++)
        {
            var state = new RacingState(24);
            await ShareBoundedTouchRunner.RunAsync(
                24,
                3,
                round % 2 == 0,
                state,
                static (s, i) => s.TouchAsync(i),
                static _ => true).WaitAsync(TimeSpan.FromSeconds(30));

            Assert.That(state.Touched, Is.All.EqualTo(1), $"round {round}: every touch runs exactly once.");
            Assert.That(state.MaxInFlight, Is.LessThanOrEqualTo(3), $"round {round}: the concurrency bound holds.");
        }
    }

    /// <summary>Touches that finish on the thread pool after a spin of random length.</summary>
    private sealed class RacingState(int count)
    {
        private int _inFlight;
        private int _maxInFlight;

        public int[] Touched { get; } = new int[count];

        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        public async Task<bool> TouchAsync(int index)
        {
            var now = Interlocked.Increment(ref _inFlight);
            int seen;
            while ((seen = Volatile.Read(ref _maxInFlight)) < now
                && Interlocked.CompareExchange(ref _maxInFlight, now, seen) != seen)
            {
            }

            Interlocked.Increment(ref Touched[index]);
            await Task.Yield();
            Thread.SpinWait(Random.Shared.Next(0, 200));
            Interlocked.Decrement(ref _inFlight);
            return false;
        }
    }

    [Test]
    public void RunAsync_rejects_a_null_touch()
    {
        Assert.That(
            async () => await ShareBoundedTouchRunner.RunAsync(1, 1, false, 0, null!, static _ => true),
            Throws.ArgumentNullException.With.Property("ParamName").EqualTo("touch"));
    }

    [Test]
    public void RunAsync_rejects_a_null_mayLaunch()
    {
        Assert.That(
            async () => await ShareBoundedTouchRunner.RunAsync(1, 1, false, 0, static (_, _) => Task.FromResult(false), null!),
            Throws.ArgumentNullException.With.Property("ParamName").EqualTo("mayLaunch"));
    }
}
