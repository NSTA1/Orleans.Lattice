using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="TxRegistryReadCoalescer"/>, the per-silo
/// single-flight coalescer over tree-wide registry reads (issue #3501). The
/// load-bearing rule pinned here is the post-fan-out one: a probe caller that
/// arrives while a round is in flight must never share that round, because that
/// round was issued before the caller's fan-out finished.
/// </summary>
[TestFixture]
public class TxRegistryReadCoalescerTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    /// <summary>A round of controllable work: each invocation gets its own completion source.</summary>
    private sealed class ControlledWork<T>
    {
        private readonly ConcurrentQueue<TaskCompletionSource<T>> _issued = new();
        private readonly SemaphoreSlim _started = new(0);

        public int IssuedCount => _issued.Count;

        public Task<T> Invoke()
        {
            var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            _issued.Enqueue(tcs);
            _started.Release();
            return tcs.Task;
        }

        public async Task<TaskCompletionSource<T>> NextIssuedAsync()
        {
            Assert.That(await _started.WaitAsync(Timeout), Is.True, "Expected a round to be issued.");
            return _issued.ToArray()[^1];
        }
    }

    [Test]
    public async Task JoinSlot_callers_join_the_round_in_flight()
    {
        var slots = new ConcurrentDictionary<string, TxRegistryReadCoalescer.JoinSlot<int>>();
        var work = new ControlledWork<int>();

        var first = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "t", work.Invoke);
        var round = await work.NextIssuedAsync();
        var second = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "t", work.Invoke);

        Assert.That(second, Is.SameAs(first));
        round.SetResult(42);
        var results = await Task.WhenAll(first, second).WaitAsync(Timeout);
        Assert.That(results, Is.EqualTo(new[] { 42, 42 }));
        Assert.That(work.IssuedCount, Is.EqualTo(1));
    }

    [Test]
    public async Task JoinSlot_drops_the_round_on_completion_so_the_next_caller_issues_fresh()
    {
        var slots = new ConcurrentDictionary<string, TxRegistryReadCoalescer.JoinSlot<int>>();
        var work = new ControlledWork<int>();

        var first = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "t", work.Invoke);
        (await work.NextIssuedAsync()).SetResult(1);
        await first;
        await WaitUntilAsync(() => slots.IsEmpty);

        var second = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "t", work.Invoke);
        (await work.NextIssuedAsync()).SetResult(2);

        Assert.That(await second, Is.EqualTo(2));
        Assert.That(work.IssuedCount, Is.EqualTo(2));
    }

    [Test]
    public async Task JoinSlot_does_not_cache_a_faulted_round()
    {
        var slots = new ConcurrentDictionary<string, TxRegistryReadCoalescer.JoinSlot<int>>();
        var work = new ControlledWork<int>();

        var first = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "t", work.Invoke);
        (await work.NextIssuedAsync()).SetException(new InvalidOperationException("boom"));
        Assert.ThrowsAsync<InvalidOperationException>(async () => await first);
        await WaitUntilAsync(() => slots.IsEmpty);

        var second = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "t", work.Invoke);
        (await work.NextIssuedAsync()).SetResult(7);

        Assert.That(await second, Is.EqualTo(7));
    }

    [Test]
    public async Task JoinSlot_keeps_trees_independent()
    {
        var slots = new ConcurrentDictionary<string, TxRegistryReadCoalescer.JoinSlot<int>>();
        var work = new ControlledWork<int>();

        var a = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "a", work.Invoke);
        await work.NextIssuedAsync();
        var b = TxRegistryReadCoalescer.JoinSlot<int>.Join(slots, "b", work.Invoke);
        await work.NextIssuedAsync();

        Assert.That(b, Is.Not.SameAs(a));
        Assert.That(work.IssuedCount, Is.EqualTo(2));
    }

    [Test]
    public async Task FreshSlot_caller_arriving_mid_round_does_not_join_that_round()
    {
        var slots = new ConcurrentDictionary<string, TxRegistryReadCoalescer.FreshSlot<int>>();
        var work = new ControlledWork<int>();

        var early = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);
        var firstRound = await work.NextIssuedAsync();

        // Arrives while the first round is in flight: must get a later round.
        var late = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);
        Assert.That(late, Is.Not.SameAs(early));

        firstRound.SetResult(1);
        Assert.That(await early, Is.EqualTo(1));
        Assert.That(late.IsCompleted, Is.False, "The late caller must not observe the round issued before it arrived.");

        var secondRound = await work.NextIssuedAsync();
        secondRound.SetResult(2);

        Assert.That(await late.WaitAsync(Timeout), Is.EqualTo(2));
        Assert.That(work.IssuedCount, Is.EqualTo(2));
    }

    [Test]
    public async Task FreshSlot_late_callers_share_one_queued_round()
    {
        var slots = new ConcurrentDictionary<string, TxRegistryReadCoalescer.FreshSlot<int>>();
        var work = new ControlledWork<int>();

        _ = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);
        var firstRound = await work.NextIssuedAsync();
        var late1 = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);
        var late2 = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);

        Assert.That(late2, Is.SameAs(late1));
        firstRound.SetResult(1);
        (await work.NextIssuedAsync()).SetResult(2);

        Assert.That(await late1.WaitAsync(Timeout), Is.EqualTo(2));
        Assert.That(work.IssuedCount, Is.EqualTo(2), "At most one round in flight and one queued.");
        await WaitUntilAsync(() => slots.IsEmpty);
    }

    [Test]
    public async Task FreshSlot_propagates_a_queued_round_fault_without_caching_it()
    {
        var slots = new ConcurrentDictionary<string, TxRegistryReadCoalescer.FreshSlot<int>>();
        var work = new ControlledWork<int>();

        _ = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);
        var firstRound = await work.NextIssuedAsync();
        var late = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);
        firstRound.SetResult(1);
        (await work.NextIssuedAsync()).SetException(new InvalidOperationException("boom"));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await late.WaitAsync(Timeout));
        await WaitUntilAsync(() => slots.IsEmpty);

        var next = TxRegistryReadCoalescer.FreshSlot<int>.Join(slots, "t", work.Invoke);
        (await work.NextIssuedAsync()).SetResult(3);
        Assert.That(await next, Is.EqualTo(3));
    }

    [Test]
    public async Task GetSnapshotAsync_coalesces_concurrent_callers_into_one_fan_out()
    {
        var (coalescer, _, gate) = CreateGated();

        var first = coalescer.GetSnapshotAsync("tree");
        await WaitUntilAsync(() => gate.Calls > 0);
        var second = coalescer.GetSnapshotAsync("tree");
        gate.Release();

        var results = await Task.WhenAll(first, second).WaitAsync(Timeout);
        Assert.Multiple(() =>
        {
            Assert.That(results[0].Revision, Is.EqualTo(5));
            Assert.That(results[1].Revision, Is.EqualTo(5));
            Assert.That(gate.Calls, Is.EqualTo(1), "Both callers must share one registry read.");
        });
        await WaitUntilAsync(() => coalescer.ActiveTreeCount == 0);
    }

    [Test]
    public async Task GetFreshSnapshotAsync_mid_round_caller_waits_for_the_next_round()
    {
        var (coalescer, _, gate) = CreateGated();

        var early = coalescer.GetFreshSnapshotAsync("tree");
        await WaitUntilAsync(() => gate.Calls == 1);
        var late = coalescer.GetFreshSnapshotAsync("tree");
        gate.Release();
        await early.WaitAsync(Timeout);
        await WaitUntilAsync(() => gate.Calls == 2);
        gate.Release();

        await late.WaitAsync(Timeout);
        Assert.That(gate.Calls, Is.EqualTo(2));
        await WaitUntilAsync(() => coalescer.ActiveTreeCount == 0);
    }

    [Test]
    public async Task GetRevisionAsync_mid_round_caller_waits_for_the_next_round()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ITxRegistryGrain>();
        var calls = 0;
        var rounds = new ConcurrentQueue<TaskCompletionSource<long>>();
        registry.GetDecisionsRevisionAsync().Returns(_ =>
        {
            Interlocked.Increment(ref calls);
            var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            rounds.Enqueue(tcs);
            return tcs.Task;
        });
        factory.GetGrain<ITxRegistryGrain>("tree").Returns(registry);
        var coalescer = new TxRegistryReadCoalescer(factory, SingleShardOptions());

        var early = coalescer.GetRevisionAsync("tree");
        await WaitUntilAsync(() => Volatile.Read(ref calls) == 1);
        var late = coalescer.GetRevisionAsync("tree");
        rounds.TryDequeue(out var first);
        first!.SetResult(10);
        Assert.That(await early.WaitAsync(Timeout), Is.EqualTo(10));

        await WaitUntilAsync(() => Volatile.Read(ref calls) == 2);
        rounds.TryDequeue(out var second);
        second!.SetResult(11);

        Assert.That(await late.WaitAsync(Timeout), Is.EqualTo(11));
    }

    [Test]
    public async Task Cancelling_one_waiter_does_not_cancel_the_shared_round()
    {
        var (coalescer, _, gate) = CreateGated();
        using var cts = new CancellationTokenSource();

        var cancelled = coalescer.GetSnapshotAsync("tree", cts.Token);
        await WaitUntilAsync(() => gate.Calls > 0);
        var survivor = coalescer.GetSnapshotAsync("tree");
        cts.Cancel();

        Assert.That(async () => await cancelled, Throws.InstanceOf<OperationCanceledException>());
        gate.Release();
        Assert.That((await survivor.WaitAsync(Timeout)).Revision, Is.EqualTo(5));
    }

    [Test]
    public void Constructor_and_members_reject_null_arguments()
    {
        var factory = Substitute.For<IGrainFactory>();
        var options = SingleShardOptions();
        var coalescer = new TxRegistryReadCoalescer(factory, options);

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new TxRegistryReadCoalescer(null!, options));
            Assert.Throws<ArgumentNullException>(() => new TxRegistryReadCoalescer(factory, null!));
            Assert.Throws<ArgumentNullException>(() => coalescer.GetSnapshotAsync(null!));
            Assert.Throws<ArgumentNullException>(() => coalescer.GetFreshSnapshotAsync(null!));
            Assert.Throws<ArgumentNullException>(() => coalescer.GetRevisionAsync(null!));
        });
    }

    private sealed class Gate
    {
        private readonly SemaphoreSlim _release = new(0);
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public async Task<TxRegistrySnapshot> Snapshot()
        {
            Interlocked.Increment(ref _calls);
            await _release.WaitAsync();
            return new TxRegistrySnapshot { Decisions = new(), Revision = 5 };
        }

        public void Release() => _release.Release();
    }

    private static (TxRegistryReadCoalescer Coalescer, IGrainFactory Factory, Gate Gate) CreateGated()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ITxRegistryGrain>();
        var gate = new Gate();
        registry.SnapshotWithRevisionAsync().Returns(_ => gate.Snapshot());
        factory.GetGrain<ITxRegistryGrain>("tree").Returns(registry);
        return (new TxRegistryReadCoalescer(factory, SingleShardOptions()), factory, gate);
    }

    private static IOptionsMonitor<LatticeOptions> SingleShardOptions()
    {
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { TxRegistryShardCount = 1 });
        return options;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail("Condition not reached within the timeout.");
            }

            await Task.Delay(5);
        }
    }
}
