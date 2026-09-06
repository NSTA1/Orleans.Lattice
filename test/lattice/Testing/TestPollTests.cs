using System.Diagnostics;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Testing;

/// <summary>
/// Unit coverage for <see cref="TestPoll"/>, the shared bounded-poll barrier
/// that the cluster-backed fixtures now depend on in place of their own private
/// wait helpers.
/// <para>
/// The behaviour that matters is the failure behaviour, because that is the
/// whole reason the type exists: a barrier that falls through silently lets a
/// caller sample a baseline from a state the barrier never reached, and the
/// resulting vacuous comparison passes. So each test here pins one half of the
/// contract - the hard form fails AT the barrier and names what it waited for,
/// the try form reports the timeout to the caller instead of failing - rather
/// than merely checking that a satisfied condition returns.
/// </para>
/// </summary>
[TestFixture]
public sealed class TestPollTests
{
    private static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(150);
    private static readonly TimeSpan ShortCadence = TimeSpan.FromMilliseconds(5);

    [Test]
    public async Task TryUntilAsync_returns_true_as_soon_as_the_condition_holds()
    {
        var polls = 0;
        var held = await TestPoll.TryUntilAsync(() => ++polls >= 3, ShortTimeout, ShortCadence);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True);
            Assert.That(polls, Is.EqualTo(3),
                "the barrier must stop sampling the moment the condition holds");
        });
    }

    [Test]
    public async Task TryUntilAsync_returns_false_when_the_deadline_elapses()
    {
        var held = await TestPoll.TryUntilAsync(() => false, ShortTimeout, ShortCadence);

        Assert.That(held, Is.False,
            "the try form reports a timeout to the caller rather than failing the test");
    }

    [Test]
    public async Task TryUntilAsync_samples_once_more_after_the_deadline()
    {
        // The first sample deliberately consumes the entire budget before
        // returning false, so by the time it returns the loop's deadline has
        // certainly elapsed. That makes the documented final sample the ONLY
        // way the condition can be observed - and it is established by the
        // probe itself rather than by a second racing clock, so the test is
        // deterministic. Without that final sample a scheduling hiccup that
        // straddles the deadline fails a test whose condition did in fact hold.
        var polls = 0;

        var held = await TestPoll.TryUntilAsync(
            () =>
            {
                if (++polls > 1)
                {
                    return true;
                }

                Thread.Sleep(ShortTimeout + ShortTimeout);
                return false;
            },
            ShortTimeout,
            ShortCadence);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True,
                "the condition only became observable after the deadline had elapsed");
            Assert.That(polls, Is.EqualTo(2),
                "exactly one further sample is taken once the deadline has passed");
        });
    }

    [Test]
    public async Task UntilAsync_returns_without_failing_when_the_condition_holds()
    {
        var polls = 0;

        await TestPoll.UntilAsync(
            () => ++polls >= 2, "the sample to land", ShortTimeout, ShortCadence);

        Assert.That(polls, Is.EqualTo(2),
            "the hard barrier returns on the first sample that holds, without failing");
    }

    [Test]
    public void UntilAsync_fails_at_the_barrier_and_quotes_what_it_waited_for()
    {
        // This is the contract that makes the type worth having: the failure
        // names the unmet condition at the point the wait gave up, instead of
        // letting the caller sample an unreached state and assert on it.
        Assert.That(
            async () => await TestPoll.UntilAsync(
                () => false, "the durable pin to be published", ShortTimeout, ShortCadence),
            Throws.InstanceOf<AssertionException>()
                  .And.Message.Contains("the durable pin to be published"));
    }

    [Test]
    public async Task TryUntilAsync_awaits_an_asynchronous_probe_until_it_holds()
    {
        var polls = 0;
        var held = await TestPoll.TryUntilAsync(
            async () =>
            {
                await Task.Yield();
                return ++polls >= 3;
            },
            ShortTimeout,
            ShortCadence);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.True);
            Assert.That(polls, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task TryUntilAsync_returns_false_when_an_asynchronous_probe_never_holds()
    {
        var held = await TestPoll.TryUntilAsync(
            async () => { await Task.Yield(); return false; }, ShortTimeout, ShortCadence);

        Assert.That(held, Is.False);
    }

    [Test]
    public void UntilAsync_fails_at_the_barrier_for_an_asynchronous_probe_too()
    {
        Assert.That(
            async () => await TestPoll.UntilAsync(
                async () => { await Task.Yield(); return false; },
                "the shipped batch to be applied",
                ShortTimeout,
                ShortCadence),
            Throws.InstanceOf<AssertionException>()
                  .And.Message.Contains("the shipped batch to be applied"));
    }

    [Test]
    public void A_null_condition_is_rejected_by_both_probe_shapes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await TestPoll.TryUntilAsync((Func<bool>)null!),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await TestPoll.TryUntilAsync((Func<Task<bool>>)null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_failing_barrier_does_not_spin_far_past_its_deadline()
    {
        // A barrier that overruns its own deadline turns one unmet condition
        // into a whole-suite slowdown, so the bound is part of the contract.
        var clock = Stopwatch.StartNew();
        _ = await TestPoll.TryUntilAsync(() => false, ShortTimeout, ShortCadence);
        clock.Stop();

        Assert.That(clock.Elapsed, Is.LessThan(ShortTimeout + TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void The_defaults_are_a_usable_barrier()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TestPoll.DefaultTimeout, Is.GreaterThan(TimeSpan.Zero));
            Assert.That(TestPoll.DefaultCadence, Is.GreaterThan(TimeSpan.Zero));
            Assert.That(TestPoll.DefaultCadence, Is.LessThan(TestPoll.DefaultTimeout),
                "a cadence at or above the timeout would sample at most once");
        });
    }
}
